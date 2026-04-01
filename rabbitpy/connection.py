import logging
import queue
import sys
import threading
import time
import types
import typing

from rabbitpy import (
    __version__,
    channel0,
    events,
    exceptions,
    io,
    state,
    url_parser,
)
from rabbitpy import (
    channel as channel_mod,
)
from rabbitpy import (
    heartbeat as heartbeat_mod,
)

LOGGER = logging.getLogger(__name__)


class ClientProperties:
    information = 'See https://rabbitpy.readthedocs.io'
    platform = 'Python {0}.{1}.{2}'.format(*sys.version_info)  # noqa: UP030
    product = 'rabbitpy'
    version = __version__

    def __init__(
        self, additional_properties: dict[str, typing.Any] | None = None
    ) -> None:
        self._properties: dict[str, typing.Any] = {
            'information': self.information,
            'platform': self.platform,
            'product': self.product,
            'version': self.version,
        }
        if additional_properties:
            self._properties.update(additional_properties)
        self._properties['capabilities'] = {
            'authentication_failure_close': True,
            'basic.nack': True,
            'connection.blocked': True,
            'consumer_cancel_notify': True,
            'publisher_confirms': True,
        }

    def as_dict(self) -> dict[str, typing.Any]:
        return self._properties


class Connection(state.StatefulBase):
    """The Connection object is responsible for negotiating a connection and
    managing its state. When creating a new instance of the Connection object,
    if no URL is passed in, it uses the default connection parameters of
    localhost port 5672, virtual host / with the guest/guest username/password
    combination. Represented as a AMQP URL the connection information is:

        :code:`amqp://guest:guest@localhost:5672/%2F`

    To use a different connection, pass in a AMQP URL that follows the standard
    format:

        :code:`[scheme]://[username]:[password]@[host]:[port]/[virtual_host]`

    The following example connects to the `test` virtual host on a RabbitMQ
    server running at 192.168.1.200 port 5672 as the user "www" and the
    password rabbitmq:

        :code:`amqp://www:rabbitmq@192.168.1.200:5672/test`

    .. note::

        You should be aware that most connection exceptions may be raised
        during the use of all functionality in the library.

    :param url: The AMQP connection URL
    :param connection_name: An optional name for the connection
    :param client_properties: Optional client properties to send
    :raises: exceptions.AMQPException
    :raises: exceptions.ConnectionException
    :raises: exceptions.ConnectionResetException
    :raises: exceptions.RemoteClosedException

    """

    CANCEL_METHOD: typing.ClassVar[list[str]] = ['Basic.Cancel']

    def __init__(
        self,
        url: str | None = None,
        connection_name: str | None = None,
        client_properties: dict[str, typing.Any] | None = None,
    ) -> None:
        """Create a new instance of the Connection object"""
        super().__init__()
        self._args: url_parser.ConnectionArgs = url_parser.parse(url)
        self._channel_lock = threading.Lock()
        self._channel0: channel0.Channel0 | None = None
        self._channels: dict[int, channel_mod.Channel] = {}
        self._freed_channel_ids: set[int] = set()
        self._client_properties = ClientProperties(client_properties)
        self._events = events.Events()
        self._exceptions: queue.Queue[Exception] = queue.Queue()
        self._heartbeat: heartbeat_mod.Heartbeat | None = None
        self._io: io.IO | None = None
        self._name = connection_name or '0x%x' % id(self)  # noqa: UP031

    def __enter__(self) -> typing.Self:
        """For use as a context manager, return a handle to this object
        instance.

        """
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        unused_exc_tb: types.TracebackType | None,
    ) -> bool | None:
        """Close the connection when the context exits"""
        if exc_type and exc_val:
            self._stop_io()
            self._set_state(self.CLOSED)
            return False  # Re-raise the original exception
        try:
            exc = self._exceptions.get_nowait()
        except queue.Empty:
            pass
        else:
            self._stop_io()
            self._set_state(self.CLOSED)
            raise exc
        self.close()
        return None

    @property
    def args(self) -> url_parser.ConnectionArgs:
        """Return the connection arguments."""
        return typing.cast(url_parser.ConnectionArgs, dict(self._args))

    @property
    def blocked(self) -> bool:
        """Indicates if the connection is blocked from publishing by RabbitMQ.

        This flag indicates communication from RabbitMQ that the connection is
        blocked using the Connection.Blocked RPC notification from RabbitMQ
        that was added in RabbitMQ 3.2.

        """
        return bool(self._events.is_set(events.CONNECTION_BLOCKED))

    @property
    def capabilities(self) -> dict[str, typing.Any]:
        """Return the RabbitMQ server capabilities from negotiation."""
        if self._channel0 is None:
            return {}
        result: dict[str, typing.Any] = self._channel0.properties.get(
            'capabilities', {}
        )
        return result

    @property
    def server_properties(self) -> dict[str, typing.Any]:
        """Return the RabbitMQ server properties from negotiation."""
        if self._channel0 is None:
            return {}
        return typing.cast(dict[str, typing.Any], self._channel0.properties)

    def channel(self, blocking_read: bool = False) -> channel_mod.Channel:
        """Create and return a new AMQP channel.

        If ``blocking_read`` is ``True``, :py:meth:`Queue.get` calls within
        the channel use blocking operations which lower CPU usage and increase
        throughput.  However, ``KeyboardInterrupt`` will not be raised while
        the channel is blocked waiting for a frame.

        :param blocking_read: Enable blocking reads for higher throughput
        :raises: exceptions.ConnectionClosed
        :raises: exceptions.TooManyChannelsError

        """
        if self.closed:
            raise exceptions.ConnectionClosed()
        with self._channel_lock:
            channel_id = self._get_next_channel_id()
            channel_frames: queue.Queue[typing.Any] = queue.Queue()
            assert self._io is not None  # noqa: S101
            assert self._channel0 is not None  # noqa: S101
            ch = channel_mod.Channel(
                channel_id=channel_id,
                server_capabilities=self.capabilities,
                events=self._events,
                exception_queue=self._exceptions,
                read_queue=channel_frames,
                write_queue=self._io.write_queue,
                maximum_frame_size=self._channel0.maximum_frame_size,
                write_trigger=self._io.write_trigger,
                connection=self,
                blocking_read=blocking_read,
            )
            self._channels[channel_id] = ch
            self._io.add_channel(channel_id, channel_frames)
            ch.open()
            return ch

    def close(self) -> None:
        """Close the connection to RabbitMQ, including all open channels."""
        self._set_state(self.CLOSING)
        self._close_channels()
        if self._heartbeat is not None:
            self._heartbeat.stop()
            self._heartbeat = None
        if self._channel0 is not None:
            self._channel0.close()
        self._events.clear(events.CONNECTION_EVENT)
        self._events.clear(events.SOCKET_CLOSE)
        self._events.clear(events.SOCKET_CLOSED)
        self._events.clear(events.SOCKET_OPENED)
        self._stop_io()
        self._set_state(self.CLOSED)

    def connect(self) -> None:
        """Connect to RabbitMQ and negotiate the AMQP connection."""
        self._set_state(self.OPENING)

        self._channel0 = channel0.Channel0(
            args=self._args,
            events=self._events,
            exceptions_queue=self._exceptions,
        )

        self._io = io.IO(
            self._args['host'],
            self._args['port'],
            self._args['ssl'],
            self._args['ssl_options'],
            self._events,
            self._exceptions,
            self._args['timeout'],
        )
        self._io.add_channel(0, self._channel0.pending_frames)
        self._io.start()

        # Wait for the TCP socket to open
        socket_opened = events.SOCKET_OPENED
        while self.is_opening and not self._events.is_set(socket_opened):
            if not self._exceptions.empty():
                exception = self._exceptions.get()
                raise exception

            self._events.wait(socket_opened, self._args['timeout'])
            if not self._events.is_set(socket_opened):
                raise RuntimeError('Timeout waiting for opening the socket')

        # If the socket could not be opened, clean up and return
        if self.is_closed:
            return self.close()

        # Start AMQP channel-0 negotiation in its own thread
        self._channel0.start(self._io)

        # Wait for the AMQP handshake to complete (#133, #124: proper timeout
        # prevents indefinite hang when the server comes up mid-retry or
        # returns an error such as an invalid vhost)
        negotiation_timeout = max(self._args['timeout'] * 5, 15)
        deadline = time.monotonic() + negotiation_timeout
        while time.monotonic() < deadline:
            if self._events.is_set(events.CHANNEL0_OPENED):
                break
            if not self._exceptions.empty():
                exception = self._exceptions.get()
                self._stop_io()
                self._set_state(self.CLOSED)
                raise exception
            time.sleep(0.01)
        else:
            self._stop_io()
            self._set_state(self.CLOSED)
            raise RuntimeError(
                'Timeout waiting for AMQP connection negotiation'
            )

        # Start the heartbeat keeper if the server requires it
        if self._channel0.heartbeat_interval:
            self._heartbeat = heartbeat_mod.Heartbeat(
                self._io,
                self._channel0,
                self._channel0.heartbeat_interval,
            )
            self._heartbeat.start()

        self._set_state(self.OPEN)

    # -------------------------------------------------------------------------
    # Private helpers
    # -------------------------------------------------------------------------

    def _close_channels(self) -> None:
        """Close all open channels."""
        for ch in list(self._channels.values()):
            if not ch.closed:
                try:
                    ch.close()
                except OSError as exc:
                    LOGGER.debug('Error closing channel: %r', exc)

    def _get_next_channel_id(self) -> int:
        """Return the next available channel ID.

        Reuses IDs from previously closed channels before allocating a new
        one, preventing exhaustion of the 65 535-channel limit on long-lived
        connections (#121).  Must be called with ``_channel_lock`` held.

        """
        # Reclaim IDs from channels that have been closed since last call
        for channel_id, ch in list(self._channels.items()):
            if ch.closed:
                del self._channels[channel_id]
                self._freed_channel_ids.add(channel_id)

        if self._freed_channel_ids:
            return self._freed_channel_ids.pop()

        if not self._channels:
            return 1

        assert self._channel0 is not None  # noqa: S101
        next_id = max(self._channels.keys()) + 1
        if next_id > self._channel0.maximum_channels:
            raise exceptions.TooManyChannelsError
        return next_id

    def _stop_io(self) -> None:
        """Signal the IO thread to stop and wait for it to finish.

        Blocks until the IO thread has exited (or until the connection timeout
        elapses) so that callers can rely on the socket being fully torn down
        before they transition to CLOSED.  The join is skipped when called from
        inside the IO thread itself to prevent a deadlock.

        """
        io_thread = self._io
        if io_thread is None:
            return
        io_thread.stop()
        if (
            io_thread.is_alive()
            and io_thread is not threading.current_thread()
        ):
            io_thread.join(self._args['timeout'])
