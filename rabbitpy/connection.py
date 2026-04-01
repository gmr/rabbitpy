import logging
import queue
import sys
import threading
import types
import typing

from rabbitpy import __version__, channel0, events, io, state, url_parser

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
        self._channel0 = channel0.Channel0()
        self._client_properties = ClientProperties(client_properties)
        self._events = events.Events()
        self._exceptions: queue.Queue[Exception] = queue.Queue()
        self._io: io.IO | None = None
        self._max_frame_size: int | None = None
        self._name = connection_name or '0x%x' % id(self)  # noqa: UP031
        self._write_queue: queue.Queue[typing.Any] = queue.Queue()

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

    def close(self) -> None:
        """Close the connection to RabbitMQ."""
        self._set_state(self.CLOSING)
        self._events.clear(events.CONNECTION_EVENT)
        self._events.clear(events.SOCKET_CLOSE)
        self._events.clear(events.SOCKET_CLOSED)
        self._events.clear(events.SOCKET_OPENED)
        self._stop_io()
        self._set_state(self.CLOSED)

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

    def connect(self) -> None:
        """Connect to the RabbitMQ Server"""
        self._set_state(self.OPENING)
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

        while self.is_opening and not self._events.is_set(
            events.SOCKET_OPENED
        ):
            if not self._exceptions.empty():
                exception = self._exceptions.get()
                raise exception

            self._events.wait(events.SOCKET_OPENED, self._args['timeout'])
            if not self._events.is_set(events.SOCKET_OPENED):
                raise RuntimeError('Timeout waiting for opening the socket')

        # If the socket could not be opened, return instead of waiting
        if self.is_closed:
            return self.close()
