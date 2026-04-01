"""
Channel0 handles AMQP connection negotiation on channel 0.

After the TCP socket is connected, a Channel0 thread is started which sends
the AMQP ProtocolHeader and completes the Connection.Start/Tune/Open handshake.
Once negotiation succeeds the CHANNEL0_OPENED event is set.  On failure an
exception is placed in the shared exceptions queue.

"""

import logging
import queue
import sys
import threading
import typing

import pamqp.frame
import pamqp.heartbeat
from pamqp import commands, header

from rabbitpy import __version__, exceptions
from rabbitpy import events as ev_module

if typing.TYPE_CHECKING:
    from rabbitpy import io as io_module
    from rabbitpy.url_parser import ConnectionArgs

LOGGER = logging.getLogger(__name__)

DEFAULT_LOCALE = 'en_US'


class Channel0(threading.Thread):
    """Negotiate and maintain the AMQP connection on channel 0.

    :param args: Parsed connection arguments (from url_parser.parse())
    :param events: Shared Events object for cross-thread signalling
    :param exceptions_queue: Queue for propagating exceptions to the main thread

    """

    def __init__(
        self,
        args: 'ConnectionArgs',
        events: ev_module.Events,
        exceptions_queue: queue.Queue,
    ) -> None:
        super().__init__(daemon=True, name='Channel0')
        self.pending_frames: queue.Queue[pamqp.frame.FrameTypes | None] = (
            queue.Queue()
        )
        self._args = args
        self._events = events
        self._exceptions = exceptions_queue
        self._io: io_module.IO | None = None
        self._open = False
        self._properties: dict = {}
        self._max_channels: int = args.get('channel_max', 65535)
        self._max_frame_size: int = args.get('frame_max', 131072)
        self._heartbeat_interval: int = args.get('heartbeat', 60)

    # -------------------------------------------------------------------------
    # Public interface
    # -------------------------------------------------------------------------

    @property
    def heartbeat_interval(self) -> int:
        """Return the negotiated AMQP heartbeat interval in seconds."""
        return self._heartbeat_interval

    @property
    def maximum_channels(self) -> int:
        """Return the maximum number of channels for the connection."""
        return self._max_channels

    @property
    def maximum_frame_size(self) -> int:
        """Return the maximum AMQP frame size in bytes."""
        return self._max_frame_size

    @property
    def open(self) -> bool:
        """Return True if the AMQP connection has been fully negotiated."""
        return self._open

    @property
    def properties(self) -> dict:
        """Return the server properties received in Connection.Start."""
        return self._properties

    def close(self) -> None:
        """Send Connection.Close if the connection is currently open."""
        if self._open and self._io is not None:
            try:
                self._io.write_frame(
                    0,
                    commands.Connection.Close(
                        reply_code=200,
                        reply_text='Normal shutdown',
                        class_id=0,
                        method_id=0,
                    ),
                )
            except OSError as exc:
                LOGGER.debug('Error sending Connection.Close: %r', exc)
            self._open = False

    def send_heartbeat(self) -> None:
        """Write a heartbeat frame to the server."""
        if self._io is not None:
            try:
                self._io.write_frame(0, pamqp.heartbeat.Heartbeat())
            except OSError as exc:
                LOGGER.debug('Error sending heartbeat: %r', exc)

    def start(self, io: 'io_module.IO') -> None:  # type: ignore[override]
        """Attach the IO object and start the negotiation thread.

        :param io: The connected IO thread to write frames through

        """
        self._io = io
        super().start()

    # -------------------------------------------------------------------------
    # Thread entry point
    # -------------------------------------------------------------------------

    def run(self) -> None:
        """Run the AMQP handshake.  Any exception is queued for the caller."""
        try:
            self._negotiate()
        except BaseException as exc:  # noqa: BLE001
            LOGGER.debug('Channel0 negotiation failed: %r', exc)
            self._exceptions.put(exc)
            self._events.set(ev_module.EXCEPTION_RAISED)

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _negotiate(self) -> None:
        """Perform the full AMQP 0-9-1 connection handshake."""
        assert self._io is not None  # noqa: S101

        # Step 1: send the protocol header
        self._io.write_frame(0, header.ProtocolHeader())

        # Step 2: wait for Connection.Start
        frame = self._wait_for_frame(timeout=self._args['timeout'])
        if not isinstance(frame, commands.Connection.Start):
            raise exceptions.ConnectionException(
                f'Expected Connection.Start, got {type(frame).__name__}'
            )
        self._on_connection_start(frame)

        # Step 3: wait for Connection.Tune
        frame = self._wait_for_frame(timeout=self._args['timeout'])
        if not isinstance(frame, commands.Connection.Tune):
            raise exceptions.ConnectionException(
                f'Expected Connection.Tune, got {type(frame).__name__}'
            )
        self._on_connection_tune(frame)

        # Step 4: send Connection.Open (TuneOk + Open sent in _on_connection_tune)
        frame = self._wait_for_frame(timeout=self._args['timeout'])
        if not isinstance(frame, commands.Connection.OpenOk):
            raise exceptions.ConnectionException(
                f'Expected Connection.OpenOk, got {type(frame).__name__}'
            )

        LOGGER.debug('AMQP connection negotiated')
        self._open = True
        self._events.set(ev_module.CHANNEL0_OPENED)

        # Step 5: process channel-0 frames for the lifetime of the connection
        self._run_loop()

    def _run_loop(self) -> None:
        """Process channel-0 management frames (Blocked/Unblocked/Close)."""
        while self._open:
            try:
                frame = self.pending_frames.get(timeout=1.0)
            except queue.Empty:
                continue
            if frame is None:
                break
            self._handle_runtime_frame(frame)

    def _handle_runtime_frame(self, frame: pamqp.frame.FrameTypes) -> None:
        """Dispatch a channel-0 frame received after connection is open."""
        if isinstance(frame, commands.Connection.Blocked):
            LOGGER.warning('Connection is blocked: %s', frame.reason)
            self._events.set(ev_module.CONNECTION_BLOCKED)
        elif isinstance(frame, commands.Connection.Unblocked):
            LOGGER.info('Connection is unblocked')
            self._events.clear(ev_module.CONNECTION_BLOCKED)
        elif isinstance(frame, commands.Connection.Close):
            LOGGER.error(
                'Server closed the connection (%s): %s',
                frame.reply_code,
                frame.reply_text,
            )
            self._open = False
            exc_cls = exceptions.AMQP.get(
                frame.reply_code, exceptions.RemoteClosedException
            )
            self._exceptions.put(exc_cls(frame.reply_code, frame.reply_text))
            self._events.set(ev_module.EXCEPTION_RAISED)

    def _wait_for_frame(self, timeout: float) -> pamqp.frame.FrameTypes:
        """Block until a frame arrives on pending_frames or timeout expires.

        :param timeout: Maximum seconds to wait
        :raises: exceptions.ConnectionException on timeout or IO shutdown

        """
        deadline = timeout * 3  # give extra time for negotiation round-trips
        waited = 0.0
        interval = 0.1
        while waited < deadline:
            if not self._exceptions.empty():
                raise self._exceptions.get()
            try:
                frame = self.pending_frames.get(timeout=interval)
                if frame is not None:
                    return frame
            except queue.Empty:
                pass
            waited += interval

        raise exceptions.ConnectionException(
            'Timed out waiting for AMQP server response during negotiation'
        )

    def _on_connection_start(self, frame: commands.Connection.Start) -> None:
        """Process Connection.Start and send Connection.StartOk."""
        self._properties = dict(frame.server_properties or {})
        LOGGER.debug('Server properties: %r', self._properties)
        self._io.write_frame(  # type: ignore[union-attr]
            0,
            commands.Connection.StartOk(
                client_properties=self._build_client_properties(),
                mechanism='PLAIN',
                response=f'\0{self._args["username"]}\0{self._args["password"]}',
                locale=self._args.get('locale') or DEFAULT_LOCALE,
            ),
        )

    def _on_connection_tune(self, frame: commands.Connection.Tune) -> None:
        """Process Connection.Tune, send TuneOk, then send Connection.Open."""
        self._max_channels = self._negotiate_value(
            frame.channel_max, self._args.get('channel_max', 0)
        )
        self._max_frame_size = self._negotiate_value(
            frame.frame_max, self._args.get('frame_max', 0)
        )
        # Heartbeat: 0 from either side means disabled; otherwise take minimum
        server_hb = frame.heartbeat or 0
        client_hb = self._args.get('heartbeat', 60) or 0
        if server_hb == 0 or client_hb == 0:
            self._heartbeat_interval = 0
        else:
            self._heartbeat_interval = min(server_hb, client_hb)

        LOGGER.debug(
            'Negotiated: channels=%i frame_size=%i heartbeat=%i',
            self._max_channels,
            self._max_frame_size,
            self._heartbeat_interval,
        )
        assert self._io is not None  # noqa: S101
        self._io.write_frame(
            0,
            commands.Connection.TuneOk(
                channel_max=self._max_channels,
                frame_max=self._max_frame_size,
                heartbeat=self._heartbeat_interval,
            ),
        )
        self._io.write_frame(
            0,
            commands.Connection.Open(
                virtual_host=self._args['virtual_host'],
            ),
        )

    @staticmethod
    def _negotiate_value(server_value: int, client_value: int) -> int:
        """Return the negotiated value: minimum of both, or either if the
        other is zero (meaning 'no preference').

        """
        if server_value == 0:
            return client_value
        if client_value == 0:
            return server_value
        return min(server_value, client_value)

    @staticmethod
    def _build_client_properties() -> dict:
        """Return the client property dict sent in Connection.StartOk."""
        return {
            'product': 'rabbitpy',
            'version': __version__,
            'platform': 'Python {}.{}.{}'.format(*sys.version_info[:3]),
            'information': 'See https://rabbitpy.readthedocs.io',
            'capabilities': {
                'authentication_failure_close': True,
                'basic.nack': True,
                'connection.blocked': True,
                'consumer_cancel_notify': True,
                'publisher_confirms': True,
            },
        }
