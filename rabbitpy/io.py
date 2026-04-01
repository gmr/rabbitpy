import asyncio
import collections
import logging
import queue
import socket
import ssl
import threading
import typing

import pamqp.base
import pamqp.body
import pamqp.exceptions
import pamqp.frame
import pamqp.header
import pamqp.heartbeat

import rabbitpy.events
import rabbitpy.exceptions
from rabbitpy.url_parser import SslOptions

LOGGER = logging.getLogger(__name__)

# Convenience alias: pamqp 4 exposes FrameTypes as the canonical union.
# We extend it with None to represent "no frame decoded yet".
PamqpFrame = pamqp.frame.FrameTypes | None

AddrInfo = list[
    tuple[socket.AddressFamily, socket.SocketKind, int, str, typing.Any]
]


class IO(threading.Thread):
    """Handles asynchronous I/O for RabbitMQ connections."""

    def __init__(
        self,
        host: str,
        port: int,
        use_ssl: bool,
        ssl_options: SslOptions,
        events: rabbitpy.events.Events,
        exceptions: queue.Queue[Exception],
        timeout: float = 0.01,
    ) -> None:
        """Initialize the IO thread."""
        super().__init__(daemon=True, name='IO')
        self._events = events
        self._exceptions = exceptions
        self._host = host
        self._port = port
        self._ssl_options = ssl_options
        self._timeout = timeout
        self._use_ssl = use_ssl

        self._buffer = b''
        self._closed = asyncio.Event()
        self._closed.set()
        self._stop_requested = threading.Event()
        self._lock = threading.Lock()
        self._bytes_read = 0
        self._bytes_written = 0
        self._channels: collections.defaultdict[
            int, queue.Queue[PamqpFrame]
        ] = collections.defaultdict(queue.Queue)
        self._ioloop: asyncio.AbstractEventLoop | None = None
        self._remote_name: str | None = None
        self._socket: socket.socket | None = None
        self._write_buffer: collections.deque[bytes] = collections.deque()

    def add_channel(
        self, channel: int, read_queue: queue.Queue[PamqpFrame]
    ) -> None:
        """Associate a channel with a queue for frame dispatching."""
        with self._lock:
            self._channels[int(channel)] = read_queue

    def close(self) -> None:
        """Close the socket and update event states.

        Called from within the IO thread's finally block after the event loop
        exits. Do not call this from outside — use stop() instead.

        """
        with self._lock:
            self._events.clear(rabbitpy.events.SOCKET_OPENED)
            self._close_socket()
            self._closed.set()
            self._events.set(rabbitpy.events.SOCKET_CLOSED)

    def stop(self) -> None:
        """Signal the I/O loop to stop from outside the IO thread.

        Thread-safe: schedules the close event via the running event loop, or
        sets it directly if the loop is not yet running.  Also sets
        _stop_requested so that a stop() call received during startup (before
        _connect() runs) is not silently discarded when _connect() clears
        _closed.

        """
        self._stop_requested.set()
        loop = self._ioloop
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(self._closed.set)
        else:
            self._closed.set()

    def run(self) -> None:
        """Run the I/O loop."""
        try:
            asyncio.run(self._run())
        except asyncio.CancelledError:
            LOGGER.debug('IO thread cancelled')
        except OSError as error:
            self._on_error(error)
        finally:
            self.close()

    def write_frame(
        self, channel: int, frame_value: pamqp.frame.FrameTypes
    ) -> None:
        """Write an AMQP frame to the socket."""
        if not self.is_connected:
            self._add_exception(
                rabbitpy.exceptions.ConnectionException(
                    self._host, self._port, 'Not connected'
                )
            )
            return
        with self._lock:
            payload = pamqp.frame.marshal(frame_value, channel)
            self._write_buffer.append(payload)

    @property
    def bytes_received(self) -> int:
        """Return the number of bytes received."""
        with self._lock:
            return self._bytes_read

    @property
    def bytes_written(self) -> int:
        """Return the number of bytes written."""
        with self._lock:
            return self._bytes_written

    @property
    def is_connected(self) -> bool:
        """Indicate socket connection status."""
        with self._lock:
            return not self._closed.is_set()

    @property
    def remote_name(self) -> str | None:
        """Return the remote socket name."""
        with self._lock:
            return self._remote_name

    # Internal Methods

    def _add_exception(self, exc: Exception) -> None:
        """Add an exception to the exception queue and signal the event."""
        LOGGER.debug('Adding exception %r', exc)
        self._exceptions.put(exc)
        self._events.set(rabbitpy.events.EXCEPTION_RAISED)

    def _close_socket(self) -> None:
        """Close the underlying socket.

        Must be called with self._lock held. Reader/writer removal from the
        event loop is handled inside _run() while the loop is still active.

        """
        if self._socket is not None:
            try:
                self._socket.shutdown(socket.SHUT_RDWR)
                self._socket.close()
            except (AttributeError, OSError):
                pass
            self._socket = None

    async def _connect(self) -> None:
        """Establish a connection to the RabbitMQ server."""
        sock: socket.socket | None = None
        for addr_info in self._get_addr_info():
            LOGGER.debug('Attempting to connect to %r', addr_info[4])
            try:
                sock = self._create_socket(*addr_info[0:3])
                sock.connect(addr_info[4])
            except OSError as error:
                if sock is not None:
                    sock.close()
                sock = None
                LOGGER.debug('Error connecting to %r: %s', addr_info[4], error)
                continue
            else:
                break

        if sock is None:
            self._add_exception(
                rabbitpy.exceptions.ConnectionException(
                    self._host, self._port, 'Could not connect'
                )
            )
            return

        if self._stop_requested.is_set():
            sock.close()
            return  # A stop() arrived during startup; honour it
        self._closed.clear()
        self._socket = sock
        self._socket.setblocking(False)
        self._socket.settimeout(self._timeout)
        self._events.set(rabbitpy.events.SOCKET_OPENED)

    def _create_socket(
        self, address_family: int, sock_type: int, protocol: int
    ) -> socket.socket | ssl.SSLSocket:
        """Create a new socket, optionally wrapped with SSL."""
        sock = socket.socket(address_family, sock_type, protocol)
        context = self._get_ssl_context()
        if context:
            return context.wrap_socket(sock=sock)
        return sock

    def _get_addr_info(self) -> AddrInfo:
        """Resolve hostname and port to address information."""
        family = socket.AF_UNSPEC if socket.has_ipv6 else socket.AF_INET
        try:
            res = socket.getaddrinfo(
                self._host, self._port, family, socket.SOCK_STREAM, 0
            )
        except OSError as error:
            LOGGER.debug('Could not resolve %s: %s', self._host, error)
            return []
        return res

    def _get_ssl_context(self) -> ssl.SSLContext | None:
        """Create and configure an SSL context if SSL is enabled."""
        if self._use_ssl:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            if self._ssl_options.get('check_hostname') is not None:
                ssl_context.check_hostname = self._ssl_options[
                    'check_hostname'
                ]
            if self._ssl_options.get('cafile') or self._ssl_options.get(
                'capath'
            ):
                ssl_context.load_verify_locations(
                    cafile=self._ssl_options.get('cafile'),
                    capath=self._ssl_options.get('capath'),
                )
            else:
                ssl_context.load_default_certs(ssl.Purpose.SERVER_AUTH)
            certfile = self._ssl_options.get('certfile')
            if certfile:
                ssl_context.load_cert_chain(
                    certfile, self._ssl_options.get('keyfile')
                )
            verify = self._ssl_options.get('verify')
            if verify is not None:
                ssl_context.verify_mode = verify
            return ssl_context
        return None

    @staticmethod
    def _on_data_received(
        value: bytes,
    ) -> tuple[bytes, int | None, PamqpFrame, int]:
        """Unmarshal a pamqp frame from received socket data.

        :param value: The bytes received from the socket.
        :return: Tuple containing remaining bytes, channel ID,
                frame, and bytes consumed.

        """
        if not value:
            return value, None, None, 0
        try:
            byte_count, channel_id, frame_in = pamqp.frame.unmarshal(value)
        except (pamqp.exceptions.UnmarshalingException, ValueError):
            return value, None, None, 0
        return value[byte_count:], channel_id, frame_in, byte_count

    def _on_error(self, exception: OSError) -> None:
        """Handle socket errors, add exceptions, and signal events."""
        if self._events.is_set(rabbitpy.events.SOCKET_CLOSED):
            return
        self._add_exception(
            rabbitpy.exceptions.ConnectionException(
                self._host, self._port, str(exception)
            )
        )

    def _on_read_ready(self) -> None:
        """Read data from the socket and process any complete frames."""
        with self._lock:
            if self._socket is None:
                return
            try:
                data = self._socket.recv(32768)
            except TimeoutError:
                return  # Transient; wait for the next readable event
            except OSError as error:
                self._on_error(error)
                self._closed.set()
                return
            if not data:
                # Remote end closed the connection cleanly
                self._add_exception(
                    rabbitpy.exceptions.ConnectionResetException()
                )
                self._closed.set()
                return
            self._buffer += data
            while self._buffer:
                remaining, channel_id, frame_value, bytes_read = (
                    self._on_data_received(self._buffer)
                )
                if remaining == self._buffer:
                    break  # Incomplete frame — wait for more data
                self._buffer = remaining
                self._bytes_read += bytes_read
                if channel_id is not None:
                    self._channels[channel_id].put(frame_value)

    def _on_write_ready(self) -> None:
        """Write pending data from the write buffer to the socket."""
        try:
            with self._lock:
                payload = self._write_buffer.popleft()
        except IndexError:
            return
        if self._socket is None:
            return
        try:
            self._socket.sendall(payload)
        except TimeoutError:
            LOGGER.warning(
                'Timed out writing %i bytes to socket', len(payload)
            )
            with self._lock:
                self._write_buffer.appendleft(payload)
        except OSError as error:
            with self._lock:
                self._write_buffer.appendleft(payload)
            if error.errno == 35:
                LOGGER.debug('socket resource temp unavailable')
            else:
                self._on_error(error)
        else:
            with self._lock:
                self._bytes_written += len(payload)

    async def _run(self) -> None:
        """Manage the I/O loop: connect, read, write, and handle closure."""
        loop = asyncio.get_running_loop()
        self._ioloop = loop
        with self._lock:
            await self._connect()

        if not self._socket:
            return  # Could not establish connection

        loop.add_reader(self._socket, self._on_read_ready)
        loop.add_writer(self._socket, self._on_write_ready)
        local_sock = self._socket.getsockname()
        peer_sock = self._socket.getpeername()
        self._remote_name = (
            f'{local_sock[0]}:{local_sock[1]} -> {peer_sock[0]}:{peer_sock[1]}'
        )

        await self._closed.wait()

        # Remove readers/writers from inside the event loop (thread-safe)
        with self._lock:
            if self._socket is not None:
                try:
                    loop.remove_reader(self._socket.fileno())
                    loop.remove_writer(self._socket.fileno())
                except (AttributeError, OSError):
                    pass
