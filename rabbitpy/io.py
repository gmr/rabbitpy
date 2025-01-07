import asyncio
import collections
import logging
import queue
import socket
import ssl
import threading
import typing

import pamqp.exceptions
from pamqp import base, frame

import rabbitpy.events
import rabbitpy.exceptions

LOGGER = logging.getLogger(__name__)
MAX_BUFFER_PROCESSING_RETRIES = 5

AddrInfo = typing.Union[
    list[typing.Any],
    list[tuple[socket.AddressFamily, socket.SocketKind, int, str,
         tuple[str, int], tuple[str, int, int, int]]]
]


class IO(threading.Thread):

    def __init__(self,
                 host: str,
                 port: int,
                 use_ssl: bool,
                 ssl_options: dict,
                 events: rabbitpy.events.Events,
                 exceptions: queue.Queue,
                 timeout: float = 0.01):
        super().__init__(group=None,
                         target=None,
                         name='IO',
                         args=(),
                         kwargs={},
                         daemon=True)
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
        self._lock = threading.Lock()
        self._bytes_read = 0
        self._bytes_written = 0
        self._channels: collections.defaultdict[int, queue.Queue] = (
            collections.defaultdict(queue.Queue))
        self._ioloop: typing.Optional[asyncio.AbstractEventLoop] = None
        self._remote_name: typing.Optional[str] = None
        self._socket: typing.Optional[socket.socket] = None
        self._write_buffer = collections.deque()
        self._write_queue: queue.Queue = queue.Queue()

    def add_channel(self, channel: int, read_queue: queue.Queue) -> None:
        """Add a channel to the channel queue dict for dispatching frames
        to the channel.

        :param channel: The channel to add
        :param read_queue: Queue for sending frames to the channel

        """
        with self._lock:
            self._channels[int(channel)] = read_queue

    def close(self) -> None:
        """Close the socket and set the proper event states"""
        with self._lock:
            self._events.clear(rabbitpy.events.SOCKET_OPENED)
            self._close_socket()
            self._closed.set()
            self._events.set(rabbitpy.events.SOCKET_CLOSED)

    def run(self) -> None:
        """Run the IO thread, invoked by calling IO.start()"""
        try:
            asyncio.run(self._run())
        except asyncio.CancelledError:
            LOGGER.debug('IO thread cancelled')
            self.close()
        except OSError as error:
            self._on_error(error)
            self.close()

    def write_frame(self, channel: int, frame_value: base.Frame) -> None:
        """Write an AMQP frame to the socket."""
        if not self.is_connected:
            return self._add_exception(
                rabbitpy.exceptions.ConnectionException(
                    self._host, self._port, 'Not connected'))
        with self._lock:
            payload = frame.marshal(frame_value, channel)
            self._write_buffer.append(payload)

    @property
    def bytes_received(self) -> int:
        """Return the number of bytes read/received from RabbitMQ"""
        with self._lock:
            return self._bytes_read

    @property
    def bytes_written(self) -> int:
        """Return the number of bytes written to RabbitMQ"""
        with self._lock:
            return self._bytes_written

    @property
    def is_connected(self):
        """Returns True if the socket is connected"""
        with self._lock:
            return not self._closed.is_set()

    @property
    def remote_name(self) -> str:
        """Return the remote name of the socket"""
        with self._lock:
            return self._remote_name

    # Internal Methods

    def _add_exception(self, exc: Exception) -> None:
        """Add an exception to the exception queue"""
        LOGGER.debug('Adding exception %r', exc)
        self._exceptions.put(exc)
        self._events.set(rabbitpy.events.EXCEPTION_RAISED)

    def _close_socket(self) -> None:
        """Close the socket, removing the FDs from the IOLoop"""
        try:
            self._ioloop.remove_reader(self._socket)
            self._ioloop.remove_writer(self._socket)
        except (AttributeError, IndexError):
            pass
        try:
            self._socket.shutdown(socket.SHUT_RDWR)
            self._socket.close()
        except (AttributeError, OSError):
            pass

    async def _connect(self) -> None:
        """Connect to the RabbitMQ Server

        :raises: ConnectionException

        """
        sock = None
        for (addr_family, sock_type, protocol, _cname, sock_addr) \
                in self._get_addr_info():
            LOGGER.debug('Attempting to connect to %r', sock_addr)
            try:
                sock = self._create_socket(addr_family, sock_type, protocol)
                sock.connect(sock_addr)
            except OSError as error:
                sock.close()
                sock = None
                LOGGER.debug('Error connecting to %r: %s', sock_addr, error)
                continue
            else:
                break

        if not sock:
            return self._add_exception(
                rabbitpy.exceptions.ConnectionException(
                    self._host, self._port, 'Could not connect'))

        self._closed.clear()
        self._socket = sock
        self._socket.setblocking(False)
        self._socket.settimeout(self._timeout)
        self._events.set(rabbitpy.events.SOCKET_OPENED)

    def _create_socket(self,
                       address_family: int,
                       sock_type: int,
                       protocol: int) \
            -> typing.Union[socket.socket, ssl.SSLSocket]:
        """Create the new socket, optionally with SSL support.

        :param address_family: The address family to use when creating
        :param sock_type: The type of socket to create

        """
        sock = socket.socket(address_family, sock_type, protocol)
        context = self._get_ssl_context()
        if context:
            return context.wrap_socket(sock=sock)
        return sock

    def _get_addr_info(self) -> AddrInfo:
        family = socket.AF_UNSPEC if socket.has_ipv6 else socket.AF_INET
        try:
            res = socket.getaddrinfo(
                self._host, self._port, family, socket.SOCK_STREAM, 0)
        except OSError as error:
            LOGGER.debug('Could not resolve %s: %s', self._host, error)
            return []
        return res

    def _get_ssl_context(self) -> typing.Optional[ssl.SSLContext]:
        """Return the configured SSLContext to use if needed"""
        if self._use_ssl:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            if self._ssl_options.get('check_hostname') is not None:
                ssl_context.check_hostname = \
                    self._ssl_options['check_hostname']
            if self._ssl_options.get('cafile') \
                    or self._ssl_options.get('capath'):
                ssl_context.load_verify_locations(
                    cafile=self._ssl_options.get('cafile'),
                    capath=self._ssl_options.get('capath'))
            else:
                ssl_context.load_default_certs(ssl.Purpose.SERVER_AUTH)
            if self._ssl_options.get('certfile'):
                ssl_context.load_cert_chain(
                    self._ssl_options.get('certfile'),
                    self._ssl_options.get('keyfile'))
            if self._ssl_options.get('verify') is not None:
                ssl_context.verify_mode = self._ssl_options['verify']
            return ssl_context
        return None

    @staticmethod
    def _on_data_received(value: bytes) \
            -> tuple[bytes,
                     typing.Optional[int],
                     typing.Optional[base.Frame],
                     int]:
        """Get the pamqp frame from the value read from the socket.

        :param value: The value to parse for a pamqp frame
        :return: Remainder of value, channel id, frame value, and bytes read

        """
        if not value:
            return value, None, None, 0
        try:
            byte_count, channel_id, frame_in = frame.unmarshal(value)
        except (pamqp.exceptions.UnmarshalingException, ValueError):
            return value, None, None, 0
        return value[byte_count:], channel_id, frame_in, byte_count

    def _on_error(self, exception: OSError) -> None:
        """Common functions when a socket error occurs. Make sure to set closed
        and add the exception, and note an exception event.

        :param exception: The socket error

        """
        if self._events.is_set(rabbitpy.events.SOCKET_CLOSED):
            return
        args = [self._host, self._port, str(exception)]
        """
        if self._channels[0][0].open:
            self._exceptions.put(
                rabbitpy_exceptions.ConnectionResetException(*args))
        else:
        """
        self._add_exception(rabbitpy.exceptions.ConnectionException(*args))

    def _on_read_ready(self) -> None:
        """Append the data that is read to the buffer and try and parse
        frames out of it.

        """
        with self._lock:
            self._buffer += self._socket.recv(32768)
            while self._buffer:
                remaining, channel_id, frame_value, bytes_read =\
                    self._on_data_received(self._buffer)
                if remaining == self._buffer:
                    break
                self._buffer = remaining
                self._bytes_read += bytes_read
                self._channels[channel_id].put(frame_value)

    def _on_write_ready(self) -> None:
        """Write the next frame to the socket"""
        with self._lock:
            try:
                payload = self._write_buffer.popleft()
            except IndexError:
                return
            try:
                self._socket.sendall(payload)
            except socket.timeout:
                LOGGER.warning('Timed out writing %i bytes to socket',
                               len(payload))
                self._write_buffer.appendleft(payload)
            except OSError as error:
                self._write_buffer.appendleft(payload)
                if error.errno == 35:
                    LOGGER.debug('socket resource temp unavailable')
                else:
                    self._on_error(error)
            else:
                self._bytes_written += len(payload)

    async def _run(self):
        """Core logic that connects and blocks until the socket is closed"""
        self._ioloop = asyncio.get_running_loop()

        with self._lock:
            await self._connect()

        if not self._socket:
            return

        self._ioloop.add_reader(self._socket, self._on_read_ready)
        self._ioloop.add_writer(self._socket, self._on_write_ready)
        local_sock = self._socket.getsockname()
        peer_sock = self._socket.getpeername()
        self._remote_name = \
            f'{local_sock[0]}:{local_sock[1]} -> {peer_sock[0]}:{peer_sock[1]}'

        await self._closed.wait()
