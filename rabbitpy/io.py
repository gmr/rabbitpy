"""
Core IO for rabbitpy

"""
import logging
try:
    import queue
except ImportError:
    import Queue as queue
import socket
import ssl
import threading
import warnings

LOGGER = logging.getLogger(__name__)

from pamqp import frame
from pamqp import exceptions as pamqp_exceptions
from pamqp import specification
from pamqp import PYTHON3

from rabbitpy import base
from rabbitpy import events
from rabbitpy import exceptions


class IO(threading.Thread, base.StatefulObject):

    CONNECTION_TIMEOUT = 3
    CONTENT_METHODS = ['Basic.Deliver', 'Basic.GetOk', 'Basic.Return']
    READ_BUFFER_SIZE = specification.FRAME_MAX_SIZE

    def __init__(self, group=None, target=None, name=None, args=(),
                 kwargs=None):
        if kwargs is None:
            kwargs = dict()
        super(IO, self).__init__(group, target, name, args, kwargs)

        self._args = kwargs['connection_args']
        self._events = kwargs['events']
        self._exceptions = kwargs['exceptions']
        self._write_queue = kwargs['write_queue']

        self._buffer = bytes()
        self._channels = dict()
        self._remote_name = None
        self._socket = None
        self._state = None
        self._writer = None

    def add_channel(self, channel, write_queue):
        """Add a channel to the channel queue dict for dispatching frames
        to the channel.

        :param rabbitpy.channel.Channel: The channel to add
        :param Queue.Queue write_queue: Queue for sending frames to the channel

        """
        LOGGER.debug('Adding channel')
        with threading.Lock():
            self._channels[int(channel)] = channel, write_queue

    def run(self):
        try:
            self._run()
        except Exception as exception:
            LOGGER.exception('Exception raised: %s', exception)
            self._exceptions.put(exception)
            if self._events.is_set(events.CHANNEL0_OPENED):
                self._events.set(events.CHANNEL0_CLOSE)
                self._events.wait(events.CHANNEL0_CLOSED)
            if self.open:
                self._close()
            self._events.set(events.EXCEPTION_RAISED)

        del self._writer
        del self._socket
        del self._channels
        del self._buffer
        LOGGER.debug('Exiting IO')

    def _run(self):
        """Start the thread, which will connect to the socket and run the
        event loop.

        """
        self._connect()
        LOGGER.debug('Socket connected')

        # Create the remote name
        address, port = self._socket.getsockname()
        peer_address, peer_port = self._socket.getpeername()
        self._remote_name = '%s:%s -> %s:%s' % (address, port,
                                                peer_address, peer_port)

        LOGGER.debug('In main loop')
        while self.open and not self._events.is_set(events.SOCKET_CLOSED):

            if self._events.is_set(events.SOCKET_CLOSE):
                return self._close()

            # Read and process data
            value = self._read_frame()
            if value[0] is not None:

                # Close the channel if it's a Channel.Close frame
                if isinstance(value[1], specification.Channel.Close):
                    self._remote_close_channel(value[0], value[1])

                # Let the channel know a message was returned
                elif isinstance(value[1], specification.Basic.Return):
                    self._notify_of_basic_return(value[0], value[1])

                # Add the frame to the channel queue
                else:
                    self._add_frame_to_queue(value[0], value[1])

        # Run the close method if the socket closes unexpectedly
        if not self._events.is_set(events.SOCKET_CLOSE):
            LOGGER.debug('Running IO._close')
            self._close()
        LOGGER.debug('Exiting IO._run')

    def _add_frame_to_queue(self, channel_id, frame_value):
        """Add the frame to the stack by creating the key value used in
        expectations and then add it to the list.

        :param int channel_id: The channel id the frame was received on
        :param pamqp.specification.Frame frame_value: The frame to add

        """
        LOGGER.debug('Adding frame to channel %i: %s',
                     channel_id, frame_value.name)
        self._channels[channel_id][1].put(frame_value)

    def _close(self):
        self._set_state(self.CLOSING)
        self._socket.close()
        self._events.clear(events.SOCKET_OPENED)
        self._events.set(events.SOCKET_CLOSED)
        self._set_state(self.CLOSED)

    def _connect_socket(self, sock, address):
        """Connect the socket to the specified host and port."""
        LOGGER.debug('Connecting to %r', address)
        sock.settimeout(self.CONNECTION_TIMEOUT)
        sock.connect(address)

    def _connect(self):
        """Connect to the RabbitMQ Server

        :raises: ConnectionException

        """
        self._set_state(self.OPENING)
        sock = None
        for (af, socktype, proto, canonname, sockaddr) in self._get_addr_info():
            LOGGER.info('Record: %r', (af, socktype, proto, sockaddr))
            try:
                sock = self._create_socket(af, socktype, proto)
                self._connect_socket(sock, sockaddr)
                break
            except socket.error as error:
                LOGGER.debug('Error connecting to %r: %s', sockaddr, error)
                sock = None
                continue

        if not sock:
            raise exceptions.ConnectionException(self._args['host'],
                                                 self._args['port'])

        self._socket = sock
        self._socket.settimeout(0.1)
        self._events.set(events.SOCKET_OPENED)
        self._set_state(self.OPEN)
        self._writer = self._create_writer()
        self._writer.start()

    def _create_socket(self, af, socktype, proto):
        """Create the new socket, optionally with SSL support.

        :rtype: socket.socket or ssl.SSLSocket

        """
        #LOGGER.debug('Connection timeout: %s', self.CONNECTION_TIMEOUT)
        sock = socket.socket(af, socktype, proto)
        if self._args['ssl']:
            LOGGER.debug('Wrapping socket for SSL')
            return ssl.wrap_socket(sock,
                                   self._args['ssl_key'],
                                   self._args['ssl_cert'],
                                   False,
                                   self._args['ssl_validation'],
                                   self._args['ssl_version'],
                                   self._args['ssl_cacert'])
        return sock

    def _create_writer(self):
        """Create the writing thread"""
        return IOWriter(kwargs={'events': self._events,
                                'exceptions': self._exceptions,
                                'socket': self._socket,
                                'ssl': self._args['ssl'],
                                'write_queue': self._write_queue})

    def _disconnect_socket(self):
        """Close the existing socket connection"""
        self._socket.close()

    def _get_addr_info(self):
        family = socket.AF_UNSPEC
        if not socket.has_ipv6:
            family = socket.AF_INET
        try:
            res = socket.getaddrinfo(self._args['host'],
                                     self._args['port'],
                                     family,
                                     socket.SOCK_STREAM,
                                     0)
        except socket.error as error:
            LOGGER.exception('Could not resolve %s: %s',
                             self._args['host'], error)
            return []
        return res

    @staticmethod
    def _get_frame_from_str(value):
        """Get the pamqp frame from the string value.

        :param str value: The value to parse for an pamqp frame
        :return (str, int, pamqp.specification.Frame): Remainder of value,
                                                       channel id and
                                                       frame value

        """
        if not value:
            return value, None, None
        try:
            byte_count, channel_id, frame_in = frame.unmarshal(value)
        except (pamqp_exceptions.UnmarshalingException,
                specification.AMQPFrameError) as error:
            LOGGER.debug('Failed to demarshal: %r', error)
            return value, None, None
        return value[byte_count:], channel_id, frame_in

    def _notify_of_basic_return(self, channel_id, frame_value):
        """Invoke the on_basic_return code in the specified channel. This will
        block the IO loop unless the exception is caught.

        :param int channel_id: The channel for the basic return
        :param pamqp.specification.Frame frame_value: The Basic.Return frame

        """
        with threading.Lock():
            self._channels[channel_id][0].on_basic_return(frame_value)

    def _read_frame(self):
        """Read in a full frame and return it, trying to read it from the
        buffer before reading it from the socket.

        :return tuple: The channel the frame came in on and the frame

        """
        channel_id, frame_value = self._read_frame_from_buffer()
        if frame_value:
            return channel_id, frame_value
        return self._read_frame_from_socket()

    def _read_frame_from_buffer(self):
        """Read from the buffer and try and get the demarshaled frame.

        :rtype (int, pamqp.specification.Frame): The channel and frame

        """
        self._buffer, chan_id, value = self._get_frame_from_str(self._buffer)
        return chan_id, value

    def _read_frame_from_socket(self):
        """Read from the socket, appending to the buffer, then try and get the
        frame from the buffer.

        :rtype (int, pamqp.specification.Frame): The channel and frame

        """
        self._buffer += self._read_socket()
        if not self._buffer:
            return None, None
        return self._read_frame_from_buffer()

    def _read_socket(self):
        """Read the negotiated maximum frame size from the socket. Default
        value for self.maximum_frame_size = pamqp.specification.FRAME_MAX_SIZE

        :rtype: str

        """
        try:
            value = self._socket.recv(self.READ_BUFFER_SIZE)
            return value
        except socket.timeout:
            return bytes() if PYTHON3 else str()
        except socket.error as exception:
            LOGGER.exception('Socket error in reading')
            self._socket_error(exception)

    def _remote_close_channel(self, channel_id, frame_value):
        """Invoke the on_channel_close code in the specified channel. This will
        block the IO loop unless the exception is caught.

        :param int channel_id: The channel to remote close
        :param pamqp.specification.Frame frame_value: The Channel.Close frame

        """
        with threading.Lock():
            self._channels[channel_id][0].on_remote_close(frame_value)

    def _socket_error(self, exception):
        """Common functions when a socket error occurs. Make sure to set closed
        and add the exception, and note an exception event.

        :param socket.error exception: The socket error

        """
        self._set_state(self.CLOSED)
        LOGGER.exception('Socket error: %s', exception)
        self._exceptions.put(exception)
        self._events.clear(events.SOCKET_OPENED)
        self._events.set(events.SOCKET_CLOSED)
        self._events.set(events.EXCEPTION_RAISED)


class IOWriter(threading.Thread):
    def __init__(self, group=None, target=None, name=None, args=(),
                 kwargs=None):
        if kwargs is None:
            kwargs = dict()
        super(IOWriter, self).__init__(group, target, name, args, kwargs)
        self._write_queue = kwargs['write_queue']
        self._exceptions = kwargs['exceptions']
        self._events = kwargs['events']
        self._socket = kwargs['socket']
        self._ssl = kwargs['ssl']

    def can_write(self):
        return (self._events.is_set(events.SOCKET_OPENED) and
                not self._events.is_set(events.EXCEPTION_RAISED) and
                not self._events.is_set(events.SOCKET_CLOSE))

    def run(self):
        while self.can_write():
            if self._events.is_set(events.SOCKET_CLOSE):
                break

            if not self._events.is_set(events.CONNECTION_BLOCKED):
                try:
                    value = self._write_queue.get(True, 0.1)
                    self._write_frame(*value)
                except queue.Empty:
                    pass
            else:
                warnings.warn('%i frames behind' % self._write_queue.qsize(),
                              exceptions.ConnectionBlockedWarning)
        LOGGER.debug('Exiting IOWriter')

    def _socket_error(self, exception):
        """Common functions when a socket error occurs. Make sure to set closed
        and add the exception, and note an exception event.

        :param socket.error exception: The socket error

        """
        LOGGER.exception('Socket error: %s', exception)
        self._exceptions.put(exception)
        self._events.clear(events.SOCKET_OPENED)
        self._events.set(events.SOCKET_CLOSED)
        self._events.set(events.EXCEPTION_RAISED)

    def _write_frame(self, channel_id, frame_value):
        """Marshal the frame and write it to the socket.
        :param frame_value: The frame to write
        :type frame_value: pamqp.specification.Frame|pamqp.header.ProtocolHeader
        :param int channel_id: The channel id to send
        :rtype: int

        """
        frame_data = frame.marshal(frame_value, channel_id)
        bytes_sent = 0
        while bytes_sent < len(frame_data) and self.can_write():
            try:
                bytes_sent += self._write_frame_data(frame_data[bytes_sent:])
            except socket.timeout:
                pass
            except socket.error as exception:
                LOGGER.exception('Socket error in writing')
                self._socket_error(exception)
                bytes_sent = 0
        return bytes_sent

    def _write_frame_data(self, frame_data):
        """Write the frame data to the socket

        :param str frame_data: The frame data to write
        :return int: bytes written

        """
        if self._ssl:
            return self._socket.write(frame_data)
        return self._socket.send(frame_data)