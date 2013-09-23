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

    CONNECTION_TIMEOUT = 0.005
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
        self._channels[int(channel)] = channel, write_queue

    def run(self):
        try:
            self._run()
        except Exception as exception:
            self._exceptions.put(exception)
            if self._events.is_set(events.CHANNEL0_OPENED):
                self._events.set(events.CHANNEL0_CLOSE)
                self._events.wait(events.CHANNEL0_CLOSED)
            if self.open:
                self._close()
            self._events.set(events.EXCEPTION_RAISED)

    def _run(self):
        """Start the thread, which will connect to the socket and run the
        event loop.

        """
        self._connect()

        # Create the remote name
        address, port = self._socket.getsockname()
        peer_address, peer_port = self._socket.getpeername()
        self._remote_name = '%s:%s -> %s:%s' % (address, port,
                                                peer_address, peer_port)

        while self.open and not self._events.is_set(events.SOCKET_CLOSED):

            # Read and process data
            value = self._read_frame()
            if value[0] is not None:

                # When RabbitMQ is closing a channel, remotely close it
                if isinstance(value[1], specification.Channel.Close):
                    self._channels[value[0]][0]._remote_close(value[1])
                else:
                    # Add the frame to the channel queue
                    self._channels[value[0]][1].put(value[1])

            if self._events.is_set(events.SOCKET_CLOSE):
                return self._close()

        # Run the close method if the socket closes unexpectedly
        self._close()

    def _add_frame_to_queue(self, channel_id, frame_value):
        """Add the frame to the stack by creating the key value used in
        expectations and then add it to the list.

        :param int channel_id: The channel id the frame was received on
        :param pamqp.specification.Frame frame_value: The frame to add

        """
        LOGGER.debug('Adding frame to channel %i: %s',
                     channel_id, type(frame_value))
        self._channels[channel_id].put(frame_value)

    def _close(self):
        self._set_state(self.CLOSING)
        self._socket.close()
        self._events.clear(events.SOCKET_OPENED)
        self._events.set(events.SOCKET_CLOSED)
        self._set_state(self.CLOSED)

    def _connect_socket(self):
        """Connect the socket to the specified host and port."""
        LOGGER.debug('Connecting to %(host)s:%(port)i', self._args)
        self._socket.connect((self._args['host'], self._args['port']))

    def _connect(self):
        """Connect to the RabbitMQ Server

        :raises: ConnectionException

        """
        self._set_state(self.OPENING)
        self._socket = self._create_socket()
        self._connect_socket()
        self._events.set(events.SOCKET_OPENED)
        self._set_state(self.OPEN)
        self._writer = self._create_writer()
        self._writer.start()

    def _create_socket(self):
        """Create the new socket, optionally with SSL support.

        :rtype: socket.socket or ssl.SSLSocket

        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        sock.setblocking(1)
        sock.settimeout(self.CONNECTION_TIMEOUT)
        if self._args['ssl']:
            return ssl.wrap_socket(sock,
                                   self._args['ssl_key'],
                                   self._args['ssl_cert'],
                                   False,
                                   self._args['ssl_validation'],
                                   self._args['ssl_version'],
                                   self._args['ssl_cacert'])
        return sock

    def _create_writer(self):
        return IOWriter(kwargs={'events': self._events,
                                'exceptions': self._exceptions,
                                'socket': self._socket,
                                'ssl': self._args['ssl'],
                                'write_queue': self._write_queue})

    def _disconnect_socket(self):
        """Close the existing socket connection"""
        self._socket.close()

    def _get_frame_from_str(self, value):
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

    def _socket_error(self, exception):
        """Common functions when a socket error occurs. Make sure to set closed
        and add the exception, and note an exception event.

        :param socket.error exception: The socket error

        """
        self._set_state(self.CLOSED)
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
                not self._events.is_set(events.EXCEPTION_RAISED))

    def run(self):
        while self.can_write():
            if not self._events.is_set(events.CONNECTION_BLOCKED):
                try:
                    value = self._write_queue.get(True, 0.1)
                    self._write_frame(*value)
                except queue.Empty:
                    pass
            else:
                warnings.warn('%i frames behind' % self._write_queue.qsize(),
                              exceptions.ConnectionBlockedWarning)
        LOGGER.debug('Exiting')

    def _socket_error(self, exception):
        """Common functions when a socket error occurs. Make sure to set closed
        and add the exception, and note an exception event.

        :param socket.error exception: The socket error

        """
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