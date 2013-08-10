__author__ = 'gmr'
import logging
try:
    import queue
except ImportError:
    import Queue as queue
import socket
import ssl
import threading

LOGGER = logging.getLogger(__name__)

from pamqp import frame
from pamqp import exceptions as pamqp_exceptions
from pamqp import specification
from pamqp import PYTHON3

from rabbitpy import base
from rabbitpy import exceptions


class IO(threading.Thread, base.StatefulObject):

    CONNECTION_TIMEOUT = 0.01
    CONTENT_METHODS = ['Basic.Deliver', 'Basic.GetOk', 'Basic.Return']
    READ_BUFFER_SIZE = specification.FRAME_MAX_SIZE

    def __init__(self, group=None, target=None, name=None, args=(),
                 kwargs=None):
        if kwargs is None:
            kwargs = dict()
        super(IO, self).__init__(group, target, name, args, kwargs)

        self._args = kwargs['connection_args']
        self._on_connected = kwargs['connected']
        self._write_queue = kwargs['write_queue']
        self._close_event = kwargs['close']

        self._buffer = bytes()
        self._channels = dict()
        self._remote_name = None
        self._socket = None
        self._state = None

    def add_channel(self, channel_number, write_queue):
        """Add a channel to the channel queue dict for dispatching frames
        to the channel.

        :param int channel_number: The channel to write
        :param Queue.Queue write_queue: Queue for sending frames to the channel

        """
        self._channels[channel_number] = write_queue

    def run(self):
        """Start the thread, which will connect to the socket and run the
        event loop.

        """
        self._connect()

        # Create the remote name
        address, port = self._socket.getsockname()
        peer_address, peer_port = self._socket.getpeername()
        self._remote_name = '%s:%s -> %s:%s' % (address, port,
                                                peer_address, peer_port)

        while self.open and not self._close_event.is_set():
            try:
                frame_value = self._write_queue.get(False)
                self._write_frame(*frame_value)
            except queue.Empty:
                pass

            # Read and process data
            frame_value = self._read_frame()
            if frame_value[0] is not None:
                self._channels[frame_value[0]].put(frame_value[1])

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
        self._set_state(self.CLOSED)

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
        self._set_state(self.OPEN)
        self._on_connected.set()

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
        except socket.error as error:
            raise exceptions.RemoteClosedException(-1,
                                                   'Socket Error: %s' %
                                                   error)

    def _write_frame(self, channel_id, frame_value):
        """Marshal the frame and write it to the socket.
        :param frame_value: The frame to write
        :type frame_value: pamqp.specification.Frame|pamqp.header.ProtocolHeader
        :param int channel_id: The channel id to send
        :rtype: int

        """
        frame_data = frame.marshal(frame_value, channel_id)
        bytes_sent = 0
        while bytes_sent < len(frame_data):
            bytes_sent += self._write_frame_data(frame_data)
        return bytes_sent

    def _write_frame_data(self, frame_data):
        """Write the frame data to the socket

        :param str frame_data: The frame data to write
        :return int: bytes written

        """
        if self._args['ssl']:
            return self._socket.write(frame_data)
        return self._socket.send(frame_data)

