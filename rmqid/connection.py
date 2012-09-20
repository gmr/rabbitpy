"""
The Connection class negotiates and manages the connection state.

"""
import collections
import locale
import logging
import sys
import socket
try:
    import ssl
except ImportError:
    ssl = None

from pamqp import frame
from pamqp import header
from pamqp import specification

from rmqid import base
from rmqid import __version__

LOGGER = logging.getLogger(__name__)


class Connection(base.StatefulObject):
    """The Connection object is responsible for negotiating a connection and
    managing its state.

    """
    CHANNEL = 0
    CONNECTION_TIMEOUT = 3

    def __init__(self, host, port, virtual_host, username, password,
                 use_ssl=False):
        """Create a new instance of the Connection object

        :param str host: The host to connect to
        :param int port: The port to connect on
        :param str virtual_host: The virtual host to connect to
        :param str username: The username to use
        :param str password: The password to use
        :param bool use_ssl: Connect via SSL

        """
        super(Connection, self).__init__()
        self._socket = None

        if use_ssl and not ssl:
            LOGGER.warning('SSL requested but not available, disabling')
            use_ssl = False

        self._args = {'host': host, 'port': port,
                      'virtual_host': virtual_host,
                      'username': username,
                      'password': password,
                      'ssl': use_ssl}
        self._buffer = str()
        self._frame_buffer = list()
        self._connect()
        self._properties = dict()

    def _build_start_ok_frame(self):
        version = sys.version_info
        properties = {'product': 'rmqid',
                      'platform': 'Python %s.%s.%s' % (version.major,
                                                       version.minor,
                                                       version.micro),
                      'capabilities': {'basic.nack': True,
                                       'consumer_cancel_notify': True,
                                       'publisher_confirms': True},
                      'information': 'See http://github.com/gmr/rmqid',
                      'version': __version__}
        return specification.Connection.StartOk(client_properties=properties,
                                                response=self._credentials,
                                                locale=self._get_locale())

    def _create_socket(self, use_ssl):
        """Create the new socket, optionally with SSL support.

        :param bool use_ssl: Use SSL to connect?
        :rtype: socket.socket or ssl.SSLSocket

        """
        LOGGER.debug('Creating a new socket')
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        sock.setblocking(1)
        sock.settimeout(self.CONNECTION_TIMEOUT)
        if use_ssl:
            LOGGER.debug('Wrapping socket')
            return ssl.wrap_socket(sock)
        return sock

    @property
    def _credentials(self):
        return '\0%s\0%s' % (self._args['username'], self._args['password'])

    def _connect_socket(self, host, port):
        """Connect the socket to the specified host and port.

        :param str host: The host to connect to
        :param int port: The port to connect on

        """
        LOGGER.debug('Connecting to %s:%i', host, port)
        self._socket.connect((host, port))

    def _connect(self):
        """Connect to the RabbitMQ Server

        :raises: ConnectionException

        """
        self._set_state(self.OPENING)
        self._socket = self._create_socket(self._args['ssl'])
        self._connect_socket(self._args['host'], self._args['port'])

        # AMQP Connection negotiation
        self._write_protocol_header()

        if not self._start_connection():
            return

        if not self._tune_connection():
            return

    def _disconnect_socket(self):
        """Close the existing socket connection"""
        self._socket.close()


    def _get_frame_from_buffer(self, expectation, channel=CHANNEL):

        for offset in range(0, len(self._frame_buffer)):
            if (channel == self._frame_buffer[offset][0] and
                isinstance(self._frame_buffer[offset][1], expectation)):
                LOGGER.debug('Found a buffered %s frame', expectation.name)
                return self._frame_buffer.pop(offset)

    def _get_locale(self):
        return locale.getlocale()[0] or 'en_US'

    def _read_frame(self):

        frame_in = None
        while not frame_in:
            try:
                self._buffer += self._read_socket()
            except socket.timeout:
                continue
            LOGGER.debug('Buffer: %r', self._buffer)
            bytes_read, channel, frame_in = frame.demarshal(self._buffer)
            LOGGER.debug('Read %i bytes returning %r', bytes_read, frame_in)
            if bytes_read < len(self._buffer):
                self._buffer = self._buffer[bytes_read + 1:]
            if frame_in:
                return channel, frame_in

    def _read_socket(self):
        return self._socket.recv(specification.FRAME_MAX_SIZE)

    def _start_connection(self):

        # Wait for a Connection.Start
        LOGGER.debug('Waiting for a Connection.Start')
        frame_value = self._wait_on_frame(specification.Connection.Start)

        if not self._validate_connection_start(frame_value):
            LOGGER.error('Could not negotiate a connection, disconnecting')
            self._disconnect_socket()
            return False

        self._properties = frame_value.server_properties
        self._write_frame(self._build_start_ok_frame())
        return True

    def _tune_connection(self):
        # Wait for a Connection.Start
        LOGGER.debug('Waiting for a Connection.Tune')
        frame_value = self._wait_on_frame(specification.Connection.Tune)

    def _validate_connection_start(self, frame_value):
        """Validate the received Connection.Start frame

        :param specification.Connection.Start frame_value: The frame to validate
        :rtype: bool

        """
        LOGGER.debug('Validating %r', frame_value)
        if (frame_value.version_major,
            frame_value.version_minor) != (specification.VERSION[0],
                                           specification.VERSION[1]):
            LOGGER.warning('AMQP version mismatch, received %i.%i, expected %r',
                           frame_value.version_major, frame_value.version_minor,
                           specification.VERSION)
            return False
        return True

    def _wait_on_frame(self, expectation, channel=CHANNEL):
        """Wait for a frame to come in from the broker.

        :param class expectation: The class expected
        :param int channel: The channel number to wait for the frame on
        :rtype: pamqp.specification.Frame

        """
        # First make sure the frame is not already received
        frame_value = self._get_frame_from_buffer(expectation, channel)
        if frame_value:
            LOGGER.debug('Returning %r frame from buffer', frame_value)
            return frame_value

        # Loop until the frame is received
        while not self.closed:

            # Read a frame in
            channel_id, frame_value = self._read_frame()

            # If the frame is the right type, return it
            if isinstance(frame_value, expectation) and channel_id == channel:
                LOGGER.debug('Returning a direct match for %r', frame_value)
                return frame_value

            LOGGER.debug('Appending %r received on channel %i to the buffer',
                         frame_value, channel_id)
            self._frame_buffer.append((channel_id, frame_value))

    def _write_frame(self, frame_value, channel=CHANNEL):
        """Marshal the frame and write it to the socket.

        :param pamqp.specification.Frame or
               pamqp.header.ProtocolHeader frame_value: The frame to write

        """
        LOGGER.debug('Writing %r frame', frame_value)
        frame_data = frame.marshal(frame_value, channel)
        bytes_sent = 0
        while bytes_sent < len(frame_data):
            bytes_sent += self._write_frame_data(frame_data)

    def _write_frame_data(self, frame_data):
        """Write the frame data to the socket

        :param str frame_data: The frame data to write
        :return int: bytes written

        """
        if self.closed:
            LOGGER.error('Can not write to a closed socket')
            return

        LOGGER.debug('Writing %i bytes to the socket', len(frame_data))
        if self._args['ssl']:
            return self._socket.write(frame_data)
        return self._socket.send(frame_data)

    def _write_protocol_header(self):
        """Send the protocol header to the connected server."""
        protocol_header = header.ProtocolHeader()
        self._write_frame(protocol_header, self.CHANNEL)
