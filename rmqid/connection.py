"""
The Connection class negotiates and manages the connection state.

"""
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
from rmqid import channel
from rmqid import exceptions
from rmqid import __version__

LOGGER = logging.getLogger(__name__)


class Connection(base.StatefulObject):
    """The Connection object is responsible for negotiating a connection and
    managing its state.

    """
    CHANNEL = 0
    CONNECTION_TIMEOUT = 3
    DEFAULT_HEARTBEAT_INTERVAL = 3
    DEFAULT_LOCALE = 'en_US'

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
        self._channels = dict()
        self._frame_buffer = list()
        self._heartbeat = 0
        self._maximum_channels = 0
        self._maximum_frame_size = specification.FRAME_MAX_SIZE
        self._minimum_frame_size = specification.FRAME_MIN_SIZE
        self._properties = dict()
        self._connect()

    def _build_close_frame(self):
        """Build and return the Connection.Close frame.

        :rtype: pamqp.specification.Connection.Close

        """
        return specification.Connection.Close(200, 'Normal Shutdown')

    def _build_open_frame(self):
        """Build and return the Connection.Open frame.

        :rtype: pamqp.specification.Connection.Open

        """
        return specification.Connection.Open(self._args['virtual_host'])

    def _build_start_ok_frame(self):
        """Build and return the Connection.StartOk frame.

        :rtype: pamqp.specification.Connection.StartOk

        """
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

    def _build_tune_ok_frame(self):
        """Build and return the Connection.TuneOk frame.

        :rtype: pamqp.specification.Connection.TuneOk

        """
        return specification.Connection.TuneOk(self._maximum_channels,
                                               self._maximum_frame_size,
                                               self._heartbeat)

    def _close_channels(self):
        """Close all the channels that are currently open."""
        if not self._channels:
            return
        LOGGER.debug('Closing %i channel%s', len(self._channels.keys()),
                     's' if len(self._channels.keys()) > 1 else '')
        for channel_id in self._channels.keys():
            self._channels[channel_id].close()
            del self._channels[channel_id]

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
        self._write_protocol_header()
        if not self._connection_start():
            return
        self._connection_tune()
        self._connection_open()
        self._set_state(self.OPEN)
        LOGGER.info('Connection to %s:%i:%s open',
                    self._args['host'],
                    self._args['port'],
                    self._args['virtual_host'])

    def _connection_open(self):
        """Negotiate the Connection.Start process, writing out a
        Connection.StartOk frame when the Connection.Start frame is received.

        """
        self.write_frame(self._build_open_frame())
        LOGGER.debug('Waiting for a Connection.OpenOk')
        frame_value = self.wait_on_frame(specification.Connection.OpenOk)
        LOGGER.debug('Connected with known hosts: %r', frame_value.known_hosts)

    def _connection_start(self):
        """Negotiate the Connection.Start process, writing out a
        Connection.StartOk frame when the Connection.Start frame is received.

        :rtype: bool

        """
        LOGGER.debug('Waiting for a Connection.Start')
        frame_value = self.wait_on_frame(specification.Connection.Start)
        if not self._validate_connection_start(frame_value):
            LOGGER.error('Could not negotiate a connection, disconnecting')
            self._disconnect_socket()
            return False
        self._properties = frame_value.server_properties
        self.write_frame(self._build_start_ok_frame())
        return True

    def _connection_tune(self):
        """Negotiate the Connection.Tune frames, waiting for the Connection.Tune
        frame from RabbitMQ and sending the Connection.TuneOk frame.

        """
        # Wait for a Connection.Start
        LOGGER.debug('Waiting for a Connection.Tune')
        frame_value = self.wait_on_frame(specification.Connection.Tune)
        self._maximum_channels = frame_value.channel_max
        if frame_value.frame_max <> self._maximum_frame_size:
            self._maximum_frame_size = frame_value.frame_max
        if frame_value.heartbeat:
            self._heartbeat = frame_value.heartbeat
        self.write_frame(self._build_tune_ok_frame())

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
        """Return the marshalled credentials for the AMQP connection.

        :rtype: str

        """
        return '\0%s\0%s' % (self._args['username'], self._args['password'])

    def _disconnect_socket(self):
        """Close the existing socket connection"""
        self._socket.close()

    def _get_frame_from_buffer(self, expectation, channel_id=CHANNEL):
        """Get the expected frame from the buffer if it exists.

        :param class expectation: The frame class to get
        :param int channel_id: The channel number the frame should be on
        :rtype: pamqp.specification.Frame

        """
        for offset in range(0, len(self._frame_buffer)):
            if (channel_id == self._frame_buffer[offset][0] and
                self._frame_buffer[offset][1].name in expectation):
                LOGGER.debug('Found a buffered %s frame', expectation.name)
                return self._frame_buffer.pop(offset)

    def _get_locale(self):
        """Return the current locale for the python interpreter or the default
        locale.

        :rtype: str

        """
        return locale.getlocale()[0] or self.DEFAULT_LOCALE

    def _is_server_rpc(self, frame_value):

        return frame_value.name in ['Connection.Close', 'Channel.Close']

    def _process_server_rpc(self, channel_id, frame_value):

        if frame_value.name == 'Channel.Close':
            del self._channels[channel_id]
            raise exceptions.ChannelClosedException(channel_id,
                                                    frame_value.reply_code,
                                                    frame_value.reply_text)

        elif frame_value.name == 'Connection.Close':
            self._set_state(self.CLOSED)
            raise exceptions.ConnectionClosedException(frame_value.reply_code,
                                                       frame_value.reply_text)

    def _read_frame(self):
        """Read in a full frame and return it.

        :return tuple: The channel the frame came in on and the frame

        """
        frame_in = None
        while not frame_in:
            try:
                self._buffer += self._read_socket()
            except socket.error as error:
                self._set_state(self.CLOSED)
                raise exceptions.ConnectionClosedException(-1,
                                                           'Socket Error: %s' %
                                                           error)
            LOGGER.debug('Buffer: %r', self._buffer)
            bytes_read, channel_id, frame_in = frame.demarshal(self._buffer)
            LOGGER.debug('Read %i bytes returning %s from channel %i',
                         bytes_read, frame_in.name, channel_id)
            self._buffer = self._buffer[bytes_read + 1:]
            return channel_id, frame_in

    def _read_socket(self):
        """Read the negotiated maximum frame size from the socket. Default
        value for self._maximum_frame_size = pamqp.specification.FRAME_MAX_SIZE

        :rtype: str

        """
        return self._socket.recv(self._maximum_frame_size)

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
        self.write_frame(protocol_header, self.CHANNEL)

    def _get_next_channel_id(self):
        """Return the next channel id

        """
        if not self._channels:
            return 1
        if len(self._channels.keys()) == self._maximum_channels:
            raise exceptions.TooManyChannelsError
        return max(self._channels.keys())

    def close(self):
        """Close the connection, including all open channels

        """
        if not self.closed:
            LOGGER.debug('Closing the connection')
            self._set_state(self.CLOSING)
            self._close_channels()
            self.write_frame(self._build_close_frame())
            self.wait_on_frame(specification.Connection.CloseOk)
            self._set_state(self.CLOSED)

    def channel(self):
        """Create a new channel"""
        LOGGER.debug('Creating a new channel')
        channel_id = self._get_next_channel_id()
        self._channels[channel_id] = channel.Channel(channel_id, self)
        return self._channels[channel_id]

    def enable_heartbeats(self, interval=DEFAULT_HEARTBEAT_INTERVAL):
        """Turn on heartbeat sending, defaulting to the
        Connection.DEFAULT_HEARTBEAT_INTERVAL

        :param int interval: The heartbeat interval to use
        """
        if not self._heartbeat:
            self._heartbeat = interval

    def write_frame(self, frame_value, channel_id=CHANNEL):
        """Marshal the frame and write it to the socket.

        :param pamqp.specification.Frame or
               pamqp.header.ProtocolHeader frame_value: The frame to write
        :param int channel_id: The channel id to send

        """
        LOGGER.debug('Writing %s frame', frame_value.name)
        frame_data = frame.marshal(frame_value, channel_id)
        bytes_sent = 0
        while bytes_sent < len(frame_data):
            bytes_sent += self._write_frame_data(frame_data)

    def _normalize_expectation(self, expectation):
        """Turn a class or list of classes into a list of class names.

        :param class or list expectation: List of classes or class
        :rtype: list

        """
        if isinstance(expectation, list):
            output = list()
            for value in expectation:
                if isinstance(value, str):
                    output.append(value)
                else:
                    output.append(value.name)
            return output
        return [expectation.name]

    def wait_on_frame(self, expectation, channel_id=CHANNEL):
        """Wait for a frame to come in from the broker.

        :param list or class expectation: Class.Method or list of Class.Methods
        :param int channel_id: The channel number to wait for the frame on
        :rtype: pamqp.specification.Frame

        """
        expectations = self._normalize_expectation(expectation)
        LOGGER.debug('Waiting on %r frame(s) on channel %i',
                     expectations, channel_id)
        frame_value = self._get_frame_from_buffer(expectations, channel_id)

        if frame_value:
            LOGGER.debug('Returning %r frame from buffer', frame_value.name)
            return frame_value

        while not self.closed:

            channel_value, frame_value = self._read_frame()

            if self._is_server_rpc(frame_value):
                return self._process_server_rpc(channel_value, frame_value)

            if frame_value.name in expectations and channel_value == channel_id:
                LOGGER.debug('Returning a direct match for %r', frame_value)
                return frame_value

            LOGGER.debug('Appending %r received on channel %i to the buffer',
                         frame_value, channel_value)
            self._frame_buffer.append((channel_value, frame_value))
