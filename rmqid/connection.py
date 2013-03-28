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
from pamqp import exceptions as pamqp_exceptions
from pamqp import specification
from pamqp import PYTHON3

from rmqid import base
from rmqid import channel
from rmqid import exceptions
from rmqid import message
from rmqid import utils
from rmqid import __version__

LOGGER = logging.getLogger(__name__)


class Connection(base.StatefulObject):
    """The Connection object is responsible for negotiating a connection and
    managing its state.

    """
    CANCEL_METHOD = ['Basic.Cancel']
    CHANNEL = 0
    CONNECTION_TIMEOUT = 1
    CONTENT_METHODS = ['Basic.Deliver', 'Basic.GetOk', 'Basic.Return']
    DEFAULT_HEARTBEAT_INTERVAL = 3
    DEFAULT_LOCALE = 'en_US'
    DEFAULT_URL = 'amqp://guest:guest@localhost:5672/%2F'
    DEFAULT_VHOST = '%2F'
    GUEST = 'guest'
    PORTS = {'amqp': 5672, 'amqps': 5671}

    def __init__(self, url=None):
        """Create a new instance of the Connection object

        :param str url: The AMQP connection URL

        """
        super(Connection, self).__init__()
        self._args = self._process_url(url or self.DEFAULT_URL)
        self._buffer = bytes() if PYTHON3 else str()
        self._channels = dict()
        self._messages = dict()
        self._stack = list()
        self._heartbeat = 0
        self._maximum_channels = 0
        self.maximum_frame_size = specification.FRAME_MAX_SIZE
        self.minimum_frame_size = specification.FRAME_MIN_SIZE
        self._properties = dict()
        self._socket = None
        self._connect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            LOGGER.exception('Connection context manager closed on exception',
                             exc_tb)
            raise exc_type(exc_val)
        LOGGER.debug('Closing connection')
        self.close()

    def close(self):
        """Close the connection, including all open channels"""
        if not self.closed:
            LOGGER.debug('Closing the connection')
            self._set_state(self.CLOSING)
            self._close_channels()
            self._write_frame(self._build_close_frame())
            self._wait_on_frame(specification.Connection.CloseOk)
            self._set_state(self.CLOSED)

    @property
    def closed(self):
        return self._state == self.CLOSED

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

    def _add_to_frame_stack(self, channel_id, frame_value):
        """Add the frame to the stack by creating the key value used in
        expectations and then add it to the list.

        :param int channel_id: The channel id the frame was received on
        :param pamqp.specification.Frame frame_value: The frame to add

        """
        key = '%i:%s' % (channel_id, frame_value.name)
        self._stack.append((key, frame_value))

    def _add_to_msg_stack(self, channel_id, frame_value, message):
        """Add the frame to the stack by creating the key value used in
        expectations and then add it to the list.

        :param int channel_id: The channel id the frame was received on
        :param pamqp.specification.Frame frame_value: The msg method frame
        :param pmqid.message.message message: The message to add

        """
        # If it's a basic return, process it elsewhere
        if frame_value.name == 'Basic.Return':
            return self._process_basic_return(channel_id,
                                              frame_value,
                                              message)

        key = '%i:%s' % (channel_id, frame_value.name)
        if key not in self._messages:
            self._messages[key] = list()
        self._messages[key].append(message)

    def _process_basic_return(self, channel_id, frame_value, message):
        """Raise a MessageReturnedException so the publisher can handle returned
        messages.

        :param int channel_id: The channel id the frame was received on
        :param pamqp.specification.Basic.Return frame_value: Method frame
        :param pmqid.message.message message: The message to add
        :raises: rmqid.exceptions.MessageReturnedException

        """
        LOGGER.warning('Basic.Return received on channel %i', channel_id)
        message_id = message.properties.get('message_id', 'Unknown')
        raise exceptions.MessageReturnedException(message_id,
                                                  frame_value.reply_code,
                                                  frame_value.reply_text)

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
                                               self.maximum_frame_size,
                                               self._heartbeat)

    def _close_channels(self):
        """Close all the channels that are currently open."""
        if not self._channels:
            return
        channels = list(self._channels.keys())
        LOGGER.debug('Closing %i channel%s', len(channels),
                     's' if len(channels) > 1 else '')
        for channel_id in channels:
            if not self._channels[channel_id].closed:
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
        self._write_frame(self._build_open_frame())
        frame_value = self._wait_on_frame(specification.Connection.OpenOk)
        LOGGER.debug('Connected with known hosts: %r', frame_value.known_hosts)

    def _connection_start(self):
        """Negotiate the Connection.Start process, writing out a
        Connection.StartOk frame when the Connection.Start frame is received.

        :rtype: bool

        """
        frame_value = self._wait_on_frame(specification.Connection.Start)
        if not self._validate_connection_start(frame_value):
            LOGGER.error('Could not negotiate a connection, disconnecting')
            self._disconnect_socket()
            return False
        self._properties = frame_value.server_properties
        self._write_frame(self._build_start_ok_frame())
        return True

    def _connection_tune(self):
        """Negotiate the Connection.Tune frames, waiting for the Connection.Tune
        frame from RabbitMQ and sending the Connection.TuneOk frame.

        """
        frame_value = self._wait_on_frame(specification.Connection.Tune)
        self._maximum_channels = frame_value.channel_max
        if frame_value.frame_max != self.maximum_frame_size:
            self.maximum_frame_size = frame_value.frame_max
        if frame_value.heartbeat:
            self._heartbeat = frame_value.heartbeat
        self._write_frame(self._build_tune_ok_frame())

    def _create_message(self, channel_id, method_frame, header_frame, body):
        """Create a message instance with the channel it was received on and the
        dictionary of message parts.

        :param int channel_id: The channel id the message was sent on
        :param pamqp.specification.Frame method_frame: The method frame value
        :param pamqp.header.ContentHeader header_frame: The header frame value
        :param str body: The message body
        :rtype: rmqid.message.Message

        """
        msg = message.Message(self._channels[channel_id],
                              body,
                              header_frame.properties.to_dict())
        msg.method = method_frame
        msg.name = method_frame.name
        return msg

    def _create_socket(self, use_ssl):
        """Create the new socket, optionally with SSL support.

        :param bool use_ssl: Use SSL to connect?
        :rtype: socket.socket or ssl.SSLSocket

        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        sock.setblocking(1)
        sock.settimeout(self.CONNECTION_TIMEOUT)
        if use_ssl:
            LOGGER.debug('Wrapping socket with SSL')
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

    def _get_from_stack(self, expectations):
        for expectation in expectations:
            if expectation in self._messages and self._messages[expectation]:
                return self._messages[expectation].pop(0)
        return self._get_frame_from_stack(expectations)

    def _get_frame_from_stack(self, expectations):
        """Get the expected frame from the buffer if it exists.

        :param list expectations: The list of keys that will satisfy the request
        :rtype: pamqp.specification.Frame | rmqid.message.Message

        """
        for offset, value in enumerate(self._stack):
            if value[0] in expectations:
                del self._stack[offset]
                return value[1]
        return None

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

    def _get_locale(self):
        """Return the current locale for the python interpreter or the default
        locale.

        :rtype: str

        """
        return locale.getlocale()[0] or self.DEFAULT_LOCALE

    def _get_next_channel_id(self):
        """Return the next channel id

        :rtype: int

        """
        if not self._channels:
            return 1
        if len(list(self._channels.keys())) == self._maximum_channels:
            raise exceptions.TooManyChannelsError
        return max(list(self._channels.keys()))

    def _is_server_rpc(self, frame_value):
        """Returns True if the frame received is a server sent RPC command.

        :rtype: bool

        """
        return frame_value.name in ['Channel.Close',
                                    'Connection.Close']

    def _normalize_expectations(self, channel_id, expectations):
        """Turn a class or list of classes into a list of class names.

        :param expectations: List of classes or class name or class obj
        :type expectations: list|str|pamqp.specification.Frame
        :rtype: list

        """
        LOGGER.debug('Normalizing %r', expectations)
        if isinstance(expectations, list):
            output = list()
            for value in expectations:
                if isinstance(value, str):
                    output.append('%i:%s' % (channel_id, value))
                else:
                    output.append('%i:%s' % (channel_id, value.name))
            return output
        elif utils.is_string(expectations):
            return ['%i:%s' % (channel_id, expectations)]
        return ['%i:%s' % (channel_id, expectations.name)]

    def _process_content_rpc(self, channel_id, frame_value):
        """Called when a content related RPC command is received.

        :param int channel_id: The channel the message was received on
        :param pamqp.specification.Frame frame_value: The rpc frame
        :rtype: rmqid.message.Message

        """
        header_value = self._wait_on_frame('ContentHeader', channel_id)
        body_value = bytes() if PYTHON3 else str()
        while len(body_value) < header_value.body_size:
            body_part = self._wait_on_frame('ContentBody', channel_id)
            body_value += body_part.value
            if len(body_value) == header_value.body_size:
                break
        return self._create_message(channel_id, frame_value,
                                    header_value, body_value)

    def _process_server_rpc(self, channel_id, value):
        """Process a RPC frame received from the server

        :param int channel_id: The channel number
        :param pamqp.message.Message value: The message value
        :rtype: rmqid.message.Message
        :raises: rmqid.exceptions.ChannelClosedException
        :raises: rmqid.exceptions.ConnectionClosedException

        """
        if value.name == 'Channel.Close':
            LOGGER.warning('Received remote close for channel %i', channel_id)
            self._channels[channel_id]._remote_close()
            raise exceptions.RemoteClosedChannelException(channel_id,
                                                          value.reply_code,
                                                          value.reply_text)
        elif value.name == 'Connection.Close':
            LOGGER.warning('Received remote close for the connection')
            self._set_state(self.CLOSED)
            raise exceptions.RemoteClosedException(value.reply_code,
                                                   value.reply_text)
        else:
            LOGGER.critical('Unhandled RPC request: %r', value)

    def _process_url(self, url):
        """Parse the AMQP URL passed in and return the configuration information
        in a dictionary of values.

        The URL format is as follows:

            amqp[s]://username:password@host:port/virtual_host

        Values in the URL such as the virtual_host should be URL encoded or
        quoted just as a URL would be in a web browser. The default virtual
        host / in RabbitMQ should be passed as %2F.

        Default values:

            - If port is omitted, port 5762 is used for AMQP and port 5671 is
              used for AMQPS
            - If username or password is omitted, the default value is guest
            - If the virtual host is omitted, the default value of %2F is used

        :param str url: The AMQP url passed in
        :rtype: dict
        :raises: ValueError

        """
        parsed = utils.urlparse(url)

        # Ensure the protocol scheme is what is expected
        if parsed.scheme not in list(self.PORTS.keys()):
            raise ValueError('Unsupported protocol: %s' % parsed.scheme)

        # Toggle the SSL flag based upon the URL scheme
        use_ssl = True if parsed.scheme == 'amqps' else False

        # Ensure that SSL is available if SSL is requested
        if use_ssl and not ssl:
            LOGGER.warning('SSL requested but not available, disabling')
            use_ssl = False

        # Use the default ports if one is not specified
        port = parsed.port or (self.PORTS['amqps'] if parsed.scheme == 'amqps'
                               else self.PORTS['amqp'])

        # Set the vhost to be after the base slash if it was specified
        vhost = parsed.path[1:] if parsed.path else self.DEFAULT_VHOST

        # If the path was just the base path, set the vhost to the default
        if not vhost:
            vhost = self.DEFAULT_VHOST

        # Return the configuration dictionary to use when connecting
        return {'host': parsed.hostname,
                'port': port,
                'virtual_host': utils.unquote(vhost),
                'username': parsed.username or self.GUEST,
                'password': parsed.password or self.GUEST,
                'ssl': use_ssl}

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
            value = self._socket.recv(self.maximum_frame_size)
            return value
        except socket.timeout:
            LOGGER.debug('Socket timeout')
            return bytes() if PYTHON3 else str()
        except socket.error as error:
            self._set_state(self.CLOSED)
            raise exceptions.RemoteClosedException(-1,
                                                   'Socket Error: %s' %
                                                   error)

    def _validate_connection_start(self, frame_value):
        """Validate the received Connection.Start frame

        :param specification.Connection.Start frame_value: The frame to validate
        :rtype: bool

        """
        if (frame_value.version_major,
            frame_value.version_minor) != (specification.VERSION[0],
                                           specification.VERSION[1]):
            LOGGER.warning('AMQP version mismatch, received %i.%i, expected %r',
                           frame_value.version_major, frame_value.version_minor,
                           specification.VERSION)
            return False
        return True

    def _wait_on_frame(self, expectations, channel_id=CHANNEL):
        """Wait for a frame to come in from the broker.

        :param list or class expectations: Class.Method or list of Class.Methods
        :param int channel_id: The channel number to wait for the frame on
        :rtype: pamqp.specification.Frame | pamqp.message.Message

        """
        expectations = self._normalize_expectations(channel_id, expectations)
        while not self.closed:

            # Try and get the value
            value = self._get_from_stack(expectations)
            if value is not None:
                LOGGER.debug('Met expectations with %r', value)
                return value

            # Since the value is not on the stack, try and read in the frame
            channel_value, frame_value = self._read_frame()

            # If there is no frame, try again
            if frame_value is None:
                continue

            # If the server sent a command, process it
            if self._is_server_rpc(frame_value):
                self._process_server_rpc(channel_value, frame_value)
                continue

            # If it's a content method, append a Message object for the frame
            if frame_value.name in self.CONTENT_METHODS:
                self._add_to_msg_stack(channel_value, frame_value,
                                       self._process_content_rpc(channel_value,
                                                                 frame_value))
                continue

            # Is not a content method, just add it to the stack
            self._add_to_frame_stack(channel_value, frame_value)

    def _write_frame(self, frame_value, channel_id=CHANNEL):
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
