"""
The Connection class negotiates and manages the connection state.

"""
import logging
try:
    import queue
except ImportError:
    import Queue as queue
import socket
try:
    import ssl
except ImportError:
    ssl = None
import time

from rabbitpy import DEBUG
from rabbitpy import base
from rabbitpy import io
from rabbitpy import channel
from rabbitpy import channel0
from rabbitpy import events
from rabbitpy import exceptions
from rabbitpy import message
from rabbitpy import utils

LOGGER = logging.getLogger(__name__)

AMQP = 'amqp'
AMQPS = 'amqps'

if ssl:
    SSL_CERT_MAP = {'ignore': ssl.CERT_NONE,
                    'optional': ssl.CERT_OPTIONAL,
                    'required': ssl.CERT_REQUIRED}
    SSL_VERSION_MAP = dict()
    if hasattr(ssl, 'PROTOCOL_SSLv2'):
        SSL_VERSION_MAP['SSLv2'] = ssl.PROTOCOL_SSLv2
    if hasattr(ssl, 'PROTOCOL_SSLv3'):
        SSL_VERSION_MAP['SSLv3'] = ssl.PROTOCOL_SSLv3
    if hasattr(ssl, 'PROTOCOL_SSLv23'):
        SSL_VERSION_MAP['SSLv23'] = ssl.PROTOCOL_SSLv23
    if hasattr(ssl, 'PROTOCOL_TLSv1'):
        SSL_VERSION_MAP['TLSv1'] = ssl.PROTOCOL_TLSv1
else:
    SSL_CERT_MAP, SSL_VERSION_MAP = dict(), dict()


class Connection(base.StatefulObject):
    """The Connection object is responsible for negotiating a connection and
    managing its state. When creating a new instance of the Connection object,
    if no URL is passed in, it uses the default connection parameters of
    localhost port 5672, virtual host / with the guest/guest username/password
    combination. Represented as a AMQP URL the connection information is:

        :code:`amqp://guest:guest@localhost:5672/%2F`

    To use a different connection, pass in a AMQP URL that follows the standard
    format:

        :code:`[scheme]://[username]:[password]@[host]:[port]/[virtual_host]`

    The following example connects to the test virtual host on a RabbitMQ
    server running at 192.168.1.200 port 5672 as the user "www" and the
    password rabbitmq:

        :code:`amqp://admin192.168.1.200:5672/test`

    :param str url: The AMQP connection URL

    """
    CANCEL_METHOD = ['Basic.Cancel']
    DEFAULT_HEARTBEAT_INTERVAL = 3
    DEFAULT_LOCALE = 'en_US'
    DEFAULT_URL = 'amqp://guest:guest@localhost:5672/%2F'
    DEFAULT_VHOST = '%2F'
    GUEST = 'guest'
    PORTS = {'amqp': 5672, 'amqps': 5671, 'api': 15672}

    QUEUE_WAIT = 0.01

    def __init__(self, url=None):
        """Create a new instance of the Connection object"""
        super(Connection, self).__init__()

        # Create a name for the connection
        self._name = '0x%x' % id(self)

        # Extract parts of connection URL for use later
        self._args = self._process_url(url or self.DEFAULT_URL)

        # General events and queues shared across threads
        self._events = events.Events()

        # A queue for the child threads to put exceptions in
        self._exceptions = queue.Queue()

        # One queue for writing frames, regardless of the channel sending them
        self._write_queue = queue.Queue()

        # Attributes for core object threads
        self._channel0 = None
        self._channels = dict()
        self._io = None

        # Used by Message for breaking up body frames
        self._maximum_frame_size = None

        # Connect to RabbitMQ
        self._connect()

    def __enter__(self):
        """For use as a context manager, return a handle to this object
        instance.

        :rtype: Connection

        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """When leaving the context, examine why the context is leaving, if
        it's an exception or what.

        """
        if exc_type:
            LOGGER.error('Shutting down connection on unhandled exception: %s',
                         exc_type)
        self.close()

    @property
    def blocked(self):
        """Indicates if the connection is blocked from publishing by RabbitMQ.

        This flag indicates communication from RabbitMQ that the connection is
        blocked using the Connection.Blocked RPC notification from RabbitMQ
        that was added in RabbitMQ 3.2.

        @TODO If RabbitMQ version < 3.2, use the HTTP management API to query
        the value

        :rtype: bool

        """
        return self._events.is_set(events.CONNECTION_BLOCKED)

    def channel(self):
        """Create a new channel"""
        channel_id = self._get_next_channel_id()
        channel_frames = queue.Queue()
        self._channels[channel_id] = channel.Channel(channel_id,
                                                     self._events,
                                                     self._exceptions,
                                                     channel_frames,
                                                     self._write_queue,
                                                     self._maximum_frame_size,
                                                     self._io.write_trigger)
        self._add_channel_to_io(self._channels[channel_id], channel_frames)
        self._channels[channel_id].open()
        return self._channels[channel_id]

    def close(self):
        """Close the connection, including all open channels"""
        if not self.closed:
            self._set_state(self.CLOSING)

            # Shutdown the IO thread and socket
            self._shutdown_connection()

            # Set state and clear out remote name
            self._set_state(self.CLOSED)

    @property
    def server_properties(self):
        """Return the RabbitMQ Server properties from the connection
        negotiation process.

        :rtype: dict

        """
        return self._channel0.properties

    def _add_channel_to_io(self, channel_id, channel_queue):
        """Add a channel and queue to the IO object.

        :param Queue.Queue channel_queue: Channel inbound msg queue
        :param rabbitpy.base.AMQPChannel: The channel to add

        """
        if DEBUG:
            LOGGER.debug('Adding channel %s to io', int(channel_id))
        self._io.add_channel(channel_id, channel_queue)

    @property
    def _api_credentials(self):
        """Return the auth credentials as a tuple

        @rtype: tuple

        """
        return self._args['username'], self._args['password']

    def _close_channels(self):
        """Close all the channels that are currently open."""
        for channel_id in self._channels:
            if (self._channels[channel_id].open and
                    not self._channels[channel_id].closing):
                self._channels[channel_id].close()

    def _connect(self):
        """Connect to the RabbitMQ Server"""
        self._set_state(self.OPENING)

        # Create and start the IO object that reads, writes & dispatches frames
        self._io = self._create_io_thread()
        self._io.start()

        # Wait for IO to connect to the socket or raise an exception
        while self.opening and not self._events.is_set(events.SOCKET_OPENED):
            if not self._exceptions.empty():
                exception = self._exceptions.get()
                raise exception

        # If the socket could not be opened, return instead of waiting
        if self.closed:
            return self.close()

        # Create the Channel0 queue and add it to the IO thread
        self._channel0 = self._create_channel0()
        self._add_channel_to_io(self._channel0, None)
        self._channel0.start()

        # Wait for Channel0 to raise an exception or negotiate the connection
        while not self._channel0.open:
            if not self._exceptions.empty():
                exception = self._exceptions.get()
                self._io.stop()
                raise exception
            time.sleep(0.1)

        # Set the maximum frame size for channel use
        self._maximum_frame_size = self._channel0.maximum_frame_size

    def _create_channel0(self):
        """Each connection should have a distinct channel0

        :rtype: rabbitpy.channel0.Channel0

        """
        return channel0.Channel0(connection_args=self._args,
                                 events_obj=self._events,
                                 exception_queue=self._exceptions,
                                 write_queue=self._write_queue,
                                 write_trigger=self._io.write_trigger)

    def _create_io_thread(self):
        """Create the IO thread and the objects it uses for communication.

        :rtype: rabbitpy.io.IO

        """
        return io.IO(name='%s-io' % self._name,
                     kwargs={'events': self._events,
                             'exceptions': self._exceptions,
                             'connection_args': self._args,
                             'write_queue': self._write_queue})

    def _create_message(self, channel_id, method_frame, header_frame, body):
        """Create a message instance with the channel it was received on and
        the dictionary of message parts.

        :param int channel_id: The channel id the message was sent on
        :param pamqp.specification.Frame method_frame: The method frame value
        :param pamqp.header.ContentHeader header_frame: The header frame value
        :param str body: The message body
        :rtype: rabbitpy.message.Message

        """
        msg = message.Message(self._channels[channel_id],
                              body,
                              header_frame.properties.to_dict())
        msg.method = method_frame
        msg.name = method_frame.name
        return msg

    def _get_next_channel_id(self):
        """Return the next channel id

        :rtype: int

        """
        if not self._channels:
            return 1
        if self._max_channel_id == self._channel0.maximum_channels:
            raise exceptions.TooManyChannelsError
        return self._max_channel_id + 1

    @staticmethod
    def _get_ssl_validation(values):
        """Return the value mapped from the string value in the query string
        for the AMQP URL specifying which level of server certificate
        validation is required, if any.

        :param dict values: The dict of query values from the AMQP URI
        :rtype: int

        """
        validation = values.get('ssl_validation', [None])[0]
        if validation is None:
            return None
        if validation not in SSL_CERT_MAP:
            raise ValueError('Unsupported server cert validation option: %s',
                             validation)
        return SSL_VERSION_MAP[validation]

    @staticmethod
    def _get_ssl_version(values):
        """Return the value mapped from the string value in the query string
        for the AMQP URL for SSL version.

        :param dict values: The dict of query values from the AMQP URI
        :rtype: int

        """
        version = values.get('ssl_version', [None])[0]
        if version is None:
            return None
        if version not in SSL_VERSION_MAP:
            raise ValueError('Unuspported SSL version: %s' % version)
        return SSL_VERSION_MAP[version]

    @property
    def _max_channel_id(self):
        return max(list(self._channels.keys()))

    @staticmethod
    def _normalize_expectations(channel_id, expectations):
        """Turn a class or list of classes into a list of class names.

        :param expectations: List of classes or class name or class obj
        :type expectations: list or str or pamqp.specification.Frame
        :rtype: list

        """
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

    def _process_url(self, url):
        """Parse the AMQP URL passed in and return the configuration
        information in a dictionary of values.

        The URL format is as follows:

            amqp[s]://username:password@host:port/virtual_host[?query string]

        Values in the URL such as the virtual_host should be URL encoded or
        quoted just as a URL would be in a web browser. The default virtual
        host / in RabbitMQ should be passed as %2F.

        Default values:

            - If port is omitted, port 5762 is used for AMQP and port 5671 is
              used for AMQPS
            - If username or password is omitted, the default value is guest
            - If the virtual host is omitted, the default value of %2F is used

        Query string options:

            - heartbeat_interval
            - locale
            - ssl_cacert - Path to CA certificate file
            - ssl_cert - Path to client certificate file
            - ssl_key - Path to client certificate key
            - ssl_validation - Server certificate validation requirements (1)
            - ssl_version - SSL version to use (2)

            (1) Should be one of three values:

               - ignore - Ignore the cert if provided (default)
               - optional - Cert is validated if provided
               - required - Cert is required and validated

            (2) Should be one of four values:

              - SSLv2
              - SSLv3
              - SSLv23
              - TLSv1

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
        port = parsed.port or (self.PORTS[AMQPS] if parsed.scheme == AMQPS
                               else self.PORTS[AMQP])

        # Set the vhost to be after the base slash if it was specified
        vhost = parsed.path[1:] if parsed.path else self.DEFAULT_VHOST

        # If the path was just the base path, set the vhost to the default
        if not vhost:
            vhost = self.DEFAULT_VHOST

        # Parse the query string
        query_values = utils.parse_qs(parsed.query)

        # Make sure the heartbeat is an int if it is not None
        heartbeat = int(query_values.get('heartbeat_interval', [None])[0] or 0)

        # Return the configuration dictionary to use when connecting
        return {'host': parsed.hostname,
                'port': port,
                'virtual_host': utils.unquote(vhost),
                'username': parsed.username or self.GUEST,
                'password': parsed.password or self.GUEST,
                'heartbeat': heartbeat,
                'locale': query_values.get('locale', [None])[0],
                'ssl': use_ssl,
                'ssl_cacert': query_values.get('ssl_cacert', [None])[0],
                'ssl_cert': query_values.get('ssl_cert', [None])[0],
                'ssl_key': query_values.get('ssl_key', [None])[0],
                'ssl_validation': self._get_ssl_validation(query_values),
                'ssl_version': self._get_ssl_version(query_values)}

    def _shutdown_connection(self):
        """Tell Channel0 and IO to stop if they are not stopped."""
        #
        if not self._io.is_alive():
            self._set_state(self.CLOSED)
            if DEBUG:
                LOGGER.debug('Cant shutdown connection, IO is no longer alive')
            return

        # Close any open channels
        for chan_id in [chan_id for chan_id in self._channels
                        if not self._channels[chan_id].closed]:
            self._channels[chan_id].close()

        # If the connection is still established, close it
        if self._channel0.open:
            self._channel0.close()

            # Loop while Channel 0 closes
            if DEBUG:
                LOGGER.debug('Waiting on channel0 to close')
            while not self._channel0.closed:
                time.sleep(0.1)
            if DEBUG:
                LOGGER.debug('channel0 closed')

        # Close the socket
        if (self._events.is_set(events.SOCKET_OPENED) and
                not self._events.is_set(events.SOCKET_CLOSED)):
            if DEBUG:
                LOGGER.debug('Requesting IO socket close')
            self._events.set(events.SOCKET_CLOSE)

            # Break out of select waiting
            self._trigger_write()

            if DEBUG:
                LOGGER.debug('Waiting on socket to close')
            self._events.wait(events.SOCKET_CLOSED, 0.1)
            while self._io.is_alive():
                time.sleep(0.25)

    def _trigger_write(self):
        """Notifies the IO loop we need to write a frame by writing a byte
        to a local socket.

        """
        try:
            self._io.write_trigger.send(b'0')
        except socket.error:
            pass
