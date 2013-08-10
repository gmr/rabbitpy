"""
The Connection class negotiates and manages the connection state.

"""
import logging
import Queue as queue
try:
    import ssl
except ImportError:
    ssl = None
import threading
import time


from rabbitpy import base
from rabbitpy import io
from rabbitpy import channel
from rabbitpy import channel0
from rabbitpy import exceptions
from rabbitpy import message
from rabbitpy import utils

LOGGER = logging.getLogger(__name__)

if ssl:
    SSL_CERT_MAP = {'ignore': ssl.CERT_NONE,
                    'optional': ssl.CERT_OPTIONAL,
                    'required': ssl.CERT_REQUIRED}
    SSL_VERSION_MAP = {'SSLv2': ssl.PROTOCOL_SSLv2,
                       'SSLv3': ssl.PROTOCOL_SSLv3,
                       'SSLv23': ssl.PROTOCOL_SSLv23,
                       'TLSv1': ssl.PROTOCOL_TLSv1}
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

    The following example connects to the test virtual host on a RabbitMQ server
    running at 192.168.1.200 port 5672 as the user "www" and the password
    rabbitmq:

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

    def __init__(self, url=None):
        """Create a new instance of the Connection object"""
        super(Connection, self).__init__()
        self._args = self._process_url(url or self.DEFAULT_URL)
        self._channel0 = None
        self._channels = dict()
        self._io = None
        self._write_queue = None

        # Create a name for the connection
        self._name = 'c-%x' % id(self)

        # Connect to RabbitMQ
        self._connect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            LOGGER.exception('Context manager closed on exception: %s',
                             exc_val)
            raise
        LOGGER.debug('Closing connection')
        self.close()

    @property
    def blocked(self):
        """Indicates if the connection is blocked from publishing by RabbitMQ.
        This flag indicates communication from RabbitMQ that the connection is
        blocked using the Connection.Blocked RPC notification from RabbitMQ
        slated to be added in 3.2.

        :rtype: bool

        """
        return self._blocked

    def channel(self):
        """Create a new channel"""
        LOGGER.debug('Creating a new channel')
        channel_id = self._get_next_channel_id()
        self._channels[channel_id] = channel.Channel(channel_id, self)
        return self._channels[channel_id]

    def close(self):
        """Close the connection, including all open channels"""
        if not self.closed:
            self._set_state(self.CLOSING)
            self._close_channels()
            self._channel0[1].set()
            while self._channel0[0].is_alive and self._channel0[0].open:
                time.sleep(0.2)
            self._io[1].set()
            while self._io[0].is_alive and self._io[0].open:
                time.sleep(0.2)
            self._set_state(self.CLOSED)
            self._remote_name = None

    @property
    def closed(self):
        return self._state == self.CLOSED

    @property
    def name(self):
        """Return the name of the connection as RabbitMQ internally holds it
        for use when trying to get the state of the connection or channel from
        the RabbitMQ API.

        :rtype: str

        """
        return self._remote_name

    def set_api_port(self, port):
        """Change the default API port from 15672 to the specified value.

        :param int port: The API port value

        """
        self._api_port = port

    @property
    def _api_base_url(self):
        """Return the base API URL

        :rtype: str

        """
        return 'http://%s:%s/api' % (self._args['host'], self._api_port)

    def _add_channel_to_io(self, channel_queue, channel):
        """Add a channel and queue to the IO object.

        :param Queue.Queue channel_queue: Channel inbound msg queue
        :param rabbitpy.channel.BaseChannel: The channel to add

        """
        self._io[0].add_channel(int(channel), channel_queue)

    @property
    def _api_credentials(self):
        """Return the auth credentials as a tuple

        @rtype: tuple

        """
        return self._args['username'], self._args['password']

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

    def _connect(self):
        """Connect to the RabbitMQ Server

        :raises: ConnectionException

        """
        self._set_state(self.OPENING)

        # Create and start the IO object that reads, writes & dispatches frames
        (on_connected, close_event,
         self._write_queue, obj) = self._create_io_thread()
        self._io = (obj, close_event)
        self._io[0].start()

        # Wait for IO to connect to the socket or raise an exception
        on_connected.wait()

        # Create the Channel0 queue and add it to the IO thread
        (on_connected, close_event,
         channel0_queue, obj) = self._create_channel0()
        self._channel0 = (obj, close_event)
        self._add_channel_to_io(channel0_queue, self._channel0[0])

        # Start the Channel0 thread, starting the connection process
        self._channel0[0].start()

        # Wait for Channel0 to raise an exception or negotiate the connection
        on_connected.wait()

    def _create_message(self, channel_id, method_frame, header_frame, body):
        """Create a message instance with the channel it was received on and the
        dictionary of message parts.

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

    def _create_channel0(self):
        """Each connection should have a distinct channel0

        :rtype: tuple(threading.Event, Queue.Queue, rabbitpy.channel0.Channel0)

        """
        channel0_frames = queue.Queue()
        on_connected = threading.Event()
        close_event = threading.Event()
        return (on_connected,
                close_event,
                channel0_frames,
                channel0.Channel0(name='%s-channel0' % self._name,
                                  kwargs={'channel_number': 0,
                                          'close': close_event,
                                          'connection_args': self._args,
                                          'inbound': channel0_frames,
                                          'outbound': self._write_queue,
                                          'connected': on_connected}))

    def _create_io_thread(self):
        """Create the IO thread and the objects it uses for communication.

        :rtype: tuple(threading.Event, rabbitpy.io.IO)

        """
        io_write_queue = queue.Queue()
        on_connected = threading.Event()
        close_event = threading.Event()
        return (on_connected,
                close_event,
                io_write_queue,
                io.IO(name='%s-io' % self._name,
                      kwargs={'connected': on_connected,
                              'close': close_event,
                              'connection_args': self._args,
                              'write_queue': io_write_queue}))

    def _get_next_channel_id(self):
        """Return the next channel id

        :rtype: int

        """
        if not self._channels:
            return 1
        #if len(list(self._channels.keys())) ==
        # self._channel0[0].maximum_channels:
        #    raise exceptions.TooManyChannelsError
        return max(list(self._channels.keys()))

    def _get_ssl_validation(self, values):
        """Return the value mapped from the string value in the query string
        for the AMQP URL specifying which level of server certificate
        validation is required, if any.

        :param dict values: The dict of query values from the AMQP URI
        :rtype: int

        """
        if values.get('ssl_validation') is None:
            return None
        if values.get('ssl_validation') not in SSL_CERT_MAP:
            raise ValueError('Unsupported server cert validation option: %s',
                             values['ssl_version'])
        return SSL_VERSION_MAP[values['ssl_version']]

    def _get_ssl_version(self, values):
        """Return the value mapped from the string value in the query string
        for the AMQP URL for SSL version.

        :param dict values: The dict of query values from the AMQP URI
        :rtype: int

        """
        if values.get('ssl_version') is None:
            return None
        if values.get('ssl_version') not in SSL_VERSION_MAP:
            raise ValueError('Unuspported SSL version: %s' %
                             values['ssl_version'])
        return SSL_VERSION_MAP[values['ssl_version']]

    def _normalize_expectations(self, channel_id, expectations):
        """Turn a class or list of classes into a list of class names.

        :param expectations: List of classes or class name or class obj
        :type expectations: list|str|pamqp.specification.Frame
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
        """Parse the AMQP URL passed in and return the configuration information
        in a dictionary of values.

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
        port = parsed.port or (self.PORTS['amqps'] if parsed.scheme == 'amqps'
                               else self.PORTS['amqp'])

        # Set the vhost to be after the base slash if it was specified
        vhost = parsed.path[1:] if parsed.path else self.DEFAULT_VHOST

        # If the path was just the base path, set the vhost to the default
        if not vhost:
            vhost = self.DEFAULT_VHOST

        # Parse the query string
        query_values = utils.parse_qs(parsed.query)

        # Return the configuration dictionary to use when connecting
        return {'host': parsed.hostname,
                'port': port,
                'virtual_host': utils.unquote(vhost),
                'username': parsed.username or self.GUEST,
                'password': parsed.password or self.GUEST,
                'heartbeat': query_values.get('heartbeat_interval'),
                'locale': query_values.get('locale'),
                'ssl': use_ssl,
                'ssl_cacert': query_values.get('ssl_cacert'),
                'ssl_cert': query_values.get('ssl_cert'),
                'ssl_key': query_values.get('ssl_key'),
                'ssl_validation': self._get_ssl_validation(query_values),
                'ssl_version': self._get_ssl_version(query_values)}
