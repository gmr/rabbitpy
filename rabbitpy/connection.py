"""
The Connection class negotiates and manages the connection state.

"""
import logging
try:
    import ssl
except ImportError:
    ssl = None
import threading
import time

from pamqp import specification as spec

from rabbitpy import base
from rabbitpy import heartbeat
from rabbitpy import io
from rabbitpy import channel
from rabbitpy import channel0
from rabbitpy import events
from rabbitpy import exceptions
from rabbitpy import message
from rabbitpy.utils import queue
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
        SSL_VERSION_MAP['SSLv2'] = getattr(ssl, 'PROTOCOL_SSLv2')
    if hasattr(ssl, 'PROTOCOL_SSLv3'):
        SSL_VERSION_MAP['SSLv3'] = getattr(ssl, 'PROTOCOL_SSLv3')
    if hasattr(ssl, 'PROTOCOL_SSLv23'):
        SSL_VERSION_MAP['SSLv23'] = getattr(ssl, 'PROTOCOL_SSLv23')
    if hasattr(ssl, 'PROTOCOL_TLSv1'):
        SSL_VERSION_MAP['TLSv1'] = getattr(ssl, 'PROTOCOL_TLSv1')
else:
    SSL_CERT_MAP, SSL_VERSION_MAP = dict(), dict()


# pylint: disable=too-many-instance-attributes
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

    .. note::

        You should be aware that most connection exceptions may be raised
        during the use of all functionality in the library.

    :param str url: The AMQP connection URL
    :raises: rabbitpy.exceptions.AMQPException
    :raises: rabbitpy.exceptions.ConnectionException
    :raises: rabbitpy.exceptions.ConnectionResetException
    :raises: rabbitpy.exceptions.RemoteClosedException

    """
    CANCEL_METHOD = ['Basic.Cancel']
    DEFAULT_CHANNEL_MAX = 65535
    DEFAULT_TIMEOUT = 3
    DEFAULT_HEARTBEAT_INTERVAL = 300
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

        # Lock used when managing the channel stack
        self._channel_lock = threading.Lock()

        # Attributes for core object threads
        self._channel0 = None
        self._channels = dict()
        self._heartbeat_checker = None
        self._io = None

        # Used by Message for breaking up body frames
        self._max_frame_size = None

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
        if exc_type and exc_val:
            self._set_state(self.CLOSED)
            raise
        self._set_state(self.CLOSED)
        self._shutdown_connection(True)

    @property
    def args(self):
        """Return the connection arguments.

        :rtype: dict

        """
        return dict(self._args)

    @property
    def blocked(self):
        """Indicates if the connection is blocked from publishing by RabbitMQ.

        This flag indicates communication from RabbitMQ that the connection is
        blocked using the Connection.Blocked RPC notification from RabbitMQ
        that was added in RabbitMQ 3.2.

        :rtype: bool

        """
        return self._events.is_set(events.CONNECTION_BLOCKED)

    def channel(self, blocking_read=False):
        """Create a new channel

        If blocking_read is True, the cross-thread Queue.get use will use
        blocking operations that lower resource utilization and increase
        throughput. However, due to how Python's blocking Queue.get is
        implemented, KeyboardInterrupt is not raised when CTRL-C is
        pressed.

        :param bool blocking_read: Enable for higher throughput
        :raises: rabbitpy.exceptions.AMQPException
        :raises: rabbitpy.exceptions.RemoteClosedChannelException

        """
        with self._channel_lock:
            channel_id = self._get_next_channel_id()
            channel_frames = queue.Queue()
            self._channels[channel_id] = \
                channel.Channel(channel_id,
                                self.capabilities,
                                self._events,
                                self._exceptions,
                                channel_frames,
                                self._write_queue,
                                self._max_frame_size,
                                self._io.write_trigger,
                                blocking_read)
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
    def capabilities(self):
        """Return the RabbitMQ Server capabilities from the connection
        negotiation process.

        :rtype: dict

        """
        return self._channel0.properties.get(b'capabilities', dict())

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
        LOGGER.debug('Adding channel %s to io', int(channel_id))
        self._io.add_channel(channel_id, channel_queue)

    @property
    def _api_credentials(self):
        """Return the auth credentials as a tuple

        @rtype: tuple

        """
        return self._args['username'], self._args['password']

    @property
    def _channel0_closed(self):
        """Returns a boolean indicating if the base connection channel (0)
        is closed.

        :rtype: bool

        """
        return self._channel0.open and not \
            self._events.is_set(events.CHANNEL0_CLOSED)

    def _close_all_channels(self, force=False):
        """Close all open channels

        :param force: Force the connection to shutdown without AMQP negotiation
        :type force: bool

        """
        for chan_id in [chan_id for chan_id in self._channels
                        if not self._channels[chan_id].closed]:
            if force:
                # pylint: disable=protected-access
                self._channels[chan_id]._force_close()
            else:
                self._channels[chan_id].close()

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
        self._io.daemon = True
        self._io.start()

        # Wait for IO to connect to the socket or raise an exception
        while self.opening and not self._events.is_set(events.SOCKET_OPENED):
            if not self._exceptions.empty():
                exception = self._exceptions.get()
                raise exception

            # :meth:`threading.html#threading.Event.wait` always returns None
            # in 2.6, so it is impossible to simply check wait() result
            self._events.wait(events.SOCKET_OPENED, self._args['timeout'])
            if not self._events.is_set(events.SOCKET_OPENED):
                raise RuntimeError("Timeout waiting for opening the socket")

        # If the socket could not be opened, return instead of waiting
        if self.closed:
            return self.close()

        # Create the heartbeat checker
        self._heartbeat_checker = heartbeat.Checker(self._io, self._exceptions)

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
            time.sleep(0.01)

        # Set the maximum frame size for channel use
        self._max_frame_size = self._channel0.maximum_frame_size

        self._heartbeat_checker.start(self._channel0.heartbeat_interval)

    def _create_channel0(self):
        """Each connection should have a distinct channel0

        :rtype: rabbitpy.channel0.Channel0

        """
        return channel0.Channel0(connection_args=self._args,
                                 events_obj=self._events,
                                 exception_queue=self._exceptions,
                                 heartbeat_checker=self._heartbeat_checker,
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
        :param method_frame: The method frame value
        :type method_frame: pamqp.specification.Frame
        :param header_frame: The header frame value
        :type header_frame: pamqp.header.ContentHeader
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

    @property
    def _max_channel_id(self):
        """Return the maximum channel ID that is currently being used.

        :rtype: int

        """
        return max(list(self._channels.keys()))

    def _maybe_close_connection(self):
        """Perform the steps required to shutdown channel0 and close the
        socket.

        """
        if not self._channel0_closed:
            self._channel0.close()

        # Ensure the connection is closed
        self._trigger_write()

        # Let the IOLoop know to close
        self._events.set(events.SOCKET_CLOSE)

        # Break out of select waiting
        self._trigger_write()

        if (self._events.is_set(events.SOCKET_OPENED) and
                not self._events.is_set(events.SOCKET_CLOSED)):
            LOGGER.debug('Waiting on socket to close')
            self._events.wait(events.SOCKET_CLOSED, 0.1)

    @staticmethod
    def _normalize_expectations(channel_id, expectations):
        """Turn a class or list of classes into a list of class names.

        :param int channel_id: The channel to normalize for
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

            - heartbeat
            - channel_max
            - frame_max
            - locale
            - cacertfile - Path to CA certificate file
            - certfile - Path to client certificate file
            - keyfile - Path to client certificate key
            - verify - Server certificate validation requirements (1)
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

        self._validate_uri_scheme(parsed.scheme)

        # Toggle the SSL flag based upon the URL scheme and if SSL is enabled
        use_ssl = True if parsed.scheme == 'amqps' and ssl else False

        # Ensure that SSL is available if SSL is requested
        if parsed.scheme == 'amqps' and not ssl:
            LOGGER.warning('SSL requested but not available, disabling')

        # Figure out the port as specified by the scheme
        scheme_port = self.PORTS[AMQPS] if parsed.scheme == AMQPS \
            else self.PORTS[AMQP]

        # Set the vhost to be after the base slash if it was specified
        vhost = self.DEFAULT_VHOST
        if parsed.path:
            vhost = parsed.path[1:] or self.DEFAULT_VHOST

        # Parse the query string
        qargs = utils.parse_qs(parsed.query)

        # Return the configuration dictionary to use when connecting
        return {
            'host': parsed.hostname,
            'port': parsed.port or scheme_port,
            'virtual_host': utils.unquote(vhost),
            'username': parsed.username or self.GUEST,
            'password': parsed.password or self.GUEST,
            'timeout': self._qargs_int('timeout', qargs, self.DEFAULT_TIMEOUT),
            'heartbeat': self._qargs_int('heartbeat', qargs,
                                         self.DEFAULT_HEARTBEAT_INTERVAL),
            'frame_max': self._qargs_int('frame_max', qargs,
                                         spec.FRAME_MAX_SIZE),
            'channel_max': self._qargs_int('channel_max', qargs,
                                           self.DEFAULT_CHANNEL_MAX),
            'locale': self._qargs_value('locale', qargs),
            'ssl': use_ssl,
            'cacertfile': self._qargs_mk_value(['cacertfile', 'ssl_cacert'],
                                               qargs),
            'certfile': self._qargs_mk_value(['certfile', 'ssl_cert'], qargs),
            'keyfile': self._qargs_mk_value(['keyfile', 'ssl_key'], qargs),
            'verify': self._qargs_ssl_validation(qargs),
            'ssl_version': self._qargs_ssl_version(qargs)}

    @staticmethod
    def _qargs_int(key, values, default):
        """Return the query arg value as an integer for the specified key or
        return the specified default value.

        :param str key: The key to return the value for
        :param dict values: The query value dict returned by urlparse
        :param int default: The default return value
        :rtype: int

        """
        return int(values.get(key, [default])[0])

    @staticmethod
    def _qargs_float(key, values, default):
        """Return the query arg value as a float for the specified key or
        return the specified default value.

        :param str key: The key to return the value for
        :param dict values: The query value dict returned by urlparse
        :param float default: The default return value
        :rtype: float

        """
        return float(values.get(key, [default])[0])

    def _qargs_ssl_validation(self, values):
        """Return the value mapped from the string value in the query string
        for the AMQP URL specifying which level of server certificate
        validation is required, if any.

        :param dict values: The dict of query values from the AMQP URI
        :rtype: int

        """
        validation = self._qargs_mk_value(['verify', 'ssl_validation'], values)
        if not validation:
            return
        elif validation not in SSL_CERT_MAP:
            raise ValueError(
                'Unsupported server cert validation option: %s',
                validation)
        return SSL_CERT_MAP[validation]

    def _qargs_ssl_version(self, values):
        """Return the value mapped from the string value in the query string
        for the AMQP URL for SSL version.

        :param dict values: The dict of query values from the AMQP URI
        :rtype: int

        """
        version = self._qargs_value('ssl_version', values)
        if not version:
            return
        elif version not in SSL_VERSION_MAP:
            raise ValueError('Unuspported SSL version: %s' % version)
        return SSL_VERSION_MAP[version]

    @staticmethod
    def _qargs_value(key, values, default=None):
        """Return the value from the query arguments for the specified key
        or the default value.

        :param str key: The key to get the value for
        :param dict values: The query value dict returned by urlparse
        :return: mixed

        """
        return values.get(key, [default])[0]

    def _qargs_mk_value(self, keys, values):
        """Try and find the query string value where the value can be specified
        with different keys.

        :param lists keys: The keys to check
        :param dict values: The query value dict returned by urlparse
        :return: mixed

        """
        for key in keys:
            value = self._qargs_value(key, values)
            if value is not None:
                return value
        return None

    def _shutdown_connection(self, force=False):
        """Tell Channel0 and IO to stop if they are not stopped.

        :param force: Force the connection to shutdown without AMQP negotiation
        :type force: bool

        """
        # Make sure the heartbeat checker is not running
        self._heartbeat_checker.stop()

        if not force and not self._io.is_alive():
            self._set_state(self.CLOSED)
            LOGGER.debug('Cant shutdown connection, IO is no longer alive')
            return

        self._close_all_channels(force)
        self._maybe_close_connection()

        self._io.stop()

        # Wait for the IO thread to stop
        while self._io.is_alive():
            time.sleep(0.25)

    def _trigger_write(self):
        """Notifies the IO loop we need to write a frame by writing a byte
        to a local socket.

        """
        utils.trigger_write(self._io.write_trigger)

    def _validate_uri_scheme(self, scheme):
        """Insure that the specified URI scheme is supported by rabbitpy

        :param str scheme: The value to validate
        :raises: ValueError

        """
        if scheme not in list(self.PORTS.keys()):
            raise ValueError('Unsupported URI scheme: %s' % scheme)
