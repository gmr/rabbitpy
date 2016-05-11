"""
Channel0 is used for connection level communication between RabbitMQ and the
client on channel 0.

"""
import locale
import logging
import sys

from pamqp import header
from pamqp import heartbeat
from pamqp import specification

from rabbitpy import base
from rabbitpy import events
from rabbitpy import exceptions
from rabbitpy import version
from rabbitpy.utils import queue

LOGGER = logging.getLogger(__name__)
DEFAULT_LOCALE = locale.getdefaultlocale()
del locale


class Channel0(base.AMQPChannel):
    """Channel0 is used to negotiate a connection with RabbitMQ and for
    processing and dispatching events on channel 0 once connected.

    :param dict connection_args: Data required to negotiate the connection
    :param events_obj: The shared events coordination object
    :type events_obj: rabbitpy.events.Events
    :param exception_queue: The queue where any pending exceptions live
    :type exception_queue: queue.Queue
    :param heartbeat_checker: The object that implements heartbeat check logic
    :type heartbeat_checker: rabbitpy.heartbeat.Checker
    :param write_queue: The queue to place data to write in
    :type write_queue: queue.Queue
    :param write_trigger: The socket to write to, to trigger IO writes
    :type write_trigger: socket.socket

    """
    CHANNEL = 0

    CLOSE_REQUEST_FRAME = specification.Connection.Close
    DEFAULT_LOCALE = 'en-US'

    def __init__(self, connection_args, events_obj, exception_queue,
                 heartbeat_checker, write_queue, write_trigger):
        super(Channel0, self).__init__(exception_queue, write_trigger)
        self._channel_id = 0
        self._args = connection_args
        self._events = events_obj
        self._exceptions = exception_queue
        self._read_queue = queue.Queue()
        self._write_queue = write_queue
        self._write_trigger = write_trigger
        self._state = self.CLOSED
        self._max_channels = connection_args['channel_max']
        self._max_frame_size = connection_args['frame_max']
        self._heartbeat_checker = heartbeat_checker
        self._heartbeat_interval = connection_args['heartbeat']
        self.properties = None

    def close(self):
        # Stop the heartbeat timer if it's running
        self._set_state(self.CLOSING)
        self.write_frame(specification.Connection.Close())

    @property
    def heartbeat_interval(self):
        """Return the AMQP heartbeat interval for the connection"""
        return self._heartbeat_interval

    @property
    def maximum_channels(self):
        """Return the AMQP maximum channel count for the connection"""
        return self._max_channels

    @property
    def maximum_frame_size(self):
        """Return the AMQP maximum frame size for the connection"""
        return self._max_frame_size

    def on_frame(self, value):
        """Process a RPC frame received from the server

        :param pamqp.message.Message value: The message value

        """
        LOGGER.debug('Received frame: %r', value.name)
        if value.name == 'Connection.Close':
            LOGGER.warning('RabbitMQ closed the connection (%s): %s',
                           value.reply_code, value.reply_text)
            self._set_state(self.CLOSED)
            self._events.set(events.SOCKET_CLOSE)
            self._events.set(events.CHANNEL0_CLOSED)
            if value.reply_code in exceptions.AMQP:
                err = exceptions.AMQP[value.reply_code](value.reply_text)
            else:
                err = exceptions.RemoteClosedException(value.reply_code,
                                                       value.reply_text)
            self._exceptions.put(err)
            self._trigger_write()
        elif value.name == 'Connection.Blocked':
            LOGGER.warning('RabbitMQ has blocked the connection: %s',
                           value.reason)
            self._events.set(events.CONNECTION_BLOCKED)
        elif value.name == 'Connection.CloseOk':
            self._set_state(self.CLOSED)
            self._events.set(events.CHANNEL0_CLOSED)
        elif value.name == 'Connection.OpenOk':
            self._on_connection_open_ok()
        elif value.name == 'Connection.Start':
            self._on_connection_start(value)
        elif value.name == 'Connection.Tune':
            self._on_connection_tune(value)
        elif value.name == 'Connection.Unblocked':
            LOGGER.info('Connection is no longer blocked')
            self._events.clear(events.CONNECTION_BLOCKED)
        elif value.name == 'Heartbeat':
            if not self._heartbeat_interval:
                LOGGER.warning('Unexpected Heartbeat frame received')
            self._heartbeat_checker.on_heartbeat()
            self.write_frame(heartbeat.Heartbeat())
            self._trigger_write()
        else:
            LOGGER.warning('Unexpected Channel0 Frame: %r', value)
            raise specification.AMQPUnexpectedFrame(value)

    def start(self):
        """Start the AMQP protocol negotiation"""
        self._set_state(self.OPENING)
        self._write_protocol_header()

    def _build_open_frame(self):
        """Build and return the Connection.Open frame.

        :rtype: pamqp.specification.Connection.Open

        """
        return specification.Connection.Open(self._args['virtual_host'])

    def _build_start_ok_frame(self):
        """Build and return the Connection.StartOk frame.

        :rtype: pamqp.specification.Connection.StartOk

        """
        properties = {'product': 'rabbitpy',
                      'platform':
                          'Python {0}.{1}.{2}'.format(*sys.version_info),
                      'capabilities': {'authentication_failure_close': True,
                                       'basic.nack': True,
                                       'connection.blocked': True,
                                       'consumer_cancel_notify': True,
                                       'publisher_confirms': True},
                      'information': 'See https://rabbitpy.readthedocs.io',
                      'version': version.__version__}
        return specification.Connection.StartOk(client_properties=properties,
                                                response=self._credentials,
                                                locale=self._get_locale())

    def _build_tune_ok_frame(self):
        """Build and return the Connection.TuneOk frame.

        :rtype: pamqp.specification.Connection.TuneOk

        """
        return specification.Connection.TuneOk(self._max_channels,
                                               self._max_frame_size,
                                               self._heartbeat_interval)

    @property
    def _credentials(self):
        """Return the marshaled credentials for the AMQP connection.

        :rtype: str

        """
        return '\0%s\0%s' % (self._args['username'], self._args['password'])

    def _get_locale(self):
        """Return the current locale for the python interpreter or the default
        locale.

        :rtype: str

        """
        if not self._args['locale']:
            return DEFAULT_LOCALE[0] or self.DEFAULT_LOCALE
        return self._args['locale']

    @staticmethod
    def _negotiate(client_value, server_value):
        """Return the negotiated value between what the client has requested
        and the server has requested for how the two will communicate.

        :param int client_value:
        :param int server_value:
        :return: int

        """
        return min(client_value, server_value) or \
            (client_value or server_value)

    def _on_connection_open_ok(self):
        LOGGER.debug('Connection opened')
        self._set_state(self.OPEN)
        self._events.set(events.CHANNEL0_OPENED)

    def _on_connection_start(self, frame_value):
        """Negotiate the Connection.Start process, writing out a
        Connection.StartOk frame when the Connection.Start frame is received.

        :type frame_value: pamqp.specification.Connection.Start
        :raises: rabbitpy.exceptions.ConnectionException

        """
        if not self._validate_connection_start(frame_value):
            LOGGER.error('Could not negotiate a connection, disconnecting')
            raise exceptions.ConnectionResetException()

        self.properties = frame_value.server_properties
        for key in self.properties:
            if key == 'capabilities':
                for capability in self.properties[key]:
                    LOGGER.debug('Server supports %s: %r',
                                 capability, self.properties[key][capability])
            else:
                LOGGER.debug('Server %s: %r', key, self.properties[key])
        self.write_frame(self._build_start_ok_frame())

    def _on_connection_tune(self, frame_value):
        """Negotiate the Connection.Tune frames, waiting for the
        Connection.Tune frame from RabbitMQ and sending the Connection.TuneOk
        frame.

        :param specification.Connection.Tune frame_value: Tune frame

        """
        LOGGER.debug('Tuning, client: %r', self._heartbeat_interval)
        self._max_frame_size = self._negotiate(self._max_frame_size,
                                               frame_value.frame_max)
        self._max_channels = self._negotiate(self._max_channels,
                                             frame_value.channel_max)

        # Properly negotiate the heartbeat interval
        if self._heartbeat_interval is None:
            self._heartbeat_interval = frame_value.heartbeat
        elif self._heartbeat_interval == 0 or frame_value.heartbeat == 0:
            self._heartbeat_interval = 0

        self.write_frame(self._build_tune_ok_frame())
        self.write_frame(self._build_open_frame())

    @staticmethod
    def _validate_connection_start(frame_value):
        """Validate the received Connection.Start frame

        :param specification.Connection.Start frame_value: Frame to validate
        :rtype: bool

        """
        if (frame_value.version_major, frame_value.version_minor) != \
                (specification.VERSION[0], specification.VERSION[1]):
            LOGGER.warning('AMQP version error (received %i.%i, expected %r)',
                           frame_value.version_major,
                           frame_value.version_minor,
                           specification.VERSION)
            return False
        return True

    def _write_protocol_header(self):
        """Send the protocol header to the connected server."""
        self.write_frame(header.ProtocolHeader())
