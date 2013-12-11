"""
Channel0 is used for connection level communication between RabbitMQ and the
client on channel 0.

"""
import locale
import logging
try:
    import queue
except ImportError:
    import Queue as queue
import sys
import threading

from pamqp import header
from pamqp import heartbeat
from pamqp import specification

from rabbitpy import __version__
from rabbitpy import base
from rabbitpy import events
from rabbitpy import exceptions

LOGGER = logging.getLogger(__name__)


class Channel0(threading.Thread, base.AMQPChannel):
    """Channel0 is used to negotiate a connection with RabbitMQ and for
    processing and dispatching events on channel 0 once connected.

    :param dict connection_args: Data required to negotiate the connection
    :param Queue.Queue inbound: Inbound frame queue
    :param Queue.Queue outbound: Outbound frame queue

    """
    CHANNEL = 0

    CLOSE_REQUEST_FRAME = specification.Connection.Close
    DEFAULT_LOCALE = 'en-US'

    def __init__(self, group=None, target=None, name=None,
                 args=None, kwargs=None):
        super(Channel0, self).__init__(group, target, name,
                                       args or (), kwargs or {})
        self._channel_id = 0
        self._args = kwargs['connection_args']
        self._events = kwargs['events']
        self._exceptions = kwargs['exceptions']
        self._read_queue = kwargs['inbound']
        self._write_queue = kwargs['outbound']
        self._write_trigger = kwargs['write_trigger']
        self._heartbeat = self._args['heartbeat']
        self._maximum_channels = 0
        self._state = self.CLOSED
        self.maximum_frame_size = specification.FRAME_MAX_SIZE
        self.minimum_frame_size = specification.FRAME_MIN_SIZE
        self.properties = None

    @property
    def maximum_channels(self):
        return self._maximum_channels

    def run(self):
        try:
            self._run()
        except Exception as exception:
            self._exceptions.put(exception)
            if self.open:
                self.close()
            self._events.set(events.EXCEPTION_RAISED)

    def _run(self):
        self._set_state(self.OPENING)
        self._write_protocol_header()

        # If we can negotiate a connection, do so
        if self._connection_start():
            self._connection_tune()
            self._connection_open()

            # Loop as long as the connection is open, waiting for RPC requests
            while self.open and not self._events.is_set(events.CHANNEL0_CLOSE):
                if self._events.is_set(events.EXCEPTION_RAISED):
                    LOGGER.debug('exiting main loop due to exceptions')
                    sys.exit(1)
                try:
                    self._process_server_rpc(self._read_queue.get(True, 0.1))
                except queue.Empty:
                    pass

        # Close out Channel0
        if not self.closing and not self.closed:
            self.close()

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
        properties = {'product': 'rabbitpy',
                      'platform': 'Python %s.%s.%s' % (version[0],
                                                       version[1],
                                                       version[2]),
                      'capabilities': {'basic.nack': True,
                                       'connection.blocked': True,
                                       'consumer_cancel_notify': True,
                                       'publisher_confirms': True},
                      'information': 'See http://rabbitpy.readthedocs.org',
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

    def _connection_open(self):
        """Open the connection, sending the Connection.Open frame. If a
        connection.OpenOk is received, set the state and notify the connection.

        """
        self._write_frame(self._build_open_frame())
        if self._wait_on_frame(specification.Connection.OpenOk):
            self._set_state(self.OPEN)
            self._events.set(events.CHANNEL0_OPENED)

    def _connection_start(self):
        """Negotiate the Connection.Start process, writing out a
        Connection.StartOk frame when the Connection.Start frame is received.

        :rtype: bool

        """
        frame_value = self._wait_on_frame(specification.Connection.Start)
        if not self._validate_connection_start(frame_value):
            LOGGER.error('Could not negotiate a connection, disconnecting')
            return False
        self.properties = frame_value.server_properties
        for key in self.properties:
            if key == 'capabilities':
                for capability in self.properties[key]:
                    LOGGER.debug('Server supports %s: %r',
                                 capability, self.properties[key][capability])
            else:
                LOGGER.debug('Server %s: %r', key, self.properties[key])
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
            if self._heartbeat is None:
                self._heartbeat = frame_value.heartbeat
            elif self._heartbeat > frame_value.heartbeat:
                self._heartbeat = frame_value.heartbeat
        self._write_frame(self._build_tune_ok_frame())

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
            return locale.getdefaultlocale()[0] or self.DEFAULT_LOCALE
        return self._args['locale']

    def _process_server_rpc(self, value):
        """Process a RPC frame received from the server

        :param pamqp.message.Message value: The message value

        """
        if value.name == 'Connection.Close':
            LOGGER.warning('RabbitMQ closed the connection (%s): %s',
                           value.reply_code, value.reply_text)
            self._set_state(self.CLOSED)
            self._events.set(events.SOCKET_CLOSE)
            exception = exceptions.RemoteClosedException(value.reply_code,
                                                         value.reply_text)
            self._exceptions.put(exception)
        elif value.name == 'Heartbeat':
            LOGGER.debug('Received Heartbeat, sending one back')
            self._write_frame(heartbeat.Heartbeat())
        elif value.name == 'Connection.Blocked':
            LOGGER.warning('RabbitMQ has blocked the connection: %s',
                           value.reason)
            self._events.set(events.CONNECTION_BLOCKED)
        elif value.name == 'Connection.Unblocked':
            LOGGER.info('Connection is no longer blocked')
            self._events.clear(events.CONNECTION_BLOCKED)
        else:
            LOGGER.critical('Unhandled RPC request: %r', value)

    @staticmethod
    def _validate_connection_start(frame_value):
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

    def _write_protocol_header(self):
        """Send the protocol header to the connected server."""
        self._write_frame(header.ProtocolHeader())
