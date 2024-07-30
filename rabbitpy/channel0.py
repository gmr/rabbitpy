"""
Channel0 is used for connection level communication between RabbitMQ and the
client on channel 0.

"""
import locale
import logging
import queue
import socket
import sys
import typing

from pamqp import base as pamqp_base, commands, header, heartbeat

from rabbitpy import __version__, connection as conn, base, events, exceptions

LOGGER = logging.getLogger(__name__)
DEFAULT_LOCALE = locale.getdefaultlocale()
del locale


class Channel0(base.AMQPChannel):
    """Channel0 is used to negotiate a connection with RabbitMQ and for
    processing and dispatching events on channel 0 once connected.

    :param connection_args: Data required to negotiate the connection
    :param events_obj: The shared events coordination object
    :param exception_queue: The queue where any pending exceptions live
    :param write_queue: The queue to place data to write in
    :param write_trigger: The socket to write to, to trigger IO writes

    """
    CHANNEL = 0

    CLOSE_REQUEST_FRAME = commands.Connection.Close
    DEFAULT_LOCALE = 'en-US'

    def __init__(self,
                 connection_args: dict,
                 events_obj: events.Events,
                 exception_queue: queue.Queue,
                 write_queue: queue.Queue,
                 write_trigger: socket.socket,
                 connection: conn.Connection):
        super(Channel0, self).__init__(exception_queue, write_trigger,
                                       connection)
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
        self._heartbeat_interval = connection_args['heartbeat']
        self.properties: typing.Optional[dict] = None

    def close(self):
        """Close the connection via Channel0 communication."""
        if self.open:
            self._set_state(self.CLOSING)
            self.rpc(commands.Connection.Close())

    @property
    def heartbeat_interval(self):
        """Return the AMQP heartbeat interval for the connection

        :rtype: int

        """
        return self._heartbeat_interval

    @property
    def maximum_channels(self):
        """Return the AMQP maximum channel count for the connection

        :rtype: int

        """
        return self._max_channels

    @property
    def maximum_frame_size(self):
        """Return the AMQP maximum frame size for the connection

        :rtype: int

        """
        return self._max_frame_size

    def on_frame(self, value: typing.Union[commands.Connection.Blocked,
                                           commands.Connection.Close,
                                           commands.Connection.Start,
                                           commands.Connection.Tune,
                                           pamqp_base.Frame]) -> None:
        """Process an RPC frame received from the server"""
        LOGGER.debug('Received frame: %r', value.name)
        if value.name == 'Connection.Close':
            LOGGER.warning('RabbitMQ closed the connection (%s): %s',
                           value.reply_code, value.reply_text)
            self._set_state(self.CLOSED)
            self._events.set(events.SOCKET_CLOSED)
            self._events.set(events.CHANNEL0_CLOSED)
            self._connection.close()
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
            pass
        else:
            LOGGER.warning('Unexpected Channel0 Frame: %r', value)
            raise commands.AMQPUnexpectedFrame(value)

    def send_heartbeat(self) -> None:
        """Send a heartbeat frame to the remote connection."""
        self.write_frame(heartbeat.Heartbeat())

    def start(self) -> None:
        """Start the AMQP protocol negotiation"""
        self._set_state(self.OPENING)
        self._write_protocol_header()

    def _build_open_frame(self) -> commands.Connection.Open:
        """Build and return the Connection.Open frame."""
        return commands.Connection.Open(self._args['virtual_host'])

    def _build_start_ok_frame(self) -> commands.Connection.StartOk:
        """Build and return the Connection.StartOk frame."""
        properties = {
            'product': 'rabbitpy',
            'platform': 'Python {0}.{1}.{2}'.format(*sys.version_info),
            'capabilities': {
                'authentication_failure_close': True,
                'basic.nack': True,
                'connection.blocked': True,
                'consumer_cancel_notify': True,
                'publisher_confirms': True
            },
            'information': 'See https://rabbitpy.readthedocs.io',
            'version': __version__
        }
        return commands.Connection.StartOk(
            client_properties=properties,
            response=self._credentials,
            locale=self._get_locale())

    def _build_tune_ok_frame(self) -> commands.Connection.TuneOk:
        """Build and return the Connection.TuneOk frame."""
        return commands.Connection.TuneOk(
            self._max_channels, self._max_frame_size, self._heartbeat_interval)

    @property
    def _credentials(self) -> str:
        """Return the marshaled credentials for the AMQP connection."""
        return '\0%s\0%s' % (self._args['username'], self._args['password'])

    def _get_locale(self) -> str:
        """Return the current locale for the python interpreter or the default
        locale.

        """
        if not self._args['locale']:
            return DEFAULT_LOCALE[0] or self.DEFAULT_LOCALE
        return self._args['locale']

    @staticmethod
    def _negotiate(client_value: int, server_value: int) -> int:
        """Return the negotiated value between what the client has requested
        and the server has requested for how the two will communicate.

        :param client_value:
        :param server_value:

        """
        return (min(client_value, server_value) or
                (client_value or server_value))

    def _on_connection_open_ok(self) -> None:
        LOGGER.debug('Connection opened')
        self._set_state(self.OPEN)
        self._events.set(events.CHANNEL0_OPENED)

    def _on_connection_start(self, value: commands.Connection.Start) -> None:
        """Negotiate the Connection.Start process, writing out a
        Connection.StartOk frame when the Connection.Start frame is received.

        :raises: rabbitpy.exceptions.ConnectionException

        """
        if not self._validate_connection_start(value):
            LOGGER.error('Could not negotiate a connection, disconnecting')
            raise exceptions.ConnectionResetException()

        self.properties = value.server_properties
        for key in self.properties:
            if key == 'capabilities':
                for capability in self.properties[key]:
                    LOGGER.debug('Server supports %s: %r', capability,
                                 self.properties[key][capability])
            else:
                LOGGER.debug('Server %s: %r', key, self.properties[key])
        self.write_frame(self._build_start_ok_frame())

    def _on_connection_tune(self, value: commands.Connection.Tune) -> None:
        """Negotiate the Connection.Tune frames, waiting for the
        Connection.Tune frame from RabbitMQ and sending the Connection.TuneOk
        frame.

        """
        self._max_frame_size = self._negotiate(
            self._max_frame_size, value.frame_max)
        self._max_channels = self._negotiate(
            self._max_channels, value.channel_max)

        LOGGER.debug('Heartbeat interval (server/client): %r/%r',
                     value.heartbeat, self._heartbeat_interval)

        # Properly negotiate the heartbeat interval
        if self._heartbeat_interval is None:
            self._heartbeat_interval = value.heartbeat
        elif self._heartbeat_interval == 0 or value.heartbeat == 0:
            self._heartbeat_interval = 0

        self.write_frame(self._build_tune_ok_frame())
        self.write_frame(self._build_open_frame())

    @staticmethod
    def _validate_connection_start(value: commands.Connection.Start) -> bool:
        """Validate the received Connection.Start frame

        :param value: Frame to validate
        :rtype: bool

        """
        if (value.version_major, value.version_minor) != \
                (commands.VERSION[0], commands.VERSION[1]):
            LOGGER.warning('AMQP version error (received %i.%i, expected %r)',
                           value.version_major,
                           value.version_minor, commands.VERSION)
            return False
        return True

    def _write_protocol_header(self) -> None:
        """Send the protocol header to the connected server."""
        self.write_frame(header.ProtocolHeader())
