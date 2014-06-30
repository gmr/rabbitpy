"""
Base classes for various parts of rabbitpy

"""
import logging
try:
    import queue
except ImportError:
    import Queue as queue
import socket
from pamqp import specification

from rabbitpy import exceptions
from rabbitpy import utils

LOGGER = logging.getLogger(__name__)


class AMQPClass(object):
    """Base Class object for wrapping the specification.Frame classes

    """
    def __init__(self, channel, name):
        """Create a new ClassObject.

        :param rabbitpy.Channel channel: The channel to execute commands on
        :param str name: Set the name
        :raises: ValueError

        """
        # Use type so there's not a circular dependency
        if channel.__class__.__name__ != 'Channel':
            raise ValueError('channel must be a valid rabbitpy Channel object')
        if not utils.is_string(name):
            raise ValueError('name must be str, bytes or unicode')

        self.channel = channel
        self.name = name

    def _rpc(self, frame_value):
        """Execute the RPC command for the frame.

        :param pamqp.specification.Frame frame_value: The frame to send
        :rtype: pamqp.specification.Frame or pamqp.message.Message

        """
        LOGGER.debug('Issuing RPC to RabbitMQ: %r', frame_value)
        if self.channel.closed:
            raise exceptions.ChannelClosedException()
        return self.channel.rpc(frame_value)

    def _write_frame(self, frame_value):
        """Write a frame to the channel's connection

        :param pamqp.specification.Frame frame_value: The frame to send

        """
        self.channel._write_frame(frame_value)


class StatefulObject(object):
    """Base object for rabbitpy classes that need to maintain state such as
    connection and channel.

    """
    CLOSED = 0x00
    CLOSING = 0x01
    OPEN = 0x02
    OPENING = 0x03

    STATES = {0x00: 'Closed',
              0x01: 'Closing',
              0x02: 'Open',
              0x03: 'Opening'}

    def __init__(self):
        """Create a new instance of the object defaulting to a closed state."""
        self._state = self.CLOSED

    def _set_state(self, value):
        """Set the state to the specified value, validating it is a supported
        state value.

        :param int value: The new state value
        :raises: ValueError
        """
        if value not in list(self.STATES.keys()):
            raise ValueError('Invalid state value: %r' % value)
        LOGGER.debug('%s setting state to %r',
                     self.__class__.__name__, self.STATES[value])
        self._state = value

    @property
    def closed(self):
        """Returns True if in the CLOSED runtime state

        :rtype: bool

        """
        return self._state == self.CLOSED

    @property
    def closing(self):
        """Returns True if in the CLOSING runtime state

        :rtype: bool

        """
        return self._state == self.CLOSING

    @property
    def open(self):
        """Returns True if in the OPEN runtime state

        :rtype: bool

        """
        return self._state == self.OPEN

    @property
    def opening(self):
        """Returns True if in the OPENING runtime state

        :rtype: bool

        """
        return self._state == self.OPENING

    @property
    def state(self):
        """Return the runtime state value

        :rtype: int

        """
        return self._state

    @property
    def state_description(self):
        """Returns the text based description of the runtime state

        :rtype: str

        """
        return self.STATES[self._state]


class AMQPChannel(StatefulObject):

    CLOSE_REQUEST_FRAME = specification.Channel.Close
    DEFAULT_CLOSE_CODE = 200
    DEFAULT_CLOSE_REASON = 'Normal Shutdown'

    def __init__(self, exception_queue, write_trigger, blocking_read=False):
        super(AMQPChannel, self).__init__()
        self.blocking_read = blocking_read
        self._channel_id = None
        self._exceptions = exception_queue
        self._state = self.CLOSED
        self._read_queue = None
        self._write_queue = None
        self._write_trigger = write_trigger

    def __int__(self):
        return self._channel_id

    def close(self):
        if self.closed:
            LOGGER.debug('AMQPChannel %i close invoked and already closed',
                         self._channel_id)
            return
        LOGGER.debug('Channel %i close invoked while %s',
                     self._channel_id, self.state_description)
        if not self.closing:
            self._set_state(self.CLOSING)
        frame_value = self._build_close_frame()
        LOGGER.debug('Channel %i Waiting for a valid response for %s',
                     self._channel_id, frame_value.name)
        self.rpc(frame_value)
        self._set_state(self.CLOSED)
        LOGGER.debug('Channel #%i closed', self._channel_id)

    def rpc(self, frame_value):
        """Send a RPC command to the remote server.

        :param pamqp.specification.Frame frame_value: The frame to send
        :rtype: pamqp.specification.Frame or None

        """
        if self.closed:
            raise exceptions.ChannelClosedException()
        self._write_frame(frame_value)
        if frame_value.synchronous:
            return self._wait_on_frame(frame_value.valid_responses)

    def _build_close_frame(self):
        """Return the proper close frame for this object.

        :rtype: pamqp.specification.Channel.Close

        """
        return self.CLOSE_REQUEST_FRAME(self.DEFAULT_CLOSE_CODE,
                                        self.DEFAULT_CLOSE_REASON)

    def _check_for_exceptions(self):
        """Check if there are any queued exceptions to raise, raising it if
        there is.

        """
        if not self._exceptions.empty():
            exception = self._exceptions.get()
            self._exceptions.task_done()
            raise exception

    def _check_for_rpc_request(self, value):
        """Implement in child objects to inspect frames for channel specific
        RPC requests from RabbitMQ.

        """
        pass

    def _force_close(self):
        """Force the channel to mark itself as closed"""
        self._set_state(self.CLOSED)
        LOGGER.debug('Channel #%i closed', self._channel_id)

    def _read_from_queue(self):
        """Check to see if a frame is in the queue and if so, return it

        :rtype: amqp.specification.Frame or None

        """
        if self.blocking_read:
            value = self._read_queue.get()
            self._read_queue.task_done()
        else:
            try:
                value = self._read_queue.get(True, .5)
                self._read_queue.task_done()
            except queue.Empty:
                value = None
        return value

    def _trigger_write(self):
        """Notifies the IO loop we need to write a frame by writing a byte
        to a local socket.

        """
        try:
            self._write_trigger.send(b'0')
        except socket.error:
            pass

    def _validate_frame_type(self, frame_value, frame_type):
        """Validate the frame value against the frame type. The frame type can
        be an individual frame type or a list of frame types.

        :param pamqp.specification.Frame frame_value: The frame to check
        :param frame_type: The frame(s) to check against
        :type frame_type: pamqp.specification.Frame or list
        :rtype: bool

        """
        if frame_value is None:
            LOGGER.debug('Frame value is none?')
            return False
        if isinstance(frame_type, str):
            if frame_value.name == frame_type:
                return True
        elif isinstance(frame_type, list):
            for frame_t in frame_type:
                result = self._validate_frame_type(frame_value, frame_t)
                if result:
                    return True
            return False
        elif isinstance(frame_value, specification.Frame):
            return frame_value.name == frame_type.name
        return False

    def _wait_on_frame(self, frame_type=None):
        """Read from the queue, blocking until a result is returned. An
        individual frame type or a list of frame types can be passed in to wait
        for specific frame types. If there is no match on the frame retrieved
        from the queue, put the frame back in the queue and recursively
        call the method.

        :param frame_type: The name or list of names of the frame type(s)
        :type frame_type: str or list or pamqp.specification.Frame
        :rtype: Frame

        """
        if isinstance(frame_type, list) and len(frame_type) == 1:
            frame_type = frame_type[0]
        start_state = self.state
        while not self.closed and start_state == self.state:
            value = self._read_from_queue()
            if value is not None:
                self._check_for_rpc_request(value)
                if frame_type and self._validate_frame_type(value, frame_type):
                    return value
                self._read_queue.put(value)
            self._check_for_exceptions()

    def _write_frame(self, frame):
        """Put the frame in the write queue for the IOWriter object to write to
        the socket when it can.

        :param pamqp.specification.Frame frame: The frame to write

        """
        if self.closed:
            return
        self._check_for_exceptions()
        self._write_queue.put((self._channel_id, frame))
        self._trigger_write()
