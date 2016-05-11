"""
Base classes for various parts of rabbitpy

"""
import logging
import threading

from pamqp import specification

from rabbitpy import exceptions
from rabbitpy import utils
from rabbitpy.utils import queue


LOGGER = logging.getLogger(__name__)


class ChannelWriter(object):  # pylint: disable=too-few-public-methods

    """The AMQP Adapter provides a more generic, non-opinionated interface to
    RabbitMQ by providing methods that map to the AMQP API.

    :param channel: The channel to use
    :type channel: rabbitpy.channel.Channel

    """
    def __init__(self, channel):
        self.channel = channel

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
        self.channel.write_frame(frame_value)


class AMQPClass(ChannelWriter):  # pylint: disable=too-few-public-methods
    """Base Class object AMQP object classes"""
    def __init__(self, channel, name):
        """Create a new ClassObject.

        :param channel: The channel to execute commands on
        :type channel: rabbitpy.Channel
        :param str name: Set the name
        :raises: ValueError

        """
        super(AMQPClass, self).__init__(channel)
        # Use type so there's not a circular dependency
        if channel.__class__.__name__ != 'Channel':
            raise ValueError('channel must be a valid rabbitpy Channel object')
        elif not utils.is_string(name):
            raise ValueError('name must be str, bytes or unicode')
        self.name = name


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
    """Base AMQP Channel Object"""

    CLOSE_REQUEST_FRAME = specification.Channel.Close
    DEFAULT_CLOSE_CODE = 200
    DEFAULT_CLOSE_REASON = 'Normal Shutdown'
    REMOTE_CLOSED = 0x04

    def __init__(self, exception_queue, write_trigger, blocking_read=False):
        super(AMQPChannel, self).__init__()
        if blocking_read:
            LOGGER.debug('Initialized with blocking read')
        self.blocking_read = blocking_read
        self._debugging = None
        self._interrupt = {'event': threading.Event(),
                           'callback': None,
                           'args': None}
        self._channel_id = None
        self._exceptions = exception_queue
        self._state = self.CLOSED
        self._read_queue = None
        self._waiting = False
        self._write_lock = threading.Lock()
        self._write_queue = None
        self._write_trigger = write_trigger

    def __int__(self):
        return self._channel_id

    def close(self):
        """Close the AMQP channel"""
        if self.closed:
            if self._is_debugging:
                LOGGER.debug('AMQPChannel %i close invoked and already closed',
                             self._channel_id)
            return
        if self._is_debugging:
            LOGGER.debug('Channel %i close invoked while %s',
                         self._channel_id, self.state_description)

        # Make sure there are no RPC frames pending
        self._check_for_pending_frames()

        if not self.closing:
            self._set_state(self.CLOSING)
        frame_value = self._build_close_frame()
        if self._is_debugging:
            LOGGER.debug('Channel %i Waiting for a valid response for %s',
                         self._channel_id, frame_value.name)
        self.rpc(frame_value)
        self._set_state(self.CLOSED)
        if self._is_debugging:
            LOGGER.debug('Channel #%i closed', self._channel_id)

    def rpc(self, frame_value):
        """Send a RPC command to the remote server. This should not be directly
        invoked.

        :param pamqp.specification.Frame frame_value: The frame to send
        :rtype: pamqp.specification.Frame or None

        """
        if self.closed:
            raise exceptions.ChannelClosedException()
        if self._is_debugging:
            LOGGER.debug('Sending %r', frame_value.name)
        self.write_frame(frame_value)
        if frame_value.synchronous:
            return self._wait_on_frame(frame_value.valid_responses)

    def wait_for_confirmation(self):
        """Used by the Message.publish method when publisher confirmations are
        enabled.

        :rtype: pamqp.frame.Frame

        """
        return self._wait_on_frame([specification.Basic.Ack,
                                    specification.Basic.Nack])

    def write_frame(self, frame):
        """Put the frame in the write queue for the IOWriter object to write to
        the socket when it can. This should not be directly invoked.

        :param pamqp.specification.Frame frame: The frame to write

        """
        if self.closed:
            return
        self._check_for_exceptions()
        if self._is_debugging:
            LOGGER.debug('Writing frame: %s', frame.name)
        with self._write_lock:
            self._write_queue.put((self._channel_id, frame))
        self._trigger_write()

    def write_frames(self, frames):
        """Add a list of frames for the IOWriter object to write to the socket
        when it can.

        :param list frames: The list of frame to write

        """
        if self.closed:
            return
        self._check_for_exceptions()
        if self._is_debugging:
            LOGGER.debug('Writing frames: %r',
                         [frame.name for frame in frames])
        with self._write_lock:
            # pylint: disable=expression-not-assigned
            [self._write_queue.put((self._channel_id, frame))
             for frame in frames]
        self._trigger_write()

    def _build_close_frame(self):
        """Return the proper close frame for this object.

        :rtype: pamqp.specification.Channel.Close

        """
        return self.CLOSE_REQUEST_FRAME(self.DEFAULT_CLOSE_CODE,
                                        self.DEFAULT_CLOSE_REASON)

    @property
    def _is_debugging(self):
        """Indicates that something has set the logger to ``logging.DEBUG``
        to perform a minor micro-optimization preventing ``LOGGER.debug`` calls
        when they are not required.

        :return: bool

        """
        if self._debugging is None:
            self._debugging = LOGGER.getEffectiveLevel() == logging.DEBUG
        return self._debugging

    def _check_for_exceptions(self):
        """Check if there are any queued exceptions to raise, raising it if
        there is.

        """
        if not self._exceptions.empty():
            exception = self._exceptions.get()
            self._exceptions.task_done()
            raise exception

    def _check_for_pending_frames(self):
        value = self._read_from_queue()
        if value:
            self._check_for_rpc_request(value)
            LOGGER.debug('Read frame while shutting down: %r', value)

    def _check_for_rpc_request(self, value):
        """Implement in child objects to inspect frames for channel specific
        RPC requests from RabbitMQ.

        """
        if isinstance(value, specification.Channel.Close):
            LOGGER.debug('Channel closed')
            self._on_remote_close(value)

    def _force_close(self):
        """Force the channel to mark itself as closed"""
        self._set_state(self.CLOSED)
        LOGGER.debug('Channel #%i closed', self._channel_id)

    def _interrupt_wait_on_frame(self, callback, *args):
        """Invoke to interrupt the current self._wait_on_frame blocking loop
        in order to allow for a flow such as waiting on a full message while
        consuming. Will wait until the ``_wait_on_frame_interrupt`` is cleared
        to make this a blocking operation.

        :param callback: The method to call
        :type callback: typing.Callable
        :param list args: Args to pass to the callback

        """
        if not self._waiting:
            if self._is_debugging:
                LOGGER.debug('No need to interrupt wait')
            return callback(*args)
        LOGGER.debug('Interrupting the wait on frame')
        self._interrupt['callback'] = callback
        self._interrupt['args'] = args
        self._interrupt['event'].set()

    @property
    def _interrupt_is_set(self):
        return self._interrupt['event'].is_set()

    def _on_interrupt_set(self):
        # pylint: disable=not-an-iterable,not-callable
        self._interrupt['callback'](*self._interrupt['args'])
        self._interrupt['event'].clear()
        self._interrupt['callback'] = None
        self._interrupt['args'] = None

    def _on_remote_close(self, value):
        """
        Handle RabbitMQ remotely closing the channel

        :param value: The Channel.Close method frame
        :type value: pamqp.spec.Channel.Close
        :raises: exceptions.RemoteClosedChannelException
        :raises: exceptions.AMQPException

        """
        self._set_state(self.REMOTE_CLOSED)
        if value.reply_code in exceptions.AMQP:
            LOGGER.error('Received remote close (%s): %s',
                         value.reply_code, value.reply_text)
            raise exceptions.AMQP[value.reply_code](value)
        else:
            raise exceptions.RemoteClosedChannelException(self._channel_id,
                                                          value.reply_code,
                                                          value.reply_text)

    def _read_from_queue(self):
        """Check to see if a frame is in the queue and if so, return it

        :rtype: amqp.specification.Frame or None

        """
        if not self.closing and self.blocking_read:
            LOGGER.debug('Performing a blocking read')
            value = self._read_queue.get()
            self._read_queue.task_done()
        else:
            try:
                value = self._read_queue.get(True, .1)
                self._read_queue.task_done()
            except queue.Empty:
                value = None
        return value

    def _trigger_write(self):
        """Notifies the IO loop we need to write a frame by writing a byte
        to a local socket.

        """
        utils.trigger_write(self._write_trigger)

    def _validate_frame_type(self, frame_value, frame_type):
        """Validate the frame value against the frame type. The frame type can
        be an individual frame type or a list of frame types.

        :param frame_value: The frame to check
        :type frame_value: pamqp.specification.Frame
        :param frame_type: The frame(s) to check against
        :type frame_type: pamqp.specification.Frame or list
        :rtype: bool

        """
        if frame_value is None:
            if self._is_debugging:
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
        :type frame_type: str|list|pamqp.specification.Frame
        :rtype: Frame

        """
        if isinstance(frame_type, list) and len(frame_type) == 1:
            frame_type = frame_type[0]
        if self._is_debugging:
            LOGGER.debug('Waiting on %r frame(s)', frame_type)
        start_state = self.state
        self._waiting = True
        while not self.closed and start_state == self.state:
            value = self._read_from_queue()
            if value is not None:
                self._check_for_rpc_request(value)
                if frame_type and self._validate_frame_type(value, frame_type):
                    self._waiting = False
                    return value
                self._read_queue.put(value)

            try:
                self._check_for_exceptions()
            except:
                self._waiting = False
                raise

            # If the wait interrupt is set, break out of the loop
            if self._interrupt_is_set:
                if self._is_debugging:
                    LOGGER.debug('Exiting wait due to interrupt')
                break

        self._waiting = False

        # Clear here to ensure out of processing loop before proceeding
        if self._interrupt_is_set:
            self._on_interrupt_set()
