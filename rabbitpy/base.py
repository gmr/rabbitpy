"""
Base classes for various parts of rabbitpy

"""
import logging
try:
    import queue
except ImportError:
    import Queue as queue

from pamqp import specification

from rabbitpy import exceptions

LOGGER = logging.getLogger(__name__)


class AMQPClass(object):
    """Base Class object for wrapping the specification.Frame classes

    """
    def __init__(self, channel, name):
        """Create a new ClassObject.

        :param rabbitpy.Channel channel: The channel to execute commands on
        :param str name: Set the name

        """
        self.name = name
        self.channel = channel

    def _rpc(self, frame_value):
        """Execute the RPC command for the frame.

        :param pamqp.specification.Frame frame_value: The frame to send
        :rtype: pamqp.specification.Frame | pamqp.message.Message

        """
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
        LOGGER.debug('%s setting state to %r', self.__class__.__name__,
                     self.STATES[value])
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

    def __init__(self, exceptions):
        super(AMQPChannel, self).__init__()
        self._channel_id = None
        self._exceptions = exceptions
        self._state = self.CLOSED
        self._read_queue = None
        self._write_queue = None

    def __int__(self):
        return self._channel_id

    def _validate_frame_type(self, frame_value, frame_type):
        if isinstance(frame_type, basestring):
            if frame_value.name == frame_type:
                return True
        elif isinstance(frame_type, list):
            for frame in frame_type:
                result = self._validate_frame_type(frame_value, frame)
                if result:
                    return True
            return False
        elif isinstance(frame_value, frame_type):
            return True
        else:
            raise ValueError('Unknown frame type to wait for: %r', frame_type)

    def _wait_on_frame(self, frame_type=None):
        """Read from the queue, blocking until a result is returned. An
        individual frame type or a list of frame types can be passed in to wait
        for specific frame types. If there is no match on the frame retrieved
        from the queue, put the frame back in the queue and recursively
        call the method.

        :param frame_type: The name or list of names of the frame type(s)
        :type frame_type:  str|list
        :rtype: Frame

        """
        if frame_type:
            value = self._read_from_queue()
            if self._validate_frame_type(value, frame_type):
                return value

            # Put the frame at the end of the queue and call this method again
            self._read_queue.put(value)
            return self._wait_on_frame(frame_type)
        return self._read_from_queue()

    def _read_from_queue(self):
        while self.opening or self.open or self.closing:
            try:
                return self._read_queue.get(True, 0.01)
            except queue.Empty:
                pass
            if not self._exceptions.empty():
                exception = self._exceptions.get()
                raise exception

    def _write_frame(self, frame):
        self._write_queue.put((self._channel_id, frame))