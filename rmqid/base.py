"""
Base rmqid classes

"""
import logging

LOGGER = logging.getLogger(__name__)

class AMQPClass(object):
    """Base Class object for wrapping the specification.Frame classes

    """
    def __init__(self, channel, name):
        """Create a new ClassObject.

        :param rmqid.Channel channel: The channel to execute commands on
        :param str name: Set the name

        """
        self.name = name
        self.channel = channel

    def rpc(self, frame_value):
        """Execute the RPC command for the frame.

        :param pamqp.specification.Frame frame_value: The frame to send
        :rtype: pamqp.specification.Frame | pamqp.message.Message

        """
        return self.channel.rpc(frame_value)


class StatefulObject(object):
    """Base object for rmqid classes that need to maintain state such as
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
        if value not in self.STATES.keys():
            raise ValueError('Invalid state value: %r', value)
        LOGGER.debug('Setting state to %r', self.STATES[value])
        self._state = value

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

    @property
    def open(self):
        """Returns True if in the OPEN runtime state

        :rtype: bool

        """
        return self._state == self.OPEN

    @property
    def closed(self):
        """Returns True if in the CLOSED runtime state

        :rtype: bool

        """
        return self._state == self.CLOSED
