"""
Common rabbitpy events

"""
import logging
import threading

LOGGER = logging.getLogger(__name__)

CHANNEL0_CLOSE = 0x01
CHANNEL0_CLOSED = 0x02
CHANNEL0_OPENED = 0x03
CONNECTION_BLOCKED = 0x04
CONNECTION_EVENT = 0x05
EXCEPTION_RAISED = 0x06
SOCKET_CLOSE = 0x07
SOCKET_CLOSED = 0x08
SOCKET_OPENED = 0x09

DESCRIPTIONS = {0x01: 'Channel 0 Close Requested',
                0x02: 'Channel 0 Closed',
                0x03: 'Channel 0 Opened',
                0x04: 'Connection is blocked',
                0x05: 'Connection Event Occurred',
                0x06: 'Exception Raised',
                0x07: 'Socket Close Requested',
                0x08: 'Socket Closed',
                0x09: 'Socket Connected'}


def description(event_id):
    """Return the text description for an event"""
    return DESCRIPTIONS.get(event_id, event_id)


class Events(object):
    """All events that get triggered in rabbitpy are funneled through this
    object for a common structure and method for raising and checking for them.

    """
    def __init__(self):
        """Create a new instance of Events"""
        self._events = self._create_event_objects()

    @staticmethod
    def _create_event_objects():
        """Events are used like signals across threads for communicating state
        changes, used by the various threaded objects to communicate with each
        other when an action needs to be taken.

        :rtype: dict

        """
        events = dict()
        for event in [CHANNEL0_CLOSE,
                      CHANNEL0_CLOSED,
                      CHANNEL0_OPENED,
                      CONNECTION_BLOCKED,
                      CONNECTION_EVENT,
                      EXCEPTION_RAISED,
                      SOCKET_CLOSE,
                      SOCKET_CLOSED,
                      SOCKET_OPENED]:
            events[event] = threading.Event()
        return events

    def clear(self, event_id):
        """Clear a set event, returning bool indicating success and None for
        an invalid event.

        :param int event_id: The event to set
        :rtype: bool

        """
        if event_id not in self._events:
            LOGGER.debug('Event does not exist: %s', description(event_id))
            return None

        if not self.is_set(event_id):
            LOGGER.debug('Event is not set: %s', description(event_id))
            return False

        self._events[event_id].clear()
        return True

    def is_set(self, event_id):
        """Check if an event is triggered. Returns bool indicating state of the
        event being set. If the event is invalid, a None is returned instead.

        :param int event_id: The event to fire
        :rtype: bool

        """
        if event_id not in self._events:
            LOGGER.debug('Event does not exist: %s', description(event_id))
            return None
        return self._events[event_id].is_set()

    def set(self, event_id):
        """Trigger an event to fire. Returns bool indicating success in firing
        the event. If the event is not valid, return None.

        :param int event_id: The event to fire
        :rtype: bool

        """
        if event_id not in self._events:
            LOGGER.debug('Event does not exist: %s', description(event_id))
            return None

        if self.is_set(event_id):
            LOGGER.debug('Event is already set: %s', description(event_id))
            return False

        self._events[event_id].set()
        return True

    def wait(self, event_id, timeout=1):
        """Wait for an event to be set for up to `timeout` seconds. If
        `timeout` is None, block until the event is set. If the event is
        invalid, None will be returned, otherwise False is used to indicate
        the event is still not set when using a timeout.

        :param int event_id: The event to wait for
        :param float timeout: The number of seconds to wait

        """
        if event_id not in self._events:
            LOGGER.debug('Event does not exist: %s', description(event_id))
            return None
        LOGGER.debug('Waiting for %i seconds on event: %s',
                     timeout, description(event_id))
        return self._events[event_id].wait(timeout)
