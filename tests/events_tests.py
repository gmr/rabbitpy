"""
Test the rabbitpy events class

"""
import mock
import threading
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from rabbitpy import events


class BaseEventsTest(unittest.TestCase):

    def setUp(self):
        self._events = events.Events()

    def tearDown(self):
        del self._events


class EventClearTests(BaseEventsTest):

    def test_invalid_event(self):
        self.assertIsNone(self._events.clear(0))

    def test_valid_clear_returns_true(self):
        self._events.set(events.CHANNEL0_OPENED)
        self.assertTrue(self._events.clear(events.CHANNEL0_OPENED))

    def test_unset_event_returns_false(self):
        self.assertFalse(self._events.clear(events.CHANNEL0_OPENED))


class EventInitTests(BaseEventsTest):

    def test_all_events_created(self):
        try:
            cls = threading._Event
        except AttributeError:
            cls = threading.Event
        for event in events.DESCRIPTIONS.keys():
            self.assertIsInstance(self._events._events[event], cls,
                                  type(self._events._events[event]))


class EventIsSetTests(BaseEventsTest):

    def test_invalid_event(self):
        self.assertIsNone(self._events.is_set(0))

    def test_valid_is_set_returns_true(self):
        self._events.set(events.CHANNEL0_CLOSED)
        self.assertTrue(self._events.is_set(events.CHANNEL0_CLOSED))

    def test_unset_event_returns_false(self):
        self.assertFalse(self._events.is_set(events.CHANNEL0_OPENED))


class EventSetTests(BaseEventsTest):

    def test_invalid_event(self):
        self.assertIsNone(self._events.set(0))

    def test_valid_set_returns_true(self):
        self.assertTrue(self._events.set(events.CHANNEL0_CLOSED))

    def test_already_set_event_returns_false(self):
        self._events.set(events.CHANNEL0_OPENED)
        self.assertFalse(self._events.set(events.CHANNEL0_OPENED))


class EventWaitTests(BaseEventsTest):

    def test_invalid_event(self):
        self.assertIsNone(self._events.wait(0))

    def test_blocking_wait_returns_true(self):
        try:
            cls = threading._Event
        except AttributeError:
            cls = threading.Event
        with mock.patch.object(cls, 'wait') as mock_method:
            mock_method.return_value = True
            self.assertTrue(self._events.wait(events.CHANNEL0_CLOSED))

    def test_blocking_wait_returns_false(self):
        try:
            cls = threading._Event
        except AttributeError:
            cls = threading.Event
        with mock.patch.object(cls, 'wait') as mock_method:
            mock_method.return_value = False
            self.assertFalse(self._events.wait(events.CHANNEL0_CLOSED, 1))
