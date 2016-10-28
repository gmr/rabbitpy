try:
    import unittest2 as unittest
except ImportError:
    import unittest

import mock

from rabbitpy import channel, connection, events


class TestCase(unittest.TestCase):

    def setUp(self):
        self.connection = mock.MagicMock('rabbitpy.connection.Connection')
        self.connection._io = mock.Mock()
        self.connection._io.write_trigger = mock.Mock('socket.socket')
        self.connection._io.write_trigger.send = mock.Mock()
        self.connection._channel0 = mock.Mock()
        self.connection._channel0.properties = {}
        self.connection._events = events.Events()
        self.connection._exceptions = connection.queue.Queue()
        self.connection.open = True
        self.connection.closed = False
        self.channel = channel.Channel(1, {},
                                       self.connection._events,
                                       self.connection._exceptions,
                                       connection.queue.Queue(),
                                       connection.queue.Queue(), 32768,
                                       self.connection._io.write_trigger,
                                       connection=self.connection)
        self.channel._set_state(self.channel.OPEN)