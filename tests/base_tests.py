"""
Test the rabbitpy.base classes

"""
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from rabbitpy import base
from rabbitpy import channel

class AMQPClassTests(unittest.TestCase):

    def test_channel_valid(self):
        chan = channel.Channel(1, None, None, None, None, 32768, None)
        obj = base.AMQPClass(chan, 'Foo')
        self.assertEqual(obj.channel, chan)

    def test_channel_invalid(self):
        self.assertRaises(ValueError, base.AMQPClass, 'Foo', 'Bar')

    def test_name_bytes(self):
        chan = channel.Channel(1, None, None, None, None, 32768, None)
        obj = base.AMQPClass(chan, bytes('Foo'))
        self.assertEqual(obj.name, bytes('Foo'))

    def test_name_str(self):
        chan = channel.Channel(1, None, None, None, None, 32768, None)
        obj = base.AMQPClass(chan, str('Foo'))
        self.assertEqual(obj.name, str('Foo'))

    def test_name_unicode(self):
        chan = channel.Channel(1, None, None, None, None, 32768, None)
        obj = base.AMQPClass(chan, unicode('Foo'))
        self.assertEqual(obj.name, unicode('Foo'))

    def test_name_invalid(self):
        chan = channel.Channel(1, None, None, None, None, 32768, None)
        self.assertRaises(ValueError, base.AMQPClass, chan, 1)
