"""
Test the rabbitpy.base classes

"""
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from rabbitpy import base
from rabbitpy import channel
from rabbitpy import utils


class AMQPClassTests(unittest.TestCase):

    def setUp(self):
        self.chan = channel.Channel(1, None, None, None, None, 32768, None)

    def test_channel_valid(self):
        obj = base.AMQPClass(self.chan, 'Foo')
        self.assertEqual(obj.channel, self.chan)

    def test_channel_invalid(self):
        self.assertRaises(ValueError, base.AMQPClass, 'Foo', 'Bar')

    def test_name_bytes(self):
        obj = base.AMQPClass(self.chan, b'Foo')
        self.assertIsInstance(obj.name, bytes)

    def test_name_str(self):
        obj = base.AMQPClass(self.chan, 'Foo')
        self.assertIsInstance(obj.name, bytes)

    @unittest.skipIf(utils.PYTHON3, 'No unicode in Python 3')
    def test_name_unicode(self):
        obj = base.AMQPClass(self.chan, unicode('Foo'))
        self.assertIsInstance(obj.name, unicode)

    def test_name_value(self):
        obj = base.AMQPClass(self.chan, 'Foo')
        self.assertEqual(obj.name, 'Foo')

    def test_name_invalid(self):
        self.assertRaises(ValueError, base.AMQPClass, self.chan, 1)
