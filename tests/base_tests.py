"""
Test the rabbitpy.base classes

"""
from rabbitpy import base, utils

from . import helpers


class AMQPClassTests(helpers.TestCase):

    def test_channel_valid(self):
        obj = base.AMQPClass(self.channel, 'Foo')
        self.assertEqual(obj.channel, self.channel)

    def test_channel_invalid(self):
        self.assertRaises(ValueError, base.AMQPClass, 'Foo', 'Bar')

    def test_name_bytes(self):
        obj = base.AMQPClass(self.channel, b'Foo')
        self.assertIsInstance(obj.name, bytes)

    def test_name_str(self):
        obj = base.AMQPClass(self.channel, 'Foo')
        self.assertIsInstance(obj.name, str)

    @helpers.unittest.skipIf(utils.PYTHON3, 'No unicode in Python 3')
    def test_name_unicode(self):
        obj = base.AMQPClass(self.channel, unicode('Foo'))
        self.assertIsInstance(obj.name, unicode)

    def test_name_value(self):
        obj = base.AMQPClass(self.channel, 'Foo')
        self.assertEqual(obj.name, 'Foo')

    def test_name_invalid(self):
        self.assertRaises(ValueError, base.AMQPClass, self.channel, 1)
