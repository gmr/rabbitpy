"""
Test the rabbitpy.base classes

"""

from rabbitpy import base, utils
from tests import helpers


class AMQPClassTests(helpers.TestCase):
    def test_channel_valid(self):
        obj = base.AMQPClass(self.channel, 'Foo')
        self.assertEqual(obj.channel, self.channel)

    def test_channel_invalid(self):
        self.assertRaises(ValueError, base.AMQPClass, 'Foo', 'Bar')

    def test_name_bytes(self):
        # Python 3 only: bytes names are not accepted
        self.assertRaises(ValueError, base.AMQPClass, self.channel, b'Foo')

    def test_name_str(self):
        obj = base.AMQPClass(self.channel, 'Foo')
        self.assertIsInstance(obj.name, str)

    @helpers.unittest.skipIf(utils.PYTHON3, 'No unicode in Python 3')
    def test_name_py2_unicode(self):
        obj = base.AMQPClass(self.channel, 'Foo')
        self.assertIsInstance(obj.name, str)

    def test_name_value(self):
        obj = base.AMQPClass(self.channel, 'Foo')
        self.assertEqual(obj.name, 'Foo')

    def test_name_invalid(self):
        self.assertRaises(ValueError, base.AMQPClass, self.channel, 1)
