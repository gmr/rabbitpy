"""
Test the rabbitpy.amqp class

"""
import mock
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from rabbitpy import amqp
from rabbitpy import channel
from rabbitpy import exceptions


class BasicAckTests(unittest.TestCase):

    def test_basic_ack_invokes_write_frame(self):
        with mock.patch('rabbitpy.channel.Channel.write_frame') as method:
            chan = channel.Channel(1, {}, None, None, None, None, 32768, None)
            obj = amqp.AMQP(chan)
            obj.basic_ack(123, True)
            args, kwargs = method.call_args
            self.assertEqual(len(args), 1)
            self.assertEqual(args[0].delivery_tag, 123)
            self.assertEqual(args[0].multiple, True)

