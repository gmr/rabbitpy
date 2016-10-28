"""
Test the rabbitpy.amqp class

"""
import mock

from rabbitpy import amqp, base, channel

from . import helpers

class BasicAckTests(helpers.TestCase):

    def test_basic_ack_invokes_write_frame(self):
        with mock.patch.object(self.channel, 'write_frame') as method:
            obj = amqp.AMQP(self.channel)
            obj.basic_ack(123, True)
            args, kwargs = method.call_args
            self.assertEqual(len(args), 1)
            self.assertEqual(args[0].delivery_tag, 123)
            self.assertEqual(args[0].multiple, True)
