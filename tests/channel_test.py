"""
Test the rabbitpy.channel classes

"""
from rabbitpy import exceptions

from . import helpers

class ServerCapabilitiesTest(helpers.TestCase):

    def test_basic_nack_disabled(self):
        self.channel._server_capabilities[b'basic.nack'] = False
        self.assertFalse(self.channel._supports_basic_nack)

    def test_basic_nack_enabled(self):
        self.channel._server_capabilities[b'basic.nack'] = True
        self.assertTrue(self.channel._supports_basic_nack)

    def test_consumer_cancel_notify_disabled(self):
        self.channel._server_capabilities[b'consumer_cancel_notify'] = False
        self.assertFalse(self.channel._supports_consumer_cancel_notify)

    def test_consumer_cancel_notify_enabled(self):
        self.channel._server_capabilities[b'consumer_cancel_notify'] = True
        self.assertTrue(self.channel._supports_consumer_cancel_notify)

    def test_consumer_priorities_disabled(self):
        self.channel._server_capabilities[b'consumer_priorities'] = False
        self.assertFalse(self.channel._supports_consumer_priorities)

    def test_consumer_priorities_enabled(self):
        self.channel._server_capabilities[b'consumer_priorities'] = True
        self.assertTrue(self.channel._supports_consumer_priorities)

    def test_per_consumer_qos_disabled(self):
        self.channel._server_capabilities[b'per_consumer_qos'] = False
        self.assertFalse(self.channel._supports_per_consumer_qos)

    def test_per_consumer_qos_enabled(self):
        self.channel._server_capabilities[b'per_consumer_qos'] = True
        self.assertTrue(self.channel._supports_per_consumer_qos)

    def test_publisher_confirms_disabled(self):
        self.channel._server_capabilities[b'publisher_confirms'] = False
        self.assertFalse(self.channel._supports_publisher_confirms)

    def test_publisher_confirms_enabled(self):
        self.channel._server_capabilities[b'publisher_confirms'] = True
        self.assertTrue(self.channel._supports_publisher_confirms)

    def test_invoking_consume_raises(self):
        self.channel._server_capabilities[b'consumer_priorities'] = False
        self.assertRaises(exceptions.NotSupportedError,
                          self.channel._consume, self, True, 100)

    def test_invoking_basic_nack_raises(self):
        self.channel._server_capabilities[b'basic_nack'] = False
        self.assertRaises(exceptions.NotSupportedError,
                          self.channel._multi_nack, 100)

    def test_invoking_enable_publisher_confirms_raises(self):
        self.channel._server_capabilities[b'publisher_confirms'] = False
        self.assertRaises(exceptions.NotSupportedError,
                          self.channel.enable_publisher_confirms)
