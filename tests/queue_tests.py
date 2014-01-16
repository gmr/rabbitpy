"""
Test the rabbitpy.amqp_queue classes

"""
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from rabbitpy import amqp_queue
from rabbitpy import channel
from rabbitpy import utils


class QueueInitializationTests(unittest.TestCase):

    def setUp(self):
        self.chan = channel.Channel(1, None, None, None, None, 32768, None)

    def test_empty_queue_name(self):
        queue = amqp_queue.Queue(self.chan)
        self.assertEqual(queue.name, '')

    def test_invalid_queue_name(self):
        self.assertRaises(ValueError, amqp_queue.Queue, self.chan, None)

    def test_auto_delete_default(self):
        queue = amqp_queue.Queue(self.chan)
        self.assertFalse(queue._auto_delete)

    def test_auto_delete_true(self):
        queue = amqp_queue.Queue(self.chan, auto_delete=True)
        self.assertTrue(queue._auto_delete)

    def test_auto_delete_false(self):
        queue = amqp_queue.Queue(self.chan, auto_delete=False)
        self.assertFalse(queue._auto_delete)

    def test_auto_delete_validation(self):
        self.assertRaises(ValueError, amqp_queue.Queue,
                          self.chan, None, None, None, 10)

    def test_durable_default(self):
        queue = amqp_queue.Queue(self.chan)
        self.assertTrue(queue._durable)

    def test_durable_true(self):
        queue = amqp_queue.Queue(self.chan, durable=True)
        self.assertTrue(queue._durable)

    def test_durable_false(self):
        queue = amqp_queue.Queue(self.chan, durable=False)
        self.assertFalse(queue._durable)

    def test_durable_validation(self):
        self.assertRaises(ValueError, amqp_queue.Queue,
                          self.chan, None, 'Foo')

    def test_exclusive_default(self):
        queue = amqp_queue.Queue(self.chan)
        self.assertFalse(queue._exclusive)

    def test_exclusive_true(self):
        queue = amqp_queue.Queue(self.chan, exclusive=True)
        self.assertTrue(queue._exclusive)

    def test_exclusive_false(self):
        queue = amqp_queue.Queue(self.chan, exclusive=False)
        self.assertFalse(queue._exclusive)

    def test_exclusive_validation(self):
        self.assertRaises(ValueError, amqp_queue.Queue,
                          self.chan, None, None, 'Bar')

    def test_expires_default(self):
        queue = amqp_queue.Queue(self.chan)
        self.assertIsNone(queue._expires)

    def test_expires_named_value(self):
        queue = amqp_queue.Queue(self.chan, expires=10)
        self.assertEqual(queue._expires, 10)
        self.assertIsInstance(queue._expires, int)

    def test_expires_positional_value(self):
        queue = amqp_queue.Queue(self.chan, '', True, False, True,
                                 None, None, 10)
        self.assertEqual(queue._expires, 10)
        self.assertIsInstance(queue._expires, int)

    def test_expires_validation(self):
        self.assertRaises(ValueError, amqp_queue.Queue,
                          self.chan, '', True, False, True, None, None, 'Foo')

    def test_max_length_default(self):
        queue = amqp_queue.Queue(self.chan)
        self.assertIsNone(queue._max_length)

    def test_max_length_named_value(self):
        queue = amqp_queue.Queue(self.chan, max_length=10)
        self.assertEqual(queue._max_length, 10)
        self.assertIsInstance(queue._max_length, int)

    def test_max_length_positional_value(self):
        queue = amqp_queue.Queue(self.chan, '', True, False, True, 10)
        self.assertEqual(queue._max_length, 10)
        self.assertIsInstance(queue._max_length, int)

    def test_max_length_validation(self):
        self.assertRaises(ValueError, amqp_queue.Queue,
                          self.chan, '', True, False, True, 'Foo')

    def test_message_ttl_default(self):
        queue = amqp_queue.Queue(self.chan)
        self.assertIsNone(queue._message_ttl)

    def test_message_ttl_value(self):
        queue = amqp_queue.Queue(self.chan, message_ttl=10)
        self.assertEqual(queue._message_ttl, 10)
        self.assertIsInstance(queue._message_ttl, int)

    def test_message_ttl_positional_value(self):
        queue = amqp_queue.Queue(self.chan, '', True, False, True, None, 10)
        self.assertEqual(queue._message_ttl, 10)
        self.assertIsInstance(queue._message_ttl, int)

    def test_message_ttl_validation(self):
        self.assertRaises(ValueError, amqp_queue.Queue,
                          self.chan, '', True, False, True, None, 'Foo')

    def test_dlx_default(self):
        queue = amqp_queue.Queue(self.chan)
        self.assertIsNone(queue._dlx)

    def test_dlx_value(self):
        queue = amqp_queue.Queue(self.chan, dead_letter_exchange='dlx-name')
        self.assertEqual(queue._dlx, 'dlx-name')

    def test_dlx_bytes(self):
        queue = amqp_queue.Queue(self.chan, dead_letter_exchange=b'dlx-name')
        self.assertIsInstance(queue._dlx, bytes)

    def test_dlx_str(self):
        queue = amqp_queue.Queue(self.chan, dead_letter_exchange='dlx-name')
        self.assertIsInstance(queue._dlx, bytes)

    @unittest.skipIf(utils.PYTHON3, 'No unicode in Python 3')
    def test_dlx_unicode(self):
        queue = amqp_queue.Queue(self.chan, dead_letter_exchange=u'dlx-name')
        self.assertIsInstance(queue._dlx, unicode)

    def test_message_dlx_validation(self):
        self.assertRaises(ValueError, amqp_queue.Queue, self.chan, '', True,
                          False, True, None, None, None, True)

    def test_dlr_default(self):
        queue = amqp_queue.Queue(self.chan)
        self.assertIsNone(queue._dlr)

    def test_dlr_value(self):
        queue = amqp_queue.Queue(self.chan,
                                 dead_letter_routing_key='routing-key')
        self.assertEqual(queue._dlr, 'routing-key')

    def test_dlr_bytes(self):
        queue = amqp_queue.Queue(self.chan,
                                 dead_letter_routing_key=b'routing-key')
        self.assertIsInstance(queue._dlr, bytes)

    def test_dlr_str(self):
        queue = amqp_queue.Queue(self.chan,
                                 dead_letter_routing_key='routing-key')
        self.assertIsInstance(queue._dlr, bytes)


    @unittest.skipIf(utils.PYTHON3, 'No unicode in Python 3')
    def test_dlr_unicode(self):
        queue = amqp_queue.Queue(self.chan,
                                 dead_letter_routing_key=u'routing-key')
        self.assertIsInstance(queue._dlr, unicode)

    def test_dlr_validation(self):
        self.assertRaises(ValueError, amqp_queue.Queue, self.chan, '', True,
                          False, True, None, None, None, None, True)


class QueueDeclareTests(unittest.TestCase):

    def setUp(self):
        self.chan = channel.Channel(1, None, None, None, None, 32768, None)

    def test_default_declare(self):
        obj = amqp_queue.Queue(self.chan)
        expectation = {'arguments': {},
                       'auto_delete': False,
                       'durable': True,
                       'exclusive': False,
                       'nowait': False,
                       'passive': False,
                       'queue': '',
                       'ticket': 0}
        self.assertDictEqual(obj._declare(False).__dict__, expectation)

    def test_default_declare_passive(self):
        obj = amqp_queue.Queue(self.chan)
        expectation = {'arguments': {},
                       'auto_delete': False,
                       'durable': True,
                       'exclusive': False,
                       'nowait': False,
                       'passive': True,
                       'queue': '',
                       'ticket': 0}
        self.assertDictEqual(obj._declare(True).__dict__, expectation)

    def test_queue_name(self):
        obj = amqp_queue.Queue(self.chan, 'my-queue')
        expectation = {'arguments': {},
                       'auto_delete': False,
                       'durable': True,
                       'exclusive': False,
                       'nowait': False,
                       'passive': False,
                       'queue': 'my-queue',
                       'ticket': 0}
        self.assertDictEqual(obj._declare(False).__dict__, expectation)

    def test_non_defaults(self):
        obj = amqp_queue.Queue(self.chan, 'my-queue', False, True, True,
                               100, 30000, 60000, 'dlx-name', 'dlrk')
        expectation = {'arguments': {'x-expires': 60000,
                                     'x-max-length': 100,
                                     'x-message-ttl': 30000,
                                     'x-dead-letter-exchange': 'dlx-name',
                                     'x-dead-letter-routing-key': 'dlrk'},
                       'auto_delete': True,
                       'durable': False,
                       'exclusive': True,
                       'nowait': False,
                       'passive': False,
                       'queue': 'my-queue',
                       'ticket': 0}
        self.assertDictEqual(obj._declare(False).__dict__, expectation)



