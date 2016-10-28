"""
Test the rabbitpy.amqp_queue classes

"""
import mock
from pamqp import specification

from rabbitpy import amqp_queue
from rabbitpy import channel
from rabbitpy import exceptions
from rabbitpy import utils

from . import helpers


class QueueInitializationTests(helpers.TestCase):

    def test_empty_queue_name(self):
        queue = amqp_queue.Queue(self.channel)
        self.assertEqual(queue.name, '')

    def test_invalid_queue_name(self):
        self.assertRaises(ValueError, amqp_queue.Queue, self.channel, None)

    def test_auto_delete_default(self):
        queue = amqp_queue.Queue(self.channel)
        self.assertFalse(queue.auto_delete)

    def test_auto_delete_true(self):
        queue = amqp_queue.Queue(self.channel, auto_delete=True)
        self.assertTrue(queue.auto_delete)

    def test_auto_delete_false(self):
        queue = amqp_queue.Queue(self.channel, auto_delete=False)
        self.assertFalse(queue.auto_delete)

    def test_auto_delete_validation(self):
        self.assertRaises(ValueError, amqp_queue.Queue,
                          self.channel, None, None, None, 10)

    def test_durable_default(self):
        queue = amqp_queue.Queue(self.channel)
        self.assertFalse(queue.durable)

    def test_durable_true(self):
        queue = amqp_queue.Queue(self.channel, durable=True)
        self.assertTrue(queue.durable)

    def test_durable_false(self):
        queue = amqp_queue.Queue(self.channel, durable=False)
        self.assertFalse(queue.durable)

    def test_durable_validation(self):
        self.assertRaises(ValueError, amqp_queue.Queue,
                          self.channel, None, 'Foo')

    def test_exclusive_default(self):
        queue = amqp_queue.Queue(self.channel)
        self.assertFalse(queue.exclusive)

    def test_exclusive_true(self):
        queue = amqp_queue.Queue(self.channel, exclusive=True)
        self.assertTrue(queue.exclusive)

    def test_exclusive_false(self):
        queue = amqp_queue.Queue(self.channel, exclusive=False)
        self.assertFalse(queue.exclusive)

    def test_exclusive_validation(self):
        self.assertRaises(ValueError, amqp_queue.Queue,
                          self.channel, None, None, 'Bar')

    def test_expires_default(self):
        queue = amqp_queue.Queue(self.channel)
        self.assertIsNone(queue.expires)

    def test_expires_named_value(self):
        queue = amqp_queue.Queue(self.channel, expires=10)
        self.assertEqual(queue.expires, 10)
        self.assertIsInstance(queue.expires, int)

    def test_expires_positional_value(self):
        queue = amqp_queue.Queue(self.channel, '', True, False, True,
                                 None, None, 10)
        self.assertEqual(queue.expires, 10)
        self.assertIsInstance(queue.expires, int)

    def test_expires_validation(self):
        self.assertRaises(ValueError, amqp_queue.Queue,
                          self.channel, '', True, False, True, None, None, 'Foo')

    def test_max_length_default(self):
        queue = amqp_queue.Queue(self.channel)
        self.assertIsNone(queue.max_length)

    def test_max_length_named_value(self):
        queue = amqp_queue.Queue(self.channel, max_length=10)
        self.assertEqual(queue.max_length, 10)
        self.assertIsInstance(queue.max_length, int)

    def test_max_length_positional_value(self):
        queue = amqp_queue.Queue(self.channel, '', True, False, True, 10)
        self.assertEqual(queue.max_length, 10)
        self.assertIsInstance(queue.max_length, int)

    def test_max_length_validation(self):
        self.assertRaises(ValueError, amqp_queue.Queue,
                          self.channel, '', True, False, True, 'Foo')

    def test_message_ttl_default(self):
        queue = amqp_queue.Queue(self.channel)
        self.assertIsNone(queue.message_ttl)

    def test_message_ttl_value(self):
        queue = amqp_queue.Queue(self.channel, message_ttl=10)
        self.assertEqual(queue.message_ttl, 10)
        self.assertIsInstance(queue.message_ttl, int)

    def test_message_ttl_positional_value(self):
        queue = amqp_queue.Queue(self.channel, '', True, False, True, None, 10)
        self.assertEqual(queue.message_ttl, 10)
        self.assertIsInstance(queue.message_ttl, int)

    def test_message_ttl_validation(self):
        self.assertRaises(ValueError, amqp_queue.Queue,
                          self.channel, '', True, False, True, None, 'Foo')

    def test_dlx_default(self):
        queue = amqp_queue.Queue(self.channel)
        self.assertIsNone(queue.dead_letter_exchange)

    def test_dlx_value(self):
        queue = amqp_queue.Queue(self.channel, dead_letter_exchange='dlx-name')
        self.assertEqual(queue.dead_letter_exchange, 'dlx-name')

    def test_dlx_bytes(self):
        queue = amqp_queue.Queue(self.channel, dead_letter_exchange=b'dlx-name')
        self.assertIsInstance(queue.dead_letter_exchange, bytes)

    def test_dlx_str(self):
        queue = amqp_queue.Queue(self.channel, dead_letter_exchange='dlx-name')
        self.assertIsInstance(queue.dead_letter_exchange, str)

    @helpers.unittest.skipIf(utils.PYTHON3, 'No unicode in Python 3')
    def test_dlx_unicode(self):
        queue = amqp_queue.Queue(self.channel,
                                 dead_letter_exchange=unicode('dlx-name'))
        self.assertIsInstance(queue.dead_letter_exchange, unicode)

    def test_message_dlx_validation(self):
        self.assertRaises(ValueError, amqp_queue.Queue, self.channel, '', True,
                          False, True, None, None, None, True)

    def test_dlr_default(self):
        queue = amqp_queue.Queue(self.channel)
        self.assertIsNone(queue.dead_letter_routing_key)

    def test_dlr_value(self):
        queue = amqp_queue.Queue(self.channel,
                                 dead_letter_routing_key='routing-key')
        self.assertEqual(queue.dead_letter_routing_key, 'routing-key')

    def test_dlr_bytes(self):
        queue = amqp_queue.Queue(self.channel,
                                 dead_letter_routing_key=b'routing-key')
        self.assertIsInstance(queue.dead_letter_routing_key, bytes)

    def test_dlr_str(self):
        queue = amqp_queue.Queue(self.channel,
                                 dead_letter_routing_key='routing-key')
        self.assertIsInstance(queue.dead_letter_routing_key, str)

    @helpers.unittest.skipIf(utils.PYTHON3, 'No unicode in Python 3')
    def test_dlr_unicode(self):
        routing_key = unicode('routing-key')
        queue = amqp_queue.Queue(self.channel,
                                 dead_letter_routing_key=routing_key)
        self.assertIsInstance(queue.dead_letter_routing_key, unicode)

    def test_dlr_validation(self):
        self.assertRaises(ValueError, amqp_queue.Queue, self.channel, '', True,
                          False, True, None, None, None, None, True)

    @helpers.unittest.skipIf(
        utils.PYPY, 'PyPy bails this due to improper __exit__ behavior')
    def test_stop_consuming_raises_exception(self):
        queue = amqp_queue.Queue(self.channel)
        self.assertRaises(exceptions.NotConsumingError, queue.stop_consuming)


class QueueDeclareTests(helpers.TestCase):

    def test_default_declare(self):
        obj = amqp_queue.Queue(self.channel)
        expectation = {'arguments': {},
                       'auto_delete': False,
                       'durable': False,
                       'exclusive': False,
                       'nowait': False,
                       'passive': False,
                       'queue': '',
                       'ticket': 0}
        self.assertDictEqual(dict(obj._declare(False)), expectation)

    def test_default_declare_passive(self):
        obj = amqp_queue.Queue(self.channel)
        expectation = {'arguments': {},
                       'auto_delete': False,
                       'durable': False,
                       'exclusive': False,
                       'nowait': False,
                       'passive': True,
                       'queue': '',
                       'ticket': 0}
        self.assertDictEqual(dict(obj._declare(True)), expectation)

    def test_queue_name(self):
        obj = amqp_queue.Queue(self.channel, 'my-queue')
        expectation = {'arguments': {},
                       'auto_delete': False,
                       'durable': False,
                       'exclusive': False,
                       'nowait': False,
                       'passive': False,
                       'queue': 'my-queue',
                       'ticket': 0}
        self.assertDictEqual(dict(obj._declare(False)), expectation)

    def test_non_defaults(self):
        obj = amqp_queue.Queue(self.channel, 'my-queue', False, True, True,
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
        self.assertDictEqual(dict(obj._declare(False)), expectation)


class QueueAssignmentTests(helpers.TestCase):

    def setUp(self):
        super(QueueAssignmentTests, self).setUp()
        self.queue = amqp_queue.Queue(self.channel)

    def test_auto_delete_assign_true(self):
        self.queue.auto_delete = True
        self.assertTrue(self.queue.auto_delete)

    def test_auto_delete_assign_false(self):
        self.queue.auto_delete = False
        self.assertFalse(self.queue.auto_delete)

    def test_auto_delete_assign_raises_type_error(self):
        def assign_value():
            self.queue.auto_delete = 'Hello'
        self.assertRaises(ValueError, assign_value)

    def test_durable_assign_true(self):
        self.queue.durable = True
        self.assertTrue(self.queue.durable)

    def test_durable_assign_false(self):
        self.queue.durable = False
        self.assertFalse(self.queue.durable)

    def test_durable_assign_raises_type_error(self):
        def assign_value():
            self.queue.durable = 'Hello'
        self.assertRaises(ValueError, assign_value)

    def test_exclusive_assign_true(self):
        self.queue.exclusive = True
        self.assertTrue(self.queue.exclusive)

    def test_exclusive_assign_false(self):
        self.queue.exclusive = False
        self.assertFalse(self.queue.exclusive)

    def test_exclusive_assign_raises_type_error(self):
        def assign_value():
            self.queue.exclusive = 'Hello'
        self.assertRaises(ValueError, assign_value)

    def test_expires_assign_value(self):
        self.queue.expires = 100
        self.assertEqual(self.queue.expires, 100)

    def test_expires_assign_raises_type_error(self):
        def assign_value():
            self.queue.expires = 'Hello'
        self.assertRaises(ValueError, assign_value)

    def test_max_length_assign_value(self):
        self.queue.max_length = 100
        self.assertEqual(self.queue.max_length, 100)

    def test_max_length_assign_raises_type_error(self):
        def assign_value():
            self.queue.max_length = 'Hello'
        self.assertRaises(ValueError, assign_value)

    def test_message_ttl_assign_value(self):
        self.queue.message_ttl = 100
        self.assertEqual(self.queue.message_ttl, 100)

    def test_message_ttl_assign_raises_type_error(self):
        def assign_value():
            self.queue.message_ttl = 'Hello'
        self.assertRaises(ValueError, assign_value)

    def test_dead_letter_exchange_assign_value(self):
        self.queue.dead_letter_exchange = 'abcd'
        self.assertEqual(self.queue.dead_letter_exchange, 'abcd')

    def test_dead_letter_exchange_assign_raises_type_error(self):
        def assign_value():
            self.queue.dead_letter_exchange = 1234
        self.assertRaises(ValueError, assign_value)

    def test_dead_letter_routing_key_assign_value(self):
        self.queue.dead_letter_routing_key = 'abcd'
        self.assertEqual(self.queue.dead_letter_routing_key, 'abcd')

    def test_dead_letter_routing_key_assign_raises_type_error(self):
        def assign_value():
            self.queue.dead_letter_routing_key = 1234
        self.assertRaises(ValueError, assign_value)

    def test_arguments_assign_value(self):
        self.queue.arguments = {'foo': 'bar'}
        self.assertDictEqual(self.queue.arguments, {'foo': 'bar'})

    def test_arguments_assign_raises_type_error(self):
        def assign_value():
            self.queue.arguments = 1234
        self.assertRaises(ValueError, assign_value)


class WriteFrameTests(helpers.TestCase):
    NAME = 'test'
    DURABLE = True
    EXCLUSIVE = True
    AUTO_DELETE = False
    MAX_LENGTH = 200
    MESSAGE_TTL = 60000
    EXPIRES = 10
    DEAD_LETTER_EXCHANGE = 'dlx'
    DEAD_LETTER_ROUTING_KEY = 'dead'

    def setUp(self):
        super(WriteFrameTests, self).setUp()
        self.queue = amqp_queue.Queue(self.channel, self.NAME, self.DURABLE,
                                      self.EXCLUSIVE, self.AUTO_DELETE,
                                      self.MAX_LENGTH, self.MESSAGE_TTL,
                                      self.EXPIRES, self.DEAD_LETTER_EXCHANGE,
                                      self.DEAD_LETTER_ROUTING_KEY)

    @mock.patch('rabbitpy.amqp_queue.Queue._rpc')
    def test_declare_invokes_write_frame_with_queue_declare(self, rpc):
        self.queue.declare()
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Queue.Declare)

    @mock.patch('rabbitpy.amqp_queue.Queue._rpc')
    def test_ha_declare_invokes_write_frame_with_queue_declare(self, rpc):
        self.queue.ha_declare()
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Queue.Declare)

    @mock.patch('rabbitpy.amqp_queue.Queue._rpc')
    def test_ha_declare_list_invokes_write_frame_with_queue_declare(self, rpc):
        self.queue.ha_declare(['foo', 'bar'])
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Queue.Declare)

    @mock.patch('rabbitpy.amqp_queue.Queue._rpc')
    def test_ha_declare_list_sets_proper_attributes(self, rpc):
        self.queue.ha_declare(['foo', 'bar'])
        self.assertListEqual(self.queue.arguments['x-ha-nodes'],
                             ['foo', 'bar'])
        self.assertEqual(self.queue.arguments['x-ha-policy'], 'nodes')

    @mock.patch('rabbitpy.amqp_queue.Queue._rpc')
    def test_ha_declare_list_invokes_write_frame_with_queue_declare(self, rpc):
        self.queue.arguments['x-ha-nodes'] = ['foo', 'bar']
        self.queue.ha_declare()
        self.assertNotIn('x-ha-nodes', self.queue.arguments)
        self.assertEqual(self.queue.arguments['x-ha-policy'], 'all')

    @mock.patch('rabbitpy.amqp_queue.Queue._rpc')
    def test_bind_invokes_write_frame_with_queue_bind(self, rpc):
        self.queue.bind('foo', 'bar')
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Queue.Bind)

    @mock.patch('rabbitpy.amqp_queue.Queue._rpc')
    def test_unbind_invokes_write_frame_with_queue_declare(self, rpc):
        self.queue.unbind('foo', 'bar')
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Queue.Unbind)

    @mock.patch('rabbitpy.amqp_queue.Queue._rpc')
    def test_unbind_with_obj_invokes_write_frame_with_queue_declare(self, rpc):
        exchange = mock.Mock()
        exchange.name = 'foo'
        self.queue.unbind(exchange, 'bar')
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Queue.Unbind)


    @mock.patch('rabbitpy.amqp_queue.Queue._rpc')
    def test_unbind_invokes_write_frame_with_queue_delete(self, rpc):
        self.queue.delete()
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Queue.Delete)

    @mock.patch('rabbitpy.amqp_queue.Queue._rpc')
    def test_purge_invokes_write_frame_with_queue_purge(self, rpc):
        self.queue.purge()
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Queue.Purge)
