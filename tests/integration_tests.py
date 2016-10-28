import logging
import re
import threading
import time
try:
    import unittest2 as unittest
except ImportError:
    import unittest
import uuid

import rabbitpy
from rabbitpy import exceptions
from rabbitpy import utils

LOGGER = logging.getLogger(__name__)

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('rabbitpy').setLevel(logging.DEBUG)


class ConfirmedPublishQueueLengthTest(unittest.TestCase):

    ITERATIONS = 5

    def setUp(self):
        self.connection = rabbitpy.Connection()
        self.channel = self.connection.channel()
        self.channel.enable_publisher_confirms()
        self.exchange = rabbitpy.TopicExchange(self.channel, 'pql-test')
        self.exchange.declare()
        self.queue = rabbitpy.Queue(self.channel, 'pql-queue')
        self.queue.declare()
        self.queue.bind(self.exchange, 'test.#')

        for iteration in range(0, self.ITERATIONS):
            message = rabbitpy.Message(self.channel, str(uuid.uuid4()))
            if not message.publish(self.exchange, 'test.publish.pql'):
                LOGGER.error('Error publishing message %i', iteration)

    def tearDown(self):
        self.queue.delete()
        self.exchange.delete()
        self.channel.close()
        self.connection.close()

    def test_get_returns_expected_message(self):
        self.assertEqual(len(self.queue), self.ITERATIONS)


class PublishAndGetTest(unittest.TestCase):

    def setUp(self):
        self.connection = rabbitpy.Connection()
        self.channel = self.connection.channel()
        self.exchange = rabbitpy.TopicExchange(self.channel, 'test-pagt')
        self.exchange.declare()
        self.queue = rabbitpy.Queue(self.channel, 'pagt-queue')
        self.queue.declare()
        self.queue.bind(self.exchange, 'test.#')

        self.app_id = b'PublishAndGetTest'
        self.message_body = b'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        self.message_type = b'test'

        self.msg = rabbitpy.Message(self.channel,
                                    self.message_body,
                                    {'app_id': self.app_id,
                                     'message_id': str(uuid.uuid4()),
                                     'timestamp': int(time.time()),
                                     'message_type': self.message_type})
        self.msg.publish(self.exchange, 'test.publish.get')

    def tearDown(self):
        self.queue.delete()
        self.exchange.delete()
        self.channel.close()
        self.connection.close()

    def test_get_returns_expected_message(self):
        msg = self.queue.get(True)
        self.assertEqual(msg.body, self.message_body)
        self.assertEqual(msg.properties['app_id'],
                         self.msg.properties['app_id'])
        self.assertEqual(msg.properties['message_id'],
                         self.msg.properties['message_id'])
        self.assertEqual(msg.properties['timestamp'],
                         self.msg.properties['timestamp'])
        self.assertEqual(msg.properties['message_type'],
                         self.msg.properties['message_type'])


class PublishAndConsumeTest(unittest.TestCase):

    def setUp(self):
        self.connection = rabbitpy.Connection()
        self.channel = self.connection.channel()
        self.exchange = rabbitpy.TopicExchange(self.channel, 'test-pact')
        self.exchange.declare()
        self.queue = rabbitpy.Queue(self.channel, 'pact-queue')
        self.queue.declare()
        self.queue.bind(self.exchange, 'test.#')

        self.app_id = b'PublishAndConsumeIteratorTest'
        self.message_body = b'ABC1234567890'
        self.message_type = b'test'

        self.msg = rabbitpy.Message(self.channel,
                                    self.message_body,
                                    {'app_id': self.app_id,
                                     'message_id': str(uuid.uuid4()),
                                     'timestamp': int(time.time()),
                                     'message_type': self.message_type})
        self.msg.publish(self.exchange, 'test.publish.consume')

    def tearDown(self):
        self.queue.delete()
        self.exchange.delete()
        self.channel.close()
        self.connection.close()

    def test_get_returns_expected_message(self):
        for msg in self.queue.consume(no_ack=True, prefetch=1):
            self.assertEqual(msg.body, self.message_body)
            self.assertEqual(msg.properties['app_id'],
                             self.msg.properties['app_id'])
            self.assertEqual(msg.properties['message_id'],
                             self.msg.properties['message_id'])
            self.assertEqual(msg.properties['timestamp'],
                             self.msg.properties['timestamp'])
            self.assertEqual(msg.properties['message_type'],
                             self.msg.properties['message_type'])
            break
        if utils.PYPY:
            self.queue.stop_consuming()


class PublishAndConsumeIteratorTest(unittest.TestCase):

    def setUp(self):
        self.connection = rabbitpy.Connection()
        self.channel = self.connection.channel()
        self.exchange = rabbitpy.TopicExchange(self.channel, 'test-pacit')
        self.exchange.declare()
        self.queue = rabbitpy.Queue(self.channel, 'pacit-queue')
        self.queue.declare()
        self.queue.bind(self.exchange, 'test.#')

        self.app_id = b'PublishAndConsumeIteratorTest'
        self.message_body = b'ABC1234567890'
        self.message_type = b'test'

        self.msg = rabbitpy.Message(self.channel,
                                    self.message_body,
                                    {'app_id': self.app_id,
                                     'message_id': str(uuid.uuid4()),
                                     'timestamp': int(time.time()),
                                     'message_type': self.message_type})
        self.msg.publish(self.exchange, 'test.publish.consume')

    def tearDown(self):
        self.queue.delete()
        self.exchange.delete()
        self.channel.close()
        self.connection.close()

    def test_iterator_returns_expected_message(self):
        for msg in self.queue:
            self.assertEqual(msg.body, self.message_body)
            self.assertEqual(msg.properties['app_id'],
                             self.msg.properties['app_id'])
            self.assertEqual(msg.properties['message_id'],
                             self.msg.properties['message_id'])
            self.assertEqual(msg.properties['timestamp'],
                             self.msg.properties['timestamp'])
            self.assertEqual(msg.properties['message_type'],
                             self.msg.properties['message_type'])
            msg.ack()
            LOGGER.info('breaking out of iterator')
            break
        if utils.PYPY:
            self.queue.stop_consuming()
        self.assertFalse(self.queue.consuming)


class PublishAndConsumeIteratorStopTest(unittest.TestCase):

    PUBLISH_COUNT = 10

    def setUp(self):
        self.connection = rabbitpy.Connection()
        self.channel = self.connection.channel()
        self.exchange = rabbitpy.TopicExchange(self.channel, 'test-pacist')
        self.exchange.declare()
        self.queue = rabbitpy.Queue(self.channel, 'pacist-queue')
        self.queue.declare()
        self.queue.bind(self.exchange, 'test.#')

        self.app_id = 'PublishAndConsumeIteratorTest'
        self.message_body = 'ABC1234567890'
        self.message_type = 'test'

        for iteration in range(0, self.PUBLISH_COUNT):
            self.msg = rabbitpy.Message(self.channel,
                                        self.message_body,
                                        {'app_id': self.app_id,
                                         'message_id': str(uuid.uuid4()),
                                         'timestamp': int(time.time()),
                                         'message_type': self.message_type})
            self.msg.publish(self.exchange,
                             'test.publish.consume {0}'.format(iteration))

    def stop_consumer(self):
        LOGGER.info('Stopping the consumer')
        self.queue.stop_consuming()

    def tearDown(self):
        self.queue.delete()
        self.exchange.delete()
        self.channel.close()
        self.connection.close()

    def test_iterator_exits_on_stop(self):
        LOGGER.info('Starting stop timer')
        timer = threading.Timer(2.5, self.stop_consumer)
        timer.daemon = True
        timer.start()
        qty = 0
        for msg in self.queue:
            if not msg:
                LOGGER.info('Message is empty')
                break
            qty += 1
            msg.ack()
        if utils.PYPY:
            self.queue.stop_consuming()
        LOGGER.info('Exited iterator, %r, %r', self.queue.consuming, qty)
        self.assertFalse(self.queue.consuming)
        self.assertEqual(qty, self.PUBLISH_COUNT)


class RedeliveredFlagTest(unittest.TestCase):

    def setUp(self):
        self.connection = rabbitpy.Connection()
        self.channel = self.connection.channel()
        self.queue = rabbitpy.Queue(self.channel, 'redeliver-test')
        self.queue.declare()

        # Publish the message that will be rejected
        message = rabbitpy.Message(self.channel, 'Payload Value')
        message.publish('', 'redeliver-test')

        # Get and reject the message
        msg1 = self.queue.get()
        msg1.reject(requeue=True)

    def tearDown(self):
        self.queue.delete()
        self.channel.close()
        self.connection.close()

    def test_redelivered_flag_is_set(self):
        msg = self.queue.get()
        msg.ack()
        self.assertTrue(msg.redelivered)


class UnnamedQueueDeclareTest(unittest.TestCase):

    def setUp(self):
        self.connection = rabbitpy.Connection()
        self.channel = self.connection.channel()

    def tearDown(self):
        self.channel.close()
        self.connection.close()

    def test_declaring_nameless_queue(self):
        self.queue = rabbitpy.Queue(self.channel)
        self.queue.declare()
        matches = re.match(b'^amq\.gen\-[\w_\-]+$', self.queue.name)
        self.assertIsNotNone(matches)
        self.queue.delete()


class SimpleCreateQueueTests(unittest.TestCase):

    def test_create_queue(self):
        name = 'simple-create-queue'
        rabbitpy.create_queue(queue_name=name)
        with rabbitpy.Connection() as conn:
            with conn.channel() as channel:
                queue = rabbitpy.Queue(channel, name)
                response = queue.declare(True)
                self.assertEqual(response, (0, 0))
                queue.delete()


class SimpleCreateDirectExchangeTests(unittest.TestCase):

    def test_create(self):
        name = 'direct-exchange-name'
        rabbitpy.create_direct_exchange(exchange_name=name)
        with rabbitpy.Connection() as conn:
            with conn.channel() as channel:
                obj = rabbitpy.DirectExchange(channel, name)
                obj.declare(True)
                obj.delete()

    def test_raises_on_empty_name(self):
        self.assertRaises(ValueError, rabbitpy.create_direct_exchange)


class SimpleCreateFanoutExchangeTests(unittest.TestCase):

    def test_create(self):
        name = 'fanout-exchange-name'
        rabbitpy.create_fanout_exchange(exchange_name=name)
        with rabbitpy.Connection() as conn:
            with conn.channel() as channel:
                obj = rabbitpy.FanoutExchange(channel, name)
                obj.declare(True)
                obj.delete()

    def test_raises_on_empty_name(self):
        self.assertRaises(ValueError, rabbitpy.create_fanout_exchange)


class SimpleCreateHeadersExchangeTests(unittest.TestCase):

    def test_create(self):
        name = 'headers-exchange-name'
        rabbitpy.create_headers_exchange(exchange_name=name)
        with rabbitpy.Connection() as conn:
            with conn.channel() as channel:
                obj = rabbitpy.HeadersExchange(channel, name)
                obj.declare(True)
                obj.delete()

    def test_raises_on_empty_name(self):
        self.assertRaises(ValueError, rabbitpy.create_headers_exchange)


class SimpleCreateTopicExchangeTests(unittest.TestCase):

    def test_create(self):
        name = 'topic-exchange-name'
        rabbitpy.create_topic_exchange(exchange_name=name)
        with rabbitpy.Connection() as conn:
            with conn.channel() as channel:
                obj = rabbitpy.TopicExchange(channel, name)
                obj.declare(True)
                obj.delete()

    def test_raises_on_empty_name(self):
        self.assertRaises(ValueError, rabbitpy.create_topic_exchange)


class SimpleDeleteExchangeTests(unittest.TestCase):

    def test_delete(self):
        name = 'delete-exchange-name'
        rabbitpy.create_topic_exchange(exchange_name=name)
        rabbitpy.delete_exchange(exchange_name=name)
        with rabbitpy.Connection() as conn:
            with conn.channel() as channel:
                obj = rabbitpy.TopicExchange(channel, name)
                self.assertRaises(exceptions.AMQPNotFound,
                                  obj.declare, True)

    def test_raises_on_empty_name(self):
        self.assertRaises(ValueError, rabbitpy.delete_exchange)


class SimpleDeleteQueueTests(unittest.TestCase):

    def test_delete(self):
        name = 'delete-queue-name'
        rabbitpy.create_queue(queue_name=name)
        rabbitpy.delete_queue(queue_name=name)
        with rabbitpy.Connection() as conn:
            with conn.channel() as channel:
                obj = rabbitpy.Queue(channel, name)
                self.assertRaises(exceptions.AMQPNotFound,
                                  obj.declare, True)

    def test_raises_on_empty_name(self):
        self.assertRaises(ValueError, rabbitpy.delete_queue)


class SimpleGetTests(unittest.TestCase):

    def test_get_empty(self):
        name = 'queue-name-get'
        rabbitpy.create_queue(queue_name=name)
        self.assertIsNone(rabbitpy.get(queue_name=name))
        rabbitpy.delete_queue(queue_name=name)

    def test_get_msg(self):
        body = b'test-body'
        name = 'queue-name-get'
        rabbitpy.create_queue(queue_name=name)
        rabbitpy.publish(routing_key=name, body=body)
        result = rabbitpy.get(queue_name=name)
        self.assertEqual(result.body, body)
        rabbitpy.delete_queue(queue_name=name)

    def test_raises_on_empty_name(self):
        self.assertRaises(ValueError, rabbitpy.get)


class SimplePublishTests(unittest.TestCase):

    def test_publish_with_confirm(self):
        body = b'test-body'
        name = 'simple-publish'
        rabbitpy.create_queue(queue_name=name)
        self.assertTrue(rabbitpy.publish(routing_key=name, body=body,
                                         confirm=True))
        result = rabbitpy.get(queue_name=name)
        self.assertEqual(result.body, body)
        rabbitpy.delete_queue(queue_name=name)


class SimpleConsumeTests(unittest.TestCase):

    def test_publish_with_confirm(self):
        body = b'test-body'
        name = 'simple-consume-tests'
        rabbitpy.create_queue(queue_name=name)
        self.assertTrue(rabbitpy.publish(routing_key=name, body=body,
                                         confirm=True))
        for message in rabbitpy.consume(queue_name=name, no_ack=True):
            self.assertEqual(message.body, body)
            break
        rabbitpy.delete_queue(queue_name=name)

    def test_raises_on_empty_name(self):
        try:
            for msg in rabbitpy.consume():
                break
            assert False, 'Did not raise ValueError'
        except ValueError:
            assert True
