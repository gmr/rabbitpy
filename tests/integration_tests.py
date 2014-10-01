import logging
import re
import time
try:
    import unittest2 as unittest
except ImportError:
    import unittest
import uuid

import rabbitpy

LOGGER = logging.getLogger(__name__)


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

        self.app_id = 'PublishAndGetTest'
        self.message_body = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        self.message_type = 'test'

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

    def test_get_returns_expected_message(self):
        msg = self.queue.get(True)
        self.assertEqual(msg.body.decode('utf-8'), self.message_body)
        self.assertEqual(msg.properties['app_id'].decode('utf-8'),
                         self.msg.properties['app_id'])
        self.assertEqual(msg.properties['message_id'].decode('utf-8'),
                         self.msg.properties['message_id'])
        self.assertEqual(msg.properties['timestamp'],
                         self.msg.properties['timestamp'])
        self.assertEqual(msg.properties['message_type'].decode('utf-8'),
                         self.msg.properties['message_type'])


class PublishAndConsumeTest(unittest.TestCase):

    def setUp(self):
        self.connection = rabbitpy.Connection()
        self.channel = self.connection.channel()
        self.exchange = rabbitpy.TopicExchange(self.channel, 'test-pacit')
        self.exchange.declare()
        self.queue = rabbitpy.Queue(self.channel, 'pacit-queue')
        self.queue.declare()
        self.queue.bind(self.exchange, 'test.#')

        self.app_id = 'PublishAndConsumeIteratorTest'
        self.message_body = 'ABC1234567890'
        self.message_type = 'test'

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

    def test_get_returns_expected_message(self):
        for msg in self.queue.consume_messages(no_ack=True, prefetch=1):
            self.assertEqual(msg.body.decode('utf-8'), self.message_body)
            self.assertEqual(msg.properties['app_id'].decode('utf-8'),
                             self.msg.properties['app_id'])
            self.assertEqual(msg.properties['message_id'].decode('utf-8'),
                             self.msg.properties['message_id'])
            self.assertEqual(msg.properties['timestamp'],
                             self.msg.properties['timestamp'])
            self.assertEqual(msg.properties['message_type'].decode('utf-8'),
                             self.msg.properties['message_type'])
            break


class PublishAndConsumeIteratorTest(unittest.TestCase):

    def setUp(self):
        self.connection = rabbitpy.Connection()
        self.channel = self.connection.channel()
        self.exchange = rabbitpy.TopicExchange(self.channel, 'test-pacit')
        self.exchange.declare()
        self.queue = rabbitpy.Queue(self.channel, 'pacit-queue')
        self.queue.declare()
        self.queue.bind(self.exchange, 'test.#')

        self.app_id = 'PublishAndConsumeIteratorTest'
        self.message_body = 'ABC1234567890'
        self.message_type = 'test'

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

    def test_get_returns_expected_message(self):
        for msg in self.queue:
            self.assertEqual(msg.body.decode('utf-8'), self.message_body)
            self.assertEqual(msg.properties['app_id'].decode('utf-8'),
                             self.msg.properties['app_id'])
            self.assertEqual(msg.properties['message_id'].decode('utf-8'),
                             self.msg.properties['message_id'])
            self.assertEqual(msg.properties['timestamp'],
                             self.msg.properties['timestamp'])
            self.assertEqual(msg.properties['message_type'].decode('utf-8'),
                             self.msg.properties['message_type'])
            msg.ack()
            self.queue.stop_consuming()
        self.assertFalse(self.queue.consuming)


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

    def test_declaring_nameless_queue(self):
        self.queue = rabbitpy.Queue(self.channel)
        self.queue.declare()
        matches = re.match(b'^amq\.gen\-[\w_\-]+$', self.queue.name)
        self.assertIsNotNone(matches)
