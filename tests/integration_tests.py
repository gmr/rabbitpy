import logging
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
    app_id = 'PublishAndGetTest'
    message_body = str(uuid.uuid4())
    message_type = 'test'

    def setUp(self):
        self.connection = rabbitpy.Connection()
        self.channel = self.connection.channel()
        self.exchange = rabbitpy.TopicExchange(self.channel, 'test-pagt')
        self.exchange.declare()
        self.queue = rabbitpy.Queue(self.channel, 'pagt-queue')
        self.queue.declare()
        self.queue.bind(self.exchange, 'test.#')

        self.message = rabbitpy.Message(self.channel,
                                        self.message_body,
                                        {'app_id': self.app_id,
                                         'message_id': str(uuid.uuid4()),
                                         'timestamp': int(time.time()),
                                         'message_type': self.message_type})
        self.message.publish(self.exchange, 'test.publish.get')

    def tearDown(self):
        self.queue.delete()
        self.exchange.delete()

    def test_get_returns_expected_message(self):
        message = self.queue.get(True)
        self.assertEqual(message.body, self.message_body)
        self.assertEqual(message.properties['app_id'],
                         self.message.properties['app_id'])
        self.assertEqual(message.properties['message_id'],
                         self.message.properties['message_id'])
        self.assertEqual(message.properties['timestamp'],
                         self.message.properties['timestamp'])
        self.assertEqual(message.properties['message_type'],
                         self.message.properties['message_type'])


class PublishAndConsumeIteratorTest(unittest.TestCase):
    app_id = 'PublishAndConsumeIteratorTest'
    message_body = str(uuid.uuid4())
    message_type = 'test'

    def setUp(self):
        self.connection = rabbitpy.Connection()
        self.channel = self.connection.channel()
        self.exchange = rabbitpy.TopicExchange(self.channel, 'test-pacit')
        self.exchange.declare()
        self.queue = rabbitpy.Queue(self.channel, 'pacit-queue')
        self.queue.declare()
        self.queue.bind(self.exchange, 'test.#')

        self.message = rabbitpy.Message(self.channel,
                                        self.message_body,
                                        {'app_id': self.app_id,
                                         'message_id': str(uuid.uuid4()),
                                         'timestamp': int(time.time()),
                                         'message_type': self.message_type})
        self.message.publish(self.exchange, 'test.publish.consume')

    def tearDown(self):
        self.queue.delete()
        self.exchange.delete()

    def test_get_returns_expected_message(self):
        for message in self.queue:
            self.assertEqual(message.body, self.message_body)
            self.assertEqual(message.properties['app_id'],
                             self.message.properties['app_id'])
            self.assertEqual(message.properties['message_id'],
                             self.message.properties['message_id'])
            self.assertEqual(message.properties['timestamp'],
                             self.message.properties['timestamp'])
            self.assertEqual(message.properties['message_type'],
                             self.message.properties['message_type'])
            break
