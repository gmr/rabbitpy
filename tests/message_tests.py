"""
Test the rabbitpy.message.Message class

"""
import datetime
import json
import logging
import time
try:
    import unittest2 as unittest
except ImportError:
    import unittest
import uuid

from rabbitpy import channel
from rabbitpy import message


logging.basicConfig(level=logging.DEBUG)


class TestMessageCreation(unittest.TestCase):

    def setUp(self):
        self.chan = channel.Channel(1, None, None, None, None, 32768, None)
        self.body = uuid.uuid4()
        self.msg = message.Message(self.chan, self.body)

    def test_channel_assignment(self):
        self.assertEqual(self.msg.channel, self.chan)

    def test_message_body(self):
        self.assertEqual(self.msg.body, self.body)

    def test_message_message_id_property_set(self):
        self.assertIsNotNone(self.msg.properties['message_id'])


class TestMessageCreationWithDictBody(unittest.TestCase):

    def setUp(self):
        self.chan = channel.Channel(1, None, None, None, None, 32768, None)
        self.body = {'foo': str(uuid.uuid4())}
        self.msg = message.Message(self.chan, self.body)

    def test_message_body(self):
        self.assertEqual(self.msg.body, json.dumps(self.body))

    def test_message_content_type_is_set(self):
        self.assertEqual(self.msg.properties['content_type'],
                         'application/json')


class TestMessageCreationWithStructTimeTimestamp(unittest.TestCase):

    def setUp(self):
        self.chan = channel.Channel(1, None, None, None, None, 32768, None)
        self.msg = message.Message(self.chan, str(uuid.uuid4()),
                                   {'timestamp': time.localtime()})

    def test_message_timestamp_property_is_datetime(self):
        self.assertIsInstance(self.msg.properties['timestamp'],
                              datetime.datetime)


class TestMessageCreationWithFloatTimestamp(unittest.TestCase):

    def setUp(self):
        self.chan = channel.Channel(1, None, None, None, None, 32768, None)
        self.msg = message.Message(self.chan, str(uuid.uuid4()),
                                   {'timestamp': time.time()})

    def test_message_timestamp_property_is_datetime(self):
        self.assertIsInstance(self.msg.properties['timestamp'],
                              datetime.datetime)


class TestMessageCreationWithIntTimestamp(unittest.TestCase):

    def setUp(self):
        self.chan = channel.Channel(1, None, None, None, None, 32768, None)
        self.msg = message.Message(self.chan, str(uuid.uuid4()),
                                   {'timestamp': int(time.time())})

    def test_message_timestamp_property_is_datetime(self):
        self.assertIsInstance(self.msg.properties['timestamp'],
                              datetime.datetime)


class TestMessageCreationWithDictBodyAndProperties(unittest.TestCase):

    def setUp(self):
        self.chan = channel.Channel(1, None, None, None, None, 32768, None)
        self.body = {'foo': str(uuid.uuid4())}
        self.msg = message.Message(self.chan, self.body, {'app_id': 'foo'})

    def test_message_body(self):
        self.assertEqual(self.msg.body, json.dumps(self.body))

    def test_message_content_type_is_set(self):
        self.assertEqual(self.msg.properties['content_type'],
                         'application/json')


class TestMessageCreationWithNoAutoID(unittest.TestCase):

    def setUp(self):
        self.chan = channel.Channel(1, None, None, None, None, 32768, None)
        self.body = str(uuid.uuid4())
        self.msg = message.Message(self.chan, self.body, auto_id=False)

    def test_message_body(self):
        self.assertEqual(self.msg.body, self.body)

    def test_message_message_id_property_is_not_set(self):
        self.assertNotIn('message_id', self.msg.properties)


class TestMessageWithPropertiesCreation(unittest.TestCase):

    def setUp(self):
        self.chan = channel.Channel(1, None, None, None, None, 32768, None)
        self.body = uuid.uuid4()
        self.props = {'app_id': b'Foo',
                      'content_type': b'application/json',
                      'content_encoding': b'gzip',
                      'correlation_id': str(uuid.uuid4()),
                      'delivery_mode': 2,
                      'expiration': int(time.time()) + 10,
                      'headers': {'foo': 'bar'},
                      'message_id': str(uuid.uuid4()),
                      'message_type': b'TestMessageCreation',
                      'priority': 9,
                      'reply_to': b'none',
                      'timestamp': datetime.datetime.now(),
                      'user_id': b'guest'}
        self.msg = message.Message(self.chan, self.body, dict(self.props))

    def test_message_body(self):
        self.assertEqual(self.msg.body, self.body)

    def test_message_properties_match(self):
        self.assertDictEqual(self.msg.properties, self.props)


class TestMessageInvalidPropertyHandling(unittest.TestCase):

    def setUp(self):
        self.chan = channel.Channel(1, None, None, None, None, 32768, None)

    def test_invalid_property_raises_key_error(self):
        self.assertRaises(KeyError,
                          message.Message,
                          self.chan,
                          str(uuid.uuid4()), {'invalid': True})
