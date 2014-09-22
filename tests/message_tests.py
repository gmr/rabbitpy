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

import mock
from pamqp import body
from pamqp import header
from pamqp import specification

from rabbitpy import channel
from rabbitpy import exceptions
from rabbitpy import exchange
from rabbitpy import message


logging.basicConfig(level=logging.DEBUG)


class BaseTestCase(unittest.TestCase):

    def setUp(self):
        self.chan = channel.Channel(1, None, None, None, None, 32768, None)



class TestCreation(BaseTestCase):

    def setUp(self):
        super(TestCreation, self).setUp()
        self.body = uuid.uuid4()
        self.msg = message.Message(self.chan, self.body)

    def test_channel_assignment(self):
        self.assertEqual(self.msg.channel, self.chan)

    def test_message_body(self):
        self.assertEqual(self.msg.body, self.body)

    def test_message_message_id_property_set(self):
        self.assertIsNotNone(self.msg.properties['message_id'])


class TestCreationWithDictBody(BaseTestCase):

    def setUp(self):
        super(TestCreationWithDictBody, self).setUp()
        self.body = {'foo': str(uuid.uuid4())}
        self.msg = message.Message(self.chan, self.body)

    def test_message_body(self):
        self.assertEqual(self.msg.body, json.dumps(self.body))

    def test_message_content_type_is_set(self):
        self.assertEqual(self.msg.properties['content_type'],
                         'application/json')


class TestCreationWithStructTimeTimestamp(BaseTestCase):

    def setUp(self):
        super(TestCreationWithStructTimeTimestamp, self).setUp()
        self.msg = message.Message(self.chan, str(uuid.uuid4()),
                                   {'timestamp': time.localtime()})

    def test_message_timestamp_property_is_datetime(self):
        self.assertIsInstance(self.msg.properties['timestamp'],
                              datetime.datetime)


class TestCreationWithFloatTimestamp(BaseTestCase):

    def setUp(self):
        super(TestCreationWithFloatTimestamp, self).setUp()
        self.msg = message.Message(self.chan, str(uuid.uuid4()),
                                   {'timestamp': time.time()})

    def test_message_timestamp_property_is_datetime(self):
        self.assertIsInstance(self.msg.properties['timestamp'],
                              datetime.datetime)


class TestCreationWithIntTimestamp(BaseTestCase):

    def setUp(self):
        super(TestCreationWithIntTimestamp, self).setUp()
        self.msg = message.Message(self.chan, str(uuid.uuid4()),
                                   {'timestamp': int(time.time())})

    def test_message_timestamp_property_is_datetime(self):
        self.assertIsInstance(self.msg.properties['timestamp'],
                              datetime.datetime)


class TestCreationWithInvalidTimestampType(BaseTestCase):

    def test_message_timestamp_property_is_datetime(self):
        self.assertRaises(ValueError,
                          message.Message,
                          self.chan,
                          str(uuid.uuid4()),
                          {'timestamp': ['Ohai']})


class TestCreationWithNoneTimestamp(BaseTestCase):

    def setUp(self):
        super(TestCreationWithNoneTimestamp, self).setUp()
        self.msg = message.Message(self.chan, str(uuid.uuid4()),
                                   {'timestamp': None})

    def test_message_timestamp_property_is_datetime(self):
        self.assertIsNone(self.msg.properties['timestamp'])


class TestCreationWithStrTimestamp(BaseTestCase):

    def setUp(self):
        super(TestCreationWithStrTimestamp, self).setUp()
        self.msg = message.Message(self.chan, str(uuid.uuid4()),
                                   {'timestamp': str(int(time.time()))})

    def test_message_timestamp_property_is_datetime(self):
        self.assertIsInstance(self.msg.properties['timestamp'],
                              datetime.datetime)


class TestCreationWithDictBodyAndProperties(BaseTestCase):

    def setUp(self):
        super(TestCreationWithDictBodyAndProperties, self).setUp()
        self.body = {'foo': str(uuid.uuid4())}
        self.msg = message.Message(self.chan, self.body, {'app_id': 'foo'})

    def test_message_body(self):
        self.assertEqual(self.msg.body, json.dumps(self.body))

    def test_message_content_type_is_set(self):
        self.assertEqual(self.msg.properties['content_type'],
                         'application/json')


class TestCreationWithNoAutoID(BaseTestCase):

    def setUp(self):
        super(TestCreationWithNoAutoID, self).setUp()
        self.body = str(uuid.uuid4())
        self.msg = message.Message(self.chan, self.body, auto_id=False)

    def test_message_body(self):
        self.assertEqual(self.msg.body, self.body)

    def test_message_message_id_property_is_not_set(self):
        self.assertNotIn('message_id', self.msg.properties)


class TestWithPropertiesCreation(BaseTestCase):

    def setUp(self):
        super(TestWithPropertiesCreation, self).setUp()
        self.body = uuid.uuid4()
        self.props = {'app_id': b'Foo',
                      'content_type': b'application/json',
                      'content_encoding': b'gzip',
                      'correlation_id': str(uuid.uuid4()),
                      'delivery_mode': 2,
                      'expiration': int(time.time()) + 10,
                      'headers': {'foo': 'bar'},
                      'message_id': str(uuid.uuid4()),
                      'message_type': b'TestCreation',
                      'priority': 9,
                      'reply_to': b'none',
                      'timestamp': datetime.datetime.now(),
                      'user_id': b'guest'}
        self.msg = message.Message(self.chan, self.body, dict(self.props))

    def test_message_body(self):
        self.assertEqual(self.msg.body, self.body)

    def test_message_properties_match(self):
        self.assertDictEqual(self.msg.properties, self.props)


class TestInvalidPropertyHandling(BaseTestCase):

    def test_invalid_property_raises_key_error(self):
        self.assertRaises(KeyError,
                          message.Message,
                          self.chan,
                          str(uuid.uuid4()), {'invalid': True})


class TestDeliveredMessageObject(BaseTestCase):

    BODY = '{"foo": "bar", "val": 1}'
    PROPERTIES = {'message_type': 'test'}
    CONSUMER_TAG = 'ctag0'
    DELIVERY_TAG = 100
    REDELIVERED = True
    EXCHANGE = 'test-exchange'
    ROUTING_KEY = 'test-routing-key'

    def setUp(self):
        super(TestDeliveredMessageObject, self).setUp()
        self.method = specification.Basic.Deliver(self.CONSUMER_TAG,
                                                  self.DELIVERY_TAG,
                                                  self.REDELIVERED,
                                                  self.EXCHANGE,
                                                  self.ROUTING_KEY)
        self.msg = message.Message(self.chan, self.BODY, self.PROPERTIES)
        self.msg.method = self.method
        self.msg.name = self.method.name

    def test_delivery_tag_property(self):
        self.assertEqual(self.msg.delivery_tag, self.DELIVERY_TAG)

    def test_redelivered_property(self):
        self.assertEqual(self.msg.redelivered, self.REDELIVERED)

    def test_routing_key_property(self):
        self.assertEqual(self.msg.routing_key, self.ROUTING_KEY)

    def test_exchange_property(self):
        self.assertEqual(self.msg.exchange, self.EXCHANGE)

    def test_json_body_value(self):
        self.assertDictEqual(self.msg.json(), json.loads(self.BODY))

    def test_ack_invokes_channel_write_frame(self):
        with mock.patch.object(self.chan, '_write_frame') as write_frame:
            self.msg.ack()
            write_frame.assert_called_once()

    def test_ack_channel_write_frame_type(self):
        with mock.patch('rabbitpy.channel.Channel._write_frame') as wframe:
            self.msg.ack()
            frame_value = wframe.mock_calls[0][1][0]
            self.assertIsInstance(frame_value, specification.Basic.Ack)

    def test_ack_channel_write_frame_delivery_tag_value(self):
        with mock.patch('rabbitpy.channel.Channel._write_frame') as wframe:
            self.msg.ack()
            frame_value = wframe.mock_calls[0][1][0]
            self.assertEqual(frame_value.delivery_tag,
                             self.DELIVERY_TAG)

    def test_ack_channel_write_frame_multiple_false_value(self):
        with mock.patch('rabbitpy.channel.Channel._write_frame') as wframe:
            self.msg.ack()
            frame_value = wframe.mock_calls[0][1][0]
            self.assertFalse(frame_value.multiple)

    def test_ack_channel_write_frame_multiple_true_value(self):
        with mock.patch('rabbitpy.channel.Channel._write_frame') as wframe:
            self.msg.ack(True)
            frame_value = wframe.mock_calls[0][1][0]
            self.assertTrue(frame_value.multiple)

    def test_nack_invokes_channel_write_frame(self):
        with mock.patch.object(self.chan, '_write_frame') as write_frame:
            self.msg.nack()
            write_frame.assert_called_once()

    def test_nack_channel_write_frame_type(self):
        with mock.patch('rabbitpy.channel.Channel._write_frame') as wframe:
            self.msg.nack()
            frame_value = wframe.mock_calls[0][1][0]
            self.assertIsInstance(frame_value, specification.Basic.Nack)

    def test_nack_channel_write_frame_delivery_tag_value(self):
        with mock.patch('rabbitpy.channel.Channel._write_frame') as wframe:
            self.msg.nack()
            frame_value = wframe.mock_calls[0][1][0]
            self.assertEqual(frame_value.delivery_tag,
                             self.DELIVERY_TAG)

    def test_nack_channel_write_frame_requeue_false_value(self):
        with mock.patch('rabbitpy.channel.Channel._write_frame') as wframe:
            self.msg.nack(requeue=False)
            frame_value = wframe.mock_calls[0][1][0]
            self.assertFalse(frame_value.requeue)

    def test_nack_channel_write_frame_requeue_true_value(self):
        with mock.patch('rabbitpy.channel.Channel._write_frame') as wframe:
            self.msg.nack(requeue=True)
            frame_value = wframe.mock_calls[0][1][0]
            self.assertTrue(frame_value.requeue)

    def test_nack_channel_write_frame_multiple_false_value(self):
        with mock.patch('rabbitpy.channel.Channel._write_frame') as wframe:
            self.msg.nack()
            frame_value = wframe.mock_calls[0][1][0]
            self.assertFalse(frame_value.multiple)

    def test_nack_channel_write_frame_multiple_true_value(self):
        with mock.patch('rabbitpy.channel.Channel._write_frame') as wframe:
            self.msg.nack(all_previous=True)
            frame_value = wframe.mock_calls[0][1][0]
            self.assertTrue(frame_value.multiple)

    def test_reject_invokes_channel_write_frame(self):
        with mock.patch.object(self.chan, '_write_frame') as write_frame:
            self.msg.reject()
            write_frame.assert_called_once()

    def test_reject_channel_write_frame_type(self):
        with mock.patch('rabbitpy.channel.Channel._write_frame') as wframe:
            self.msg.reject()
            frame_value = wframe.mock_calls[0][1][0]
            self.assertIsInstance(frame_value, specification.Basic.Reject)

    def test_reject_channel_write_frame_delivery_tag_value(self):
        with mock.patch('rabbitpy.channel.Channel._write_frame') as wframe:
            self.msg.reject()
            frame_value = wframe.mock_calls[0][1][0]
            self.assertEqual(frame_value.delivery_tag,
                             self.DELIVERY_TAG)

    def test_reject_channel_write_frame_requeue_false_value(self):
        with mock.patch('rabbitpy.channel.Channel._write_frame') as wframe:
            self.msg.reject(requeue=False)
            frame_value = wframe.mock_calls[0][1][0]
            self.assertFalse(frame_value.requeue)

    def test_reject_channel_write_frame_requeue_true_value(self):
        with mock.patch('rabbitpy.channel.Channel._write_frame') as wframe:
            self.msg.reject(requeue=True)
            frame_value = wframe.mock_calls[0][1][0]
            self.assertTrue(frame_value.requeue)


class TestNonDeliveredMessageObject(BaseTestCase):

    BODY = {'foo': str(uuid.uuid4()),
            'bar': 'baz',
            'qux': 1}

    def setUp(self):
        super(TestNonDeliveredMessageObject, self).setUp()
        self.body = self.BODY
        self.msg = message.Message(self.chan, self.body, {'app_id': 'foo'})

    def test_ack_raises_action_exception(self):
        self.assertRaises(exceptions.ActionException, self.msg.ack)

    def test_nack_raises_action_exception(self):
        self.assertRaises(exceptions.ActionException, self.msg.nack)

    def test_reject_raises_action_exception(self):
        self.assertRaises(exceptions.ActionException, self.msg.reject)

    def test_prune_invalid_properties_removes_bogus_property(self):
        self.msg.properties['invalid'] = True
        self.msg._prune_invalid_properties()
        self.assertNotIn('invalid', self.msg.properties)

    def test_coerce_property_int_to_str(self):
        self.msg.properties['expiration'] = 123
        self.msg._coerce_properties()
        self.assertIsInstance(self.msg.properties['expiration'], bytes)

    def test_coerce_property_str_to_int(self):
        self.msg.properties['priority'] = '9'
        self.msg._coerce_properties()
        self.assertIsInstance(self.msg.properties['priority'], int)

    def test_coerce_property_str_to_empty_dict(self):
        self.msg.properties['headers'] = '9'
        self.msg._coerce_properties()
        self.assertDictEqual(self.msg.properties['headers'], dict())

    def test_coerce_property_str_timestamp(self):
        self.msg.properties['timestamp'] = str(int(time.time()))
        self.msg._coerce_properties()
        self.assertIsInstance(self.msg.properties['timestamp'],
                              datetime.datetime)


class TestPublishing(BaseTestCase):

    BODY = {'foo': str(uuid.uuid4()),
            'bar': 'baz',
            'qux': 1}
    EXCHANGE = 'foo'
    ROUTING_KEY = 'bar.baz'

    @mock.patch('rabbitpy.channel.Channel._write_frame')
    def setUp(self, write_frame):
        super(TestPublishing, self).setUp()
        self.write_frame = write_frame
        self.msg = message.Message(self.chan, self.BODY, {'app_id': 'foo'})
        self.msg.publish(self.EXCHANGE, self.ROUTING_KEY)

    def test_publish_invokes_write_frame_with_basic_publish(self):
        self.assertIsInstance(self.write_frame.mock_calls[0][1][0],
                              specification.Basic.Publish)

    def test_publish_with_exchange_object(self):
        _exchange = exchange.Exchange(self.chan, self.EXCHANGE)
        with mock.patch('rabbitpy.channel.Channel._write_frame') as wframe:
            self.msg.publish(_exchange, self.ROUTING_KEY)
            self.assertEqual(wframe.mock_calls[0][1][0].exchange,
                             self.EXCHANGE)

    def test_publish_with_exchange_str(self):
        self.assertEqual(self.write_frame.mock_calls[0][1][0].exchange,
                         self.EXCHANGE)

    def test_publish_routing_key_value(self):
        self.assertEqual(self.write_frame.mock_calls[0][1][0].routing_key,
                         self.ROUTING_KEY)

    def test_publish_mandatory_false_value(self):
        self.assertFalse(self.write_frame.mock_calls[0][1][0].mandatory)

    def test_publish_mandatory_true_value(self):
        with mock.patch('rabbitpy.channel.Channel._write_frame') as wframe:
            self.msg.publish(self.EXCHANGE, self.ROUTING_KEY, True)
            self.assertTrue(wframe.mock_calls[0][1][0].mandatory)

    def test_publish_invokes_write_frame_with_content_header(self):
        self.assertIsInstance(self.write_frame.mock_calls[1][1][0],
                              header.ContentHeader)

    def test_content_header_frame_body_size(self):
        self.assertEqual(self.write_frame.mock_calls[1][1][0].body_size,
                         len(self.msg.body))

    def test_content_header_frame_properties(self):
        value = self.write_frame.mock_calls[1][1][0].properties
        for key in self.msg.properties:
            self.assertEqual(self.msg.properties[key],
                             getattr(value, key))

    def test_publish_invokes_write_frame_with_body(self):
        self.assertIsInstance(self.write_frame.mock_calls[2][1][0],
                              body.ContentBody)

    def test_content_body_value(self):
        self.assertEqual(self.write_frame.mock_calls[2][1][0].value,
                         bytes(json.dumps(self.BODY).encode('utf-8')))
