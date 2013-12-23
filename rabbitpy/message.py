"""
The Message class represents a message that is sent or received and contains
methods for publishing the message, or in the case that the message was
delivered by RabbitMQ, acknowledging it, rejecting it or negatively
acknowledging it.

"""
import datetime
import json
import logging
import math
import time
import pprint
import uuid

from pamqp import body
from pamqp import header
from pamqp import specification

from rabbitpy import DEBUG
from rabbitpy import base
from rabbitpy import exceptions

LOGGER = logging.getLogger(__name__)


class Properties(specification.Basic.Properties):
    """Proxy class for :py:class:`pamqp.specification.Basic.Properties`"""
    pass


class Message(base.AMQPClass):
    """Created by both rabbitpy internally when a message is delivered or returned
    from RabbitMQ and by implementing applications, the Message class is used
    to publish a message to and access and respond to a message from RabbitMQ.

    When specifying properties for a message, pass in a dict of key value items
    that match the AMQP Basic.Properties specification with a small caveat.

    Due to an overlap in the AMQP specification and the Python keyword
    :code:`type`, the :code:`type` property is referred to as
    :code:`message_type`.

    The following is a list of the available properties:

    * app_id
    * content_type
    * content_encoding
    * correlation_id
    * delivery_node
    * expiration
    * headers
    * message_id
    * message_type
    * priority
    * reply_to
    * timestamp
    * user_id

    **Automated features**

    When passing in the body value, if it is a dict or list, it will
    automatically be JSON serialized and the content type "application/json"
    will be set on the message properties.

    When publishing a message to RabbitMQ, if the auto_id value is True and no
    message_id value was passed in as a property, a UUID will be generated and
    specified as a property of the message.

    If a timestamp is not specified when passing in properties, the current
    Unix epoch value will be set in the message properties.

    :param channel: The channel object for the message object to act upon
    :type channel: :py:class:`rabbitpy.channel.Channel`
    :param str or dict or list body_value: The message body
    :param dict properties: A dictionary of message properties
    :param bool auto_id: Add a message id if no properties were passed in.

    """
    method = None
    name = 'Message'

    def __init__(self, channel, body_value, properties=None, auto_id=True):
        """Create a new instance of the Message object."""
        super(Message, self).__init__(channel, 'Message')

        if isinstance(body_value, dict) or isinstance(body_value, list):
            body_value = json.dumps(body_value, ensure_ascii=False)
            if properties is None:
                properties = {'content_type': 'application/json'}
            else:
                properties['content_type'] = 'application/json'
        self.body = body_value
        self.properties = properties or self._base_properties
        if auto_id and 'message_id' not in self.properties:
            self._add_auto_message_id()
        if 'timestamp' not in self.properties:
            self._add_timestamp()
        if isinstance(self.properties['timestamp'], time.struct_time):
            self.properties['timestamp'] = self._timestamp_from_struct_time()

    def _add_auto_message_id(self):
        """Set the message_id property to a new UUID."""
        self.properties['message_id'] = str(uuid.uuid4())

    def _add_timestamp(self):
        """Add the timestamp to the properties"""
        self.properties['timestamp'] = datetime.datetime.now()

    def _timestamp_from_struct_time(self):
        return datetime.datetime(*self.properties['timestamp'][:6])

    @property
    def _base_properties(self):
        """Return a base set of properties if no properties were passed into
        the constructor.

        :rtype: dict

        """
        return {"message_id": str(uuid.uuid4()),
                "timestamp": datetime.datetime.now()}

    @property
    def _properties(self):
        """Return a new Basic.Properties object representing the message
        properties.

        :rtype: pamqp.specification.Basic.Properties

        """
        invalid_keys = [key for key in self.properties
                        if key not in specification.Basic.Properties.attributes]
        self._prune_invalid_properties(invalid_keys)
        self._coerce_properties()
        return specification.Basic.Properties(**self.properties)

    def _coerce_properties(self):
        """Force properties to be set to the correct data type"""
        for key in self.properties:
            _type = getattr(specification.Basic.Properties, key)
            if DEBUG:
                LOGGER.debug('Type: %s, %s', _type, type(self.properties[key]))
            if _type == 'shortstr' and not isinstance(self.properties[key],
                                                      basestring):
                LOGGER.warning('Coercing property %s to bytes', key)
                self.properties[key] = bytes(self.properties[key])
            elif _type == 'octet' and not isinstance(self.properties[key], int):
                LOGGER.warning('Coercing property %s to int', key)
                self.properties[key] = int(self.properties[key])
            elif _type == 'table' and not isinstance(self.properties[key],
                                                     dict):
                LOGGER.warning('Resetting invalid value for %s to None', key)
                self.properties[key] = {}
            elif _type == 'timestamp' and isinstance(self.properties[key],
                                                     int):
                LOGGER.warning('Coercing property %s to datetime', key)
                self.properties[key] = \
                    datetime.datetime.fromtimestamp(self.properties[key])

    def _prune_invalid_properties(self, invalid_keys):
        """Remove invalid properties from the message properties.

        :param list invalid_keys: A list of invalid property names to remove

        """
        for key in invalid_keys:
            LOGGER.warning('Removing invalid property "%s"', key)
            del self.properties[key]

    def ack(self, all_previous=False):
        """Acknowledge receipt of the message to RabbitMQ. Will raise an
        ActionException if the message was not received from a broker.

        :raises: ActionException

        """
        if not self.method:
            raise exceptions.ActionException('Can not ack non-received '
                                             'message')
        basic_ack = specification.Basic.Ack(self.method.delivery_tag,
                                            multiple=all_previous)
        self.channel._write_frame(basic_ack)

    def json(self):
        """Deserialize the message body if it is JSON, returning the value.

        :rtype: any

        """
        return json.loads(self.body)

    def nack(self, requeue=False, all_previous=False):
        """Negatively acknowledge receipt of the message to RabbitMQ. Will raise
        an ActionException if the message was not received from a broker.

        :param bool requeue: Requeue the message
        :param bool all_previous: Nack all previous unacked messages up to and
                                  including this one
        :raises: ActionException

        """
        if not self.method:
            raise exceptions.ActionException('Can not nack non-received '
                                             'message')
        basic_nack = specification.Basic.Nack(self.method.delivery_tag,
                                              requeue=requeue,
                                              multiple=all_previous)
        self.channel._write_frame(basic_nack)

    def pprint(self, properties=False):
        """Print a formatted representation of the message.

        :param bool properties: Include properties in the representation

        """
        print('Exchange: %s\n' % self.method.exchange)
        print('Routing Key: %s\n' % self.method.routing_key)
        if properties:
            print('Properties:\n')
            pprint.pprint(self.properties)
            print('\nBody:\n')
        pprint.pprint(self.body)

    def publish(self, exchange, routing_key='', mandatory=False):
        """Publish the message to the exchange with the specified routing
        key.

        :param str | rabbitpy.base.AMQPClass exchange: The exchange to bind to
        :param str routing_key: The routing key to use
        :param bool mandatory: Requires the message is published
        :return: bool | None
        :raises: rabbitpy.exceptions.MessageReturnedException

        """
        if isinstance(exchange, base.AMQPClass):
            exchange = exchange.name
        method_frame = specification.Basic.Publish(exchange=exchange,
                                                   routing_key=routing_key or
                                                               '',
                                                   mandatory=mandatory)
        self.channel._write_frame(method_frame)
        header_frame = header.ContentHeader(body_size=len(self.body),
                                            properties=self._properties)
        self.channel._write_frame(header_frame)
        pieces = int(math.ceil(len(self.body) /
                               float(self.channel.maximum_frame_size)))

        # Send the message
        for offset in range(0, pieces):
            start = self.channel.maximum_frame_size * offset
            end = start + self.channel.maximum_frame_size
            if end > len(self.body):
                end = len(self.body)
            self.channel._write_frame(body.ContentBody(self.body[start:end]))

        # If publisher confirmations are enabled, wait for the response
        if self.channel.publisher_confirms:
            response = self.channel._wait_for_confirmation()
            if isinstance(response, specification.Basic.Ack):
                return True
            elif isinstance(response, specification.Basic.Nack):
                return False
            else:
                raise exceptions.UnexpectedResponseError(response)

    def reject(self, requeue=False):
        """Reject receipt of the message to RabbitMQ. Will raise
        an ActionException if the message was not received from a broker.

        :param bool requeue: Requeue the message
        :raises: ActionException

        """
        if not self.method:
            raise exceptions.ActionException('Can not reject non-received '
                                             'message')
        basic_reject = specification.Basic.Reject(self.method.delivery_tag,
                                                  requeue=requeue)
        self.channel._write_frame(basic_reject)
