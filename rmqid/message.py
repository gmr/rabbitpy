"""
The Message class represents a message that is sent or received

"""
import datetime
import json
import logging
import math
import time
import uuid

from pamqp import body
from pamqp import header
from pamqp import specification

from rmqid import base
from rmqid import exceptions

LOGGER = logging.getLogger(__name__)


class Properties(specification.Basic.Properties):
    pass


class Message(base.AMQPClass):
    """Represent a message for delivery and receipt from RabbitMQ"""
    method = None
    name = 'Message'

    def __init__(self, channel, body_value, properties=None, auto_id=True):
        """Create a new instance of the Message object.

        :param rmqid.channel.Channel channel: The channel object for the message
        :param str body_value: The message body
        :param dict properties: A dictionary of message properties
        :param bool auto_id: Add a message id if no properties were passed in.

        """
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
        return specification.Basic.Properties(**self.properties)

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

    def publish(self, exchange, routing_key='', mandatory=False):
        """Publish the message to the exchange with the specified routing
        key.

        :param str | rmqid.base.AMQPClass exchange: The exchange to bind to
        :param str routing_key: The routing key to use
        :param bool mandatory: Requires the message is published
        :return: bool | None
        :raises: rmqid.exceptions.MessageReturnedException

        """
        if isinstance(exchange, base.AMQPClass):
            exchange = exchange.name
        method_frame = specification.Basic.Publish(exchange=exchange,
                                                   routing_key=routing_key,
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
