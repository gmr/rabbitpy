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
import typing
import pprint
import uuid

from pamqp import body, commands, common, header

from rabbitpy import base, channel as chan, exceptions, exchange as exc, utils

LOGGER = logging.getLogger(__name__)


class Properties(commands.Basic.Properties):
    """Proxy class for :py:class:`pamqp.specification.Basic.Properties`"""
    pass


class Message(base.AMQPClass):
    """Created by both rabbitpy internally when a message is delivered or
    returned from RabbitMQ and by implementing applications, the Message class
    is used to publish a message to and access and respond to a message from
    RabbitMQ.

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
    * delivery_mode
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
    automatically be JSON serialized and the content type ``application/json``
    will be set on the message properties.

    When publishing a message to RabbitMQ, if the opinionated value is ``True``
    and no ``message_id`` value was passed in as a property, a UUID will be
    generated and specified as a property of the message.

    Additionally, if opinionated is ``True`` and the ``timestamp`` property
    is not specified when passing in ``properties``, the current Unix epoch
    value will be set in the message properties.

    :param channel: The channel object for the message object to act upon
    :param body_value: The message body
    :param properties: A dictionary of message properties
    :param opinionated: Automatically populate properties if True
    :raises KeyError: Raised when an invalid property is passed in

    """
    method = None
    name = 'Message'

    def __init__(self,
                 channel: chan.Channel,
                 body_value: typing.Union[str, bytes, memoryview, dict, list],
                 properties: typing.Optional[dict] = None,
                 opinionated: bool = False):
        """Create a new instance of the Message object."""
        super(Message, self).__init__(channel, 'Message')

        # Always have a dict of properties set
        self.properties = properties or {}

        # Assign the body value
        if isinstance(body_value, memoryview):
            self.body = bytes(body_value)
        else:
            self.body = self._auto_serialize(body_value)

        # Add a message id if auto_id is not turned off and it is not set
        if opinionated and 'message_id' not in self.properties:
            self._add_auto_message_id()

        if opinionated:
            if 'timestamp' not in self.properties:
                self._add_timestamp()

        # Enforce datetime timestamps
        if 'timestamp' in self.properties:
            self.properties['timestamp'] = \
                self._as_datetime(self.properties['timestamp'])

        # Don't let invalid property keys in
        if self._invalid_properties:
            raise KeyError('Invalid property: {}'.format(
                self._invalid_properties[0]))

    @property
    def delivery_tag(self) -> typing.Union[int, None]:
        """Return the delivery tag for a message that was delivered or gotten
        from RabbitMQ.

        """
        return self.method.delivery_tag if self.method else None

    @property
    def redelivered(self) -> typing.Union[bool, None]:
        """Indicates if this message may have been delivered before (but not
        acknowledged)

        """
        return self.method.redelivered if self.method else None

    @property
    def routing_key(self) -> typing.Union[str, None]:
        """Return the routing_key for a message that was delivered or gotten
        from RabbitMQ.

        """
        return self.method.routing_key if self.method else None

    @property
    def exchange(self) -> typing.Union[str, None]:
        """Return the source exchange for a message that was delivered or
        gotten from RabbitMQ.

        """
        return self.method.exchange if self.method else None

    def ack(self, all_previous: bool = False) -> None:
        """Acknowledge receipt of the message to RabbitMQ. Will raise an
        ActionException if the message was not received from a broker.

        :raises: ActionException

        """
        if not self.method:
            raise exceptions.ActionException(
                'Can not ack non-received message')
        basic_ack = commands.Basic.Ack(self.method.delivery_tag,
                                       multiple=all_previous)
        self.channel.write_frame(basic_ack)

    def json(self) -> bytes:
        """Deserialize the message body if it is JSON, returning the value."""
        try:
            return json.loads(self.body)
        except TypeError:  # pragma: no cover
            return json.loads(self.body.decode('utf-8'))

    def nack(self, requeue: bool = False, all_previous: bool = False) -> None:
        """Negatively acknowledge receipt of the message to RabbitMQ. Will
        raise an ActionException if the message was not received from a broker.

        :param requeue: Requeue the message
        :param all_previous: Nack all previous unacked messages up to and
                             including this one
        :raises: ActionException

        """
        if not self.method:
            raise exceptions.ActionException(
                'Can not nack non-received message')
        basic_nack = commands.Basic.Nack(self.method.delivery_tag,
                                         requeue=requeue,
                                         multiple=all_previous)
        self.channel.write_frame(basic_nack)

    def pprint(self, properties: bool = False) -> None:  # pragma: no cover
        """Print a formatted representation of the message.

        :param properties: Include properties in the representation

        """
        print('Exchange: %s\n' % self.method.exchange)
        print('Routing Key: %s\n' % self.method.routing_key)
        if properties:
            print('Properties:\n')
            pprint.pprint(self.properties)
            print('\nBody:\n')
        pprint.pprint(self.body)

    def publish(self,
                exchange: typing.Union[str, exc.Exchange],
                routing_key: str = '',
                mandatory: bool = False,
                immediate: bool = False) -> typing.Union[bool, None]:
        """Publish the message to the exchange with the specified routing
        key.

        If the message is a ``str`` value it will be converted to
        a ``bytes`` value using ``bytes(value.encode('UTF-8'))``. If you do
        not want the auto-conversion to take place, set the body to a
        ``bytes`` value prior to publishing.

        :param exchange: The exchange to publish the message to
        :param routing_key: The routing key to use
        :param mandatory: Requires the message is published
        :param immediate: Request immediate delivery
        :raises: rabbitpy.exceptions.MessageReturnedException

        """
        if isinstance(exchange, base.AMQPClass):
            exchange = exchange.name

        # Coerce the body to the proper type
        payload = utils.maybe_utf8_encode(self.body)

        frames = [
            commands.Basic.Publish(exchange=exchange,
                                   routing_key=routing_key or '',
                                   mandatory=mandatory,
                                   immediate=immediate),
            header.ContentHeader(body_size=len(payload),
                                 properties=self._properties)
        ]

        # Calculate how many body frames are needed
        pieces = int(
            math.ceil(len(payload) / float(self.channel.maximum_frame_size)))

        # Send the message
        for offset in range(0, pieces):
            start = self.channel.maximum_frame_size * offset
            end = start + self.channel.maximum_frame_size
            if end > len(payload):
                end = len(payload)
            frames.append(body.ContentBody(payload[start:end]))

        # Write the frames out
        self.channel.write_frames(frames)

        # If publisher confirmations are enabled, wait for the response
        if self.channel.publisher_confirms:
            response = self.channel.wait_for_confirmation()
            if isinstance(response, commands.Basic.Ack):
                return True
            elif isinstance(response, commands.Basic.Nack):
                return False
            else:
                raise exceptions.UnexpectedResponseError(response)

    def reject(self, requeue: bool = False) -> None:
        """Reject receipt of the message to RabbitMQ. Will raise
        an ActionException if the message was not received from a broker.

        :param requeue: Requeue the message
        :raises: ActionException

        """
        if not self.method:
            raise exceptions.ActionException(
                'Can not reject non-received message')
        basic_reject = commands.Basic.Reject(self.method.delivery_tag,
                                             requeue=requeue)
        self.channel.write_frame(basic_reject)

    def _add_auto_message_id(self) -> None:
        """Set the message_id property to a new UUID."""
        self.properties['message_id'] = str(uuid.uuid4())

    def _add_timestamp(self) -> None:
        """Add the timestamp to the properties"""
        self.properties['timestamp'] = datetime.datetime.utcnow()

    @staticmethod
    def _as_datetime(value: typing.Union[datetime.datetime,
                                         time.struct_time, int, float,
                                         str, bytes]) \
            -> typing.Union[datetime.datetime, None]:
        """Return the passed in value as a ``datetime.datetime`` value.

        :param value: The value to convert or pass through
        :raises: TypeError

        """
        if value is None:
            return None

        if isinstance(value, datetime.datetime):
            return value

        if isinstance(value, time.struct_time):
            return datetime.datetime(*value[:6])

        if isinstance(value, (bytes, str)):
            value = int(value)

        if isinstance(value, float) or isinstance(value, int):
            return datetime.datetime.fromtimestamp(value)

        raise TypeError(
            f'Could not cast a {value} value to a datetime.datetime')

    def _auto_serialize(self, value: typing.Union[str, bytes, memoryview,
                                                  dict, list]) \
            -> typing.Union[str, bytes]:
        """Automatically serialize the body as JSON if it is a dict or list.

        :param value: The message body passed into the constructor

        """
        if isinstance(value, dict) or isinstance(value, list):
            self.properties['content_type'] = 'application/json'
            return json.dumps(value, ensure_ascii=False)
        return value

    def _coerce_properties(self) -> None:
        """Force properties to be set to the correct data type"""
        for key, value in self.properties.items():
            if self.properties[key] is None:
                continue
            if commands.Basic.Properties.__annotations__[key] == str:
                if not isinstance(value, (bytes, str)):
                    LOGGER.warning('Coercing property %s to bytes', key)
                    value = str(value)
                self.properties[key] = utils.maybe_utf8_encode(value)
            elif commands.Basic.Properties.__annotations__[key] == int:
                LOGGER.warning('Coercing property %s to int', key)
                try:
                    self.properties[key] = int(value)
                except TypeError as error:
                    LOGGER.warning('Could not coerce %s: %s', key, error)
            elif commands.Basic.Properties.__annotations__[key] == \
                    common.FieldTable and not isinstance(value, dict):
                LOGGER.warning('Resetting invalid value for %s to None', key)
                self.properties[key] = {}
            if commands.Basic.Properties.__annotations__[key] == \
                    datetime.datetime:
                self.properties[key] = self._as_datetime(value)

    @property
    def _invalid_properties(self) -> typing.List[str]:
        """Return a list of invalid properties that currently exist in the the
        properties that are set.

        """
        return [
            key for key in self.properties
            if key not in commands.Basic.Properties.attributes()
        ]

    @property
    def _properties(self) -> commands.Basic.Properties:
        """Return a new Basic.Properties object representing the message
        properties.

        """
        self._prune_invalid_properties()
        self._coerce_properties()
        return commands.Basic.Properties(**self.properties)

    def _prune_invalid_properties(self) -> None:
        """Remove invalid properties from the message properties."""
        for key in self._invalid_properties:
            LOGGER.warning('Removing invalid property "%s"', key)
            del self.properties[key]
