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

from rabbitpy import base
from rabbitpy import exceptions
from rabbitpy import utils

LOGGER = logging.getLogger(__name__)


# Python 2.6 does not have a memoryview object, create dummy for isinstance
try:
    _PY_VERSION_CHECK = memoryview(b'foo')
except NameError:
    # pylint: disable=too-few-public-methods, redefined-builtin
    # pylint: disable=invalid-name, missing-docstring
    class memoryview(object):
        pass


class Properties(specification.Basic.Properties):
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

    .. note:: As of 0.21.0 ``auto_id`` is deprecated in favor of
    ``opinionated`` and it will be removed in a future version. As of
    0.22.0 ``opinionated`` is defaulted to ``False``.

    :param channel: The channel object for the message object to act upon
    :type channel: :py:class:`rabbitpy.channel.Channel`
    :param body_value: The message body
    :type body_value: str|bytes|unicode|memoryview|dict|json
    :param dict properties: A dictionary of message properties
    :param bool auto_id: Add a message id if no properties were passed in.
    :param bool opinionated: Automatically populate properties if True
    :raises KeyError: Raised when an invalid property is passed in

    """
    method = None
    name = 'Message'

    def __init__(self, channel, body_value, properties=None, auto_id=False,
                 opinionated=False):
        """Create a new instance of the Message object."""
        super(Message, self).__init__(channel, 'Message')

        # Always have a dict of properties set
        self.properties = properties or {}

        # Assign the body value
        if isinstance(body_value, memoryview):
            self.body = bytes(body_value)
        else:
            # pylint: disable=redefined-variable-type
            self.body = self._auto_serialize(body_value)

        # Add a message id if auto_id is not turned off and it is not set
        if (opinionated or auto_id) and 'message_id' not in self.properties:
            if auto_id:
                raise DeprecationWarning('Use opinionated instead of auto_id')
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
            msg = 'Invalid property: %s' % self._invalid_properties[0]
            raise KeyError(msg)

    @property
    def delivery_tag(self):
        """Return the delivery tag for a message that was delivered or gotten
        from RabbitMQ.

        :rtype: int or None

        """
        return self.method.delivery_tag if self.method else None

    @property
    def redelivered(self):
        """Indicates if this message may have been delivered before (but not
        acknowledged)"

        :rtype: bool or None

        """
        return self.method.redelivered if self.method else None

    @property
    def routing_key(self):
        """Return the routing_key for a message that was delivered or gotten
        from RabbitMQ.

        :rtype: int or None

        """
        return self.method.routing_key if self.method else None

    @property
    def exchange(self):
        """Return the source exchange for a message that was delivered or
        gotten from RabbitMQ.

        :rtype: string or None

        """
        return self.method.exchange if self.method else None

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
        self.channel.write_frame(basic_ack)

    def json(self):
        """Deserialize the message body if it is JSON, returning the value.

        :rtype: any

        """
        try:
            return json.loads(self.body)
        except TypeError:  # pragma: no cover
            return json.loads(self.body.decode('utf-8'))

    def nack(self, requeue=False, all_previous=False):
        """Negatively acknowledge receipt of the message to RabbitMQ. Will
        raise an ActionException if the message was not received from a broker.

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
        self.channel.write_frame(basic_nack)

    def pprint(self, properties=False):  # pragma: no cover
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

    def publish(self, exchange, routing_key='', mandatory=False,
                immediate=False):
        """Publish the message to the exchange with the specified routing
        key.

        In Python 2 if the message is a ``unicode`` value it will be converted
        to a ``str`` using ``str.encode('UTF-8')``. If you do not want the
        auto-conversion to take place, set the body to a ``str`` or ``bytes``
        value prior to publishing.

        In Python 3 if the message is a ``str`` value it will be converted to
        a ``bytes`` value using ``bytes(value.encode('UTF-8'))``. If you do
        not want the auto-conversion to take place, set the body to a
        ``bytes`` value prior to publishing.

        :param exchange: The exchange to publish the message to
        :type exchange: str or :class:`rabbitpy.Exchange`
        :param str routing_key: The routing key to use
        :param bool mandatory: Requires the message is published
        :param bool immediate: Request immediate delivery
        :return: bool or None
        :raises: rabbitpy.exceptions.MessageReturnedException

        """
        if isinstance(exchange, base.AMQPClass):
            exchange = exchange.name

        # Coerce the body to the proper type
        payload = utils.maybe_utf8_encode(self.body)

        frames = [specification.Basic.Publish(exchange=exchange,
                                              routing_key=routing_key or '',
                                              mandatory=mandatory,
                                              immediate=immediate),
                  header.ContentHeader(body_size=len(payload),
                                       properties=self._properties)]

        # Calculate how many body frames are needed
        pieces = int(math.ceil(len(payload) /
                               float(self.channel.maximum_frame_size)))

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
        self.channel.write_frame(basic_reject)

    def _add_auto_message_id(self):
        """Set the message_id property to a new UUID."""
        self.properties['message_id'] = str(uuid.uuid4())

    def _add_timestamp(self):
        """Add the timestamp to the properties"""
        self.properties['timestamp'] = datetime.datetime.utcnow()

    @staticmethod
    def _as_datetime(value):
        """Return the passed in value as a ``datetime.datetime`` value.

        :param value: The value to convert or pass through
        :type value: datetime.datetime
        :type value: time.struct_time
        :type value: int
        :type value: float
        :type value: str
        :type value: bytes
        :type value: unicode
        :rtype: datetime.datetime
        :raises: TypeError

        """
        if value is None:
            return None

        if isinstance(value, datetime.datetime):
            return value

        if isinstance(value, time.struct_time):
            return datetime.datetime(*value[:6])

        if utils.is_string(value):
            value = int(value)

        if isinstance(value, float) or isinstance(value, int):
            return datetime.datetime.fromtimestamp(value)

        raise TypeError('Could not cast a %s value to a datetime.datetime' %
                        type(value))

    def _auto_serialize(self, body_value):
        """Automatically serialize the body as JSON if it is a dict or list.

        :param mixed body_value: The message body passed into the constructor
        :return: bytes|str

        """
        if isinstance(body_value, dict) or isinstance(body_value, list):
            self.properties['content_type'] = 'application/json'
            return json.dumps(body_value, ensure_ascii=False)
        return body_value

    def _coerce_properties(self):
        """Force properties to be set to the correct data type"""
        for key, value in self.properties.items():
            _type = specification.Basic.Properties.type(key)
            if self.properties[key] is None:
                continue
            if _type == 'shortstr':
                if not utils.is_string(value):
                    LOGGER.warning('Coercing property %s to bytes', key)
                    value = str(value)
                self.properties[key] = utils.maybe_utf8_encode(value)
            elif _type == 'octet' and not isinstance(value, int):
                LOGGER.warning('Coercing property %s to int', key)
                try:
                    self.properties[key] = int(value)
                except TypeError as error:
                    LOGGER.warning('Could not coerce %s: %s', key, error)
            elif _type == 'table' and not isinstance(value, dict):
                LOGGER.warning('Resetting invalid value for %s to None', key)
                self.properties[key] = {}
            if key == 'timestamp':
                self.properties[key] = self._as_datetime(value)

    @property
    def _invalid_properties(self):
        """Return a list of invalid properties that currently exist in the the
        properties that are set.

        :rtype: list

        """
        return [key for key in self.properties
                if key not in specification.Basic.Properties.attributes()]

    @property
    def _properties(self):
        """Return a new Basic.Properties object representing the message
        properties.

        :rtype: pamqp.specification.Basic.Properties

        """
        self._prune_invalid_properties()
        self._coerce_properties()
        return specification.Basic.Properties(**self.properties)

    def _prune_invalid_properties(self):
        """Remove invalid properties from the message properties."""
        for key in self._invalid_properties:
            LOGGER.warning('Removing invalid property "%s"', key)
            del self.properties[key]
