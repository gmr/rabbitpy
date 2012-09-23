"""
The Message class represents a message that is sent or received

"""
import logging
import math
from pamqp import body
from pamqp import header
from pamqp import specification
import time

from rmqid import base

LOGGER = logging.getLogger(__name__)


class Message(base.AMQPClass):

    def __init__(self, channel, body_value, properties):
        super(Message, self).__init__(channel, 'Message')
        self.body = body_value
        self.properties = properties or self._base_properties

    @property
    def _base_properties(self):
        return {"timestamp": time.time()}

    @property
    def _properties(self):

        invalid_keys = [key for key in self.properties
                        if key not in specification.Basic.Properties.attributes]
        self._prune_invalid_properties(invalid_keys)
        return specification.Basic.Properties(**self.properties)

    def _prune_invalid_properties(self, invalid_keys):
        for key in invalid_keys:
            LOGGER.warning('Removing invalid property "%s"', key)
            del self.properties[key]

    def publish(self, exchange, routing_key=''):
        """Publish the message to the exchange with the specified routing
        key.

        :param str | rmqid.base.AMQPClass exchange: The exchange to bind to
        :param str routing_key: The routing key to use

        """
        if isinstance(exchange, base.AMQPClass):
            exchange = exchange.name
        method_frame = specification.Basic.Publish(exchange=exchange,
                                                   routing_key=routing_key)
        self.channel.write_frame(method_frame)
        header_frame = header.ContentHeader(body_size=len(self.body),
                                            properties=self._properties)
        self.channel.write_frame(header_frame)
        pieces = int(math.ceil(len(self.body) /
                               float(self.channel.maximum_frame_size)))
        for offset in xrange(0, pieces):
            start = self.channel.maximum_frame_size * offset
            end = start + self.channel.maximum_frame_size
            if end > len(self.body):
                end = len(self.body)
            self.channel.write_frame(body.ContentBody(self.body[start:end]))

