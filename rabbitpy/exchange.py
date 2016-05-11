"""
The :py:class:`Exchange` class is used to create and manage exchanges in
RabbitMQ and provides four classes as wrappers:

* :py:class:`DirectExchange`
* :py:class:`FanoutExchange`
* :py:class:`HeadersExchange`
* :py:class:`TopicExchange`

"""
import logging
from pamqp import specification

from rabbitpy import base

LOGGER = logging.getLogger(__name__)


class _Exchange(base.AMQPClass):
    """Exchange class for interacting with an exchange in RabbitMQ including
    declaration, binding and deletion.

    :param channel: The channel object to communicate on
    :type channel: :py:class:`rabbitpy.channel.Channel`
    :param str name: The name of the exchange
    :param str exchange_type: The exchange type
    :param bool durable: Request a durable exchange
    :param bool auto_delete: Automatically delete when not in use
    :param dict arguments: Optional key/value arguments

    """
    durable = False
    arguments = dict()
    auto_delete = False
    type = 'direct'

    def __init__(self, channel, name, durable=False, auto_delete=False,
                 arguments=None):
        """Create a new instance of the exchange object."""
        super(_Exchange, self).__init__(channel, name)
        self.durable = durable
        self.auto_delete = auto_delete
        self.arguments = arguments or dict()

    def bind(self, source, routing_key=None):
        """Bind to another exchange with the routing key.

        :param source: The exchange to bind to
        :type source: str or :py:class:`rabbitpy.Exchange`
        :param str routing_key: The routing key to use

        """
        if hasattr(source, 'name'):
            source = source.name
        self._rpc(specification.Exchange.Bind(destination=self.name,
                                              source=source,
                                              routing_key=routing_key))

    def declare(self, passive=False):
        """Declare the exchange with RabbitMQ. If passive is True and the
        command arguments do not match, the channel will be closed.

        :param bool passive: Do not actually create the exchange

        """
        self._rpc(specification.Exchange.Declare(exchange=self.name,
                                                 exchange_type=self.type,
                                                 durable=self.durable,
                                                 passive=passive,
                                                 auto_delete=self.auto_delete,
                                                 arguments=self.arguments))

    def delete(self, if_unused=False):
        """Delete the exchange from RabbitMQ.

        :param bool if_unused: Delete only if unused

        """
        self._rpc(specification.Exchange.Delete(exchange=self.name,
                                                if_unused=if_unused))

    def unbind(self, source, routing_key=None):
        """Unbind the exchange from the source exchange with the
        routing key. If routing key is None, use the queue or exchange name.

        :param source: The exchange to unbind from
        :type source: str or :py:class:`rabbitpy.Exchange`
        :param str routing_key: The routing key that binds them

        """
        if hasattr(source, 'name'):
            source = source.name
        self._rpc(specification.Exchange.Unbind(destination=self.name,
                                                source=source,
                                                routing_key=routing_key))


class Exchange(_Exchange):
    """Exchange class for interacting with an exchange in RabbitMQ including
    declaration, binding and deletion.

    :param channel: The channel object to communicate on
    :type channel: :py:class:`rabbitpy.channel.Channel`
    :param str name: The name of the exchange
    :param str exchange_type: The exchange type
    :param bool durable: Request a durable exchange
    :param bool auto_delete: Automatically delete when not in use
    :param dict arguments: Optional key/value arguments

    """
    def __init__(self, channel, name, exchange_type='direct',
                 durable=False, auto_delete=False,
                 arguments=None):
        """Create a new instance of the exchange object."""
        self.type = exchange_type
        super(Exchange, self).__init__(channel, name, durable, auto_delete,
                                       arguments)


class DirectExchange(_Exchange):
    """The DirectExchange class is used for interacting with direct exchanges
    only.

    :param channel: The channel object to communicate on
    :type channel: :py:class:`rabbitpy.channel.Channel`
    :param str name: The name of the exchange
    :param bool durable: Request a durable exchange
    :param bool auto_delete: Automatically delete when not in use
    :param dict arguments: Optional key/value arguments

    """
    type = 'direct'


class FanoutExchange(_Exchange):
    """The FanoutExchange class is used for interacting with fanout exchanges
    only.

    :param channel: The channel object to communicate on
    :type channel: :py:class:`rabbitpy.channel.Channel`
    :param str name: The name of the exchange
    :param bool durable: Request a durable exchange
    :param bool auto_delete: Automatically delete when not in use
    :param dict arguments: Optional key/value arguments

    """
    type = 'fanout'


class HeadersExchange(_Exchange):
    """The HeadersExchange class is used for interacting with direct exchanges
    only.

    :param channel: The channel object to communicate on
    :type channel: :py:class:`rabbitpy.channel.Channel`
    :param str name: The name of the exchange
    :param bool durable: Request a durable exchange
    :param bool auto_delete: Automatically delete when not in use
    :param dict arguments: Optional key/value arguments

    """
    type = 'headers'


class TopicExchange(_Exchange):
    """The TopicExchange class is used for interacting with topic exchanges
    only.

    :param channel: The channel object to communicate on
    :type channel: :py:class:`rabbitpy.channel.Channel`
    :param str name: The name of the exchange
    :param bool durable: Request a durable exchange
    :param bool auto_delete: Automatically delete when not in use
    :param dict arguments: Optional key/value arguments

    """
    type = 'topic'
