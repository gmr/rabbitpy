"""
The :py:class:`Exchange` class is used to create and manage exchanges in
RabbitMQ and provides four classes as wrappers:

* :py:class:`DirectExchange`
* :py:class:`FanoutExchange`
* :py:class:`HeadersExchange`
* :py:class:`TopicExchange`

"""

import logging
import typing

from pamqp import commands

from rabbitpy import base
from rabbitpy import channel as chan

LOGGER = logging.getLogger(__name__)

ExchangeTypes = typing.Union[
    str,
    '_Exchange',
    'Exchange',
    'DirectExchange',
    'FanoutExchange',
    'HeadersExchange',
    'TopicExchange',
]


class _Exchange(base.AMQPClass):
    """Exchange class for interacting with an exchange in RabbitMQ including
    declaration, binding and deletion.

    :param channel: The channel object to communicate on
    :param name: The name of the exchange
    :param durable: Request a durable exchange
    :param auto_delete: Automatically delete when not in use
    :param arguments: Optional key/value arguments

    """

    durable = False
    arguments = {}
    auto_delete = False
    type = 'direct'

    def __init__(
        self,
        channel: chan.Channel,
        name: str,
        durable: bool = False,
        auto_delete: bool = False,
        arguments: dict | None = None,
    ):
        """Create a new instance of the exchange object."""
        super().__init__(channel, name)
        self.durable = durable
        self.auto_delete = auto_delete
        self.arguments = arguments or {}

    def bind(
        self, source: ExchangeTypes, routing_key: str | None = None
    ) -> None:
        """Bind to another exchange with the routing key.

        :param source: The exchange to bind to
        :param routing_key: The routing key to use

        """
        if hasattr(source, 'name'):
            source = source.name
        self._rpc(
            commands.Exchange.Bind(
                destination=self.name, source=source, routing_key=routing_key
            )
        )

    def declare(self, passive: bool = False) -> None:
        """Declare the exchange with RabbitMQ. If passive is True and the
        command arguments do not match, the channel will be closed.

        :param passive: Do not actually create the exchange

        """
        self._rpc(
            commands.Exchange.Declare(
                exchange=self.name,
                exchange_type=self.type,
                durable=self.durable,
                passive=passive,
                auto_delete=self.auto_delete,
                arguments=self.arguments,
            )
        )

    def delete(self, if_unused: bool = False) -> None:
        """Delete the exchange from RabbitMQ.

        :param bool if_unused: Delete only if unused

        """
        self._rpc(
            commands.Exchange.Delete(exchange=self.name, if_unused=if_unused)
        )

    def unbind(
        self, source: ExchangeTypes, routing_key: str | None = None
    ) -> None:
        """Unbind the exchange from the source exchange with the
        routing key. If routing key is None, use the queue or exchange name.

        :param source: The exchange to unbind from
        :param routing_key: The routing key that binds them

        """
        if hasattr(source, 'name'):
            source = source.name
        self._rpc(
            commands.Exchange.Unbind(
                destination=self.name, source=source, routing_key=routing_key
            )
        )


class Exchange(_Exchange):
    """Exchange class for interacting with an exchange in RabbitMQ including
    declaration, binding and deletion.

    :param channel: The channel object to communicate on
    :param name: The name of the exchange
    :param exchange_type: The exchange type
    :param durable: Request a durable exchange
    :param auto_delete: Automatically delete when not in use
    :param arguments: Optional key/value arguments

    """

    def __init__(
        self,
        channel: chan.Channel,
        name: str,
        exchange_type: str = 'direct',
        durable: bool = False,
        auto_delete: bool = False,
        arguments: bool | None = None,
    ):
        """Create a new instance of the exchange object."""
        self.type = exchange_type
        super().__init__(channel, name, durable, auto_delete, arguments)


class DirectExchange(_Exchange):
    """The DirectExchange class is used for interacting with direct exchanges
    only.

    :param channel: The channel object to communicate on
    :param name: The name of the exchange
    :param durable: Request a durable exchange
    :param auto_delete: Automatically delete when not in use
    :param arguments: Optional key/value arguments

    """

    type = 'direct'


class FanoutExchange(_Exchange):
    """The FanoutExchange class is used for interacting with fanout exchanges
    only.

    :param channel: The channel object to communicate on
    :param name: The name of the exchange
    :param durable: Request a durable exchange
    :param auto_delete: Automatically delete when not in use
    :param arguments: Optional key/value arguments

    """

    type = 'fanout'


class HeadersExchange(_Exchange):
    """The HeadersExchange class is used for interacting with direct exchanges
    only.

    :param channel: The channel object to communicate on
    :param name: The name of the exchange
    :param durable: Request a durable exchange
    :param auto_delete: Automatically delete when not in use
    :param arguments: Optional key/value arguments

    """

    type = 'headers'


class TopicExchange(_Exchange):
    """The TopicExchange class is used for interacting with topic exchanges
    only.

    :param channel: The channel object to communicate on
    :param name: The name of the exchange
    :param durable: Request a durable exchange
    :param auto_delete: Automatically delete when not in use
    :param arguments: Optional key/value arguments

    """

    type = 'topic'
