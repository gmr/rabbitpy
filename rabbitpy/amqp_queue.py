"""
The rabbitpy.amqp_queue module contains two classes :py:class:`Queue` and
:py:class:`Consumer`. The :py:class:`Queue` class is an object that is used
create and work with queues on a RabbitMQ server.

To consume messages you can iterate over the Queue object itself if the
defaults for the :py:meth:`Queue.__iter__() <Queue.__iter__>` method work
for your needs:

.. code:: python

    with conn.channel() as channel:
        for message in rabbitpy.Queue(channel, 'example'):
            print('Message: %r' % message)
            message.ack()

or by the :py:meth:`Queue.consume() <Queue.consume>` method
if you would like to specify `no_ack`, `prefetch_count`, or `priority`:

.. code:: python

    with conn.channel() as channel:
        queue = rabbitpy.Queue(channel, 'example')
        for message in queue.consume:
            print('Message: %r' % message)
            message.ack()

"""

from __future__ import annotations

import logging
import typing

from pamqp import commands

from rabbitpy import base, exceptions, exchange, message, utils

if typing.TYPE_CHECKING:
    from rabbitpy import channel as chan

LOGGER = logging.getLogger(__name__)


class Queue(base.AMQPClass):
    """Create and manage RabbitMQ queues.

    :param channel: The channel object to communicate on
    :param name: The queue name
    :param exclusive: The queue can only be used by this channel and will
                      auto-delete once the channel is closed.
    :param durable: Indicates if the queue should survive a RabbitMQ is restart
    :param auto_delete: Automatically delete when all consumers disconnect
    :param max_length: Maximum queue length
    :param message_ttl: Time-to-live of a message in milliseconds
    :param expires: Number of milliseconds until a queue is removed after
                    becoming idle
    :param dead_letter_exchange: Dead letter exchange for rejected messages
    :param dead_letter_routing_key: Routing key for dead lettered messages
    :param arguments: Custom arguments for the queue

    :attributes:
      - **consumer_tag** (*str*) – Contains the consumer tag used to register
        with RabbitMQ. Can be overwritten with custom value prior to consuming.

    :raises: :py:exc:`~rabbitpy.exceptions.RemoteClosedChannelException`
    :raises: :py:exc:`~rabbitpy.exceptions.RemoteCancellationException`

    """

    arguments = {}
    auto_delete = False
    dead_letter_exchange = None
    dead_letter_routing_key = None
    durable = False
    exclusive = False
    expires = None
    max_length = None
    message_ttl = None

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        channel: chan.Channel,
        name: str = '',
        durable: bool = False,
        exclusive: bool = False,
        auto_delete: bool = False,
        max_length: int | None = None,
        message_ttl: int | None = None,
        expires: int | None = None,
        dead_letter_exchange: str | None = None,
        dead_letter_routing_key: str | None = None,
        arguments: dict | None = None,
    ):
        """Create a new Queue object instance. Only the
        :py:class:`rabbitpy.Channel` object is required.

        .. warning:: You should only use a single
             :py:class:`~rabbitpy.Queue` instance per channel
             when consuming or getting messages. Failure to do so can
             have unintended consequences.

        """
        if name is None:
            raise ValueError('Queue name may not be None')
        super().__init__(channel, name)

        # Defaults
        self.consumer_tag = f'rabbitpy.{self.channel.id}.{id(self)}'
        self.consuming = False

        # Assign Arguments
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.arguments = arguments or {}
        self.max_length = max_length
        self.message_ttl = message_ttl
        self.expires = expires
        self.dead_letter_exchange = dead_letter_exchange
        self.dead_letter_routing_key = dead_letter_routing_key

    def __iter__(self) -> typing.Iterable[message.Message]:
        """Quick way to consume messages using defaults of ``no_ack=False``,
        prefetch and priority not set.

        .. warning:: You should only use a single :py:class:`~rabbitpy.Queue`
             instance per channel when consuming messages. Failure to do so can
             have unintended consequences.

        :yields: :class:`~rabbitpy.Message`

        """
        return self.consume()

    def __len__(self) -> int:
        """Return the pending number of messages in the queue by doing a
        passive Queue declare.

        """
        response = self._rpc(self._declare(True))
        return response.message_count

    def __setattr__(self, name: str, value: typing.Any) -> None:
        """Validate the data types for specific attributes when setting them,
        otherwise fall throw to the parent ``__setattr__``

        :param name: The attribute to set
        :param value: The value to set
        :raises: ValueError

        """
        if value is not None:
            if name in [
                'auto_delete',
                'durable',
                'exclusive',
            ] and not isinstance(value, bool):
                raise ValueError(f'{name} must be True or False')
            elif name in [
                'max_length',
                'message_ttl',
                'expires',
            ] and not isinstance(value, int):
                raise ValueError(f'{name} must be an int')
            elif name in [
                'consumer_tag',
                'dead_letter_exchange',
                'dead_letter_routing_key',
            ] and not isinstance(value, (bytes, str)):
                raise ValueError(f'{name} must be a str or bytes')
            elif name == 'arguments' and not isinstance(value, dict):
                raise ValueError('arguments must be a dict')
        super().__setattr__(name, value)

    def bind(
        self,
        source: exchange.ExchangeTypes,
        routing_key: str | None = None,
        arguments: dict | None = None,
    ) -> bool:
        """Bind the queue to the specified exchange or routing key.

        :param source: The exchange to bind to
        :param routing_key: The routing key to use
        :param arguments: Optional arguments for RabbitMQ

        """
        if hasattr(source, 'name'):
            source = source.name
        frame = commands.Queue.Bind(
            queue=self.name,
            exchange=source,
            routing_key=routing_key or '',
            arguments=arguments,
        )
        response = self._rpc(frame)
        return isinstance(response, commands.Queue.BindOk)

    def consume(
        self,
        no_ack: bool = False,
        prefetch: int | None = None,
        priority: int | None = None,
        consumer_tag: str | None = None,
    ) -> typing.Generator[message.Message, None, None]:
        """Consume messages from the queue as a :py:class:`generator`:

        .. code:: python
            for message in queue.consume():
                message.ack()

        You can use this method instead of the queue object as an iterator
        if you need to alter the prefect count, set the consumer priority or
        consume in no_ack mode.

        .. warning:: You should only use a single :py:class:`~rabbitpy.Queue`
             instance per channel when consuming messages. Failure to do so can
             have unintended consequences.

        :param no_ack: Do not require acknowledgements
        :param prefetch: Set a prefetch count for the channel
        :param priority: Consumer priority
        :param consumer_tag: Optional consumer tag
        :raises: :exc:`~rabbitpy.exceptions.RemoteCancellationException`

        """
        if consumer_tag:
            self.consumer_tag = consumer_tag
        self._register_consumer(no_ack, prefetch, priority)
        try:
            while self.consuming:
                value = self.channel.consume_message()
                if value:
                    yield value
                else:
                    if self.consuming:
                        self.stop_consuming()
                    break
        finally:
            if self.consuming:
                self.stop_consuming()

    def declare(self, passive: bool = False) -> tuple[int, int]:
        """Declare the queue on the RabbitMQ channel passed into the
        constructor, returning the current message count for the queue and
        its consumer count as a tuple.

        :param bool passive: Passive declare to retrieve message count and
                             consumer count information
        :return: Message count, Consumer count
        :rtype: tuple(int, int)

        """
        response = self._rpc(self._declare(passive))
        if not self.name:
            self.name = response.queue
        return response.message_count, response.consumer_count

    def delete(self, if_unused: bool = False, if_empty: bool = False) -> None:
        """Delete the queue

        :param if_unused: Delete only if unused
        :param if_empty: Delete only if empty

        """
        self._rpc(
            commands.Queue.Delete(
                queue=self.name, if_unused=if_unused, if_empty=if_empty
            )
        )

    def get(self, acknowledge: bool = True) -> message.Message | None:
        """Request a single message from RabbitMQ using the Basic.Get AMQP
        command.

        .. warning:: You should only use a single :py:class:`~rabbitpy.Queue`
             instance per channel when getting messages. Failure to do so can
             have unintended consequences.


        :param acknowledge: Let RabbitMQ know if you will manually
                            acknowledge or negatively acknowledge the
                            message after each get.

        """
        self._write_frame(
            commands.Basic.Get(queue=self.name, no_ack=not acknowledge)
        )
        return self.channel.get_message()

    def ha_declare(self, nodes: list[str] | None = None) -> tuple[int, int]:
        """Declare the queue as highly available, passing in a list of nodes
        the queue should live  on. If no nodes are passed, the queue will be
        declared across all nodes in the cluster.

        :param list nodes: A list of nodes to declare. If left empty, queue
                           will be declared on all cluster nodes.
        :return: Message count, Consumer count

        """
        if nodes:
            self.arguments['x-ha-policy'] = 'nodes'
            self.arguments['x-ha-nodes'] = nodes
        else:
            self.arguments['x-ha-policy'] = 'all'
            if 'x-ha-nodes' in self.arguments:
                del self.arguments['x-ha-nodes']
        return self.declare()

    def purge(self) -> None:
        """Purge the queue of all of its messages."""
        self._rpc(commands.Queue.Purge(queue=self.name))

    def stop_consuming(self) -> None:
        """Stop consuming messages. This is usually invoked if you want to
        cancel your consumer from outside the context manager or generator.

        If you invoke this, there is a possibility that the generator method
        will return None instead of a :py:class:`rabbitpy.Message`.

        """
        if utils.PYPY and not self.consuming:
            return
        if not self.consuming:
            raise exceptions.NotConsumingError()
        self.channel.cancel_consumer(self)
        self.consuming = False

    def unbind(
        self, source: exchange.ExchangeTypes, routing_key: str | None = None
    ) -> None:
        """Unbind queue from the specified exchange where it is bound the
        routing key. If routing key is None, use the queue name.

        :param source: The exchange to unbind from
        :param routing_key: The routing key that binds them

        """
        if hasattr(source, 'name'):
            source = source.name
        routing_key = routing_key or self.name
        self._rpc(
            commands.Queue.Unbind(
                queue=self.name, exchange=source, routing_key=routing_key
            )
        )

    def _declare(self, passive: bool = False) -> commands.Queue.Declare:
        """Return a commands.Queue.Declare class pre-composed for the rpc
        method since this can be called multiple times.

        :param passive: Passive declare to retrieve message count and
                        consumer count information

        """
        arguments = dict(self.arguments)
        if self.expires:
            arguments['x-expires'] = self.expires
        if self.message_ttl:
            arguments['x-message-ttl'] = self.message_ttl
        if self.max_length:
            arguments['x-max-length'] = self.max_length
        if self.dead_letter_exchange:
            arguments['x-dead-letter-exchange'] = self.dead_letter_exchange
        if self.dead_letter_routing_key:
            arguments['x-dead-letter-routing-key'] = (
                self.dead_letter_routing_key
            )

        LOGGER.debug(
            'Declaring Queue %s, durable=%s, passive=%s, '
            'exclusive=%s, auto_delete=%s, arguments=%r',
            self.name,
            self.durable,
            passive,
            self.exclusive,
            self.auto_delete,
            arguments,
        )
        return commands.Queue.Declare(
            queue=self.name,
            durable=self.durable,
            passive=passive,
            exclusive=self.exclusive,
            auto_delete=self.auto_delete,
            arguments=arguments,
        )

    def _register_consumer(
        self,
        no_ack: bool = False,
        prefetch: int | None = None,
        priority: int | None = None,
    ) -> None:
        """Start consuming on the channel, optionally setting the prefect count

        :param no_ack: Do not require acknowledgements
        :param prefetch: Set a prefetch count for the channel
        :param priority: Consumer priority

        """
        if prefetch:
            self.channel.prefetch_count(prefetch, False)
        self.channel.register_consumer(self, no_ack, priority)
        self.consuming = True
