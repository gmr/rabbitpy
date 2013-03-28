"""
The rmqid.queue module contains two classes :py:class:`Queue` and
:py:class:`Consumer`. The :py:class:`Queue` class is an object that is used
create and work with queues on a RabbitMQ server. The :py:class:`Consumer`
contains a generator method, :py:meth:`next_message <Consumer.next_message>`
which returns messages delivered by RabbitMQ. The :py:class:`Consumer` class
should not be invoked directly, but rather by the
:py:meth:`Queue.consumer() <Queue.consumer>` method::

    with conn.channel() as channel:
        queue = rmqid.Queue(channel, 'example')
            with queue.consumer() as consumer:
                for message in consumer.next_message():
                    print 'Message: %r' % message
                    message.ack()

"""
import contextlib
import logging
from pamqp import specification

from rmqid import base

LOGGER = logging.getLogger(__name__)


class Queue(base.AMQPClass):
    """Create and manage RabbitMQ queues.

    :param channel: The channel object to communicate on
    :type channel: :py:class:`rmqid.channel.Channel`
    :param str name: The name of the queue
    :param durable: Indicates if the queue should survive a RabbitMQ is restart
    :type durable: bool
    :param bool auto_delete: Automatically delete when all consumers disconnect

    """
    def __init__(self, channel, name, durable=True, exclusive=False,
                 auto_delete=False, arguments=None):
        super(Queue, self).__init__(channel, name)
        self.consumer_tag = 'rmqid.%i.%s' % (self.channel.id, id(self))
        self.consuming = False
        self._durable = durable
        self._exclusive = exclusive
        self._auto_delete = auto_delete
        self._arguments = arguments or {}

    def __len__(self):
        """Return the pending number of messages in the queue by doing a passive
        Queue declare.

        :rtype: int

        """
        response = self._rpc(self._declare(True))
        return response.message_count

    def bind(self, exchange, routing_key=None, arguments=None):
        """Bind the queue to the specified exchange or routing key.
        If routing key is None, use the queue name.

        :type exchange: str or :py:class:`rmqid.exchange.Exchange` exchange
        :param exchange: The exchange to bind to
        :param str routing_key: The routing key to use
        :param dict arguments: Optional arguments for for RabbitMQ

        """
        if isinstance(exchange, base.AMQPClass):
            exchange = exchange.name
        self._rpc(specification.Queue.Bind(queue=self.name,
                                           exchange=exchange,
                                           routing_key=routing_key or self.name,
                                           arguments=arguments))

    @contextlib.contextmanager
    def consumer(self, no_ack=False, prefetch=None):
        """Consumer message context manager, returns a consumer message
        generator.

        :param bool no_ack: Do not require acknowledgements
        :param int prefetch: Set a prefetch count for the channel
        :rtype: :py:class:`Consumer <rmqid.queue.Consumer>`

        """
        self.consuming = True
        if prefetch:
            self.channel.prefetch(prefetch)
        self._rpc(specification.Basic.Consume(queue=self.name,
                                              consumer_tag=self.consumer_tag,
                                              no_ack=no_ack))

        yield Consumer(self)
        self.consuming = False
        self._rpc(specification.Basic.Cancel(consumer_tag=self.consumer_tag))

    def declare(self, passive=False):
        """Declare the queue on the RabbitMQ channel passed into the
        constructor, returning the current message count for the queue and
        its consumer count as a tuple.

        :param bool passive: Passive declare to retrieve message count and
                             consumer count information
        :return: Message count, Consumer count
        :rtype: tuple(int, int)

        """
        response = self._rpc(self._declare(passive))
        return response.message_count, response.consumer_count

    def delete(self, if_unused=False, if_empty=False):
        """Delete the queue

        :param bool if_unused: Delete only if unused
        :param bool if_empty: Delete only if empty

        """
        self._rpc(specification.Queue.Delete(queue=self.name,
                                             if_unused=if_unused,
                                             if_empty=if_empty))

    def get(self, acknowledge=True):
        """Request a single message from RabbitMQ using the Basic.Get AMQP
        command.

        :param bool acknowledge: Let RabbitMQ know if you will manually
                                 acknowledge or negatively acknowledge the
                                 message after each get.
        :rtype: rmqid.message.Message or None

        """
        return self._rpc(specification.Basic.Get(queue=self.name,
                                                 no_ack=not acknowledge))

    def ha_declare(self, nodes=None):
        """Declare a the queue as highly available, passing in a list of nodes
        the queue should live  on. If no nodes are passed, the queue will be
        declared across all nodes in the cluster.

        :param list nodes: A list of nodes to declare. If left empty, queue
                           will be declared on all cluster nodes.
        :return: Message count, Consumer count
        :rtype: tuple(int, int)

        """
        if nodes:
            self._arguments['x-ha-policy'] = 'nodes'
            self._arguments['x-ha-nodes'] = nodes
        else:
            self._arguments['x-ha-policy'] = 'all'
            if 'x-ha-nodes' in self._arguments:
                del self._arguments['x-ha-nodes']
        return self.declare()

    def purge(self):
        """Purge the queue of all of its messages."""
        self._rpc(specification.Queue.Purge())

    def unbind(self, exchange, routing_key=None):
        """Unbind queue from the specified exchange where it is bound the
        routing key. If routing key is None, use the queue name.

        :type exchange: str or :py:class:`rmqid.exchange.Exchange` exchange
        :param exchange: The exchange to unbind from
        :param str routing_key: The routing key that binds them

        """
        if isinstance(exchange, base.AMQPClass):
            exchange = exchange.name
        self._rpc(specification.Queue.Bind(queue=self.name,
                                           exchange=exchange,
                                           routing_key=routing_key or
                                                       self.name))

    def _declare(self, passive=False):
        """Return a specification.Queue.Declare class pre-composed for the rpc
        method since this can be called multiple times.

        :param bool passive: Passive declare to retrieve message count and
                             consumer count information
        :rtype: pamqp.specification.Queue.Declare

        """
        return specification.Queue.Declare(queue=self.name,
                                           durable=self._durable,
                                           passive=passive,
                                           exclusive=self._exclusive,
                                           auto_delete=self._auto_delete,
                                           arguments=self._arguments)


class Consumer(object):
    """The Consumer class implements an interator that will retrieve the next
    message from the stack of messages RabbitMQ has delivered until the client
    exists the iterator. It should be used with the
    :py:meth:`Queue.consumer() <rmqid.queue.Queue.consumer>` method which returns a
    context manager for consuming.

    """
    def __init__(self, queue):
        self.queue = queue

    def next_message(self):
        """Retrieve the nest message from the queue as an interator, blocking
        until the next message is available.

        :rtype: :py:class:`rmqid.message.Message`

        """
        while self.queue.consuming:
            value = self.queue.channel._get_message()
            if value:
                yield value
