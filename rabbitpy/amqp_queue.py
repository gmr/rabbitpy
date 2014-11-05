"""
The rabbitpy.amqp_queue module contains two classes :py:class:`Queue` and
:py:class:`Consumer`. The :py:class:`Queue` class is an object that is used
create and work with queues on a RabbitMQ server.


To consume messages you can iterate over the Queue object itself if the
defaults for the :py:meth:`Queue.__iter__() <Queue.__iter__>` method work
for your needs::

    with conn.channel() as channel:
        for message in rabbitpy.Queue(channel, 'example'):
            print 'Message: %r' % message
            message.ack()

or by the :py:meth:`Queue.consume_messages() <Queue.consume_messages>` method
if you would like to specify `no_ack`, `prefetch_count`, or `priority`::

    with conn.channel() as channel:
        queue = rabbitpy.Queue(channel, 'example')
        for message in queue.consume_messages():
            print 'Message: %r' % message
            message.ack()

"""
import contextlib
import logging
from pamqp import specification

from rabbitpy import base
from rabbitpy import exceptions
from rabbitpy import utils

LOGGER = logging.getLogger(__name__)


class Queue(base.AMQPClass):
    """Create and manage RabbitMQ queues.

    :param channel: The channel object to communicate on
    :type channel: :py:class:`rabbitpy.channel.Channel`
    :param str name: The name of the queue
    :param exclusive: Queue can only be used by this channel and will
                      auto-delete once the channel is closed.
    :type exclusive: bool
    :param durable: Indicates if the queue should survive a RabbitMQ is restart
    :type durable: bool
    :param bool auto_delete: Automatically delete when all consumers disconnect
    :param int max_length: Maximum queue length
    :param int message_ttl: Time-to-live of a message in milliseconds
    :param expires: Milliseconds until a queue is removed after becoming idle
    :type expires: int
    :param dead_letter_exchange: Dead letter exchange for rejected messages
    :type dead_letter_exchange: str
    :param dead_letter_routing_key: Routing key for dead lettered messages
    :type dead_letter_routing_key: str
    :param dict arguments: Custom arguments for the queue

    """
    arguments = dict()
    auto_delete = False
    dead_letter_exchange = None
    dead_letter_routing_key = None
    durable = False
    exclusive = False
    expires = None
    max_length = None
    message_ttl = None

    def __init__(self, channel, name='',
                 durable=False, exclusive=False, auto_delete=False,
                 max_length=None, message_ttl=None, expires=None,
                 dead_letter_exchange=None, dead_letter_routing_key=None,
                 arguments=None):
        """Create a new Queue object instance. Only the
        :class:`rabbitpy.Channel` object is required.

        """
        super(Queue, self).__init__(channel, name)

        # Defaults
        self.consumer_tag = 'rabbitpy.%s.%s' % (self.channel.id, id(self))
        self.consuming = False
        self._consumer = None

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

    def __iter__(self):
        """Quick way to consume messages using defaults of no_ack=False,
        prefetch of 100, and no priority set.

        :yields: rabbitpy.message.Message

        """
        with self.consumer() as consumer:
            for message in consumer.next_message():
                yield message

    def __len__(self):
        """Return the pending number of messages in the queue by doing a
        passive Queue declare.

        :rtype: int

        """
        response = self._rpc(self._declare(True))
        return response.message_count

    def __setattr__(self, name, value):
        """Validate the data types for specific attributes when setting them,
        otherwise fall throw to the parent __setattr__

        :param str name: The attribute to set
        :param mixed value: The value to set
        :raises: ValueError

        """
        if value is not None:

            if (name in ['auto_delete', 'durable', 'exclusive'] and
                    not isinstance(value, bool)):
                raise ValueError('%s must be True or False' % name)

            if (name in ['max_length', 'message_ttl', 'expires'] and
                    not isinstance(value, int)):
                    raise ValueError('%s must be an int' % name)

            if (name in ['dead_letter_exchange', 'dead_letter_routing_key'] and
                    not utils.is_string(value)):
                    raise ValueError('%s must be a str, bytes or unicode' %
                                     name)

            if name == 'arguments' and not isinstance(value, dict):
                raise ValueError('arguments must be a dict')

        # Set the value
        super(Queue, self).__setattr__(name, value)

    def bind(self, source, routing_key=None, arguments=None):
        """Bind the queue to the specified exchange or routing key.

        :type source: str or :py:class:`rabbitpy.exchange.Exchange` exchange
        :param source: The exchange to bind to
        :param str routing_key: The routing key to use
        :param dict arguments: Optional arguments for for RabbitMQ
        :return: bool

        """
        if hasattr(source, 'name'):
            source = source.name
        frame = specification.Queue.Bind(queue=self.name,
                                         exchange=source,
                                         routing_key=routing_key or '',
                                         arguments=arguments)
        response = self._rpc(frame)
        return isinstance(response, specification.Queue.BindOk)

    @contextlib.contextmanager
    def consumer(self, no_ack=False, prefetch=None, priority=None):
        """Consumer message context manager, returns a consumer message
        generator.

        :param bool no_ack: Do not require acknowledgements
        :param int prefetch: Set a prefetch count for the channel
        :param int priority: Consumer priority
        :rtype: :py:class:`Consumer <rabbitpy.queue.Consumer>`

        """
        if prefetch is not None:
            self.channel.prefetch_count(prefetch)
        self._consumer = Consumer(self)
        self.channel._consume(self, no_ack, priority)
        self.consuming = True
        yield self._consumer

    def consume_messages(self, no_ack=False, prefetch=None, priority=None):
        """Consume messages from the queue as a generator:

        ```
            for message in queue.consume_messages():
                message.ack()
        ```

        You can use this message instead of the queue object as an iterator
        if you need to alter the prefect count, set the consumer priority or
        consume in no_ack mode.

        :param bool no_ack: Do not require acknowledgements
        :param int prefetch: Set a prefetch count for the channel
        :param int priority: Consumer priority
        :rtype: :py:class:`Iterator`

        """
        with self.consumer(no_ack, prefetch, priority) as consumer:
            for message in consumer.next_message():
                yield message

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
        if not self.name:
            self.name = response.queue
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
        :rtype: rabbitpy.message.Message or None

        """
        self._write_frame(specification.Basic.Get(queue=self.name,
                                                  no_ack=not acknowledge))
        return self.channel._get_message()

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
            self.arguments['x-ha-policy'] = 'nodes'
            self.arguments['x-ha-nodes'] = nodes
        else:
            self.arguments['x-ha-policy'] = 'all'
            if 'x-ha-nodes' in self.arguments:
                del self.arguments['x-ha-nodes']
        return self.declare()

    def purge(self):
        """Purge the queue of all of its messages."""
        self._rpc(specification.Queue.Purge())

    def stop_consuming(self):
        """Stop consuming messages. This is usually invoked if you want to
        cancel your consumer from outside the context manager or generator.

        If you invoke this, there is a possibility that the generator method
        will return None instead of a :py:class:`rabbitpy.Message`.

        """
        if not self._consumer:
            raise exceptions.NotConsumingError
        self._consumer.cancel()

    def unbind(self, source, routing_key=None):
        """Unbind queue from the specified exchange where it is bound the
        routing key. If routing key is None, use the queue name.

        :type source: str or :py:class:`rabbitpy.exchange.Exchange` exchange
        :param source: The exchange to unbind from
        :param str routing_key: The routing key that binds them

        """
        if hasattr(source, 'name'):
            source = source.name
        routing_key = routing_key or self.name
        self._rpc(specification.Queue.Unbind(queue=self.name, exchange=source,
                                             routing_key=routing_key))

    def _declare(self, passive=False):
        """Return a specification.Queue.Declare class pre-composed for the rpc
        method since this can be called multiple times.

        :param bool passive: Passive declare to retrieve message count and
                             consumer count information
        :rtype: pamqp.specification.Queue.Declare

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
            arguments['x-dead-letter-routing-key'] = \
                self.dead_letter_routing_key

        LOGGER.debug('Declaring Queue %s, durable=%s, passive=%s, '
                     'exclusive=%s, auto_delete=%s, arguments=%r',
                     self.name, self.durable, passive, self.exclusive,
                     self.auto_delete, arguments)

        return specification.Queue.Declare(queue=self.name,
                                           durable=self.durable,
                                           passive=passive,
                                           exclusive=self.exclusive,
                                           auto_delete=self.auto_delete,
                                           arguments=arguments)


class Consumer(object):
    """The Consumer class implements an iterator that will retrieve the next
    message from the stack of messages RabbitMQ has delivered until the client
    exists the iterator. It should be used with the
    :py:meth:`Queue.consumer() <rabbitpy.queue.Queue.consumer>` method which
    returns a context manager for consuming.

    """
    def __init__(self, queue):
        self.queue = queue

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Called when exiting the consumer iterator"""
        self.cancel()

    @property
    def _basic_cancel(self):
        frame = specification.Basic.Cancel(consumer_tag=self.queue.consumer_tag)
        # If we wait for a reply here the CancelOK frame is consumed and
        # Channel._consume_message cannot observe CancelOK and cannot
        # return None to it's caller.
        # This behavior is needed when Consumer.Cancel() is called from
        # a different thread that runs Consumer.next_message().
        frame.synchronous = False
        return frame

    def cancel(self):
        """Cancel the consumer"""
        self.queue.consuming = False
        self.queue.channel.rpc(self._basic_cancel)

    def next_message(self):
        """Retrieve the nest message from the queue as an iterator, blocking
        until the next message is available.

        You should check the return value to ensure it's not ``None``, as it
        will be returned if your consumer is cancelled by invoking
        :py:meth:`Queue.stop_consuming()` or :py:meth:`Consumer:cancel`.

        :rtype: :py:class:`rabbitpy.message.Message` or None

        """
        while self.queue.consuming:
            yield self.queue.channel._consume_message()
