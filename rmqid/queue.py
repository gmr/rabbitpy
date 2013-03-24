"""
Queue is a class that encompasses and returns the methods of the
Specification.Queue class

"""
import contextlib
import logging
from pamqp import specification

from rmqid import base

LOGGER = logging.getLogger(__name__)


class Queue(base.AMQPClass):
    """Queue class that with methods that return the specification class
    method frames.

    """
    def __init__(self, channel, name, passive=False, durable=True,
                 exclusive=False, auto_delete=False):
        """Create a new instance of the queue object.

        :param rmqid.channel.Channel: The channel object to work with
        :param str name: The name of the queue
        :param bool passive: Do not create exchange
        :param bool durable: Request a durable exchange
        :param bool auto_delete: Automatically delete when not in use

        """
        super(Queue, self).__init__(channel, name)
        self.consumer_tag = 'rmqid.%i.%s' % (self.channel.id, id(self))
        self.consuming = False
        self._passive = passive
        self._durable = durable
        self._exclusive = exclusive
        self._auto_delete = auto_delete

    def __len__(self):
        """Return the pending number of messages in the queue by doing a passive
        Queue declare.

        :rtype: int

        """
        response = self.rpc(self._declare(True))
        return response.message_count

    def bind(self, exchange, routing_key=None):
        """Bind the queue to the specified exchange or routing key.
        If routing key is None, use the queue name.

        :param str | rmqid.base.AMQPClass exchange: The exchange to bind to
        :param str routing_key: The routing key to use

        """
        if isinstance(exchange, base.AMQPClass):
            exchange = exchange.name
        self.rpc(specification.Queue.Bind(queue=self.name,
                                          exchange=exchange,
                                          routing_key=routing_key or self.name))

    @contextlib.contextmanager
    def consumer(self, no_ack=False, prefetch=None):
        """Consumer message context manager, returns a consumer message
        generator.

        :param bool no_ack: Do not require acknowledgements
        :param int prefetch: Set a prefetch count for the channel
        :rtype: Consumer

        """
        self.consuming = True
        if prefetch:
            self.channel.prefetch(prefetch)
        self.rpc(specification.Basic.Consume(queue=self.name,
                                             consumer_tag=self.consumer_tag,
                                             no_ack=no_ack))

        yield Consumer(self)
        self.consuming = False
        self.rpc(specification.Basic.Cancel(consumer_tag=self.consumer_tag))

    def declare(self):
        """Declare the queue"""
        self.rpc(self._declare())

    def delete(self, if_unused=False, if_empty=False):
        """Delete the queue

        :param bool if_unused: Delete only if unused
        :param bool if_empty: Delete only if empty

        """
        self.rpc(specification.Queue.Delete(queue=self.name,
                                            if_unused=if_unused,
                                            if_empty=if_empty))

    def get(self, no_ack=False):
        """Return the results of a Basic.Get

        :param bool no_ack: Broker should not expect a Basic.Ack,
                            Basic.Reject or Basic.Nack
        :rtype: rmqid.message.Message

        """
        return self.rpc(specification.Basic.Get(queue=self.name, no_ack=no_ack))

    def unbind(self, exchange, routing_key=None):
        """Unbind queue from the specified exchange where it is bound the
        routing key. If routing key is None, use the queue name.

        :param str | rmqid.base.AMQPClass exchange: Exchange to unbind from
        :param str routing_key: The routing key that binds them

        """
        if isinstance(exchange, base.AMQPClass):
            exchange = exchange.name
        self.rpc(specification.Queue.Bind(queue=self.name,
                                          exchange=exchange,
                                          routing_key=routing_key or self.name))

    def _declare(self, passive=None):
        """Return a specification.Queue.Declare class pre-composed for the rpc
        method since this can be called multiple times.

        :rtype: pamqp.specification.Queue.Declare

        """
        return specification.Queue.Declare(queue=self.name,
                                           durable=self._durable,
                                           passive=passive or self._passive,
                                           exclusive=self._exclusive,
                                           auto_delete=self._auto_delete)


class Consumer(object):

    def __init__(self, queue):
        self.queue = queue

    def next_message(self):
        while self.queue.consuming:
            value = self.queue.channel._get_message()
            if value:
                yield value
