"""
AMQP Adapter

"""
from pamqp import specification as spec

from rabbitpy import base
from rabbitpy import message
from rabbitpy import exceptions
from rabbitpy import utils


# pylint: disable=too-many-public-methods
class AMQP(base.ChannelWriter):
    """The AMQP Adapter provides a more generic, non-opinionated interface to
    RabbitMQ by providing methods that map to the AMQP API.

    :param rabbitmq.channel.Channel channel: The channel to use

    """
    def __init__(self, channel):
        super(AMQP, self).__init__(channel)
        self.consumer_tag = 'rabbitpy.%s.%s' % (self.channel.id, id(self))
        self._consuming = False

    def basic_ack(self, delivery_tag=0, multiple=False):
        """Acknowledge one or more messages

        This method acknowledges one or more messages delivered via the Deliver
        or Get-Ok methods. The client can ask to confirm a single message or a
        set of messages up to and including a specific message.

        :param delivery_tag: Server-assigned delivery tag
        :type delivery_tag: int|long
        :param bool multiple: Acknowledge multiple messages

        """
        self._write_frame(spec.Basic.Ack(delivery_tag, multiple))

    def basic_consume(self, queue='', consumer_tag='', no_local=False,
                      no_ack=False, exclusive=False, nowait=False,
                      arguments=None):
        """Start a queue consumer

        This method asks the server to start a "consumer", which is a transient
        request for messages from a specific queue. Consumers last as long as
        the channel they were declared on, or until the client cancels them.

        This method will act as an generator, returning messages as they are
        delivered from the server.

        Example use:

        .. code:: python

            for message in basic_consume(queue_name):
                print message.body
                message.ack()

        :param str queue: The queue name to consume from
        :param str consumer_tag: The consumer tag
        :param bool no_local: Do not deliver own messages
        :param bool no_ack: No acknowledgement needed
        :param bool exclusive: Request exclusive access
        :param bool nowait: Do not send a reply method
        :param dict arguments: Arguments for declaration

        """
        if not consumer_tag:
            consumer_tag = self.consumer_tag
        # pylint: disable=protected-access
        self.channel._consumers[consumer_tag] = (self, no_ack)
        self._rpc(spec.Basic.Consume(0, queue, consumer_tag, no_local, no_ack,
                                     exclusive, nowait, arguments))
        self._consuming = True
        try:
            while self._consuming:
                # pylint: disable=protected-access
                msg = self.channel._consume_message()
                if msg:
                    yield msg
                else:
                    if self._consuming:
                        self.basic_cancel(consumer_tag)
                    break
        finally:
            if self._consuming:
                self.basic_cancel(consumer_tag)

    def basic_cancel(self, consumer_tag='', nowait=False):
        """End a queue consumer

        This method cancels a consumer. This does not affect already delivered
        messages, but it does mean the server will not send any more messages
        for that consumer. The client may receive an arbitrary number of
        messages in between sending the cancel method and receiving the cancel-
        ok reply.

        :param str consumer_tag: Consumer tag
        :param bool nowait: Do not send a reply method

        """
        if utils.PYPY and not self._consuming:
            return
        if not self._consuming:
            raise exceptions.NotConsumingError()
        # pylint: disable=protected-access
        self.channel._cancel_consumer(self, consumer_tag, nowait)
        self._consuming = False

    def basic_get(self, queue='', no_ack=False):
        """Direct access to a queue

        This method provides a direct access to the messages in a queue using a
        synchronous dialogue that is designed for specific types of application
        where synchronous functionality is more important than performance.

        :param str queue: The queue name
        :param bool no_ack: No acknowledgement needed

        """
        self._rpc(spec.Basic.Get(0, queue, no_ack))

    def basic_nack(self, delivery_tag=0, multiple=False, requeue=True):
        """Reject one or more incoming messages.

        This method allows a client to reject one or more incoming messages. It
        can be used to interrupt and cancel large incoming messages, or return
        untreatable messages to their original queue. This method is also used
        by the server to inform publishers on channels in confirm mode of
        unhandled messages. If a publisher receives this method, it probably
        needs to republish the offending messages.

        :param delivery_tag: Server-assigned delivery tag
        :type delivery_tag: int|long
        :param bool multiple: Reject multiple messages
        :param bool requeue: Requeue the message

        """
        self._write_frame(spec.Basic.Nack(delivery_tag, multiple, requeue))

    def basic_publish(self, exchange='', routing_key='', body='',
                      properties=None, mandatory=False, immediate=False):
        """Publish a message

        This method publishes a message to a specific exchange. The message
        will be routed to queues as defined by the exchange configuration and
        distributed to any active consumers when the transaction, if any, is
        committed.

        :param str exchange: The exchange name
        :param str routing_key: Message routing key
        :param body: The message body
        :type body: str|bytes
        :param dict properties: AMQP message properties
        :param bool mandatory: Indicate mandatory routing
        :param bool immediate: Request immediate delivery
        :return: bool or None

        """
        msg = message.Message(self.channel, body,
                              properties or {}, False, False)
        return msg.publish(exchange, routing_key, mandatory, immediate)

    def basic_qos(self, prefetch_size=0, prefetch_count=0, global_flag=False):
        """Specify quality of service

        This method requests a specific quality of service. The QoS can be
        specified for the current channel or for all channels on the
        connection. The particular properties and semantics of a qos method
        always depend on the content class semantics. Though the qos method
        could in principle apply to both peers, it is currently meaningful only
        for the server.

        :param prefetch_size: Prefetch window in octets
        :type prefetch_size: int|long
        :param int prefetch_count: Prefetch window in messages
        :param bool global_flag: Apply to entire connection

        """
        self._rpc(spec.Basic.Qos(prefetch_size, prefetch_count, global_flag))

    def basic_reject(self, delivery_tag=0, requeue=True):
        """Reject an incoming message

        This method allows a client to reject a message. It can be used to
        interrupt and cancel large incoming messages, or return untreatable
        messages to their original queue.

        :param delivery_tag: Server-assigned delivery tag
        :type delivery_tag: int|long
        :param bool requeue: Requeue the message

        """
        self._write_frame(spec.Basic.Reject(delivery_tag, requeue))

    def basic_recover(self, requeue=False):
        """Redeliver unacknowledged messages

        This method asks the server to redeliver all unacknowledged messages on
        a specified channel. Zero or more messages may be redelivered.  This
        method replaces the asynchronous Recover.

        :param bool requeue: Requeue the message

        """
        self._rpc(spec.Basic.Recover(requeue))

    def confirm_select(self):
        """This method sets the channel to use publisher acknowledgements. The
        client can only use this method on a non-transactional channel.

        """
        self._rpc(spec.Confirm.Select())

    def exchange_declare(self, exchange='', exchange_type='direct',
                         passive=False, durable=False, auto_delete=False,
                         internal=False, nowait=False, arguments=None):
        """Verify exchange exists, create if needed

        This method creates an exchange if it does not already exist, and if
        the exchange exists, verifies that it is of the correct and expected
        class.

        :param str exchange: The exchange name
        :param str exchange_type: Exchange type
        :param bool passive: Do not create exchange
        :param bool durable: Request a durable exchange
        :param bool auto_delete: Automatically delete when not in use
        :param bool internal: Deprecated
        :param bool nowait: Do not send a reply method
        :param dict arguments: Arguments for declaration

        """
        self._rpc(spec.Exchange.Declare(0, exchange, exchange_type, passive,
                                        durable, auto_delete, internal, nowait,
                                        arguments))

    def exchange_delete(self, exchange='', if_unused=False,
                        nowait=False):
        """Delete an exchange

        This method deletes an exchange. When an exchange is deleted all queue
        bindings on the exchange are cancelled.

        :param str exchange: The exchange name
        :param bool if_unused: Delete only if unused
        :param bool nowait: Do not send a reply method

        """
        self._rpc(spec.Exchange.Delete(0, exchange, if_unused, nowait))

    def exchange_bind(self, destination='', source='',
                      routing_key='', nowait=False, arguments=None):
        """Bind exchange to an exchange.

        This method binds an exchange to an exchange.

        :param str destination: The destination exchange name
        :param str source: The source exchange name
        :param str routing_key: The routing key to bind with
        :param bool nowait: Do not send a reply method
        :param dict arguments: Optional arguments

        """
        self._rpc(spec.Exchange.Bind(0, destination, source, routing_key,
                                     nowait, arguments))

    def exchange_unbind(self, destination='', source='',
                        routing_key='', nowait=False, arguments=None):
        """Unbind an exchange from an exchange.

        This method unbinds an exchange from an exchange.

        :param str destination: The destination exchange name
        :param str source: The source exchange name
        :param str routing_key: The routing key to bind with
        :param bool nowait: Do not send a reply method
        :param dict arguments: Optional arguments

        """
        self._rpc(spec.Exchange.Unbind(0, destination, source, routing_key,
                                       nowait, arguments))

    def queue_bind(self, queue='', exchange='', routing_key='',
                   nowait=False, arguments=None):
        """Bind queue to an exchange

        This method binds a queue to an exchange. Until a queue is bound it
        will not receive any messages. In a classic messaging model, store-and-
        forward queues are bound to a direct exchange and subscription queues
        are bound to a topic exchange.

        :param str queue: The queue name
        :param str exchange: Name of the exchange to bind to
        :param str routing_key: Message routing key
        :param bool nowait: Do not send a reply method
        :param dict arguments: Arguments for binding

        """
        self._rpc(spec.Queue.Bind(0, queue, exchange, routing_key, nowait,
                                  arguments))

    def queue_declare(self, queue='', passive=False, durable=False,
                      exclusive=False, auto_delete=False, nowait=False,
                      arguments=None):
        """Declare queue, create if needed

        This method creates or checks a queue. When creating a new queue the
        client can specify various properties that control the durability of
        the queue and its contents, and the level of sharing for the queue.

        :param str queue: The queue name
        :param bool passive: Do not create queue
        :param bool durable: Request a durable queue
        :param bool exclusive: Request an exclusive queue
        :param bool auto_delete: Auto-delete queue when unused
        :param bool nowait: Do not send a reply method
        :param dict arguments: Arguments for declaration

        """
        self._rpc(spec.Queue.Declare(0, queue, passive, durable, exclusive,
                                     auto_delete, nowait, arguments))

    def queue_delete(self, queue='', if_unused=False, if_empty=False,
                     nowait=False):
        """Delete a queue

        This method deletes a queue. When a queue is deleted any pending
        messages are sent to a dead-letter queue if this is defined in the
        server configuration, and all consumers on the queue are cancelled.

        :param str queue: The queue name
        :param bool if_unused: Delete only if unused
        :param bool if_empty: Delete only if empty
        :param bool nowait: Do not send a reply method

        """
        self._rpc(spec.Queue.Delete(0, queue, if_unused, if_empty, nowait))

    def queue_purge(self, queue='', nowait=False):
        """Purge a queue

        This method removes all messages from a queue which are not awaiting
        acknowledgment.

        :param str queue: The queue name
        :param bool nowait: Do not send a reply method

        """
        self._rpc(spec.Queue.Purge(0, queue, nowait))

    def queue_unbind(self, queue='', exchange='', routing_key='',
                     arguments=None):
        """Unbind a queue from an exchange

        This method unbinds a queue from an exchange.

        :param str queue: The queue name
        :param str exchange: The exchange name
        :param str routing_key: Routing key of binding
        :param dict arguments: Arguments of binding

        """
        self._rpc(spec.Queue.Unbind(0, queue, exchange, routing_key,
                                    arguments))

    def tx_select(self):
        """Select standard transaction mode

        This method sets the channel to use standard transactions. The client
        must use this method at least once on a channel before using the Commit
        or Rollback methods.

        """
        self._rpc(spec.Tx.Select())

    def tx_commit(self):
        """Commit the current transaction

        This method commits all message publications and acknowledgments
        performed in the current transaction.  A new transaction starts
        immediately after a commit.

        """
        self._rpc(spec.Tx.Commit())

    def tx_rollback(self):
        """Abandon the current transaction

        This method abandons all message publications and acknowledgments
        performed in the current transaction. A new transaction starts
        immediately after a rollback. Note that unacked messages will not be
        automatically redelivered by rollback; if that is required an explicit
        recover call should be issued.

        """
        self._rpc(spec.Tx.Rollback())
