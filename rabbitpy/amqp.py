"""
AMQP Adapter

"""

from pamqp import commands

from rabbitpy import base, channel, exceptions, message, utils


# pylint: disable=too-many-public-methods
class AMQP(base.ChannelWriter):
    """The AMQP Adapter provides a more generic, non-opinionated interface to
    RabbitMQ by providing methods that map to the AMQP API.

    :param rabbitpy_channel: The channel to use

    """

    def __init__(self, rabbitpy_channel: channel.Channel):
        super().__init__(rabbitpy_channel)
        self.consumer_tag = f'rabbitpy.{self.channel.id}.{id(self)}'
        self._consuming = False

    def basic_ack(self, delivery_tag: int = 0, multiple: bool = False) -> None:
        """Acknowledge one or more messages

        This method acknowledges one or more messages delivered via the Deliver
        or Get-Ok methods. The client can ask to confirm a single message or a
        set of messages up to and including a specific message.

        :param delivery_tag: Server-assigned delivery tag
        :param multiple: Acknowledge multiple messages

        """
        self._write_frame(commands.Basic.Ack(delivery_tag, multiple))

    def basic_consume(self,
                      queue: str = '',
                      consumer_tag: str = '',
                      no_local: bool = False,
                      no_ack: bool = False,
                      exclusive: bool = False,
                      nowait: bool = False,
                      arguments: dict | None = None) \
            -> None:
        """Start a queue consumer

        This method asks the server to start a "consumer", which is a transient
        request for messages from a specific queue. Consumers last as long as
        the channel they were declared on, or until the client cancels them.

        This method will act as a generator, returning messages as they are
        delivered from the server.

        Example use:

        .. code:: python

            for message in basic_consume(queue_name):
                print(message.body)
                message.ack()

        :param queue: The queue name to consume from
        :param consumer_tag: The consumer tag
        :param no_local: Do not deliver own messages
        :param no_ack: No acknowledgement needed
        :param exclusive: Request exclusive access
        :param nowait: Do not send a reply method
        :param arguments: Arguments for declaration

        """
        if not consumer_tag:
            consumer_tag = self.consumer_tag
        self.channel.consumers[consumer_tag] = (self, no_ack)
        self._rpc(
            commands.Basic.Consume(0, queue, consumer_tag, no_local, no_ack,
                                   exclusive, nowait, arguments))
        self._consuming = True
        try:
            while self._consuming:
                msg = self.channel.consume_message()
                if msg:
                    yield msg
                else:
                    if self._consuming:
                        self.basic_cancel(consumer_tag)
                    break
        finally:
            if self._consuming:
                self.basic_cancel(consumer_tag)

    def basic_cancel(self,
                     consumer_tag: str = '',
                     nowait: bool = False) -> None:
        """End a queue consumer

        This method cancels a consumer. This does not affect already delivered
        messages, but it does mean the server will not send any more messages
        for that consumer. The client may receive an arbitrary number of
        messages in between sending the cancel method and receiving the
        cancel-ok reply.

        :param consumer_tag: Consumer tag
        :param nowait: Do not send a reply method

        """
        if utils.PYPY and not self._consuming:
            return
        if not self._consuming:
            raise exceptions.NotConsumingError()
        self.channel.cancel_consumer(self, consumer_tag, nowait)
        self._consuming = False

    def basic_get(self, queue: str = '', no_ack: bool = False) -> None:
        """Direct access to a queue

        This method provides direct access to the messages in a queue using a
        synchronous dialogue that is designed for specific types of application
        where synchronous functionality is more important than performance.

        :param queue: The queue name
        :param no_ack: No acknowledgement needed

        """
        self._rpc(commands.Basic.Get(0, queue, no_ack))

    def basic_nack(self,
                   delivery_tag: int = 0,
                   multiple: bool = False,
                   requeue: bool = True) -> None:
        """Reject one or more incoming messages.

        This method allows a client to reject one or more incoming messages. It
        can be used to interrupt and cancel large incoming messages, or return
        untreatable messages to their original queue. This method is also used
        by the server to inform publishers on channels in confirm mode of
        unhandled messages. If a publisher receives this method, it probably
        needs to republish the offending messages.

        :param delivery_tag: Server-assigned delivery tag
        :param multiple: Reject multiple messages
        :param requeue: Requeue the message

        """
        self._write_frame(commands.Basic.Nack(delivery_tag, multiple, requeue))

    def basic_publish(self,
                      exchange: str = '',
                      routing_key: str = '',
                      body: str = '',
                      properties: dict | bytes | None = None,
                      mandatory: bool = False,
                      immediate: bool = False) \
            -> bool | None:
        """Publish a message

        This method publishes a message to a specific exchange. The message
        will be routed to queues as defined by the exchange configuration and
        distributed to any active consumers when the transaction, if any, is
        committed.

        :param exchange: The exchange name
        :param routing_key: Message routing key
        :param body: The message body
        :param properties: AMQP message properties
        :param mandatory: Indicate mandatory routing
        :param immediate: Request immediate delivery

        """
        msg = message.Message(self.channel, body, properties or {}, False,
                              False)
        return msg.publish(exchange, routing_key, mandatory, immediate)

    def basic_qos(self,
                  prefetch_size: int = 0,
                  prefetch_count: int = 0,
                  global_flag: bool = False) -> None:
        """Specify quality of service

        This method requests a specific quality of service. The QoS can be
        specified for the current channel or for all channels on the
        connection. The particular properties and semantics of a qos method
        always depend on the content class semantics. Though the qos method
        could in principle apply to both peers, it is currently meaningful only
        for the server.

        :param prefetch_size: Prefetch window in octets
        :param prefetch_count: Prefetch window in messages
        :param global_flag: Apply to entire connection

        """
        self._rpc(
            commands.Basic.Qos(prefetch_size, prefetch_count, global_flag))

    def basic_reject(self,
                     delivery_tag: int = 0,
                     requeue: bool = True) -> None:
        """Reject an incoming message

        This method allows a client to reject a message. It can be used to
        interrupt and cancel large incoming messages, or return untreatable
        messages to their original queue.

        :param delivery_tag: Server-assigned delivery tag
        :param requeue: Requeue the message

        """
        self._write_frame(commands.Basic.Reject(delivery_tag, requeue))

    def basic_recover(self, requeue: bool = False) -> None:
        """Redeliver unacknowledged messages

        This method asks the server to redeliver all unacknowledged messages on
        a specified channel. Zero or more messages may be redelivered.  This
        method replaces the asynchronous Recover.

        :param requeue: Requeue the message

        """
        self._rpc(commands.Basic.Recover(requeue))

    def confirm_select(self) -> None:
        """This method sets the channel to use publisher acknowledgements. The
        client can only use this method on a non-transactional channel.

        """
        self._rpc(commands.Confirm.Select())

    def exchange_declare(self,
                         exchange: str = '',
                         exchange_type: str = 'direct',
                         passive: bool = False,
                         durable: bool = False,
                         auto_delete: bool = False,
                         internal: bool = False,
                         nowait: bool = False,
                         arguments: dict | None = None) \
            -> None:
        """Verify exchange exists, create if needed

        This method creates an exchange if it does not already exist, and if
        the exchange exists, verifies that it is of the correct and expected
        class.

        :param exchange: The exchange name
        :param exchange_type: Exchange type
        :param passive: Do not create exchange
        :param durable: Request a durable exchange
        :param auto_delete: Automatically delete when not in use
        :param internal: Deprecated
        :param nowait: Do not send a reply method
        :param arguments: Arguments for declaration

        """
        self._rpc(
            commands.Exchange.Declare(0, exchange, exchange_type, passive,
                                      durable, auto_delete, internal, nowait,
                                      arguments))

    def exchange_delete(self,
                        exchange: str = '',
                        if_unused: bool = False,
                        nowait: bool = False) -> None:
        """Delete an exchange

        This method deletes an exchange. When an exchange is deleted all queue
        bindings on the exchange are cancelled.

        :param exchange: The exchange name
        :param if_unused: Delete only if unused
        :param nowait: Do not send a reply method

        """
        self._rpc(commands.Exchange.Delete(0, exchange, if_unused, nowait))

    def exchange_bind(self,
                      destination: str = '',
                      source: str = '',
                      routing_key: str = '',
                      nowait: bool = False,
                      arguments: dict | None = None) \
            -> None:
        """Bind exchange to an exchange.

        This method binds an exchange to an exchange.

        :param destination: The destination exchange name
        :param source: The source exchange name
        :param routing_key: The routing key to bind with
        :param nowait: Do not send a reply method
        :param arguments: Optional arguments

        """
        self._rpc(
            commands.Exchange.Bind(0, destination, source, routing_key, nowait,
                                   arguments))

    def exchange_unbind(self,
                        destination: str = '',
                        source: str = '',
                        routing_key: str = '',
                        nowait: bool = False,
                        arguments: dict | None = None) \
            -> None:
        """Unbind an exchange from an exchange.

        This method unbinds an exchange from an exchange.

        :param destination: The destination exchange name
        :param source: The source exchange name
        :param routing_key: The routing key to bind with
        :param nowait: Do not send a reply method
        :param arguments: Optional arguments

        """
        self._rpc(
            commands.Exchange.Unbind(0, destination, source, routing_key,
                                     nowait, arguments))

    def queue_bind(self,
                   queue: str = '',
                   exchange: str = '',
                   routing_key: str = '',
                   nowait: bool = False,
                   arguments: dict | None = None) \
            -> None:
        """Bind queue to an exchange

        This method binds a queue to an exchange. Until a queue is bound it
        will not receive any messages. In a classic messaging model, store-and-
        forward queues are bound to a direct exchange and subscription queues
        are bound to a topic exchange.

        :param queue: The queue name
        :param exchange: The name of the exchange to bind to
        :param routing_key: Message routing key
        :param nowait: Do not send a reply method
        :param arguments: Arguments for binding

        """
        self._rpc(
            commands.Queue.Bind(0, queue, exchange, routing_key, nowait,
                                arguments))

    def queue_declare(self,
                      queue: str = '',
                      passive: bool = False,
                      durable: bool = False,
                      exclusive: bool = False,
                      auto_delete: bool = False,
                      nowait: bool = False,
                      arguments: dict | None = None) \
            -> None:
        """Declare queue, create if needed

        This method creates or checks a queue. When creating a new queue the
        client can specific various properties that control the durability of
        the queue and its contents, and the level of sharing for the queue.

        :param queue: The queue name
        :param passive: Do not create queue
        :param durable: Request a durable queue
        :param exclusive: Request an exclusive queue
        :param auto_delete: Auto-delete queue when unused
        :param nowait: Do not send a reply method
        :param arguments: Arguments for declaration

        """
        self._rpc(
            commands.Queue.Declare(0, queue, passive, durable, exclusive,
                                   auto_delete, nowait, arguments))

    def queue_delete(self,
                     queue: str = '',
                     if_unused: bool = False,
                     if_empty: bool = False,
                     nowait: bool = False) -> None:
        """Delete a queue

        This method deletes a queue. When a queue is deleted any pending
        messages are sent to a dead-letter queue if this is defined in the
        server configuration, and all consumers on the queue are cancelled.

        :param queue: The queue name
        :param if_unused: Delete only if unused
        :param if_empty: Delete only if empty
        :param nowait: Do not send a reply method

        """
        self._rpc(commands.Queue.Delete(0, queue, if_unused, if_empty, nowait))

    def queue_purge(self, queue: str = '', nowait: bool = False) -> None:
        """Purge a queue

        This method removes all messages from a queue which are not awaiting
        acknowledgment.

        :param queue: The queue name
        :param nowait: Do not send a reply method

        """
        self._rpc(commands.Queue.Purge(0, queue, nowait))

    def queue_unbind(self,
                     queue: str = '',
                     exchange: str = '',
                     routing_key: str = '',
                     arguments: dict | None = None) \
            -> None:
        """Unbind a queue from an exchange

        This method unbinds a queue from an exchange.

        :param queue: The queue name
        :param exchange: The exchange name
        :param routing_key: Routing key of binding
        :param arguments: Arguments of binding

        """
        self._rpc(
            commands.Queue.Unbind(0, queue, exchange, routing_key, arguments))

    def tx_select(self) -> None:
        """Select standard transaction mode

        This method sets the channel to use standard transactions. The client
        must use this method at least once on a channel before using the Commit
        or Rollback methods.

        """
        self._rpc(commands.Tx.Select())

    def tx_commit(self) -> None:
        """Commit the current transaction

        This method commits all message publications and acknowledgments
        performed in the current transaction.  A new transaction starts
        immediately after a commit.

        """
        self._rpc(commands.Tx.Commit())

    def tx_rollback(self) -> None:
        """Abandon the current transaction

        This method abandons all message publications and acknowledgments
        performed in the current transaction. A new transaction starts
        immediately after a rollback. Note that un-acked messages will not be
        automatically redelivered by rollback; if that is required an explicit
        recover call should be issued.

        """
        self._rpc(commands.Tx.Rollback())
