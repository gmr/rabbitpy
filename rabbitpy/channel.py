"""
Implement the base channel construct that is used by rabbitpy objects
such as :py:class:`Exchange <rabbitpy.Exchange>` or
:py:class:`Exchange <rabbitpy.Queue>`. It is responsible for coordinating
the communication between the IO thread and the higher-level objects.

"""
import logging
try:
    import queue as Queue
except ImportError:
    import Queue

from pamqp import specification as spec
from pamqp import PYTHON3

from rabbitpy import base
from rabbitpy import exceptions
from rabbitpy import message

LOGGER = logging.getLogger(__name__)

BASIC_DELIVER = 'Basic.Deliver'
CONTENT_BODY = 'ContentBody'
CONTENT_HEADER = 'ContentHeader'


class Channel(base.AMQPChannel):
    """The Channel object is the communications object used by Exchanges,
    Messages, Queues, and Transactions. It is created by invoking the
    :py:meth:`rabbitpy.Connection.channel()
    <rabbitpy.connection.Connection.channel>` method. It can act as a context
    manager, allowing for quick shorthand use:

    .. code:: python

        with connection.channel():
           # Do something

    To create a new channel, invoke
    py:meth:`rabbitpy.connection.Connection.channel`


    To improve performance, pass blocking_read to True. Note that doing
    so prevents ``KeyboardInterrupt``/CTRL-C from exiting the Python
    interpreter.

    :param int channel_id: The channel # to use for this instance
    :param dict server_capabilities: Features the server supports
    :param events rabbitpy.Events: Event management object
    :param queue.Queue exception_queue: Exception queue
    :param queue.Queue read_queue: Queue to read pending frames from
    :param queue.Queue write_queue: Queue to write pending AMQP objs to
    :param int maximum_frame_size: The max frame size for msg bodies
    :param socket write_trigger: Write to this socket to break IO waiting
    :param bool blocking_read: Use blocking Queue.get to improve performance
    :raises: rabbitpy.exceptions.RemoteClosedChannelException
    :raises: rabbitpy.exceptions.AMQPException

    """
    STATES = base.AMQPChannel.STATES
    STATES[0x04] = 'Remotely Closed'

    def __init__(self, channel_id, server_capabilities, events,
                 exception_queue, read_queue, write_queue,
                 maximum_frame_size, write_trigger, blocking_read=False):
        """Create a new instance of the Channel class"""
        super(Channel, self).__init__(exception_queue, write_trigger,
                                      blocking_read)
        self._channel_id = channel_id
        self._consumers = {}
        self._events = events
        self._maximum_frame_size = maximum_frame_size
        self._publisher_confirms = False
        self._read_queue = read_queue
        self._write_queue = write_queue
        self._server_capabilities = server_capabilities

    def __enter__(self):
        """For use as a context manager, return a handle to this object
        instance.

        :rtype: Channel

        """
        return self

    def __exit__(self, exc_type, exc_val, unused_exc_tb):
        """When leaving the context, examine why the context is leaving, if
        it's  an exception or what.

        """
        if exc_type and exc_val:
            LOGGER.debug('Exiting due to exception: %r', exc_val)
            self._set_state(self.CLOSED)
            raise
        if self.open:
            self.close()

    def close(self):
        """Close the channel, cancelling any active consumers, purging the read
        queue, while looking to see if a Basic.Nack should be sent, sending it
        if so.

        """
        if self.closed:
            LOGGER.debug('Channel %i close invoked when already closed',
                         self._channel_id)
            return

        self._set_state(self.CLOSING)

        # Empty the queue and nack the max id (and all previous)
        if self._consumers:
            delivery_tag = 0
            discard_counter = 0
            ack_tags = []
            for queue_obj, no_ack in self._consumers.values():
                self._cancel_consumer(queue_obj)
                if not no_ack:
                    LOGGER.debug('Channel %i will nack messages for %s',
                                 self._channel_id, queue_obj.consumer_tag)
                    ack_tags.append(queue_obj.consumer_tag)

            # If there are any ack tags, get the last msg to nack
            if ack_tags:
                while not self._read_queue.empty():
                    frame_value = self._get_from_read_queue()
                    if not frame_value:
                        break
                    if (frame_value.name == BASIC_DELIVER and
                            frame_value.consumer_tag in ack_tags):
                        if delivery_tag < frame_value.delivery_tag:
                            delivery_tag = frame_value.delivery_tag
                    discard_counter += 1
                if delivery_tag:
                    self._multi_nack(delivery_tag)

        super(Channel, self).close()

    def enable_publisher_confirms(self):
        """Turn on Publisher Confirms. If confirms are turned on, the
        Message.publish command will return a bool indicating if a message has
        been successfully published.

        """
        if not self._supports_publisher_confirms:
            raise exceptions.NotSupportedError('Confirm.Select')
        self.rpc(spec.Confirm.Select())
        self._publisher_confirms = True

    @property
    def id(self):
        """Return the channel id

        :rtype: int

        """
        return self._channel_id

    @property
    def maximum_frame_size(self):
        return self._maximum_frame_size

    def open(self):
        """Open the channel, invoked directly upon creation by the Connection

        """
        self._set_state(self.OPENING)
        self._write_frame(self._build_open_frame())
        self._wait_on_frame(spec.Channel.OpenOk)
        self._set_state(self.OPEN)
        LOGGER.debug('Channel #%i open', self._channel_id)

    def prefetch_count(self, value, all_channels=False):
        """Set a prefetch count for the channel (or all channels on the same
        connection).

        :param int value: The prefetch count to set
        :param bool all_channels: Set the prefetch count on all channels on the
                                  same connection

        """
        self.rpc(spec.Basic.Qos(prefetch_count=value, global_=all_channels))

    def prefetch_size(self, value, all_channels=False):
        """Set a prefetch size in bytes for the channel (or all channels on the
        same connection).

        :param int value: The prefetch size to set
        :param bool all_channels: Set the prefetch size on all channels on the
                                  same connection

        """
        if value is None:
            return
        self.rpc(spec.Basic.Qos(prefetch_size=value, global_=all_channels))

    @property
    def publisher_confirms(self):
        """Returns True if publisher confirms are enabled.

        :rtype: bool

        """
        return self._publisher_confirms

    def recover(self, requeue=False):
        """Recover all unacknowledged messages that are associated with this
        channel.

        :param bool requeue: Requeue the message

        """
        self.rpc(spec.Basic.Recover(requeue=requeue))

    def basic_ack(self, delivery_tag=0, multiple=False):
        """Acknowledge one or more messages

        This method acknowledges one or more messages delivered via the Deliver
        or Get-Ok methods. The client can ask to confirm a single message or a
        set of messages up to and including a specific message.

        .. versionadded:: 0.26

        :param int/long delivery_tag: Server-assigned delivery tag
        :param bool multiple: Acknowledge multiple messages

        """
        self._write_frame(spec.Basic.Ack(delivery_tag, multiple))

    def basic_consume(self, queue='', consumer_tag='', no_local=False,
                      no_ack=False, exclusive=False, nowait=False,
                      arguments=None):
        """Start a queue consumer

        @TODO implement this similar to Queue.consume()

        This method asks the server to start a "consumer", which is a transient
        request for messages from a specific queue. Consumers last as long as
        the channel they were declared on, or until the client cancels them.

        .. versionadded:: 0.26

        :param str queue: The queue name to consume from
        :param str consumer-tag: The consumer tag
        :param bool no_local: Do not deliver own messages
        :param bool no_ack: No acknowledgement needed
        :param bool exclusive: Request exclusive access
        :param bool nowait: Do not send a reply method
        :param dict arguments: Arguments for declaration

        """
        self.rpc(spec.Basic.Consume(0, queue, consumer_tag, no_local, no_ack,
                                    exclusive, nowait, arguments))

    def basic_cancel(self, consumer_tag='', nowait=False):
        """End a queue consumer

        This method cancels a consumer. This does not affect already delivered
        messages, but it does mean the server will not send any more messages
        for that consumer. The client may receive an arbitrary number of
        messages in between sending the cancel method and receiving the cancel-
        ok reply.

        .. versionadded:: 0.26

        :param str consumer_tag: Consumer tag
        :param bool nowait: Do not send a reply method

        """
        self.rpc(spec.Basic.Cancel(consumer_tag, nowait))

    def basic_get(self, queue='', no_ack=False):
        """Direct access to a queue

        This method provides a direct access to the messages in a queue using a
        synchronous dialogue that is designed for specific types of application
        where synchronous functionality is more important than performance.

        .. versionadded:: 0.26

        :param str queue: The queue name
        :param bool no_ack: No acknowledgement needed

        """
        self.rpc(spec.Basic.Get(0, queue, no_ack))

    def basic_nack(self, delivery_tag=0, multiple=False, requeue=True):
        """Reject one or more incoming messages.

        This method allows a client to reject one or more incoming messages. It
        can be used to interrupt and cancel large incoming messages, or return
        untreatable messages to their original queue. This method is also used
        by the server to inform publishers on channels in confirm mode of
        unhandled messages. If a publisher receives this method, it probably
        needs to republish the offending messages.

        .. versionadded:: 0.26

        :param int/long delivery_tag: Server-assigned delivery tag
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

        .. versionadded:: 0.26

        :param str exchange: The exchange name
        :param str routing_key: Message routing key
        :param bool mandatory: Indicate mandatory routing
        :param bool immediate: Request immediate delivery
        :return: bool or None

        """
        msg = message.Message(self, body, properties or {}, False, False)
        return msg.publish(exchange, routing_key, mandatory, immediate)

    def basic_qos(self, prefetch_size=0, prefetch_count=0, global_flag=False):
        """Specify quality of service

        .. versionadded:: 0.26

        This method requests a specific quality of service. The QoS can be
        specified for the current channel or for all channels on the
        connection. The particular properties and semantics of a qos method
        always depend on the content class semantics. Though the qos method
        could in principle apply to both peers, it is currently meaningful only
        for the server.

        :param int/long prefetch_size: Prefetch window in octets
        :param int prefetch_count: Prefetch window in messages
        :param bool global_flag: Apply to entire connection

        """
        self.rpc(spec.Basic.Qos(prefetch_size, prefetch_count, global_flag))

    def basic_reject(self, delivery_tag=0, requeue=True):
        """Reject an incoming message

        .. versionadded:: 0.26

        This method allows a client to reject a message. It can be used to
        interrupt and cancel large incoming messages, or return untreatable
        messages to their original queue.

        :param int/long delivery_tag: Server-assigned delivery tag
        :param bool requeue: Requeue the message

        """
        self._write_frame(spec.Basic.Reject(delivery_tag, requeue))

    def basic_recover(self, requeue=False):
        """Redeliver unacknowledged messages

        .. versionadded:: 0.26

        This method asks the server to redeliver all unacknowledged messages on
        a specified channel. Zero or more messages may be redelivered.  This
        method replaces the asynchronous Recover.

        :param bool requeue: Requeue the message

        """
        self.rpc(spec.Basic.Recover(requeue))

    def exchange_declare(self, exchange='', exchange_type='direct',
                         passive=False, durable=False, auto_delete=False,
                         internal=False, nowait=False, arguments=None):
        """Verify exchange exists, create if needed

        .. versionadded:: 0.26

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
        self.rpc(spec.Exchange.Declare(0, exchange, exchange_type, passive,
                                       durable, auto_delete, internal, nowait,
                                       arguments))

    def exchange_delete(self, exchange='', if_unused=False,
                        nowait=False):
        """Delete an exchange

        .. versionadded:: 0.26

        This method deletes an exchange. When an exchange is deleted all queue
        bindings on the exchange are cancelled.

        :param str exchange: The exchange name
        :param bool if_unused: Delete only if unused
        :param bool nowait: Do not send a reply method

        """
        self.rpc(spec.Exchange.Delete(0, exchange, if_unused, nowait))

    def exchange_bind(self, destination='', source='',
                      routing_key='', nowait=False, arguments=None):
        """Bind exchange to an exchange.

        .. versionadded:: 0.26

        This method binds an exchange to an exchange.

        :param str destination: The destination exchange name
        :param str source: The source exchange name
        :param str routing_key: The routing key to bind with
        :param bool nowait: Do not send a reply method
        :param dict arguments: Optional arguments

        """
        self.rpc(spec.Exchange.Bind(0, destination, source, routing_key, nowait,
                                    arguments))

    def exchange_unbind(self, destination='', source='',
                        routing_key='', nowait=False, arguments=None):
        """Unbind an exchange from an exchange.

        This method unbinds an exchange from an exchange.

        .. versionadded:: 0.26

        :param str destination: The destination exchange name
        :param str source: The source exchange name
        :param str routing_key: The routing key to bind with
        :param bool nowait: Do not send a reply method
        :param dict arguments: Optional arguments

        """
        self.rpc(spec.Exchange.Unbind(0, destination, source, routing_key,
                                      nowait, arguments))

    def queue_bind(self, queue='', exchange='', routing_key='',
                   nowait=False, arguments=None):
        """Bind queue to an exchange

        This method binds a queue to an exchange. Until a queue is bound it
        will not receive any messages. In a classic messaging model, store-and-
        forward queues are bound to a direct exchange and subscription queues
        are bound to a topic exchange.

        .. versionadded:: 0.26

        :param str queue: The queue name
        :param str exchange: Name of the exchange to bind to
        :param str routing_key: Message routing key
        :param bool nowait: Do not send a reply method
        :param dict arguments: Arguments for binding

        """
        self.rpc(spec.Queue.Bind(0, queue, exchange, routing_key, nowait,
                                 arguments))

    def queue_declare(self, queue='', passive=False, durable=False,
                      exclusive=False, auto_delete=False, nowait=False,
                      arguments=None):
        """Declare queue, create if needed

        This method creates or checks a queue. When creating a new queue the
        client can specify various properties that control the durability of
        the queue and its contents, and the level of sharing for the queue.

        .. versionadded:: 0.26

        :param str queue: The queue name
        :param bool passive: Do not create queue
        :param bool durable: Request a durable queue
        :param bool exclusive: Request an exclusive queue
        :param bool auto_delete: Auto-delete queue when unused
        :param bool nowait: Do not send a reply method
        :param dict arguments: Arguments for declaration

        """
        self.rpc(spec.Queue.Declare(0, queue, passive, durable, exclusive,
                                    auto_delete, nowait, arguments))

    def queue_delete(self, queue='', if_unused=False, if_empty=False,
                     nowait=False):
        """Delete a queue

        This method deletes a queue. When a queue is deleted any pending
        messages are sent to a dead-letter queue if this is defined in the
        server configuration, and all consumers on the queue are cancelled.

        .. versionadded:: 0.26

        :param str queue: The queue name
        :param bool if_unused: Delete only if unused
        :param bool if_empty: Delete only if empty
        :param bool nowait: Do not send a reply method

        """
        self.rpc(spec.Queue.Delete(0, queue, if_unused, if_empty, nowait))

    def queue_purge(self, queue='', nowait=False):
        """Purge a queue

        This method removes all messages from a queue which are not awaiting
        acknowledgment.

        .. versionadded:: 0.26

        :param str queue: The queue name
        :param bool nowait: Do not send a reply method

        """
        self.rpc(spec.Queue.Purge(0, queue, nowait))

    def queue_unbind(self, queue='', exchange='', routing_key='',
                     arguments=None):
        """Unbind a queue from an exchange

        This method unbinds a queue from an exchange.

        .. versionadded:: 0.26

        :param str queue: The queue name
        :param str exchange: The exchange name
        :param str routing_key: Routing key of binding
        :param dict arguments: Arguments of binding

        """
        self.rpc(spec.Queue.Unbind(0, queue, exchange, routing_key, arguments))

    def tx_select(self):
        """Select standard transaction mode

        .. versionadded:: 0.26

        This method sets the channel to use standard transactions. The client
        must use this method at least once on a channel before using the Commit
        or Rollback methods.

        """
        self.rpc(spec.Tx.Select())

    def tx_commit(self):
        """Commit the current transaction

        .. versionadded:: 0.26

        This method commits all message publications and acknowledgments
        performed in the current transaction.  A new transaction starts
        immediately after a commit.

        """
        self.rpc(spec.Tx.Commit())

    def tx_rollback(self):
        """Abandon the current transaction

        .. versionadded:: 0.26

        This method abandons all message publications and acknowledgments
        performed in the current transaction. A new transaction starts
        immediately after a rollback. Note that unacked messages will not be
        automatically redelivered by rollback; if that is required an explicit
        recover call should be issued.

        """
        self.rpc(spec.Tx.Rollback())

    @staticmethod
    def _build_open_frame():
        """Build and return a channel open frame

        :rtype: pamqp.spec.Channel.Open

        """
        return spec.Channel.Open()

    def _cancel_consumer(self, obj):
        """Cancel the consuming of a queue.

        :param rabbitpy.amqp_queue.Queue obj: The queue to cancel

        """
        self._interrupt_wait_on_frame()
        if obj.consumer_tag in self._consumers:
            del self._consumers[obj.consumer_tag]
        self._write_frame(spec.Basic.Cancel(consumer_tag=obj.consumer_tag))
        if not self.closed:
            self._wait_on_frame(spec.Basic.CancelOk)

    def _check_for_rpc_request(self, value):
        """Inspect a frame to see if it's a RPC request from RabbitMQ.

        :param spec.Frame value:

        """
        super(Channel, self)._check_for_rpc_request(value)
        if isinstance(value, spec.Basic.Return):
            self._on_basic_return(self._wait_for_content_frames(value))
        elif isinstance(value, spec.Basic.Cancel):
            self._waiting = False
            if value.consumer_tag in self._consumers:
                del self._consumers[value.consumer_tag]
            raise exceptions.RemoteCancellationException(value.consumer_tag)

    def _consume(self, obj, no_ack, priority=None):
        """Register a Queue object as a consumer, issuing Basic.Consume.

        :param rabbitpy.amqp_queue.Queue obj: The queue to consume
        :param bool no_ack: no_ack mode
        :param int priority: Consumer priority
        :raises: ValueError

        """
        args = dict()
        if priority is not None:
            if not self._supports_consumer_priorities:
                raise exceptions.NotSupportedError('consumer_priorities')
            if not isinstance(priority, int):
                raise ValueError('Consumer priority must be an int')
            args['x-priority'] = priority
        self.rpc(spec.Basic.Consume(queue=obj.name,
                                    consumer_tag=obj.consumer_tag,
                                    no_ack=no_ack,
                                    arguments=args))
        self._consumers[obj.consumer_tag] = (obj, no_ack)

    def _consume_message(self):
        """Get a message from the stack, blocking while doing so. If a consumer
        is cancelled out-of-band, we will receive a Basic.CancelOk
        instead.

        :rtype: rabbitpy.message.Message

        """
        if not self._consumers:
            raise exceptions.NotConsumingError
        frame_value = self._wait_on_frame([spec.Basic.Deliver])
        if frame_value:
            return self._wait_for_content_frames(frame_value)
        return None

    def _create_message(self, method_frame, header_frame, body):
        """Create a message instance with the channel it was received on and
        the dictionary of message parts. Will return None if no message can be
        created.

        :param pamqp.specification.Frame method_frame: The method frame value
        :param header_frame: Header frame value
        :type header_frame: pamqp.header.ContentHeader or None
        :param body: The message body
        :type body: str or None
        :rtype: rabbitpy.message.Message or None

        """
        if not method_frame:
            LOGGER.warning('Received empty method_frame, returning None')
            return None
        if not header_frame:
            LOGGER.debug('Malformed header frame: %r', header_frame)
        props = header_frame.properties.to_dict() if header_frame else dict()
        msg = message.Message(self, body, props)
        msg.method = method_frame
        msg.name = method_frame.name
        return msg

    def _get_from_read_queue(self):
        """Fetch a frame from the read queue and return it, otherwise return
        None

        :rtype: pamqp.specification.Frame

        """
        try:
            frame_value = self._read_queue.get(False)
        except Queue.Empty:
            return None

        try:
            self._read_queue.task_done()
        except ValueError:
            pass
        return frame_value

    def _get_message(self):
        """Try and get a delivered message from the connection's message stack.

        :rtype: rabbitpy.message.Message or None

        """
        frame_value = self._wait_on_frame([spec.Basic.GetOk,
                                           spec.Basic.GetEmpty])
        if isinstance(frame_value, spec.Basic.GetEmpty):
            return None
        return self._wait_for_content_frames(frame_value)

    def _multi_nack(self, delivery_tag, requeue=True):
        """Send a multiple negative acknowledgement, re-queueing the items

        :param int delivery_tag: The delivery tag for this channel
        :param bool requeue: Requeue the messages

        """
        if not self._supports_basic_nack:
            raise exceptions.NotSupportedError('Basic.Nack')
        if self._is_debugging:
            LOGGER.debug('Sending Basic.Nack with requeue')
        self.rpc(spec.Basic.Nack(delivery_tag=delivery_tag,
                                 multiple=True,
                                 requeue=requeue))

    def _on_basic_return(self, msg):
        """Raise a MessageReturnedException so the publisher can handle
        returned messages.

        :param pmqid.message.message msg: The message to add
        :raises: rabbitpy.exceptions.MessageReturnedException

        """
        # Could happen when closing
        if not msg:
            return
        LOGGER.warning('Basic.Return received on channel %i', self._channel_id)
        message_id = msg.properties.get('message_id', 'Unknown')
        raise exceptions.MessageReturnedException(message_id,
                                                  msg.method.reply_code,
                                                  msg.method.reply_text)

    def _reject_inbound_message(self, method_frame):
        """Used internally to reject a message when it's been received during
        a state that it should not have been.

        :param pamqp.specification.Basic.Deliver method_frame: The method frame

        """
        self.rpc(spec.Basic.Reject(delivery_tag=method_frame.delivery_tag,
                                   requeue=True))

    @property
    def _supports_basic_nack(self):
        """Indicates if the server supports Basic.Nack

        :rtype: bool

        """
        return self._server_capabilities.get(b'basic.nack', False)

    @property
    def _supports_consumer_cancel_notify(self):
        """Indicates if the server supports sending consumer cancellation
        notifications

        :rtype: bool

        """
        return self._server_capabilities.get(b'consumer_cancel_notify', False)

    @property
    def _supports_consumer_priorities(self):
        """Indicates if the server supports consumer priorities

        :rtype: bool

        """
        return self._server_capabilities.get(b'consumer_priorities', False)

    @property
    def _supports_per_consumer_qos(self):
        """Indicates if the server supports per consumer qos

        :rtype: bool

        """
        return self._server_capabilities.get(b'per_consumer_qos', False)

    @property
    def _supports_publisher_confirms(self):
        """Indicates if the server supports publisher confirmations

        :rtype: bool

        """
        return self._server_capabilities.get(b'publisher_confirms', False)

    def _wait_for_confirmation(self):
        """Used by the Message.publish method when publisher confirmations are
        enabled.

        :rtype: pamqp.frame.Frame

        """
        return self._wait_on_frame([spec.Basic.Ack, spec.Basic.Nack])

    def _wait_for_content_frames(self, method_frame):
        """Used by both Channel._get_message and Channel._consume_message for
        getting a message parts off the queue and returning the fully
        constructed message.

        :param method_frame: The method frame for the message
        :type method_frame: Basic.Deliver or Basic.Get or Basic.Return
        :rtype: rabbitpy.Message

        """
        if self.closing or self.closed:
            return None

        consuming = isinstance(method_frame, spec.Basic.Deliver)
        if consuming and not self._consumers:
            return None

        if self._is_debugging:
            LOGGER.debug('Waiting on content frames for %s: %r',
                         method_frame.name, method_frame.delivery_tag)

        header_value = self._wait_on_frame(CONTENT_HEADER)
        if not header_value:
            self._reject_inbound_message(method_frame)
            return None

        self._check_for_rpc_request(header_value)
        body_value = bytes() if PYTHON3 else str()
        while len(body_value) < header_value.body_size:
            body_part = self._wait_on_frame(CONTENT_BODY)
            self._check_for_rpc_request(body_part)
            if not body_part:
                break
            body_value += body_part.value
            if len(body_value) == header_value.body_size:
                break
            if self.closing or self.closed:
                return None
            if consuming and not self._consumers:
                self._reject_inbound_message(method_frame)
                return None
        return self._create_message(method_frame, header_value, body_value)
