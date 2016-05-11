"""
Implement the base channel construct that is used by rabbitpy objects
such as :py:class:`Exchange <rabbitpy.Exchange>` or
:py:class:`Exchange <rabbitpy.Queue>`. It is responsible for coordinating
the communication between the IO thread and the higher-level objects.

"""
import logging

from pamqp import specification as spec
from pamqp import PYTHON3

from rabbitpy import base
from rabbitpy import exceptions
from rabbitpy import message
from rabbitpy.utils import queue

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
    py:meth:`~rabbitpy.connection.Connection.channel()`


    To improve performance, pass blocking_read to True. Note that doing
    so prevents ``KeyboardInterrupt``/CTRL-C from exiting the Python
    interpreter.

    :param int channel_id: The channel # to use for this instance
    :param dict server_capabilities: Features the server supports
    :param events: Event management object
    :type events: rabbitpy.Events
    :param exception_queue: Exception queue
    :type exception_queue: queue.Queue
    :param read_queue: Queue to read pending frames from
    :type read_queue: queue.Queue
    :param write_queue: Queue to write pending AMQP objs to
    :type write_queue: queue.Queue
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
        self._consuming = False
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
    def id(self):  # pylint: disable=invalid-name
        """Return the channel id

        :rtype: int

        """
        return self._channel_id

    @property
    def maximum_frame_size(self):
        """Return the AMQP maximum frame size

        :rtype: int

        """
        return self._maximum_frame_size

    def open(self):
        """Open the channel, invoked directly upon creation by the Connection

        """
        self._set_state(self.OPENING)
        self.write_frame(self._build_open_frame())
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

    @staticmethod
    def _build_open_frame():
        """Build and return a channel open frame

        :rtype: pamqp.spec.Channel.Open

        """
        return spec.Channel.Open()

    def _cancel_consumer(self, obj, consumer_tag=None, nowait=False):
        """Cancel the consuming of a queue.

        :param rabbitpy.amqp_queue.Queue obj: The queue to cancel

        """
        consumer_tag = consumer_tag or obj.consumer_tag
        self._interrupt_wait_on_frame(self._on_ready_to_cancel,
                                      consumer_tag, nowait)

    def _on_ready_to_cancel(self, consumer_tag, nowait):
        LOGGER.debug('Cancelling consumer')
        if consumer_tag in self._consumers:
            del self._consumers[consumer_tag]
        if nowait:
            self.write_frame(spec.Basic.Cancel(consumer_tag=consumer_tag,
                                               nowait=True))
            return
        self.rpc(spec.Basic.Cancel(consumer_tag=consumer_tag))

    def _check_for_rpc_request(self, value):
        """Inspect a frame to see if it's a RPC request from RabbitMQ.

        :param spec.Frame value:

        """
        LOGGER.debug('Checking for RPC request: %r', value)
        super(Channel, self)._check_for_rpc_request(value)
        if isinstance(value, spec.Basic.Return):
            raise exceptions.MessageReturnedException(value.reply_code,
                                                      value.reply_text,
                                                      value.exchange,
                                                      value.routing_key)
        elif isinstance(value, spec.Basic.Cancel):
            self._waiting = False
            if value.consumer_tag in self._consumers:
                del self._consumers[value.consumer_tag]
            raise exceptions.RemoteCancellationException(value.consumer_tag)

    def _consume(self, obj, no_ack, priority=None):
        """Register a Queue object as a consumer, issuing Basic.Consume.

        :param obj: The queue to consume
        :type obj: rabbitpy.amqp_queue.Queue
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
        LOGGER.debug('Waited on frame, got %r', frame_value)
        if frame_value:
            return self._wait_for_content_frames(frame_value)
        return None

    def _create_message(self, method_frame, header_frame, body):
        """Create a message instance with the channel it was received on and
        the dictionary of message parts. Will return None if no message can be
        created.

        :param method_frame: The method frame value
        :type method_frame: pamqp.specification.Frame
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
        except queue.Empty:
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
    def _supports_consumer_cancel_notify(self):  # pylint: disable=invalid-name
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
        if self._interrupt_is_set:
            return self._on_interrupt_set()

        error = False
        body_value = bytes() if PYTHON3 else str()
        while len(body_value) < header_value.body_size:
            body_part = self._wait_on_frame(CONTENT_BODY)
            self._check_for_rpc_request(body_part)
            if self._interrupt_is_set:
                self._on_interrupt_set()
                error = True
            elif not body_part:
                break
            elif self.closing or self.closed:
                error = True
            elif consuming and not self._consumers:
                self._reject_inbound_message(method_frame)
                error = True

            if error:
                return

            body_value += body_part.value
            if len(body_value) == header_value.body_size:
                break

        return self._create_message(method_frame, header_value, body_value)
