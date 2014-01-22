"""


"""
import logging
try:
    import queue
except ImportError:
    import Queue as queue

from pamqp import specification
from pamqp import PYTHON3

from rabbitpy import DEBUG
from rabbitpy import base
from rabbitpy import exceptions
from rabbitpy import message

LOGGER = logging.getLogger(__name__)


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

    """
    DEFAULT_CLOSE_CODE = 200
    DEFAULT_CLOSE_REASON = 'Normal Shutdown'
    REMOTE_CLOSED = 0x04
    STATES = base.AMQPChannel.STATES
    STATES[0x04] = 'Remotely Closed'

    def __init__(self, channel_id, events,
                 exception_queue, read_queue, write_queue,
                 maximum_frame_size, write_trigger):
        """Create a new instance of the Channel class

        :param int channel_id: The channel # to use for this instance
        :param events rabbitpy.Events: Event management object
        :param queue.Queue exception_queue: Exception queue
        :param queue.Queue read_queue: Queue to read pending frames from
        :param queue.Queue write_queue: Queue to write pending AMQP objs to
        :param int maximum_frame_size: The max frame size for msg bodies
        :param socket write_trigger: Write to this socket to break IO waiting

        """
        super(Channel, self).__init__(exception_queue, write_trigger)
        self._channel_id = channel_id
        self._consumers = []
        self._events = events
        self._maximum_frame_size = maximum_frame_size
        self._read_queue = read_queue
        self._write_queue = write_queue
        self._publisher_confirms = False

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
        if exc_val:
            LOGGER.error('Channel context manager closed on %r exception',
                         exc_val, exc_info=True)
            raise exc_type(exc_val)
        if not self.closing and not self.closed:
            self.close()

    def close(self):
        """Close the channel, cancelling any active consumers, purging the read
        queue, while looking to see if a Basic.Nack should be sent, sending it
        if so.

        """
        if self.closed:
            if DEBUG:
                LOGGER.debug('Channel %i close invoked when already closed',
                             self._channel_id)
            return
        self._set_state(self.CLOSING)
        for queue_obj in self._consumers:
            self._cancel_consumer(queue_obj)

        # Empty the queue and nack the max id (and all previous)
        if self._consumers:
            if DEBUG:
                LOGGER.debug('Channel %i purging read queue & nacking msgs',
                             self._channel_id)
            delivery_tag = 0
            discard_counter = 0
            while not self._read_queue.empty():
                try:
                    frame_value = self._read_queue.get(False)
                    self._read_queue.task_done()
                except queue.Empty:
                    break
                if frame_value.name == 'Basic.Deliver':
                    if delivery_tag < frame_value.delivery_tag:
                        delivery_tag = frame_value.delivery_tag
                discard_counter += 1
            if delivery_tag:
                self._multi_nack(delivery_tag)
            if DEBUG:
                LOGGER.debug('Discarded %i pending frames', discard_counter)
        super(Channel, self).close()

    def enable_publisher_confirms(self):
        """Turn on Publisher Confirms. If confirms are turned on, the
        Message.publish command will return a bool indicating if a message has
        been successfully published.

        """
        self.rpc(specification.Confirm.Select())
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

    def on_remote_close(self, value):
        """Invoked by rabbitpy.connection.Connection when a remote channel
        close is issued.

        :param value: The Channel.Close method frame
        :type value: pamqp.specification.Channel.Close

        """
        self._set_state(self.REMOTE_CLOSED)
        if value.reply_code in exceptions.AMQP:
            raise exceptions.AMQP[value.reply_code](value)
        else:
            raise exceptions.RemoteClosedChannelException(self._channel_id,
                                                          value.reply_code,
                                                          value.reply_text)

    def on_basic_cancel(self, frame_value):
        """Invoked by rabbit.py.io._run when a Basic.Return is delivered from
        RabbitMQ.

        :param frame_value: The Basic.Return method frame
        :type frame_value: pamqp.specification.Basic.Return

        """
        self._process_basic_return(self._wait_for_content_frames(frame_value))

    def on_basic_return(self, frame_value):
        """Invoked by rabbit.py.io._run when a Basic.Return is delivered from
        RabbitMQ.

        :param frame_value: The Basic.Return method frame
        :type frame_value: pamqp.specification.Basic.Return

        """
        self._process_basic_return(self._wait_for_content_frames(frame_value))

    def open(self):
        """Open the channel, invoked directly upon creation by the Connection

        """
        self._set_state(self.OPENING)
        self._write_frame(self._build_open_frame())
        self._wait_on_frame(specification.Channel.OpenOk)
        self._set_state(self.OPEN)
        if DEBUG:
            LOGGER.debug('Channel #%i open', self._channel_id)

    def prefetch_count(self, value, all_channels=False):
        """Set a prefetch count for the channel (or all channels on the same
        connection).

        :param int value: The prefetch count to set
        :param bool all_channels: Set the prefetch count on all channels on the
                                  same connection

        """
        self.rpc(specification.Basic.Qos(prefetch_count=value,
                                         global_=all_channels))

    def prefetch_size(self, value, all_channels=False):
        """Set a prefetch size in bytes for the channel (or all channels on the
        same connection).

        :param int value: The prefetch size to set
        :param bool all_channels: Set the prefetch size on all channels on the
                                  same connection

        """
        if value is None:
            return
        self.rpc(specification.Basic.Qos(prefetch_count=value,
                                         global_=all_channels))

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
        self.rpc(specification.Basic.Recover(requeue=requeue))

    @staticmethod
    def _build_open_frame():
        """Build and return a channel open frame

        :rtype: pamqp.specification.Channel.Open

        """
        return specification.Channel.Open()

    def _cancel_consumer(self, obj):
        """Cancel the consuming of a queue.

        :param rabbitpy.amqp_queue.Queue obj: The queue to cancel

        """
        frame_value = specification.Basic.Cancel(consumer_tag=obj.consumer_tag)
        self._write_frame(frame_value)

    def _consume(self, obj, no_ack, priority):
        """Register a Queue object as a consumer, issuing Basic.Consume.

        :param rabbitpy.amqp_queue.Queue obj: The queue to consume
        :param bool no_ack: no_ack mode
        :param int priority: Consumer priority
        :raises: ValueError

        """
        args = dict()
        if priority is not None:
            if not isinstance(priority, int):
                raise ValueError('Consumer priority must be an int')
            args['x-priority'] = priority
        self.rpc(specification.Basic.Consume(queue=obj.name,
                                             consumer_tag=obj.consumer_tag,
                                             no_ack=no_ack,
                                             arguments=args))
        self._consumers.append(obj)

    def _consume_message(self):
        """Get a message from the stack, blocking while doing so.

        :rtype: rabbitpy.message.Message

        """
        frame_value = self._wait_on_frame('Basic.Deliver')
        return self._wait_for_content_frames(frame_value)

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
        if DEBUG and not header_frame:
            LOGGER.debug('Malformed header frame: %r', header_frame)
        props = header_frame.properties.to_dict() if header_frame else dict()
        msg = message.Message(self, body, props)
        msg.method = method_frame
        msg.name = method_frame.name
        return msg

    def _get_message(self):
        """Try and get a delivered message from the connection's message stack.

        :rtype: rabbitpy.message.Message or None

        """
        if DEBUG:
            LOGGER.debug('Waiting on GetOk or GetEmpty')
        frame_value = self._wait_on_frame([specification.Basic.GetOk,
                                           specification.Basic.GetEmpty])
        if DEBUG:
            LOGGER.debug('Returned with %r', frame_value)
        if isinstance(frame_value, specification.Basic.GetEmpty):
            return None
        if DEBUG:
            LOGGER.debug('Waiting on content frames for %r', frame_value)
        return self._wait_for_content_frames(frame_value)

    def _multi_nack(self, delivery_tag):
        """Send a multiple negative acknowledgement, re-queueing the items

        :param int delivery_tag: The delivery tag for this channel

        """
        if DEBUG:
            LOGGER.debug('Sending Basic.Nack with requeue')
        self.rpc(specification.Basic.Nack(delivery_tag=delivery_tag,
                                          multiple=True,
                                          requeue=True))

    def _process_basic_return(self, msg):
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

    def _wait_for_confirmation(self):
        """Used by the Message.publish method when publisher confirmations are
        enabled.

        :rtype: pamqp.frame.Frame

        """
        return self._wait_on_frame(['Basic.Ack', 'Basic.Nack'])

    def _wait_for_content_frames(self, method_frame):
        """Used by both Channel._get_message and Channel._consume_message for
        getting a message parts off the queue and returning the fully
        constructed message.

        :param method_frame: The method frame for the message
        :type method_frame: Basic.Deliver or Basic.Get or Basic.Return
        :rtype: rabbitpy.Message

        """
        if DEBUG:
            LOGGER.debug('Waiting on ContentHeader')
        header_value = self._wait_on_frame('ContentHeader')
        if not header_value:
            if DEBUG:
                LOGGER.debug('No header value')
            return self._create_message(method_frame, None, None)
        body_value = bytes() if PYTHON3 else str()
        if DEBUG:
            LOGGER.debug('Waiting for %i body bytes', header_value.body_size)
        while len(body_value) < header_value.body_size:
            body_part = self._wait_on_frame('ContentBody')
            if not body_part:
                break
            body_value += body_part.value
            if len(body_value) == header_value.body_size:
                break
            if self.closing or self.closed:
                if DEBUG:
                    LOGGER.debug('Exiting from waiting for content frames')
                return None
        return self._create_message(method_frame, header_value, body_value)
