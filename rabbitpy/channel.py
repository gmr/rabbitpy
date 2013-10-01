"""
The Channel object is the communications object used by Exchanges, Messages,
Queues, and Transactions. It is created by invoking the
:py:meth:`rabbitpy.connection.Connection.channel` method. It can act as a context
manager, allowing for quick shorthand use:

    with connection.channel():
       # Do something

"""
import logging
try:
    import queue
except ImportError:
    import Queue as queue

from pamqp import specification
from pamqp import PYTHON3

from rabbitpy import base
from rabbitpy import exceptions
from rabbitpy import message

LOGGER = logging.getLogger(__name__)


class Channel(base.AMQPChannel):
    """The Channel class implements the channel communication layer on top of
    the Connection object, which is responsible for creating new channels.

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
                 maximum_frame_size):
        """Create a new instance of the Channel class

        :param int channel_id: The channel # to use for this instance
        :param events rabbitpy.Events: Event management object
        :param queue.Queue exception_queue: Exception queue
        :param queue.Queue read_queue: Queue to read pending frames from
        :param queue.Queue write_queue: Queue to write pending AMQP objs to
        :param int maximum_frame_size: The max frame size for msg bodies

        """
        super(Channel, self).__init__(exception_queue)
        self._channel_id = channel_id
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
        """When leaving the context, examine why the context is leaving, if it's
         an exception or what.

        """
        if exc_type:
            LOGGER.exception('Channel context manager closed on %s exception',
                             exc_type)
            raise exc_type(exc_val)
        if not self.closing and not self.closed:
            LOGGER.debug('Closing channel')
            self.close()

    def close(self):
        """Close the channel"""
        LOGGER.debug('Channel %i close invoked while %s',
                     self._channel_id, self.state_description)
        if not self.closing or self.closed:
            self._close()
            LOGGER.debug('Channel #%i closed', self._channel_id)

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

    def on_remote_close(self, frame_value):
        """Invoked by rabbitpy.connection.Connection when a remote channel close
        is issued.

        :param frame_value: The Channel.Close method frame
        :type frame_value: pamqp.specification.Channel.Close

        """
        self._set_state(self.REMOTE_CLOSED)
        raise exceptions.RemoteClosedChannelException(frame_value.reply_code,
                                                      frame_value.reply_text)

    def on_basic_return(self, frame_value):
        """Invoked by rabbit.py.io._run when a Basic.Return is delivered from
        RabbitMQ.

        :param frame_value: The Basic.Return method frame
        :type frame_value: pamqp.specification.Basic.Return

        """
        msg = self._wait_for_content_frames(frame_value)
        self._process_basic_return(msg)

    def open(self):
        """Open the channel, invoked directly upon creation by the Connection

        """
        self._set_state(self.OPENING)
        self._write_frame(self._build_open_frame())
        self._wait_on_frame(specification.Channel.OpenOk)
        self._set_state(self.OPEN)
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

    def rpc(self, frame_value):
        """Send a RPC command to the remote server.

        :param pamqp.specification.Frame frame_value: The frame to send
        :rtype: pamqp.specification.Frame or None

        """
        if self.closed:
            raise exceptions.ChannelClosedException()
        self._write_frame(frame_value)
        if frame_value.synchronous:
            return self._wait_on_frame(frame_value.valid_responses)

    def _build_open_frame(self):
        """Build and return a channel open frame

        :rtype: pamqp.specification.Channel.Open

        """
        return specification.Channel.Open()

    def _consume_message(self):
        """Try and get a delivered message from the connection's message stack.

        :rtype: rabbitpy.message.Message

        """
        frame_value = self._wait_on_frame([specification.Basic.Deliver,
                                           specification.Basic.Return])
        msg = self._wait_for_content_frames(frame_value)
        if isinstance(frame_value, specification.Basic.Return):
            self._process_basic_return(msg)
        return msg

    def _create_message(self, method_frame, header_frame, body):
        """Create a message instance with the channel it was received on and the
        dictionary of message parts.

        :param pamqp.specification.Frame method_frame: The method frame value
        :param pamqp.header.ContentHeader|None header_frame: Header frame value
        :param str|None body: The message body
        :rtype: rabbitpy.message.Message

        """
        if not header_frame:
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
        frame_value = self._wait_on_frame([specification.Basic.GetOk,
                                           specification.Basic.GetEmpty])
        if isinstance(frame_value, specification.Basic.GetEmpty):
            return None
        return self._wait_for_content_frames(frame_value)

    def _process_basic_return(self, msg):
        """Raise a MessageReturnedException so the publisher can handle returned
        messages.

        :param pmqid.message.message msg: The message to add
        :raises: rabbitpy.exceptions.MessageReturnedException

        """
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
        return self._wait_on_frame(specification.Confirm.SelectOk)

    def _wait_for_content_frames(self, method_frame):
        """Used by both Channel._get_message and Channel._consume_message for
        getting a message parts off the queue and returning the fully
        constructed message.

        :param method_frame: The method frame for the message
        :type method_frame: Basic.Deliver or Basic.Get or Basic.Return
        :rtype: rabbitpy.Message

        """
        header_value = self._wait_on_frame('ContentHeader')
        if not header_value:
            return self._create_message(method_frame, None, None)
        body_value = bytes() if PYTHON3 else str()
        while len(body_value) < header_value.body_size:
            body_part = self._wait_on_frame('ContentBody')
            if not body_part:
                break
            body_value += body_part.value
            if len(body_value) == header_value.body_size:
                break
        return self._create_message(method_frame, header_value, body_value)
