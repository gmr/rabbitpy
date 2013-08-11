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
import traceback

from pamqp import specification

from rabbitpy import base
from rabbitpy import exceptions


LOGGER = logging.getLogger(__name__)


class Channel(base.AMQPChannel):
    """The Channel class implements the channel communication layer on top of
    the Connection object, which is responsible for creating new channels.

    To create a new channel, invoke
    py:meth:`rabbitpy.connection.Connection.channel`

    """
    def __init__(self, channel_id, events, read_queue, write_queue):
        """Create a new instance of the Channel class

        :param int channel_id: The channel # to use for this instance
        :param events rabbitpy.Events: Event management object
        :param queue.Queue read_queue: Queue to read pending frames from
        :param queue.Queue write_queue: Queue to write pending AMQP objs to
        :param on_close threading.Event: Event to notify connection the channel

        """
        super(Channel, self).__init__()
        self._channel_id = channel_id
        self._events = events
        self._read_queue = read_queue
        self._write_queue = write_queue
        self._publisher_confirms = False

    def __enter__(self):
        """For use as a context manager, return a handle to this object
        instance.

        :rtype: Channel

        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """When leaving the context, examine why the context is leaving, if it's
         an exception or what.

        """
        if exc_type:
            traceback.print_exception(exc_type, exc_val, exc_tb)
            LOGGER.exception('Channel context manager closed on exception: %s',
                             exc_val)
            raise
        LOGGER.info('Closing channel')
        self.close()

    def close(self):
        """Close the channel"""
        self._set_state(self.CLOSING)
        self._write_frame(self._build_close_frame())
        close_ok = self._wait_on_frame()
        self._set_state(self.CLOSED)
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
    def name(self):
        """Return the name of the connection as RabbitMQ internally holds it
        for use when trying to get the state of the channel from the
        RabbitMQ API.

        :rtype: str

        """
        return '%s (%i)' % (self._connection.name, self._channel_id)

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
            return self._wait_on_frame()

    def _build_close_frame(self):
        """Build and return a channel close frame

        :rtype: pamqp.specification.Channel.Close

        """
        return specification.Channel.Close(200, 'Normal Shutdown')

    def _build_open_frame(self):
        """Build and return a channel open frame

        :rtype: pamqp.specification.Channel.Open

        """
        return specification.Channel.Open()

    def _get_message(self):
        """Try and get a delivered message from the connection's message stack.

        :rtype: rabbitpy.message.Message

        """
        return self._wait_on_frame()

    def _open(self):
        """Open the channel"""
        self._set_state(self.OPENING)
        self._write_frame(self._build_open_frame())
        open_ok = self._wait_on_frame()
        LOGGER.debug(open_ok)
        self._set_state(self.OPEN)
        LOGGER.debug('Channel #%i open', self._channel_id)

    def _process_basic_return(self, frame_value, message):
        """Raise a MessageReturnedException so the publisher can handle returned
        messages.

        :param int channel_id: The channel id the frame was received on
        :param pamqp.specification.Basic.Return frame_value: Method frame
        :param pmqid.message.message message: The message to add
        :raises: rabbitpy.exceptions.MessageReturnedException

        """
        LOGGER.warning('Basic.Return received on channel %i', self._channel_id)
        message_id = message.properties.get('message_id', 'Unknown')
        raise exceptions.MessageReturnedException(message_id,
                                                  frame_value.reply_code,
                                                  frame_value.reply_text)

    def _remote_close(self):
        """Invoked by rabbitpy.connection.Connection when a remote channel close
        is issued.

        """
        self._set_state(self.CLOSED)

    def _wait_for_confirmation(self):
        """Used by the Message.publish method when publisher confirmations are
        enabled.

        :rtype: pamqp.frame.Frame

        """
        return self._wait_on_frame()
