"""
Class representation of an AMQP channel

"""
import logging
from pamqp import specification

from rmqid import base
from rmqid import exceptions


LOGGER = logging.getLogger(__name__)


class Channel(base.StatefulObject):
    """The Connection object is responsible for negotiating a connection and
    managing its state.

    """
    def __init__(self, channel_id, connection):
        """Create a new instance of the Channel class

        :param int channel_id: The channel id to use for this instance
        :param rmqid.Connection: The connection to communicate with

        """
        super(Channel, self).__init__()
        self._channel_id = channel_id
        self._connection = connection
        self.maximum_frame_size = connection.maximum_frame_size
        self._open()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            raise exc_type(exc_val)
        LOGGER.info('Closing channel')
        self.close()

    def close(self):
        """Close the channel"""
        self._set_state(self.CLOSING)
        self._connection._write_frame(self._build_close_frame(),
                                      self._channel_id)
        self._connection._wait_on_frame(specification.Channel.CloseOk,
                                        self._channel_id)
        self._set_state(self.CLOSED)
        LOGGER.debug('Channel #%i closed', self._channel_id)

    @property
    def closed(self):
        return self._state == self.CLOSED

    @property
    def id(self):
        """Return the channel id

        :rtype: int

        """
        return self._channel_id

    def prefetch_count(self, value, all_channels=False):
        self.rpc(specification.Basic.Qos(prefetch_count=value,
                                         global_=all_channels))

    def prefetch_size(self, value, all_channels=False):
        self.rpc(specification.Basic.Qos(prefetch_count=value,
                                         global_=all_channels))

    def rpc(self, frame_value):
        """Send a RPC command to the remote server.

        :param pamqp.specification.Frame frame_value: The frame to send
        :rtype: pamqp.specification.Frame or None

        """
        if self.closed:
            raise exceptions.ChannelClosedException()

        self._write_frame(frame_value)
        if frame_value.synchronous:
            return self._connection._wait_on_frame(frame_value.valid_responses,
                                                   self._channel_id)

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

        :rtype: rmqid.message.Message

        """
        return self._connection._wait_on_frame([specification.Basic.Deliver,
                                                specification.Basic.CancelOk],
                                               self._channel_id)

    def _open(self):
        """Open the channel"""
        self._set_state(self.OPENING)
        self._connection._write_frame(self._build_open_frame(),
                                      self._channel_id)
        self._connection._wait_on_frame(specification.Channel.OpenOk,
                                        self._channel_id)
        self._set_state(self.OPEN)
        LOGGER.debug('Channel #%i open', self._channel_id)


    def _remote_close(self):
        self._set_state(self.CLOSED)

    def _write_frame(self, frame_value):
        """Marshal the frame and write it to the socket.

        :param frame_value: The frame to send
        :type frame_value: pamqp.specification.Frame

        """
        self._connection._write_frame(frame_value, self._channel_id)
