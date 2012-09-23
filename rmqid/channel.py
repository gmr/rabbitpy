"""
Class representation of an AMQP channel

"""
import logging
from pamqp import specification

from rmqid import base


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

    def _open(self):
        """Open the channel"""
        self._set_state(self.OPENING)
        self._connection.write_frame(self._build_open_frame(),
                                     self._channel_id)
        self._connection.wait_on_frame(specification.Channel.OpenOk,
                                       self._channel_id)
        self._set_state(self.OPEN)
        LOGGER.debug('Channel #%i open', self._channel_id)

    def close(self):
        """Close the channel"""
        self._set_state(self.CLOSING)
        self._connection.write_frame(self._build_close_frame(),
                                     self._channel_id)
        self._connection.wait_on_frame(specification.Channel.CloseOk,
                                       self._channel_id)
        self._set_state(self.CLOSED)
        LOGGER.debug('Channel #%i closed', self._channel_id)

    def rpc(self, frame_value):
        """Send a RPC command to the remote server.

        :param pamqp.specification.Frame frame_value: The frame to send
        :rtype: pamqp.specification.Frame or None

        """
        self.write_frame(frame_value)
        if frame_value.synchronous:
            return self._connection.wait_on_frame(frame_value.valid_responses,
                                                  self._channel_id)

    def write_frame(self, frame_value):
        """Marshal the frame and write it to the socket.

        :param pamqp.specification.Frame or
               pamqp.header.ProtocolHeader frame_value: The frame to write

        """
        self._connection.write_frame(frame_value, self._channel_id)
