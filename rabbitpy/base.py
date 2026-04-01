"""
Base classes for various parts of rabbitpy

"""

import logging
import queue
import socket
import threading
import typing

from pamqp import base, body, commands, header, heartbeat

from rabbitpy import exceptions, utils

if typing.TYPE_CHECKING:
    from rabbitpy import channel as chan
    from rabbitpy import connection as conn
    from rabbitpy import message

LOGGER = logging.getLogger(__name__)


class Interrupt(typing.TypedDict):
    event: threading.Event
    callback: typing.Callable | None
    args: typing.Iterable[typing.Any] | None


FrameTypes = typing.Union[str, type[base.Frame], list[str | type[base.Frame]]]


class ChannelWriter:  # pylint: disable=too-few-public-methods
    """The AMQP Adapter provides a more generic, non-opinionated interface to
    RabbitMQ by providing methods that map to the AMQP API.

    :param channel: The channel to write to

    """

    def __init__(self, channel: chan.Channel):
        self.channel = channel

    def _rpc(
        self, frame_value: base.Frame
    ) -> (
        base.Frame
        | message.Message
        | commands.Basic.GetOk
        | commands.Basic.GetEmpty
        | commands.Queue.DeclareOk
    ):
        """Execute the RPC command for the frame.

        :param frame_value: The frame to send

        """
        LOGGER.debug('Issuing RPC to RabbitMQ: %r', frame_value)
        if self.channel.closed:
            raise exceptions.ChannelClosedException()
        return self.channel.rpc(frame_value)

    def _write_frame(self, frame_value: base.Frame) -> None:
        """Write a frame to the channel's connection

        :param frame_value: The frame to send

        """
        self.channel.write_frame(frame_value)


class AMQPClass(ChannelWriter):  # pylint: disable=too-few-public-methods
    """Base Class object AMQP object classes"""

    def __init__(self, channel: chan.Channel, name: str):
        """Create a new ClassObject.

        :param channel: The channel to execute commands on
        :param name: Set the name
        :raises: ValueError

        """
        super().__init__(channel)
        from rabbitpy import channel as chan_mod  # local import to avoid cycle

        if not isinstance(channel, chan_mod.Channel):
            raise ValueError('channel must be a valid rabbitpy Channel object')
        if not isinstance(name, str):
            raise ValueError('name must be a string')
        self.name = name


class StatefulObject(utils.DebuggingOptimizationMixin):
    """Base object for rabbitpy classes that need to maintain state such as
    connection and channel.

    """

    CLOSED = 0
    CLOSING = 1
    OPEN = 2
    OPENING = 3

    STATES = {0: 'Closed', 1: 'Closing', 2: 'Open', 3: 'Opening'}

    def __init__(self):
        """Create a new instance of the object defaulting to a closed state."""
        super().__init__()
        self._state: int = self.CLOSED

    def _set_state(self, value: int) -> None:
        """Set the state to the specified value, validating it is a supported
        state value.

        :param value: The new state value

        """
        if value not in self.STATES.keys():
            raise ValueError(f'Invalid state value: {value!r}')
        if self._is_debugging:
            LOGGER.debug(
                '%s setting state to %r',
                self.__class__.__name__,
                self.STATES[value],
            )
        self._state = value

    @property
    def closed(self) -> bool:
        """Returns True if in the CLOSED runtime state"""
        return self._state == self.CLOSED

    @property
    def closing(self) -> bool:
        """Returns True if in the CLOSING runtime state"""
        return self._state == self.CLOSING

    @property
    def open(self) -> bool:
        """Returns True if in the OPEN runtime state"""
        return self._state == self.OPEN

    @property
    def opening(self) -> bool:
        """Returns True if in the OPENING runtime state"""
        return self._state == self.OPENING

    @property
    def state(self) -> int:
        """Return the runtime state value"""
        return self._state

    @property
    def state_description(self) -> str:
        """Returns the text based description of the runtime state"""
        return self.STATES[self._state]


class AMQPChannel(StatefulObject):
    """Base AMQP Channel Object"""

    CLOSE_REQUEST_FRAME = commands.Channel.Close
    DEFAULT_CLOSE_CODE = 200
    DEFAULT_CLOSE_REASON = 'Normal Shutdown'
    REMOTE_CLOSED = 0x04

    def __init__(
        self,
        exception_queue: queue.Queue,
        write_trigger: socket.socket,
        connection: conn.Connection,
        blocking_read: bool = False,
    ):
        super().__init__()
        if blocking_read:
            LOGGER.debug('Initialized with blocking read')
        self.blocking_read: bool = blocking_read
        self._interrupt: Interrupt = {
            'event': threading.Event(),
            'callback': None,
            'args': None,
        }
        self._channel_id: int | None = None
        self._connection: conn.Connection = connection
        self._exceptions: queue.Queue = exception_queue
        self._read_queue: queue.Queue | None = None
        self._waiting: bool = False
        self._write_lock = threading.Lock()
        self._write_queue: queue.Queue | None = None
        self._write_trigger: socket.socket = write_trigger

    def __int__(self) -> int:
        """Return the numeric channel ID"""
        return self._channel_id

    def close(self) -> None:
        """Close the AMQP channel"""
        if self._connection.closed:
            LOGGER.debug('Connection is closed, bailing')
            return

        if self.closed:
            LOGGER.debug(
                'AMQPChannel %i close invoked and already closed',
                self._channel_id,
            )
            return

        LOGGER.debug(
            'Channel %i close invoked while %s',
            self._channel_id,
            self.state_description,
        )

        # Make sure there are no RPC frames pending
        self._check_for_pending_frames()

        if not self.closing:
            self._set_state(self.CLOSING)

        frame_value = self._build_close_frame()
        if self._is_debugging:
            LOGGER.debug(
                'Channel %i Waiting for a valid response for %s',
                self._channel_id,
                frame_value.name,
            )
        self.rpc(frame_value)
        if self._is_debugging:
            LOGGER.debug('Channel #%i closed', self._channel_id)
        self._set_state(self.CLOSED)

    def rpc(
        self, frame_value: base.Frame
    ) -> base.Frame | commands.Queue.DeclareOk:
        """Send an RPC command to the remote server. This should not be
        directly invoked.

        :param frame_value: The frame to send

        """
        if self.closed:
            raise exceptions.ChannelClosedException()
        if self._is_debugging:
            LOGGER.debug('Sending %r', frame_value.name)
        self.write_frame(frame_value)
        if frame_value.synchronous:
            return self._wait_on_frame(frame_value.valid_responses)

    def wait_for_confirmation(self) -> base.Frame:
        """Used by the `Message.publish` method when publisher confirmations
        are enabled.

        """
        return self._wait_on_frame([commands.Basic.Ack, commands.Basic.Nack])

    def write_frame(self, value: base.Frame | heartbeat.Heartbeat) -> None:
        """Put the frame in the write queue for the IOWriter object to write to
        the socket when it can. This should not be directly invoked.

        :param value: The frame to write

        """
        if self._can_write():
            if self._is_debugging:
                LOGGER.debug('Writing frame: %s', value.name)
            with self._write_lock:
                self._write_queue.put((self._channel_id, value))
            self._trigger_write()

    def write_frames(self, frames: list[base.Frame]) -> None:
        """Add a list of frames for the IOWriter object to write to the socket
        when it can.

        :param frames: The list of frame to write

        """
        if self._can_write():
            if self._is_debugging:
                LOGGER.debug(
                    'Writing frames: %r', [frame.name for frame in frames]
                )
            with self._write_lock:
                for frame in frames:
                    self._write_queue.put((self._channel_id, frame))
            self._trigger_write()

    def _build_close_frame(self) -> commands.Channel.Close:
        """Return the proper close frame for this object."""
        return self.CLOSE_REQUEST_FRAME(
            self.DEFAULT_CLOSE_CODE, self.DEFAULT_CLOSE_REASON
        )

    def _can_write(self) -> bool:
        self._check_for_exceptions()
        if self._connection.closed:
            raise exceptions.ConnectionClosed()
        elif self.closed:
            raise exceptions.ChannelClosedException()
        return True

    def _check_for_exceptions(self) -> None:
        """Check if there are any queued exceptions to raise, raising it if
        there is.

        """
        if not self._exceptions.empty():
            exception = self._exceptions.get()
            self._exceptions.task_done()
            raise exception

    def _check_for_pending_frames(self) -> None:
        value = self._read_from_queue()
        if value:
            self._check_for_rpc_request(value)
            if self._is_debugging:
                LOGGER.debug('Read frame while shutting down: %r', value)

    def _check_for_rpc_request(self, value: base.Frame) -> None:
        """Implement in child objects to inspect frames for channel specific
        RPC requests from RabbitMQ.

        """
        if isinstance(value, commands.Channel.Close):
            if self._is_debugging:
                LOGGER.debug('Channel closed')
            self._on_remote_close(value)

    def _force_close(self) -> None:
        """Force the channel to mark itself as closed"""
        self._set_state(self.CLOSED)
        if self._is_debugging:
            LOGGER.debug('Channel #%i closed', self._channel_id)

    def _interrupt_wait_on_frame(
        self, callback: typing.Callable, *args
    ) -> None:
        """Invoke to interrupt the current self._wait_on_frame blocking loop
        in order to allow for a flow such as waiting on a full message while
        consuming. Will wait until the ``_wait_on_frame_interrupt`` is cleared
        to make this a blocking operation.

        :param callback: The method to call
        :param args: Args to pass to the callback

        """
        self._check_for_exceptions()
        if not self._waiting:
            if self._is_debugging:
                LOGGER.debug('No need to interrupt wait')
            return callback(*args)
        LOGGER.debug('Interrupting the wait on frame')
        self._interrupt['callback'] = callback
        self._interrupt['args'] = args
        self._interrupt['event'].set()

    @property
    def _interrupt_is_set(self) -> bool:
        return self._interrupt['event'].is_set()

    def _on_interrupt_set(self) -> None:
        """Invoke interrupt value then clear out stack"""
        self._interrupt['callback'](*self._interrupt['args'])
        self._interrupt['event'].clear()
        self._interrupt['callback'] = None
        self._interrupt['args'] = None

    def _on_remote_close(self, value: commands.Channel.Close) -> None:
        """Handle RabbitMQ remotely closing the channel

        :param value: The Channel.Close method frame
        :raises: exceptions.RemoteClosedChannelException
        :raises: exceptions.AMQPException

        """
        self._set_state(self.REMOTE_CLOSED)
        if value.reply_code in exceptions.AMQP:
            LOGGER.error(
                'Received remote close (%s): %s',
                value.reply_code,
                value.reply_text,
            )
            raise exceptions.AMQP[value.reply_code](value)
        else:
            raise exceptions.RemoteClosedChannelException(
                self._channel_id, value.reply_code, value.reply_text
            )

    def _read_from_queue(self) -> base.Frame | commands.Basic.Deliver | None:
        """Check to see if a frame is in the queue and if so, return it"""
        if self._can_write() and not self.closing and self.blocking_read:
            if self._is_debugging:
                LOGGER.debug('Performing a blocking read')
            value = self._read_queue.get()
            self._read_queue.task_done()
        else:
            try:
                value = self._read_queue.get(True, 0.1)
                self._read_queue.task_done()
            except queue.Empty:
                value = None
        return value

    def _trigger_write(self) -> None:
        """Notifies the IO loop we need to write a frame by writing a byte
        to a local socket.

        """
        try:
            self._write_trigger.send(b'0')
        except OSError:
            pass

    def _validate_frame_type(
        self, frame_value: base.Frame, frame_type: FrameTypes
    ) -> bool:
        """Validate the frame value against the frame type. The frame type can
        be an individual frame type or a list of frame types.

        :param frame_value: The frame to check
        :param frame_type: The frame(s) to check against

        """
        if frame_value is None:
            if self._is_debugging:
                LOGGER.debug('Frame value is none?')
            return False
        if isinstance(frame_type, str):
            if frame_value.name == frame_type:
                return True
        elif isinstance(frame_type, list):
            for frame_t in frame_type:
                result = self._validate_frame_type(frame_value, frame_t)
                if result:
                    return True
            return False
        elif isinstance(frame_value, commands.Frame):
            return frame_value.name == frame_type.name
        return False

    def _wait_on_frame(
        self, frame_type: FrameTypes
    ) -> (
        base.Frame
        | commands.Basic.Deliver
        | commands.Queue.DeclareOk
        | header.ContentHeader
        | body.ContentBody
    ):
        """Read from the queue, blocking until a result is returned. An
        individual frame type or a list of frame types can be passed in to wait
        for specific frame types. If there is no match on the frame retrieved
        from the queue, put the frame back in the queue and recursively
        call the method.

        :param frame_type: The name, frame type, list of names or frame types

        """
        self._check_for_exceptions()
        if isinstance(frame_type, list) and len(frame_type) == 1:
            frame_type = frame_type[0]
        if self._is_debugging:
            LOGGER.debug('Waiting on %r frame(s)', frame_type)
        start_state = self.state
        self._waiting = True
        while (
            start_state == self.state
            and not self.closed
            and not self._connection.closed
        ):
            value = self._read_from_queue()
            if value is not None:
                self._check_for_rpc_request(value)
                if frame_type and self._validate_frame_type(value, frame_type):
                    self._waiting = False
                    return value
                self._read_queue.put(value)

            # Allow for any exceptions to be raised
            self._check_for_exceptions()

            # If the wait interrupt is set, break out of the loop
            if self._interrupt_is_set:
                break

        self._waiting = False

        # Clear here to ensure out of processing loop before proceeding
        if self._interrupt_is_set:
            self._on_interrupt_set()
