"""
The heartbeat class implements the logic for sending heartbeats every
configured interval.

"""
import logging
import threading
import typing

from rabbitpy import channel0 as rabbitpy_channel0, io as rabbitpy_io

LOGGER = logging.getLogger(__name__)


class Heartbeat:
    """Send a heartbeat frame every interval if no data has been written.

    :param io: Used to get the # of bytes written each interval
    :param channel0: The channel that the heartbeat is sent over.

    """
    def __init__(self,
                 io: rabbitpy_io.IO,
                 channel0: rabbitpy_channel0.Channel0,
                 interval: float):
        self._channel0 = channel0
        self._interval = float(interval) / 2.0
        self._io = io
        self._last_written = self._io.bytes_written
        self._lock = threading.Lock()
        self._timer: typing.Optional[threading.Timer] = None

    def start(self) -> None:
        """Start the heartbeat checker"""
        if not self._interval:
            LOGGER.debug('Heartbeats are disabled, not starting')
            return
        self._start_timer()
        LOGGER.debug(
            'Heartbeat started, ensuring data is written every %.2f seconds',
            self._interval)

    def stop(self) -> None:
        """Stop the heartbeat checker"""
        if self._timer:
            self._timer.cancel()
            self._timer = None

    def _start_timer(self) -> None:
        """Create a new thread timer, destroying the last if it existed."""
        if self._timer:
            del self._timer
        self._timer = threading.Timer(self._interval, self._maybe_send)
        self._timer.daemon = True
        self._timer.start()

    def _maybe_send(self) -> None:
        """Fired by threading.Timer every ``self._interval`` seconds to
        maybe send a heartbeat to the remote connection, if no other frames
        have been written.

        """
        if not self._io.bytes_written - self._last_written:
            self._channel0.send_heartbeat()
        self._lock.acquire(True)
        self._last_written = self._io.bytes_written
        self._lock.release()
        self._start_timer()
