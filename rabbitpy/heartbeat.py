"""
The heartbeat class implements the logic for sending heartbeats every
configured interval.

"""
import logging
import threading

LOGGER = logging.getLogger(__name__)


class Heartbeat(object):
    """Send a heartbeat frame every interval if no data has been written.

    :param rabbitpy.io.IO io: Used to get the # of bytes written each interval
    :param rabbitpy.channel0.Channel channel0: The channel that the heartbeat
        is sent over.

    """

    def __init__(self, io, channel0, interval):
        self._channel0 = channel0
        self._interval = float(interval) / 2.0
        self._io = io
        self._last_written = self._io.bytes_written
        self._lock = threading.Lock()
        self._timer = None

    def start(self):
        """Start the heartbeat checker"""
        if not self._interval:
            LOGGER.debug('Heartbeats are disabled, not starting')
            return
        self._start_timer()
        LOGGER.debug('Heartbeat started, ensuring data is written at least '
                     'every %.2f seconds', self._interval)

    def stop(self):
        """Stop the heartbeat checker"""
        if self._timer:
            self._timer.cancel()
            self._timer = None

    def _start_timer(self):
        """Create a new thread timer, destroying the last if it existed."""
        if self._timer:
            del self._timer
        self._timer = threading.Timer(self._interval, self._maybe_send)
        self._timer.daemon = True
        self._timer.start()

    def _maybe_send(self):
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
