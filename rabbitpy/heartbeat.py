"""
The heartbeat checker implements heartbeat check behavior and is non-client
facing.

"""
import logging
import threading
import time

from rabbitpy import exceptions

LOGGER = logging.getLogger(__name__)


class Checker(object):
    """The Checker object implements the logic to ensure that heartbeats have
    been received and are replied to. If no heartbeat or data has been received
    for the specified interval, it will add an exception to the exception
    queue, causing the connection to shutdown.

    :param io: The rabbitpy IO object
    :type io: rabbitpy.io.IO
    :param exception_queue: The exception queue
    :type exception_queue: queue.Queue

    """
    MAX_MISSED_HEARTBEATS = 2

    def __init__(self, io, exception_queue):
        self._exceptions = exception_queue
        self._io = io
        self._interval = 0
        self._last_bytes = 0
        self._last_heartbeat = 0
        self._lock = threading.Lock()
        self._timer = None

    def on_heartbeat(self):
        """Callback invoked when a heartbeat is received"""
        LOGGER.debug('Heartbeat received, updating the last_heartbeat time')
        self._lock.acquire(True)
        self._last_heartbeat = time.time()
        self._lock.release()

    def start(self, interval):
        """Start the heartbeat checker

        :param int interval: How often to expect heartbeats.

        """
        self._interval = interval
        self._start_timer()

    def stop(self):
        """Stop the heartbeat checker"""
        self._interval = 0
        if self._timer:
            self._timer.cancel()
            self._timer = None

    def _check(self):

        # If the byte count has incremented no need to check time
        if self._io.bytes_received > self._last_bytes:
            LOGGER.debug('Data has been received, exiting heartbeat check')
            self._lock.acquire(True)
            self._last_bytes = self._io.bytes_received
            self._lock.release()
            self._start_timer()
            return

        age = time.time() - self._last_heartbeat
        threshold = self._interval * self.MAX_MISSED_HEARTBEATS
        LOGGER.debug('Checking for heartbeat, last: %i sec ago, threshold: %i',
                     age, threshold)
        if age >= threshold:
            LOGGER.error('Have not received a heartbeat in %i seconds', age)
            message = 'No heartbeat in {0} seconds'.format(age)
            self._exceptions.put(exceptions.ConnectionResetException(message))
        else:
            self._start_timer()

    def _start_timer(self):
        """Create and start the timer that will check every N*2 seconds to
        ensure that a heartbeat has been requested.

        """
        if not self._interval:
            return
        LOGGER.debug('Started a heartbeat timer that will fire in %i sec',
                     self._interval)
        self._timer = threading.Timer(self._interval, self._check)
        self._timer.daemon = True
        self._timer.start()
