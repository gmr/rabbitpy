import logging
import queue

LOGGER = logging.getLogger(__name__)


class Channel0:
    def __init__(self):
        self.pending_frames = queue.Queue()
