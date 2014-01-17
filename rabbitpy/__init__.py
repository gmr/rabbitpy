__version__ = '0.13.0'
version = __version__

DEBUG = False

import logging

try:
    from logging import NullHandler
except ImportError:
    # Python 2.6 does not have a NullHandler
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger('rabbitpy').addHandler(NullHandler())

from rabbitpy.connection import Connection
from rabbitpy.exchange import Exchange
from rabbitpy.message import Message
from rabbitpy.amqp_queue import Queue
from rabbitpy.tx import Tx

from rabbitpy.simple import consume
from rabbitpy.simple import get
from rabbitpy.simple import publish
from rabbitpy.simple import create_queue
from rabbitpy.simple import delete_queue
from rabbitpy.simple import create_direct_exchange
from rabbitpy.simple import create_fanout_exchange
from rabbitpy.simple import create_topic_exchange
from rabbitpy.simple import delete_exchange
