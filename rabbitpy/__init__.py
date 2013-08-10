__version__ = '0.5.0p1'

from rabbitpy.connection import Connection
from rabbitpy.exchange import Exchange
from rabbitpy.message import Message
from rabbitpy.queue import Queue
from rabbitpy.tx import Tx

from rabbitpy.simple import consumer
from rabbitpy.simple import get
from rabbitpy.simple import publish

import logging

try:
    from logging import NullHandler
except ImportError:
    # Python 2.6 does not have a NullHandler
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger('rabbitpy').addHandler(NullHandler())
