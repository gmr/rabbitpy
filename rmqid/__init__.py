__version__ = '0.4.0'

from rmqid.connection import Connection
from rmqid.exchange import Exchange
from rmqid.message import Message
from rmqid.queue import Queue
from rmqid.tx import Tx

from rmqid.simple import consumer
from rmqid.simple import get
from rmqid.simple import publish

import logging

try:
    from logging import NullHandler
except ImportError:
    # Python 2.6 does not have a NullHandler
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger('rmqid').addHandler(NullHandler())
