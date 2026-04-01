"""
rabbitpy, a pythonic RabbitMQ client

"""

__version__ = '3.0.0'

import logging

from rabbitpy.amqp import AMQP
from rabbitpy.amqp_queue import Queue
from rabbitpy.channel import Channel
from rabbitpy.connection import Connection
from rabbitpy.exchange import (
    DirectExchange,
    Exchange,
    FanoutExchange,
    HeadersExchange,
    TopicExchange,
)
from rabbitpy.message import Message
from rabbitpy.simple import (
    SimpleChannel,
    consume,
    create_direct_exchange,
    create_fanout_exchange,
    create_headers_exchange,
    create_queue,
    create_topic_exchange,
    delete_exchange,
    delete_queue,
    get,
    publish,
)
from rabbitpy.tx import Tx

logging.getLogger('rabbitpy').addHandler(logging.NullHandler())

__all__ = [
    'AMQP',
    'Channel',
    'Connection',
    'DirectExchange',
    'Exchange',
    'FanoutExchange',
    'HeadersExchange',
    'Message',
    'Queue',
    'SimpleChannel',
    'TopicExchange',
    'Tx',
    '__version__',
    'consume',
    'create_direct_exchange',
    'create_fanout_exchange',
    'create_headers_exchange',
    'create_queue',
    'create_topic_exchange',
    'delete_exchange',
    'delete_queue',
    'get',
    'publish',
]
