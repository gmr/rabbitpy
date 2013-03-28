"""
Wrapper for easy access to simple operations, making them simpler

"""
from rmqid import connection
from rmqid import queue
from rmqid import exchange
from rmqid import message

__since__ = '2013-03-27'


def get(uri, queue_name):
    """Get a message from RabbitMQ, auto-acknowledging with RabbitMQ if one
    is returned.

    :param str uri: AMQP URI to connect to
    :param str queue_name: The queue name to get the message from
    :rtype: py:class:`rmqid.message.Message` or None

    """
    with connection.Connection(uri) as conn:
        with conn.channel() as channel:
            q = queue.Queue(channel, queue_name)
            return q.get(False)


def publish(uri, exchange, routing_key=None,
            body=None, properties=None, confirm=False):
    """Publish a message to RabbitMQ. This should only be used for one-off
    publishing, as you will suffer a performance penality if you use it
    repeatedly instead creating a connection and channel and publishing on that


    """
    with connection.Connection(uri) as conn:
        with conn.channel() as channel:
            msg = message.Message(channel, body or '', properties or dict())
            if confirm:
                channel.enable_publisher_confirms()
                return msg.publish(exchange, routing_key or '')
            else:
                msg.publish(exchange, routing_key or '')

