"""
Wrapper for easy access to simple operations, making them simpler

"""
from rmqid import connection
from rmqid import queue
from rmqid import exchange
from rmqid import message

__since__ = '2013-03-27'


def publish(uri, exchange, routing_key, body, properties, confirm):
    """Publish a message to RabbitMQ. This should only be used for one-off
    publishing, as you will suffer a performance penality if you use it
    repeatedly instead creating a connection and channel and publishing on that


    """
    with connection.Connection(uri) as conn:
        with conn.channel() as channel:
            msg = message.Message(channel, body, properties)
            if confirm:
                channel.enable_publisher_confirms()
                return msg.publish(exchange, routing_key)
            else:
                msg.publish(exchange, routing_key)

