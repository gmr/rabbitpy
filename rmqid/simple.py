"""
Wrapper methods for easy access to common operations, making them both less
complex and less verbose for one off or simple use cases.

"""
from rmqid import connection
import contextlib
from rmqid import exceptions
from rmqid import queue
from rmqid import message


@contextlib.contextmanager
def consumer(uri=None, queue_name=None):
    """Create a queue consumer, returning a :py:class:`rmqid.queue.Consumer`
    generator class that you can retrieve messages from using
    :py:class:`rmqid.queue.Consumer.next_message`

    Invoke directly as rmqid.consumer()

    :rtype: :py:class:`rmqid.queue.Consumer`
    :raises: :py:class:`rmqid.exceptions.EmptyQueueNameError`

    """
    if not queue_name:
        raise exceptions.EmptyQueueNameError()

    with connection.Connection(uri) as conn:
        with conn.channel() as channel:
            q = queue.Queue(channel, queue_name)
            with q.consumer() as consumer:
                yield consumer


def get(uri=None, queue_name=None):
    """Get a message from RabbitMQ, auto-acknowledging with RabbitMQ if one
    is returned.

    Invoke directly as rmqid.get()


    :param str uri: AMQP URI to connect to
    :param str queue_name: The queue name to get the message from
    :rtype: py:class:`rmqid.message.Message` or None
    :raises: :py:class:`rmqid.exceptions.EmptyQueueNameError`

    """
    if not queue_name:
        raise exceptions.EmptyQueueNameError()

    with connection.Connection(uri) as conn:
        with conn.channel() as channel:
            q = queue.Queue(channel, queue_name)
            return q.get(False)


def publish(uri=None, exchange=None, routing_key=None,
            body=None, properties=None, confirm=False):
    """Publish a message to RabbitMQ. This should only be used for one-off
    publishing, as you will suffer a performance penality if you use it
    repeatedly instead creating a connection and channel and publishing on that

    :param str uri: AMQP URI to connect to
    :param str exchange: The exchange to publish to
    :param str routing_key: The routing_key to publish with
    :param str or unicode or bytes or dict or list: The message body
    :param dict properties: Dict representation of Basic.Properties
    :param bool confirm: Confirm this delivery with Publisher Confirms
    :rtype: bool or None
    :raises: :py:class:`rmqid.exceptions.EmptyExchangeNameError`


    """
    if not exchange:
        raise exceptions.EmptyExceptionNameError()

    with connection.Connection(uri) as conn:
        with conn.channel() as channel:
            msg = message.Message(channel, body or '', properties or dict())
            if confirm:
                channel.enable_publisher_confirms()
                return msg.publish(exchange, routing_key or '')
            else:
                msg.publish(exchange, routing_key or '')

