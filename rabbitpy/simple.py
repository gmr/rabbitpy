"""
Wrapper methods for easy access to common operations, making them both less
complex and less verbose for one off or simple use cases.

"""
import contextlib

from rabbitpy import connection
from rabbitpy import exchange
from rabbitpy import exceptions
from rabbitpy import amqp_queue
from rabbitpy import message


@contextlib.contextmanager
def consumer(uri=None, queue_name=None):
    """Create a queue consumer, returning a :py:class:`rabbitpy.queue.Consumer`
    generator class that you can retrieve messages from using
    :py:class:`rabbitpy.queue.Consumer.next_message`

    Invoke directly as rabbitpy.consumer()

    :rtype: :py:class:`rabbitpy.queue.Consumer`
    :raises: :py:class:`rabbitpy.exceptions.EmptyQueueNameError`

    """
    if not queue_name:
        raise exceptions.EmptyQueueNameError()

    with connection.Connection(uri) as conn:
        with conn.channel() as channel:
            q = amqp_queue.Queue(channel, queue_name)
            with q.consumer() as consumer:
                yield consumer


def get(uri=None, queue_name=None):
    """Get a message from RabbitMQ, auto-acknowledging with RabbitMQ if one
    is returned.

    Invoke directly as rabbitpy.get()


    :param str uri: AMQP URI to connect to
    :param str queue_name: The queue name to get the message from
    :rtype: py:class:`rabbitpy.message.Message` or None
    :raises: :py:class:`rabbitpy.exceptions.EmptyQueueNameError`

    """
    if not queue_name:
        raise exceptions.EmptyQueueNameError()

    with connection.Connection(uri) as conn:
        with conn.channel() as channel:
            q = amqp_queue.Queue(channel, queue_name)
            return q.get(False)


def publish(uri=None, exchange=None, routing_key=None,
            body=None, properties=None, confirm=False):
    """Publish a message to RabbitMQ. This should only be used for one-off
    publishing, as you will suffer a performance penalty if you use it
    repeatedly instead creating a connection and channel and publishing on that

    :param str uri: AMQP URI to connect to
    :param str exchange: The exchange to publish to
    :param str routing_key: The routing_key to publish with
    :param str or unicode or bytes or dict or list: The message body
    :param dict properties: Dict representation of Basic.Properties
    :param bool confirm: Confirm this delivery with Publisher Confirms
    :rtype: bool or None
    :raises: :py:class:`rabbitpy.exceptions.EmptyExchangeNameError`

    """
    if not exchange:
        raise exceptions.EmptyExchangeNameError()

    with connection.Connection(uri) as conn:
        with conn.channel() as channel:
            msg = message.Message(channel, body or '', properties or dict())
            if confirm:
                channel.enable_publisher_confirms()
                return msg.publish(exchange, routing_key or '')
            else:
                msg.publish(exchange, routing_key or '')


def create_queue(uri=None, queue_name=None, durable=True, auto_delete=False):
    """Create a queue with RabbitMQ. This should only be used for one-off
    operations.

    :param str uri: AMQP URI to connect to
    :param str queue_name: The queue name to create
    :param durable: Indicates if the queue should survive a RabbitMQ is restart
    :type durable: bool
    :param bool auto_delete: Automatically delete when all consumers disconnect
    :raises: :py:class:`rabbitpy.exceptions.EmptyQueueNameError`
    :raises: :py:class:`rabbitpy.RemoteClosedException`

    """
    if not queue_name:
        raise exceptions.EmptyQueueNameError()

    with connection.Connection(uri) as conn:
        with conn.channel() as channel:
            obj = amqp_queue.Queue(channel, queue_name, durable=durable,
                                   auto_delete=auto_delete)
            obj.declare()


def delete_queue(uri=None, queue_name=None):
    """Delete a queue from RabbitMQ. This should only be used for one-off
    operations.

    :param str uri: AMQP URI to connect to
    :param str queue_name: The queue name to delete
    :rtype: bool
    :raises: :py:class:`rabbitpy.exceptions.EmptyQueueNameError`
    :raises: :py:class:`rabbitpy.RemoteClosedException`

    """
    if not queue_name:
        raise exceptions.EmptyQueueNameError()

    with connection.Connection(uri) as conn:
        with conn.channel() as channel:
            queue = amqp_queue.Queue(channel, queue_name)
            queue.delete()


def create_direct_exchange(uri=None, exchange_name=None, durable=True):
    """Create a direct exchange with RabbitMQ. This should only be used for
    one-off operations.

    :param str uri: AMQP URI to connect to
    :param str exchange_name: The exchange name to create
    :param bool durable: Exchange should survive server restarts
    :raises: :py:class:`rabbitpy.exceptions.EmptyExchangeNameError`
    :raises: :py:class:`rabbitpy.RemoteClosedException`

    """
    if not exchange_name:
        raise exceptions.EmptyExchangeNameError()

    with connection.Connection(uri) as conn:
        with conn.channel() as channel:
            obj = exchange.DirectExchange(channel, exchange_name,
                                          durable=durable)
            obj.declare()

def create_fanout_exchange(uri=None, exchange_name=None, durable=True):
    """Create a fanout exchange with RabbitMQ. This should only be used for
    one-off operations.

    :param str uri: AMQP URI to connect to
    :param str exchange_name: The exchange name to create
    :param bool durable: Exchange should survive server restarts
    :raises: :py:class:`rabbitpy.exceptions.EmptyExchangeNameError`
    :raises: :py:class:`rabbitpy.RemoteClosedException`

    """
    if not exchange_name:
        raise exceptions.EmptyExchangeNameError()

    with connection.Connection(uri) as conn:
        with conn.channel() as channel:
            obj = exchange.FanoutExchange(channel, exchange_name,
                                          durable=durable)
            obj.declare()


def create_topic_exchange(uri=None, exchange_name=None, durable=True):
    """Create an exchange from RabbitMQ. This should only be used for one-off
    operations.

    :param str uri: AMQP URI to connect to
    :param str exchange_name: The exchange name to create
    :param bool durable: Exchange should survive server restarts
    :raises: :py:class:`rabbitpy.exceptions.EmptyExchangeNameError`
    :raises: :py:class:`rabbitpy.RemoteClosedException`

    """
    if not exchange_name:
        raise exceptions.EmptyExchangeNameError()

    with connection.Connection(uri) as conn:
        with conn.channel() as channel:
            obj = exchange.TopicExchange(channel, exchange_name,
                                         durable=durable)
            obj.declare()


def delete_exchange(uri=None, exchange_name=None):
    """Delete an exchange from RabbitMQ. This should only be used for one-off
    operations.

    :param str uri: AMQP URI to connect to
    :param str exchange_name: The exchange name to delete
    :raises: :py:class:`rabbitpy.exceptions.EmptyExchangeNameError`
    :raises: :py:class:`rabbitpy.RemoteClosedException`

    """
    if not exchange_name:
        raise exceptions.EmptyExchangeNameError()

    with connection.Connection(uri) as conn:
        with conn.channel() as channel:
            obj = exchange.Exchange(channel, exchange_name)
            obj.delete()
