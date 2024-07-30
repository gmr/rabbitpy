"""
Wrapper methods for easy access to common operations, making them both less
complex and less verbose for one-off or simple use cases.

"""
import types
import typing

from rabbitpy import amqp_queue, channel, connection, exchange, message


class SimpleChannel:
    """The rabbitpy.simple.Channel class creates a context manager
    implementation for use on a single channel where the connection is
    automatically created and managed for you.

    Example:

    .. code:: python

        import rabbitpy

        with rabbitpy.SimpleChannel('amqp://localhost/%2f') as channel:
            queue = rabbitpy.Queue(channel, 'my-queue')

    :param str uri: The AMQP URI to connect with. For URI options, see the
        :class:`~rabbitpy.connection.Connection` class documentation.

    """

    def __init__(self, uri: str):
        self.connection = None
        self.channel = None
        self.uri = uri

    def __enter__(self) -> channel.Channel:
        self.connection = connection.Connection(self.uri)
        self.channel = self.connection.channel()
        return self.channel

    def __exit__(self,
                 exc_type: typing.Optional[typing.Type[BaseException]],
                 exc_val: typing.Optional[BaseException],
                 unused_exc_tb: typing.Optional[types.TracebackType]):
        if not self.channel.closed:
            self.channel.close()
        if not self.connection.closed:
            self.connection.close()
        if exc_type and exc_val:
            raise


def consume(uri: typing.Optional[str] = None,
            queue_name: typing.Optional[str] = None,
            no_ack: typing.Optional[bool] = False,
            prefetch: typing.Optional[int] = None,
            priority: typing.Optional[int] = None) \
        -> typing.Generator[message.Message, None, None]:
    """Consume messages from the queue as a generator:

    .. code:: python

        for message in rabbitpy.consume('amqp://localhost/%2F', 'my-queue'):
            message.ack()

    :param uri: AMQP connection URI
    :param queue_name: The name of the queue to consume from
    :param no_ack: Do not require acknowledgements
    :param prefetch: Set a prefetch count for the channel
    :param priority: Set the consumer priority
    :raises: py:class:`ValueError`

    """
    _validate_name(queue_name, 'queue')
    with SimpleChannel(uri) as chan:
        queue = amqp_queue.Queue(chan, queue_name)
        for msg in queue.consume(no_ack, prefetch, priority):
            yield msg


def get(uri: typing.Optional[str] = None,
        queue_name: typing.Optional[str] = None) \
        -> typing.Union[message.Message, None]:
    """Get a message from RabbitMQ, auto-acknowledging with RabbitMQ if one
    is returned.

    Invoke directly as ``rabbitpy.get()``

    :param uri: AMQP URI to connect to
    :param queue_name: The queue name to get the message from
    :raises: py:class:`ValueError`

    """
    _validate_name(queue_name, 'queue')
    with SimpleChannel(uri) as chan:
        queue = amqp_queue.Queue(chan, queue_name)
        return queue.get(False)


def publish(uri: typing.Optional[str] = None,
            exchange_name: typing.Optional[str] = None,
            routing_key: typing.Optional[str] = None,
            body: typing.Optional[bytes, str] = None,
            properties: typing.Optional[dict] = None,
            confirm: bool = False) -> typing.Union[bool, None]:
    """Publish a message to RabbitMQ. This should only be used for one-off
    publishing, as you will suffer a performance penalty if you use it
    repeatedly instead creating a connection and channel and publishing on that

    :param uri: AMQP URI to connect to
    :param exchange_name: The exchange to publish to
    :param routing_key: The routing_key to publish with
    :param body: The message body
    :param properties: Dict representation of Basic.Properties
    :param confirm: Confirm this delivery with Publisher Confirms

    """
    if exchange_name is None:
        exchange_name = ''

    with SimpleChannel(uri) as chan:
        msg = message.Message(chan, body or '', properties or dict())
        if confirm:
            chan.enable_publisher_confirms()
            return msg.publish(
                exchange_name, routing_key or '', mandatory=True)
        else:
            msg.publish(exchange_name, routing_key or '')


def create_queue(uri: typing.Optional[str] = None,
                 queue_name: str = '',
                 durable: bool = True,
                 auto_delete: bool = False,
                 max_length: typing.Optional[int] = None,
                 message_ttl: typing.Optional[int] = None,
                 expires: typing.Optional[int] = None,
                 dead_letter_exchange: typing.Optional[str] = None,
                 dead_letter_routing_key: typing.Optional[str] = None,
                 arguments: typing.Optional[dict] = None):
    """Create a queue with RabbitMQ. This should only be used for one-off
    operations. If a queue name is omitted, the name will be automatically
    generated by RabbitMQ.

    :param uri: AMQP URI to connect to
    :param queue_name: The queue name to create
    :param durable: Indicates if the queue should survive a RabbitMQ is restart
    :param auto_delete: Automatically delete when all consumers disconnect
    :param max_length: Maximum queue length
    :param message_ttl: Time-to-live of a message in milliseconds
    :param expires: Milliseconds until a queue is removed after becoming idle
    :param dead_letter_exchange: Dead letter exchange for rejected messages
    :param dead_letter_routing_key: Routing key for dead lettered messages
    :param arguments: Custom arguments for the queue
    :raises: :py:class:`ValueError`
    :raises: :py:class:`rabbitpy.RemoteClosedException`

    """
    _validate_name(queue_name, 'queue')
    with SimpleChannel(uri) as c:
        amqp_queue.Queue(c,
                         queue_name,
                         durable=durable,
                         auto_delete=auto_delete,
                         max_length=max_length,
                         message_ttl=message_ttl,
                         expires=expires,
                         dead_letter_exchange=dead_letter_exchange,
                         dead_letter_routing_key=dead_letter_routing_key,
                         arguments=arguments).declare()


def delete_queue(uri: typing.Optional[str] = None,
                 queue_name: str = '') -> None:
    """Delete a queue from RabbitMQ. This should only be used for one-off
    operations.

    :param uri: AMQP URI to connect to
    :param queue_name: The queue name to delete
    :raises: :py:class:`ValueError`
    :raises: :py:class:`rabbitpy.RemoteClosedException`

    """
    _validate_name(queue_name, 'queue')
    with SimpleChannel(uri) as c:
        amqp_queue.Queue(c, queue_name).delete()


def create_direct_exchange(uri: typing.Optional[str] = None,
                           exchange_name: typing.Optional[str] = None,
                           durable: bool = True) -> None:
    """Create a direct exchange with RabbitMQ. This should only be used for
    one-off operations.

    :param uri: AMQP URI to connect to
    :param exchange_name: The exchange name to create
    :param durable: Exchange should survive server restarts
    :raises: :py:class:`ValueError`
    :raises: :py:class:`rabbitpy.RemoteClosedException`

    """
    _create_exchange(uri, exchange_name, exchange.DirectExchange, durable)


def create_fanout_exchange(uri: typing.Optional[str] = None,
                           exchange_name: typing.Optional[str] = None,
                           durable: bool = True) -> None:
    """Create a fanout exchange with RabbitMQ. This should only be used for
    one-off operations.

    :param uri: AMQP URI to connect to
    :param exchange_name: The exchange name to create
    :param durable: Exchange should survive server restarts
    :raises: :py:class:`ValueError`
    :raises: :py:class:`rabbitpy.RemoteClosedException`

    """
    _create_exchange(uri, exchange_name, exchange.FanoutExchange, durable)


def create_headers_exchange(uri: typing.Optional[str] = None,
                            exchange_name: typing.Optional[str] = None,
                            durable: bool = True) -> None:
    """Create a headers exchange with RabbitMQ. This should only be used for
    one-off operations.

    :param uri: AMQP URI to connect to
    :param exchange_name: The exchange name to create
    :param durable: Exchange should survive server restarts
    :raises: :py:class:`ValueError`
    :raises: :py:class:`rabbitpy.RemoteClosedException`

    """
    _create_exchange(uri, exchange_name, exchange.HeadersExchange, durable)


def create_topic_exchange(uri: typing.Optional[str] = None,
                          exchange_name: typing.Optional[str] = None,
                          durable: bool = True) -> None:
    """Create an exchange from RabbitMQ. This should only be used for one-off
    operations.

    :param uri: AMQP URI to connect to
    :param exchange_name: The exchange name to create
    :param durable: Exchange should survive server restarts
    :raises: :py:class:`ValueError`
    :raises: :py:class:`rabbitpy.RemoteClosedException`

    """
    _create_exchange(uri, exchange_name, exchange.TopicExchange, durable)


def delete_exchange(uri: typing.Optional[str] = None,
                    exchange_name: typing.Optional[str] = None) -> None:
    """Delete an exchange from RabbitMQ. This should only be used for one-off
    operations.

    :param uri: AMQP URI to connect to
    :param exchange_name: The exchange name to delete
    :raises: :py:class:`ValueError`
    :raises: :py:class:`rabbitpy.RemoteClosedException`

    """
    _validate_name(exchange_name, 'exchange')
    with SimpleChannel(uri) as c:
        exchange.Exchange(c, exchange_name).delete()


def _create_exchange(uri: typing.Optional[str],
                     exchange_name: typing.Optional[str],
                     exchange_class: typing.Callable,
                     durable: bool) -> None:
    """Create an exchange from RabbitMQ. This should only be used for one-off
    operations.

    :param uri: AMQP URI to connect to
    :param exchange_name: The exchange name to create
    :param exchange_class: The exchange class to use
    :param durable: Exchange should survive server restarts
    :raises: :py:class:`ValueError`
    :raises: :py:class:`rabbitpy.RemoteClosedException`

    """
    _validate_name(exchange_name, 'exchange')
    with SimpleChannel(uri) as c:
        exchange_class(c, exchange_name, durable=durable).declare()


def _validate_name(value: str, obj_type: str) -> None:
    """Validate the specified name is set.

    :param value: The value to validate
    :param obj_type: The object type for the error message if needed
    :raises: ValueError

    """
    if not value:
        raise ValueError(f'You must specify the {obj_type} name')
