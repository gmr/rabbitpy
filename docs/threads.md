# Multi-threaded Use

To ensure that the network communication module at the core of rabbitpy is
thread-safe, the `rabbitpy.io.IO` class is a daemonic Python thread that uses
a combination of `threading.Event`, `queue.Queue`, and a local cross-platform
implementation of a read-write socket pair.

While ensuring that the core socket I/O and dispatching of AMQP frames across
threads is safe, it does not protect against cross-thread channel utilization.

Due to the way channel events are managed, **restrict each channel to a single
thread**. Sharing channels across threads can create issues with channel state
in the AMQP protocol.

## Example

The following example uses the main thread to connect to RabbitMQ and then
spawns separate threads for publishing and consuming:

```python
import rabbitpy
import threading

EXCHANGE = 'threading_example'
QUEUE = 'threading_queue'
ROUTING_KEY = 'test'
MESSAGE_COUNT = 100


def consumer(connection):
    """Consume MESSAGE_COUNT messages then exit."""
    received = 0
    with connection.channel() as channel:
        for message in rabbitpy.Queue(channel, QUEUE):
            print(message.body)
            message.ack()
            received += 1
            if received == MESSAGE_COUNT:
                break


def publisher(connection):
    """Publish MESSAGE_COUNT messages."""
    with connection.channel() as channel:
        for index in range(MESSAGE_COUNT):
            message = rabbitpy.Message(channel, f'Message #{index}')
            message.publish(EXCHANGE, ROUTING_KEY)


with rabbitpy.Connection() as connection:
    with connection.channel() as channel:
        exchange = rabbitpy.Exchange(channel, EXCHANGE)
        exchange.declare()

        queue = rabbitpy.Queue(channel, QUEUE)
        queue.declare()
        queue.bind(EXCHANGE, ROUTING_KEY)

    kwargs = {'connection': connection}

    consumer_thread = threading.Thread(target=consumer, kwargs=kwargs)
    consumer_thread.start()

    publisher_thread = threading.Thread(target=publisher, kwargs=kwargs)
    publisher_thread.start()

    consumer_thread.join()
```
