# rabbitpy

rabbitpy is a pure python, thread-safe, and pythonic BSD-licensed AMQP/RabbitMQ
client library. It aims to provide a simple and easy to use API for interfacing
with RabbitMQ, minimizing the programming overhead often found in other libraries.

## Installation

```bash
pip install rabbitpy
```

## Quick Start

### Connect and publish a message

```python
import rabbitpy

with rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
    with conn.channel() as channel:
        message = rabbitpy.Message(channel, 'Hello, RabbitMQ!')
        message.publish('my-exchange', 'routing-key')
```

### Consume messages

```python
import rabbitpy

with rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
    with conn.channel() as channel:
        queue = rabbitpy.Queue(channel, 'my-queue')
        for message in queue:
            print(message.body)
            message.ack()
```

## Links

- [GitHub](https://github.com/gmr/rabbitpy)
- [PyPI](https://pypi.org/project/rabbitpy/)
- [Issue Tracker](https://github.com/gmr/rabbitpy/issues)
