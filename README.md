# rabbitpy

A pure-Python, thread-safe, minimalistic RabbitMQ client library.

rabbitpy provides a straightforward API for working with RabbitMQ. Connections,
channels, queues, exchanges, and messages are plain Python objects. The library
handles the AMQP protocol details so you don't have to.

## Installation

```bash
pip install rabbitpy
```

## Requirements

- Python 3.11+
- RabbitMQ 3.8+

## Quick start

### Publish a message

```python
import rabbitpy

with rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
    with conn.channel() as channel:
        exchange = rabbitpy.Exchange(channel, 'my-exchange')
        exchange.declare()

        queue = rabbitpy.Queue(channel, 'my-queue')
        queue.declare()
        queue.bind(exchange, 'my-routing-key')

        message = rabbitpy.Message(channel, 'Hello, world!')
        message.publish(exchange, 'my-routing-key')
```

### Consume messages

```python
import rabbitpy

with rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
    with conn.channel() as channel:
        for message in rabbitpy.Queue(channel, 'my-queue'):
            print(message.body)
            message.ack()
```

### Get a single message

```python
import rabbitpy

with rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
    with conn.channel() as channel:
        queue = rabbitpy.Queue(channel, 'my-queue')
        message = queue.get()
        if message:
            print(message.body)
            message.ack()
```

### Publish with confirmations

```python
import rabbitpy

with rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
    with conn.channel() as channel:
        channel.enable_publisher_confirms()
        message = rabbitpy.Message(channel, 'Hello, world!')
        if message.publish('', routing_key='my-queue'):
            print('Confirmed by broker')
```

### Transactional publishing

```python
import rabbitpy

with rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
    with conn.channel() as channel:
        tx = rabbitpy.Tx(channel)
        tx.select()
        message = rabbitpy.Message(channel, 'Hello, world!')
        message.publish('', routing_key='my-queue')
        tx.commit()
```

### Simple one-liner API

For scripts and simple use cases, rabbitpy provides module-level functions
that manage the connection and channel for you:

```python
import rabbitpy

# Publish
rabbitpy.publish('amqp://localhost/%2f', routing_key='my-queue', body=b'hello')

# Get
message = rabbitpy.get('amqp://localhost/%2f', queue_name='my-queue')

# Consume
for message in rabbitpy.consume('amqp://localhost/%2f', queue_name='my-queue'):
    print(message.body)
    message.ack()
    break
```

## Connection URL

```
amqp[s]://username:password@host:port/virtual_host[?options]
```

Query string options:

| Option | Description |
|---|---|
| `heartbeat` | Heartbeat interval in seconds (default: 60) |
| `channel_max` | Maximum number of channels |
| `frame_max` | Maximum frame size in bytes |
| `timeout` | Connection timeout in seconds (default: 3) |
| `cacertfile` | Path to CA certificate file |
| `certfile` | Path to client certificate file |
| `keyfile` | Path to client certificate key |
| `verify` | Certificate verification: `ignore`, `optional`, or `required` |

Example: `amqp://guest:guest@localhost:5672/%2f?heartbeat=30&timeout=5`

## Documentation

Full API documentation is available at [rabbitpy.readthedocs.io](https://rabbitpy.readthedocs.io).

## License

BSD 3-Clause. See [LICENSE](LICENSE) for details.
