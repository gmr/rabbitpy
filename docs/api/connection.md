# Connection

rabbitpy Connection objects are used to connect to RabbitMQ. They provide a
thread-safe connection to RabbitMQ that is used to authenticate and send all
channel based RPC commands over. Connections use
[AMQP URI syntax](http://www.rabbitmq.com/uri-spec.html) for specifying all
of the connection information, including any connection negotiation options,
such as the heartbeat interval.

## Usage

A `Connection` can be used as a normal Python object:

```python
conn = rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2F')
conn.close()
```

Or as a Python context manager:

```python
with rabbitpy.Connection() as conn:
    with conn.channel() as channel:
        # use channel
        pass
```

When used as a context manager, the connection is automatically closed when
the block exits.

If RabbitMQ remotely closes the connection, rabbitpy will raise the appropriate
exception (see [Exceptions](exceptions.md)).

If heartbeats are enabled (default: 5 minutes) and RabbitMQ does not send a
heartbeat in >= 2 heartbeat intervals, a `ConnectionResetException` will be raised.

## Connection URL Parameters

The connection URL follows standard AMQP URI syntax:

```
amqp[s]://[user:password@]host[:port]/[vhost][?key=value...]
```

Common query parameters:

| Parameter   | Default | Description                       |
| ----------- | ------- | --------------------------------- |
| `heartbeat` | `300`   | Heartbeat interval in seconds     |
| `channel_max` | `0`   | Maximum number of channels        |
| `frame_max` | `131072` | Maximum frame size in bytes      |

## API

::: rabbitpy.connection.Connection
