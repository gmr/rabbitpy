# Exceptions

rabbitpy contains two types of exceptions: exceptions specific to rabbitpy
and exceptions raised as a result of a Channel or Connection closure from
RabbitMQ.

## Example

```python
import rabbitpy

try:
    with rabbitpy.Connection() as connection:
        with connection.channel() as channel:
            queue = rabbitpy.Queue(channel, 'exception-test')
            queue.durable = True
            queue.declare()
            queue.durable = False
            queue.declare()  # raises AMQPPreconditionFailed
except rabbitpy.exceptions.AMQPPreconditionFailed:
    print('Queue already exists with different parameters')
```

## rabbitpy Exceptions

::: rabbitpy.exceptions.RabbitpyException

::: rabbitpy.exceptions.ActionException

::: rabbitpy.exceptions.ChannelClosedException

::: rabbitpy.exceptions.ConnectionException

::: rabbitpy.exceptions.ConnectionClosed

::: rabbitpy.exceptions.ConnectionResetException

::: rabbitpy.exceptions.MessageReturnedException

::: rabbitpy.exceptions.NoActiveTransactionError

::: rabbitpy.exceptions.NotConsumingError

::: rabbitpy.exceptions.NotSupportedError

::: rabbitpy.exceptions.ReceivedOnClosedChannelException

::: rabbitpy.exceptions.RemoteCancellationException

::: rabbitpy.exceptions.RemoteClosedChannelException

::: rabbitpy.exceptions.RemoteClosedException

::: rabbitpy.exceptions.TooManyChannelsError

::: rabbitpy.exceptions.UnexpectedResponseError

## AMQP Exceptions

These exceptions are raised when RabbitMQ sends a channel or connection close
with a specific AMQP reply code.

::: rabbitpy.exceptions.AMQPException

::: rabbitpy.exceptions.AMQPAccessRefused

::: rabbitpy.exceptions.AMQPChannelError

::: rabbitpy.exceptions.AMQPCommandInvalid

::: rabbitpy.exceptions.AMQPConnectionForced

::: rabbitpy.exceptions.AMQPContentTooLarge

::: rabbitpy.exceptions.AMQPFrameError

::: rabbitpy.exceptions.AMQPInternalError

::: rabbitpy.exceptions.AMQPInvalidPath

::: rabbitpy.exceptions.AMQPNoConsumers

::: rabbitpy.exceptions.AMQPNoRoute

::: rabbitpy.exceptions.AMQPNotAllowed

::: rabbitpy.exceptions.AMQPNotFound

::: rabbitpy.exceptions.AMQPNotImplemented

::: rabbitpy.exceptions.AMQPPreconditionFailed

::: rabbitpy.exceptions.AMQPResourceError

::: rabbitpy.exceptions.AMQPResourceLocked

::: rabbitpy.exceptions.AMQPSyntaxError

::: rabbitpy.exceptions.AMQPUnexpectedFrame
