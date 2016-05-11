"""
Exceptions that may be raised by rabbitpy during use
----------------------------------------------------

"""


class RabbitpyException(Exception):
    """Base exception of all rabbitpy exceptions."""
    pass


class AMQPException(RabbitpyException):
    """Base exception of all AMQP exceptions."""
    pass


class ActionException(RabbitpyException):
    """Raised when an action is taken on a Rabbitpy object that is not
    supported due to the state of the object. An example would be trying to
    ack a Message object when the message object was locally created and not
    sent by RabbitMQ via an AMQP Basic.Get or Basic.Consume.

    """
    def __str__(self):
        return self.args[0]


class ChannelClosedException(RabbitpyException):
    """Raised when an action is attempted on a channel that is closed."""
    def __str__(self):
        return 'Can not perform RPC requests on a closed channel, you must ' \
               'create a new channel'


class ConnectionException(RabbitpyException):
    """Raised when Rabbitpy can not connect to the specified server and if
    a connection fails and the RabbitMQ version does not support the
    authentication_failure_close feature added in RabbitMQ 3.2.

    """
    def __str__(self):
        return 'Unable to connect to the remote server {0}'.format(self.args)


class ConnectionResetException(RabbitpyException):
    """Raised if the socket level connection was reset. This can happen due
    to the loss of network connection or socket timeout, or more than 2
    missed heartbeat intervals if heartbeats are enabled.

    """
    def __str__(self):
        return 'Connection was reset at socket level'


class RemoteCancellationException(RabbitpyException):
    """Raised if RabbitMQ cancels an active consumer"""
    def __str__(self):
        return 'Remote server cancelled the active consumer'


class RemoteClosedChannelException(RabbitpyException):
    """Raised if RabbitMQ closes the channel and the reply_code in the
    Channel.Close RPC request does not have a mapped exception in Rabbitpy.

    """
    def __str__(self):
        return 'Channel {0} was closed by the remote server ' \
               '({1}): {2}'.format(*self.args)


class RemoteClosedException(RabbitpyException):
    """Raised if RabbitMQ closes the connection and the reply_code in the
    Connection.Close RPC request does not have a mapped exception in Rabbitpy.

    """
    def __str__(self):
        return 'Connection was closed by the remote server ' \
               '({0}): {1}'.format(*self.args)


class MessageReturnedException(RabbitpyException):
    """Raised if the RabbitMQ sends a message back to a publisher via
    the Basic.Return RPC call.

    """
    def __str__(self):
        return 'Message was returned by RabbitMQ: ({0}) ' \
               'for exchange {1}'.format(*self.args)


class NoActiveTransactionError(RabbitpyException):
    """Raised when a transaction method is issued but the transaction has not
    been initiated.

    """
    def __str__(self):
        return 'No active transaction for the request, channel closed'


class NotConsumingError(RabbitpyException):
    """Raised Queue.cancel_consumer() is invoked but the queue is not
    actively consuming.

    """
    def __str__(self):
        return 'No active consumer to cancel'


class NotSupportedError(RabbitpyException):
    """Raised when a feature is requested that is not supported by the RabbitMQ
    server.

    """
    def __str__(self):
        return 'The selected feature "{0}" is not supported'.format(self.args)


class TooManyChannelsError(RabbitpyException):
    """Raised if an application attempts to create a channel, exceeding the
    maximum number of channels (MAXINT or 2,147,483,647) available for a
    single connection. Note that each time a channel object is created, it will
    take a new channel id. If you create and destroy 2,147,483,648 channels,
    this exception will be raised.

    """
    def __str__(self):
        return 'The maximum amount of negotiated channels has been reached'


class UnexpectedResponseError(RabbitpyException):
    """Raised when an RPC call is made to RabbitMQ but the response it sent
    back is not recognized.

    """
    def __str__(self):
        return 'Received an expected response, expected {0}, ' \
               'received {1}'.format(*self.args)


# AMQP Exceptions


class AMQPContentTooLarge(AMQPException):
    """
    The client attempted to transfer content larger than the server could
    accept at the present time. The client may retry at a later time.

    """
    pass


class AMQPNoRoute(AMQPException):
    """
    Undocumented AMQP Soft Error

    """
    pass


class AMQPNoConsumers(AMQPException):
    """
    When the exchange cannot deliver to a consumer when the immediate flag is
    set. As a result of pending data on the queue or the absence of any
    consumers of the queue.

    """
    pass


class AMQPAccessRefused(AMQPException):
    """
    The client attempted to work with a server entity to which it has no access
    due to security settings.

    """
    pass


class AMQPNotFound(AMQPException):
    """
    The client attempted to work with a server entity that does not exist.

    """
    pass


class AMQPResourceLocked(AMQPException):
    """
    The client attempted to work with a server entity to which it has no access
    because another client is working with it.

    """
    pass


class AMQPPreconditionFailed(AMQPException):
    """
    The client requested a method that was not allowed because some
    precondition failed.

    """
    pass


class AMQPConnectionForced(AMQPException):
    """
    An operator intervened to close the connection for some reason. The client
    may retry at some later date.

    """
    pass


class AMQPInvalidPath(AMQPException):
    """
    The client tried to work with an unknown virtual host.

    """
    pass


class AMQPFrameError(AMQPException):
    """
    The sender sent a malformed frame that the recipient could not decode. This
    strongly implies a programming error in the sending peer.

    """
    pass


class AMQPSyntaxError(AMQPException):
    """
    The sender sent a frame that contained illegal values for one or more
    fields. This strongly implies a programming error in the sending peer.

    """
    pass


class AMQPCommandInvalid(AMQPException):
    """
    The client sent an invalid sequence of frames, attempting to perform an
    operation that was considered invalid by the server. This usually implies a
    programming error in the client.

    """
    pass


class AMQPChannelError(AMQPException):
    """
    The client attempted to work with a channel that had not been correctly
    opened. This most likely indicates a fault in the client layer.

    """
    pass


class AMQPUnexpectedFrame(AMQPException):
    """
    The peer sent a frame that was not expected, usually in the context of a
    content header and body.  This strongly indicates a fault in the peer's
    content processing.

    """
    pass


class AMQPResourceError(AMQPException):
    """
    The server could not complete the method because it lacked sufficient
    resources. This may be due to the client creating too many of some type of
    entity.

    """
    pass


class AMQPNotAllowed(AMQPException):
    """
    The client tried to work with some entity in a manner that is prohibited by
    the server, due to security settings or by some other criteria.

    """
    pass


class AMQPNotImplemented(AMQPException):
    """
    The client tried to use functionality that is not implemented in the
    server.

    """
    pass


class AMQPInternalError(AMQPException):
    """
    The server could not complete the method because of an internal error. The
    server may require intervention by an operator in order to resume normal
    operations.

    """
    pass


AMQP = {311: AMQPContentTooLarge,
        312: AMQPNoRoute,
        313: AMQPNoConsumers,
        320: AMQPConnectionForced,
        402: AMQPInvalidPath,
        403: AMQPAccessRefused,
        404: AMQPNotFound,
        405: AMQPResourceLocked,
        406: AMQPPreconditionFailed,
        501: AMQPFrameError,
        502: AMQPSyntaxError,
        503: AMQPCommandInvalid,
        504: AMQPChannelError,
        505: AMQPUnexpectedFrame,
        506: AMQPResourceError,
        530: AMQPNotAllowed,
        540: AMQPNotImplemented,
        541: AMQPInternalError}
