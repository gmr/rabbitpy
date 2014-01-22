"""
rabbitpy Specific Exceptions

"""
from pamqp import specification


class ActionException(Exception):
    def __repr__(self):
        return self.args[0]


class ChannelClosedException(Exception):
    def __repr__(self):
        return 'Can not perform RPC requests on a closed channel, you must ' \
               'create a new channel'


class ConnectionBlockedWarning(Warning):
    def __repr__(self):
        return 'Will not write to a connection that RabbitMQ is throttling'


class ConnectionException(Exception):
    def __repr__(self):
        return 'Unable to connect to the remote server %r' % self.args


class ConnectionResetException(Exception):
    def __repr__(self):
        return 'Connection was reset at socket level'


class EmptyExchangeNameError(Exception):
    def __repr__(self):
        return 'You must specify an Exchange name'


class EmptyQueueNameError(Exception):
    def __repr__(self):
        return 'You must specify a Queue name'


class RemoteClosedChannelException(Exception):
    def __repr__(self):
        return 'Channel %i was closed by the remote server (%i): %s' % \
               (self.args[0], self.args[1], self.args[2])


class RemoteClosedException(Exception):
    def __repr__(self):
        return 'Connection was closed by the remote server (%i): %s' % \
               (self.args[0], self.args[1])


class MessageReturnedException(Exception):
    def __repr__(self):
        return 'Message %s was returned by RabbitMQ: (%s) %s' % \
               (self.args[0], self.args[1], self.args[2])


class NoActiveTransactionError(Exception):
    def __repr__(self):
        return 'No active transaction for the request, channel closed'


class TooManyChannelsError(Exception):
    def __repr__(self):
        return 'The maximum amount of negotiated channels has been reached'


class UnexpectedResponseError(Exception):
    def __repr__(self):
        return 'Received an expected response, expected %s, received %s' % \
               (self.args[0], self.args[1])


# AMQP Exceptions


class AMQPContentTooLarge(Warning):
    """
    The client attempted to transfer content larger than the server could
    accept at the present time. The client may retry at a later time.

    """
    name = 'CONTENT-TOO-LARGE'
    value = 311


class AMQPNoRoute(Warning):
    """
    Undocumented AMQP Soft Error

    """
    name = 'NO-ROUTE'
    value = 312


class AMQPNoConsumers(Warning):
    """
    When the exchange cannot deliver to a consumer when the immediate flag is
    set. As a result of pending data on the queue or the absence of any
    consumers of the queue.

    """
    name = 'NO-CONSUMERS'
    value = 313


class AMQPAccessRefused(Warning):
    """
    The client attempted to work with a server entity to which it has no access
    due to security settings.

    """
    name = 'ACCESS-REFUSED'
    value = 403


class AMQPNotFound(Warning):
    """
    The client attempted to work with a server entity that does not exist.

    """
    name = 'NOT-FOUND'
    value = 404


class AMQPResourceLocked(Warning):
    """
    The client attempted to work with a server entity to which it has no access
    because another client is working with it.

    """
    name = 'RESOURCE-LOCKED'
    value = 405


class AMQPPreconditionFailed(Warning):
    """
    The client requested a method that was not allowed because some
    precondition failed.

    """
    name = 'PRECONDITION-FAILED'
    value = 406


class AMQPConnectionForced(Exception):
    """
    An operator intervened to close the connection for some reason. The client
    may retry at some later date.

    """
    name = 'CONNECTION-FORCED'
    value = 320


class AMQPInvalidPath(Exception):
    """
    The client tried to work with an unknown virtual host.

    """
    name = 'INVALID-PATH'
    value = 402


class AMQPFrameError(Exception):
    """
    The sender sent a malformed frame that the recipient could not decode. This
    strongly implies a programming error in the sending peer.

    """
    name = 'FRAME-ERROR'
    value = 501


class AMQPSyntaxError(Exception):
    """
    The sender sent a frame that contained illegal values for one or more
    fields. This strongly implies a programming error in the sending peer.

    """
    name = 'SYNTAX-ERROR'
    value = 502


class AMQPCommandInvalid(Exception):
    """
    The client sent an invalid sequence of frames, attempting to perform an
    operation that was considered invalid by the server. This usually implies a
    programming error in the client.

    """
    name = 'COMMAND-INVALID'
    value = 503


class AMQPChannelError(Exception):
    """
    The client attempted to work with a channel that had not been correctly
    opened. This most likely indicates a fault in the client layer.

    """
    name = 'CHANNEL-ERROR'
    value = 504


class AMQPUnexpectedFrame(Exception):
    """
    The peer sent a frame that was not expected, usually in the context of a
    content header and body.  This strongly indicates a fault in the peer's
    content processing.

    """
    name = 'UNEXPECTED-FRAME'
    value = 505


class AMQPResourceError(Exception):
    """
    The server could not complete the method because it lacked sufficient
    resources. This may be due to the client creating too many of some type of
    entity.

    """
    name = 'RESOURCE-ERROR'
    value = 506


class AMQPNotAllowed(Exception):
    """
    The client tried to work with some entity in a manner that is prohibited by
    the server, due to security settings or by some other criteria.

    """
    name = 'NOT-ALLOWED'
    value = 530


class AMQPNotImplemented(Exception):
    """
    The client tried to use functionality that is not implemented in the
    server.

    """
    name = 'NOT-IMPLEMENTED'
    value = 540


class AMQPInternalError(Exception):
    """
    The server could not complete the method because of an internal error. The
    server may require intervention by an operator in order to resume normal
    operations.

    """
    name = 'INTERNAL-ERROR'
    value = 541


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
