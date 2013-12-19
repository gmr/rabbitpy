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

AMQPContentTooLarge = specification.AMQPContentTooLarge
AMQPNoRoute = specification.AMQPNoRoute
AMQPNoConsumers = specification.AMQPNoConsumers
AMQPConnectionForced = specification.AMQPConnectionForced
AMQPInvalidPath = specification.AMQPInvalidPath
AMQPAccessRefused = specification.AMQPAccessRefused
AMQPNotFound = specification.AMQPNotFound
AMQPResourceLocked = specification.AMQPResourceLocked
AMQPPreconditionFailed = specification.AMQPPreconditionFailed
AMQPFrameError = specification.AMQPFrameError
AMQPSyntaxError = specification.AMQPSyntaxError
AMQPCommandInvalid = specification.AMQPCommandInvalid
AMQPChannelError = specification.AMQPChannelError
AMQPUnexpectedFrame = specification.AMQPUnexpectedFrame
AMQPResourceError = specification.AMQPResourceError
AMQPNotAllowed = specification.AMQPNotAllowed
AMQPNotImplemented = specification.AMQPNotImplemented
AMQPInternalError = specification.AMQPInternalError

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
