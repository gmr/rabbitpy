"""
rmqid Specific Exceptions

"""

class ActionException(Exception):
    def __repr__(self):
        return self.args[0]


class ChannelClosedException(Exception):
    def __repr__(self):
        return 'Can not perform RPC requests on a closed channel, you must ' \
               'create a new channel'


class EmptyExceptionNameError(Exception):
    def __repr__(self):
        return 'You must specify a Queue name'


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
        return 'Message %s was returned by RabbitMQ: (%s) %s' % (self.args[0],
                                                                 self.args[1],
                                                                 self.args[2])

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
