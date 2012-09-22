"""
rmqid Specific Exceptions

"""

class ChannelClosedException(Exception):
    def __repr__(self):
        return 'Channel %i was closed by the remote server (%i): %s' % \
               (self.args[0], self.args[1], self.args[2])


class ConnectionClosedException(Exception):
    def __repr__(self):
        return 'Connection was closed by the remote server (%i): %s' % \
               (self.args[0], self.args[1])


class TooManyChannelsError(Exception):
    def __repr__(self):
        return 'The maximum amount of negotiated channels has been reached'

