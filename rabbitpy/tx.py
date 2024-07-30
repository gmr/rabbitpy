"""
The TX or transaction class implements transactional functionality in RabbitMQ
and allows for any AMQP command to be issued, then committed or rolled back.

"""
import logging
import types
import typing

from pamqp import commands

from rabbitpy import base, channel as chan, exceptions

LOGGER = logging.getLogger(__name__)


class Tx(base.AMQPClass):
    """Work with transactions

    The Tx class allows publish and ack operations to be batched into atomic
    units of work.  The intention is that all publish and ack requests issued
    within a transaction will complete successfully or none of them will.
    Servers SHOULD implement atomic transactions at least where all publish or
    ack requests affect a single queue.  Transactions that cover multiple
    queues may be non-atomic, given that queues can be created and destroyed
    asynchronously, and such events do not form part of any transaction.
    Further, the behaviour of transactions with respect to the immediate and
    mandatory flags on Basic.Publish methods is not defined.

    :param channel: The channel object to start the transaction on

    """

    def __init__(self, channel: chan.Channel):
        super(Tx, self).__init__(channel, 'Tx')
        self._selected = False

    def __enter__(self) -> 'Tx':
        """For use as a context manager, return a handle to this object
        instance.

        """
        self.select()
        return self

    def __exit__(self,
                 exc_type: typing.Optional[typing.Type[BaseException]],
                 exc_val: typing.Optional[BaseException],
                 unused_exc_tb: typing.Optional[types.TracebackType]):
        """When leaving the context, examine why the context is leaving, if
        it's an exception or what.

        """
        if exc_type:
            LOGGER.warning('Exiting Transaction on exception: %r', exc_val)
            if self._selected:
                self.rollback()
            raise exc_val
        else:
            LOGGER.debug('Committing transaction on exit of context block')
            if self._selected:
                self.commit()

    def select(self) -> bool:
        """Select standard transaction mode

        This method sets the channel to use standard transactions. The client
        must use this method at least once on a channel before using the Commit
        or Rollback methods.

        """
        response = self._rpc(commands.Tx.Select())
        self._selected = isinstance(response, commands.Tx.SelectOk)
        return self._selected

    def commit(self) -> bool:
        """Commit the current transaction

        This method commits all message publications and acknowledgments
        performed in the current transaction.  A new transaction starts
        immediately after a commit.

        :raises: rabbitpy.exceptions.NoActiveTransactionError

        """
        try:
            response = self._rpc(commands.Tx.Commit())
        except exceptions.ChannelClosedException as error:
            LOGGER.warning('Error committing transaction: %s', error)
            raise exceptions.NoActiveTransactionError()
        self._selected = False
        return isinstance(response, commands.Tx.CommitOk)

    def rollback(self) -> bool:
        """Abandon the current transaction

        This method abandons all message publications and acknowledgments
        performed in the current transaction. A new transaction starts
        immediately after a rollback. Note that unacked messages will not be
        automatically redelivered by rollback; if that is required an explicit
        recover call should be issued.

        :raises: rabbitpy.exceptions.NoActiveTransactionError

        """
        try:
            response = self._rpc(commands.Tx.Rollback())
        except exceptions.ChannelClosedException as error:
            LOGGER.warning('Error rolling back transaction: %s', error)
            raise exceptions.NoActiveTransactionError()
        self._selected = False
        return isinstance(response, commands.Tx.RollbackOk)
