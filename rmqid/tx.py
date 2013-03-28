"""
The TX or transaction class implements transactional functionality in RabbitMQ
and allows for any AMQP command to be issued, then committed or rolled back.

"""
from pamqp import specification as spec

from rmqid import base
from rmqid import exceptions


class TX(base.AMQPClass):
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
    :type channel: :py:class:`rmqid.channel.Channel`

    """
    def __init__(self, channel):
        super(TX, self).__init__(channel, 'TX')

    def select(self):
        """Select standard transaction mode

        This method sets the channel to use standard transactions. The client
        must use this method at least once on a channel before using the Commit
        or Rollback methods.

        :raises: rmqid.exceptions.UnexpectedResponseError

        """
        response = self._rpc(spec.Tx.Select())
        if not isinstance(response, spec.Tx.SelectOk):
            raise exceptions.UnexpectedResponseError(spec.Tx.SelectOk,
                                                     response)

    def commit(self):
        """Commit the current transaction

        This method commits all message publications and acknowledgments
        performed in the current transaction.  A new transaction starts
        immediately after a commit.

        :raises: rmqid.exceptions.NoActiveTransactionError
        :raises: rmqid.exceptions.UnexpectedResponseError

        """
        try:
            response = self._rpc(spec.Tx.Commit())
        except exceptions.ChannelClosedException:
            raise exceptions.NoActiveTransactionError()

        if not isinstance(response, spec.Tx.CommitOk):
            raise exceptions.UnexpectedResponseError(spec.Tx.CommitOk,
                                                     response)

    def rollback(self):
        """Abandon the current transaction

        This method abandons all message publications and acknowledgments
        performed in the current transaction. A new transaction starts
        immediately after a rollback. Note that unacked messages will not be
        automatically redelivered by rollback; if that is required an explicit
        recover call should be issued.

        :raises: rmqid.exceptions.NoActiveTransactionError
        :raises: rmqid.exceptions.UnexpectedResponseError

        """
        try:
            response = self._rpc(spec.Tx.Rollback())
        except exceptions.ChannelClosedException:
            raise exceptions.NoActiveTransactionError()
        if not isinstance(response, spec.Tx.RollbackOk):
            raise exceptions.UnexpectedResponseError(spec.Tx.RollbackOk,
                                                     response)
