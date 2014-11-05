"""
Test the rabbitpy.tx classes

"""
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import mock
from pamqp import specification

from rabbitpy import channel
from rabbitpy import exceptions
from rabbitpy import tx


class TxTests(unittest.TestCase):

    def setUp(self):
        self.chan = channel.Channel(1, {}, None, None, None, None, 32768, None)

    def test_obj_creation_does_not_invoke_select(self):
        with mock.patch('rabbitpy.tx.Tx.select') as select:
            transaction = tx.Tx(self.chan)
            self.assertFalse(transaction._selected)
            select.assert_not_called()

    def test_enter_invokes_select(self):
        with mock.patch('rabbitpy.tx.Tx.select') as select:
            with tx.Tx(self.chan):
                select.assert_called_once()

    @mock.patch('rabbitpy.tx.Tx._rpc')
    def test_exit_invokes_commit(self, rpc):
        rpc.return_value = specification.Tx.SelectOk
        with mock.patch('rabbitpy.tx.Tx.select') as select:
            with mock.patch('rabbitpy.tx.Tx.commit') as commit:
                with tx.Tx(self.chan) as transaction:
                    transaction._selected = True
                commit.assert_called_once()

    @mock.patch('rabbitpy.tx.Tx._rpc')
    def test_exit_on_exception_invokes_commit_with_selected(self, rpc):
        rpc.return_value = specification.Tx.SelectOk
        with mock.patch('rabbitpy.tx.Tx.select') as select:
            with mock.patch('rabbitpy.tx.Tx.rollback') as rollback:
                try:
                    with tx.Tx(self.chan) as transaction:
                        transaction._selected = True
                        raise exceptions.AMQPChannelError()
                except exceptions.AMQPChannelError:
                    pass
                rollback.assert_called_once()

    @mock.patch('rabbitpy.tx.Tx._rpc')
    def test_select_invokes_rpc_with_tx_select(self, rpc):
        rpc.return_value = specification.Tx.CommitOk
        with tx.Tx(self.chan):
            pass
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Tx.Select)

    @mock.patch('rabbitpy.tx.Tx._rpc')
    def test_commit_invokes_rpc_with_tx_commit(self, rpc):
        rpc.return_value = specification.Tx.SelectOk
        obj = tx.Tx(self.chan)
        obj.select()
        rpc.return_value = specification.Tx.CommitOk
        obj.commit()
        self.assertIsInstance(rpc.mock_calls[1][1][0],
                              specification.Tx.Commit)

    @mock.patch('rabbitpy.tx.Tx._rpc')
    def test_commit_raises_when_channel_closed(self, rpc):
        obj = tx.Tx(self.chan)
        obj.select()
        rpc.side_effect = exceptions.ChannelClosedException
        self.assertRaises(exceptions.NoActiveTransactionError,
                          obj.commit)

    @mock.patch('rabbitpy.tx.Tx._rpc')
    def test_rollback_invokes_rpc_with_tx_rollback(self, rpc):
        rpc.return_value = specification.Tx.SelectOk
        obj = tx.Tx(self.chan)
        obj.select()
        rpc.return_value = specification.Tx.RollbackOk
        obj.rollback()
        self.assertIsInstance(rpc.mock_calls[1][1][0],
                              specification.Tx.Rollback)

    @mock.patch('rabbitpy.tx.Tx._rpc')
    def test_rollback_raises_when_channel_closed(self, rpc):
        obj = tx.Tx(self.chan)
        obj.select()
        rpc.side_effect = exceptions.ChannelClosedException
        self.assertRaises(exceptions.NoActiveTransactionError,
                          obj.rollback)
