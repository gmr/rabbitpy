"""
Test the rabbitpy.exchange classes

"""
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import mock
from pamqp import specification

from rabbitpy import channel
from rabbitpy import exceptions
from rabbitpy import exchange


class TxTests(unittest.TestCase):

    def setUp(self):
        self.chan = channel.Channel(1, None, None, None, None, 32768, None)

    @mock.patch('rabbitpy.exchange.Exchange._rpc')
    def test_bind_sends_exchange_declare(self, rpc):
        rpc.return_value = specification.Exchange.DeclareOk
        obj = exchange.Exchange(self.chan, 'foo')
        obj.declare()
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Exchange.Declare)

    @mock.patch('rabbitpy.exchange.Exchange._rpc')
    def test_bind_sends_exchange_delete(self, rpc):
        rpc.return_value = specification.Exchange.DeleteOk
        obj = exchange.Exchange(self.chan, 'foo')
        obj.delete()
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Exchange.Delete)

    @mock.patch('rabbitpy.exchange.Exchange._rpc')
    def test_bind_sends_exchange_bind(self, rpc):
        rpc.return_value = specification.Exchange.BindOk
        obj = exchange.Exchange(self.chan, 'foo')
        obj.bind('a', 'b')
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Exchange.Bind)

    @mock.patch('rabbitpy.exchange.Exchange._rpc')
    def test_bind_sends_exchange_unbind(self, rpc):
        rpc.return_value = specification.Exchange.UnbindOk
        obj = exchange.Exchange(self.chan, 'foo')
        obj.unbind('a', 'b')
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Exchange.Unbind)

    @mock.patch('rabbitpy.exchange.Exchange._rpc')
    def test_bind_sends_exchange_bind_obj(self, rpc):
        rpc.return_value = specification.Exchange.BindOk
        obj = exchange.Exchange(self.chan, 'foo')
        val = mock.Mock()
        val.name = 'bar'
        obj.bind(val, 'b')
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Exchange.Bind)

    @mock.patch('rabbitpy.exchange.Exchange._rpc')
    def test_bind_sends_exchange_unbind_obj(self, rpc):
        rpc.return_value = specification.Exchange.UnbindOk
        obj = exchange.Exchange(self.chan, 'foo')
        val = mock.Mock()
        val.name = 'bar'
        obj.unbind(val, 'b')
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Exchange.Unbind)


class DirectExchangeCreationTests(unittest.TestCase):

    def setUp(self):
        self.chan = channel.Channel(1, None, None, None, None, 32768, None)

    def test_init_creates_direct_exchange(self):
        obj = exchange.DirectExchange(self.chan, 'direct-test')
        self.assertEqual(obj.type, 'direct')


class FanoutExchangeCreationTests(unittest.TestCase):

    def setUp(self):
        self.chan = channel.Channel(1, None, None, None, None, 32768, None)

    def test_init_creates_direct_exchange(self):
        obj = exchange.FanoutExchange(self.chan, 'fanout-test')
        self.assertEqual(obj.type, 'fanout')


class HeadersExchangeCreationTests(unittest.TestCase):

    def setUp(self):
        self.chan = channel.Channel(1, None, None, None, None, 32768, None)

    def test_init_creates_direct_exchange(self):
        obj = exchange.HeadersExchange(self.chan, 'headers-test')
        self.assertEqual(obj.type, 'headers')


class TopicExchangeCreationTests(unittest.TestCase):

    def setUp(self):
        self.chan = channel.Channel(1, None, None, None, None, 32768, None)

    def test_init_creates_direct_exchange(self):
        obj = exchange.TopicExchange(self.chan, 'topic-test')
        self.assertEqual(obj.type, 'topic')


