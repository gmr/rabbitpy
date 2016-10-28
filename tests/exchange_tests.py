"""
Test the rabbitpy.exchange classes

"""
import mock
from pamqp import specification

from rabbitpy import exchange

from . import helpers


class TxTests(helpers.TestCase):

    @mock.patch('rabbitpy.exchange.Exchange._rpc')
    def test_bind_sends_exchange_declare(self, rpc):
        rpc.return_value = specification.Exchange.DeclareOk
        obj = exchange.Exchange(self.channel, 'foo')
        obj.declare()
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Exchange.Declare)

    @mock.patch('rabbitpy.exchange.Exchange._rpc')
    def test_bind_sends_exchange_delete(self, rpc):
        rpc.return_value = specification.Exchange.DeleteOk
        obj = exchange.Exchange(self.channel, 'foo')
        obj.delete()
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Exchange.Delete)

    @mock.patch('rabbitpy.exchange.Exchange._rpc')
    def test_bind_sends_exchange_bind(self, rpc):
        rpc.return_value = specification.Exchange.BindOk
        obj = exchange.Exchange(self.channel, 'foo')
        obj.bind('a', 'b')
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Exchange.Bind)

    @mock.patch('rabbitpy.exchange.Exchange._rpc')
    def test_bind_sends_exchange_unbind(self, rpc):
        rpc.return_value = specification.Exchange.UnbindOk
        obj = exchange.Exchange(self.channel, 'foo')
        obj.unbind('a', 'b')
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Exchange.Unbind)

    @mock.patch('rabbitpy.exchange.Exchange._rpc')
    def test_bind_sends_exchange_bind_obj(self, rpc):
        rpc.return_value = specification.Exchange.BindOk
        obj = exchange.Exchange(self.channel, 'foo')
        val = mock.Mock()
        val.name = 'bar'
        obj.bind(val, 'b')
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Exchange.Bind)

    @mock.patch('rabbitpy.exchange.Exchange._rpc')
    def test_bind_sends_exchange_unbind_obj(self, rpc):
        rpc.return_value = specification.Exchange.UnbindOk
        obj = exchange.Exchange(self.channel, 'foo')
        val = mock.Mock()
        val.name = 'bar'
        obj.unbind(val, 'b')
        self.assertIsInstance(rpc.mock_calls[0][1][0],
                              specification.Exchange.Unbind)


class DirectExchangeCreationTests(helpers.TestCase):

    def test_init_creates_direct_exchange(self):
        obj = exchange.DirectExchange(self.channel, 'direct-test')
        self.assertEqual(obj.type, 'direct')


class FanoutExchangeCreationTests(helpers.TestCase):

    def test_init_creates_direct_exchange(self):
        obj = exchange.FanoutExchange(self.channel, 'fanout-test')
        self.assertEqual(obj.type, 'fanout')


class HeadersExchangeCreationTests(helpers.TestCase):

    def test_init_creates_direct_exchange(self):
        obj = exchange.HeadersExchange(self.channel, 'headers-test')
        self.assertEqual(obj.type, 'headers')


class TopicExchangeCreationTests(helpers.TestCase):

    def test_init_creates_direct_exchange(self):
        obj = exchange.TopicExchange(self.channel, 'topic-test')
        self.assertEqual(obj.type, 'topic')
