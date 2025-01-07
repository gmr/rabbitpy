import asyncio
import logging
import pathlib
import queue
import random
import ssl
import struct
import typing
import unittest

from pamqp import commands, constants, header

import rabbitpy.events
from rabbitpy import events, exceptions, io, url_parser
from tests import mixins

LOGGER = logging.getLogger(__name__)


class TestCase(unittest.TestCase):

    def setUp(self):
        """Set up a common IO instance for tests."""
        self.events = events.Events()
        self.exceptions = queue.Queue()
        self.read_queue = queue.Queue()
        self.write_queue = queue.Queue()

    def tearDown(self):
        try:
            exc = self.exceptions.get(False)
        except queue.Empty:
            pass
        else:
            LOGGER.exception('Uncaught exception: %r', exc)
            raise exc


class TestIO(TestCase):

    def setUp(self):
        """Set up a common IO instance for tests."""
        super().setUp()
        self.io = io.IO(
            'localhost', 5672, False, {}, self.events,
            self.exceptions)
        self.io.add_channel(0, self.read_queue)

    def test_initialization(self):
        """Test IO class initialization."""
        self.assertEqual(self.io._host, 'localhost')
        self.assertEqual(self.io._port, 5672)
        self.assertFalse(self.io._use_ssl)
        self.assertIsInstance(self.io._channels, dict)
        self.assertIsInstance(self.io._events, events.Events)
        self.assertIsInstance(self.io._exceptions, queue.Queue)
        self.assertFalse(self.io.is_connected)

    def test_on_data_received(self):
        data_in = (
            b'\x01\x00\x00\x00\x00\x01G\x00\n\x00\n\x00\t'
            b'\x00\x00\x01"\x0ccapabilitiesF\x00\x00\x00X'
            b'\x12publisher_confirmst\x01\x1aexchange_exc'
            b'hange_bindingst\x01\nbasic.nackt\x01\x16con'
            b'sumer_cancel_notifyt\x01\tcopyrightS\x00'
            b'\x00\x00$Copyright (C) 2007-2011 VMware, I'
            b'nc.\x0binformationS\x00\x00\x005Licensed u'
            b'nder the MPL.  See http://www.rabbitmq.com'
            b'/\x08platformS\x00\x00\x00\nErlang/OTP\x07'
            b'productS\x00\x00\x00\x08RabbitMQ\x07versio'
            b'nS\x00\x00\x00\x052.6.1\x00\x00\x00\x0ePLA'
            b'IN AMQPLAIN\x00\x00\x00\x05en_US\xce')
        remaining, channel, frame, count = self.io._on_data_received(data_in)
        self.assertEqual(remaining, b'')
        self.assertEqual(channel, 0)
        self.assertIsInstance(frame, commands.Connection.Start)
        self.assertEqual(frame.locales, 'en_US')
        self.assertEqual(count, 335)

    def test_on_data_received_empty(self):
        remaining, channel, frame, byte_count = self.io._on_data_received(b'')
        self.assertEqual(remaining, b'')
        self.assertIsNone(channel)
        self.assertIsNone(frame)
        self.assertEqual(byte_count, 0)

    def test_on_data_received_error(self):
        payload = struct.pack('>L', 42949)
        data_in = b''.join([struct.pack('>BHI', 1, 0, len(payload)),
                             payload, constants.FRAME_END_CHAR])
        remaining, channel, frame, count = self.io._on_data_received(data_in)
        self.assertEqual(remaining, data_in)
        self.assertIsNone(channel)
        self.assertIsNone(frame)
        self.assertEqual(count, 0)

    def test_on_error(self):
        self.io._on_error(OSError('Foo'))
        error = self.exceptions.get(True, 3)
        self.assertIsInstance(error, exceptions.ConnectionException)
        self.assertTrue(self.events.is_set(events.EXCEPTION_RAISED))

    def test_on_error_already_closed(self):
        self.events.set(events.SOCKET_CLOSED)
        self.io._on_error(OSError('Foo'))
        with self.assertRaises(queue.Empty):
            self.exceptions.get(False)

    def test_connect_failure(self):
        instance = io.IO(
            'localhost', random.randint(1024, 32768),  # noqa: S311
            False, {}, self.events, self.exceptions)
        instance.start()
        self.assertIsInstance(
            self.exceptions.get(True, 3),
            rabbitpy.exceptions.ConnectionException)
        self.assertFalse(self.io.is_connected)

    def test_disconnected_close(self):
        self.io.close()

    def test_write_frame_when_closed(self):
        with self.assertRaises(exceptions.ConnectionException):
            self.io.write_frame(0, commands.Connection.Start())

    def test_get_ssl_context(self):
        self.io._use_ssl = True
        self.io._ssl_options = {
            'check_hostname': False,
            'verify': ssl.CERT_NONE
        }
        context = self.io._get_ssl_context()
        self.assertIsInstance(context, ssl.SSLContext)
        self.assertFalse(context.check_hostname)

    def test_get_ssl_context_certs(self):
        data_path = pathlib.Path(__file__).parent.resolve() / 'data'
        self.io._use_ssl = True
        self.io._ssl_options = {
            'ca_certs': data_path / 'ca.crt',
            'certfile': data_path / 'client.crt',
            'keyfile': data_path / 'client.key',
            'verify': ssl.CERT_REQUIRED
        }
        context = self.io._get_ssl_context()
        self.assertIsInstance(context, ssl.SSLContext)
        self.assertTrue(context.check_hostname)


class MockServer(asyncio.Protocol):

    instances = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._expectation: typing.Optional[bytes] = None
        self._response: typing.Optional[bytes] = None
        self._transport: typing.Optional[asyncio.Transport] = None
        MockServer.instances.append(self)

    def connection_lost(self, exc):
        MockServer.instances.remove(self)

    def connection_made(self, transport):
        self._transport = transport
        LOGGER.debug('Connection from %r',
                     transport.get_extra_info('peername'))

    def data_received(self, data):
        LOGGER.debug('Received %r', data)
        if self._expectation and data != self._expectation:
            return self._transport.write(b'Bad data: ' + data)
        if self._response:
            self._transport.write(self._response)

        # Connection.CloseOk
        if data == '\x01\x00\x00\x00\x00\x00\x04\x00\n\x003\xce':
            LOGGER.debug('Closing connection')
            self._transport.close()

    def set_expectation(self, expectation: bytes):
        self._expectation = expectation

    def set_response(self, response: bytes):
        self._response = response


class MockServerTestCase(mixins.EnvironmentVariableMixin,
                         TestCase,
                         unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        loop = asyncio.get_running_loop()
        self.server = await loop.create_server(MockServer, '127.0.0.1')
        args = url_parser.parse(
            f'amqp://127.0.0.1:{self.server.sockets[0].getsockname()[1]}')
        self.io = io.IO(
            args['host'], args['port'], args['ssl'], args['ssl_options'],
            self.events, self.exceptions)
        self.io.add_channel(0, self.read_queue)
        self.io.add_channel(1, self.read_queue)
        self.io.start()
        connected = self.events.wait(rabbitpy.events.SOCKET_OPENED, 3)
        self.assertTrue(connected, 'Timeout waiting for SOCKET_OPENED event')
        self.assertTrue(self.io.is_connected)

    async def asyncTearDown(self):
        self.server.close()
        await super().asyncTearDown()

    async def get_mock_server(self, retries=10, delay=0.1):
        connected = self.events.wait(rabbitpy.events.SOCKET_OPENED, 3)
        self.assertTrue(connected, 'Timeout waiting for SOCKET_OPENED event')
        socket_local = self.io._socket.getsockname()  # (address, port)
        socket_remote = self.io._socket.getpeername()  # (address, port)
        for _attempt in range(retries):
            for instance in MockServer.instances:
                if instance._transport:
                    server = instance._transport.get_extra_info('sockname')
                    peer = instance._transport.get_extra_info('peername')
                    if socket_local == peer and socket_remote == server:
                        return instance  # Return the matched MockServer
            await asyncio.sleep(delay)
        raise AssertionError('No MockServer found')

    async def write_protocol_header(self):
        await self.write_frame(0, header.ProtocolHeader())
        self.assertEqual(self.io.bytes_written, 8)

    async def write_frame(self, channel: int, frame):
        self.io.write_frame(channel, frame)
        await asyncio.sleep(0.1)

    async def test_on_read_ready(self):
        mock_server = await self.get_mock_server()
        mock_server.set_expectation(b'AMQP\x00\x00\t\x01')
        mock_server.set_response(
            b'\x01\x00\x00\x00\x00\x01G\x00\n\x00\n\x00\t'
            b'\x00\x00\x01"\x0ccapabilitiesF\x00\x00\x00X'
            b'\x12publisher_confirmst\x01\x1aexchange_exc'
            b'hange_bindingst\x01\nbasic.nackt\x01\x16con'
            b'sumer_cancel_notifyt\x01\tcopyrightS\x00'
            b'\x00\x00$Copyright (C) 2007-2011 VMware, I'
            b'nc.\x0binformationS\x00\x00\x005Licensed u'
            b'nder the MPL.  See http://www.rabbitmq.com'
            b'/\x08platformS\x00\x00\x00\nErlang/OTP\x07'
            b'productS\x00\x00\x00\x08RabbitMQ\x07versio'
            b'nS\x00\x00\x00\x052.6.1\x00\x00\x00\x0ePLA'
            b'IN AMQPLAIN\x00\x00\x00\x05en_US\xce')

        await self.write_protocol_header()

        self.assertEqual(self.io._buffer, b'')
        self.assertEqual(self.io.bytes_received, 335)

        frame = self.io._channels[0].get(True, 3)
        self.assertIsInstance(frame, commands.Connection.Start)
        self.assertEqual(frame.locales, 'en_US')

    async def test_on_data_received_multiple_frames(self):
        mock_server = await self.get_mock_server()
        mock_server.set_expectation(b'AMQP\x00\x00\t\x01')
        mock_server.set_response(
            b'\x01\x00\x00\x00\x00\x01G\x00\n\x00\n\x00\t'
            b'\x00\x00\x01"\x0ccapabilitiesF\x00\x00\x00X'
            b'\x12publisher_confirmst\x01\x1aexchange_exc'
            b'hange_bindingst\x01\nbasic.nackt\x01\x16con'
            b'sumer_cancel_notifyt\x01\tcopyrightS\x00'
            b'\x00\x00$Copyright (C) 2007-2011 VMware, I'
            b'nc.\x0binformationS\x00\x00\x005Licensed u'
            b'nder the MPL.  See http://www.rabbitmq.com'
            b'/\x08platformS\x00\x00\x00\nErlang/OTP\x07'
            b'productS\x00\x00\x00\x08RabbitMQ\x07versio'
            b'nS\x00\x00\x00\x052.6.1\x00\x00\x00\x0ePLA'
            b'IN AMQPLAIN\x00\x00\x00\x05en_US\xce\x01\x00'
            b'\x01\x00\x00\x00\x04\x00Z\x00\x1f\xce')

        await self.write_protocol_header()

        self.assertEqual(self.io.bytes_received, 347)

        frame = self.io._channels[0].get(True, 3)
        self.assertIsInstance(frame, commands.Connection.Start)
        self.assertEqual(frame.locales, 'en_US')

        frame = self.io._channels[1].get(True, 3)
        self.assertIsInstance(frame, commands.Tx.RollbackOk)

    async def test_on_data_received_remaining_buffer(self):
        mock_server = await self.get_mock_server()
        mock_server.set_expectation(b'AMQP\x00\x00\t\x01')
        mock_server.set_response(
            b'\x01\x00\x01\x00\x00\x00\x04\x00Z\x00\x1f\xce\x01\x00\x00')

        await self.write_protocol_header()

        self.assertEqual(self.io._buffer, b'\x01\x00\x00')
        self.assertEqual(self.io.bytes_received, 12)
        frame = self.read_queue.get(True, 3)
        self.assertIsInstance(frame, commands.Tx.RollbackOk)

    async def test_remote_name(self):
        mock_server = await self.get_mock_server()
        remote = mock_server._transport.get_extra_info('sockname')
        local = self.io._socket.getsockname()
        self.assertEqual(
            self.io.remote_name,
            f'{local[0]}:{local[1]} -> {remote[0]}:{remote[1]}')

    async def test_close(self):
        _mock_server = await self.get_mock_server()
        self.io.close()
        self.assertTrue(self.events.is_set(events.SOCKET_CLOSED))
        self.assertFalse(self.io.is_connected)
