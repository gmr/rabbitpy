import asyncio
import logging
import pathlib
import queue
import random
import socket
import ssl
import struct
import typing
import unittest
from unittest import mock

import pamqp.frame
from pamqp import commands, constants, header

import rabbitpy.events
from rabbitpy import events, exceptions, io, url_parser
from rabbitpy.url_parser import SslOptions
from tests import mixins

_DEFAULT_SSL_OPTIONS = SslOptions(
    check_hostname=True,
    cafile=None,
    capath=None,
    certfile=None,
    keyfile=None,
    verify=None,
)

LOGGER = logging.getLogger(__name__)

DATA_PATH = pathlib.Path(__file__).parent.resolve() / 'data'


class TestCase(unittest.TestCase):
    def setUp(self):
        """Set up a common IO instance for tests."""
        self.events = events.Events()
        self.exceptions: queue.Queue[Exception] = queue.Queue()
        self.read_queue: queue.Queue[io.PamqpFrame] = queue.Queue()
        self.write_queue: queue.Queue[io.PamqpFrame] = queue.Queue()

    def assertExceptionAdded(self, expectation):
        error = self.exceptions.get(False)
        self.assertIsInstance(error, expectation)
        self.assertTrue(self.events.is_set(events.EXCEPTION_RAISED))

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
            'localhost',
            5672,
            False,
            _DEFAULT_SSL_OPTIONS,
            self.events,
            self.exceptions,
        )
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
            b'IN AMQPLAIN\x00\x00\x00\x05en_US\xce'
        )
        remaining, channel, frame, count = self.io._on_data_received(data_in)
        self.assertEqual(remaining, b'')
        self.assertEqual(channel, 0)
        self.assertIsInstance(frame, commands.Connection.Start)
        assert isinstance(frame, commands.Connection.Start)
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
        data_in = b''.join(
            [
                struct.pack('>BHI', 1, 0, len(payload)),
                payload,
                constants.FRAME_END_CHAR,
            ]
        )
        remaining, channel, frame, count = self.io._on_data_received(data_in)
        self.assertEqual(remaining, data_in)
        self.assertIsNone(channel)
        self.assertIsNone(frame)
        self.assertEqual(count, 0)

    def test_on_error(self):
        self.io._on_error(OSError('Foo'))
        self.assertExceptionAdded(exceptions.ConnectionException)

    def test_on_error_already_closed(self):
        self.events.set(events.SOCKET_CLOSED)
        self.io._on_error(OSError('Foo'))
        with self.assertRaises(queue.Empty):
            self.exceptions.get(False)

    def test_connect_failure(self):
        instance = io.IO(
            'localhost',
            random.randint(1024, 32768),
            False,
            _DEFAULT_SSL_OPTIONS,
            self.events,
            self.exceptions,
        )
        instance.start()
        connected = self.events.wait(rabbitpy.events.SOCKET_OPENED, 1)
        self.assertFalse(connected, 'Should not have connected')
        self.assertExceptionAdded(exceptions.ConnectionException)
        self.assertFalse(self.io.is_connected)

    def test_disconnected_close(self):
        self.io.close()

    def test_write_frame_when_closed(self):
        self.io.write_frame(0, commands.Connection.Start())
        self.assertExceptionAdded(exceptions.ConnectionException)

    def test_get_ssl_context(self):
        self.io._use_ssl = True
        self.io._ssl_options = _DEFAULT_SSL_OPTIONS
        context = self.io._get_ssl_context()
        self.assertIsInstance(context, ssl.SSLContext)
        assert isinstance(context, ssl.SSLContext)
        self.assertTrue(context.check_hostname)

    def test_get_ssl_context_verify_none(self):
        self.io._use_ssl = True
        self.io._ssl_options = SslOptions(
            check_hostname=False,
            cafile=None,
            capath=None,
            certfile=None,
            keyfile=None,
            verify=ssl.CERT_NONE,
        )
        context = self.io._get_ssl_context()
        self.assertIsInstance(context, ssl.SSLContext)
        assert isinstance(context, ssl.SSLContext)
        self.assertFalse(context.check_hostname)

    def test_get_ssl_context_certs(self):
        self.io._use_ssl = True
        self.io._ssl_options = SslOptions(
            check_hostname=True,
            cafile=str(DATA_PATH / 'ca.crt'),
            capath=None,
            certfile=str(DATA_PATH / 'client.crt'),
            keyfile=str(DATA_PATH / 'client.key'),
            verify=ssl.CERT_REQUIRED,
        )
        context = self.io._get_ssl_context()
        self.assertIsInstance(context, ssl.SSLContext)
        assert isinstance(context, ssl.SSLContext)
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)


class MockServer(asyncio.Protocol):
    instances: typing.ClassVar[list['MockServer']] = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._expectation: bytes | None = None
        self._response: bytes | None = None
        self._transport: asyncio.BaseTransport | None = None
        MockServer.instances.append(self)

    def connection_lost(self, exc: Exception | None) -> None:
        MockServer.instances.remove(self)

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = transport
        LOGGER.debug(
            'Connection from %r', transport.get_extra_info('peername')
        )

    def data_received(self, data: bytes) -> None:
        LOGGER.debug('Received %r', data)
        if self._transport is None:
            return
        if self._expectation and data != self._expectation:
            self._transport.write(b'Bad data: ' + data)  # type: ignore[attr-defined]
            return
        if self._response:
            self._transport.write(self._response)  # type: ignore[attr-defined]

        # Connection.CloseOk
        if data == b'\x01\x00\x00\x00\x00\x00\x04\x00\n\x003\xce':
            LOGGER.debug('Closing connection')
            self._transport.close()

    def set_expectation(self, expectation: bytes) -> None:
        self._expectation = expectation

    def set_response(self, response: bytes) -> None:
        self._response = response


class MockServerTestCase(
    mixins.EnvironmentVariableMixin, TestCase, unittest.IsolatedAsyncioTestCase
):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.server = await self._create_server()
        args = self._get_io_args()
        LOGGER.debug('Connection args: %r', args)
        self.io = io.IO(
            args['host'],
            args['port'],
            args['ssl'],
            args['ssl_options'],
            self.events,
            self.exceptions,
        )
        self.io.add_channel(0, self.read_queue)
        self.io.add_channel(1, self.read_queue)

    async def asyncTearDown(self):
        self.server.close()
        await super().asyncTearDown()

    async def get_mock_server(self, retries=10, delay=0.1):
        connected = self.events.wait(rabbitpy.events.SOCKET_OPENED, 3)
        self.assertTrue(connected, 'Timeout waiting for SOCKET_OPENED event')
        assert self.io._socket is not None
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

    async def write_frame(
        self, channel: int, frame: pamqp.frame.FrameTypes
    ) -> None:
        self.io.write_frame(channel, frame)
        await asyncio.sleep(0.1)

    async def write_protocol_header(self) -> None:
        await self.write_frame(0, header.ProtocolHeader())
        self.assertEqual(self.io.bytes_written, 8)

    def _get_io_args(self):
        return url_parser.parse(f'amqp://127.0.0.1:{self._server_port}')

    @staticmethod
    async def _create_server():
        loop = asyncio.get_running_loop()
        return await loop.create_server(MockServer, '127.0.0.1')

    @property
    def _server_port(self):
        return self.server.sockets[0].getsockname()[1]


class MockServerConnectedTestCase(MockServerTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.io.start()
        connected = self.events.wait(rabbitpy.events.SOCKET_OPENED, 3)
        self.assertTrue(connected, 'Timeout waiting for SOCKET_OPENED event')
        self.assertTrue(self.io.is_connected)

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
            b'IN AMQPLAIN\x00\x00\x00\x05en_US\xce'
        )

        await self.write_protocol_header()

        self.assertEqual(self.io._buffer, b'')
        self.assertEqual(self.io.bytes_received, 335)

        frame = self.io._channels[0].get(True, 3)
        self.assertIsInstance(frame, commands.Connection.Start)
        assert isinstance(frame, commands.Connection.Start)
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
            b'\x01\x00\x00\x00\x04\x00Z\x00\x1f\xce'
        )

        await self.write_protocol_header()

        self.assertEqual(self.io.bytes_received, 347)

        frame = self.io._channels[0].get(True, 3)
        self.assertIsInstance(frame, commands.Connection.Start)
        assert isinstance(frame, commands.Connection.Start)
        self.assertEqual(frame.locales, 'en_US')

        frame = self.io._channels[1].get(True, 3)
        self.assertIsInstance(frame, commands.Tx.RollbackOk)

    async def test_on_data_received_remaining_buffer(self):
        mock_server = await self.get_mock_server()
        mock_server.set_expectation(b'AMQP\x00\x00\t\x01')
        mock_server.set_response(
            b'\x01\x00\x01\x00\x00\x00\x04\x00Z\x00\x1f\xce\x01\x00\x00'
        )

        await self.write_protocol_header()

        self.assertEqual(self.io._buffer, b'\x01\x00\x00')
        self.assertEqual(self.io.bytes_received, 12)
        frame = self.read_queue.get(True, 3)
        self.assertIsInstance(frame, commands.Tx.RollbackOk)

    async def test_remote_name(self):
        mock_server = await self.get_mock_server()
        assert mock_server._transport is not None
        remote = mock_server._transport.get_extra_info('sockname')
        assert self.io._socket is not None
        local = self.io._socket.getsockname()
        self.assertEqual(
            self.io.remote_name,
            f'{local[0]}:{local[1]} -> {remote[0]}:{remote[1]}',
        )

    async def test_close(self):
        _mock_server = await self.get_mock_server()
        self.io.close()
        self.assertTrue(self.events.is_set(events.SOCKET_CLOSED))
        self.assertFalse(self.io.is_connected)


class EdgeCaseMockServerTestCase(MockServerTestCase):
    @mock.patch('socket.getaddrinfo')
    async def test_error_on_getaddrinfo(self, mock_getaddrinfo):
        mock_getaddrinfo.side_effect = OSError('Foo')
        self.io.start()
        await asyncio.sleep(0.1)
        self.assertFalse(self.io.is_connected)
        self.assertExceptionAdded(exceptions.ConnectionException)

    async def test_write_ready_socket_timeout(self):
        with mock.patch('socket.socket.sendall') as mock_sendall:
            mock_sendall.side_effect = socket.timeout
            self.io.start()
            await asyncio.sleep(0.1)
            self.assertEqual(self.io.bytes_written, 0)

            self.io.write_frame(1, commands.Tx.RollbackOk())
            self.io._on_write_ready()
            self.assertEqual(self.io.bytes_written, 0)

            # a timeout should have the frame placed back on the top of stack
            value = self.io._write_buffer.popleft()
            expectation = pamqp.frame.marshal(commands.Tx.RollbackOk(), 1)
            self.assertEqual(value, expectation)

    async def test_write_ready_socket_oserror(self):
        with mock.patch('socket.socket.sendall') as mock_sendall:
            self.io.start()
            await asyncio.sleep(0.1)
            mock_sendall.side_effect = OSError('Foo')
            self.io.write_frame(1, commands.Tx.RollbackOk())
            self.io._on_write_ready()

            self.assertEqual(self.io.bytes_written, 0)

            # a timeout should have the frame placed back on the top of stack
            value = self.io._write_buffer.popleft()
            expectation = pamqp.frame.marshal(commands.Tx.RollbackOk(), 1)
            self.assertEqual(value, expectation)

            self.assertExceptionAdded(exceptions.ConnectionException)

    async def test_write_ready_socket_errno_35(self):
        with mock.patch('socket.socket.sendall') as mock_sendall:
            self.io.start()
            await asyncio.sleep(0.1)
            mock_sendall.side_effect = Error35(35, 'Mock Error 35')
            self.io.write_frame(1, commands.Tx.RollbackOk())
            self.io._on_write_ready()
            self.assertEqual(self.io.bytes_written, 0)

            # a timeout should have the frame placed back on the top of stack
            value = self.io._write_buffer.popleft()
            expectation = pamqp.frame.marshal(commands.Tx.RollbackOk(), 1)
            self.assertEqual(value, expectation)

            # No exception should have been added
            with self.assertRaises(queue.Empty):
                self.exceptions.get(False)

    async def test_run_asyncio_cancelled(self):
        with mock.patch.object(self.io, '_run') as mock_run:
            mock_run.side_effect = asyncio.CancelledError()
            self.io.start()
            self.assertFalse(self.io.is_connected)

    async def test_run_oserror(self):
        with mock.patch.object(self.io, '_run') as mock_run:
            mock_run.side_effect = OSError('Mock Error')
            self.io.start()
            self.assertFalse(self.io.is_connected)
        await asyncio.sleep(0.1)
        self.assertExceptionAdded(exceptions.ConnectionException)


class SSLMockServerTestCase(MockServerTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.io.start()
        # Yield to the event loop so the in-process SSL mock server can
        # complete its handshake (blocking wait() would deadlock the loop).
        for _ in range(50):
            if self.events.is_set(rabbitpy.events.SOCKET_OPENED):
                break
            await asyncio.sleep(0.1)
        self.assertTrue(
            self.events.is_set(rabbitpy.events.SOCKET_OPENED),
            'Timeout waiting for SOCKET_OPENED event',
        )
        self.assertTrue(self.io.is_connected)

    @staticmethod
    async def _create_server():
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_verify_locations(DATA_PATH / 'ca.crt')
        context.load_cert_chain(
            DATA_PATH / 'server.crt', DATA_PATH / 'server.key'
        )
        loop = asyncio.get_running_loop()
        server = await loop.create_server(MockServer, '127.0.0.1', ssl=context)
        LOGGER.debug(
            'Mock server running on: %s', server.sockets[0].getsockname()
        )
        return server

    def _get_io_args(self):
        return url_parser.parse(
            f'amqps://127.0.0.1:{self._server_port}?'
            f'ssl_check_hostname=false&ssl_verify=ignore&'
            f'cafile=tests%2Fdata%2Fca.crt'
        )

    async def test_on_data_received(self):
        mock_server = await self.get_mock_server()
        mock_server.set_expectation(b'AMQP\x00\x00\t\x01')
        mock_server.set_response(
            b'\x01\x00\x01\x00\x00\x00\x04\x00Z\x00\x1f\xce\x01\x00\x00'
        )

        await self.write_protocol_header()

        # Get the frame first — blocks until the IO thread has processed the
        # full response, avoiding a race on _buffer and bytes_received.
        frame = self.read_queue.get(True, 3)
        self.assertIsInstance(frame, commands.Tx.RollbackOk)
        self.assertEqual(self.io._buffer, b'\x01\x00\x00')
        self.assertEqual(self.io.bytes_received, 12)


class Error35(OSError):
    """Mock Exception for socket.error errno=35"""

    def __init__(self, errno, *args):
        super().__init__(*args)
        self.errno = errno
