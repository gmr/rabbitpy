"""Unit tests for rabbitpy.channel0 AMQP connection-negotiation thread."""

import queue
import unittest
from unittest import mock

import pamqp.heartbeat
from pamqp import commands, header

from rabbitpy import channel0, events, exceptions
from rabbitpy.url_parser import ConnectionArgs, SslOptions

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEFAULT_SSL_OPTIONS: SslOptions = {
    'check_hostname': False,
    'cafile': None,
    'capath': None,
    'certfile': None,
    'keyfile': None,
    'verify': None,
}


def _make_args(**overrides: object) -> ConnectionArgs:
    base: ConnectionArgs = {
        'username': 'guest',
        'password': 'guest',
        'virtual_host': '/',
        'timeout': 3,
        'heartbeat': 60,
        'frame_max': 131072,
        'channel_max': 65535,
        'locale': 'en_US',
        'host': 'localhost',
        'port': 5672,
        'ssl': False,
        'ssl_options': _DEFAULT_SSL_OPTIONS,
    }
    base.update(overrides)  # type: ignore[typeddict-item]
    return base


def _make(**arg_overrides):
    """Return (Channel0, Events, exc_queue)."""
    args = _make_args(**arg_overrides)
    ev = events.Events()
    exc: queue.Queue[Exception] = queue.Queue()
    ch0 = channel0.Channel0(args=args, events=ev, exceptions_queue=exc)
    return ch0, ev, exc


def _mock_io():
    return mock.MagicMock(name='io.IO')


# Pre-built frames for the happy-path handshake
_START = commands.Connection.Start(
    version_major=0,
    version_minor=9,
    server_properties={'capabilities': {'publisher_confirms': True}},
    mechanisms='PLAIN',
    locales='en_US',
)
_TUNE = commands.Connection.Tune(channel_max=0, frame_max=131072, heartbeat=60)
_OPEN_OK = commands.Connection.OpenOk()


def _load_happy_path(ch0_obj):
    """Pre-load negotiation frames and a None terminator for _run_loop."""
    for frame in (_START, _TUNE, _OPEN_OK, None):
        ch0_obj.pending_frames.put(frame)


def _run(ch0_obj, mock_io=None, timeout=5):
    """Start ch0 thread and wait for it to finish."""
    ch0_obj.start(mock_io or _mock_io())
    ch0_obj.join(timeout=timeout)


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------


class TestChannel0Properties(unittest.TestCase):
    def setUp(self):
        self.ch0, self.ev, self.exc = _make()

    def test_initial_open_is_false(self):
        self.assertFalse(self.ch0.open)

    def test_initial_properties_empty(self):
        self.assertEqual(self.ch0.properties, {})

    def test_maximum_channels_from_args(self):
        self.assertEqual(self.ch0.maximum_channels, 65535)

    def test_maximum_frame_size_from_args(self):
        self.assertEqual(self.ch0.maximum_frame_size, 131072)

    def test_heartbeat_interval_from_args(self):
        self.assertEqual(self.ch0.heartbeat_interval, 60)

    def test_daemon_thread(self):
        self.assertTrue(self.ch0.daemon)


# ---------------------------------------------------------------------------
# Static helpers
# ---------------------------------------------------------------------------


class TestNegotiateValue(unittest.TestCase):
    def _n(self, s, c):
        return channel0.Channel0._negotiate_value(s, c)

    def test_both_nonzero_returns_minimum(self):
        self.assertEqual(self._n(100, 50), 50)
        self.assertEqual(self._n(50, 100), 50)

    def test_server_zero_returns_client(self):
        self.assertEqual(self._n(0, 50), 50)

    def test_client_zero_returns_server(self):
        self.assertEqual(self._n(100, 0), 100)

    def test_both_zero_returns_zero(self):
        self.assertEqual(self._n(0, 0), 0)

    def test_equal_values_returns_same(self):
        self.assertEqual(self._n(60, 60), 60)


class TestBuildClientProperties(unittest.TestCase):
    def setUp(self):
        self.props = channel0.Channel0._build_client_properties()

    def test_product_is_rabbitpy(self):
        self.assertEqual(self.props['product'], 'rabbitpy')

    def test_capabilities_present(self):
        self.assertIn('capabilities', self.props)

    def test_publisher_confirms_capability(self):
        self.assertTrue(self.props['capabilities']['publisher_confirms'])

    def test_version_present(self):
        self.assertIn('version', self.props)

    def test_platform_present(self):
        self.assertIn('platform', self.props)


# ---------------------------------------------------------------------------
# Happy-path negotiation
# ---------------------------------------------------------------------------


class TestHappyPathNegotiation(unittest.TestCase):
    def setUp(self):
        self.ch0, self.ev, self.exc = _make()
        self.mock_io = _mock_io()
        _load_happy_path(self.ch0)
        _run(self.ch0, self.mock_io)

    def _frames_written(self):
        return [c[0][1] for c in self.mock_io.write_frame.call_args_list]

    def test_open_is_true(self):
        self.assertTrue(self.ch0.open)

    def test_channel0_opened_event_set(self):
        self.assertTrue(self.ev.is_set(events.CHANNEL0_OPENED))

    def test_no_exceptions(self):
        self.assertTrue(self.exc.empty())

    def test_protocol_header_sent_first(self):
        self.assertIsInstance(self._frames_written()[0], header.ProtocolHeader)

    def test_start_ok_sent(self):
        frames = self._frames_written()
        self.assertTrue(
            any(isinstance(f, commands.Connection.StartOk) for f in frames)
        )

    def test_credentials_in_start_ok(self):
        frames = self._frames_written()
        start_ok = next(
            f for f in frames if isinstance(f, commands.Connection.StartOk)
        )
        self.assertIn('guest', start_ok.response)

    def test_tune_ok_sent(self):
        frames = self._frames_written()
        self.assertTrue(
            any(isinstance(f, commands.Connection.TuneOk) for f in frames)
        )

    def test_connection_open_sent(self):
        frames = self._frames_written()
        self.assertTrue(
            any(isinstance(f, commands.Connection.Open) for f in frames)
        )

    def test_virtual_host_in_open(self):
        frames = self._frames_written()
        open_frame = next(
            f for f in frames if isinstance(f, commands.Connection.Open)
        )
        self.assertEqual(open_frame.virtual_host, '/')

    def test_server_properties_stored(self):
        self.assertIn('capabilities', self.ch0.properties)

    def test_heartbeat_negotiated(self):
        # server=60, client=60 → min(60,60) = 60
        self.assertEqual(self.ch0.heartbeat_interval, 60)


# ---------------------------------------------------------------------------
# Heartbeat negotiation edge cases
# ---------------------------------------------------------------------------


class TestHeartbeatNegotiation(unittest.TestCase):
    def _negotiate(self, server_hb, client_hb, **extra_tune):
        ch0, _, _ = _make(heartbeat=client_hb)
        tune = commands.Connection.Tune(
            channel_max=extra_tune.get('channel_max', 0),
            frame_max=extra_tune.get('frame_max', 131072),
            heartbeat=server_hb,
        )
        for frame in (_START, tune, _OPEN_OK, None):
            ch0.pending_frames.put(frame)
        _run(ch0)
        return ch0

    def test_both_nonzero_takes_minimum(self):
        ch0 = self._negotiate(30, 60)
        self.assertEqual(ch0.heartbeat_interval, 30)

    def test_server_zero_disables(self):
        ch0 = self._negotiate(0, 60)
        self.assertEqual(ch0.heartbeat_interval, 0)

    def test_client_zero_disables(self):
        ch0 = self._negotiate(60, 0)
        self.assertEqual(ch0.heartbeat_interval, 0)

    def test_both_zero_disables(self):
        ch0 = self._negotiate(0, 0)
        self.assertEqual(ch0.heartbeat_interval, 0)

    def test_server_zero_frame_max_uses_client(self):
        ch0 = self._negotiate(60, 60, frame_max=0, channel_max=0)
        self.assertEqual(ch0.maximum_frame_size, 131072)
        self.assertEqual(ch0.maximum_channels, 65535)

    def test_server_smaller_frame_max_used(self):
        ch0 = self._negotiate(60, 60, frame_max=65536)
        self.assertEqual(ch0.maximum_frame_size, 65536)


# ---------------------------------------------------------------------------
# Negotiation error paths
# ---------------------------------------------------------------------------


class TestNegotiationErrors(unittest.TestCase):
    def _run_and_join(self, ch0_obj):
        _run(ch0_obj)

    def test_wrong_first_frame_raises_connection_exception(self):
        ch0, _ev, exc = _make()
        ch0.pending_frames.put(commands.Connection.Tune(0, 131072, 60))
        self._run_and_join(ch0)
        self.assertFalse(ch0.open)
        self.assertFalse(exc.empty())
        self.assertIsInstance(exc.get_nowait(), exceptions.ConnectionException)

    def test_wrong_tune_frame_raises_connection_exception(self):
        ch0, _ev, exc = _make()
        ch0.pending_frames.put(_START)
        ch0.pending_frames.put(commands.Connection.OpenOk())
        self._run_and_join(ch0)
        self.assertFalse(ch0.open)
        self.assertFalse(exc.empty())
        self.assertIsInstance(exc.get_nowait(), exceptions.ConnectionException)

    def test_wrong_open_ok_frame_raises_connection_exception(self):
        ch0, _ev, exc = _make()
        ch0.pending_frames.put(_START)
        ch0.pending_frames.put(_TUNE)
        ch0.pending_frames.put(_TUNE)  # wrong type
        self._run_and_join(ch0)
        self.assertFalse(ch0.open)
        self.assertFalse(exc.empty())

    def test_pre_injected_exception_propagates(self):
        ch0, _ev, exc = _make()
        exc.put(exceptions.ConnectionException('pre-injected'))
        self._run_and_join(ch0)
        self.assertFalse(ch0.open)
        self.assertFalse(exc.empty())

    def test_exception_raised_event_set_on_error(self):
        ch0, ev, _exc = _make()
        ch0.pending_frames.put(commands.Connection.Tune(0, 0, 0))
        self._run_and_join(ch0)
        self.assertTrue(ev.is_set(events.EXCEPTION_RAISED))

    def test_channel0_opened_not_set_on_error(self):
        ch0, ev, _exc = _make()
        ch0.pending_frames.put(commands.Connection.Tune(0, 0, 0))
        self._run_and_join(ch0)
        self.assertFalse(ev.is_set(events.CHANNEL0_OPENED))


# ---------------------------------------------------------------------------
# Runtime frames (post-negotiation)
# ---------------------------------------------------------------------------


class TestRuntimeFrames(unittest.TestCase):
    def _open(self, extra_frames):
        ch0, ev, exc = _make()
        for frame in (_START, _TUNE, _OPEN_OK, *extra_frames, None):
            ch0.pending_frames.put(frame)
        _run(ch0)
        return ch0, ev, exc

    def test_connection_blocked_sets_event(self):
        _, ev, _ = self._open(
            [commands.Connection.Blocked(reason='memory alarm')]
        )
        self.assertTrue(ev.is_set(events.CONNECTION_BLOCKED))

    def test_connection_unblocked_clears_event(self):
        _, ev, _ = self._open(
            [
                commands.Connection.Blocked(reason='memory alarm'),
                commands.Connection.Unblocked(),
            ]
        )
        self.assertFalse(ev.is_set(events.CONNECTION_BLOCKED))

    def test_server_close_queues_exception(self):
        ch0, _, exc = self._open(
            [
                commands.Connection.Close(
                    reply_code=320,
                    reply_text='CONNECTION_FORCED',
                    class_id=0,
                    method_id=0,
                )
            ]
        )
        self.assertFalse(ch0.open)
        self.assertFalse(exc.empty())

    def test_server_close_maps_known_reply_code(self):
        _ch0, _, exc = self._open(
            [
                commands.Connection.Close(
                    reply_code=403,
                    reply_text='ACCESS_REFUSED',
                    class_id=0,
                    method_id=0,
                )
            ]
        )
        err = exc.get_nowait()
        self.assertIsInstance(err, exceptions.AMQPAccessRefused)


# ---------------------------------------------------------------------------
# Heartbeat and close
# ---------------------------------------------------------------------------


class TestSendHeartbeat(unittest.TestCase):
    def test_sends_heartbeat_frame(self):
        ch0, _, _ = _make()
        mock_io = _mock_io()
        ch0._io = mock_io
        ch0.send_heartbeat()
        mock_io.write_frame.assert_called_once()
        args = mock_io.write_frame.call_args[0]
        self.assertEqual(args[0], 0)
        self.assertIsInstance(args[1], pamqp.heartbeat.Heartbeat)

    def test_no_io_does_not_raise(self):
        ch0, _, _ = _make()
        # _io is None — should succeed silently
        ch0.send_heartbeat()

    def test_close_when_open_sends_frame(self):
        ch0, _, _ = _make()
        mock_io = _mock_io()
        ch0._io = mock_io
        ch0._open = True
        ch0.close()
        mock_io.write_frame.assert_called_once()
        close_frame = mock_io.write_frame.call_args[0][1]
        self.assertIsInstance(close_frame, commands.Connection.Close)

    def test_close_marks_not_open(self):
        ch0, _, _ = _make()
        ch0._io = _mock_io()
        ch0._open = True
        ch0.close()
        self.assertFalse(ch0.open)

    def test_close_when_not_open_does_not_write(self):
        ch0, _, _ = _make()
        mock_io = _mock_io()
        ch0._io = mock_io
        ch0._open = False
        ch0.close()
        mock_io.write_frame.assert_not_called()
