"""Unit tests for rabbitpy.connection.Connection."""
import queue
import unittest
from unittest import mock

from rabbitpy import connection, events, exceptions


_DEFAULT_URL = 'amqp://guest:guest@localhost:5672/%2F'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_conn(url=_DEFAULT_URL, **kwargs):
    with mock.patch('rabbitpy.connection.Connection.connect'):
        return connection.Connection(url, **kwargs)


def _make_mock_io(events_obj, *, signal_opened=True):
    m = mock.MagicMock(name='io.IO')
    m.write_trigger = mock.MagicMock()
    m.write_queue = queue.Queue()
    if signal_opened:
        def _start():
            events_obj.set(events.SOCKET_OPENED)
        m.start.side_effect = _start
    return m


def _make_mock_ch0(events_obj, *, signal_opened=True, exc_queue=None,
                   exception=None):
    m = mock.MagicMock(name='channel0.Channel0')
    m.pending_frames = queue.Queue()
    m.open = True
    m.heartbeat_interval = 0
    m.maximum_frame_size = 131072
    m.maximum_channels = 65535
    m.properties = {}
    if signal_opened and exception is None:
        def _start(io):
            events_obj.set(events.CHANNEL0_OPENED)
        m.start.side_effect = _start
    elif exception is not None and exc_queue is not None:
        def _start_exc(io):
            exc_queue.put(exception)
        m.start.side_effect = _start_exc
    return m


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestConnectionInit(unittest.TestCase):
    def test_default_url_host(self):
        conn = _make_conn()
        self.assertEqual(conn._args['host'], 'localhost')

    def test_default_url_port(self):
        conn = _make_conn()
        self.assertEqual(conn._args['port'], 5672)

    def test_custom_url_parsed(self):
        conn = _make_conn('amqp://user:pass@myhost:1234/vhost')
        self.assertEqual(conn._args['host'], 'myhost')
        self.assertEqual(conn._args['username'], 'user')
        self.assertEqual(conn._args['password'], 'pass')

    def test_initial_state_is_closed(self):
        conn = _make_conn()
        self.assertTrue(conn.is_closed)

    def test_custom_connection_name(self):
        conn = _make_conn(connection_name='my-conn')
        self.assertEqual(conn._name, 'my-conn')

    def test_channels_dict_empty(self):
        conn = _make_conn()
        self.assertEqual(conn._channels, {})

    def test_freed_ids_set_empty(self):
        conn = _make_conn()
        self.assertEqual(conn._freed_channel_ids, set())


# ---------------------------------------------------------------------------
# connect() lifecycle
# ---------------------------------------------------------------------------

class TestConnectionConnect(unittest.TestCase):
    def setUp(self):
        self.conn = _make_conn()

    @mock.patch('rabbitpy.connection.io.IO')
    @mock.patch('rabbitpy.connection.channel0.Channel0')
    def test_happy_path_state_is_open(self, mock_ch0_cls, mock_io_cls):
        mock_io_cls.return_value = _make_mock_io(self.conn._events)
        mock_ch0_cls.return_value = _make_mock_ch0(self.conn._events)
        self.conn.connect()
        self.assertTrue(self.conn.is_open)

    @mock.patch('rabbitpy.connection.io.IO')
    @mock.patch('rabbitpy.connection.channel0.Channel0')
    def test_context_manager_opens_and_closes(self, mock_ch0_cls, mock_io_cls):
        mock_io = _make_mock_io(self.conn._events)
        mock_io_cls.return_value = mock_io
        mock_ch0_cls.return_value = _make_mock_ch0(self.conn._events)
        with self.conn:
            self.assertTrue(self.conn.is_open)
        self.assertTrue(self.conn.is_closed)

    @mock.patch('rabbitpy.connection.io.IO')
    @mock.patch('rabbitpy.connection.channel0.Channel0')
    def test_socket_timeout_raises_runtime_error(self, mock_ch0_cls,
                                                 mock_io_cls):
        mock_io_cls.return_value = _make_mock_io(
            self.conn._events, signal_opened=False
        )
        mock_ch0_cls.return_value = mock.MagicMock()
        with self.assertRaises(RuntimeError):
            self.conn.connect()

    @mock.patch('rabbitpy.connection.io.IO')
    @mock.patch('rabbitpy.connection.channel0.Channel0')
    def test_socket_exception_propagates(self, mock_ch0_cls, mock_io_cls):
        exc = exceptions.ConnectionException('refused')

        def _start():
            self.conn._exceptions.put(exc)

        mock_io = mock.MagicMock()
        mock_io.start.side_effect = _start
        mock_io_cls.return_value = mock_io
        mock_ch0_cls.return_value = mock.MagicMock()
        with self.assertRaises(exceptions.ConnectionException):
            self.conn.connect()

    @mock.patch('rabbitpy.connection.time')
    @mock.patch('rabbitpy.connection.io.IO')
    @mock.patch('rabbitpy.connection.channel0.Channel0')
    def test_negotiation_timeout_raises_runtime_error(
        self, mock_ch0_cls, mock_io_cls, mock_time
    ):
        mock_io_cls.return_value = _make_mock_io(self.conn._events)
        mock_ch0 = mock.MagicMock()
        mock_ch0.pending_frames = queue.Queue()
        mock_ch0_cls.return_value = mock_ch0
        # Deadline expires immediately on second monotonic() call
        mock_time.monotonic.side_effect = [0.0, 9999.0]
        mock_time.sleep = mock.MagicMock()
        with self.assertRaises(RuntimeError):
            self.conn.connect()
        self.assertTrue(self.conn.is_closed)

    @mock.patch('rabbitpy.connection.io.IO')
    @mock.patch('rabbitpy.connection.channel0.Channel0')
    def test_negotiation_exception_propagates(self, mock_ch0_cls, mock_io_cls):
        mock_io_cls.return_value = _make_mock_io(self.conn._events)
        exc = exceptions.AMQPAccessRefused('access refused')
        mock_ch0_cls.return_value = _make_mock_ch0(
            self.conn._events,
            signal_opened=False,
            exc_queue=self.conn._exceptions,
            exception=exc,
        )
        with self.assertRaises(exceptions.AMQPAccessRefused):
            self.conn.connect()
        self.assertTrue(self.conn.is_closed)

    @mock.patch('rabbitpy.connection.io.IO')
    @mock.patch('rabbitpy.connection.channel0.Channel0')
    def test_heartbeat_started_when_interval_nonzero(
        self, mock_ch0_cls, mock_io_cls
    ):
        mock_io_cls.return_value = _make_mock_io(self.conn._events)
        mock_ch0 = _make_mock_ch0(self.conn._events)
        mock_ch0.heartbeat_interval = 60
        mock_ch0_cls.return_value = mock_ch0
        with mock.patch(
            'rabbitpy.connection.heartbeat_mod.Heartbeat'
        ) as mock_hb_cls:
            mock_hb_cls.return_value = mock.MagicMock()
            self.conn.connect()
            mock_hb_cls.return_value.start.assert_called_once()

    @mock.patch('rabbitpy.connection.io.IO')
    @mock.patch('rabbitpy.connection.channel0.Channel0')
    def test_heartbeat_not_started_when_interval_zero(
        self, mock_ch0_cls, mock_io_cls
    ):
        mock_io_cls.return_value = _make_mock_io(self.conn._events)
        mock_ch0_cls.return_value = _make_mock_ch0(self.conn._events)
        with mock.patch(
            'rabbitpy.connection.heartbeat_mod.Heartbeat'
        ) as mock_hb_cls:
            self.conn.connect()
            mock_hb_cls.assert_not_called()


# ---------------------------------------------------------------------------
# channel() method
# ---------------------------------------------------------------------------

class TestConnectionChannelMethod(unittest.TestCase):
    """Tests for Connection.channel() using a pre-opened mock connection.

    Channel.open() is overridden to set state=OPEN without doing any I/O,
    so that the channel lifecycle tracking in the connection works correctly.
    """

    def setUp(self):
        from rabbitpy import channel as channel_mod
        self.conn = _make_conn()
        self.conn._set_state(self.conn.OPEN)
        mock_io = mock.MagicMock()
        mock_io.write_trigger = mock.MagicMock()
        mock_io.write_queue = queue.Queue()
        self.conn._io = mock_io
        mock_ch0 = mock.MagicMock()
        mock_ch0.maximum_frame_size = 131072
        mock_ch0.maximum_channels = 65535
        mock_ch0.properties = {}
        self.conn._channel0 = mock_ch0
        # Override open() so channels reach OPEN state without real I/O
        self._orig_open = channel_mod.Channel.open
        channel_mod.Channel.open = lambda self: self._set_state(self.OPEN)

    def tearDown(self):
        from rabbitpy import channel as channel_mod
        channel_mod.Channel.open = self._orig_open

    def test_channel_returns_channel_instance(self):
        from rabbitpy import channel as channel_mod
        ch = self.conn.channel()
        self.assertIsInstance(ch, channel_mod.Channel)

    def test_first_channel_id_is_one(self):
        ch = self.conn.channel()
        self.assertEqual(ch.id, 1)

    def test_second_channel_id_is_two(self):
        self.conn.channel()
        ch2 = self.conn.channel()
        self.assertEqual(ch2.id, 2)

    def test_channel_registered_in_channels(self):
        ch = self.conn.channel()
        self.assertIn(ch.id, self.conn._channels)

    def test_channel_registered_in_io(self):
        self.conn.channel()
        self.conn._io.add_channel.assert_called()

    def test_channel_raises_when_connection_closed(self):
        self.conn._set_state(self.conn.CLOSED)
        with self.assertRaises(exceptions.ConnectionClosed):
            self.conn.channel()


# ---------------------------------------------------------------------------
# Channel ID reuse (#121) — tested via _get_next_channel_id directly so that
# we can inject mock channels with controlled closed/open states.
# ---------------------------------------------------------------------------

class TestChannelIdReuse(unittest.TestCase):
    def setUp(self):
        self.conn = _make_conn()
        self.conn._set_state(self.conn.OPEN)
        mock_ch0 = mock.MagicMock()
        mock_ch0.maximum_channels = 65535
        self.conn._channel0 = mock_ch0

    def _mock_chan(self, closed=False):
        m = mock.MagicMock()
        m.closed = closed
        return m

    def _next_id(self):
        with self.conn._channel_lock:
            return self.conn._get_next_channel_id()

    def test_first_id_is_one_when_no_channels(self):
        self.assertEqual(self._next_id(), 1)

    def test_id_increments_when_channels_open(self):
        self.conn._channels[1] = self._mock_chan(closed=False)
        self.conn._channels[2] = self._mock_chan(closed=False)
        self.assertEqual(self._next_id(), 3)

    def test_closed_channel_id_is_reclaimed(self):
        self.conn._channels[1] = self._mock_chan(closed=True)
        self.conn._channels[2] = self._mock_chan(closed=False)
        self.assertEqual(self._next_id(), 1)

    def test_all_closed_ids_reclaimed_before_new_allocation(self):
        self.conn._channels[1] = self._mock_chan(closed=True)
        self.conn._channels[2] = self._mock_chan(closed=True)
        first = self._next_id()
        self.assertIn(first, {1, 2})

    def test_closed_channel_removed_from_dict_during_cleanup(self):
        self.conn._channels[1] = self._mock_chan(closed=True)
        self._next_id()
        self.assertNotIn(1, self.conn._channels)

    def test_too_many_channels_raises(self):
        self.conn._channel0.maximum_channels = 2
        self.conn._channels[1] = self._mock_chan(closed=False)
        self.conn._channels[2] = self._mock_chan(closed=False)
        with self.assertRaises(exceptions.TooManyChannelsError):
            self._next_id()


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

class TestConnectionProperties(unittest.TestCase):
    def test_capabilities_empty_without_channel0(self):
        conn = _make_conn()
        conn._channel0 = None
        self.assertEqual(conn.capabilities, {})

    def test_server_properties_empty_without_channel0(self):
        conn = _make_conn()
        conn._channel0 = None
        self.assertEqual(conn.server_properties, {})

    def test_capabilities_from_channel0(self):
        conn = _make_conn()
        m = mock.MagicMock()
        m.properties = {'capabilities': {'basic.nack': True}}
        conn._channel0 = m
        self.assertEqual(conn.capabilities, {'basic.nack': True})

    def test_server_properties_from_channel0(self):
        conn = _make_conn()
        m = mock.MagicMock()
        m.properties = {'version': '3.12'}
        conn._channel0 = m
        self.assertEqual(conn.server_properties, {'version': '3.12'})

    def test_blocked_false_when_event_not_set(self):
        conn = _make_conn()
        self.assertFalse(conn.blocked)

    def test_blocked_true_when_event_set(self):
        conn = _make_conn()
        conn._events.set(events.CONNECTION_BLOCKED)
        self.assertTrue(conn.blocked)
