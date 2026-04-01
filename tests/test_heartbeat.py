"""Unit tests for rabbitpy.heartbeat.Heartbeat."""
import unittest
from unittest import mock

from rabbitpy import heartbeat


def _make(interval=60):
    mock_io = mock.MagicMock(name='io.IO')
    mock_io.bytes_written = 0
    mock_ch0 = mock.MagicMock(name='channel0.Channel0')
    hb = heartbeat.Heartbeat(mock_io, mock_ch0, interval)
    return hb, mock_io, mock_ch0


class TestHeartbeatInit(unittest.TestCase):
    def test_interval_is_half_of_configured(self):
        hb, _, _ = _make(60)
        self.assertEqual(hb._interval, 30.0)

    def test_float_interval(self):
        hb, _, _ = _make(10)
        self.assertEqual(hb._interval, 5.0)

    def test_initial_stopped_false(self):
        hb, _, _ = _make()
        self.assertFalse(hb._stopped)

    def test_initial_timer_none(self):
        hb, _, _ = _make()
        self.assertIsNone(hb._timer)


class TestHeartbeatStartStop(unittest.TestCase):
    def test_start_creates_timer(self):
        hb, _, _ = _make()
        hb.start()
        try:
            self.assertIsNotNone(hb._timer)
            self.assertFalse(hb._stopped)
        finally:
            hb.stop()

    def test_stop_cancels_timer(self):
        hb, _, _ = _make()
        hb.start()
        hb.stop()
        self.assertIsNone(hb._timer)
        self.assertTrue(hb._stopped)

    def test_stop_without_start_is_safe(self):
        hb, _, _ = _make()
        hb.stop()  # should not raise
        self.assertTrue(hb._stopped)

    def test_double_stop_is_safe(self):
        hb, _, _ = _make()
        hb.start()
        hb.stop()
        hb.stop()


class TestHeartbeatDisabled(unittest.TestCase):
    def test_zero_interval_does_not_create_timer(self):
        hb, _, _ = _make(0)
        hb.start()
        self.assertIsNone(hb._timer)

    def test_zero_interval_start_does_not_raise(self):
        hb, _, _ = _make(0)
        hb.start()  # should not raise


class TestMaybeSend(unittest.TestCase):
    def setUp(self):
        self.hb, self.mock_io, self.mock_ch0 = _make(2)
        self.hb._stopped = False
        self.mock_io.bytes_written = 100
        self.hb._last_written = 100

    def test_sends_heartbeat_when_no_data_written(self):
        self.hb._maybe_send()
        self.mock_ch0.send_heartbeat.assert_called_once()

    def test_no_send_when_data_was_written(self):
        self.mock_io.bytes_written = 200
        self.hb._maybe_send()
        self.mock_ch0.send_heartbeat.assert_not_called()

    def test_stopped_skips_send(self):
        self.hb._stopped = True
        self.hb._maybe_send()
        self.mock_ch0.send_heartbeat.assert_not_called()

    def test_last_written_updated_after_check(self):
        self.mock_io.bytes_written = 200
        self.hb._maybe_send()
        self.assertEqual(self.hb._last_written, 200)

    def test_restarts_timer_after_send(self):
        with mock.patch.object(self.hb, '_start_timer') as mock_start:
            self.hb._maybe_send()
            mock_start.assert_called_once()

    def test_no_timer_restart_when_stopped_after_send(self):
        """If stopped is set after the first lock check, timer is not restarted."""
        original_send = self.mock_ch0.send_heartbeat

        def _stop_during_send():
            self.hb._stopped = True

        original_send.side_effect = _stop_during_send
        with mock.patch.object(self.hb, '_start_timer') as mock_start:
            self.hb._maybe_send()
            mock_start.assert_not_called()
