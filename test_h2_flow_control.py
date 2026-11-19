"""Unit tests for the HTTP/2 flow-control simulator."""

import unittest

from h2_flow_control import (
    DATA,
    DEFAULT_INITIAL_WINDOW_SIZE,
    END_STREAM,
    HEADERS,
    MAX_WINDOW_SIZE,
    RST_STREAM,
    SETTINGS,
    WINDOW_UPDATE,
    Event,
    FlowControlSimulator,
    SCOPE_CONNECTION,
    SCOPE_STREAM,
)


def open_sim(*stream_ids, **kwargs):
    sim = FlowControlSimulator(**kwargs)
    for sid in stream_ids:
        result = sim.apply(Event(HEADERS, stream_id=sid))
        assert result.error is None, result.error
    return sim


class WindowShrinkTest(unittest.TestCase):
    """SETTINGS_INITIAL_WINDOW_SIZE shrinking existing streams."""

    def test_shrink_applies_to_all_open_streams(self):
        sim = open_sim(1, 3)
        result = sim.apply(Event(SETTINGS, initial_window_size=16384))
        self.assertIsNone(result.error)
        delta = 16384 - DEFAULT_INITIAL_WINDOW_SIZE
        expected = DEFAULT_INITIAL_WINDOW_SIZE + delta
        self.assertEqual(result.stream_windows, {1: expected, 3: expected})
        self.assertEqual(result.connection_window,
                         DEFAULT_INITIAL_WINDOW_SIZE)

    def test_shrink_can_drive_window_negative(self):
        sim = open_sim(1)
        sim.apply(Event(DATA, stream_id=1, size=60000))
        result = sim.apply(Event(SETTINGS, initial_window_size=0))
        self.assertIsNone(result.error)
        # 65535 - 60000 - 65535 = -60000: negative is legal (RFC 6.9.2).
        self.assertEqual(result.stream_windows[1], -60000)
        self.assertEqual(result.blocked_streams, [1])

    def test_shrink_does_not_touch_closed_streams(self):
        sim = open_sim(1, 3)
        sim.apply(Event(RST_STREAM, stream_id=3))
        result = sim.apply(Event(SETTINGS, initial_window_size=1024))
        self.assertIsNone(result.error)
        self.assertNotIn(3, result.stream_windows)
        self.assertEqual(result.stream_windows[1], 1024)

    def test_new_streams_use_latest_initial_window(self):
        sim = open_sim(1)
        sim.apply(Event(SETTINGS, initial_window_size=100))
        result = sim.apply(Event(HEADERS, stream_id=3))
        self.assertEqual(result.stream_windows, {1: 100, 3: 100})


class NegativeWindowRecoveryTest(unittest.TestCase):
    """A negative window must be repaid before DATA can flow again."""

    def test_negative_window_recovery(self):
        sim = open_sim(1)
        sim.apply(Event(DATA, stream_id=1, size=65535))
        # Refill the connection window so only the stream window matters.
        sim.apply(Event(WINDOW_UPDATE, stream_id=0, increment=65535))
        sim.apply(Event(SETTINGS, initial_window_size=0))
        self.assertEqual(sim.stream_windows()[1], -65535)

        # Partial repayment: still negative, still blocked.
        result = sim.apply(Event(WINDOW_UPDATE, stream_id=1,
                                 increment=30000))
        self.assertEqual(result.stream_windows[1], -35535)
        self.assertEqual(result.blocked_streams, [1])
        self.assertEqual(sim.sendable(1), 0)

        # Full repayment plus headroom: unblocked again.
        result = sim.apply(Event(WINDOW_UPDATE, stream_id=1,
                                 increment=40000))
        self.assertEqual(result.stream_windows[1], 4465)
        self.assertEqual(result.blocked_streams, [])
        self.assertEqual(sim.sendable(1), 4465)

    def test_connection_window_blocks_even_with_stream_credit(self):
        sim = open_sim(1)
        sim.apply(Event(DATA, stream_id=1, size=65535))
        # Stream window is 0 and connection window is 0.
        self.assertEqual(sim.blocked_streams(), [1])
        result = sim.apply(Event(WINDOW_UPDATE, stream_id=1,
                                 increment=1000))
        # Stream has credit but the connection window is still empty.
        self.assertEqual(result.stream_windows[1], 1000)
        self.assertEqual(result.blocked_streams, [1])
        result = sim.apply(Event(WINDOW_UPDATE, stream_id=0,
                                 increment=500))
        self.assertEqual(result.blocked_streams, [])
        self.assertEqual(sim.sendable(1), 500)


class OversizedIncrementTest(unittest.TestCase):
    """WINDOW_UPDATE increment validation."""

    def test_stream_window_overflow_is_stream_error(self):
        sim = open_sim(1, 3)
        result = sim.apply(Event(WINDOW_UPDATE, stream_id=1,
                                 increment=MAX_WINDOW_SIZE))
        self.assertEqual(result.error_scope, SCOPE_STREAM)
        self.assertIn("FLOW_CONTROL_ERROR", result.error)
        # Only stream 1 is reset; stream 3 is untouched.
        self.assertNotIn(1, result.stream_windows)
        self.assertEqual(result.stream_windows[3],
                         DEFAULT_INITIAL_WINDOW_SIZE)
        self.assertIsNone(sim.apply(Event(DATA, stream_id=3,
                                          size=10)).error)

    def test_connection_window_overflow_is_connection_error(self):
        sim = open_sim(1)
        result = sim.apply(Event(WINDOW_UPDATE, stream_id=0,
                                 increment=MAX_WINDOW_SIZE))
        self.assertEqual(result.error_scope, SCOPE_CONNECTION)
        # Connection is dead: later events are refused without effect.
        followup = sim.apply(Event(DATA, stream_id=1, size=1))
        self.assertEqual(followup.error_scope, SCOPE_CONNECTION)
        self.assertIn("connection already closed", followup.error)
        self.assertEqual(followup.stream_windows[1],
                         DEFAULT_INITIAL_WINDOW_SIZE)

    def test_zero_increment_on_stream_is_stream_error(self):
        sim = open_sim(1, 3)
        result = sim.apply(Event(WINDOW_UPDATE, stream_id=1, increment=0))
        self.assertEqual(result.error_scope, SCOPE_STREAM)
        self.assertIn("PROTOCOL_ERROR", result.error)
        self.assertNotIn(1, result.stream_windows)
        self.assertIn(3, result.stream_windows)

    def test_zero_increment_on_connection_is_connection_error(self):
        sim = open_sim(1)
        result = sim.apply(Event(WINDOW_UPDATE, stream_id=0, increment=0))
        self.assertEqual(result.error_scope, SCOPE_CONNECTION)
        self.assertIn("PROTOCOL_ERROR", result.error)

    def test_maximum_legal_increment_is_accepted(self):
        sim = open_sim(1)
        sim.apply(Event(DATA, stream_id=1, size=65535))
        increment = MAX_WINDOW_SIZE
        result = sim.apply(Event(WINDOW_UPDATE, stream_id=1,
                                 increment=increment))
        self.assertIsNone(result.error)
        self.assertEqual(result.stream_windows[1], MAX_WINDOW_SIZE)


class StreamResetTest(unittest.TestCase):
    """RST_STREAM / END_STREAM lifecycle handling."""

    def test_rst_stream_releases_the_stream(self):
        sim = open_sim(1, 3)
        sim.apply(Event(DATA, stream_id=1, size=65535))
        sim.apply(Event(WINDOW_UPDATE, stream_id=0, increment=65535))
        sim.apply(Event(DATA, stream_id=3, size=65535))
        self.assertEqual(sim.blocked_streams(), [1, 3])
        result = sim.apply(Event(RST_STREAM, stream_id=1))
        self.assertIsNone(result.error)
        self.assertEqual(result.stream_windows,
                         {3: 0})
        # The closed stream no longer shows up as blocked.
        self.assertEqual(result.blocked_streams, [3])

    def test_events_on_reset_stream_are_stream_errors(self):
        sim = open_sim(1, 3)
        sim.apply(Event(RST_STREAM, stream_id=1))
        result = sim.apply(Event(DATA, stream_id=1, size=1))
        self.assertEqual(result.error_scope, SCOPE_STREAM)
        self.assertIn("STREAM_CLOSED", result.error)
        # Other streams keep working undisturbed.
        result = sim.apply(Event(DATA, stream_id=3, size=100))
        self.assertIsNone(result.error)
        self.assertEqual(result.stream_windows[3],
                         DEFAULT_INITIAL_WINDOW_SIZE - 100)

    def test_end_stream_stops_sending_but_keeps_window_tracking(self):
        sim = open_sim(1)
        result = sim.apply(Event(END_STREAM, stream_id=1))
        self.assertIsNone(result.error)
        self.assertEqual(sim.sendable(1), 0)
        self.assertEqual(result.blocked_streams, [])
        # Half-closed streams still receive SETTINGS deltas (not closed).
        result = sim.apply(Event(SETTINGS, initial_window_size=100))
        self.assertEqual(result.stream_windows[1], 100)
        # But DATA after END_STREAM is a stream error.
        result = sim.apply(Event(DATA, stream_id=1, size=1))
        self.assertEqual(result.error_scope, SCOPE_STREAM)
        self.assertIn("STREAM_CLOSED", result.error)


class InterleavedUpdateTest(unittest.TestCase):
    """Interleaved DATA/WINDOW_UPDATE across several streams."""

    def test_interleaved_events_keep_consistent_accounting(self):
        sim = open_sim(1, 3, 5)
        results = sim.run([
            Event(DATA, stream_id=1, size=10000),
            Event(DATA, stream_id=3, size=20000),
            Event(WINDOW_UPDATE, stream_id=0, increment=15000),
            Event(DATA, stream_id=5, size=5000),
            Event(WINDOW_UPDATE, stream_id=1, increment=4000),
            Event(DATA, stream_id=1, size=4000),
        ])
        for result in results:
            self.assertIsNone(result.error)
        final = results[-1]
        # Connection: 65535 - 10000 - 20000 + 15000 - 5000 - 4000.
        self.assertEqual(final.connection_window, 41535)
        self.assertEqual(final.stream_windows, {
            1: 65535 - 10000 + 4000 - 4000,
            3: 65535 - 20000,
            5: 65535 - 5000,
        })
        self.assertEqual(final.blocked_streams, [])

    def test_blocked_set_tracks_each_event(self):
        sim = open_sim(1, 3)
        result = sim.apply(Event(DATA, stream_id=1, size=65535))
        # Connection window is 0, so stream 3 is blocked as well.
        self.assertEqual(result.blocked_streams, [1, 3])
        result = sim.apply(Event(WINDOW_UPDATE, stream_id=0,
                                 increment=65535))
        self.assertEqual(result.connection_window, 65535)
        self.assertEqual(result.blocked_streams, [1])
        result = sim.apply(Event(DATA, stream_id=3, size=65535))
        # Connection window is drained again, so stream 3 blocks too.
        self.assertEqual(result.connection_window, 0)
        self.assertEqual(result.blocked_streams, [1, 3])
        result = sim.apply(Event(WINDOW_UPDATE, stream_id=0,
                                 increment=65535))
        self.assertEqual(result.blocked_streams, [1, 3])
        result = sim.apply(Event(WINDOW_UPDATE, stream_id=1,
                                 increment=65535))
        self.assertEqual(result.blocked_streams, [3])

    def test_data_exceeding_window_is_connection_error(self):
        sim = open_sim(1)
        result = sim.apply(Event(DATA, stream_id=1, size=65536))
        self.assertEqual(result.error_scope, SCOPE_CONNECTION)
        self.assertIn("FLOW_CONTROL_ERROR", result.error)
        # State was not mutated by the rejected frame.
        self.assertEqual(result.connection_window,
                         DEFAULT_INITIAL_WINDOW_SIZE)
        self.assertEqual(result.stream_windows[1],
                         DEFAULT_INITIAL_WINDOW_SIZE)


class OutOfOrderEventTest(unittest.TestCase):
    """Detection of out-of-order / invalid input event sequences."""

    def test_data_before_headers(self):
        sim = FlowControlSimulator()
        result = sim.apply(Event(DATA, stream_id=1, size=10))
        self.assertEqual(result.error_scope, SCOPE_CONNECTION)
        self.assertIn("idle", result.error)

    def test_window_update_before_headers(self):
        sim = FlowControlSimulator()
        result = sim.apply(Event(WINDOW_UPDATE, stream_id=7,
                                 increment=100))
        self.assertEqual(result.error_scope, SCOPE_CONNECTION)
        self.assertIn("idle", result.error)

    def test_non_increasing_stream_id(self):
        sim = open_sim(1, 3)
        result = sim.apply(Event(HEADERS, stream_id=1))
        self.assertEqual(result.error_scope, SCOPE_CONNECTION)
        self.assertIn("already exists", result.error)

    def test_decreasing_stream_id(self):
        sim = open_sim(5)
        result = sim.apply(Event(HEADERS, stream_id=3))
        self.assertEqual(result.error_scope, SCOPE_CONNECTION)
        self.assertIn("not increasing", result.error)

    def test_headers_on_reset_stream(self):
        sim = open_sim(1)
        sim.apply(Event(RST_STREAM, stream_id=1))
        result = sim.apply(Event(HEADERS, stream_id=1))
        self.assertEqual(result.error_scope, SCOPE_CONNECTION)
        self.assertIn("already exists", result.error)

    def test_unknown_event_type(self):
        sim = FlowControlSimulator()
        result = sim.apply(Event("PING"))
        self.assertEqual(result.error_scope, SCOPE_CONNECTION)
        self.assertIn("unknown event type", result.error)


class ConstructorValidationTest(unittest.TestCase):

    def test_invalid_initial_windows_rejected(self):
        with self.assertRaises(ValueError):
            FlowControlSimulator(initial_stream_window=-1)
        with self.assertRaises(ValueError):
            FlowControlSimulator(
                initial_stream_window=MAX_WINDOW_SIZE + 1)
        with self.assertRaises(ValueError):
            FlowControlSimulator(connection_window=-1)

    def test_invalid_settings_value_is_connection_error(self):
        sim = open_sim(1)
        result = sim.apply(Event(SETTINGS,
                                 initial_window_size=MAX_WINDOW_SIZE + 1))
        self.assertEqual(result.error_scope, SCOPE_CONNECTION)
        self.assertIn("FLOW_CONTROL_ERROR", result.error)


if __name__ == "__main__":
    unittest.main()
