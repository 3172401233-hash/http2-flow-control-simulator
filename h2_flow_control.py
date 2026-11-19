"""Event-driven HTTP/2 flow-control state simulator.

This module simulates the *state machine* of HTTP/2 flow control
(RFC 7540, sections 5.2, 6.5 and 6.9).  It does NOT encode or decode
frames on the wire; it only tracks, for a sending endpoint:

* the connection-level send window,
* the per-stream send window of every active stream,
* how SETTINGS_INITIAL_WINDOW_SIZE changes shift stream windows
  (including into negative territory),
* which streams are currently blocked on flow control,
* protocol errors, scoped so that a stream error only kills the
  affected stream while a connection error stops everything.

Typical usage::

    sim = FlowControlSimulator()
    results = sim.run([
        Event(HEADERS, stream_id=1),
        Event(DATA, stream_id=1, size=16384),
        Event(WINDOW_UPDATE, stream_id=0, increment=16384),
    ])
    for step in results:
        print(step)
"""

from dataclasses import dataclass
from typing import Dict, List, Optional

# RFC 7540, section 6.5.2 / 6.9.2: windows and increments are 31-bit.
MAX_WINDOW_SIZE = (1 << 31) - 1
# RFC 7540, section 6.9.2: default initial window for streams and the
# connection-level window.
DEFAULT_INITIAL_WINDOW_SIZE = 65535

# Event types.
HEADERS = "HEADERS"
DATA = "DATA"
WINDOW_UPDATE = "WINDOW_UPDATE"
SETTINGS = "SETTINGS"
RST_STREAM = "RST_STREAM"
END_STREAM = "END_STREAM"

# Stream lifecycle states (send-side view).
STATE_OPEN = "open"
STATE_HALF_CLOSED_LOCAL = "half_closed_local"
STATE_CLOSED = "closed"

# Error scopes.
SCOPE_STREAM = "stream"
SCOPE_CONNECTION = "connection"


class ConnectionError_(Exception):
    """Connection-level protocol error: the whole connection stops."""


class StreamError_(Exception):
    """Stream-level protocol error: only the affected stream is reset."""

    def __init__(self, stream_id, reason):
        super().__init__(reason)
        self.stream_id = stream_id
        self.reason = reason


@dataclass
class Event:
    """One input event for the simulator.

    * HEADERS:        opens ``stream_id`` (must be new and increasing).
    * DATA:           consumes ``size`` bytes of both windows.
    * WINDOW_UPDATE:  adds ``increment`` to the window of ``stream_id``
                      (``stream_id=0`` targets the connection window).
    * SETTINGS:       sets SETTINGS_INITIAL_WINDOW_SIZE to
                      ``initial_window_size``; the delta is applied to
                      every stream that is not closed.
    * RST_STREAM:     closes ``stream_id`` immediately.
    * END_STREAM:     half-closes ``stream_id`` (local side done sending).
    """

    type: str
    stream_id: int = 0
    size: int = 0
    increment: int = 0
    initial_window_size: Optional[int] = None


@dataclass
class StepResult:
    """Observable state after one event has been applied."""

    event: Event
    connection_window: int
    stream_windows: Dict[int, int]
    blocked_streams: List[int]
    error: Optional[str] = None
    error_scope: Optional[str] = None

    def __str__(self):
        parts = [
            "event=%s" % self.event.type,
            "conn_window=%d" % self.connection_window,
            "streams=%s" % (self.stream_windows or "{}"),
            "blocked=%s" % (self.blocked_streams or "[]"),
        ]
        if self.error:
            parts.append("error[%s]=%s" % (self.error_scope, self.error))
        return " ".join(parts)


@dataclass
class _Stream:
    stream_id: int
    window: int
    state: str = STATE_OPEN


class FlowControlSimulator:
    """Simulates send-side HTTP/2 flow-control windows event by event."""

    def __init__(self,
                 connection_window=DEFAULT_INITIAL_WINDOW_SIZE,
                 initial_stream_window=DEFAULT_INITIAL_WINDOW_SIZE):
        if not 0 <= initial_stream_window <= MAX_WINDOW_SIZE:
            raise ValueError("initial stream window out of range")
        if not 0 <= connection_window <= MAX_WINDOW_SIZE:
            raise ValueError("connection window out of range")
        self.connection_window = connection_window
        self.initial_stream_window = initial_stream_window
        self._streams = {}          # stream_id -> _Stream (not closed)
        self._closed_streams = set()
        self._max_stream_id = 0
        self._connection_error = None

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------
    def run(self, events):
        """Apply a sequence of events, returning one StepResult each."""
        return [self.apply(event) for event in events]

    def apply(self, event):
        """Apply a single event and return the resulting StepResult."""
        error = None
        scope = None
        if self._connection_error is not None:
            # Connection already torn down: refuse to touch any state so
            # the failure cannot pollute other streams' accounting.
            error = "connection already closed: %s" % self._connection_error
            scope = SCOPE_CONNECTION
        else:
            try:
                self._dispatch(event)
            except StreamError_ as exc:
                # Stream error: reset only the affected stream.
                self._reset_stream(exc.stream_id)
                error = str(exc)
                scope = SCOPE_STREAM
            except ConnectionError_ as exc:
                # Connection error: stop everything.
                self._connection_error = str(exc)
                error = str(exc)
                scope = SCOPE_CONNECTION
        return StepResult(
            event=event,
            connection_window=self.connection_window,
            stream_windows=self.stream_windows(),
            blocked_streams=self.blocked_streams(),
            error=error,
            error_scope=scope,
        )

    def stream_windows(self):
        """Current send window of every non-closed stream."""
        return {sid: s.window for sid, s in sorted(self._streams.items())}

    def sendable(self, stream_id):
        """Bytes the endpoint may send on ``stream_id`` right now."""
        stream = self._streams.get(stream_id)
        if stream is None or stream.state != STATE_OPEN:
            return 0
        return max(0, min(self.connection_window, stream.window))

    def blocked_streams(self):
        """Open streams that cannot send any DATA byte right now."""
        return [sid for sid in sorted(self._streams)
                if self._streams[sid].state == STATE_OPEN
                and min(self.connection_window,
                        self._streams[sid].window) <= 0]

    # ------------------------------------------------------------------
    # event dispatch
    # ------------------------------------------------------------------
    def _dispatch(self, event):
        handler = {
            HEADERS: self._on_headers,
            DATA: self._on_data,
            WINDOW_UPDATE: self._on_window_update,
            SETTINGS: self._on_settings,
            RST_STREAM: self._on_rst_stream,
            END_STREAM: self._on_end_stream,
        }.get(event.type)
        if handler is None:
            raise ConnectionError_(
                "PROTOCOL_ERROR: unknown event type %r" % (event.type,))
        handler(event)

    def _on_headers(self, event):
        sid = event.stream_id
        if sid <= 0:
            raise ConnectionError_(
                "PROTOCOL_ERROR: HEADERS requires a stream id > 0")
        if sid in self._closed_streams or sid in self._streams:
            raise ConnectionError_(
                "PROTOCOL_ERROR: stream %d already exists" % sid)
        if sid < self._max_stream_id:
            # Out-of-order input: stream ids must be monotonically
            # increasing on a connection.
            raise ConnectionError_(
                "PROTOCOL_ERROR: stream id %d is not increasing "
                "(already saw %d)" % (sid, self._max_stream_id))
        self._max_stream_id = sid
        self._streams[sid] = _Stream(sid, self.initial_stream_window)

    def _on_data(self, event):
        stream = self._require_stream(event.stream_id)
        if stream.state == STATE_HALF_CLOSED_LOCAL:
            raise StreamError_(
                stream.stream_id,
                "STREAM_CLOSED: DATA after END_STREAM on stream %d"
                % stream.stream_id)
        if event.size < 0:
            raise ConnectionError_(
                "PROTOCOL_ERROR: negative DATA size %d" % event.size)
        available = min(self.connection_window, stream.window)
        if event.size > available:
            # Sending more than the window allows is a connection-level
            # flow-control violation (RFC 7540, 6.9.1).
            raise ConnectionError_(
                "FLOW_CONTROL_ERROR: DATA of %d bytes on stream %d "
                "exceeds sendable window %d"
                % (event.size, stream.stream_id, available))
        stream.window -= event.size
        self.connection_window -= event.size

    def _on_window_update(self, event):
        increment = event.increment
        if increment < 0 or increment > MAX_WINDOW_SIZE:
            raise ConnectionError_(
                "PROTOCOL_ERROR: invalid WINDOW_UPDATE increment %d"
                % increment)
        if event.stream_id == 0:
            if increment == 0:
                # RFC 7540, 6.9: zero increment on the connection
                # window is a connection error.
                raise ConnectionError_(
                    "PROTOCOL_ERROR: WINDOW_UPDATE with zero increment "
                    "on the connection")
            self.connection_window = self._checked_grow(
                self.connection_window, increment, "connection")
            return
        stream = self._require_stream(event.stream_id)
        if increment == 0:
            # Zero increment on a stream is a stream error only.
            raise StreamError_(
                stream.stream_id,
                "PROTOCOL_ERROR: WINDOW_UPDATE with zero increment "
                "on stream %d" % stream.stream_id)
        try:
            stream.window = self._checked_grow(
                stream.window, increment,
                "stream %d" % stream.stream_id)
        except ConnectionError_ as exc:
            # Overflow of a *stream* window is a stream error, not a
            # connection error (RFC 7540, 6.9.1).
            raise StreamError_(
                stream.stream_id,
                str(exc).replace("PROTOCOL_ERROR", "FLOW_CONTROL_ERROR", 1))

    def _on_settings(self, event):
        new_size = event.initial_window_size
        if new_size is None or not 0 <= new_size <= MAX_WINDOW_SIZE:
            raise ConnectionError_(
                "FLOW_CONTROL_ERROR: SETTINGS_INITIAL_WINDOW_SIZE %r "
                "out of range [0, %d]" % (new_size, MAX_WINDOW_SIZE))
        delta = new_size - self.initial_stream_window
        # The delta applies to every stream that is not closed; windows
        # may legitimately become negative (RFC 7540, 6.9.2).
        for stream in self._streams.values():
            stream.window += delta
            if stream.window > MAX_WINDOW_SIZE:
                raise ConnectionError_(
                    "FLOW_CONTROL_ERROR: SETTINGS change overflows the "
                    "window of stream %d" % stream.stream_id)
        self.initial_stream_window = new_size

    def _on_rst_stream(self, event):
        self._require_stream(event.stream_id)
        self._reset_stream(event.stream_id)

    def _on_end_stream(self, event):
        stream = self._require_stream(event.stream_id)
        if stream.state == STATE_HALF_CLOSED_LOCAL:
            raise StreamError_(
                stream.stream_id,
                "STREAM_CLOSED: duplicate END_STREAM on stream %d"
                % stream.stream_id)
        stream.state = STATE_HALF_CLOSED_LOCAL

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _require_stream(self, stream_id):
        if stream_id <= 0:
            raise ConnectionError_(
                "PROTOCOL_ERROR: event requires a stream id > 0")
        if stream_id in self._closed_streams:
            raise StreamError_(
                stream_id,
                "STREAM_CLOSED: event on closed stream %d" % stream_id)
        stream = self._streams.get(stream_id)
        if stream is None:
            # Frame for a stream in the idle state: out-of-order input.
            raise ConnectionError_(
                "PROTOCOL_ERROR: event on idle (never opened) stream %d"
                % stream_id)
        return stream

    def _reset_stream(self, stream_id):
        """Close a stream so it no longer holds connection resources."""
        self._streams.pop(stream_id, None)
        self._closed_streams.add(stream_id)

    @staticmethod
    def _checked_grow(window, increment, what):
        new_window = window + increment
        if new_window > MAX_WINDOW_SIZE:
            raise ConnectionError_(
                "PROTOCOL_ERROR: %s window would exceed %d"
                % (what, MAX_WINDOW_SIZE))
        return new_window


def _demo():
    sim = FlowControlSimulator()
    events = [
        Event(HEADERS, stream_id=1),
        Event(HEADERS, stream_id=3),
        Event(DATA, stream_id=1, size=65535),
        Event(WINDOW_UPDATE, stream_id=0, increment=65535),
        Event(DATA, stream_id=3, size=1000),
        Event(SETTINGS, initial_window_size=32768),
        Event(WINDOW_UPDATE, stream_id=1, increment=40000),
        Event(END_STREAM, stream_id=1),
        Event(RST_STREAM, stream_id=3),
    ]
    for result in sim.run(events):
        print(result)


if __name__ == "__main__":
    _demo()
