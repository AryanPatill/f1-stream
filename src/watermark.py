"""Watermark: a monotonic claim about how far event time has advanced.

The watermark answers "what is the oldest event I might still see?"
It is max(event_time) - allowed_lateness, and it NEVER goes backward:
a retreating watermark would reopen closed windows and destroy the
completeness guarantee that closing is supposed to provide.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Watermark:
    """Tracks event-time progress across a stream.

    allowed_lateness — how far behind max(event_time) the watermark sits.
        Larger: windows close later, fewer events are late.
        Smaller: windows close sooner, more events arrive after close.
        This is the latency/completeness tradeoff. There is no right
        answer, only a chosen one.

    max_lateness — beyond this, a late event is dropped rather than
        amended. Bounds how long correction state must be retained.
    """

    allowed_lateness: float
    max_lateness: float
    max_event_time: float = 0.0
    _value: float = field(default=0.0, init=False)

    def __post_init__(self) -> None:
        if self.max_lateness < self.allowed_lateness:
            raise ValueError("max_lateness must be >= allowed_lateness")
        self._value = max(0.0, self.max_event_time - self.allowed_lateness)

    @property
    def value(self) -> float:
        return self._value

    def observe(self, event_time: float) -> float:
        """Fold in one event's time. Returns the current watermark.

        Monotonic: max() on the candidate ensures an old event arriving
        late can never pull the watermark backward.
        """
        if event_time > self.max_event_time:
            self.max_event_time = event_time
        candidate = self.max_event_time - self.allowed_lateness
        if candidate > self._value:
            self._value = candidate
        return self._value

    def is_closable(self, window_end: float) -> bool:
        """True when the watermark has passed this window's end."""
        return self._value >= window_end

    def lateness_of(self, event_time: float) -> float:
        """How far behind the watermark an event arrived. 0.0 if on time."""
        return max(0.0, self._value - event_time)

    def is_droppable(self, event_time: float) -> bool:
        """True when an event is too late even to amend a closed window."""
        return self.lateness_of(event_time) > self.max_lateness

    def advance_to_end(self) -> float:
        """Close out the stream: everything observed is now final."""
        self._value = self.max_event_time
        return self._value

    def to_state(self) -> dict:
        """Serializable form, for checkpointing at step 13."""
        return {"max_event_time": self.max_event_time, "value": self._value}

    @classmethod
    def from_state(
        cls, state: dict, allowed_lateness: float, max_lateness: float
    ) -> "Watermark":
        wm = cls(
            allowed_lateness=allowed_lateness,
            max_lateness=max_lateness,
            max_event_time=float(state.get("max_event_time", 0.0)),
        )
        wm._value = float(state.get("value", wm._value))
        return wm