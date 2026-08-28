"""In-memory event-time windows keyed by (driver, lap).

A window accumulates sector events and closes when the watermark passes
its end — on evidence, not on an assumption about arrival order.

Closed windows are retained for the max_lateness horizon so late events
can amend them, then their STATE is evicted to bound memory. Their KEY
is retained permanently as a tombstone: without it, a very late event
would look like a brand-new window and silently overwrite a correct
published result with a one-sector fragment.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from src.events import Event

REQUIRED_SECTORS = 3
SECTOR_GRACE_S = 0.001


@dataclass
class WindowState:
    driver: str
    lap: int
    window_start: float
    window_end: float
    lap_time: float = 0.0
    sectors_seen: int = 0
    compound: str = "UNKNOWN"
    sector_keys: set[int] = field(default_factory=set)
    version: int = 1

    def add(self, event: Event) -> bool:
        """Fold in a sector. Returns False if that sector is already present."""
        if event.sector in self.sector_keys:
            return False
        self.sector_keys.add(event.sector)
        self.lap_time += event.sector_time
        self.sectors_seen += 1
        self.window_start = min(self.window_start, event.event_time)
        self.window_end = max(self.window_end, event.event_time)
        if self.compound == "UNKNOWN":
            self.compound = event.compound
        return True

    @property
    def is_complete(self) -> bool:
        return self.sectors_seen == REQUIRED_SECTORS

    def agg(self) -> dict:
        return {
            "lap_time": round(self.lap_time, 3),
            "sectors": self.sectors_seen,
            "last_event_time": round(self.window_end, 3),
            "compound": self.compound,
        }

    def bounds(self) -> tuple[float, float]:
        end = max(self.window_end, self.window_start + SECTOR_GRACE_S)
        return self.window_start, end

    def to_state(self) -> dict:
        return {
            "driver": self.driver,
            "lap": self.lap,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "lap_time": self.lap_time,
            "sectors_seen": self.sectors_seen,
            "compound": self.compound,
            "sector_keys": sorted(self.sector_keys),
            "version": self.version,
        }

    @classmethod
    def from_state(cls, state: dict) -> "WindowState":
        window = cls(
            driver=state["driver"],
            lap=int(state["lap"]),
            window_start=float(state["window_start"]),
            window_end=float(state["window_end"]),
            lap_time=float(state["lap_time"]),
            sectors_seen=int(state["sectors_seen"]),
            compound=state["compound"],
            version=int(state.get("version", 1)),
        )
        window.sector_keys = set(state.get("sector_keys", []))
        return window


class WindowStore:
    """Holds open windows, retains recently closed ones for amendment,
    and remembers every key it has ever closed."""

    def __init__(self) -> None:
        self.open: dict[tuple[str, int], WindowState] = {}
        # Recently closed: full state, amendable, evicted past the horizon.
        self.closed: dict[tuple[str, int], WindowState] = {}
        # Tombstones: keys only, never evicted. A key here that is absent
        # from `closed` means "published and no longer amendable".
        self.tombstones: set[tuple[str, int]] = set()

    def route(self, event: Event) -> tuple[WindowState, bool]:
        """Get or create the window for this event. Returns (window, is_new).

        Callers MUST check is_tombstoned() first — routing an event whose
        window was already published resurrects a fragment.
        """
        key = (event.driver, event.lap)
        window = self.open.get(key)
        if window is not None:
            return window, False
        window = WindowState(
            driver=event.driver,
            lap=event.lap,
            window_start=event.event_time,
            window_end=event.event_time,
        )
        self.open[key] = window
        return window, True

    def is_tombstoned(self, driver: str, lap: int) -> bool:
        """True if this window was closed at some point in this run."""
        return (driver, lap) in self.tombstones

    def closable(self, watermark_value: float) -> list[WindowState]:
        """Windows whose end the watermark has passed."""
        return [
            window
            for window in self.open.values()
            if watermark_value >= window.window_end
        ]

    def close(self, window: WindowState) -> None:
        key = (window.driver, window.lap)
        self.open.pop(key, None)
        self.closed[key] = window
        self.tombstones.add(key)

    def find_closed(self, driver: str, lap: int) -> WindowState | None:
        """Return an amendable closed window, or None if evicted or unseen."""
        return self.closed.get((driver, lap))

    def evict_closed(self, before_event_time: float) -> int:
        """Drop closed window STATE past the amendment horizon.

        Tombstones survive: memory is bounded by dropping the state, and
        correctness is preserved by remembering the key.
        """
        stale = [
            key
            for key, window in self.closed.items()
            if window.window_end < before_event_time
        ]
        for key in stale:
            del self.closed[key]
        return len(stale)

    def drain(self) -> list[WindowState]:
        """End of stream: everything still open closes now."""
        remaining = list(self.open.values())
        for window in remaining:
            key = (window.driver, window.lap)
            self.closed[key] = window
            self.tombstones.add(key)
        self.open.clear()
        return remaining