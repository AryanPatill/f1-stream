"""Canonical event schema and deterministic key hashing.

Every source adapter produces a DataFrame with exactly these columns.
Everything downstream depends on that contract and nothing else.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

# The canonical column contract. Order matters for checksums.
CANONICAL_COLUMNS: tuple[str, ...] = (
    "driver",       # str   — three-letter code, e.g. "VER"
    "lap",          # int   — 1-based lap number
    "sector",       # int   — 1, 2, or 3
    "event_time",   # float — session seconds at which the sector COMPLETED
    "sector_time",  # float — seconds this sector took
    "compound",     # str   — tyre compound on this lap
)

CANONICAL_DTYPES: dict[str, str] = {
    "driver": "string",
    "lap": "int32",
    "sector": "int8",
    "event_time": "float64",
    "sector_time": "float64",
    "compound": "string",
}


def make_event_key(dataset_id: str, driver: str, lap: int, sector: int) -> str:
    """Deterministic identity for one sector completion.

    Pure function of its inputs: the same logical event always yields
    the same key, which is what lets the database reject duplicate
    delivery via the unique constraint on (run_id, event_key).
    """
    raw = f"{dataset_id}|{driver}|{lap}|{sector}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


@dataclass(frozen=True, slots=True)
class Event:
    """One sector completion, as it travels through the pipeline.

    event_time  — the EVENT clock: when this happened in the session.
    arrival_time — the PROCESSING clock: when the consumer saw it.
    These differ, and that difference is the entire project.
    """

    event_key: str
    driver: str
    lap: int
    sector: int
    event_time: float
    arrival_time: float
    sector_time: float
    compound: str

    def payload(self) -> dict:
        return {"sector_time": self.sector_time, "compound": self.compound}