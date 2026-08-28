"""Source adapter contract, normalization, checksumming, registration."""
from __future__ import annotations

import hashlib
import json
from typing import Protocol

import asyncpg
import pandas as pd

from src.config import DATA_DIR
from src.events import CANONICAL_COLUMNS, CANONICAL_DTYPES


class SourceError(RuntimeError):
    """Raised when a source cannot produce a valid canonical frame."""


class EventSource(Protocol):
    """Every adapter implements exactly this."""

    kind: str

    def describe(self) -> tuple[str, dict]:
        """Return (human label, source_config to persist as jsonb)."""
        ...

    def load(self) -> pd.DataFrame:
        """Return a frame with the canonical columns. Order irrelevant;
        normalize() handles sorting."""
        ...


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and canonicalize. Rejects rather than repairs."""
    missing = [c for c in CANONICAL_COLUMNS if c not in df.columns]
    if missing:
        raise SourceError(
            f"Source frame is missing required columns: {missing}. "
            f"Required: {list(CANONICAL_COLUMNS)}"
        )

    # Drop anything not in the contract — no schema smuggling.
    out = df[list(CANONICAL_COLUMNS)].copy()

    try:
        out = out.astype(CANONICAL_DTYPES)
    except (ValueError, TypeError) as exc:
        raise SourceError(f"Column dtype conversion failed: {exc}") from exc

    if out.empty:
        raise SourceError("Source produced zero events.")
    if not out["sector"].between(1, 3).all():
        raise SourceError("sector values must all be 1, 2, or 3.")
    if not (out["lap"] >= 1).all():
        raise SourceError("lap values must all be >= 1.")
    if not (out["event_time"] >= 0).all():
        raise SourceError("event_time values must all be >= 0.")
    if out["sector_time"].isna().any():
        raise SourceError("sector_time contains nulls.")

    dupes = out.duplicated(subset=["driver", "lap", "sector"]).sum()
    if dupes:
        raise SourceError(
            f"{dupes} duplicate (driver, lap, sector) rows in source. "
            "Each sector completion must appear exactly once."
        )

    return out.sort_values(["driver", "lap", "sector"]).reset_index(drop=True)


def compute_checksum(df: pd.DataFrame) -> str:
    """Content hash of a normalized frame.

    Hashes a canonical CSV rendering rather than Parquet bytes, because
    Parquet writes are not byte-reproducible across versions.
    """
    csv = df.to_csv(index=False, float_format="%.6f", lineterminator="\n")
    return hashlib.sha256(csv.encode("utf-8")).hexdigest()


async def register_dataset(
    pool: asyncpg.Pool, source: EventSource
) -> tuple[str, bool]:
    """Normalize, checksum, persist. Returns (dataset_id, was_created).

    If the checksum already exists, reuses that dataset rather than
    creating a second one with identical content and a divergent
    ground truth.
    """
    label, source_config = source.describe()
    df = normalize(source.load())
    checksum = compute_checksum(df)

    async with pool.acquire() as conn:
        existing = await conn.fetchval(
            "select dataset_id from datasets where checksum = $1", checksum
        )
        if existing is not None:
            return str(existing), False

        dataset_id = await conn.fetchval(
            """
            insert into datasets (label, kind, source_config, event_count, checksum)
            values ($1, $2, $3::jsonb, $4, $5)
            returning dataset_id
            """,
            label,
            source.kind,
            json.dumps(source_config),
            len(df),
            checksum,
        )

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(DATA_DIR / f"{dataset_id}.parquet", index=False)
    return str(dataset_id), True


def load_dataset_frame(dataset_id: str) -> pd.DataFrame:
    """Read a registered dataset's cached Parquet."""
    path = DATA_DIR / f"{dataset_id}.parquet"
    if not path.exists():
        raise SourceError(f"No cached Parquet for dataset {dataset_id} at {path}")
    return pd.read_parquet(path)