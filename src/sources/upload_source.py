"""User-supplied CSV/Parquet adapter with a full validation battery.

Input is treated as hostile. Checks run cheapest-first so a bad file is
rejected before anything expensive parses it. Reused verbatim behind the
HTTP upload endpoint at step 16.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import pandas as pd

from src.events import CANONICAL_COLUMNS
from src.sources.base import EventSource, SourceError, register_dataset

MAX_BYTES = 25 * 1024 * 1024      # 25 MB
MAX_ROWS = 500_000
ALLOWED_SUFFIXES = {".csv", ".parquet"}

# Leading bytes that identify a real file of each type.
_PARQUET_MAGIC = b"PAR1"


def _check_size(path: Path) -> int:
    if not path.exists():
        raise SourceError(f"File not found: {path}")
    if not path.is_file():
        raise SourceError(f"Not a regular file: {path}")
    size = path.stat().st_size
    if size == 0:
        raise SourceError("File is empty.")
    if size > MAX_BYTES:
        raise SourceError(
            f"File is {size / 1_048_576:.1f} MB, over the "
            f"{MAX_BYTES / 1_048_576:.0f} MB limit."
        )
    return size


def _check_suffix(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix not in ALLOWED_SUFFIXES:
        raise SourceError(
            f"Unsupported extension '{suffix}'. "
            f"Allowed: {sorted(ALLOWED_SUFFIXES)}"
        )
    return suffix


def _sniff(path: Path, suffix: str) -> None:
    """An extension is a claim; the bytes are the evidence."""
    with path.open("rb") as fh:
        head = fh.read(8)

    if suffix == ".parquet":
        if not head.startswith(_PARQUET_MAGIC):
            raise SourceError(
                "File claims .parquet but does not begin with the Parquet "
                "magic bytes."
            )
        return

    # .csv — reject anything that is plainly binary.
    if head.startswith(_PARQUET_MAGIC):
        raise SourceError("File claims .csv but contains Parquet data.")
    if head.startswith(b"PK\x03\x04"):
        raise SourceError("File claims .csv but is a zip archive.")
    if b"\x00" in head:
        raise SourceError("File claims .csv but contains null bytes.")


def _read(path: Path, suffix: str) -> pd.DataFrame:
    try:
        if suffix == ".parquet":
            df = pd.read_parquet(path)
        else:
            # nrows caps memory before pandas allocates the full frame.
            df = pd.read_csv(path, nrows=MAX_ROWS + 1)
    except Exception as exc:
        raise SourceError(f"Could not parse {path.name}: {exc}") from exc

    if len(df) > MAX_ROWS:
        raise SourceError(f"File has more than {MAX_ROWS:,} rows.")
    return df


class UploadSource(EventSource):
    kind = "upload"

    def __init__(self, path: str | Path, label: str | None = None) -> None:
        self.path = Path(path).resolve()
        self.label = label or f"Upload: {self.path.name}"

    def describe(self) -> tuple[str, dict]:
        return self.label, {"filename": self.path.name}

    def load(self) -> pd.DataFrame:
        size = _check_size(self.path)
        suffix = _check_suffix(self.path)
        _sniff(self.path, suffix)
        df = _read(self.path, suffix)

        missing = [c for c in CANONICAL_COLUMNS if c not in df.columns]
        if missing:
            raise SourceError(
                f"Missing required columns: {missing}. "
                f"Required: {list(CANONICAL_COLUMNS)}"
            )

        extra = [c for c in df.columns if c not in CANONICAL_COLUMNS]
        if extra:
            print(f"   discarding {len(extra)} non-contract column(s): {extra}")

        print(f"   accepted {self.path.name}: {size:,} bytes, {len(df):,} rows")
        # normalize() in base.py enforces the column allowlist and dtypes.
        return df


async def _main() -> None:
    import argparse

    from src.db import create_rw_pool

    parser = argparse.ArgumentParser(description="Register an uploaded file.")
    parser.add_argument("path", type=str, help="path to .csv or .parquet")
    parser.add_argument("--label", type=str, default=None)
    args = parser.parse_args()

    source = UploadSource(args.path, args.label)
    pool = await create_rw_pool()
    try:
        dataset_id, created = await register_dataset(pool, source)
    finally:
        await pool.close()

    verb = "registered" if created else "already existed, reused"
    print(f"Dataset {verb}: {dataset_id}")
    print(f"   label:  {source.label}")


if __name__ == "__main__":
    asyncio.run(_main())