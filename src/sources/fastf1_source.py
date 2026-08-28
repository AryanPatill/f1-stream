"""FastF1 session adapter.

Maps FastF1's wide `laps` frame onto the canonical event schema.
Network is touched exactly once per session; FastF1's own HTTP cache
plus our Parquet cache mean later runs are offline.
"""
from __future__ import annotations

import asyncio
import logging

import pandas as pd

from src.config import DATA_DIR
from src.sources.base import EventSource, SourceError, register_dataset

_FASTF1_CACHE = DATA_DIR / "fastf1_cache"

# FastF1 column -> canonical sector number
_SECTOR_COLUMNS = {
    1: ("Sector1SessionTime", "Sector1Time"),
    2: ("Sector2SessionTime", "Sector2Time"),
    3: ("Sector3SessionTime", "Sector3Time"),
}


def _to_seconds(series: pd.Series) -> pd.Series:
    """Timedelta -> float seconds. NaT becomes NaN, handled by the caller."""
    return series.dt.total_seconds()


class FastF1Source(EventSource):
    kind = "fastf1"

    def __init__(
        self,
        year: int,
        grand_prix: str,
        session: str = "R",
        label: str | None = None,
    ) -> None:
        if not 1950 <= year <= 2100:
            raise ValueError("year out of range")
        if session not in {"FP1", "FP2", "FP3", "Q", "SQ", "S", "R"}:
            raise ValueError(f"unsupported session type: {session}")
        self.year = year
        self.grand_prix = grand_prix
        self.session = session
        self.label = label or f"{grand_prix} {year} {session}"

    def describe(self) -> tuple[str, dict]:
        return self.label, {
            "year": self.year,
            "grand_prix": self.grand_prix,
            "session": self.session,
        }

    def load(self) -> pd.DataFrame:
        import fastf1

        _FASTF1_CACHE.mkdir(parents=True, exist_ok=True)
        fastf1.Cache.enable_cache(str(_FASTF1_CACHE))
        logging.getLogger("fastf1").setLevel(logging.WARNING)

        try:
            session = fastf1.get_session(self.year, self.grand_prix, self.session)
            session.load(telemetry=False, weather=False, messages=False)
        except Exception as exc:
            raise SourceError(
                f"FastF1 could not load {self.label}: {exc}. "
                "Check the year and grand prix name, or fall back to "
                "SyntheticSource."
            ) from exc

        laps = session.laps
        if laps is None or laps.empty:
            raise SourceError(f"FastF1 returned no laps for {self.label}.")

        needed = ["Driver", "LapNumber", "Compound"]
        for _, (abs_col, dur_col) in _SECTOR_COLUMNS.items():
            needed += [abs_col, dur_col]
        missing = [c for c in needed if c not in laps.columns]
        if missing:
            raise SourceError(f"FastF1 laps frame is missing columns: {missing}")

        rows: list[pd.DataFrame] = []
        for sector, (abs_col, dur_col) in _SECTOR_COLUMNS.items():
            block = pd.DataFrame(
                {
                    "driver": laps["Driver"].astype("string"),
                    "lap": laps["LapNumber"],
                    "sector": sector,
                    "event_time": _to_seconds(laps[abs_col]),
                    "sector_time": _to_seconds(laps[dur_col]),
                    "compound": laps["Compound"].astype("string"),
                }
            )
            rows.append(block)

        df = pd.concat(rows, ignore_index=True)

        before = len(df)
        # Drop rather than interpolate. A fabricated sector time would
        # silently corrupt ground truth, which is the one thing that
        # must stay trustworthy.
        df = df.dropna(subset=["driver", "lap", "event_time", "sector_time"])
        df = df[df["sector_time"] > 0]
        df = df[df["event_time"] >= 0]
        dropped = before - len(df)

        if df.empty:
            raise SourceError(
                f"All {before} sector records for {self.label} were incomplete."
            )

        df["compound"] = df["compound"].fillna("UNKNOWN")
        df["lap"] = df["lap"].astype("int32")

        # Real sessions occasionally repeat a (driver, lap, sector) after a
        # timing correction. Keep the first; normalize() would reject dupes.
        df = df.drop_duplicates(subset=["driver", "lap", "sector"], keep="first")

        if dropped:
            print(f"   dropped {dropped} incomplete sector records of {before}")

        return df


async def _main() -> None:
    import argparse

    from src.db import create_rw_pool

    parser = argparse.ArgumentParser(description="Register a FastF1 session.")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--gp", type=str, default="Monza")
    parser.add_argument("--session", type=str, default="R")
    args = parser.parse_args()

    source = FastF1Source(args.year, args.gp, args.session)
    print(f"Loading {source.label} (first run downloads; later runs are cached)...")

    pool = await create_rw_pool()
    try:
        dataset_id, created = await register_dataset(pool, source)
    finally:
        await pool.close()

    verb = "registered" if created else "already existed, reused"
    print(f"Dataset {verb}: {dataset_id}")
    print(f"   label:  {source.label}")
    print(f"   parquet: data/{dataset_id}.parquet")


if __name__ == "__main__":
    asyncio.run(_main())