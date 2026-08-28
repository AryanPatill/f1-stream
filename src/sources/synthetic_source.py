"""Seeded synthetic session generator. Zero network calls.

Same canonical schema as the real sources, so every downstream lesson
holds identically. This is the fallback if FastF1 is unavailable.
"""
from __future__ import annotations

import asyncio
import random

import pandas as pd

from src.sources.base import EventSource, register_dataset

_DRIVERS = [
    "VER", "PER", "HAM", "RUS", "LEC", "SAI", "NOR", "PIA", "ALO", "STR",
    "OCO", "GAS", "ALB", "SAR", "TSU", "RIC", "BOT", "ZHO", "MAG", "HUL",
]

# Nominal sector durations in seconds for a ~92s lap.
_SECTOR_BASE = {1: 26.0, 2: 38.0, 3: 28.0}
_COMPOUNDS = ["SOFT", "MEDIUM", "HARD"]


class SyntheticSource(EventSource):
    kind = "synthetic"

    def __init__(
        self,
        seed: int = 42,
        n_drivers: int = 20,
        n_laps: int = 50,
        label: str | None = None,
    ) -> None:
        if not 1 <= n_drivers <= len(_DRIVERS):
            raise ValueError(f"n_drivers must be 1..{len(_DRIVERS)}")
        if not 1 <= n_laps <= 200:
            raise ValueError("n_laps must be 1..200")
        self.seed = seed
        self.n_drivers = n_drivers
        self.n_laps = n_laps
        self.label = label or f"Synthetic seed={seed} {n_drivers}d x {n_laps}L"

    def describe(self) -> tuple[str, dict]:
        return self.label, {
            "seed": self.seed,
            "n_drivers": self.n_drivers,
            "n_laps": self.n_laps,
        }

    def load(self) -> pd.DataFrame:
        rng = random.Random(self.seed)
        rows: list[dict] = []

        for idx, driver in enumerate(_DRIVERS[: self.n_drivers]):
            # Persistent per-driver pace offset, plus a grid-position delay.
            skill = rng.uniform(-0.6, 0.9)
            clock = 3.0 + idx * 0.35
            compound = rng.choice(_COMPOUNDS)
            stint_lap = 0

            for lap in range(1, self.n_laps + 1):
                stint_lap += 1
                # One pit stop window per driver, mid-race.
                if stint_lap > rng.randint(18, 26):
                    compound = rng.choice(_COMPOUNDS)
                    stint_lap = 1
                    clock += rng.uniform(20.0, 24.0)  # pit loss

                degradation = 0.035 * stint_lap
                for sector in (1, 2, 3):
                    sector_time = (
                        _SECTOR_BASE[sector]
                        + skill
                        + degradation / 3.0
                        + rng.gauss(0.0, 0.18)
                    )
                    clock += sector_time
                    rows.append(
                        {
                            "driver": driver,
                            "lap": lap,
                            "sector": sector,
                            "event_time": round(clock, 3),
                            "sector_time": round(sector_time, 3),
                            "compound": compound,
                        }
                    )

        return pd.DataFrame(rows)


async def _main() -> None:
    from src.db import create_rw_pool

    source = SyntheticSource(seed=42, n_drivers=20, n_laps=50)
    pool = await create_rw_pool()
    try:
        dataset_id, created = await register_dataset(pool, source)
    finally:
        await pool.close()

    label, config = source.describe()
    verb = "registered" if created else "already existed, reused"
    print(f"Dataset {verb}: {dataset_id}")
    print(f"   label:  {label}")
    print(f"   config: {config}")
    print(f"   parquet: data/{dataset_id}.parquet")


if __name__ == "__main__":
    asyncio.run(_main())