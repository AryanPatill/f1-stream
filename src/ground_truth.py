"""Offline aggregation: the answer key.

Computed from the cached Parquet with groupby — no ordering, no
watermark, no lateness. It cannot be wrong in the way the stream can be
wrong, which is what makes it usable as a reference.

Keyed on dataset_id, not run_id: truth is a property of the input.
"""
from __future__ import annotations

import asyncio
import json

import asyncpg
import pandas as pd

from src.sources.base import load_dataset_frame

# A lap without all three sectors has no correct lap time. We record
# nothing for it rather than inventing a partial answer.
REQUIRED_SECTORS = 3


def compute(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Return (complete lap aggregates, count of incomplete laps)."""
    grouped = df.groupby(["driver", "lap"], observed=True).agg(
        lap_time=("sector_time", "sum"),
        sectors=("sector", "count"),
        last_event_time=("event_time", "max"),
        compound=("compound", "first"),
    )
    grouped = grouped.reset_index()

    complete = grouped[grouped["sectors"] == REQUIRED_SECTORS].copy()
    incomplete = len(grouped) - len(complete)

    complete["lap_time"] = complete["lap_time"].round(3)
    complete["last_event_time"] = complete["last_event_time"].round(3)
    return complete, incomplete


async def persist(
    pool: asyncpg.Pool, dataset_id: str, truth: pd.DataFrame
) -> int:
    """Write aggregates. Idempotent: re-running overwrites cleanly."""
    records = [
        (
            dataset_id,
            row.driver,
            int(row.lap),
            json.dumps(
                {
                    "lap_time": float(row.lap_time),
                    "sectors": int(row.sectors),
                    "last_event_time": float(row.last_event_time),
                    "compound": str(row.compound),
                }
            ),
        )
        for row in truth.itertuples(index=False)
    ]

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "delete from ground_truth where dataset_id = $1", dataset_id
            )
            await conn.executemany(
                """
                insert into ground_truth (dataset_id, driver, lap, agg)
                values ($1, $2, $3, $4::jsonb)
                """,
                records,
            )
    return len(records)


async def build(pool: asyncpg.Pool, dataset_id: str) -> dict:
    df = load_dataset_frame(dataset_id)
    truth, incomplete = compute(df)
    written = await persist(pool, dataset_id, truth)
    return {
        "events": len(df),
        "complete_laps": written,
        "incomplete_laps": incomplete,
        "drivers": int(truth["driver"].nunique()),
    }


async def _main() -> None:
    import argparse

    from src.db import create_rw_pool

    parser = argparse.ArgumentParser(description="Build ground truth.")
    parser.add_argument(
        "--dataset", type=str, default=None, help="dataset_id; omit for all"
    )
    args = parser.parse_args()

    pool = await create_rw_pool()
    try:
        async with pool.acquire() as conn:
            if args.dataset:
                rows = await conn.fetch(
                    "select dataset_id, label from datasets where dataset_id = $1",
                    args.dataset,
                )
                if not rows:
                    print(f"No dataset {args.dataset}")
                    return
            else:
                rows = await conn.fetch(
                    "select dataset_id, label from datasets order by created_at"
                )

        for row in rows:
            dataset_id = str(row["dataset_id"])
            try:
                stats = await build(pool, dataset_id)
            except Exception as exc:
                print(f"{row['label']}: FAILED — {exc}")
                continue
            print(f"{row['label']}")
            print(
                f"   {stats['events']:,} events -> "
                f"{stats['complete_laps']:,} complete laps "
                f"across {stats['drivers']} drivers"
            )
            if stats["incomplete_laps"]:
                print(
                    f"   {stats['incomplete_laps']} incomplete lap(s) "
                    "excluded from ground truth"
                )
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())