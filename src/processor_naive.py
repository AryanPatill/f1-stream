"""DELIBERATELY WRONG processor. Step 10 only.

Aggregates in arrival order with no watermark. Closes a (driver, lap)
window as soon as an event for a later lap by that driver arrives —
the "input is ordered" assumption, which the feed has already disproved.

Writes are batched: one round trip per 500 events rather than per event.
Against a remote database, per-row writes are latency-bound and useless.

Superseded by src/processor.py at step 11. Kept in the repo as the
before-picture.
"""
from __future__ import annotations

import asyncio
import json
import uuid

import asyncpg

from src.config import RunConfig
from src.events import Event

BATCH_SIZE = 500


async def _create_run(pool: asyncpg.Pool, dataset_id: str, config: RunConfig) -> str:
    async with pool.acquire() as conn:
        run_id = await conn.fetchval(
            """
            insert into runs (dataset_id, config, status)
            values ($1, $2::jsonb, 'running')
            returning run_id
            """,
            uuid.UUID(dataset_id),
            json.dumps({**config.to_json(), "processor": "naive"}),
        )
    return str(run_id)


async def _insert_batch(
    conn: asyncpg.Connection, run_id: str, batch: list[Event]
) -> set[str]:
    """Insert a chunk of events. Returns the keys that were actually new.

    Dedup is enforced by the unique constraint on (run_id, event_key),
    not by application logic. ON CONFLICT DO NOTHING also absorbs
    duplicates that appear twice inside this same batch.
    """
    if not batch:
        return set()

    rows = await conn.fetch(
        """
        insert into raw_events
            (run_id, event_key, driver, lap, sector,
             event_time, arrival_time, payload)
        select $1, k, d, l, s, et, at, p::jsonb
        from unnest(
            $2::text[], $3::text[], $4::int[], $5::int[],
            $6::float8[], $7::float8[], $8::text[]
        ) as t(k, d, l, s, et, at, p)
        on conflict (run_id, event_key) do nothing
        returning event_key
        """,
        uuid.UUID(run_id),
        [e.event_key for e in batch],
        [e.driver for e in batch],
        [e.lap for e in batch],
        [e.sector for e in batch],
        [e.event_time for e in batch],
        [e.arrival_time for e in batch],
        [json.dumps(e.payload()) for e in batch],
    )
    return {r["event_key"] for r in rows}


async def _flush_all(
    conn: asyncpg.Connection, run_id: str, pending: dict[tuple[str, int], dict]
) -> None:
    """Write every buffered window in one statement.

    `pending` is keyed by (driver, lap), so reassignment already gives
    last-write-wins — the same outcome sequential upserts produced, and
    it avoids Postgres refusing to touch a row twice in one command.
    """
    if not pending:
        return

    drivers, laps, starts, ends, sectors, aggs = [], [], [], [], [], []
    for (driver, lap), state in pending.items():
        drivers.append(driver)
        laps.append(lap)
        starts.append(state["window_start"])
        ends.append(max(state["window_end"], state["window_start"] + 0.001))
        sectors.append(state["sectors_seen"])
        aggs.append(
            json.dumps(
                {
                    "lap_time": round(state["lap_time"], 3),
                    "sectors": state["sectors_seen"],
                    "last_event_time": round(state["window_end"], 3),
                    "compound": state["compound"],
                }
            )
        )

    await conn.execute(
        """
        insert into windows
            (run_id, driver, lap, window_start, window_end,
             state, sectors_seen, agg, closed_at)
        select $1, d, l, ws, we, 'closed', ss, a::jsonb, now()
        from unnest(
            $2::text[], $3::int[], $4::float8[],
            $5::float8[], $6::int[], $7::text[]
        ) as t(d, l, ws, we, ss, a)
        on conflict (run_id, driver, lap) do update
        set sectors_seen = excluded.sectors_seen,
            agg          = excluded.agg,
            window_end   = excluded.window_end,
            closed_at    = excluded.closed_at
        """,
        uuid.UUID(run_id),
        drivers,
        laps,
        starts,
        ends,
        sectors,
        aggs,
    )


def _apply(
    event: Event,
    open_windows: dict[tuple[str, int], dict],
    current_lap: dict[str, int],
    pending: dict[tuple[str, int], dict],
) -> bool:
    """Fold one event into state. Returns True if a window closed early."""
    key = (event.driver, event.lap)
    previous = current_lap.get(event.driver)
    closed_early = False

    # THE BUG: a later lap arriving is treated as proof the earlier lap
    # is finished. With 766 inversions in this feed, it is not.
    if previous is not None and event.lap > previous:
        stale_key = (event.driver, previous)
        if stale_key in open_windows:
            pending[stale_key] = open_windows.pop(stale_key)
            closed_early = True

    if previous is None or event.lap > previous:
        current_lap[event.driver] = event.lap

    state = open_windows.get(key)
    if state is None:
        state = {
            "window_start": event.event_time,
            "window_end": event.event_time,
            "lap_time": 0.0,
            "sectors_seen": 0,
            "compound": event.compound,
        }
        open_windows[key] = state

    state["lap_time"] += event.sector_time
    state["sectors_seen"] += 1
    state["window_start"] = min(state["window_start"], event.event_time)
    state["window_end"] = max(state["window_end"], event.event_time)
    return closed_early


async def run_naive(
    pool: asyncpg.Pool, dataset_id: str, config: RunConfig, seed: int = 7
) -> str:
    """Consume the feed with no watermark. Returns run_id."""
    from src.feed import replay

    run_id = await _create_run(pool, dataset_id, config)
    queue: asyncio.Queue = asyncio.Queue(maxsize=5000)

    # real_time=False: no reason to sit through the replay clock here.
    feeder = asyncio.create_task(
        replay(dataset_id, config, queue, seed=seed, real_time=False)
    )

    open_windows: dict[tuple[str, int], dict] = {}
    current_lap: dict[str, int] = {}
    pending: dict[tuple[str, int], dict] = {}

    processed = 0
    duplicates = 0
    premature_closes = 0
    batch: list[Event] = []
    exhausted = False

    async with pool.acquire() as conn:
        while not exhausted:
            item = await queue.get()
            if item is None:
                exhausted = True
            else:
                batch.append(item[1])

            if not batch or (len(batch) < BATCH_SIZE and not exhausted):
                continue

            new_keys = await _insert_batch(conn, run_id, batch)
            seen_in_batch: set[str] = set()

            for event in batch:
                if event.event_key not in new_keys or event.event_key in seen_in_batch:
                    duplicates += 1
                    continue
                seen_in_batch.add(event.event_key)
                processed += 1
                if _apply(event, open_windows, current_lap, pending):
                    premature_closes += 1

            batch.clear()
            print(f"   ...{processed:,} events processed", end="\r")

        # Whatever is still open at end of stream is written as-is.
        for key, state in open_windows.items():
            pending[key] = state
        await _flush_all(conn, run_id, pending)

    stats = await feeder

    async with pool.acquire() as conn:
        await conn.execute(
            "update runs set status = 'completed', ended_at = now() "
            "where run_id = $1",
            uuid.UUID(run_id),
        )

    print(" " * 40, end="\r")
    print(f"Run {run_id}")
    print(f"   deliveries consumed: {stats.emitted:,}")
    print(f"   unique events:       {processed:,}")
    print(f"   duplicates absorbed: {duplicates:,}")
    print(f"   premature closes:    {premature_closes:,}")
    print(f"   windows written:     {len(pending):,}")
    return run_id


async def _main() -> None:
    import argparse

    from src.db import create_rw_pool

    parser = argparse.ArgumentParser(description="Run the naive processor.")
    parser.add_argument("--dataset", type=str, default=None)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    pool = await create_rw_pool()
    try:
        async with pool.acquire() as conn:
            if args.dataset:
                row = await conn.fetchrow(
                    "select dataset_id, label from datasets where dataset_id = $1",
                    uuid.UUID(args.dataset),
                )
            else:
                row = await conn.fetchrow(
                    "select dataset_id, label from datasets "
                    "where kind = 'synthetic' order by created_at limit 1"
                )
        if row is None:
            print("No dataset found.")
            return

        print(f"Naive processor on: {row['label']}")
        await run_naive(pool, str(row["dataset_id"]), RunConfig(), args.seed)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())