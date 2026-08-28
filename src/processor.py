"""The real processor: event-time windows closed by watermark advance,
with late data amended or side-output, and state checkpointed so a
killed process resumes without loss or double-counting.

Recovery is approximate on purpose. raw_events carries a unique
constraint, so replaying events across the checkpoint boundary is a
no-op. At-least-once delivery plus idempotent writes is what real
systems ship, rather than chasing exactly-once.
"""
from __future__ import annotations

import asyncio
import json
import os
import uuid

import asyncpg

from src.checkpoint import load_latest, save
from src.config import RunConfig
from src.events import Event
from src.watermark import Watermark
from src.windows import WindowState, WindowStore

BATCH_SIZE = 500


class CrashSimulated(RuntimeError):
    """Raised to abort a run at a chosen point, mimicking kill -9."""


async def create_run(
    pool: asyncpg.Pool, dataset_id: str, config: RunConfig
) -> str:
    async with pool.acquire() as conn:
        run_id = await conn.fetchval(
            """
            insert into runs (dataset_id, config, status)
            values ($1, $2::jsonb, 'running')
            returning run_id
            """,
            uuid.UUID(dataset_id),
            json.dumps({**config.to_json(), "processor": "watermark"}),
        )
    return str(run_id)


async def insert_events(
    conn: asyncpg.Connection, run_id: str, batch: list[Event]
) -> set[str]:
    """Batch insert. Returns keys that were genuinely new."""
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


async def write_windows(
    conn: asyncpg.Connection, run_id: str, windows: list[WindowState]
) -> None:
    """Upsert closed and amended windows in one statement."""
    if not windows:
        return

    unique: dict[tuple[str, int], WindowState] = {}
    for window in windows:
        unique[(window.driver, window.lap)] = window

    drivers, laps, starts, ends, states, sectors, aggs, versions = (
        [], [], [], [], [], [], [], []
    )
    for window in unique.values():
        start, end = window.bounds()
        drivers.append(window.driver)
        laps.append(window.lap)
        starts.append(start)
        ends.append(end)
        states.append("amended" if window.version > 1 else "closed")
        sectors.append(window.sectors_seen)
        aggs.append(json.dumps(window.agg()))
        versions.append(window.version)

    await conn.execute(
        """
        insert into windows
            (run_id, driver, lap, window_start, window_end,
             state, sectors_seen, agg, version, closed_at)
        select $1, d, l, ws, we, st, ss, a::jsonb, v, now()
        from unnest(
            $2::text[], $3::int[], $4::float8[], $5::float8[],
            $6::text[], $7::int[], $8::text[], $9::int[]
        ) as t(d, l, ws, we, st, ss, a, v)
        on conflict (run_id, driver, lap) do update
        set window_start = excluded.window_start,
            window_end   = excluded.window_end,
            state        = excluded.state,
            sectors_seen = excluded.sectors_seen,
            agg          = excluded.agg,
            version      = excluded.version,
            closed_at    = excluded.closed_at
        """,
        uuid.UUID(run_id),
        drivers, laps, starts, ends, states, sectors, aggs, versions,
    )


async def write_late_events(
    conn: asyncpg.Connection, run_id: str, records: list[dict]
) -> None:
    """Record late data. Amended or dropped — either way, never silent."""
    if not records:
        return

    await conn.execute(
        """
        insert into late_events
            (run_id, event_key, driver, lap, event_time, lateness, disposition)
        select $1, k, d, l, et, lt, dp
        from unnest(
            $2::text[], $3::text[], $4::int[],
            $5::float8[], $6::float8[], $7::text[]
        ) as t(k, d, l, et, lt, dp)
        """,
        uuid.UUID(run_id),
        [r["event_key"] for r in records],
        [r["driver"] for r in records],
        [r["lap"] for r in records],
        [r["event_time"] for r in records],
        [r["lateness"] for r in records],
        [r["disposition"] for r in records],
    )


async def run_processor(
    pool: asyncpg.Pool,
    dataset_id: str,
    config: RunConfig,
    seed: int = 7,
    crash_after: int | None = None,
    resume_run_id: str | None = None,
) -> tuple[str, dict]:
    """Consume the feed with event-time semantics. Returns (run_id, stats)."""
    from src.feed import replay

    start_from_seq = 0
    if resume_run_id is not None:
        run_id = resume_run_id
        restored = await load_latest(pool, run_id, config)
        if restored is None:
            print("   no checkpoint found; restarting this run from the top")
            watermark = Watermark(
                allowed_lateness=config.allowed_lateness_s,
                max_lateness=config.max_lateness_s,
            )
            store = WindowStore()
        else:
            watermark, store, start_from_seq = restored
            print(
                f"   restored: watermark {watermark.value:.1f}s, "
                f"{len(store.open):,} open, {len(store.closed):,} closed, "
                f"{len(store.tombstones):,} tombstones, "
                f"resuming at delivery {start_from_seq:,}"
            )
    else:
        run_id = await create_run(pool, dataset_id, config)
        watermark = Watermark(
            allowed_lateness=config.allowed_lateness_s,
            max_lateness=config.max_lateness_s,
        )
        store = WindowStore()

    queue: asyncio.Queue = asyncio.Queue(maxsize=5000)
    feeder = asyncio.create_task(
        replay(
            dataset_id,
            config,
            queue,
            seed=seed,
            start_from_seq=start_from_seq,
            real_time=False,
        )
    )

    processed = 0
    duplicates = 0
    amended = 0
    dropped_late = 0
    closed_total = 0
    max_lateness_seen = 0.0
    last_seq = start_from_seq
    since_checkpoint = 0
    batch: list[tuple[int, Event]] = []
    exhausted = False

    async with pool.acquire() as conn:
        while not exhausted:
            item = await queue.get()
            if item is None:
                exhausted = True
            else:
                batch.append(item)

            if not batch or (len(batch) < BATCH_SIZE and not exhausted):
                continue

            events = [event for _, event in batch]
            last_seq = batch[-1][0]

            new_keys = await insert_events(conn, run_id, events)
            seen_in_batch: set[str] = set()
            to_write: list[WindowState] = []
            late_records: list[dict] = []

            for event in events:
                if event.event_key not in new_keys or event.event_key in seen_in_batch:
                    duplicates += 1
                    continue
                seen_in_batch.add(event.event_key)
                processed += 1

                watermark.observe(event.event_time)
                lateness = watermark.lateness_of(event.event_time)
                if lateness > max_lateness_seen:
                    max_lateness_seen = lateness

                if store.is_tombstoned(event.driver, event.lap):
                    closed = store.find_closed(event.driver, event.lap)

                    if closed is None or watermark.is_droppable(event.event_time):
                        dropped_late += 1
                        late_records.append(
                            {
                                "event_key": event.event_key,
                                "driver": event.driver,
                                "lap": event.lap,
                                "event_time": event.event_time,
                                "lateness": lateness,
                                "disposition": "dropped_beyond_max_lateness",
                            }
                        )
                        continue

                    if closed.add(event):
                        closed.version += 1
                        amended += 1
                        to_write.append(closed)
                        late_records.append(
                            {
                                "event_key": event.event_key,
                                "driver": event.driver,
                                "lap": event.lap,
                                "event_time": event.event_time,
                                "lateness": lateness,
                                "disposition": "amended",
                            }
                        )
                    continue

                window, _ = store.route(event)
                window.add(event)

            ready = store.closable(watermark.value)
            for window in ready:
                store.close(window)
                to_write.append(window)
            closed_total += len(ready)

            store.evict_closed(watermark.value - config.max_lateness_s)
            await write_windows(conn, run_id, to_write)
            await write_late_events(conn, run_id, late_records)

            # Checkpoint AFTER the writes it describes are durable.
            # The other order would claim progress not yet persisted.
            since_checkpoint += len(events)
            if since_checkpoint >= config.checkpoint_every:
                await save(conn, run_id, watermark, store, last_seq)
                since_checkpoint = 0

            batch.clear()
            print(
                f"   ...{processed:,} events, watermark {watermark.value:8.1f}s, "
                f"{closed_total:,} closed, {amended:,} amended",
                end="\r",
            )

            if crash_after is not None and processed >= crash_after:
                await save(conn, run_id, watermark, store, last_seq)
                print(" " * 80, end="\r")
                print(
                    f"SIMULATED CRASH after {processed:,} events "
                    f"(delivery {last_seq:,}). State checkpointed."
                )
                print(f"   run_id: {run_id}")
                print("   resume with: python run_stream.py --resume")
                feeder.cancel()
                os._exit(137)  # mimics SIGKILL: no cleanup, no finally

        watermark.advance_to_end()
        remaining = store.drain()
        closed_total += len(remaining)
        await write_windows(conn, run_id, remaining)

    stats_feed = await feeder

    async with pool.acquire() as conn:
        await conn.execute(
            "update runs set status = 'completed', ended_at = now() "
            "where run_id = $1",
            uuid.UUID(run_id),
        )

    stats = {
        "deliveries": stats_feed.emitted,
        "processed": processed,
        "duplicates": duplicates,
        "amended": amended,
        "dropped_late": dropped_late,
        "windows_closed": closed_total,
        "final_watermark": round(watermark.value, 3),
        "max_lateness_seen": round(max_lateness_seen, 3),
    }

    print(" " * 80, end="\r")
    print(f"Run {run_id}")
    print(f"   deliveries consumed: {stats['deliveries']:,}")
    print(f"   unique events:       {stats['processed']:,}")
    print(f"   duplicates absorbed: {stats['duplicates']:,}")
    print(f"   windows amended:     {stats['amended']:,}")
    print(f"   dropped (too late):  {stats['dropped_late']:,}")
    print(f"   windows closed:      {stats['windows_closed']:,}")
    print(f"   final watermark:     {stats['final_watermark']:,}s")
    print(f"   max lateness seen:   {stats['max_lateness_seen']}s")
    return run_id, stats


async def _main() -> None:
    import argparse

    from src.db import create_rw_pool

    parser = argparse.ArgumentParser(description="Run the watermark processor.")
    parser.add_argument("--dataset", type=str, default=None)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--lateness", type=float, default=None)
    parser.add_argument("--max-lateness", type=float, default=None)
    args = parser.parse_args()

    overrides = {}
    if args.lateness is not None:
        overrides["allowed_lateness_s"] = args.lateness
    if args.max_lateness is not None:
        overrides["max_lateness_s"] = args.max_lateness
    config = RunConfig(**overrides)

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

        print(f"Watermark processor on: {row['label']}")
        print(
            f"   allowed_lateness: {config.allowed_lateness_s}s   "
            f"max_lateness: {config.max_lateness_s}s"
        )
        await run_processor(pool, str(row["dataset_id"]), config, args.seed)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())