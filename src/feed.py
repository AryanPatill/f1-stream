"""Replay simulator: emits events in ARRIVAL order, not event order.

Applies four pathologies drawn from RunConfig:
  base delay      — every event is late by a little
  heavy tail      — a minority are late by a lot
  duplication     — some events are delivered more than once
  drops           — some are never delivered at all

Reordering is not shuffled in; it emerges from per-event delay, which is
how it emerges in a real network.
"""
from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass

import pandas as pd

from src.config import RunConfig
from src.events import Event, make_event_key
from src.sources.base import load_dataset_frame


@dataclass(frozen=True, slots=True)
class FeedStats:
    """What the feed actually did, for reconciliation to account against."""

    source_events: int
    emitted: int
    duplicated: int
    dropped: int


def build_delivery_schedule(
    df: pd.DataFrame, dataset_id: str, config: RunConfig, seed: int
) -> tuple[list[Event], FeedStats]:
    """Compute every delivery and its arrival_time, then sort by arrival.

    Done eagerly so the schedule is deterministic for a given seed and
    so the sort is a single pass rather than a live priority queue.
    """
    rng = random.Random(seed)
    deliveries: list[Event] = []
    duplicated = 0
    dropped = 0

    for row in df.itertuples(index=False):
        if rng.random() < config.p_drop:
            dropped += 1
            continue

        n_copies = 2 if rng.random() < config.p_duplicate else 1
        if n_copies > 1:
            duplicated += 1

        event_key = make_event_key(
            dataset_id, str(row.driver), int(row.lap), int(row.sector)
        )
        # Session time compressed by the speed multiplier.
        base_arrival = float(row.event_time) / config.speed

        for _ in range(n_copies):
            delay = config.base_delay_s
            if rng.random() < config.p_tail:
                delay += rng.uniform(0.0, config.tail_delay_s)
            delay += rng.uniform(0.0, config.reorder_jitter_s)

            deliveries.append(
                Event(
                    event_key=event_key,
                    driver=str(row.driver),
                    lap=int(row.lap),
                    sector=int(row.sector),
                    event_time=float(row.event_time),
                    arrival_time=round(base_arrival + delay, 4),
                    sector_time=float(row.sector_time),
                    compound=str(row.compound),
                )
            )

    # Arrival order is what the consumer sees. It is NOT event order.
    deliveries.sort(key=lambda e: e.arrival_time)

    stats = FeedStats(
        source_events=len(df),
        emitted=len(deliveries),
        duplicated=duplicated,
        dropped=dropped,
    )
    return deliveries, stats


async def replay(
    dataset_id: str,
    config: RunConfig,
    queue: asyncio.Queue,
    seed: int = 7,
    start_from_seq: int = 0,
    real_time: bool = True,
) -> FeedStats:
    """Push deliveries onto `queue` in arrival order, then a None sentinel.

    start_from_seq supports crash recovery at step 13: a resumed run skips
    deliveries it already consumed.
    """
    df = load_dataset_frame(dataset_id)
    deliveries, stats = build_delivery_schedule(df, dataset_id, config, seed)

    clock = 0.0
    for seq, event in enumerate(deliveries):
        if seq < start_from_seq:
            continue
        if real_time:
            wait = event.arrival_time - clock
            if wait > 0:
                await asyncio.sleep(wait)
        clock = event.arrival_time
        await queue.put((seq, event))

    await queue.put(None)
    return stats


def inversion_count(deliveries: list[Event], sample: int = 2000) -> int:
    """How many adjacent pairs arrive out of event-time order.

    A direct measure of disorder: zero means arrival order matched event
    order and the feed did nothing.
    """
    window = deliveries[:sample]
    return sum(
        1
        for a, b in zip(window, window[1:])
        if a.event_time > b.event_time
    )


async def _main() -> None:
    import argparse

    from src.db import create_rw_pool

    parser = argparse.ArgumentParser(description="Inspect the feed.")
    parser.add_argument("--dataset", type=str, default=None)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--show", type=int, default=15)
    args = parser.parse_args()

    pool = await create_rw_pool()
    try:
        async with pool.acquire() as conn:
            if args.dataset:
                row = await conn.fetchrow(
                    "select dataset_id, label from datasets where dataset_id = $1",
                    args.dataset,
                )
            else:
                row = await conn.fetchrow(
                    "select dataset_id, label from datasets "
                    "where kind = 'synthetic' order by created_at limit 1"
                )
    finally:
        await pool.close()

    if row is None:
        print("No dataset found. Register one first.")
        return

    dataset_id = str(row["dataset_id"])
    config = RunConfig()
    df = load_dataset_frame(dataset_id)
    deliveries, stats = build_delivery_schedule(df, dataset_id, config, args.seed)

    print(f"Feed for: {row['label']}")
    print(f"   source events: {stats.source_events:,}")
    print(f"   deliveries:    {stats.emitted:,}")
    print(f"   duplicated:    {stats.duplicated:,}")
    print(f"   dropped:       {stats.dropped:,}")
    print(f"   inversions in first 2000: {inversion_count(deliveries):,}")
    print()
    print(f"First {args.show} deliveries in ARRIVAL order:")
    print(f"{'arrival':>9}  {'event_t':>9}  {'driver':<7} {'lap':>4} {'sec':>4}")

    seen: set[str] = set()
    for event in deliveries[: args.show]:
        marker = "  <-- DUPLICATE" if event.event_key in seen else ""
        seen.add(event.event_key)
        print(
            f"{event.arrival_time:9.3f}  {event.event_time:9.3f}  "
            f"{event.driver:<7} {event.lap:>4} {event.sector:>4}{marker}"
        )


if __name__ == "__main__":
    asyncio.run(_main())