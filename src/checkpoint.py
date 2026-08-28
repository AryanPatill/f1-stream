"""Checkpoint and restore processor state.

Snapshotted state:
  watermark      — max_event_time and current value
  open windows   — full accumulating state, lost on crash otherwise
  closed windows — retained amendable state within the horizon
  tombstones     — keys of every window closed so far

Not snapshotted: raw_events. Those are already durable, and the unique
constraint makes replaying them across the checkpoint boundary a no-op.
That is why recovery can be approximate and still be correct.
"""
from __future__ import annotations

import json
import uuid

import asyncpg

from src.config import RunConfig
from src.watermark import Watermark
from src.windows import WindowState, WindowStore


def capture(
    watermark: Watermark, store: WindowStore, last_arrival_seq: int
) -> dict:
    """Serialize everything needed to resume."""
    return {
        "watermark": watermark.to_state(),
        "open": [w.to_state() for w in store.open.values()],
        "closed": [w.to_state() for w in store.closed.values()],
        "tombstones": [[d, l] for (d, l) in store.tombstones],
        "last_arrival_seq": last_arrival_seq,
    }


async def save(
    conn: asyncpg.Connection,
    run_id: str,
    watermark: Watermark,
    store: WindowStore,
    last_arrival_seq: int,
) -> None:
    state = capture(watermark, store, last_arrival_seq)
    await conn.execute(
        """
        insert into checkpoints
            (run_id, watermark, last_arrival_seq, open_windows)
        values ($1, $2, $3, $4::jsonb)
        """,
        uuid.UUID(run_id),
        watermark.value,
        last_arrival_seq,
        json.dumps(state),
    )


async def load_latest(
    pool: asyncpg.Pool, run_id: str, config: RunConfig
) -> tuple[Watermark, WindowStore, int] | None:
    """Rebuild state from the newest checkpoint. None if there is none."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            select open_windows
            from checkpoints
            where run_id = $1
            order by created_at desc, id desc
            limit 1
            """,
            uuid.UUID(run_id),
        )

    if row is None:
        return None

    state = json.loads(row["open_windows"])

    watermark = Watermark.from_state(
        state["watermark"],
        allowed_lateness=config.allowed_lateness_s,
        max_lateness=config.max_lateness_s,
    )

    store = WindowStore()
    for window_state in state.get("open", []):
        window = WindowState.from_state(window_state)
        store.open[(window.driver, window.lap)] = window
    for window_state in state.get("closed", []):
        window = WindowState.from_state(window_state)
        store.closed[(window.driver, window.lap)] = window
    store.tombstones = {
        (driver, int(lap)) for driver, lap in state.get("tombstones", [])
    }

    return watermark, store, int(state["last_arrival_seq"])


async def find_resumable(pool: asyncpg.Pool) -> tuple[str, str] | None:
    """Newest run left in 'running' state. Returns (run_id, dataset_id)."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            select run_id, dataset_id
            from runs
            where status = 'running'
            order by started_at desc
            limit 1
            """
        )
    if row is None:
        return None
    return str(row["run_id"]), str(row["dataset_id"])