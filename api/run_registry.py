"""Tracks processor tasks launched from HTTP requests.

A background task outlives the request that started it, so no caller is
left to await it. Without a done-callback, an exception inside the task
is swallowed and the run sits in 'running' forever with the failure
recorded nowhere.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

import asyncpg

logger = logging.getLogger("f1stream")

# Each active run holds a pooled connection. The rw pool has max_size=4,
# so cap concurrency below that and leave headroom for other routes.
MAX_CONCURRENT_RUNS = 2


@dataclass
class RunHandle:
    run_id: str
    dataset_id: str
    task: asyncio.Task
    started_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    error: str | None = None


class RunRegistry:
    def __init__(self) -> None:
        self._runs: dict[str, RunHandle] = {}

    @property
    def active_count(self) -> int:
        return sum(1 for h in self._runs.values() if not h.task.done())

    def at_capacity(self) -> bool:
        return self.active_count >= MAX_CONCURRENT_RUNS

    def register(
        self, run_id: str, dataset_id: str, task: asyncio.Task, pool: asyncpg.Pool
    ) -> RunHandle:
        handle = RunHandle(run_id=run_id, dataset_id=dataset_id, task=task)
        self._runs[run_id] = handle

        def _on_done(finished: asyncio.Task) -> None:
            if finished.cancelled():
                handle.error = "cancelled"
                logger.warning("run %s cancelled", run_id)
            else:
                exc = finished.exception()
                if exc is not None:
                    handle.error = f"{type(exc).__name__}: {exc}"
                    logger.error("run %s failed", run_id, exc_info=exc)
                    # Fire-and-forget, but the status must reach the
                    # database or the client polls 'running' forever.
                    asyncio.create_task(_mark_crashed(pool, run_id))

        task.add_done_callback(_on_done)
        return handle

    def get(self, run_id: str) -> RunHandle | None:
        return self._runs.get(run_id)

    def is_active(self, run_id: str) -> bool:
        handle = self._runs.get(run_id)
        return handle is not None and not handle.task.done()

    def prune(self, keep: int = 20) -> None:
        """Drop finished handles, newest kept. Bounds memory in a
        long-lived server."""
        finished = sorted(
            (h for h in self._runs.values() if h.task.done()),
            key=lambda h: h.started_at,
        )
        for handle in finished[:-keep] if len(finished) > keep else []:
            self._runs.pop(handle.run_id, None)


async def _mark_crashed(pool: asyncpg.Pool, run_id: str) -> None:
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "update runs set status = 'crashed', ended_at = now() "
                "where run_id = $1 and status = 'running'",
                uuid.UUID(run_id),
            )
    except Exception:
        logger.exception("could not mark run %s crashed", run_id)


registry = RunRegistry()