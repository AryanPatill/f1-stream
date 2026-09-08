"""Server-Sent Events: push run progress instead of being polled.

Pool discipline is the whole risk here. A handler that holds a pooled
connection for the life of the stream exhausts the pool with a few open
tabs, and the app hangs rather than erroring. So: acquire, query,
release — never hold a connection across a sleep.
"""
from __future__ import annotations

import asyncio
import json
import logging

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import StreamingResponse

from api.deps import get_ro
from api.routes_runs import _parse_uuid
from api.run_registry import registry

logger = logging.getLogger("f1stream")

router = APIRouter(prefix="/api/runs", tags=["stream"])

POLL_INTERVAL_S = 1.0
HEARTBEAT_EVERY_S = 15.0
MAX_STREAM_SECONDS = 15 * 60

# Each stream briefly borrows a connection. Cap concurrent streams well
# under the ro pool's max_size so other routes are never starved.
MAX_STREAM_CLIENTS = 4
_active_streams = 0
_stream_lock = asyncio.Lock()


def _sse(event: str, data: dict) -> str:
    """One SSE frame. The blank line terminates it — without it the
    browser buffers forever waiting for more."""
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


async def _snapshot(pool, run_uuid) -> dict | None:
    """One progress reading. Connection acquired and released here,
    never held across the sleep in the generator below."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "select status from runs where run_id = $1", run_uuid
        )
        if row is None:
            return None

        counts = await conn.fetchrow(
            """
            select
              (select count(*) from raw_events where run_id = $1) as events,
              (select count(*) from windows where run_id = $1) as windows,
              (select count(*) from windows
                 where run_id = $1 and state = 'closed') as closed,
              (select count(*) from windows
                 where run_id = $1 and state = 'amended') as amended,
              (select count(*) from late_events
                 where run_id = $1 and disposition = 'amended') as late_amended,
              (select count(*) from late_events
                 where run_id = $1
                 and disposition = 'dropped_beyond_max_lateness') as dropped,
              (select watermark from checkpoints where run_id = $1
                 order by created_at desc, id desc limit 1) as watermark
            """,
            run_uuid,
        )

    return {
        "status": row["status"],
        "events": counts["events"],
        "windows": counts["windows"],
        "closed": counts["closed"],
        "amended": counts["amended"],
        "late_amended": counts["late_amended"],
        "dropped": counts["dropped"],
        "watermark": float(counts["watermark"]) if counts["watermark"] else None,
    }


@router.get("/{run_id}/events")
async def stream_run(request: Request, run_id: str) -> StreamingResponse:
    """Live progress for one run. Ends when the run finishes."""
    global _active_streams

    run_uuid = _parse_uuid(run_id, "run_id")
    pool = get_ro(request)

    async with _stream_lock:
        if _active_streams >= MAX_STREAM_CLIENTS:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many open streams.",
            )
        _active_streams += 1

    async def generator():
        global _active_streams
        elapsed = 0.0
        since_heartbeat = 0.0
        previous: dict | None = None

        try:
            while elapsed < MAX_STREAM_SECONDS:
                # Client closed the tab: stop doing work for nobody.
                if await request.is_disconnected():
                    break

                snapshot = await _snapshot(pool, run_uuid)
                if snapshot is None:
                    yield _sse("error", {"detail": "Run not found."})
                    break

                # Only send changes. An unchanged frame every second is
                # bandwidth spent to tell the client nothing.
                if snapshot != previous:
                    yield _sse("progress", snapshot)
                    previous = snapshot
                    since_heartbeat = 0.0

                if snapshot["status"] != "running" and not registry.is_active(run_id):
                    yield _sse("done", snapshot)
                    break

                if since_heartbeat >= HEARTBEAT_EVERY_S:
                    # A comment line: keeps proxies and browsers from
                    # closing an idle connection. Invisible to the client.
                    yield ": heartbeat\n\n"
                    since_heartbeat = 0.0

                await asyncio.sleep(POLL_INTERVAL_S)
                elapsed += POLL_INTERVAL_S
                since_heartbeat += POLL_INTERVAL_S
            else:
                yield _sse("timeout", {"detail": "Stream time limit reached."})

        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("stream failed for run %s", run_id)
            yield _sse("error", {"detail": "Stream error."})
        finally:
            async with _stream_lock:
                _active_streams -= 1

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            # Tells nginx and similar not to buffer the response, which
            # would defeat streaming entirely.
            "X-Accel-Buffering": "no",
        },
    )