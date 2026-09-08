"""Run routes: launch a processor run, poll its status, read results."""
from __future__ import annotations

import asyncio
import json
import logging
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from api.deps import get_ro, get_rw
from api.run_registry import registry
from api.schemas import RunRequest
from api.security import limiter, require_session
from src.config import RunConfig

logger = logging.getLogger("f1stream")

router = APIRouter(prefix="/api/runs", tags=["runs"])

MAX_ROWS = 2000


def _parse_uuid(value: str, name: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid {name}.",
        )


@router.post("", dependencies=[Depends(require_session)])
@limiter.limit("10/minute")
async def start_run(request: Request, payload: RunRequest) -> dict:
    """Launch a run in the background. Returns immediately with run_id.

    The processor takes tens of seconds; an HTTP request must not.
    """
    if registry.at_capacity():
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many runs in progress. Wait for one to finish.",
        )

    dataset_uuid = _parse_uuid(payload.dataset_id, "dataset_id")
    rw = get_rw(request)

    async with rw.acquire() as conn:
        exists = await conn.fetchval(
            "select 1 from datasets where dataset_id = $1", dataset_uuid
        )
        truth_rows = await conn.fetchval(
            "select count(*) from ground_truth where dataset_id = $1", dataset_uuid
        )
    if not exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found."
        )
    if not truth_rows:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Dataset has no ground truth; reconciliation would be meaningless.",
        )

    try:
        config = RunConfig(
            base_delay_s=payload.base_delay_s,
            tail_delay_s=payload.tail_delay_s,
            p_tail=payload.p_tail,
            p_duplicate=payload.p_duplicate,
            p_drop=payload.p_drop,
            reorder_jitter_s=payload.reorder_jitter_s,
            allowed_lateness_s=payload.allowed_lateness_s,
            max_lateness_s=payload.max_lateness_s,
            speed=payload.speed,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        ) from exc

    from src.processor import create_run, run_processor

    run_id = await create_run(rw, payload.dataset_id, config)

    async def _execute() -> None:
        # resume_run_id reuses the row we just created rather than
        # inserting a second one.
        await run_processor(
            rw,
            payload.dataset_id,
            config,
            seed=payload.seed,
            resume_run_id=run_id,
        )

    task = asyncio.create_task(_execute(), name=f"run-{run_id}")
    registry.register(run_id, payload.dataset_id, task, rw)
    registry.prune()

    return {"run_id": run_id, "status": "running"}


@router.get("")
@limiter.limit("60/minute")
async def list_runs(request: Request, limit: int = Query(default=20, ge=1, le=100)) -> list[dict]:
    pool = get_ro(request)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            select r.run_id, r.dataset_id, d.label, r.status,
                   r.started_at, r.ended_at, r.config
            from runs r
            join datasets d using (dataset_id)
            order by r.started_at desc
            limit $1
            """,
            limit,
        )
    return [
        {
            "run_id": str(r["run_id"]),
            "dataset_id": str(r["dataset_id"]),
            "dataset_label": r["label"],
            "status": r["status"],
            "started_at": r["started_at"].isoformat(),
            "ended_at": r["ended_at"].isoformat() if r["ended_at"] else None,
            "config": json.loads(r["config"]),
            "active": registry.is_active(str(r["run_id"])),
        }
        for r in rows
    ]


@router.get("/{run_id}")
@limiter.limit("120/minute")
async def run_status(request: Request, run_id: str) -> dict:
    """Poll a run. Includes live progress counts while it executes."""
    run_uuid = _parse_uuid(run_id, "run_id")
    pool = get_ro(request)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            select r.run_id, r.dataset_id, d.label, r.status,
                   r.started_at, r.ended_at, r.config
            from runs r
            join datasets d using (dataset_id)
            where r.run_id = $1
            """,
            run_uuid,
        )
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Run not found."
            )

        counts = await conn.fetchrow(
            """
            select
              (select count(*) from raw_events where run_id = $1) as events,
              (select count(*) from windows where run_id = $1) as windows,
              (select count(*) from windows
                 where run_id = $1 and state = 'amended') as amended,
              (select count(*) from late_events
                 where run_id = $1 and disposition = 'dropped_beyond_max_lateness')
                 as dropped,
              (select watermark from checkpoints where run_id = $1
                 order by created_at desc, id desc limit 1) as watermark
            """,
            run_uuid,
        )

    handle = registry.get(run_id)
    return {
        "run_id": str(row["run_id"]),
        "dataset_id": str(row["dataset_id"]),
        "dataset_label": row["label"],
        "status": row["status"],
        "active": registry.is_active(run_id),
        "error": handle.error if handle else None,
        "started_at": row["started_at"].isoformat(),
        "ended_at": row["ended_at"].isoformat() if row["ended_at"] else None,
        "config": json.loads(row["config"]),
        "progress": {
            "events": counts["events"],
            "windows": counts["windows"],
            "amended": counts["amended"],
            "dropped": counts["dropped"],
            "watermark": float(counts["watermark"]) if counts["watermark"] else None,
        },
    }


@router.get("/{run_id}/windows")
@limiter.limit("60/minute")
async def run_windows(
    request: Request,
    run_id: str,
    limit: int = Query(default=200, ge=1, le=MAX_ROWS),
    offset: int = Query(default=0, ge=0),
    state: str | None = Query(default=None),
) -> dict:
    run_uuid = _parse_uuid(run_id, "run_id")
    if state is not None and state not in {"open", "closed", "amended"}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid state filter."
        )

    pool = get_ro(request)
    async with pool.acquire() as conn:
        total = await conn.fetchval(
            "select count(*) from windows where run_id = $1 "
            "and ($2::text is null or state = $2)",
            run_uuid,
            state,
        )
        rows = await conn.fetch(
            """
            select driver, lap, state, sectors_seen, agg, version,
                   window_start, window_end
            from windows
            where run_id = $1 and ($2::text is null or state = $2)
            order by driver, lap
            limit $3 offset $4
            """,
            run_uuid,
            state,
            limit,
            offset,
        )

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "windows": [
            {
                "driver": r["driver"],
                "lap": r["lap"],
                "state": r["state"],
                "sectors_seen": r["sectors_seen"],
                "version": r["version"],
                "window_start": float(r["window_start"]),
                "window_end": float(r["window_end"]),
                "agg": json.loads(r["agg"]),
            }
            for r in rows
        ],
    }


@router.get("/{run_id}/late")
@limiter.limit("60/minute")
async def run_late_events(
    request: Request,
    run_id: str,
    limit: int = Query(default=200, ge=1, le=MAX_ROWS),
) -> dict:
    run_uuid = _parse_uuid(run_id, "run_id")
    pool = get_ro(request)

    async with pool.acquire() as conn:
        summary = await conn.fetch(
            "select disposition, count(*) as n from late_events "
            "where run_id = $1 group by disposition",
            run_uuid,
        )
        rows = await conn.fetch(
            """
            select driver, lap, event_time, lateness, disposition, recorded_at
            from late_events
            where run_id = $1
            order by recorded_at desc
            limit $2
            """,
            run_uuid,
            limit,
        )

    return {
        "summary": {r["disposition"]: r["n"] for r in summary},
        "events": [
            {
                "driver": r["driver"],
                "lap": r["lap"],
                "event_time": float(r["event_time"]),
                "lateness": float(r["lateness"]),
                "disposition": r["disposition"],
            }
            for r in rows
        ],
    }


@router.get("/{run_id}/reconciliation")
@limiter.limit("60/minute")
async def run_reconciliation(request: Request, run_id: str) -> dict:
    run_uuid = _parse_uuid(run_id, "run_id")
    pool = get_ro(request)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "select matched, mismatched, missing, extra, detail, computed_at "
            "from reconciliation where run_id = $1",
            run_uuid,
        )

    if row is None:
        return {"computed": False}

    total = row["matched"] + row["mismatched"] + row["missing"]
    return {
        "computed": True,
        "matched": row["matched"],
        "mismatched": row["mismatched"],
        "missing": row["missing"],
        "extra": row["extra"],
        "match_rate": round(100.0 * row["matched"] / total, 2) if total else 0.0,
        "detail": json.loads(row["detail"]),
        "computed_at": row["computed_at"].isoformat(),
    }


@router.post("/{run_id}/reconcile", dependencies=[Depends(require_session)])
@limiter.limit("20/minute")
async def trigger_reconcile(request: Request, run_id: str) -> dict:
    """Compare this run against ground truth. Refuses while still running."""
    run_uuid = _parse_uuid(run_id, "run_id")
    if registry.is_active(run_id):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Run still in progress.",
        )

    from src.reconcile import reconcile

    pool = get_rw(request)
    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            "select 1 from runs where run_id = $1", run_uuid
        )
    if not exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Run not found."
        )

    result = await reconcile(pool, run_id)
    total = result["matched"] + result["mismatched"] + result["missing"]
    return {
        "matched": result["matched"],
        "mismatched": result["mismatched"],
        "missing": result["missing"],
        "extra": result["extra"],
        "match_rate": round(100.0 * result["matched"] / total, 2) if total else 0.0,
    }