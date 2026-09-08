"""Dataset routes: list registered datasets, register new ones.

Uploads reuse UploadSource from step 7 unchanged. The extra checks here
exist because HTTP adds threats the CLI did not have: an untrusted
Content-Length, and an attacker-controlled filename.
"""
from __future__ import annotations

import json
import logging
import tempfile
import uuid
from pathlib import Path

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Request,
    UploadFile,
    status,
)

from api.deps import get_ro, get_rw
from api.security import limiter, require_session
from src.sources.base import SourceError, register_dataset
from src.sources.fastf1_source import FastF1Source
from src.sources.synthetic_source import SyntheticSource
from src.sources.upload_source import (
    ALLOWED_SUFFIXES,
    MAX_BYTES,
    UploadSource,
)

logger = logging.getLogger("f1stream")

router = APIRouter(prefix="/api/datasets", tags=["datasets"])

CHUNK = 64 * 1024


@router.get("")
@limiter.limit("60/minute")
async def list_datasets(request: Request) -> list[dict]:
    """Read path: uses the SELECT-only pool."""
    pool = get_ro(request)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            select d.dataset_id, d.label, d.kind, d.event_count,
                   d.created_at,
                   count(g.lap) as ground_truth_laps
            from datasets d
            left join ground_truth g using (dataset_id)
            group by d.dataset_id, d.label, d.kind, d.event_count, d.created_at
            order by d.created_at desc
            """
        )
    return [
        {
            "dataset_id": str(r["dataset_id"]),
            "label": r["label"],
            "kind": r["kind"],
            "event_count": r["event_count"],
            "ground_truth_laps": r["ground_truth_laps"],
            "created_at": r["created_at"].isoformat(),
        }
        for r in rows
    ]


async def _build_truth(pool, dataset_id: str) -> dict:
    from src.ground_truth import build

    return await build(pool, dataset_id)


@router.post("/upload", dependencies=[Depends(require_session)])
@limiter.limit("10/minute")
async def upload_dataset(
    request: Request,
    file: UploadFile = File(...),
    label: str | None = Form(default=None),
) -> dict:
    """Accept a CSV/Parquet upload, validate it, register it.

    The filename is attacker-controlled: '../../.env' is a legal value.
    We take only the suffix and generate our own name, so a traversal
    string cannot influence where anything is written.
    """
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in ALLOWED_SUFFIXES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported extension. Allowed: {sorted(ALLOWED_SUFFIXES)}",
        )

    if label is not None and len(label) > 200:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Label too long.",
        )

    tmp_dir = Path(tempfile.gettempdir())
    tmp_path = tmp_dir / f"f1stream-upload-{uuid.uuid4().hex}{suffix}"

    written = 0
    try:
        with tmp_path.open("wb") as out:
            while True:
                chunk = await file.read(CHUNK)
                if not chunk:
                    break
                written += len(chunk)
                # Count as we stream. A declared Content-Length is a
                # claim; this is the measurement.
                if written > MAX_BYTES:
                    raise HTTPException(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        detail=(
                            f"File exceeds {MAX_BYTES // 1_048_576} MB limit."
                        ),
                    )
                out.write(chunk)

        source = UploadSource(tmp_path, label=label or f"Upload: {file.filename}")
        pool = get_rw(request)
        dataset_id, created = await register_dataset(pool, source)
        truth = await _build_truth(pool, dataset_id)

    except HTTPException:
        raise
    except SourceError as exc:
        # Validation failures are the client's problem: report them.
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        logger.exception("upload failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not process upload.",
        ) from exc
    finally:
        tmp_path.unlink(missing_ok=True)

    return {
        "dataset_id": dataset_id,
        "created": created,
        "bytes_received": written,
        "ground_truth": truth,
    }


@router.post("/synthetic", dependencies=[Depends(require_session)])
@limiter.limit("10/minute")
async def create_synthetic(
    request: Request,
    seed: int = Form(default=42),
    n_drivers: int = Form(default=20),
    n_laps: int = Form(default=50),
) -> dict:
    """Generate a dataset with no network dependency."""
    if not 1 <= n_drivers <= 20 or not 1 <= n_laps <= 200 or not 0 <= seed < 2**31:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="seed 0..2^31, n_drivers 1..20, n_laps 1..200.",
        )

    try:
        source = SyntheticSource(seed=seed, n_drivers=n_drivers, n_laps=n_laps)
        pool = get_rw(request)
        dataset_id, created = await register_dataset(pool, source)
        truth = await _build_truth(pool, dataset_id)
    except SourceError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        ) from exc

    return {"dataset_id": dataset_id, "created": created, "ground_truth": truth}


@router.post("/fastf1", dependencies=[Depends(require_session)])
@limiter.limit("3/minute")
async def create_fastf1(
    request: Request,
    year: int = Form(...),
    grand_prix: str = Form(...),
    session: str = Form(default="R"),
) -> dict:
    """Register a real session. Rate-limited hard: this hits an
    upstream API we do not own."""
    if not 1950 <= year <= 2100 or len(grand_prix) > 100:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid parameters."
        )

    try:
        source = FastF1Source(year, grand_prix, session)
        pool = get_rw(request)
        dataset_id, created = await register_dataset(pool, source)
        truth = await _build_truth(pool, dataset_id)
    except (SourceError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        ) from exc

    return {"dataset_id": dataset_id, "created": created, "ground_truth": truth}