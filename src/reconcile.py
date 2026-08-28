"""Compare stream output against ground truth.

Four outcomes per (driver, lap):
  matched    — stream and truth agree within tolerance
  mismatched — both present, values differ
  missing    — in truth, absent from stream
  extra      — in stream, absent from truth
"""
from __future__ import annotations

import asyncio
import json
import uuid

import asyncpg

# Lap times are sums of floats; 1 ms of drift is arithmetic, not error.
TOLERANCE_S = 0.002
MAX_DETAIL_ROWS = 50


async def reconcile(pool: asyncpg.Pool, run_id: str) -> dict:
    async with pool.acquire() as conn:
        dataset_id = await conn.fetchval(
            "select dataset_id from runs where run_id = $1", uuid.UUID(run_id)
        )
        if dataset_id is None:
            raise ValueError(f"No run {run_id}")

        truth_rows = await conn.fetch(
            "select driver, lap, agg from ground_truth where dataset_id = $1",
            dataset_id,
        )
        stream_rows = await conn.fetch(
            "select driver, lap, agg, state, sectors_seen, version "
            "from windows where run_id = $1",
            uuid.UUID(run_id),
        )

    truth = {
        (r["driver"], r["lap"]): json.loads(r["agg"]) for r in truth_rows
    }
    stream = {
        (r["driver"], r["lap"]): {
            **json.loads(r["agg"]),
            "state": r["state"],
            "version": r["version"],
        }
        for r in stream_rows
    }

    matched = mismatched = 0
    detail: list[dict] = []

    for key, expected in truth.items():
        actual = stream.get(key)
        if actual is None:
            continue
        delta = abs(float(actual["lap_time"]) - float(expected["lap_time"]))
        if delta <= TOLERANCE_S and actual["sectors"] == expected["sectors"]:
            matched += 1
        else:
            mismatched += 1
            if len(detail) < MAX_DETAIL_ROWS:
                detail.append(
                    {
                        "driver": key[0],
                        "lap": key[1],
                        "expected_lap_time": round(float(expected["lap_time"]), 3),
                        "actual_lap_time": round(float(actual["lap_time"]), 3),
                        "delta": round(delta, 3),
                        "expected_sectors": expected["sectors"],
                        "actual_sectors": actual["sectors"],
                    }
                )

    missing = [k for k in truth if k not in stream]
    extra = [k for k in stream if k not in truth]

    result = {
        "matched": matched,
        "mismatched": mismatched,
        "missing": len(missing),
        "extra": len(extra),
        "detail": {
            "mismatches": detail,
            "missing_sample": [
                {"driver": d, "lap": l} for d, l in missing[:MAX_DETAIL_ROWS]
            ],
            "extra_sample": [
                {"driver": d, "lap": l} for d, l in extra[:MAX_DETAIL_ROWS]
            ],
            "tolerance_s": TOLERANCE_S,
        },
    }

    async with pool.acquire() as conn:
        await conn.execute(
            """
            insert into reconciliation
                (run_id, matched, mismatched, missing, extra, detail, computed_at)
            values ($1, $2, $3, $4, $5, $6::jsonb, now())
            on conflict (run_id) do update
            set matched = excluded.matched,
                mismatched = excluded.mismatched,
                missing = excluded.missing,
                extra = excluded.extra,
                detail = excluded.detail,
                computed_at = excluded.computed_at
            """,
            uuid.UUID(run_id),
            result["matched"],
            result["mismatched"],
            result["missing"],
            result["extra"],
            json.dumps(result["detail"]),
        )

    return result


def report(result: dict) -> None:
    total = (
        result["matched"] + result["mismatched"] + result["missing"]
    )
    pct = 100.0 * result["matched"] / total if total else 0.0
    print(f"   matched:    {result['matched']:,}  ({pct:.1f}%)")
    print(f"   mismatched: {result['mismatched']:,}")
    print(f"   missing:    {result['missing']:,}")
    print(f"   extra:      {result['extra']:,}")

    mismatches = result["detail"]["mismatches"]
    if mismatches:
        print("\n   First mismatches:")
        print(
            f"   {'driver':<7} {'lap':>4} {'expected':>10} "
            f"{'actual':>10} {'delta':>8} {'sectors':>9}"
        )
        for m in mismatches[:10]:
            print(
                f"   {m['driver']:<7} {m['lap']:>4} "
                f"{m['expected_lap_time']:>10.3f} {m['actual_lap_time']:>10.3f} "
                f"{m['delta']:>8.3f} "
                f"{m['expected_sectors']}->{m['actual_sectors']:>6}"
            )


async def _main() -> None:
    import argparse

    from src.db import create_rw_pool

    parser = argparse.ArgumentParser(description="Reconcile a run.")
    parser.add_argument("--run", type=str, default=None)
    args = parser.parse_args()

    pool = await create_rw_pool()
    try:
        async with pool.acquire() as conn:
            if args.run:
                run_id = args.run
            else:
                run_id = await conn.fetchval(
                    "select run_id from runs order by started_at desc limit 1"
                )
                if run_id is None:
                    print("No runs yet.")
                    return
                run_id = str(run_id)

        result = await reconcile(pool, run_id)
        print(f"Reconciliation for run {run_id}")
        report(result)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())