"""Generate a per-driver race summary from committed windows only.

Two rules make this auditable rather than merely fluent:

  1. Only windows in state 'closed' or 'amended' are read. An open
     window is still accumulating sectors, so a narrative built on one
     would be confidently wrong with nothing to reveal it.

  2. Every window used is recorded with its version. If a window is
     amended afterwards, the stored narrative is detectably stale.
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid

import asyncpg

from src.config import ANTHROPIC_API_KEY

logger = logging.getLogger("f1stream")

MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 400
MIN_LAPS = 5


class NarrateError(RuntimeError):
    pass


async def fetch_committed(
    pool: asyncpg.Pool, run_id: str, driver: str
) -> list[dict]:
    """Committed windows only. Open windows are excluded by the WHERE
    clause, not by a check the caller might forget."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            select driver, lap, state, sectors_seen, agg, version
            from windows
            where run_id = $1
              and driver = $2
              and state in ('closed', 'amended')
              and sectors_seen = 3
            order by lap
            """,
            uuid.UUID(run_id),
            driver,
        )
    return [
        {
            "driver": r["driver"],
            "lap": r["lap"],
            "state": r["state"],
            "version": r["version"],
            "agg": json.loads(r["agg"]),
        }
        for r in rows
    ]


def build_prompt(driver: str, windows: list[dict]) -> str:
    """Facts only. The model summarizes; it does not supply data."""
    laps = [
        f"lap {w['lap']}: {w['agg']['lap_time']:.3f}s on {w['agg']['compound']}"
        + (" (amended)" if w["state"] == "amended" else "")
        for w in windows
    ]
    times = [w["agg"]["lap_time"] for w in windows]
    fastest = min(times)
    fastest_lap = windows[times.index(fastest)]["lap"]
    amended = sum(1 for w in windows if w["state"] == "amended")

    return (
        f"Summarize this driver's stint pace in 3 to 4 sentences.\n\n"
        f"Driver: {driver}\n"
        f"Laps recorded: {len(windows)}\n"
        f"Fastest: {fastest:.3f}s on lap {fastest_lap}\n"
        f"Median: {sorted(times)[len(times) // 2]:.3f}s\n"
        f"Windows corrected after publication: {amended}\n\n"
        f"Lap times:\n" + "\n".join(laps) + "\n\n"
        "Describe pace trend, compound changes, and any consistency "
        "pattern visible in these numbers. Use only the data above — "
        "do not infer race position, incidents, or strategy that is not "
        "in these lap times. Plain prose, no bullet points, no heading."
    )


def stub_narrative(driver: str, windows: list[dict]) -> str:
    """Deterministic summary with no API call. Synchronous on purpose:
    there is nothing to await.

    Exists so the provenance and staleness machinery — the part of this
    step that carries the lesson — can be demonstrated without a paid
    account.
    """
    times = [w["agg"]["lap_time"] for w in windows]
    fastest = min(times)
    fastest_lap = windows[times.index(fastest)]["lap"]
    median = sorted(times)[len(times) // 2]
    spread = max(times) - fastest
    amended = sum(1 for w in windows if w["state"] == "amended")
    compounds = sorted({w["agg"]["compound"] for w in windows})

    third = max(1, len(times) // 3)
    first_third = sum(times[:third]) / third
    last_third = sum(times[-third:]) / third
    trend = (
        "pace faded slightly toward the end"
        if last_third > first_third + 0.3
        else "pace improved over the stint"
        if first_third > last_third + 0.3
        else "pace stayed level throughout"
    )

    return (
        f"{driver} completed {len(windows)} timed laps, quickest a "
        f"{fastest:.3f} on lap {fastest_lap} against a median of "
        f"{median:.3f}. The spread from fastest to slowest was "
        f"{spread:.3f} seconds and {trend}. Running on "
        f"{', '.join(compounds).lower()}"
        + (
            f", with {amended} lap time corrected after first publication."
            if amended
            else "."
        )
    )


async def call_model(prompt: str) -> str:
    from anthropic import AsyncAnthropic

    if not ANTHROPIC_API_KEY:
        raise NarrateError(
            "ANTHROPIC_API_KEY not set in .env. Add it, or run with --stub."
        )

    client = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
    try:
        message = await client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as exc:
        raise NarrateError(f"Model call failed: {exc}") from exc

    return "".join(
        block.text for block in message.content if block.type == "text"
    ).strip()


async def narrate_driver(
    pool: asyncpg.Pool, run_id: str, driver: str, stub: bool = False
) -> dict | None:
    windows = await fetch_committed(pool, run_id, driver)
    if len(windows) < MIN_LAPS:
        return None

    if stub:
        text = stub_narrative(driver, windows)
    else:
        text = await call_model(build_prompt(driver, windows))

    # Provenance: the exact window versions this text was generated from.
    source = [
        {"driver": w["driver"], "lap": w["lap"], "version": w["version"]}
        for w in windows
    ]

    async with pool.acquire() as conn:
        await conn.execute(
            """
            insert into narratives (run_id, driver, text, source_windows)
            values ($1, $2, $3, $4::jsonb)
            on conflict (run_id, driver) do update
            set text = excluded.text,
                source_windows = excluded.source_windows,
                created_at = now()
            """,
            uuid.UUID(run_id),
            driver,
            text,
            json.dumps(source),
        )

    return {"driver": driver, "text": text, "windows_used": len(windows)}


async def check_staleness(pool: asyncpg.Pool, run_id: str) -> list[dict]:
    """Find narratives whose source windows have changed since generation.

    This is what the provenance column is for: without it, a corrected
    lap time and a narrative describing the old one are indistinguishable.
    """
    async with pool.acquire() as conn:
        narratives = await conn.fetch(
            "select driver, source_windows from narratives where run_id = $1",
            uuid.UUID(run_id),
        )
        current = await conn.fetch(
            "select driver, lap, version from windows where run_id = $1",
            uuid.UUID(run_id),
        )

    live = {(r["driver"], r["lap"]): r["version"] for r in current}
    stale: list[dict] = []

    for row in narratives:
        drifted = [
            f"lap {s['lap']} v{s['version']}->v{live[(s['driver'], s['lap'])]}"
            for s in json.loads(row["source_windows"])
            if live.get((s["driver"], s["lap"])) != s["version"]
        ]
        if drifted:
            stale.append({"driver": row["driver"], "changed": drifted})

    return stale


async def _main() -> None:
    import argparse

    from src.db import create_rw_pool

    parser = argparse.ArgumentParser(description="Generate run narratives.")
    parser.add_argument("--run", type=str, default=None)
    parser.add_argument("--drivers", type=int, default=3)
    parser.add_argument("--check-stale", action="store_true")
    parser.add_argument(
        "--stub", action="store_true", help="generate offline, no API call"
    )
    args = parser.parse_args()

    pool = await create_rw_pool()
    try:
        async with pool.acquire() as conn:
            if args.run:
                run_id = args.run
            else:
                found = await conn.fetchval(
                    "select run_id from runs where status = 'completed' "
                    "order by started_at desc limit 1"
                )
                if found is None:
                    print("No completed runs.")
                    return
                run_id = str(found)

            drivers = await conn.fetch(
                """
                select distinct driver from windows
                where run_id = $1 and state in ('closed', 'amended')
                order by driver limit $2
                """,
                uuid.UUID(run_id),
                args.drivers,
            )

        if args.check_stale:
            stale = await check_staleness(pool, run_id)
            if not stale:
                print(f"All narratives for run {run_id} are current.")
            else:
                print(f"{len(stale)} stale narrative(s):")
                for entry in stale:
                    print(f"   {entry['driver']}: {', '.join(entry['changed'])}")
            return

        print(f"Narrating run {run_id}\n")
        for row in drivers:
            result = await narrate_driver(pool, run_id, row["driver"], args.stub)
            if result is None:
                print(f"{row['driver']}: too few committed laps, skipped")
                continue
            print(f"{result['driver']}  ({result['windows_used']} laps)")
            print(f"   {result['text']}\n")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())