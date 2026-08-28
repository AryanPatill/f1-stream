"""CLI entrypoint. Start a run, or resume one that was killed.

  python run_stream.py                          start fresh
  python run_stream.py --crash-after 1500        die mid-run, on purpose
  python run_stream.py --resume                  pick up from checkpoint
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid

from src.config import RunConfig
from src.db import create_rw_pool
from src.processor import run_processor
from src.checkpoint import find_resumable


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Run or resume the processor.")
    parser.add_argument("--dataset", type=str, default=None)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--lateness", type=float, default=None)
    parser.add_argument("--max-lateness", type=float, default=None)
    parser.add_argument(
        "--crash-after",
        type=int,
        default=None,
        help="hard-exit after N events, simulating kill -9",
    )
    parser.add_argument(
        "--resume", action="store_true", help="resume the newest running run"
    )
    args = parser.parse_args()

    overrides = {}
    if args.lateness is not None:
        overrides["allowed_lateness_s"] = args.lateness
    if args.max_lateness is not None:
        overrides["max_lateness_s"] = args.max_lateness
    config = RunConfig(**overrides)

    pool = await create_rw_pool()
    try:
        if args.resume:
            found = await find_resumable(pool)
            if found is None:
                print("Nothing to resume: no run is in 'running' state.")
                return
            run_id, dataset_id = found
            print(f"Resuming run {run_id}")
            await run_processor(
                pool, dataset_id, config, args.seed, resume_run_id=run_id
            )
            return

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

        print(f"Processor on: {row['label']}")
        print(
            f"   allowed_lateness: {config.allowed_lateness_s}s   "
            f"max_lateness: {config.max_lateness_s}s"
        )
        if args.crash_after:
            print(f"   will hard-exit after {args.crash_after:,} events")

        await run_processor(
            pool,
            str(row["dataset_id"]),
            config,
            args.seed,
            crash_after=args.crash_after,
        )
    finally:
        await pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)