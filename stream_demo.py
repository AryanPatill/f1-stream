"""Start a run and immediately stream its progress. One command,
no gap between launching and watching."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

import httpx
from dotenv import load_dotenv

load_dotenv()
BASE = "http://127.0.0.1:8000"


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--lateness", type=float, default=150.0)
    parser.add_argument("--max-lateness", type=float, default=600.0)
    args = parser.parse_args()

    api_key = os.environ.get("APP_API_KEY", "")
    if not api_key:
        print("APP_API_KEY not set in .env")
        sys.exit(1)

    async with httpx.AsyncClient(base_url=BASE, timeout=30.0) as client:
        auth = await client.post("/api/auth/session", json={"api_key": api_key})
        if auth.status_code != 200:
            print(f"auth failed: {auth.status_code} {auth.text}")
            sys.exit(1)

        started = await client.post(
            "/api/runs",
            json={
                "dataset_id": args.dataset,
                "allowed_lateness_s": args.lateness,
                "max_lateness_s": args.max_lateness,
            },
        )
        if started.status_code != 200:
            print(f"run failed to start: {started.status_code} {started.text}")
            sys.exit(1)

        run_id = started.json()["run_id"]
        print(f"run {run_id}\n")

        async with client.stream(
            "GET", f"/api/runs/{run_id}/events", timeout=None
        ) as response:
            event = None
            async for line in response.aiter_lines():
                if line.startswith("event: "):
                    event = line[7:]
                elif line.startswith("data: "):
                    data = json.loads(line[6:])
                    print(
                        f"[{event}] events={data.get('events'):>5} "
                        f"windows={data.get('windows'):>5} "
                        f"amended={data.get('amended'):>3} "
                        f"watermark={data.get('watermark')}"
                    )
                elif line.startswith(":"):
                    print("[heartbeat]")


if __name__ == "__main__":
    asyncio.run(main())