# F1-Stream

An event-time stream processor that stays correct when the feed does not.

Real telemetry arrives late, out of order, duplicated, and sometimes not
at all. Aggregating it in arrival order produces numbers that look
plausible and are quietly wrong. This project replays recorded Formula 1
timing data through a deliberately unreliable feed, processes it with
watermark-based event-time windowing, and reconciles every result against
ground truth computed offline.

## The result

Same 3,000 events, same 766 arrival-order inversions, two processors:

| Processor | Windows correct | Failure mode |
|---|---|---|
| Arrival order | 979 / 1,000 (97.9%) | 980 windows closed before their final sector arrived |
| Event time + watermark | 1,000 / 1,000 (100%) | none |

The 97.9% is the point. A bug that corrupts 2% of output passes
eyeballing, passes a smoke test, and ships. It was only caught by
reconciling against an independently computed answer key.

Other measured results:

- 985 laps of real 2024 Italian Grand Prix data reconciled at a 100%
  match rate
- ~590 events/sec sustained over a 12,000-event dataset, network-bound
  against remote Postgres
- 65 duplicate deliveries per run absorbed by a database unique
  constraint, with no application-side deduplication
- Process killed mid-stream with SIGKILL, resumed from checkpoint to
  identical output with zero duplicated windows

## How it works

```
FastF1 API / your CSV / synthetic generator
                  |
        source adapters -> canonical event schema
                  |
        +---------+---------+
        |                   |
   feed simulator     ground truth
   delay, duplicate,  (offline groupby,
   reorder, drop       order-independent)
        |                   |
        v                   |
   processor               |
   1. dedup on event_key    |
   2. advance watermark     |
   3. route to window       |
   4. close past watermark  |
   5. amend or side-output  |
        |                   |
        +--------> reconcile 
                  |
              FastAPI + SSE
                  |
              web frontend
```

**Watermark.** A monotonic claim that no event older than *T* will
arrive. Windows close when the watermark passes their end, rather than
when a later lap happens to show up. The margin is the completeness /
latency tradeoff, and it is a slider in the UI.

**Late data.** Within the correction horizon, a late event amends its
published window and bumps a version counter. Beyond it, the event is
written to `late_events` with its lateness recorded. Nothing is dropped
without a row explaining why.

**Tombstones.** Closed window state is evicted to bound memory, but its
key is kept forever. Without that, a very late event looks like a new
window and silently overwrites a correct result with a one-sector
fragment. This was a real bug, found by reconciliation.

**Recovery.** Watermark, open windows, and tombstones are checkpointed
periodically. Recovery is deliberately approximate: `raw_events` carries
a unique constraint, so replaying events across the checkpoint boundary
is a no-op. At-least-once delivery plus idempotent writes, which is what
production systems actually ship instead of chasing exactly-once.

## Quick start

Requires Python 3.11 or 3.12 and a free Supabase project.

```bash
git clone https://github.com/AryanPatill/f1-stream.git
cd f1-stream
python -m venv .venv
source .venv/bin/activate        # Windows: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
cp .env.example .env
```

Run `sql/001_schema.sql` then `sql/002_security.sql` in the Supabase SQL
editor, set the `stream_readonly` password, and fill in `.env`. Then:

```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"   # APP_API_KEY
python -c "import secrets; print(secrets.token_urlsafe(48))"   # SESSION_SECRET
```

Generate data and run the pipeline with no network dependency:

```bash
python -m src.sources.synthetic_source
python -m src.ground_truth
python -m src.processor
python -m src.reconcile
```

Expect `matched: 1,000 (100.0%)`.

Then start the web app and open http://127.0.0.1:8000

```bash
uvicorn api.main:app --host 127.0.0.1 --port 8000
```

Do not use `--reload` when starting runs from the UI — the reloader kills
background tasks mid-stream.

## Reproducing the failure

The watermark margin is the mechanism. Turn it down and watch correctness
degrade:

```bash
python -m src.processor --lateness 20 --max-lateness 600
python -m src.reconcile      # 100% — late events amended the windows

python -m src.processor --lateness 20 --max-lateness 21
python -m src.reconcile      # mismatches, each explained in late_events

python -m src.processor_naive
python -m src.reconcile      # 97.9% — no watermark at all
```

Crash recovery:

```bash
python run_stream.py --crash-after 1500    # os._exit(137), no cleanup
python run_stream.py --resume
python -m src.reconcile                    # still 100%
```

## Data sources

Three adapters produce one canonical schema
(`driver, lap, sector, event_time, sector_time, compound`):

- **FastF1** — any real session: `python -m src.sources.fastf1_source --year 2024 --gp Monza`
- **Upload** — your own CSV or Parquet, through a validation battery
  (size cap, extension allowlist, magic-byte sniff, row cap, column
  allowlist)
- **Synthetic** — seeded generator, zero network calls

Datasets deduplicate on a content checksum, so registering the same input
twice reuses the existing dataset rather than forking its ground truth.

## Security

Single-operator security for a local or private-network deployment. Not
hardened for public exposure: no TLS termination, no secret rotation, no
per-user isolation.

| Control | Implementation |
|---|---|
| Authentication | API key exchanged once for an HttpOnly, SameSite=Strict signed cookie; constant-time comparison |
| Rate limiting | Per-IP, per-endpoint (5/min auth, 10/min writes, 60/min reads) |
| Input validation | Pydantic with `extra="forbid"`, bounded numeric ranges |
| Upload safety | Streamed byte counting, server-generated filenames, content sniffing |
| SQL | Parameterized queries only |
| Database | Separate `stream_readonly` role for read paths, verified to reject writes |
| RLS | Enabled on all nine tables; the Supabase anon key reads nothing |
| Headers | CSP without `unsafe-inline`, nosniff, DENY framing, no referrer |

The CSP forbids inline scripts and CDNs, which is why the frontend has no
framework and Swagger's assets are vendored locally.

## Stack

Python 3.12, asyncio, asyncpg, PostgreSQL (Supabase), FastAPI, pandas,
FastF1, vanilla HTML/CSS/JS. No frontend build step.

## Layout

```
src/          stream engine: sources, feed, watermark, windows,
              processor, checkpoint, ground truth, reconcile, narrate
api/          FastAPI: security, auth, datasets, runs, SSE
web/          frontend: index.html, styles.css, app.js
sql/          schema and security migrations
run_stream.py CLI entrypoint with crash and resume
```

`src/processor_naive.py` is kept deliberately. It is the arrival-order
version that scores 97.9%, retained as the before-picture.

## Notes

- Reconciliation tolerance is 2ms; lap times are sums of floats.
- Monza reports 23 `extra` windows — laps whose sectors were incomplete
  in the source, excluded from ground truth but still emitted by the
  stream.
- The narrative layer reads only committed windows and records the exact
  window versions it used, so a later amendment makes the text detectably
  stale. Run with `--stub` to demonstrate without an API key.