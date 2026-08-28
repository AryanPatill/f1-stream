-- F1-Stream schema. Paste whole file into the Supabase SQL editor and run.
-- Idempotent: safe to run more than once.

create extension if not exists pgcrypto;

-- ---------------------------------------------------------------
-- datasets: a registered, checksummed replay input
-- ---------------------------------------------------------------
create table if not exists datasets (
    dataset_id    uuid primary key default gen_random_uuid(),
    label         text        not null,
    kind          text        not null,
    source_config jsonb       not null default '{}'::jsonb,
    event_count   integer,
    checksum      text        not null,
    created_at    timestamptz not null default now(),
    constraint datasets_kind_ck
        check (kind in ('fastf1', 'upload', 'synthetic')),
    constraint datasets_event_count_ck
        check (event_count is null or event_count >= 0),
    constraint datasets_checksum_uq unique (checksum)
);

-- ---------------------------------------------------------------
-- runs: one replay execution against one dataset
-- ---------------------------------------------------------------
create table if not exists runs (
    run_id     uuid primary key default gen_random_uuid(),
    dataset_id uuid        not null references datasets (dataset_id) on delete cascade,
    config     jsonb       not null default '{}'::jsonb,
    started_at timestamptz not null default now(),
    ended_at   timestamptz,
    status     text        not null default 'running',
    constraint runs_status_ck
        check (status in ('running', 'completed', 'crashed'))
);

create index if not exists runs_dataset_started_idx
    on runs (dataset_id, started_at desc);

-- ---------------------------------------------------------------
-- raw_events: append-only ingest log. Two clocks, on purpose.
-- ---------------------------------------------------------------
create table if not exists raw_events (
    id           bigserial primary key,
    run_id       uuid          not null references runs (run_id) on delete cascade,
    event_key    text          not null,
    driver       text          not null,
    lap          integer       not null,
    sector       smallint      not null,
    event_time   numeric(10,3) not null,
    arrival_time numeric(10,3) not null,
    payload      jsonb         not null default '{}'::jsonb,
    constraint raw_events_sector_ck check (sector between 1 and 3),
    constraint raw_events_lap_ck    check (lap >= 1),
    constraint raw_events_times_ck  check (event_time >= 0 and arrival_time >= 0),
    -- THE dedup mechanism: duplicate delivery collides here.
    constraint raw_events_key_uq unique (run_id, event_key)
);

create index if not exists raw_events_run_event_time_idx
    on raw_events (run_id, event_time);

-- ---------------------------------------------------------------
-- windows: event-time tumbling windows, keyed (run, driver, lap)
-- ---------------------------------------------------------------
create table if not exists windows (
    run_id       uuid          not null references runs (run_id) on delete cascade,
    driver       text          not null,
    lap          integer       not null,
    window_start numeric(10,3) not null,
    window_end   numeric(10,3) not null,
    state        text          not null default 'open',
    sectors_seen smallint      not null default 0,
    agg          jsonb         not null default '{}'::jsonb,
    version      integer       not null default 1,
    closed_at    timestamptz,
    constraint windows_pk primary key (run_id, driver, lap),
    constraint windows_state_ck
        check (state in ('open', 'closed', 'amended')),
    constraint windows_sectors_ck  check (sectors_seen between 0 and 3),
    constraint windows_version_ck  check (version >= 1),
    constraint windows_bounds_ck   check (window_end > window_start)
);

create index if not exists windows_run_state_idx
    on windows (run_id, state);

-- ---------------------------------------------------------------
-- late_events: the side output. Nothing is dropped uncounted.
-- ---------------------------------------------------------------
create table if not exists late_events (
    id          bigserial primary key,
    run_id      uuid          not null references runs (run_id) on delete cascade,
    event_key   text          not null,
    driver      text          not null,
    lap         integer       not null,
    event_time  numeric(10,3) not null,
    lateness    numeric(10,3) not null,
    disposition text          not null,
    recorded_at timestamptz   not null default now(),
    constraint late_events_disposition_ck
        check (disposition in ('amended', 'dropped_beyond_max_lateness')),
    constraint late_events_lateness_ck check (lateness >= 0)
);

create index if not exists late_events_run_idx
    on late_events (run_id, recorded_at desc);

-- ---------------------------------------------------------------
-- checkpoints: crash recovery state, including in-RAM open windows
-- ---------------------------------------------------------------
create table if not exists checkpoints (
    id                bigserial primary key,
    run_id            uuid          not null references runs (run_id) on delete cascade,
    watermark         numeric(10,3) not null,
    last_arrival_seq  bigint        not null,
    open_windows      jsonb         not null default '[]'::jsonb,
    created_at        timestamptz   not null default now(),
    constraint checkpoints_seq_ck check (last_arrival_seq >= 0)
);

create index if not exists checkpoints_run_created_idx
    on checkpoints (run_id, created_at desc);

-- ---------------------------------------------------------------
-- ground_truth: keyed on DATASET, not run. Truth is a property
-- of the input; a buggy run must not redefine its own answer.
-- ---------------------------------------------------------------
create table if not exists ground_truth (
    dataset_id uuid    not null references datasets (dataset_id) on delete cascade,
    driver     text    not null,
    lap        integer not null,
    agg        jsonb   not null default '{}'::jsonb,
    constraint ground_truth_pk primary key (dataset_id, driver, lap)
);

-- ---------------------------------------------------------------
-- reconciliation: stream output vs ground truth, one row per run
-- ---------------------------------------------------------------
create table if not exists reconciliation (
    run_id      uuid primary key references runs (run_id) on delete cascade,
    matched     integer     not null default 0,
    mismatched  integer     not null default 0,
    missing     integer     not null default 0,
    extra       integer     not null default 0,
    detail      jsonb       not null default '{}'::jsonb,
    computed_at timestamptz not null default now(),
    constraint reconciliation_counts_ck
        check (matched >= 0 and mismatched >= 0 and missing >= 0 and extra >= 0)
);

-- ---------------------------------------------------------------
-- narratives: LLM output with provenance (step 20)
-- ---------------------------------------------------------------
create table if not exists narratives (
    id             bigserial primary key,
    run_id         uuid        not null references runs (run_id) on delete cascade,
    driver         text        not null,
    text           text        not null,
    source_windows jsonb       not null default '[]'::jsonb,
    created_at     timestamptz not null default now(),
    constraint narratives_run_driver_uq unique (run_id, driver)
);