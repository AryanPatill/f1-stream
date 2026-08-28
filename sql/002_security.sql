-- F1-Stream security layer. Run AFTER 001_schema.sql.
-- Idempotent: safe to run more than once.
-- Contains NO passwords. The login password is set separately,
-- by hand, and is never committed to this repo.

-- ---------------------------------------------------------------
-- 1. The read-only role. Created NOLOGIN here on purpose:
--    you grant LOGIN + password yourself, outside version control.
-- ---------------------------------------------------------------
do $$
begin
    if not exists (select 1 from pg_roles where rolname = 'stream_readonly') then
        create role stream_readonly nologin;
    end if;
end
$$;

-- ---------------------------------------------------------------
-- 2. Privileges: SELECT and nothing else, ever.
-- ---------------------------------------------------------------
revoke all on schema public from stream_readonly;
grant usage on schema public to stream_readonly;

revoke all on all tables in schema public from stream_readonly;
grant select on all tables in schema public to stream_readonly;

-- No sequence access: it could not insert anyway, but be explicit.
revoke all on all sequences in schema public from stream_readonly;

-- Anything created in this schema later defaults to SELECT-only too.
alter default privileges in schema public
    revoke all on tables from stream_readonly;
alter default privileges in schema public
    grant select on tables to stream_readonly;

-- ---------------------------------------------------------------
-- 3. Row Level Security on all nine tables.
--    Already enabled if you chose "Run and enable RLS" — these
--    statements are no-ops in that case.
-- ---------------------------------------------------------------
alter table datasets       enable row level security;
alter table runs           enable row level security;
alter table raw_events     enable row level security;
alter table windows        enable row level security;
alter table late_events    enable row level security;
alter table checkpoints    enable row level security;
alter table ground_truth   enable row level security;
alter table reconciliation enable row level security;
alter table narratives     enable row level security;

-- ---------------------------------------------------------------
-- 4. SELECT policies for stream_readonly ONLY.
--    anon / authenticated get no policy, so the public Supabase
--    key reads nothing even if it leaks.
-- ---------------------------------------------------------------
drop policy if exists ro_select on datasets;
create policy ro_select on datasets
    for select to stream_readonly using (true);

drop policy if exists ro_select on runs;
create policy ro_select on runs
    for select to stream_readonly using (true);

drop policy if exists ro_select on raw_events;
create policy ro_select on raw_events
    for select to stream_readonly using (true);

drop policy if exists ro_select on windows;
create policy ro_select on windows
    for select to stream_readonly using (true);

drop policy if exists ro_select on late_events;
create policy ro_select on late_events
    for select to stream_readonly using (true);

drop policy if exists ro_select on checkpoints;
create policy ro_select on checkpoints
    for select to stream_readonly using (true);

drop policy if exists ro_select on ground_truth;
create policy ro_select on ground_truth
    for select to stream_readonly using (true);

drop policy if exists ro_select on reconciliation;
create policy ro_select on reconciliation
    for select to stream_readonly using (true);

drop policy if exists ro_select on narratives;
create policy ro_select on narratives
    for select to stream_readonly using (true);

-- ---------------------------------------------------------------
-- 5. Explicitly deny the public Supabase roles.
-- ---------------------------------------------------------------
revoke all on all tables in schema public from anon, authenticated;