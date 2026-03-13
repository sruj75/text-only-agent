create extension if not exists pgcrypto;

create table if not exists public.users (
  user_id text primary key,
  wake_time text,
  bedtime text,
  timezone text not null default 'UTC',
  onboarding_status text not null default 'pending',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table if exists public.users
  drop column if exists health_anchors;

alter table if exists public.users
  drop column if exists onboarding_completed_at;

alter table if exists public.users
  drop column if exists playbook;

create table if not exists public.system_migration_markers (
  key text primary key,
  created_at timestamptz not null default now()
);

do $$
begin
  if not exists (
    select 1
    from public.system_migration_markers
    where key = 'onboarding_rebuild_v1_20260313'
  ) then
    update public.users
    set wake_time = null,
        bedtime = null,
        onboarding_status = 'pending',
        updated_at = now();
    insert into public.system_migration_markers (key)
    values ('onboarding_rebuild_v1_20260313');
  end if;
end
$$;

create table if not exists public.sessions (
  session_id text primary key,
  user_id text not null references public.users(user_id) on delete cascade,
  date text not null,
  state jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.push_tokens (
  user_id text primary key references public.users(user_id) on delete cascade,
  expo_push_token text not null,
  updated_at timestamptz not null default now()
);

create table if not exists public.events (
  id text primary key,
  user_id text not null references public.users(user_id) on delete cascade,
  scheduled_time timestamptz not null,
  event_type text not null default 'checkin',
  payload jsonb not null default '{}'::jsonb,
  executed boolean not null default false,
  cron_job_id bigint,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.session_messages (
  id text not null,
  session_id text not null,
  user_id text not null,
  role text not null check (role in ('user', 'assistant', 'system', 'event')),
  content text not null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  primary key (session_id, id)
);

create table if not exists public.tasks (
  task_id text primary key,
  user_id text not null references public.users(user_id) on delete cascade,
  title text not null,
  status text not null default 'todo' check (status in ('todo', 'in_progress', 'done', 'blocked')),
  priority_rank integer,
  is_essential boolean not null default false,
  timebox_start_at timestamptz,
  timebox_end_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint tasks_timebox_pair check (
    (
      timebox_start_at is null
      and timebox_end_at is null
    )
    or (
      timebox_start_at is not null
      and timebox_end_at is not null
      and timebox_start_at < timebox_end_at
    )
  )
);

create table if not exists public.task_events (
  id text primary key,
  task_id text not null references public.tasks(task_id) on delete cascade,
  user_id text not null references public.users(user_id) on delete cascade,
  event_type text not null check (
    event_type in (
      'created',
      'status_updated',
      'timebox_updated',
      'top_essential_updated',
      'migrated_from_session_state'
    )
  ),
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create or replace function public.prevent_session_user_reassignment()
returns trigger
language plpgsql
set search_path = public
as $$
begin
  if old.user_id is distinct from new.user_id then
    raise exception 'Session ownership is immutable';
  end if;
  return new;
end;
$$;

drop trigger if exists trg_prevent_session_user_reassignment on public.sessions;

create trigger trg_prevent_session_user_reassignment
before update on public.sessions
for each row
execute function public.prevent_session_user_reassignment();

alter table if exists public.events
  add column if not exists last_attempt_at timestamptz;

alter table if exists public.events
  add column if not exists attempt_count integer not null default 0;

alter table if exists public.events
  add column if not exists workflow_state text not null default 'requested';

alter table if exists public.events
  add column if not exists next_retry_at timestamptz;

alter table if exists public.events
  add column if not exists dead_lettered_at timestamptz;

alter table if exists public.events
  add column if not exists cloud_task_name text;

alter table public.users enable row level security;
alter table public.sessions enable row level security;
alter table public.push_tokens enable row level security;
alter table public.events enable row level security;
alter table public.session_messages enable row level security;
alter table public.tasks enable row level security;
alter table public.task_events enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'users'
      and policyname = 'Users can select own profile'
  ) then
    create policy "Users can select own profile"
      on public.users
      for select
      to authenticated
      using (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'users'
      and policyname = 'Users can insert own profile'
  ) then
    create policy "Users can insert own profile"
      on public.users
      for insert
      to authenticated
      with check (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'users'
      and policyname = 'Users can update own profile'
  ) then
    create policy "Users can update own profile"
      on public.users
      for update
      to authenticated
      using (auth.uid()::text = user_id)
      with check (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'sessions'
      and policyname = 'Users can select own sessions'
  ) then
    create policy "Users can select own sessions"
      on public.sessions
      for select
      to authenticated
      using (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'sessions'
      and policyname = 'Users can insert own sessions'
  ) then
    create policy "Users can insert own sessions"
      on public.sessions
      for insert
      to authenticated
      with check (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'sessions'
      and policyname = 'Users can update own sessions'
  ) then
    create policy "Users can update own sessions"
      on public.sessions
      for update
      to authenticated
      using (auth.uid()::text = user_id)
      with check (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'push_tokens'
      and policyname = 'Users can select own push tokens'
  ) then
    create policy "Users can select own push tokens"
      on public.push_tokens
      for select
      to authenticated
      using (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'push_tokens'
      and policyname = 'Users can insert own push tokens'
  ) then
    create policy "Users can insert own push tokens"
      on public.push_tokens
      for insert
      to authenticated
      with check (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'push_tokens'
      and policyname = 'Users can update own push tokens'
  ) then
    create policy "Users can update own push tokens"
      on public.push_tokens
      for update
      to authenticated
      using (auth.uid()::text = user_id)
      with check (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'events'
      and policyname = 'Users can select own events'
  ) then
    create policy "Users can select own events"
      on public.events
      for select
      to authenticated
      using (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'events'
      and policyname = 'Users can insert own events'
  ) then
    create policy "Users can insert own events"
      on public.events
      for insert
      to authenticated
      with check (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'events'
      and policyname = 'Users can update own events'
  ) then
    create policy "Users can update own events"
      on public.events
      for update
      to authenticated
      using (auth.uid()::text = user_id)
      with check (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'session_messages'
      and policyname = 'Users can select own session messages'
  ) then
    create policy "Users can select own session messages"
      on public.session_messages
      for select
      to authenticated
      using (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'session_messages'
      and policyname = 'Users can insert own session messages'
  ) then
    create policy "Users can insert own session messages"
      on public.session_messages
      for insert
      to authenticated
      with check (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'tasks'
      and policyname = 'Users can select own tasks'
  ) then
    create policy "Users can select own tasks"
      on public.tasks
      for select
      to authenticated
      using (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'tasks'
      and policyname = 'Users can insert own tasks'
  ) then
    create policy "Users can insert own tasks"
      on public.tasks
      for insert
      to authenticated
      with check (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'tasks'
      and policyname = 'Users can update own tasks'
  ) then
    create policy "Users can update own tasks"
      on public.tasks
      for update
      to authenticated
      using (auth.uid()::text = user_id)
      with check (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'task_events'
      and policyname = 'Users can select own task events'
  ) then
    create policy "Users can select own task events"
      on public.task_events
      for select
      to authenticated
      using (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'task_events'
      and policyname = 'Users can insert own task events'
  ) then
    create policy "Users can insert own task events"
      on public.task_events
      for insert
      to authenticated
      with check (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'task_events'
      and policyname = 'Users can update own task events'
  ) then
    create policy "Users can update own task events"
      on public.task_events
      for update
      to authenticated
      using (auth.uid()::text = user_id)
      with check (auth.uid()::text = user_id);
  end if;
end
$$;

create index if not exists idx_sessions_user_updated
  on public.sessions (user_id, updated_at desc);

create index if not exists idx_events_user_scheduled
  on public.events (user_id, scheduled_time);

create index if not exists idx_session_messages_session_created
  on public.session_messages (session_id, created_at);

create index if not exists idx_session_messages_user_created
  on public.session_messages (user_id, created_at);

create index if not exists idx_tasks_user_updated
  on public.tasks (user_id, updated_at desc);

create index if not exists idx_tasks_user_status_priority
  on public.tasks (user_id, status, priority_rank);

create index if not exists idx_tasks_user_timebox_start
  on public.tasks (user_id, timebox_start_at);

create index if not exists idx_task_events_task_created
  on public.task_events (task_id, created_at);

create index if not exists idx_task_events_user_created
  on public.task_events (user_id, created_at desc);

create index if not exists idx_events_dispatch_due
  on public.events (executed, workflow_state, scheduled_time);

create index if not exists idx_events_cloud_task_name
  on public.events (cloud_task_name);

create index if not exists idx_events_payload_schedule_owner
  on public.events ((payload ->> 'schedule_owner'));

create index if not exists idx_events_payload_seed_date
  on public.events ((payload ->> 'seed_date'));

create index if not exists idx_events_payload_wake_purpose
  on public.events ((payload ->> 'wake_purpose'));

with task_payloads as (
  select
    s.user_id,
    task_json
  from public.sessions s
  cross join lateral jsonb_array_elements(
    coalesce(s.state -> 'task_state_v1' -> 'tasks', '[]'::jsonb)
  ) as task_json
)
insert into public.tasks (
  task_id,
  user_id,
  title,
  status,
  priority_rank,
  is_essential,
  timebox_start_at,
  timebox_end_at,
  created_at,
  updated_at
)
select
  coalesce(nullif(task_json ->> 'task_id', ''), md5(user_id || task_json::text)),
  user_id,
  coalesce(nullif(task_json ->> 'title', ''), 'Untitled task'),
  coalesce(nullif(task_json ->> 'status', ''), 'todo'),
  nullif(task_json ->> 'priority_rank', '')::integer,
  coalesce((task_json ->> 'is_essential')::boolean, false),
  nullif(task_json -> 'timebox' ->> 'start_at', '')::timestamptz,
  nullif(task_json -> 'timebox' ->> 'end_at', '')::timestamptz,
  coalesce(nullif(task_json ->> 'created_at', '')::timestamptz, now()),
  coalesce(
    nullif(task_json ->> 'updated_at', '')::timestamptz,
    now()
  )
from task_payloads
on conflict (task_id) do nothing;

with task_payloads as (
  select
    s.user_id,
    task_json
  from public.sessions s
  cross join lateral jsonb_array_elements(
    coalesce(s.state -> 'task_state_v1' -> 'tasks', '[]'::jsonb)
  ) as task_json
)
insert into public.task_events (
  id,
  task_id,
  user_id,
  event_type,
  payload,
  created_at
)
select
  'migrated_' || coalesce(nullif(task_json ->> 'task_id', ''), md5(user_id || task_json::text)),
  coalesce(nullif(task_json ->> 'task_id', ''), md5(user_id || task_json::text)),
  user_id,
  'migrated_from_session_state',
  jsonb_build_object(
    'title', task_json ->> 'title',
    'status', coalesce(nullif(task_json ->> 'status', ''), 'todo'),
    'priority_rank', nullif(task_json ->> 'priority_rank', ''),
    'is_essential', coalesce((task_json ->> 'is_essential')::boolean, false),
    'timebox_start_at', nullif(task_json -> 'timebox' ->> 'start_at', ''),
    'timebox_end_at', nullif(task_json -> 'timebox' ->> 'end_at', '')
  ),
  now()
from task_payloads
where nullif(task_json ->> 'task_id', '') is not null
on conflict (id) do nothing;

update public.sessions
set
  state = state - 'task_state_v1' - 'task_state_version',
  updated_at = now()
where state ? 'task_state_v1'
   or state ? 'task_state_version';

create or replace function public.get_user_timezone_for_execution(
  p_user_id text
) returns text
language sql
security definer
set search_path = public
as $$
  select u.timezone
  from public.users u
  where u.user_id = p_user_id
  limit 1;
$$;

create or replace function public.get_push_token_for_execution(
  p_user_id text
) returns text
language sql
security definer
set search_path = public
as $$
  select p.expo_push_token
  from public.push_tokens p
  where p.user_id = p_user_id
  limit 1;
$$;

create or replace function public.delete_push_token_for_execution(
  p_user_id text
) returns boolean
language plpgsql
security definer
set search_path = public
as $$
begin
  delete from public.push_tokens
  where user_id = p_user_id;
  return true;
end;
$$;

create or replace function public.get_event_for_execution(
  p_event_id text
) returns jsonb
language sql
security definer
set search_path = public
as $$
  select to_jsonb(e)
  from public.events e
  where e.id = p_event_id
  limit 1;
$$;

create or replace function public.schedule_event_job(
  p_event_id text,
  p_run_at timestamptz,
  p_timezone text default 'UTC'
) returns bigint
language plpgsql
security definer
set search_path = public
as $$
begin
  perform p_event_id, p_run_at, p_timezone;
  return 0;
end;
$$;

create or replace function public.unschedule_event_job(
  p_job_id bigint
) returns boolean
language plpgsql
security definer
set search_path = public
as $$
begin
  perform p_job_id;
  return true;
end;
$$;

create or replace function public.finalize_event_execution(
  p_event_id text,
  p_last_error text,
  p_attempted_at timestamptz default now(),
  p_executed boolean default true,
  p_attempt_count integer default null
) returns boolean
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows integer;
begin
  update public.events
  set executed = p_executed,
      last_error = p_last_error,
      last_attempt_at = coalesce(p_attempted_at, now()),
      attempt_count = coalesce(p_attempt_count, coalesce(attempt_count, 0) + 1),
      updated_at = now()
  where id = p_event_id;

  get diagnostics v_rows = row_count;
  return v_rows = 1;
end;
$$;

create or replace function public.schedule_event_retry(
  p_event_id text,
  p_next_run_at timestamptz,
  p_timezone text,
  p_last_error text default 'push_failed_or_missing_token',
  p_attempted_at timestamptz default now()
) returns bigint
language plpgsql
security definer
set search_path = public
as $$
declare
  v_event public.events%rowtype;
begin
  select *
  into v_event
  from public.events
  where id = p_event_id
  for update;

  if not found then
    raise exception 'Event not found: %', p_event_id;
  end if;

  perform p_timezone;

  update public.events
  set executed = false,
      scheduled_time = p_next_run_at,
      cron_job_id = null,
      cloud_task_name = null,
      last_error = p_last_error,
      last_attempt_at = coalesce(p_attempted_at, now()),
      next_retry_at = p_next_run_at,
      workflow_state = 'retry_scheduled',
      updated_at = now()
  where id = p_event_id;

  return coalesce(v_event.cron_job_id, 0);
end;
$$;

create or replace function public.ensure_next_morning_wake_event(
  p_event_id text
) returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_event record;
  v_user record;
  v_tz text;
  v_hour int;
  v_minute int;
  v_local_now timestamp;
  v_target_local timestamp;
  v_target_utc timestamptz;
  v_seed_date text;
  v_existing record;
  v_new_event_id text;
begin
  if p_event_id is null or btrim(p_event_id) = '' then
    return jsonb_build_object('status', 'skipped', 'reason', 'missing_event_id');
  end if;

  select id, user_id, event_type
  into v_event
  from public.events
  where id = p_event_id
  limit 1;

  if not found then
    return jsonb_build_object('status', 'skipped', 'reason', 'event_not_found');
  end if;

  if v_event.event_type <> 'morning_wake' then
    return jsonb_build_object('status', 'skipped', 'reason', 'not_morning_wake');
  end if;

  perform pg_advisory_xact_lock(hashtext(v_event.user_id));

  select user_id, wake_time, timezone
  into v_user
  from public.users
  where user_id = v_event.user_id
  limit 1;

  if not found then
    return jsonb_build_object('status', 'skipped', 'reason', 'user_not_found');
  end if;

  if v_user.wake_time is null
     or v_user.wake_time !~ '^(?:[01][0-9]|2[0-3]):[0-5][0-9]$' then
    return jsonb_build_object('status', 'skipped', 'reason', 'invalid_wake_time');
  end if;

  if v_user.timezone is null or btrim(v_user.timezone) = '' then
    return jsonb_build_object('status', 'skipped', 'reason', 'missing_timezone');
  end if;

  v_tz := v_user.timezone;
  begin
    v_local_now := now() at time zone v_tz;
  exception
    when others then
      return jsonb_build_object('status', 'skipped', 'reason', 'invalid_timezone');
  end;

  v_hour := split_part(v_user.wake_time, ':', 1)::int;
  v_minute := split_part(v_user.wake_time, ':', 2)::int;
  v_target_local := date_trunc('day', v_local_now) + make_interval(hours => v_hour, mins => v_minute);
  if v_target_local <= v_local_now then
    v_target_local := v_target_local + interval '1 day';
  end if;

  v_target_utc := v_target_local at time zone v_tz;
  v_seed_date := to_char(v_target_local::date, 'YYYY-MM-DD');

  select id, scheduled_time
  into v_existing
  from public.events
  where user_id = v_event.user_id
    and event_type = 'morning_wake'
    and executed = false
    and payload->>'seed_date' = v_seed_date
    and (
      payload->>'wake_purpose' = 'daily_loop'
      or (
        payload->>'wake_purpose' is null
        and coalesce(payload->>'onboarding_unlock', 'false') <> 'true'
      )
    )
  order by scheduled_time asc
  limit 1;

  if found then
    update public.events
    set scheduled_time = v_target_utc,
        payload = jsonb_build_object(
          'reason', 'daily_bootstrap',
          'seed_date', v_seed_date,
          'timezone', v_tz,
          'schedule_owner', 'system',
          'schedule_policy', 'morning_bootstrap',
          'wake_purpose', 'daily_loop'
        ),
        executed = false,
        cron_job_id = null,
        cloud_task_name = null,
        last_error = null,
        last_attempt_at = null,
        next_retry_at = null,
        dead_lettered_at = null,
        workflow_state = 'requested',
        attempt_count = 0,
        updated_at = now()
    where id = v_existing.id;

    return jsonb_build_object(
      'status', 'ok',
      'action', 'rescheduled_existing',
      'event_id', v_existing.id,
      'queued', true,
      'seed_date', v_seed_date,
      'scheduled_time', v_target_utc
    );
  end if;

  v_new_event_id := gen_random_uuid()::text;
  insert into public.events (
    id,
    user_id,
    scheduled_time,
    event_type,
    payload,
    executed,
    cron_job_id,
    cloud_task_name,
    attempt_count,
    workflow_state,
    next_retry_at,
    dead_lettered_at,
    created_at,
    updated_at
  ) values (
    v_new_event_id,
    v_event.user_id,
    v_target_utc,
    'morning_wake',
    jsonb_build_object(
      'reason', 'daily_bootstrap',
      'seed_date', v_seed_date,
      'timezone', v_tz,
      'schedule_owner', 'system',
      'schedule_policy', 'morning_bootstrap',
      'wake_purpose', 'daily_loop'
    ),
    false,
    null,
    null,
    0,
    'requested',
    null,
    null,
    now(),
    now()
  );

  return jsonb_build_object(
    'status', 'ok',
    'action', 'created',
    'event_id', v_new_event_id,
    'queued', true,
    'seed_date', v_seed_date,
    'scheduled_time', v_target_utc
  );
end;
$$;

drop function if exists public.finalize_event_execution(text, text, timestamptz, boolean);

-- A2UI hard reset: remove legacy task tables and recreate task schema.
delete from public.events
where payload->>'schedule_owner' = 'task_management';

drop table if exists public.task_events cascade;
drop table if exists public.tasks cascade;

create table if not exists public.task_items (
  task_id text primary key,
  user_id text not null references public.users(user_id) on delete cascade,
  title text not null,
  status text not null default 'todo' check (status in ('todo', 'in_progress', 'done', 'blocked')),
  priority_rank integer,
  is_essential boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.task_timeboxes (
  task_id text primary key references public.task_items(task_id) on delete cascade,
  user_id text not null references public.users(user_id) on delete cascade,
  start_at timestamptz not null,
  end_at timestamptz not null,
  source text not null default 'agent',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint task_timeboxes_window check (start_at < end_at)
);

create table if not exists public.task_operation_log (
  operation_id text primary key,
  user_id text not null references public.users(user_id) on delete cascade,
  intent text not null,
  session_id text,
  entities jsonb not null default '{}'::jsonb,
  options jsonb not null default '{}'::jsonb,
  apply_result jsonb not null default '{}'::jsonb,
  validation_errors jsonb not null default '[]'::jsonb,
  correlation_id text,
  created_at timestamptz not null default now()
);

alter table public.task_items enable row level security;
alter table public.task_timeboxes enable row level security;
alter table public.task_operation_log enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'task_items'
      and policyname = 'Users can select own task items'
  ) then
    create policy "Users can select own task items"
      on public.task_items
      for select
      to authenticated
      using (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'task_items'
      and policyname = 'Users can insert own task items'
  ) then
    create policy "Users can insert own task items"
      on public.task_items
      for insert
      to authenticated
      with check (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'task_items'
      and policyname = 'Users can update own task items'
  ) then
    create policy "Users can update own task items"
      on public.task_items
      for update
      to authenticated
      using (auth.uid()::text = user_id)
      with check (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'task_timeboxes'
      and policyname = 'Users can select own task timeboxes'
  ) then
    create policy "Users can select own task timeboxes"
      on public.task_timeboxes
      for select
      to authenticated
      using (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'task_timeboxes'
      and policyname = 'Users can insert own task timeboxes'
  ) then
    create policy "Users can insert own task timeboxes"
      on public.task_timeboxes
      for insert
      to authenticated
      with check (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'task_timeboxes'
      and policyname = 'Users can update own task timeboxes'
  ) then
    create policy "Users can update own task timeboxes"
      on public.task_timeboxes
      for update
      to authenticated
      using (auth.uid()::text = user_id)
      with check (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'task_operation_log'
      and policyname = 'Users can select own task operations'
  ) then
    create policy "Users can select own task operations"
      on public.task_operation_log
      for select
      to authenticated
      using (auth.uid()::text = user_id);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'task_operation_log'
      and policyname = 'Users can insert own task operations'
  ) then
    create policy "Users can insert own task operations"
      on public.task_operation_log
      for insert
      to authenticated
      with check (auth.uid()::text = user_id);
  end if;
end
$$;

create index if not exists idx_task_items_user_updated
  on public.task_items (user_id, updated_at desc);

create index if not exists idx_task_items_user_status_priority
  on public.task_items (user_id, status, priority_rank);

create index if not exists idx_task_timeboxes_user_start
  on public.task_timeboxes (user_id, start_at);

create index if not exists idx_task_operation_log_user_created
  on public.task_operation_log (user_id, created_at desc);
