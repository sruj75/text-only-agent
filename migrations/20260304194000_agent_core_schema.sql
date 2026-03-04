create table if not exists public.users (
  user_id text primary key,
  wake_time text,
  bedtime text,
  timezone text not null default 'UTC',
  health_anchors jsonb not null default '[]'::jsonb,
  onboarding_status text not null default 'pending',
  onboarding_completed_at timestamptz,
  playbook jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

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

alter table public.users enable row level security;
alter table public.sessions enable row level security;
alter table public.push_tokens enable row level security;
alter table public.events enable row level security;

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

create index if not exists idx_sessions_user_updated
  on public.sessions (user_id, updated_at desc);

create index if not exists idx_events_user_scheduled
  on public.events (user_id, scheduled_time);

create index if not exists idx_events_schedule_owner
  on public.events ((payload->>'schedule_owner'));

create index if not exists idx_events_seed_date
  on public.events ((payload->>'seed_date'));

create sequence if not exists public.event_job_id_seq
  start with 1001
  increment by 1;

create or replace function public.schedule_event_job(
  p_event_id text,
  p_run_at timestamptz,
  p_timezone text default 'UTC'
) returns bigint
language plpgsql
security definer
set search_path = public
as $$
declare
  v_job_id bigint;
begin
  perform p_event_id, p_run_at, p_timezone;
  select nextval('public.event_job_id_seq') into v_job_id;
  return v_job_id;
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
