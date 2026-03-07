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

alter table public.tasks enable row level security;
alter table public.task_events enable row level security;

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
