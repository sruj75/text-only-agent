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

alter table public.session_messages enable row level security;

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

create index if not exists idx_session_messages_session_created
  on public.session_messages (session_id, created_at);

create index if not exists idx_session_messages_user_created
  on public.session_messages (user_id, created_at);
