alter table if exists public.events
  add column if not exists workflow_state text not null default 'requested';

alter table if exists public.events
  add column if not exists next_retry_at timestamptz;

alter table if exists public.events
  add column if not exists dead_lettered_at timestamptz;
