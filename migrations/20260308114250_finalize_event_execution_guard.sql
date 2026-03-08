alter table if exists public.events
  add column if not exists attempt_count integer not null default 0;

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
