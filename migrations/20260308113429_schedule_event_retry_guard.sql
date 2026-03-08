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
  v_next_job_id bigint;
begin
  select *
  into v_event
  from public.events
  where id = p_event_id
  for update;

  if not found then
    raise exception 'Event not found: %', p_event_id;
  end if;

  if v_event.cron_job_id is not null then
    perform public.unschedule_event_job(v_event.cron_job_id);
  end if;

  v_next_job_id := public.schedule_event_job(p_event_id, p_next_run_at, p_timezone);

  update public.events
  set executed = false,
      scheduled_time = p_next_run_at,
      cron_job_id = v_next_job_id,
      last_error = p_last_error,
      last_attempt_at = coalesce(p_attempted_at, now()),
      attempt_count = coalesce(attempt_count, 0) + 1,
      updated_at = now()
  where id = p_event_id;

  return v_next_job_id;
end;
$$;
