do $$
begin
  if not exists (select 1 from pg_extension where extname = 'pg_cron') then
    raise exception 'Missing required extension: pg_cron';
  end if;
  if not exists (select 1 from pg_extension where extname = 'pg_net') then
    raise exception 'Missing required extension: pg_net';
  end if;
  if not exists (select 1 from pg_extension where extname = 'pgcrypto') then
    raise exception 'Missing required extension: pgcrypto';
  end if;
end
$$;

alter table if exists public.events
  add column if not exists last_attempt_at timestamptz;

alter table if exists public.events
  add column if not exists attempt_count integer not null default 0;

create or replace function public.get_scheduler_secret_for_execution()
returns text
language plpgsql
security definer
set search_path = public
as $$
declare
  v_secret text;
begin
  select decrypted_secret
  into v_secret
  from vault.decrypted_secrets
  where name = 'scheduler_secret'
  limit 1;
  return v_secret;
end;
$$;

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
declare
  v_job_name text;
  v_job_id bigint;
  v_schedule text;
  v_command text;
  v_run_at_utc timestamp;
  v_now_utc timestamp;
  v_target_url text;
  v_secret text;
  v_headers jsonb;
  v_body jsonb;
begin
  if p_event_id is null or btrim(p_event_id) = '' then
    raise exception 'event_id is required';
  end if;
  if p_run_at is null then
    raise exception 'run_at is required';
  end if;
  if p_timezone is null or btrim(p_timezone) = '' then
    raise exception 'timezone is required';
  end if;

  v_run_at_utc := date_trunc('minute', p_run_at at time zone 'UTC');
  v_now_utc := date_trunc('minute', now() at time zone 'UTC');
  if v_run_at_utc <= v_now_utc then
    v_run_at_utc := v_now_utc + interval '1 minute';
  end if;

  v_schedule := format(
    '%s %s %s %s *',
    extract(minute from v_run_at_utc)::int,
    extract(hour from v_run_at_utc)::int,
    extract(day from v_run_at_utc)::int,
    extract(month from v_run_at_utc)::int
  );

  select decrypted_secret
  into v_target_url
  from vault.decrypted_secrets
  where name = 'scheduler_execute_event_url'
  limit 1;

  if v_target_url is null then
    select decrypted_secret
    into v_target_url
    from vault.decrypted_secrets
    where name = 'project_url'
    limit 1;
    if v_target_url is not null then
      v_target_url := rtrim(v_target_url, '/') || '/functions/v1/execute-event';
    end if;
  end if;

  if v_target_url is null then
    raise exception 'Missing vault secret scheduler_execute_event_url or project_url';
  end if;

  select decrypted_secret
  into v_secret
  from vault.decrypted_secrets
  where name = 'scheduler_secret'
  limit 1;

  if v_secret is null then
    raise exception 'Missing vault secret scheduler_secret';
  end if;

  v_headers := jsonb_build_object(
    'Content-Type', 'application/json',
    'X-Scheduler-Secret', v_secret
  );
  v_body := jsonb_build_object('event_id', p_event_id);

  v_command := format(
    'select net.http_post(url := %L, headers := %L::jsonb, body := %L::jsonb, timeout_milliseconds := 10000) as request_id',
    v_target_url,
    v_headers::text,
    v_body::text
  );

  v_job_name := substr(
    format(
      'event_%s_%s',
      regexp_replace(substr(p_event_id, 1, 24), '[^a-zA-Z0-9_]+', '', 'g'),
      floor(extract(epoch from clock_timestamp()))::bigint
    ),
    1,
    63
  );

  select cron.schedule(v_job_name, v_schedule, v_command)
  into v_job_id;

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
declare
  v_removed boolean;
begin
  if p_job_id is null then
    return false;
  end if;

  select cron.unschedule(p_job_id)
  into v_removed;
  return coalesce(v_removed, false);
exception
  when others then
    return false;
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
  v_existing_cron_job bigint;
  v_new_event_id text;
  v_new_job_id bigint;
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

  select id, scheduled_time, cron_job_id
  into v_existing
  from public.events
  where user_id = v_event.user_id
    and event_type = 'morning_wake'
    and executed = false
    and payload->>'seed_date' = v_seed_date
  order by scheduled_time asc
  limit 1;

  if found then
    v_existing_cron_job := v_existing.cron_job_id;
    v_new_job_id := public.schedule_event_job(v_existing.id, v_target_utc, v_tz);

    update public.events
    set scheduled_time = v_target_utc,
        payload = jsonb_build_object(
          'reason', 'daily_bootstrap',
          'seed_date', v_seed_date,
          'timezone', v_tz,
          'schedule_owner', 'system',
          'schedule_policy', 'morning_bootstrap'
        ),
        executed = false,
        cron_job_id = v_new_job_id,
        last_error = null,
        last_attempt_at = null,
        attempt_count = 0,
        updated_at = now()
    where id = v_existing.id;

    if v_existing_cron_job is not null then
      perform public.unschedule_event_job(v_existing_cron_job);
    end if;

    return jsonb_build_object(
      'status', 'ok',
      'action', 'rescheduled_existing',
      'event_id', v_existing.id,
      'cron_job_id', v_new_job_id,
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
    attempt_count,
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
      'schedule_policy', 'morning_bootstrap'
    ),
    false,
    null,
    0,
    now(),
    now()
  );

  begin
    v_new_job_id := public.schedule_event_job(v_new_event_id, v_target_utc, v_tz);
    update public.events
    set cron_job_id = v_new_job_id,
        updated_at = now()
    where id = v_new_event_id;
  exception
    when others then
      delete from public.events where id = v_new_event_id;
      raise;
  end;

  return jsonb_build_object(
    'status', 'ok',
    'action', 'created',
    'event_id', v_new_event_id,
    'cron_job_id', v_new_job_id,
    'seed_date', v_seed_date,
    'scheduled_time', v_target_utc
  );
end;
$$;
