from __future__ import annotations

from pathlib import Path


def _baseline_sql() -> str:
    return Path("migrations/20260309190000_production_baseline.sql").read_text(encoding="utf-8").lower()


def test_baseline_and_a2ui_forward_migration_exist():
    files = sorted(path.name for path in Path("migrations").glob("*.sql"))
    assert files == [
        "20260309190000_production_baseline.sql",
        "20260310151434_a2ui_task_hard_reset.sql",
    ]


def test_baseline_includes_core_tables_and_event_columns():
    sql = _baseline_sql()

    assert "create table if not exists public.users" in sql
    assert "create table if not exists public.sessions" in sql
    assert "create table if not exists public.events" in sql
    assert "create table if not exists public.session_messages" in sql
    assert "create table if not exists public.task_items" in sql
    assert "create table if not exists public.task_timeboxes" in sql
    assert "create table if not exists public.task_operation_log" in sql
    assert "add column if not exists cloud_task_name" in sql
    assert "add column if not exists workflow_state" in sql
    assert "add column if not exists next_retry_at" in sql
    assert "add column if not exists dead_lettered_at" in sql


def test_baseline_has_cloud_tasks_scheduler_functions():
    sql = _baseline_sql()

    assert "create or replace function public.prevent_session_user_reassignment" in sql
    assert "create trigger trg_prevent_session_user_reassignment" in sql
    assert "create or replace function public.schedule_event_job" in sql
    assert "create or replace function public.unschedule_event_job" in sql
    assert "create or replace function public.schedule_event_retry" in sql
    assert "create or replace function public.ensure_next_morning_wake_event" in sql
    assert "idx_events_cloud_task_name" in sql
    assert "idx_events_payload_schedule_owner" in sql
    assert "idx_events_payload_seed_date" in sql


def test_baseline_has_no_pg_cron_or_edge_dispatch_dependencies():
    sql = _baseline_sql()

    assert "cron.schedule" not in sql
    assert "cron.unschedule" not in sql
    assert "net.http_post" not in sql
    assert "/functions/v1/execute-event" not in sql


def test_cloudbuild_runs_migrations_before_deploy():
    yaml_text = Path("cloudbuild.yaml").read_text(encoding="utf-8")

    assert "for file in migrations/*.sql" in yaml_text
    assert "SUPABASE_DB_URL" in yaml_text
    assert "psql" in yaml_text
