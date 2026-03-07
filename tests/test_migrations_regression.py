from __future__ import annotations

from pathlib import Path


def test_session_messages_primary_key_is_scoped_to_session():
    migration_path = Path("migrations/20260304193000_session_messages.sql")
    sql = migration_path.read_text(encoding="utf-8").lower()

    assert "primary key (session_id, id)" in sql


def test_scheduler_sync_migration_contains_required_rpcs_and_columns():
    migration_path = Path("migrations/20260304221000_scheduler_source_of_truth_sync.sql")
    sql = migration_path.read_text(encoding="utf-8").lower()

    assert "add column if not exists last_attempt_at" in sql
    assert "add column if not exists attempt_count" in sql

    required_functions = [
        "create or replace function public.schedule_event_job",
        "create or replace function public.unschedule_event_job",
        "create or replace function public.get_event_for_execution",
        "create or replace function public.finalize_event_execution",
        "create or replace function public.get_scheduler_secret_for_execution",
        "create or replace function public.get_user_timezone_for_execution",
        "create or replace function public.get_push_token_for_execution",
        "create or replace function public.delete_push_token_for_execution",
        "create or replace function public.ensure_next_morning_wake_event",
        "create or replace function public.schedule_event_retry",
    ]
    for fn in required_functions:
        assert fn in sql


def test_schedule_event_job_uses_pg_cron_and_pg_net():
    migration_path = Path("migrations/20260304221000_scheduler_source_of_truth_sync.sql")
    sql = migration_path.read_text(encoding="utf-8").lower()

    assert "cron.schedule" in sql
    assert "net.http_post" in sql


def test_task_persistence_migration_creates_tables_and_cleans_session_state():
    migration_path = Path("migrations/20260307133000_task_persistence_source_of_truth.sql")
    sql = migration_path.read_text(encoding="utf-8").lower()

    assert "create table if not exists public.tasks" in sql
    assert "create table if not exists public.task_events" in sql
    assert "migrated_from_session_state" in sql
    assert "state = state - 'task_state_v1' - 'task_state_version'" in sql


def test_cloudbuild_runs_task_persistence_migration_before_deploy():
    yaml_text = Path("cloudbuild.yaml").read_text(encoding="utf-8")

    assert "migrations/20260307133000_task_persistence_source_of_truth.sql" in yaml_text
    assert "SUPABASE_DB_URL" in yaml_text
    assert "psql" in yaml_text


def test_gcloudignore_keeps_migrations_in_build_context():
    ignore_text = Path(".gcloudignore").read_text(encoding="utf-8")

    assert "\nmigrations\n" not in f"\n{ignore_text}\n"
