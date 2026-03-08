from __future__ import annotations

from pathlib import Path


def test_execute_event_schedules_retry_attempts_for_delivery_failures():
    source = Path("supabase/functions/execute-event/index.ts").read_text(encoding="utf-8")

    assert 'supabase.rpc("schedule_event_retry"' in source
    assert 'status: "retry_scheduled"' in source
    assert "MAX_DELIVERY_ATTEMPTS" in source


def test_execute_event_records_failure_metadata_in_single_attempt_flow():
    source = Path("supabase/functions/execute-event/index.ts").read_text(encoding="utf-8")

    assert '"push_failed_or_missing_token"' in source
    assert '"session_persist_failed"' in source
    assert "p_last_error: lastError" in source
    assert "p_attempt_count: nextAttemptCount" in source


def test_execute_event_routes_to_daily_thread():
    source = Path("supabase/functions/execute-event/index.ts").read_text(encoding="utf-8")

    assert "function getDailySessionId" in source
    assert "session_${userId}_${dateKey}" in source
    assert '.from("sessions")' in source
    assert 'thread_type: "daily"' in source
    assert "title," in source


def test_execute_event_merges_existing_proactive_session_state():
    source = Path("supabase/functions/execute-event/index.ts").read_text(encoding="utf-8")

    assert '.select("date,state")' in source
    assert "...existingState" in source
    assert "existingSessionResult.data?.date ?? dateKey" in source


def test_execute_event_tracks_dead_letter_terminal_state():
    source = Path("supabase/functions/execute-event/index.ts").read_text(encoding="utf-8")

    assert 'workflow_state: "dead_letter"' in source
    assert 'status: "dead_letter"' in source
