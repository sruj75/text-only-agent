from __future__ import annotations

from pathlib import Path


def test_execute_event_uses_retry_path_for_push_failures():
    source = Path("supabase/functions/execute-event/index.ts").read_text(encoding="utf-8")

    assert "MAX_DELIVERY_ATTEMPTS = 4" in source
    assert 'supabase.rpc("schedule_event_retry"' in source
    assert 'status: "retry_scheduled"' in source


def test_execute_event_marks_terminal_failure_after_max_attempts():
    source = Path("supabase/functions/execute-event/index.ts").read_text(encoding="utf-8")

    assert '"delivery_failed_terminal"' in source
    assert "p_attempt_count: nextAttemptCount" in source


def test_execute_event_creates_dedicated_proactive_thread_per_invite():
    source = Path("supabase/functions/execute-event/index.ts").read_text(encoding="utf-8")

    assert "function getProactiveSessionId" in source
    assert 'session_${userId}_proactive_${eventId}' in source
    assert '.from("sessions")' in source
    assert 'thread_type: "proactive"' in source
    assert "title," in source


def test_execute_event_merges_existing_proactive_session_state():
    source = Path("supabase/functions/execute-event/index.ts").read_text(encoding="utf-8")

    assert '.select("date,state")' in source
    assert "...existingState" in source
    assert "existingSessionResult.data?.date ?? dateKey" in source


def test_execute_event_retries_session_persist_failures():
    source = Path("supabase/functions/execute-event/index.ts").read_text(encoding="utf-8")

    assert 'lastError === "session_persist_failed"' in source
    assert "p_last_error: lastError" in source
