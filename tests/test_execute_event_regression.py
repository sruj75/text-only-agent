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
