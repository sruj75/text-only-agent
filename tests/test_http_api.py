from __future__ import annotations

import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import main


def test_health_endpoint(app_client):
    client = app_client["client"]

    response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["repository_mode"] == "in_memory"
    assert payload["adk_model"] == "fake-adk-model"
    assert payload["runtime_env"] == "production"
    assert payload["strict_startup_validation"] is False


def test_runtime_validation_requires_secrets_in_production(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("STRICT_STARTUP_VALIDATION", "true")
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)

    try:
        main._validate_runtime_configuration()
    except RuntimeError as error:
        message = str(error)
        assert "GOOGLE_API_KEY" in message
        assert "SUPABASE_URL" in message
        assert "SUPABASE_SERVICE_ROLE_KEY" in message
    else:
        raise AssertionError("Expected production runtime validation to fail")


def test_runtime_validation_allows_local_fallback(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("STRICT_STARTUP_VALIDATION", "false")
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)

    snapshot = main._validate_runtime_configuration()

    assert snapshot["runtime_env"] == "production"
    assert snapshot["strict_startup_validation"] is False
    assert snapshot["missing_required_env_vars"] == [
        "GOOGLE_API_KEY",
        "SUPABASE_URL",
        "SUPABASE_SERVICE_ROLE_KEY",
    ]


def test_bootstrap_creates_daily_session_and_reuses_it(app_client):
    client = app_client["client"]
    timezone_name = "Asia/Kolkata"
    date_key = datetime.now(ZoneInfo(timezone_name)).strftime("%Y-%m-%d")

    first = client.post(
        "/agent/bootstrap-device",
        json={"device_id": "device-alpha", "timezone": timezone_name},
    )
    second = client.post(
        "/agent/bootstrap-device",
        json={"device_id": "device-alpha", "timezone": timezone_name},
    )

    assert first.status_code == 200
    assert second.status_code == 200
    first_payload = first.json()
    second_payload = second.json()

    assert first_payload["session_id"] == f"session_device-alpha_{date_key}"
    assert second_payload["session_id"] == first_payload["session_id"]
    assert len(first_payload["threads"]) == 1
    assert first_payload["messages"] == []
    assert first_payload["needs_onboarding"] is True


def test_bootstrap_accepts_explicit_session_id(app_client):
    client = app_client["client"]

    response = client.post(
        "/agent/bootstrap-device",
        json={
            "device_id": "device-beta",
            "timezone": "UTC",
            "session_id": "session_device-beta_custom_1",
            "entry_context": {
                "source": "push",
                "event_id": "event-1",
                "trigger_type": "checkin",
                "scheduled_time": "2026-03-04T10:00:00Z",
                "calendar_event_id": None,
                "entry_mode": "proactive",
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["session_id"] == "session_device-beta_custom_1"


def test_bootstrap_preserves_proactive_thread_titles(app_client):
    client = app_client["client"]
    repository = app_client["repository"]

    proactive_session_id = "session_device-proactive_proactive_event-1"
    repository.sessions[proactive_session_id] = main.SessionRecord(
        session_id=proactive_session_id,
        user_id="device-proactive",
        date="2026-03-06",
        state={
            "title": "Good morning",
            "thread_type": "proactive",
            "entry_context": {
                "source": "push",
                "event_id": "event-1",
                "trigger_type": "morning_wake",
                "scheduled_time": "2026-03-06T01:30:00Z",
                "calendar_event_id": None,
                "entry_mode": "proactive",
            },
        },
    )

    response = client.post(
        "/agent/bootstrap-device",
        json={
            "device_id": "device-proactive",
            "timezone": "UTC",
            "session_id": proactive_session_id,
            "entry_context": {
                "source": "push",
                "event_id": "event-1",
                "trigger_type": "morning_wake",
                "scheduled_time": "2026-03-06T01:30:00Z",
                "calendar_event_id": None,
                "entry_mode": "proactive",
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    thread = next(
        item for item in payload["threads"] if item["session_id"] == proactive_session_id
    )

    assert payload["session_id"] == proactive_session_id
    assert thread["title"] == "Good morning"
    assert thread["state"]["thread_type"] == "proactive"


def test_bootstrap_requires_timezone_when_user_has_none(app_client):
    client = app_client["client"]

    response = client.post(
        "/agent/bootstrap-device",
        json={"device_id": "device-no-tz"},
    )

    assert response.status_code == 400
    assert "timezone is required" in response.json()["detail"].lower()


def test_bootstrap_uses_stored_timezone_when_missing_in_request(app_client):
    client = app_client["client"]

    first = client.post(
        "/agent/bootstrap-device",
        json={"device_id": "device-stored-tz", "timezone": "Asia/Kolkata"},
    )
    assert first.status_code == 200

    second = client.post(
        "/agent/bootstrap-device",
        json={"device_id": "device-stored-tz"},
    )
    assert second.status_code == 200
    assert second.json()["timezone"] == "Asia/Kolkata"


def test_bootstrap_rejects_foreign_session_id(app_client):
    client = app_client["client"]
    repository = app_client["repository"]

    first = client.post(
        "/agent/bootstrap-device",
        json={
            "device_id": "device-owner",
            "timezone": "UTC",
            "session_id": "session_shared_forbidden",
        },
    )
    assert first.status_code == 200

    second = client.post(
        "/agent/bootstrap-device",
        json={
            "device_id": "device-attacker",
            "timezone": "UTC",
            "session_id": "session_shared_forbidden",
        },
    )

    assert second.status_code == 403
    assert second.json()["detail"] == "Session does not belong to device"
    assert repository.sessions["session_shared_forbidden"].user_id == "device-owner"


def test_thread_creation_and_listing(app_client):
    client = app_client["client"]

    bootstrap = client.post(
        "/agent/bootstrap-device",
        json={"device_id": "device-gamma", "timezone": "UTC"},
    )
    assert bootstrap.status_code == 200

    created = client.post(
        "/agent/threads",
        json={"device_id": "device-gamma", "timezone": "UTC", "title": "Sprint"},
    )
    assert created.status_code == 200
    created_payload = created.json()["thread"]

    listed = client.get("/agent/threads", params={"device_id": "device-gamma"})
    assert listed.status_code == 200
    session_ids = [thread["session_id"] for thread in listed.json()["threads"]]

    assert created_payload["session_id"] in session_ids
    assert any(thread["title"] == "Sprint" for thread in listed.json()["threads"])


def test_get_thread_messages_returns_404_for_unknown_thread(app_client):
    client = app_client["client"]

    response = client.get(
        "/agent/threads/unknown/messages",
        params={"device_id": "device-missing"},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Thread not found"


def test_agent_run_uses_real_context(app_client):
    client = app_client["client"]
    fake_agent = app_client["agent"]

    response = client.post(
        "/agent/run",
        json={
            "prompt": "Plan my day",
            "context": {
                "user_id": "user-ctx",
                "session_id": "session-ctx",
                "timezone": "Asia/Kolkata",
            },
        },
    )

    assert response.status_code == 200
    assert response.json()["output"] == "assistant::Plan my day"
    assert fake_agent.run_calls[-1]["user_id"] == "user-ctx"
    assert fake_agent.run_calls[-1]["session_id"] == "session-ctx"


def test_agent_run_returns_503_when_adk_fails(app_client):
    client = app_client["client"]

    response = client.post(
        "/agent/run",
        json={"prompt": "raise_run_error", "context": {"user_id": "u1", "session_id": "s1"}},
    )

    assert response.status_code == 503
    assert "Google ADK unavailable" in response.json()["detail"]
    assert "model=fake-adk-model" in response.json()["detail"]


def test_complete_onboarding_updates_user_and_bootstrap_context(app_client):
    client = app_client["client"]
    repository = app_client["repository"]

    bootstrap_before = client.post(
        "/agent/bootstrap-device",
        json={"device_id": "device-onboard", "timezone": "Asia/Kolkata"},
    )
    assert bootstrap_before.status_code == 200
    assert bootstrap_before.json()["needs_onboarding"] is True

    completed = client.post(
        "/agent/onboarding/complete",
        json={
            "device_id": "device-onboard",
            "timezone": "Asia/Kolkata",
            "wake_time": "07:30",
            "bedtime": "23:15",
            "playbook": "Ask me for one tiny next step and 10-minute sprint.",
            "health_anchors": ["Breakfast", "Medication"],
        },
    )
    assert completed.status_code == 200
    payload = completed.json()
    assert payload["needs_onboarding"] is False
    assert payload["profile_context"]["wake_time"] == "07:30"
    assert payload["profile_context"]["bedtime"] == "23:15"
    assert payload["profile_context"]["playbook"]["notes"].startswith("Ask me")
    morning_events = [
        event for event in repository.events.values() if event.user_id == "device-onboard" and event.event_type == "morning_wake"
    ]
    assert len(morning_events) == 1
    assert morning_events[0].cron_job_id is not None

    bootstrap_after = client.post(
        "/agent/bootstrap-device",
        json={"device_id": "device-onboard", "timezone": "Asia/Kolkata"},
    )
    assert bootstrap_after.status_code == 200
    after_payload = bootstrap_after.json()
    assert after_payload["needs_onboarding"] is False
    assert after_payload["profile_context"]["wake_time"] == "07:30"
    assert after_payload["profile_context"]["health_anchors"] == [
        "Breakfast",
        "Medication",
    ]


def test_bootstrap_backfills_missing_morning_seed_for_completed_user(app_client):
    client = app_client["client"]
    repository = app_client["repository"]

    completed = client.post(
        "/agent/onboarding/complete",
        json={
            "device_id": "device-bootstrap-seed",
            "timezone": "UTC",
            "wake_time": "07:30",
            "bedtime": "23:15",
            "playbook": "One tiny step first.",
            "health_anchors": ["Breakfast"],
        },
    )
    assert completed.status_code == 200
    repository.events.clear()

    bootstrap_after = client.post(
        "/agent/bootstrap-device",
        json={"device_id": "device-bootstrap-seed", "timezone": "UTC"},
    )
    assert bootstrap_after.status_code == 200
    seeded_events = [
        event
        for event in repository.events.values()
        if event.user_id == "device-bootstrap-seed" and event.event_type == "morning_wake"
    ]
    assert len(seeded_events) == 1
    assert seeded_events[0].cron_job_id is not None


def test_complete_onboarding_succeeds_when_morning_seed_fails(app_client, monkeypatch):
    client = app_client["client"]

    async def _raise_seed_error(*, user_id: str, timezone_name: str, wake_time: str):
        _ = user_id
        _ = timezone_name
        _ = wake_time
        raise RuntimeError("seed failed")

    monkeypatch.setattr(main, "_seed_next_morning_wake_event", _raise_seed_error)

    completed = client.post(
        "/agent/onboarding/complete",
        json={
            "device_id": "device-onboard-seed-fail",
            "timezone": "UTC",
            "wake_time": "07:30",
            "bedtime": "23:15",
            "playbook": "One tiny step first.",
            "health_anchors": ["Breakfast"],
        },
    )
    assert completed.status_code == 200
    payload = completed.json()
    assert payload["needs_onboarding"] is False
    assert payload["profile_context"]["wake_time"] == "07:30"


def test_seed_next_morning_wake_event_handles_insert_conflict_without_duplicates(
    app_client, monkeypatch
):
    repository = app_client["repository"]
    seed_time = main._next_wake_run_at_utc(timezone_name="UTC", wake_time_hhmm="07:30")
    seed_date = main._date_key_for_datetime(seed_time, "UTC")
    existing_event = main.CheckinEventRecord(
        id="existing-morning-event",
        user_id="device-seed-conflict",
        scheduled_time=main._iso_from_dt(seed_time),
        event_type="morning_wake",
        payload={"seed_date": seed_date, "reason": "daily_bootstrap"},
        executed=False,
        cron_job_id=999,
        attempt_count=0,
        created_at=main._iso_now(),
        updated_at=main._iso_now(),
    )

    list_calls = {"count": 0}

    async def _list_future_events(**kwargs):
        _ = kwargs
        list_calls["count"] += 1
        if list_calls["count"] == 1:
            return []
        return [existing_event]

    async def _raise_conflict(payload):
        _ = payload
        raise RuntimeError("Supabase error 409: duplicate key value violates unique constraint")

    monkeypatch.setattr(repository, "list_future_events", _list_future_events)
    monkeypatch.setattr(repository, "create_event", _raise_conflict)

    result = asyncio.run(
        main._seed_next_morning_wake_event(
            user_id="device-seed-conflict",
            timezone_name="UTC",
            wake_time="07:30",
        )
    )

    assert result["seeded"] is False
    assert result["reason"] == "existing_pending_morning_wake"
    assert result["event_id"] == existing_event.id
    assert result["cron_job_id"] == 999


def test_complete_onboarding_backfills_health_anchors_when_missing(app_client):
    client = app_client["client"]

    completed = client.post(
        "/agent/onboarding/complete",
        json={
            "device_id": "device-onboard-default-anchors",
            "timezone": "UTC",
            "wake_time": "07:30",
            "bedtime": "23:15",
            "playbook": "Help me start with one tiny next action.",
            "health_anchors": [],
        },
    )
    assert completed.status_code == 200
    payload = completed.json()
    assert payload["profile_context"]["health_anchors"] == [
        "Morning reset around 07:30",
        "Night shutdown around 23:15",
    ]


def test_complete_onboarding_validates_required_fields(app_client):
    client = app_client["client"]

    response = client.post(
        "/agent/onboarding/complete",
        json={
            "device_id": "device-onboard-invalid",
            "timezone": "UTC",
            "wake_time": "7:30",
            "bedtime": "23:15",
            "playbook": "ok",
            "health_anchors": [],
        },
    )

    assert response.status_code == 400
    assert "wake_time must be HH:MM" in response.json()["detail"]
