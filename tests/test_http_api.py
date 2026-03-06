from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo


def test_health_endpoint(app_client):
    client = app_client["client"]

    response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["repository_mode"] == "in_memory"


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


def test_complete_onboarding_updates_user_and_bootstrap_context(app_client):
    client = app_client["client"]

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
