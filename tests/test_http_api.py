from __future__ import annotations

import asyncio
from datetime import datetime
import uuid
from zoneinfo import ZoneInfo

import main


def _session_open_payload(
    *,
    device_id: str,
    timezone: str | None = None,
    source: str = "manual",
    open_id: str | None = None,
    **extra,
):
    payload = {
        "device_id": device_id,
        "source": source,
        "open_id": open_id or f"open-{uuid.uuid4().hex}",
        "client_version": main.RELEASE_ID,
        "contract_version": main.CONTRACT_VERSION,
    }
    if timezone is not None:
        payload["timezone"] = timezone
    payload.update(extra)
    return payload


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


def test_session_open_creates_daily_session_and_reuses_it(app_client):
    client = app_client["client"]
    timezone_name = "Asia/Kolkata"
    date_key = datetime.now(ZoneInfo(timezone_name)).strftime("%Y-%m-%d")

    first = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-alpha", timezone=timezone_name),
    )
    second = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-alpha", timezone=timezone_name),
    )

    assert first.status_code == 200
    assert second.status_code == 200
    first_payload = first.json()
    second_payload = second.json()

    assert first_payload["session_id"] == f"session_device-alpha_{date_key}"
    assert second_payload["session_id"] == first_payload["session_id"]
    assert first_payload["messages"] == []
    assert first_payload["needs_onboarding"] is True


def test_session_open_accepts_explicit_session_id(app_client):
    client = app_client["client"]

    response = client.post(
        "/agent/session/open",
        json=_session_open_payload(
            device_id="device-beta",
            timezone="UTC",
            session_id="session_device-beta_custom_1",
            source="push",
            entry_context={
                "source": "push",
                "event_id": "event-1",
                "trigger_type": "checkin",
                "scheduled_time": "2026-03-04T10:00:00Z",
                "calendar_event_id": None,
                "entry_mode": "proactive",
            },
        ),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["session_id"] == "session_device-beta_custom_1"


def test_session_open_requires_timezone_when_user_has_none(app_client):
    client = app_client["client"]

    response = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-no-tz", timezone=None),
    )

    assert response.status_code == 400
    assert "valid iana timezone is required" in response.json()["error"]["message"].lower()


def test_session_open_uses_stored_timezone_when_missing_in_request(app_client):
    client = app_client["client"]
    repository = app_client["repository"]

    first = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-stored-tz", timezone="Asia/Kolkata"),
    )
    assert first.status_code == 200

    second = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-stored-tz", timezone=None),
    )
    assert second.status_code == 200
    assert repository.users["device-stored-tz"]["timezone"] == "Asia/Kolkata"


def test_session_open_rejects_foreign_session_id(app_client):
    client = app_client["client"]
    repository = app_client["repository"]

    first = client.post(
        "/agent/session/open",
        json=_session_open_payload(
            device_id="device-owner",
            timezone="UTC",
            session_id="session_shared_forbidden",
        ),
    )
    assert first.status_code == 200

    second = client.post(
        "/agent/session/open",
        json=_session_open_payload(
            device_id="device-attacker",
            timezone="UTC",
            session_id="session_shared_forbidden",
        ),
    )

    assert second.status_code == 403
    assert second.json()["error"]["message"] == "Session does not belong to device"
    assert repository.sessions["session_shared_forbidden"].user_id == "device-owner"


def test_session_open_rejects_contract_version_mismatch(app_client):
    client = app_client["client"]

    response = client.post(
        "/agent/session/open",
        json=_session_open_payload(
            device_id="device-contract-mismatch",
            timezone="UTC",
            contract_version="1999-01-01",
        ),
    )

    assert response.status_code == 409
    payload = response.json()["error"]
    assert payload["code"] == "CONTRACT_VERSION_MISMATCH"


def test_legacy_bootstrap_route_maps_to_session_open(app_client):
    client = app_client["client"]

    response = client.post(
        "/agent/bootstrap-device",
        json={"device_id": "device-old", "timezone": "Asia/Kolkata"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["device_id"] == "device-old"
    assert payload["user_id"] == "device-old"
    assert payload["timezone"] == "Asia/Kolkata"
    assert payload["session_id"].startswith("session_device-old_")
    assert payload["threads"] == []
    assert payload["needs_onboarding"] is True
    assert isinstance(payload["messages"], list)


def test_execute_event_runs_in_cloud_run_endpoint(app_client, monkeypatch):
    client = app_client["client"]
    repository = app_client["repository"]
    monkeypatch.setenv("SCHEDULER_SECRET", "scheduler-test-secret")

    asyncio.run(repository.ensure_user(user_id="device-event", timezone_name="UTC"))
    asyncio.run(repository.upsert_push_token(user_id="device-event", expo_push_token="ExponentPushToken[test]"))
    asyncio.run(
        repository.create_event(
            main.CheckinEventRecord(
                id="event-execute-1",
                user_id="device-event",
                scheduled_time=main._iso_now(),
                event_type="checkin",
                payload={"trigger_type": "before_task", "timezone": "UTC"},
                executed=False,
            )
        )
    )

    async def _always_send_push(**kwargs):
        _ = kwargs
        return True

    monkeypatch.setattr(main, "_send_push_notification", _always_send_push)

    response = client.post(
        "/agent/events/execute",
        json={"event_id": "event-execute-1"},
        headers={"x-scheduler-secret": "scheduler-test-secret"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "executed"
    assert payload["event_id"] == "event-execute-1"
    assert repository.events["event-execute-1"].executed is True
    assert repository.events["event-execute-1"].workflow_state == "succeeded"


def test_execute_event_dead_letters_after_max_attempts(app_client, monkeypatch):
    client = app_client["client"]
    repository = app_client["repository"]
    monkeypatch.setenv("SCHEDULER_SECRET", "scheduler-test-secret")

    asyncio.run(repository.ensure_user(user_id="device-dead", timezone_name="UTC"))
    asyncio.run(
        repository.create_event(
            main.CheckinEventRecord(
                id="event-dead-letter",
                user_id="device-dead",
                scheduled_time=main._iso_now(),
                event_type="checkin",
                payload={"trigger_type": "before_task", "timezone": "UTC"},
                executed=False,
                attempt_count=main.MAX_DELIVERY_ATTEMPTS - 1,
            )
        )
    )

    async def _never_send_push(**kwargs):
        _ = kwargs
        return False

    monkeypatch.setattr(main, "_send_push_notification", _never_send_push)

    response = client.post(
        "/agent/events/execute",
        json={"event_id": "event-dead-letter"},
        headers={"x-scheduler-secret": "scheduler-test-secret"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "dead_letter"
    assert repository.events["event-dead-letter"].executed is False
    assert repository.events["event-dead-letter"].workflow_state == "dead_letter"


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
    assert "Google ADK unavailable" in response.json()["error"]["message"]
    assert "model=fake-adk-model" in response.json()["error"]["message"]


def test_session_open_generates_startup_message_with_full_history(app_client):
    client = app_client["client"]
    completed = client.post(
        "/agent/onboarding/complete",
        json={
            "device_id": "device-session-open",
            "timezone": "UTC",
            "wake_time": "07:00",
            "bedtime": "23:00",
            "playbook": "Start with one tiny step.",
            "health_anchors": ["Breakfast"],
        },
    )
    assert completed.status_code == 200
    opened = client.post(
        "/agent/session/open",
        json=_session_open_payload(
            device_id="device-session-open",
            timezone="UTC",
            entry_context={"source": "manual", "entry_mode": "reactive"},
        ),
    )
    assert opened.status_code == 200
    payload = opened.json()
    assert payload["startup_status"] == "succeeded"
    assert len(payload["messages"]) == 1
    assert payload["messages"][0]["role"] == "assistant"
    assert payload["messages"][0]["metadata"]["startup_turn"] is True


def test_session_open_is_idempotent_for_same_start_request(app_client):
    client = app_client["client"]
    open_id = f"open-{uuid.uuid4().hex}"
    completed = client.post(
        "/agent/onboarding/complete",
        json={
            "device_id": "device-session-idempotent",
            "timezone": "UTC",
            "wake_time": "07:00",
            "bedtime": "23:00",
            "playbook": "Start with one tiny step.",
            "health_anchors": ["Breakfast"],
        },
    )
    assert completed.status_code == 200
    first = client.post(
        "/agent/session/open",
        json=_session_open_payload(
            device_id="device-session-idempotent",
            timezone="UTC",
            open_id=open_id,
            entry_context={"source": "manual", "entry_mode": "reactive"},
        ),
    )
    assert first.status_code == 200

    second = client.post(
        "/agent/session/open",
        json=_session_open_payload(
            device_id="device-session-idempotent",
            timezone="UTC",
            open_id=open_id,
            session_id=first.json()["session_id"],
            entry_context={"source": "manual", "entry_mode": "reactive"},
        ),
    )
    assert second.status_code == 200
    startup_messages = [
        message
        for message in second.json()["messages"]
        if bool((message.get("metadata") or {}).get("startup_turn"))
    ]
    assert len(startup_messages) == 1


def test_session_open_generates_new_startup_for_new_open_cycle(app_client):
    client = app_client["client"]
    completed = client.post(
        "/agent/onboarding/complete",
        json={
            "device_id": "device-new-open-cycle",
            "timezone": "UTC",
            "wake_time": "07:00",
            "bedtime": "23:00",
            "playbook": "Start with one tiny step.",
            "health_anchors": ["Breakfast"],
        },
    )
    assert completed.status_code == 200

    first = client.post(
        "/agent/session/open",
        json=_session_open_payload(
            device_id="device-new-open-cycle",
            timezone="UTC",
            open_id="open-cycle-1",
            entry_context={"source": "manual", "entry_mode": "reactive"},
        ),
    )
    second = client.post(
        "/agent/session/open",
        json=_session_open_payload(
            device_id="device-new-open-cycle",
            timezone="UTC",
            open_id="open-cycle-2",
            session_id=first.json()["session_id"],
            entry_context={"source": "manual", "entry_mode": "reactive"},
        ),
    )
    assert first.status_code == 200
    assert second.status_code == 200
    startup_messages = [
        message
        for message in second.json()["messages"]
        if bool((message.get("metadata") or {}).get("startup_turn"))
    ]
    assert len(startup_messages) == 2


def test_session_open_only_marks_missed_events_when_startup_is_generated(app_client):
    client = app_client["client"]
    repository = app_client["repository"]
    fake_agent = app_client["agent"]
    device_id = "device-session-missed-gating"
    open_id = f"open-{uuid.uuid4().hex}"

    completed = client.post(
        "/agent/onboarding/complete",
        json={
            "device_id": device_id,
            "timezone": "UTC",
            "wake_time": "07:00",
            "bedtime": "23:00",
            "playbook": "Start with one tiny step.",
            "health_anchors": ["Breakfast"],
        },
    )
    assert completed.status_code == 200

    first_open = client.post(
        "/agent/session/open",
        json=_session_open_payload(
            device_id=device_id,
            timezone="UTC",
            open_id=open_id,
            entry_context={"source": "manual", "entry_mode": "reactive"},
        ),
    )
    assert first_open.status_code == 200
    assert len(fake_agent.run_calls) == 1

    missed_event_id = "event-missed-after-startup"
    now = main._utc_now()
    asyncio.run(
        repository.create_event(
            main.CheckinEventRecord(
                id=missed_event_id,
                user_id=device_id,
                scheduled_time=main._iso_from_dt(now - main.timedelta(minutes=10)),
                event_type="checkin",
                payload={"trigger_type": "before_task", "task_title": "Deep work"},
                executed=True,
            )
        )
    )

    second_open = client.post(
        "/agent/session/open",
        json=_session_open_payload(
            device_id=device_id,
            timezone="UTC",
            open_id=open_id,
            session_id=first_open.json()["session_id"],
            entry_context={"source": "manual", "entry_mode": "reactive"},
        ),
    )
    assert second_open.status_code == 200
    assert len(fake_agent.run_calls) == 1

    state = repository.sessions[first_open.json()["session_id"]].state
    assert state.get(main.MISSED_REPORTED_STATE_KEY) in (None, [])


def test_complete_onboarding_updates_user_and_session_open_context(app_client):
    client = app_client["client"]
    repository = app_client["repository"]

    opened_before = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-onboard", timezone="Asia/Kolkata"),
    )
    assert opened_before.status_code == 200
    assert opened_before.json()["needs_onboarding"] is True

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
    assert morning_events[0].cloud_task_name is not None

    opened_after = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-onboard", timezone="Asia/Kolkata"),
    )
    assert opened_after.status_code == 200
    after_payload = opened_after.json()
    assert after_payload["needs_onboarding"] is False
    assert after_payload["profile_context"]["wake_time"] == "07:30"
    assert after_payload["profile_context"]["health_anchors"] == [
        "Breakfast",
        "Medication",
    ]


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
        cloud_task_name="projects/local/locations/local/queues/default/tasks/999",
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
    assert result["cloud_task_name"] == "projects/local/locations/local/queues/default/tasks/999"


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
    assert "wake_time must be HH:MM" in response.json()["error"]["message"]
