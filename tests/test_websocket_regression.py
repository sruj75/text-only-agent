from __future__ import annotations

import asyncio
import json
import uuid

import main


def _bootstrap(
    app_client,
    *,
    device_id: str = "device-ws",
    timezone: str = "Asia/Kolkata",
    session_id: str | None = None,
    complete_onboarding: bool = True,
):
    repository = app_client["repository"]
    target_session_id = session_id or main._daily_session_id(device_id, timezone)
    asyncio.run(repository.ensure_user(user_id=device_id, timezone_name=timezone))
    if complete_onboarding:
        asyncio.run(
            repository.upsert_user_profile(
                user_id=device_id,
                timezone_name=timezone,
                wake_time="07:00",
                bedtime="23:00",
                onboarding_status=main.ONBOARDING_STATUS_COMPLETED,
            )
        )
    asyncio.run(
        main._ensure_session(
            user_id=device_id,
            session_id=target_session_id,
            timezone_name=timezone,
            entry_context=main.EntryContext(source="manual", entry_mode="reactive"),
            state_patch={"thread_type": "daily"},
        )
    )
    return {
        "device_id": device_id,
        "session_id": target_session_id,
    }


def _complete_onboarding(client, *, device_id: str, timezone: str):
    started = client.post(
        "/agent/onboarding/start",
        json={
            "device_id": device_id,
            "timezone": timezone,
        },
    )
    assert started.status_code == 200
    response = client.post(
        "/agent/onboarding/sleep-schedule",
        json={
            "device_id": device_id,
            "timezone": timezone,
            "wake_time": "07:00",
            "bedtime": "23:00",
        },
    )
    assert response.status_code == 200
    return response.json()


def _drain_until_done(ws):
    frames = []
    while True:
        frame = ws.receive_json()
        frames.append(frame)
        if frame.get("type") == "assistant_done":
            return frames


def _drain_until_done_count(ws, target_done_count: int):
    frames = []
    done_count = 0
    while done_count < target_done_count:
        frame = ws.receive_json()
        frames.append(frame)
        if frame.get("type") == "assistant_done":
            done_count += 1
    return frames


def _seed_executed_proactive_event(
    repository,
    *,
    user_id: str,
    event_id: str,
    scheduled_time: str,
    trigger_type: str,
    task_title: str | None = None,
):
    payload = {"trigger_type": trigger_type, "schedule_owner": "task_management"}
    if task_title:
        payload["task_title"] = task_title
    repository.events[event_id] = main.CheckinEventRecord(
        id=event_id,
        user_id=user_id,
        scheduled_time=scheduled_time,
        event_type="checkin",
        payload=payload,
        executed=True,
        cron_job_id=None,
        attempt_count=0,
        created_at=main._iso_now(),
        updated_at=main._iso_now(),
    )


def _init_session(
    ws,
    *,
    session_id: str,
):
    payload = {
        "type": "init",
        "session_id": session_id,
    }
    ws.send_json(payload)
    ready = ws.receive_json()
    assert ready["type"] == "session_ready"
    task_panel = ws.receive_json()
    assert task_panel["type"] == "task_panel_state"
    return ready


def _open_session(
    client,
    *,
    device_id: str,
    timezone: str,
    session_id: str,
    entry_context: dict,
    open_id: str | None = None,
):
    response = client.post(
        "/agent/session/open",
        json={
            "device_id": device_id,
            "timezone": timezone,
            "session_id": session_id,
            "entry_context": entry_context,
            "source": entry_context.get("source", "manual"),
            "open_id": open_id or f"open-{uuid.uuid4().hex}",
            "client_version": main.RELEASE_ID,
            "contract_version": main.CONTRACT_VERSION,
        },
    )
    assert response.status_code == 200
    return response.json()


def _list_messages(repository, session_id: str):
    return [
        message.model_dump()
        for message in asyncio.run(repository.list_messages(session_id=session_id))
    ]


def test_ws_requires_init_before_user_message(app_client):
    client = app_client["client"]
    bootstrap = _bootstrap(app_client)

    with client.websocket_connect(
        f"/agent/ws?device_id={bootstrap['device_id']}&timezone=Asia/Kolkata"
    ) as ws:
        ws.send_json({"type": "user_message", "message_id": "m1", "text": "hello"})
        frame = ws.receive_json()

    assert frame["type"] == "error"
    assert frame["code"] == "not_initialized"


def test_ws_init_rejects_foreign_session_id(app_client):
    client = app_client["client"]
    repository = app_client["repository"]

    owner = _bootstrap(
        app_client,
        device_id="ws-owner",
        timezone="UTC",
        session_id="ws_shared_forbidden",
    )
    assert owner["session_id"] == "ws_shared_forbidden"

    with client.websocket_connect(
        "/agent/ws?device_id=ws-attacker&timezone=UTC"
    ) as ws:
        ws.send_json(
            {
                "type": "init",
                "session_id": "ws_shared_forbidden",
            }
        )
        frame = ws.receive_json()

    assert frame["type"] == "error"
    assert frame["code"] == "session_forbidden"
    assert repository.sessions["ws_shared_forbidden"].user_id == "ws-owner"


def test_ws_streams_and_persists_messages(app_client):
    client = app_client["client"]
    fake_agent = app_client["agent"]
    _complete_onboarding(client, device_id="device-ws", timezone="Asia/Kolkata")
    bootstrap = _bootstrap(app_client)
    _open_session(
        client,
        device_id=bootstrap["device_id"],
        timezone="Asia/Kolkata",
        session_id=bootstrap["session_id"],
        entry_context={
            "source": "push",
            "event_id": "event-22",
            "trigger_type": "checkin",
            "scheduled_time": "2026-03-04T05:30:00Z",
            "calendar_event_id": None,
            "entry_mode": "proactive",
        },
    )

    with client.websocket_connect(
        f"/agent/ws?device_id={bootstrap['device_id']}&timezone=Asia/Kolkata"
    ) as ws:
        _init_session(
            ws,
            session_id=bootstrap["session_id"],
        )

        ws.send_json({"type": "user_message", "message_id": "m2", "text": "what now"})
        frames = _drain_until_done(ws)

    deltas = [f["delta"] for f in frames if f.get("type") == "assistant_delta"]
    done = [f for f in frames if f.get("type") == "assistant_done"][0]

    assert deltas == ["hello", " world"]
    assert done["text"] == "hello world"

    payload = _list_messages(app_client["repository"], bootstrap["session_id"])
    assert [msg["role"] for msg in payload] == ["assistant", "user", "assistant"]
    assert payload[0]["metadata"]["startup_turn"] is True
    assert payload[2]["content"] == "hello world"

    # Regression lock: proactive context must be passed to ADK stream calls.
    stream_context = fake_agent.stream_calls[-1]["context"]
    assert stream_context["entry_mode"] == "proactive"
    assert "due_diligence_time" in stream_context
    assert "due_diligence_schedule" in stream_context
    assert "due_diligence_tasks" in stream_context


def test_ws_blocks_duplicate_message_ids(app_client):
    client = app_client["client"]
    bootstrap = _bootstrap(app_client, device_id="device-dup")

    with client.websocket_connect(
        f"/agent/ws?device_id={bootstrap['device_id']}&timezone=UTC"
    ) as ws:
        _init_session(
            ws,
            session_id=bootstrap["session_id"],
        )

        ws.send_json({"type": "user_message", "message_id": "m-dup", "text": "hello"})
        _drain_until_done(ws)

        ws.send_json({"type": "user_message", "message_id": "m-dup", "text": "hello again"})
        frame = ws.receive_json()

    assert frame["type"] == "error"
    assert frame["code"] == "duplicate_message"


def test_ws_handles_back_to_back_user_messages(app_client):
    client = app_client["client"]
    bootstrap = _bootstrap(app_client, device_id="device-burst")

    with client.websocket_connect(
        f"/agent/ws?device_id={bootstrap['device_id']}&timezone=UTC"
    ) as ws:
        _init_session(
            ws,
            session_id=bootstrap["session_id"],
        )

        ws.send_json({"type": "user_message", "message_id": "m-burst-1", "text": "first"})
        ws.send_json({"type": "user_message", "message_id": "m-burst-2", "text": "second"})
        frames = _drain_until_done_count(ws, target_done_count=2)

    error_frames = [frame for frame in frames if frame.get("type") == "error"]
    done_frames = [frame for frame in frames if frame.get("type") == "assistant_done"]

    assert error_frames == []
    assert len(done_frames) == 2

    payload = _list_messages(app_client["repository"], bootstrap["session_id"])
    assert [msg["role"] for msg in payload] == [
        "user",
        "assistant",
        "user",
        "assistant",
    ]


def test_ws_returns_adk_error_without_followup_assistant_message(app_client):
    client = app_client["client"]
    bootstrap = _bootstrap(app_client, device_id="device-error")

    with client.websocket_connect(
        f"/agent/ws?device_id={bootstrap['device_id']}&timezone=UTC"
    ) as ws:
        _init_session(
            ws,
            session_id=bootstrap["session_id"],
        )

        ws.send_json(
            {
                "type": "user_message",
                "message_id": "m-error",
                "text": "raise_stream_error",
            }
        )
        frame = ws.receive_json()

    assert frame["type"] == "error"
    assert frame["code"] == "adk_error"
    assert "model=fake-adk-model" in frame["detail"]

    payload = _list_messages(app_client["repository"], bootstrap["session_id"])
    assert [msg["role"] for msg in payload] == ["user"]


def test_ws_includes_due_diligence_for_proactive_context(app_client):
    client = app_client["client"]
    fake_agent = app_client["agent"]
    bootstrap = _bootstrap(app_client, device_id="device-post-onboarding")
    _open_session(
        client,
        device_id=bootstrap["device_id"],
        timezone="UTC",
        session_id=bootstrap["session_id"],
        entry_context={
            "source": "push",
            "entry_mode": "proactive",
            "trigger_type": "before_task",
        },
    )

    with client.websocket_connect(
        f"/agent/ws?device_id={bootstrap['device_id']}&timezone=UTC"
    ) as ws:
        _init_session(
            ws,
            session_id=bootstrap["session_id"],
        )

        ws.send_json(
            {
                "type": "user_message",
                "message_id": "m-post-onboarding",
                "text": "help me plan",
            }
        )
        _drain_until_done(ws)

    stream_context = fake_agent.stream_calls[-1]["context"]
    assert stream_context["entry_mode"] == "proactive"
    assert stream_context["trigger_type"] == "before_task"
    assert "due_diligence_time" in stream_context
    assert "due_diligence_schedule" in stream_context
    assert "due_diligence_tasks" in stream_context


def test_ws_skips_task_repository_reads_when_task_mgmt_disabled(app_client, monkeypatch):
    client = app_client["client"]
    repository = app_client["repository"]
    bootstrap = _bootstrap(app_client, device_id="device-task-flag-off")
    list_task_calls = {"count": 0}
    original_list_tasks = repository.list_tasks

    async def tracking_list_tasks(*args, **kwargs):
        list_task_calls["count"] += 1
        return await original_list_tasks(*args, **kwargs)

    monkeypatch.setattr(main, "TASK_MGMT_V1_ENABLED", False)
    repository.list_tasks = tracking_list_tasks  # type: ignore[assignment]

    with client.websocket_connect(
        f"/agent/ws?device_id={bootstrap['device_id']}&timezone=UTC"
    ) as ws:
        _init_session(
            ws,
            session_id=bootstrap["session_id"],
        )

    assert list_task_calls["count"] == 0


def test_ws_includes_profile_context_from_onboarding(app_client):
    client = app_client["client"]
    fake_agent = app_client["agent"]

    _complete_onboarding(client, device_id="device-profile", timezone="UTC")

    bootstrap = _bootstrap(app_client, device_id="device-profile")
    with client.websocket_connect(
        f"/agent/ws?device_id={bootstrap['device_id']}&timezone=UTC"
    ) as ws:
        _init_session(
            ws,
            session_id=bootstrap["session_id"],
        )

        ws.send_json(
            {
                "type": "user_message",
                "message_id": "m-profile",
                "text": "help",
            }
        )
        _drain_until_done(ws)

    stream_context = fake_agent.stream_calls[-1]["context"]
    profile_context = json.loads(stream_context["profile_context"])
    assert profile_context["wake_time"] == "07:00"
    assert profile_context["bedtime"] == "23:00"


def test_ws_onboarding_mode_skips_task_write_guardrail_rewrites(app_client, monkeypatch):
    client = app_client["client"]
    repository = app_client["repository"]
    device_id = "device-onboarding-guardrail"
    timezone = "UTC"

    started = client.post(
        "/agent/onboarding/start",
        json={"device_id": device_id, "timezone": timezone},
    )
    assert started.status_code == 200

    target_session_id = main._daily_session_id(device_id, timezone)
    asyncio.run(
        main._ensure_session(
            user_id=device_id,
            session_id=target_session_id,
            timezone_name=timezone,
            entry_context=main.EntryContext(source="manual", entry_mode="reactive"),
            state_patch={"thread_type": "daily"},
        )
    )

    monkeypatch.setattr(main, "_looks_like_schedule_claim", lambda _text: True)

    with client.websocket_connect(f"/agent/ws?device_id={device_id}&timezone={timezone}") as ws:
        _init_session(ws, session_id=target_session_id)
        ws.send_json({"type": "user_message", "message_id": "m-onboarding", "text": "my bedtime is 10pm"})
        frames = _drain_until_done(ws)

    done = next(frame for frame in frames if frame.get("type") == "assistant_done")
    assert done["text"] == "hello world"
    saved_messages = _list_messages(repository, target_session_id)
    assert saved_messages[-1]["content"] == "hello world"


def test_session_open_creates_startup_message_for_empty_reactive_thread(app_client):
    client = app_client["client"]
    fake_agent = app_client["agent"]
    _complete_onboarding(client, device_id="device-init-starter", timezone="UTC")
    bootstrap = _bootstrap(app_client, device_id="device-init-starter")
    opened = _open_session(
        client,
        device_id=bootstrap["device_id"],
        timezone="UTC",
        session_id=bootstrap["session_id"],
        entry_context={"source": "manual", "entry_mode": "reactive"},
    )

    assert opened["startup_status"] == "succeeded"
    assert opened["messages"][0]["content"] == "hello world"
    startup_context = fake_agent.run_calls[0]["context"]
    assert startup_context["entry_mode"] == "reactive"
    assert "due_diligence_time" in startup_context
    assert "due_diligence_schedule" in startup_context
    assert "due_diligence_tasks" in startup_context
    assert fake_agent.run_calls[0]["prompt"].startswith("Conversation bootstrap:")

    payload = _list_messages(app_client["repository"], bootstrap["session_id"])
    assert [msg["role"] for msg in payload] == ["assistant"]


def test_session_open_is_idempotent_when_history_exists(app_client):
    client = app_client["client"]
    fake_agent = app_client["agent"]
    _complete_onboarding(client, device_id="device-repeat-startup", timezone="UTC")
    bootstrap = _bootstrap(app_client, device_id="device-repeat-startup")
    open_id = f"open-{uuid.uuid4().hex}"
    first = _open_session(
        client,
        device_id=bootstrap["device_id"],
        timezone="UTC",
        session_id=bootstrap["session_id"],
        entry_context={"source": "manual", "entry_mode": "reactive"},
        open_id=open_id,
    )
    assert first["startup_status"] == "succeeded"

    with client.websocket_connect(
        f"/agent/ws?device_id={bootstrap['device_id']}&timezone=UTC"
    ) as ws:
        _init_session(
            ws,
            session_id=bootstrap["session_id"],
        )
        ws.send_json({"type": "user_message", "message_id": "m-repeat", "text": "help me"})
        _drain_until_done(ws)

    second = _open_session(
        client,
        device_id=bootstrap["device_id"],
        timezone="UTC",
        session_id=bootstrap["session_id"],
        entry_context={"source": "manual", "entry_mode": "reactive"},
        open_id=open_id,
    )
    assert second["startup_status"] == "succeeded"
    thread_messages = _list_messages(app_client["repository"], bootstrap["session_id"])
    startup_messages = [
        message
        for message in thread_messages
        if bool((message.get("metadata") or {}).get("startup_turn"))
    ]
    assert len(startup_messages) == 1
    assert len(fake_agent.run_calls) == 1


def test_ws_init_acks_session_without_history_sync(app_client):
    client = app_client["client"]
    bootstrap = _bootstrap(app_client, device_id="device-cursor-sync")
    opened = _open_session(
        client,
        device_id=bootstrap["device_id"],
        timezone="UTC",
        session_id=bootstrap["session_id"],
        entry_context={"source": "manual", "entry_mode": "reactive"},
    )
    with client.websocket_connect(
        f"/agent/ws?device_id={bootstrap['device_id']}&timezone=UTC"
    ) as ws:
        ws.send_json(
            {
                "type": "init",
                "session_id": bootstrap["session_id"],
            }
        )
        ready = ws.receive_json()
        assert ready["type"] == "session_ready"
        ws.receive_json()  # task_panel_state
        ws.send_json({"type": "user_message", "message_id": "m-init-ack", "text": "hello"})
        frames = _drain_until_done(ws)

    done = [frame for frame in frames if frame.get("type") == "assistant_done"]
    assert len(done) == 1


def test_session_open_does_not_rewrite_proactive_trigger_context(app_client):
    client = app_client["client"]
    fake_agent = app_client["agent"]
    device_id = "device-proactive-context"

    _complete_onboarding(client, device_id=device_id, timezone="UTC")
    bootstrap = _bootstrap(app_client, device_id=device_id)
    daily_session_id = bootstrap["session_id"]

    _open_session(
        client,
        device_id=device_id,
        timezone="UTC",
        session_id=daily_session_id,
        entry_context={
            "source": "push",
            "entry_mode": "proactive",
            "trigger_type": "before_task",
        },
    )
    assert fake_agent.run_calls[0]["context"]["trigger_type"] == "before_task"

    second_session_id = "session_device-proactive-context_secondary"

    _open_session(
        client,
        device_id=device_id,
        timezone="UTC",
        session_id=second_session_id,
        entry_context={
            "source": "push",
            "entry_mode": "proactive",
            "trigger_type": "before_task",
        },
    )
    second_context = fake_agent.run_calls[1]["context"]
    assert second_context["entry_mode"] == "proactive"
    assert second_context["trigger_type"] == "before_task"


def test_session_open_proactive_ack_tracks_only_current_event_and_surfaces_missed_today_once(app_client):
    client = app_client["client"]
    repository = app_client["repository"]
    fake_agent = app_client["agent"]
    _complete_onboarding(client, device_id="device-missed-ack", timezone="UTC")
    bootstrap = _bootstrap(app_client, device_id="device-missed-ack")
    session_id = bootstrap["session_id"]
    device_id = bootstrap["device_id"]

    now = main._utc_now()
    missed_event_id = f"event-{uuid.uuid4()}"
    current_event_id = f"event-{uuid.uuid4()}"
    _seed_executed_proactive_event(
        repository,
        user_id=device_id,
        event_id=missed_event_id,
        scheduled_time=main._iso_from_dt(now - main.timedelta(hours=2)),
        trigger_type="before_task",
        task_title="Prepare deck",
    )
    _seed_executed_proactive_event(
        repository,
        user_id=device_id,
        event_id=current_event_id,
        scheduled_time=main._iso_from_dt(now - main.timedelta(hours=1)),
        trigger_type="before_task",
        task_title="Current task",
    )

    _open_session(
        client,
        device_id=device_id,
        timezone="UTC",
        session_id=session_id,
        entry_context={
            "source": "push",
            "entry_mode": "proactive",
            "event_id": current_event_id,
            "trigger_type": "before_task",
            "scheduled_time": main._iso_from_dt(now - main.timedelta(minutes=5)),
        },
    )

    startup_context = fake_agent.run_calls[0]["context"]
    assert startup_context["missed_proactive_count"] == "1"
    missed_events = json.loads(startup_context["missed_proactive_events"])
    assert [event["event_id"] for event in missed_events] == [missed_event_id]

    state = repository.sessions[session_id].state
    assert state[main.PROACTIVE_ACK_STATE_KEY] == [current_event_id]
    assert state[main.MISSED_REPORTED_STATE_KEY] == [missed_event_id]


def test_session_open_missed_proactive_is_reported_once_and_reactive_open_stays_reactive(app_client):
    client = app_client["client"]
    repository = app_client["repository"]
    fake_agent = app_client["agent"]
    _complete_onboarding(client, device_id="device-missed-once", timezone="UTC")
    bootstrap = _bootstrap(app_client, device_id="device-missed-once")
    session_id = bootstrap["session_id"]
    device_id = bootstrap["device_id"]
    open_id = f"open-{uuid.uuid4().hex}"

    missed_event_id = f"event-{uuid.uuid4()}"
    now = main._utc_now()
    _seed_executed_proactive_event(
        repository,
        user_id=device_id,
        event_id=missed_event_id,
        scheduled_time=main._iso_from_dt(now - main.timedelta(hours=1)),
        trigger_type="before_task",
        task_title="Write notes",
    )

    _open_session(
        client,
        device_id=device_id,
        timezone="UTC",
        session_id=session_id,
        entry_context={"source": "manual", "entry_mode": "reactive"},
        open_id=open_id,
    )
    first_startup_context = fake_agent.run_calls[0]["context"]
    assert first_startup_context["entry_mode"] == "reactive"
    assert first_startup_context["missed_proactive_count"] == "1"

    # idempotency guard: second open does not regenerate startup and does not duplicate reported ids.
    _open_session(
        client,
        device_id=device_id,
        timezone="UTC",
        session_id=session_id,
        entry_context={"source": "manual", "entry_mode": "reactive"},
        open_id=open_id,
    )
    assert len(fake_agent.run_calls) == 1
    state = repository.sessions[session_id].state
    assert state[main.MISSED_REPORTED_STATE_KEY] == [missed_event_id]
