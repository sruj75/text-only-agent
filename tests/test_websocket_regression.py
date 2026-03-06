from __future__ import annotations

import json


def _bootstrap(client, device_id: str = "device-ws"):
    response = client.post(
        "/agent/bootstrap-device",
        json={"device_id": device_id, "timezone": "Asia/Kolkata"},
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


def _init_session(
    ws,
    *,
    device_id: str,
    session_id: str,
    entry_context: dict,
):
    ws.send_json(
        {
            "type": "init",
            "device_id": device_id,
            "session_id": session_id,
            "entry_context": entry_context,
        }
    )
    ready = ws.receive_json()
    assert ready["type"] == "session_ready"
    startup_frames = _drain_until_done(ws)
    return ready, startup_frames


def test_ws_requires_init_before_user_message(app_client):
    client = app_client["client"]
    bootstrap = _bootstrap(client)

    with client.websocket_connect(
        f"/agent/ws?device_id={bootstrap['device_id']}&session_id={bootstrap['session_id']}&timezone=Asia/Kolkata"
    ) as ws:
        ws.send_json({"type": "user_message", "message_id": "m1", "text": "hello"})
        frame = ws.receive_json()

    assert frame["type"] == "error"
    assert frame["code"] == "not_initialized"


def test_ws_init_rejects_foreign_session_id(app_client):
    client = app_client["client"]
    repository = app_client["repository"]

    owner = client.post(
        "/agent/bootstrap-device",
        json={
            "device_id": "ws-owner",
            "timezone": "UTC",
            "session_id": "ws_shared_forbidden",
        },
    )
    assert owner.status_code == 200

    with client.websocket_connect(
        "/agent/ws?device_id=ws-attacker&session_id=ws_shared_forbidden&timezone=UTC"
    ) as ws:
        ws.send_json(
            {
                "type": "init",
                "device_id": "ws-attacker",
                "session_id": "ws_shared_forbidden",
                "entry_context": {"source": "manual", "entry_mode": "reactive"},
            }
        )
        frame = ws.receive_json()

    assert frame["type"] == "error"
    assert frame["code"] == "session_forbidden"
    assert repository.sessions["ws_shared_forbidden"].user_id == "ws-owner"


def test_ws_streams_and_persists_messages(app_client):
    client = app_client["client"]
    fake_agent = app_client["agent"]
    bootstrap = _bootstrap(client)

    with client.websocket_connect(
        f"/agent/ws?device_id={bootstrap['device_id']}&session_id={bootstrap['session_id']}&timezone=Asia/Kolkata&entry_mode=proactive"
    ) as ws:
        _, startup_frames = _init_session(
            ws,
            device_id=bootstrap["device_id"],
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
        startup_done = [f for f in startup_frames if f.get("type") == "assistant_done"][0]
        assert startup_done["text"] == "hello world"

        ws.send_json({"type": "user_message", "message_id": "m2", "text": "what now"})
        frames = _drain_until_done(ws)

    deltas = [f["delta"] for f in frames if f.get("type") == "assistant_delta"]
    done = [f for f in frames if f.get("type") == "assistant_done"][0]

    assert deltas == ["hello", " world"]
    assert done["text"] == "hello world"

    messages = client.get(
        f"/agent/threads/{bootstrap['session_id']}/messages",
        params={"device_id": bootstrap["device_id"]},
    )
    assert messages.status_code == 200
    payload = messages.json()["messages"]
    assert [msg["role"] for msg in payload] == ["assistant", "user", "assistant"]
    assert payload[0]["metadata"]["startup_turn"] is True
    assert payload[2]["content"] == "hello world"

    # Regression lock: proactive context must be passed to ADK stream calls.
    stream_context = fake_agent.stream_calls[-1]["context"]
    assert stream_context["entry_mode"] == "proactive"
    assert "due_diligence_time" in stream_context
    assert "due_diligence_schedule" in stream_context


def test_ws_blocks_duplicate_message_ids(app_client):
    client = app_client["client"]
    bootstrap = _bootstrap(client, device_id="device-dup")

    with client.websocket_connect(
        f"/agent/ws?device_id={bootstrap['device_id']}&session_id={bootstrap['session_id']}&timezone=UTC"
    ) as ws:
        _init_session(
            ws,
            device_id=bootstrap["device_id"],
            session_id=bootstrap["session_id"],
            entry_context={"source": "manual", "entry_mode": "reactive"},
        )

        ws.send_json({"type": "user_message", "message_id": "m-dup", "text": "hello"})
        _drain_until_done(ws)

        ws.send_json({"type": "user_message", "message_id": "m-dup", "text": "hello again"})
        frame = ws.receive_json()

    assert frame["type"] == "error"
    assert frame["code"] == "duplicate_message"


def test_ws_returns_adk_error_without_followup_assistant_message(app_client):
    client = app_client["client"]
    bootstrap = _bootstrap(client, device_id="device-error")

    with client.websocket_connect(
        f"/agent/ws?device_id={bootstrap['device_id']}&session_id={bootstrap['session_id']}&timezone=UTC"
    ) as ws:
        _init_session(
            ws,
            device_id=bootstrap["device_id"],
            session_id=bootstrap["session_id"],
            entry_context={"source": "manual", "entry_mode": "reactive"},
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

    messages = client.get(
        f"/agent/threads/{bootstrap['session_id']}/messages",
        params={"device_id": bootstrap["device_id"]},
    )
    assert messages.status_code == 200
    payload = messages.json()["messages"]
    assert [msg["role"] for msg in payload] == ["assistant", "user"]


def test_ws_skips_due_diligence_for_post_onboarding(app_client):
    client = app_client["client"]
    fake_agent = app_client["agent"]
    bootstrap = _bootstrap(client, device_id="device-post-onboarding")

    with client.websocket_connect(
        f"/agent/ws?device_id={bootstrap['device_id']}&session_id={bootstrap['session_id']}&timezone=UTC&entry_mode=proactive"
    ) as ws:
        _init_session(
            ws,
            device_id=bootstrap["device_id"],
            session_id=bootstrap["session_id"],
            entry_context={
                "source": "push",
                "entry_mode": "proactive",
                "trigger_type": "post_onboarding",
            },
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
    assert stream_context["trigger_type"] == "post_onboarding"
    assert "due_diligence_time" not in stream_context
    assert "due_diligence_schedule" not in stream_context


def test_ws_includes_profile_context_from_onboarding(app_client):
    client = app_client["client"]
    fake_agent = app_client["agent"]

    completed = client.post(
        "/agent/onboarding/complete",
        json={
            "device_id": "device-profile",
            "timezone": "UTC",
            "wake_time": "07:00",
            "bedtime": "23:00",
            "playbook": "Start with one tiny step.",
            "health_anchors": ["Breakfast"],
        },
    )
    assert completed.status_code == 200

    bootstrap = _bootstrap(client, device_id="device-profile")
    with client.websocket_connect(
        f"/agent/ws?device_id={bootstrap['device_id']}&session_id={bootstrap['session_id']}&timezone=UTC"
    ) as ws:
        _init_session(
            ws,
            device_id=bootstrap["device_id"],
            session_id=bootstrap["session_id"],
            entry_context={"source": "manual", "entry_mode": "reactive"},
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


def test_ws_init_starts_with_assistant_on_empty_reactive_thread(app_client):
    client = app_client["client"]
    fake_agent = app_client["agent"]
    bootstrap = _bootstrap(client, device_id="device-init-starter")

    with client.websocket_connect(
        f"/agent/ws?device_id={bootstrap['device_id']}&session_id={bootstrap['session_id']}&timezone=UTC"
    ) as ws:
        _, startup_frames = _init_session(
            ws,
            device_id=bootstrap["device_id"],
            session_id=bootstrap["session_id"],
            entry_context={"source": "manual", "entry_mode": "reactive"},
        )

    startup_done = [frame for frame in startup_frames if frame.get("type") == "assistant_done"][0]
    assert startup_done["text"] == "hello world"
    assert fake_agent.stream_calls[0]["context"]["entry_mode"] == "reactive"
    assert fake_agent.stream_calls[0]["prompt"].startswith("Conversation bootstrap:")

    messages = client.get(
        f"/agent/threads/{bootstrap['session_id']}/messages",
        params={"device_id": bootstrap["device_id"]},
    )
    assert messages.status_code == 200
    payload = messages.json()["messages"]
    assert [msg["role"] for msg in payload] == ["assistant"]


def test_ws_post_onboarding_handoff_runs_once_per_lifetime(app_client):
    client = app_client["client"]
    fake_agent = app_client["agent"]
    device_id = "device-post-onboarding-once"

    completed = client.post(
        "/agent/onboarding/complete",
        json={
            "device_id": device_id,
            "timezone": "UTC",
            "wake_time": "07:00",
            "bedtime": "23:00",
            "playbook": "Plan one step at a time.",
            "health_anchors": ["Breakfast"],
        },
    )
    assert completed.status_code == 200
    bootstrap = _bootstrap(client, device_id=device_id)
    daily_session_id = bootstrap["session_id"]

    with client.websocket_connect(
        f"/agent/ws?device_id={device_id}&session_id={daily_session_id}&timezone=UTC"
    ) as ws:
        _init_session(
            ws,
            device_id=device_id,
            session_id=daily_session_id,
            entry_context={
                "source": "push",
                "entry_mode": "proactive",
                "trigger_type": "post_onboarding",
            },
        )

    assert fake_agent.stream_calls[0]["context"]["trigger_type"] == "post_onboarding"

    created = client.post(
        "/agent/threads",
        json={"device_id": device_id, "timezone": "UTC", "title": "Second thread"},
    )
    assert created.status_code == 200
    second_session_id = created.json()["thread"]["session_id"]

    with client.websocket_connect(
        f"/agent/ws?device_id={device_id}&session_id={second_session_id}&timezone=UTC"
    ) as ws:
        _init_session(
            ws,
            device_id=device_id,
            session_id=second_session_id,
            entry_context={
                "source": "push",
                "entry_mode": "proactive",
                "trigger_type": "post_onboarding",
            },
        )

    second_context = fake_agent.stream_calls[1]["context"]
    assert second_context["entry_mode"] == "reactive"
    assert second_context["trigger_type"] == ""
