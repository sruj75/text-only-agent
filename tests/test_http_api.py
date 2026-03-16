from __future__ import annotations

import asyncio
from datetime import datetime
import types
import uuid
from zoneinfo import ZoneInfo

import main


class FakeLiveKitCoordinator:
    def __init__(
        self,
        *,
        dispatch_mode: str = "explicit",
        fallback_used: bool = False,
        diagnostic_code: str | None = None,
    ) -> None:
        self.url = "wss://example.livekit.cloud"
        self.dispatch_mode = dispatch_mode
        self.fallback_used = fallback_used
        self.diagnostic_code = diagnostic_code
        self.ensure_room_calls: list[dict[str, object]] = []
        self.dispatch_calls: list[dict[str, object]] = []

    async def ensure_room(self, *, room_name: str, metadata: dict[str, object]) -> None:
        self.ensure_room_calls.append({"room_name": room_name, "metadata": metadata})

    async def dispatch_agent(self, *, room_name: str, metadata: dict[str, object]):
        self.dispatch_calls.append({"room_name": room_name, "metadata": metadata})
        return type(
            "DispatchResult",
            (),
            {
                "mode": self.dispatch_mode,
                "fallback_used": self.fallback_used,
                "diagnostic_code": self.diagnostic_code,
                "diagnostic": (
                    {"diagnostic_code": self.diagnostic_code}
                    if self.diagnostic_code
                    else None
                ),
            },
        )()

    def participant_identity(self, *, device_id: str, session_id: str) -> str:
        return f"user-{device_id}-{session_id}"

    def build_participant_token(
        self,
        *,
        room_name: str,
        participant_identity: str,
        participant_name: str,
        participant_metadata: dict[str, object],
        dispatch_metadata: dict[str, object],
        use_token_dispatch_fallback: bool,
    ) -> tuple[str, str]:
        del room_name
        del participant_identity
        del participant_name
        del participant_metadata
        del dispatch_metadata
        return (
            "livekit-token",
            "2026-03-13T00:00:00+00:00" if not use_token_dispatch_fallback else "2026-03-13T00:30:00+00:00",
        )


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


def _start_and_configure_onboarding(
    client,
    *,
    device_id: str,
    timezone: str,
    wake_time: str = "07:00",
    bedtime: str = "23:00",
):
    started = client.post(
        "/agent/onboarding/start",
        json={
            "device_id": device_id,
            "timezone": timezone,
        },
    )
    assert started.status_code == 200
    scheduled = client.post(
        "/agent/onboarding/sleep-schedule",
        json={
            "device_id": device_id,
            "timezone": timezone,
            "wake_time": wake_time,
            "bedtime": bedtime,
        },
    )
    assert scheduled.status_code == 200
    return scheduled


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



def test_conversation_turn_streams_deltas_through_session_ws(app_client):
    client = app_client["client"]
    _start_and_configure_onboarding(client, device_id="device-turn-stream", timezone="UTC")
    opened = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-turn-stream", timezone="UTC"),
    )
    assert opened.status_code == 200
    session_id = opened.json()["session_id"]

    with client.websocket_connect(
        "/agent/session/ws?device_id=device-turn-stream&session_id="
        f"{session_id}&timezone=UTC"
    ) as ws:
        initial = ws.receive_json()
        assert initial["type"] == "session_snapshot"

        response = client.post(
            "/agent/conversation/turn",
            json={
                "device_id": "device-turn-stream",
                "session_id": session_id,
                "timezone": "UTC",
                "prompt": "help me focus",
                "transport": "chat",
                "user_message_id": "user-turn-1",
            },
        )
        assert response.status_code == 200
        frames = []
        while True:
            frame = ws.receive_json()
            frames.append(frame)
            if frame["type"] == "assistant_done":
                break
        snapshot = ws.receive_json()

    deltas = [frame["delta"] for frame in frames if frame["type"] == "assistant_delta"]
    done = next(frame for frame in frames if frame["type"] == "assistant_done")
    assert deltas == ["hello", " world"]
    assert done["text"] == "hello world"
    assert snapshot["type"] == "session_snapshot"
    assert response.json()["output"] == "hello world"


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


def test_session_state_returns_current_messages_and_task_panel(app_client):
    client = app_client["client"]
    repository = app_client["repository"]

    _start_and_configure_onboarding(
        client,
        device_id="device-state",
        timezone="UTC",
    )
    opened = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-state", timezone="UTC"),
    )
    assert opened.status_code == 200
    session_id = opened.json()["session_id"]

    asyncio.run(
        repository.insert_message(
            payload=main.MessageRecord(
                id="msg-state-1",
                session_id=session_id,
                user_id="device-state",
                role="assistant",
                content="already here",
                metadata={"transport": "seed"},
            )
        )
    )

    response = client.post(
        "/agent/session/state",
        json={
            "device_id": "device-state",
            "session_id": session_id,
            "timezone": "UTC",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["session_id"] == session_id
    assert payload["messages"][-1]["content"] == "already here"
    assert "task_panel_state" in payload


def test_conversation_turn_persists_user_and_assistant_messages(app_client):
    client = app_client["client"]
    fake_agent = app_client["agent"]
    repository = app_client["repository"]

    _start_and_configure_onboarding(
        client,
        device_id="device-voice-turn",
        timezone="UTC",
    )
    opened = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-voice-turn", timezone="UTC"),
    )
    assert opened.status_code == 200
    session_id = opened.json()["session_id"]

    response = client.post(
        "/agent/conversation/turn",
        json={
            "device_id": "device-voice-turn",
            "session_id": session_id,
            "timezone": "UTC",
            "prompt": "help me focus",
            "transport": "voice",
            "entry_context": {
                "source": "manual",
                "event_id": None,
                "trigger_type": None,
                "scheduled_time": None,
                "calendar_event_id": None,
                "entry_mode": "reactive",
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["output"] == "hello world"
    assert fake_agent.stream_calls[-1]["prompt"] == "help me focus"
    stored_messages = [
        message.model_dump()
        for message in asyncio.run(repository.list_messages(session_id=session_id))
    ]
    assert [message["role"] for message in stored_messages[-2:]] == ["user", "assistant"]
    assert stored_messages[-2]["metadata"]["transport"] == "voice"
    assert stored_messages[-1]["content"] == "hello world"


def test_conversation_turn_accepts_chat_transport(app_client):
    client = app_client["client"]

    _start_and_configure_onboarding(
        client,
        device_id="device-chat-turn",
        timezone="UTC",
    )
    opened = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-chat-turn", timezone="UTC"),
    )
    assert opened.status_code == 200
    session_id = opened.json()["session_id"]

    response = client.post(
        "/agent/conversation/turn",
        json={
            "device_id": "device-chat-turn",
            "session_id": session_id,
            "timezone": "UTC",
            "prompt": "hello chat",
            "transport": "chat",
            "entry_context": {
                "source": "manual",
                "event_id": None,
                "trigger_type": None,
                "scheduled_time": None,
                "calendar_event_id": None,
                "entry_mode": "reactive",
            },
        },
    )

    assert response.status_code == 200
    assert response.json()["output"] == "hello world"


def test_conversation_turn_replay_returns_original_assistant_metadata(app_client):
    client = app_client["client"]

    _start_and_configure_onboarding(
        client,
        device_id="device-chat-replay",
        timezone="UTC",
    )
    opened = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-chat-replay", timezone="UTC"),
    )
    assert opened.status_code == 200
    session_id = opened.json()["session_id"]
    payload = {
        "device_id": "device-chat-replay",
        "session_id": session_id,
        "timezone": "UTC",
        "prompt": "hello replay",
        "transport": "chat",
        "user_message_id": "user-turn-replay-1",
    }

    first = client.post("/agent/conversation/turn", json=payload)
    second = client.post("/agent/conversation/turn", json=payload)

    assert first.status_code == 200
    assert second.status_code == 200
    first_body = first.json()
    second_body = second.json()
    assert first_body["message_id"] != payload["user_message_id"]
    assert second_body["message_id"] == first_body["message_id"]
    assert second_body["created_at"] == first_body["created_at"]
    assert second_body["output"] == first_body["output"]


def test_conversation_turn_replay_reruns_when_user_message_saved_without_assistant(app_client):
    client = app_client["client"]
    fake_agent = app_client["agent"]
    repository = app_client["repository"]
    failed_once = {"value": False}

    async def flaky_run_stream(self, *, prompt, user_id, session_id, context=None):
        self.stream_calls.append(
            {
                "prompt": prompt,
                "user_id": user_id,
                "session_id": session_id,
                "context": context or {},
            }
        )
        if prompt == "flaky replay" and not failed_once["value"]:
            failed_once["value"] = True
            raise RuntimeError("stream exploded once")
        for chunk in ["recovered", " output"]:
            yield chunk

    fake_agent.run_stream = types.MethodType(flaky_run_stream, fake_agent)

    _start_and_configure_onboarding(
        client,
        device_id="device-chat-retry",
        timezone="UTC",
    )
    opened = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-chat-retry", timezone="UTC"),
    )
    assert opened.status_code == 200
    session_id = opened.json()["session_id"]
    payload = {
        "device_id": "device-chat-retry",
        "session_id": session_id,
        "timezone": "UTC",
        "prompt": "flaky replay",
        "transport": "chat",
        "user_message_id": "user-turn-flaky-retry",
    }

    first = client.post("/agent/conversation/turn", json=payload)
    second = client.post("/agent/conversation/turn", json=payload)

    assert first.status_code == 503
    assert second.status_code == 200
    second_body = second.json()
    assert second_body["message_id"] != payload["user_message_id"]
    assert second_body["output"] == "recovered output"

    persisted = [
        message.model_dump()
        for message in asyncio.run(repository.list_messages(session_id=session_id))
    ]
    persisted_users = [
        message for message in persisted if message["role"] == "user" and message["id"] == payload["user_message_id"]
    ]
    persisted_assistants = [
        message
        for message in persisted
        if message["role"] == "assistant"
        and (message.get("metadata") or {}).get("in_reply_to") == payload["user_message_id"]
    ]
    assert len(persisted_users) == 1
    assert len(persisted_assistants) == 1


def test_conversation_turn_replay_rejects_prompt_mismatch(app_client):
    client = app_client["client"]

    _start_and_configure_onboarding(
        client,
        device_id="device-chat-mismatch",
        timezone="UTC",
    )
    opened = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-chat-mismatch", timezone="UTC"),
    )
    assert opened.status_code == 200
    session_id = opened.json()["session_id"]

    first = client.post(
        "/agent/conversation/turn",
        json={
            "device_id": "device-chat-mismatch",
            "session_id": session_id,
            "timezone": "UTC",
            "prompt": "original prompt",
            "transport": "chat",
            "user_message_id": "user-turn-mismatch",
        },
    )
    assert first.status_code == 200

    replay_with_different_prompt = client.post(
        "/agent/conversation/turn",
        json={
            "device_id": "device-chat-mismatch",
            "session_id": session_id,
            "timezone": "UTC",
            "prompt": "different prompt",
            "transport": "chat",
            "user_message_id": "user-turn-mismatch",
        },
    )
    assert replay_with_different_prompt.status_code == 409
    assert replay_with_different_prompt.json()["error"]["code"] == "USER_MESSAGE_ID_PROMPT_MISMATCH"


def test_conversation_turn_replay_does_not_return_later_assistant_turn(app_client):
    client = app_client["client"]
    fake_agent = app_client["agent"]
    fail_turn_one_once = {"value": False}

    async def selective_run_stream(self, *, prompt, user_id, session_id, context=None):
        self.stream_calls.append(
            {
                "prompt": prompt,
                "user_id": user_id,
                "session_id": session_id,
                "context": context or {},
            }
        )
        if prompt == "turn one" and not fail_turn_one_once["value"]:
            fail_turn_one_once["value"] = True
            raise RuntimeError("turn one failed once")
        if prompt == "turn one":
            chunks = ["turn", " one"]
        elif prompt == "turn two":
            chunks = ["turn", " two"]
        else:
            chunks = ["hello", " world"]
        for chunk in chunks:
            yield chunk

    fake_agent.run_stream = types.MethodType(selective_run_stream, fake_agent)

    _start_and_configure_onboarding(
        client,
        device_id="device-chat-ordering",
        timezone="UTC",
    )
    opened = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-chat-ordering", timezone="UTC"),
    )
    assert opened.status_code == 200
    session_id = opened.json()["session_id"]

    first_attempt = client.post(
        "/agent/conversation/turn",
        json={
            "device_id": "device-chat-ordering",
            "session_id": session_id,
            "timezone": "UTC",
            "prompt": "turn one",
            "transport": "chat",
            "user_message_id": "user-turn-ordering-1",
        },
    )
    assert first_attempt.status_code == 503

    second_turn = client.post(
        "/agent/conversation/turn",
        json={
            "device_id": "device-chat-ordering",
            "session_id": session_id,
            "timezone": "UTC",
            "prompt": "turn two",
            "transport": "chat",
            "user_message_id": "user-turn-ordering-2",
        },
    )
    assert second_turn.status_code == 200
    second_body = second_turn.json()
    assert second_body["output"] == "turn two"

    replay_first = client.post(
        "/agent/conversation/turn",
        json={
            "device_id": "device-chat-ordering",
            "session_id": session_id,
            "timezone": "UTC",
            "prompt": "turn one",
            "transport": "chat",
            "user_message_id": "user-turn-ordering-1",
        },
    )
    assert replay_first.status_code == 200
    replay_body = replay_first.json()
    assert replay_body["output"] == "turn one"
    assert replay_body["message_id"] != "user-turn-ordering-1"
    assert replay_body["message_id"] != second_body["message_id"]


def test_conversation_turn_duplicate_insert_race_returns_replay(app_client, monkeypatch):
    client = app_client["client"]
    repository = app_client["repository"]

    _start_and_configure_onboarding(
        client,
        device_id="device-chat-race",
        timezone="UTC",
    )
    opened = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-chat-race", timezone="UTC"),
    )
    assert opened.status_code == 200
    session_id = opened.json()["session_id"]

    asyncio.run(
        repository.insert_message(
            payload=main.MessageRecord(
                id="user-turn-race-1",
                session_id=session_id,
                user_id="device-chat-race",
                role="user",
                content="race prompt",
                metadata={"transport": "chat"},
            )
        )
    )
    asyncio.run(
        repository.insert_message(
            payload=main.MessageRecord(
                id="assistant-turn-race-1",
                session_id=session_id,
                user_id="device-chat-race",
                role="assistant",
                content="race resolved",
                metadata={"in_reply_to": "user-turn-race-1"},
            )
        )
    )

    original_message_exists = repository.message_exists
    call_count = {"value": 0}

    async def staged_message_exists(session_id: str, message_id: str) -> bool:
        call_count["value"] += 1
        if call_count["value"] == 1:
            return False
        return await original_message_exists(session_id=session_id, message_id=message_id)

    async def duplicate_stream_turn(**kwargs):
        del kwargs
        raise RuntimeError(
            'Supabase error 409: duplicate key value violates unique constraint "session_messages_pkey"'
        )

    monkeypatch.setattr(repository, "message_exists", staged_message_exists)
    monkeypatch.setattr(main.conversation_engine, "stream_turn", duplicate_stream_turn)

    response = client.post(
        "/agent/conversation/turn",
        json={
            "device_id": "device-chat-race",
            "session_id": session_id,
            "timezone": "UTC",
            "prompt": "race prompt",
            "transport": "chat",
            "user_message_id": "user-turn-race-1",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["output"] == "race resolved"
    assert payload["message_id"] == "assistant-turn-race-1"


def test_voice_connect_returns_livekit_credentials(app_client, monkeypatch):
    client = app_client["client"]
    fake_livekit = FakeLiveKitCoordinator(dispatch_mode="token_fallback", fallback_used=True)
    monkeypatch.setattr(main, "livekit_voice", fake_livekit)
    monkeypatch.setenv("INTENTIVE_BACKEND_URL", "https://backend.intentive.test")

    _start_and_configure_onboarding(
        client,
        device_id="device-voice-room",
        timezone="UTC",
    )
    opened = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-voice-room", timezone="UTC"),
    )
    assert opened.status_code == 200
    session_id = opened.json()["session_id"]

    response = client.post(
        "/agent/voice/connect",
        json={
            "device_id": "device-voice-room",
            "session_id": session_id,
            "timezone": "UTC",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["room_name"] == f"voice_{session_id}"
    assert payload["server_url"] == "wss://example.livekit.cloud"
    assert payload["participant_token"] == "livekit-token"
    assert payload["dispatch_mode"] == "token_fallback"
    assert payload["fallback_used"] is True
    assert fake_livekit.ensure_room_calls[0]["room_name"] == f"voice_{session_id}"
    dispatch_metadata = fake_livekit.dispatch_calls[0]["metadata"]
    assert dispatch_metadata["backend_url"] == "https://backend.intentive.test"


def test_voice_connect_includes_debug_diagnostic_code_when_enabled(app_client, monkeypatch):
    client = app_client["client"]
    fake_livekit = FakeLiveKitCoordinator(
        dispatch_mode="token_fallback",
        fallback_used=True,
        diagnostic_code="LIVEKIT_AUTH_MISCONFIG",
    )
    monkeypatch.setattr(main, "livekit_voice", fake_livekit)
    monkeypatch.setenv("INTENTIVE_BACKEND_URL", "https://backend.intentive.test")
    monkeypatch.setenv("VOICE_DEBUG", "true")

    _start_and_configure_onboarding(
        client,
        device_id="device-voice-debug",
        timezone="UTC",
    )
    opened = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-voice-debug", timezone="UTC"),
    )
    assert opened.status_code == 200
    session_id = opened.json()["session_id"]

    response = client.post(
        "/agent/voice/connect",
        json={
            "device_id": "device-voice-debug",
            "session_id": session_id,
            "timezone": "UTC",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["fallback_used"] is True
    assert payload["voice_diagnostic_code"] == "LIVEKIT_AUTH_MISCONFIG"


def test_voice_connect_hides_diagnostic_code_when_debug_disabled(app_client, monkeypatch):
    client = app_client["client"]
    fake_livekit = FakeLiveKitCoordinator(
        dispatch_mode="token_fallback",
        fallback_used=True,
        diagnostic_code="LIVEKIT_AUTH_MISCONFIG",
    )
    monkeypatch.setattr(main, "livekit_voice", fake_livekit)
    monkeypatch.setenv("INTENTIVE_BACKEND_URL", "https://backend.intentive.test")
    monkeypatch.setenv("VOICE_DEBUG", "false")

    _start_and_configure_onboarding(
        client,
        device_id="device-voice-no-debug",
        timezone="UTC",
    )
    opened = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-voice-no-debug", timezone="UTC"),
    )
    assert opened.status_code == 200
    session_id = opened.json()["session_id"]

    response = client.post(
        "/agent/voice/connect",
        json={
            "device_id": "device-voice-no-debug",
            "session_id": session_id,
            "timezone": "UTC",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["fallback_used"] is True
    assert "voice_diagnostic_code" not in payload


def test_voice_health_reports_degraded_when_livekit_env_missing(app_client, monkeypatch):
    client = app_client["client"]
    monkeypatch.delenv("LIVEKIT_URL", raising=False)
    monkeypatch.delenv("LIVEKIT_API_KEY", raising=False)
    monkeypatch.delenv("LIVEKIT_API_SECRET", raising=False)
    monkeypatch.setattr(main, "livekit_voice", None)

    response = client.get("/agent/voice/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "degraded"
    assert payload["configured"] is False
    assert "LIVEKIT_URL" in payload["missing_env_vars"]
    assert "LIVEKIT_API_KEY" in payload["missing_env_vars"]
    assert "LIVEKIT_API_SECRET" in payload["missing_env_vars"]


def test_voice_health_reports_ok_when_configured(app_client, monkeypatch):
    client = app_client["client"]
    monkeypatch.setenv("LIVEKIT_URL", "wss://configured.livekit.cloud")
    monkeypatch.setenv("LIVEKIT_API_KEY", "api-key")
    monkeypatch.setenv("LIVEKIT_API_SECRET", "api-secret")
    monkeypatch.setenv("LIVEKIT_AGENT_NAME", "configured-agent")
    monkeypatch.setattr(main, "livekit_voice", FakeLiveKitCoordinator())

    response = client.get("/agent/voice/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["configured"] is True
    assert payload["coordinator_loaded"] is True
    assert payload["agent_name"] == "configured-agent"


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


def test_legacy_bootstrap_uses_reactive_context_after_onboarding_start(app_client):
    client = app_client["client"]
    fake_agent = app_client["agent"]

    _start_and_configure_onboarding(
        client,
        device_id="device-legacy-post-onboard",
        timezone="UTC",
        wake_time="07:30",
        bedtime="23:15",
    )

    response = client.post(
        "/agent/bootstrap-device",
        json={"device_id": "device-legacy-post-onboard", "timezone": "UTC"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["needs_onboarding"] is False
    assert payload["messages"][-1]["metadata"]["startup_turn"] is True
    assert fake_agent.run_calls[-1]["context"]["trigger_type"] == ""


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
                scheduled_time="2026-03-10T09:23:00Z",
                event_type="checkin",
                payload={
                    "trigger_type": "before_task",
                    "timezone": "UTC",
                    "task_title": "Deep work",
                    "timebox_start": "2026-03-10T09:28:00Z",
                    "timebox_end": "2026-03-10T10:00:00Z",
                },
                executed=False,
            )
        )
    )

    push_attempts: list[dict[str, object]] = []

    async def _always_send_push(**kwargs):
        push_attempts.append(dict(kwargs))
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
    assert len(push_attempts) == 1
    assert push_attempts[0]["title"] == "Up next: Deep work"
    assert push_attempts[0]["body"] == "Deep work starts at 9:28 AM. Ready to begin?"
    assert push_attempts[0]["data"]["task_title"] == "Deep work"
    assert push_attempts[0]["data"]["timebox_start"] == "2026-03-10T09:28:00Z"
    assert push_attempts[0]["data"]["timebox_end"] == "2026-03-10T10:00:00Z"


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
    _start_and_configure_onboarding(
        client,
        device_id="device-session-open",
        timezone="UTC",
    )
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
    _start_and_configure_onboarding(
        client,
        device_id="device-session-idempotent",
        timezone="UTC",
    )
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
    _start_and_configure_onboarding(
        client,
        device_id="device-new-open-cycle",
        timezone="UTC",
    )

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

    _start_and_configure_onboarding(
        client,
        device_id=device_id,
        timezone="UTC",
    )

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


def test_onboarding_start_transitions_pending_to_in_progress(app_client):
    client = app_client["client"]
    started = client.post(
        "/agent/onboarding/start",
        json={
            "device_id": "device-onboard",
            "timezone": "Asia/Kolkata",
        },
    )
    assert started.status_code == 200
    payload = started.json()
    assert payload["needs_onboarding"] is False
    assert payload["profile_context"]["onboarding_status"] == "in_progress"
    assert payload["profile_context"]["wake_time"] is None
    assert payload["profile_context"]["bedtime"] is None

    opened_after = client.post(
        "/agent/session/open",
        json=_session_open_payload(device_id="device-onboard", timezone="Asia/Kolkata"),
    )
    assert opened_after.status_code == 200
    after_payload = opened_after.json()
    assert after_payload["needs_onboarding"] is False
    assert after_payload["profile_context"]["onboarding_status"] == "in_progress"


def test_onboarding_sleep_schedule_updates_user_and_seeds_unlock_event(app_client):
    client = app_client["client"]
    repository = app_client["repository"]
    started = client.post(
        "/agent/onboarding/start",
        json={"device_id": "device-onboard-sleep", "timezone": "UTC"},
    )
    assert started.status_code == 200
    scheduled = client.post(
        "/agent/onboarding/sleep-schedule",
        json={
            "device_id": "device-onboard-sleep",
            "timezone": "UTC",
            "wake_time": "07:30",
            "bedtime": "23:15",
        },
    )
    assert scheduled.status_code == 200
    payload = scheduled.json()
    assert payload["needs_onboarding"] is False
    assert payload["profile_context"]["wake_time"] == "07:30"
    assert payload["profile_context"]["bedtime"] == "23:15"
    assert payload["profile_context"]["onboarding_status"] == "ready_for_main"
    morning_events = [
        event
        for event in repository.events.values()
        if event.user_id == "device-onboard-sleep" and event.event_type == "morning_wake"
    ]
    assert len(morning_events) == 1
    assert morning_events[0].payload["onboarding_unlock"] is True
    assert morning_events[0].cloud_task_name is not None


def test_onboarding_sleep_schedule_seeds_unlock_without_deleting_daily_loop_event(app_client):
    client = app_client["client"]
    repository = app_client["repository"]
    stale_event_id = "event-stale-morning-wake"
    asyncio.run(
        repository.create_event(
            main.CheckinEventRecord(
                id=stale_event_id,
                user_id="device-onboard-stale",
                scheduled_time="2099-01-01T07:00:00Z",
                event_type="morning_wake",
                payload={
                    "seed_date": "2099-01-01",
                    "reason": "daily_bootstrap",
                },
                executed=False,
                cloud_task_name="projects/local/locations/local/queues/default/tasks/1234",
            )
        )
    )
    started = client.post(
        "/agent/onboarding/start",
        json={"device_id": "device-onboard-stale", "timezone": "UTC"},
    )
    assert started.status_code == 200
    scheduled = client.post(
        "/agent/onboarding/sleep-schedule",
        json={
            "device_id": "device-onboard-stale",
            "timezone": "UTC",
            "wake_time": "07:30",
            "bedtime": "23:15",
        },
    )
    assert scheduled.status_code == 200
    assert stale_event_id in repository.events

    morning_events = [
        event
        for event in repository.events.values()
        if event.user_id == "device-onboard-stale" and event.event_type == "morning_wake"
    ]
    assert len(morning_events) == 2
    unlock_events = [
        event for event in morning_events if bool(event.payload.get("onboarding_unlock"))
    ]
    assert len(unlock_events) == 1
    assert unlock_events[0].payload.get("reason") == "onboarding_unlock"


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


def test_onboarding_sleep_schedule_unlock_event_execution_marks_completed(app_client, monkeypatch):
    client = app_client["client"]
    repository = app_client["repository"]
    monkeypatch.setenv("SCHEDULER_SECRET", "scheduler-test-secret")
    async def _always_send_push(**kwargs):
        _ = kwargs
        return True
    monkeypatch.setattr(main, "_send_push_notification", _always_send_push)

    _start_and_configure_onboarding(
        client,
        device_id="device-onboard-complete",
        timezone="UTC",
        wake_time="07:30",
        bedtime="23:15",
    )
    asyncio.run(
        repository.upsert_push_token(
            user_id="device-onboard-complete",
            expo_push_token="ExponentPushToken[test]",
        )
    )
    onboarding_event = next(
        event
        for event in repository.events.values()
        if event.user_id == "device-onboard-complete"
        and event.event_type == "morning_wake"
        and bool(event.payload.get("onboarding_unlock"))
    )
    response = client.post(
        "/agent/events/execute",
        json={"event_id": onboarding_event.id},
        headers={"x-scheduler-secret": "scheduler-test-secret"},
    )
    assert response.status_code == 200
    user_profile = repository.users["device-onboard-complete"]
    assert user_profile["onboarding_status"] == "completed"


def test_onboarding_sleep_schedule_validates_required_fields(app_client):
    client = app_client["client"]
    client.post(
        "/agent/onboarding/start",
        json={"device_id": "device-onboard-invalid", "timezone": "UTC"},
    )
    response = client.post(
        "/agent/onboarding/sleep-schedule",
        json={
            "device_id": "device-onboard-invalid",
            "timezone": "UTC",
            "wake_time": "7:30",
            "bedtime": "23:15",
        },
    )
    assert response.status_code == 400
    assert "wake_time must be HH:MM" in response.json()["error"]["message"]


def test_old_complete_endpoint_is_removed(app_client):
    client = app_client["client"]
    response = client.post(
        "/agent/onboarding/complete",
        json={
            "device_id": "device-removed-endpoint",
            "timezone": "UTC",
            "wake_time": "07:30",
            "bedtime": "23:15",
        },
    )
    assert response.status_code == 404
