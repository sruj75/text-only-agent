from __future__ import annotations

import asyncio

from livekit import api

from startup_agent import livekit_voice


class _FakeAgentDispatchService:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[object] = []

    async def create_dispatch(self, request):
        self.calls.append(request)
        if self.error:
            raise self.error
        return object()


class _FakeLiveKitAPI:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.agent_dispatch = _FakeAgentDispatchService(error=error)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _build_coordinator() -> livekit_voice.LiveKitVoiceCoordinator:
    return livekit_voice.LiveKitVoiceCoordinator(
        url="wss://example.livekit.cloud",
        api_key="key",
        api_secret="secret",
        agent_name="intentive-voice-agent",
        empty_timeout=600,
        departure_timeout=20,
        token_ttl_minutes=30,
    )


def test_dispatch_agent_returns_explicit_on_success(monkeypatch):
    fake_api = _FakeLiveKitAPI(error=None)
    monkeypatch.setattr(livekit_voice.api, "LiveKitAPI", lambda *args, **kwargs: fake_api)
    coordinator = _build_coordinator()

    result = asyncio.run(
        coordinator.dispatch_agent(
            room_name="voice_session_1",
            metadata={"device_id": "device-1"},
        )
    )

    assert result.mode == "explicit"
    assert result.fallback_used is False
    assert result.diagnostic_code is None
    assert len(fake_api.agent_dispatch.calls) == 1


def test_dispatch_agent_uses_token_fallback_with_diagnostic(monkeypatch):
    twirp_error = api.TwirpError(
        api.TwirpErrorCode.UNAUTHENTICATED,
        "invalid credentials",
        status=401,
    )
    fake_api = _FakeLiveKitAPI(error=twirp_error)
    monkeypatch.setattr(livekit_voice.api, "LiveKitAPI", lambda *args, **kwargs: fake_api)
    coordinator = _build_coordinator()

    result = asyncio.run(
        coordinator.dispatch_agent(
            room_name="voice_session_2",
            metadata={"device_id": "device-2"},
        )
    )

    assert result.mode == "token_fallback"
    assert result.fallback_used is True
    assert result.diagnostic_code == "LIVEKIT_AUTH_MISCONFIG"
    assert result.diagnostic is not None
    assert result.diagnostic["status"] == 401
    assert result.diagnostic["twirp_code"] == "unauthenticated"
