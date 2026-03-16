from __future__ import annotations

import livekit_voice_worker as worker


class _FakeItem:
    def __init__(self, *, role: str, text_content: str) -> None:
        self.role = role
        self.text_content = text_content


class _FakeChatContext:
    def __init__(self, items) -> None:
        self.items = items


def test_latest_user_prompt_reads_latest_user_item():
    chat_ctx = _FakeChatContext(
        [
            _FakeItem(role="assistant", text_content="hello"),
            _FakeItem(role="user", text_content="first"),
            _FakeItem(role="assistant", text_content="reply"),
            _FakeItem(role="user", text_content="latest question"),
        ]
    )

    assert worker._latest_user_prompt(chat_ctx) == "latest question"


def test_latest_user_prompt_ignores_empty_or_non_user_items():
    chat_ctx = _FakeChatContext(
        [
            _FakeItem(role="assistant", text_content="hello"),
            _FakeItem(role="user", text_content=""),
        ]
    )

    assert worker._latest_user_prompt(chat_ctx) == ""


def test_stt_descriptor_from_env_appends_language_when_missing(monkeypatch):
    monkeypatch.setenv("LIVEKIT_STT_MODEL", "deepgram/nova-3")
    monkeypatch.setenv("LIVEKIT_STT_LANGUAGE", "en")

    assert worker._stt_descriptor_from_env() == "deepgram/nova-3:en"


def test_stt_descriptor_from_env_keeps_full_descriptor(monkeypatch):
    monkeypatch.setenv("LIVEKIT_STT_MODEL", "deepgram/flux-general:en")
    monkeypatch.setenv("LIVEKIT_STT_LANGUAGE", "multi")

    assert worker._stt_descriptor_from_env() == "deepgram/flux-general:en"


def test_tts_descriptor_from_env_prefers_full_descriptor(monkeypatch):
    monkeypatch.setenv(
        "LIVEKIT_TTS_MODEL_DESCRIPTOR",
        "cartesia/sonic-3:9626c31c-bec5-4cca-baa8-f8ba9e84c8bc",
    )
    monkeypatch.setenv("LIVEKIT_TTS_MODEL", "cartesia/sonic-2")
    monkeypatch.setenv("LIVEKIT_TTS_VOICE", "ignored-voice")

    assert (
        worker._tts_descriptor_from_env()
        == "cartesia/sonic-3:9626c31c-bec5-4cca-baa8-f8ba9e84c8bc"
    )


def test_tts_descriptor_from_env_combines_model_and_voice(monkeypatch):
    monkeypatch.delenv("LIVEKIT_TTS_MODEL_DESCRIPTOR", raising=False)
    monkeypatch.setenv("LIVEKIT_TTS_MODEL", "cartesia/sonic-3")
    monkeypatch.setenv("LIVEKIT_TTS_VOICE", "voice-id-1")

    assert worker._tts_descriptor_from_env() == "cartesia/sonic-3:voice-id-1"
