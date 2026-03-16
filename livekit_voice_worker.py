from __future__ import annotations

import json
import os
from typing import Any, Dict

import httpx
from livekit import agents


DEFAULT_STT_MODEL = "deepgram/nova-3"
DEFAULT_STT_LANGUAGE = "en"
DEFAULT_TTS_MODEL = "cartesia/sonic-3"
DEFAULT_TTS_VOICE = "9626c31c-bec5-4cca-baa8-f8ba9e84c8bc"


def _stt_descriptor_from_env() -> str:
    model = str(os.getenv("LIVEKIT_STT_MODEL") or DEFAULT_STT_MODEL).strip()
    language = str(os.getenv("LIVEKIT_STT_LANGUAGE") or DEFAULT_STT_LANGUAGE).strip()
    if not model:
        return f"{DEFAULT_STT_MODEL}:{DEFAULT_STT_LANGUAGE}"
    if ":" in model:
        return model
    return f"{model}:{language or DEFAULT_STT_LANGUAGE}"


def _tts_descriptor_from_env() -> str:
    descriptor = str(os.getenv("LIVEKIT_TTS_MODEL_DESCRIPTOR") or "").strip()
    if descriptor:
        return descriptor
    model = str(os.getenv("LIVEKIT_TTS_MODEL") or DEFAULT_TTS_MODEL).strip()
    voice = str(os.getenv("LIVEKIT_TTS_VOICE") or DEFAULT_TTS_VOICE).strip()
    if not model:
        model = DEFAULT_TTS_MODEL
    if ":" in model:
        return model
    return f"{model}:{voice or DEFAULT_TTS_VOICE}"


def _latest_user_prompt(chat_ctx: agents.ChatContext) -> str:
    for item in reversed(chat_ctx.items):
        role = str(getattr(item, "role", "") or "").strip()
        text = str(getattr(item, "text_content", "") or "").strip()
        if role == "user" and text:
            return text
    return ""


class IntentiveVoiceBridgeAgent(agents.Agent):
    """Thin LiveKit bridge that delegates business logic back to the backend API.

    Keeping voice transport separate from assistant orchestration keeps the
    worker simple and lets chat + voice share one backend session contract.
    """

    def __init__(self, *, runtime_metadata: Dict[str, Any]) -> None:
        super().__init__(
            instructions="Bridge spoken turns to the Intentive backend and speak its response.",
        )
        self.runtime_metadata = runtime_metadata

    async def llm_node(
        self,
        chat_ctx: agents.ChatContext,
        tools: list[Any],
        model_settings: Any,
    ) -> str:
        del tools
        del model_settings
        prompt = _latest_user_prompt(chat_ctx)
        if not prompt:
            return "I missed that. Could you say it one more time?"

        backend_url = str(self.runtime_metadata.get("backend_url") or "").rstrip("/")
        if not backend_url:
            return "Voice mode is missing its backend URL."

        async with httpx.AsyncClient(timeout=45.0) as client:
            response = await client.post(
                f"{backend_url}/agent/conversation/turn",
                json={
                    "device_id": self.runtime_metadata["device_id"],
                    "session_id": self.runtime_metadata["session_id"],
                    "timezone": self.runtime_metadata["timezone"],
                    "prompt": prompt,
                    "transport": "voice",
                    "entry_context": self.runtime_metadata["entry_context"],
                },
            )
            response.raise_for_status()
            payload = response.json()

        return str(payload.get("output") or "I could not respond right now.").strip()


async def entrypoint(ctx: agents.JobContext) -> None:
    metadata = json.loads(ctx.job.metadata or "{}")
    await ctx.connect(auto_subscribe=agents.AutoSubscribe.AUDIO_ONLY)

    session = agents.AgentSession(
        stt=_stt_descriptor_from_env(),
        tts=_tts_descriptor_from_env(),
    )
    await session.start(
        IntentiveVoiceBridgeAgent(runtime_metadata=metadata),
        room=ctx.room,
    )


def run() -> None:
    agents.cli.run_app(
        agents.WorkerOptions(
            entrypoint_fnc=entrypoint,
            agent_name=os.getenv("LIVEKIT_AGENT_NAME", "intentive-voice-agent"),
        )
    )


if __name__ == "__main__":
    run()
