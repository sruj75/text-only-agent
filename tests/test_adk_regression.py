from __future__ import annotations

import asyncio
from typing import get_args, get_type_hints

from startup_agent.adk import SimpleADK, TaskManagementAction


def test_adk_requires_credentials(monkeypatch):
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)

    adk = SimpleADK()

    async def _consume():
        async for _ in adk.run_stream(
            prompt="hello",
            user_id="u1",
            session_id="s1",
            context={"timezone": "UTC"},
        ):
            pass

    try:
        asyncio.run(_consume())
    except RuntimeError as error:
        assert "Missing Google ADK credentials" in str(error)
    else:
        raise AssertionError("Expected credential guard to fail without credentials")


def test_adk_registers_function_tools_when_enabled():
    async def _time_handler(timezone, runtime_context):
        return {"ok": True, "timezone": timezone, "ctx": runtime_context}

    async def _task_handler(action, payload, session_id, timezone, runtime_context):
        return {
            "ok": True,
            "action": action,
            "payload": payload,
            "session_id": session_id,
            "timezone": timezone,
            "ctx": runtime_context,
        }

    adk = SimpleADK(
        get_current_time_tool=_time_handler,
        task_management_tool=_task_handler,
        enable_task_tools=True,
    )

    assert len(adk.agent.tools or []) == 2


def test_adk_task_management_parses_json_payload():
    async def _time_handler(timezone, runtime_context):
        return {"ok": True, "timezone": timezone, "ctx": runtime_context}

    async def _task_handler(action, payload, session_id, timezone, runtime_context):
        return {
            "ok": True,
            "action": action,
            "payload": payload,
            "session_id": session_id,
            "timezone": timezone,
            "ctx": runtime_context,
        }

    adk = SimpleADK(
        get_current_time_tool=_time_handler,
        task_management_tool=_task_handler,
        enable_task_tools=True,
    )

    result = asyncio.run(
        adk.task_management(
            action="capture_tasks",
            payload_json='{"titles": ["Write PRD"]}',
            session_id="s1",
            timezone="UTC",
        )
    )

    assert result["ok"] is True
    assert result["payload"] == {"titles": ["Write PRD"]}


def test_adk_task_management_action_contract_is_canonical():
    hints = get_type_hints(SimpleADK.task_management)
    action_hint = hints["action"]

    assert action_hint == TaskManagementAction
    assert set(get_args(action_hint)) == {
        "capture_tasks",
        "timebox_task",
        "set_top_essentials",
        "update_task_status",
        "get_tasks",
        "get_schedule",
    }


def test_adk_cleans_task_write_counter_when_stream_is_interrupted(monkeypatch):
    monkeypatch.setenv("GOOGLE_API_KEY", "test-key")
    adk = SimpleADK()

    class _Part:
        def __init__(self, text: str) -> None:
            self.text = text

    class _Content:
        def __init__(self, text: str) -> None:
            self.parts = [_Part(text)]

    class _Event:
        def __init__(self, text: str) -> None:
            self.author = "model"
            self.content = _Content(text)

    class _Runner:
        async def run_async(self, **kwargs):
            _ = kwargs
            yield _Event("hello")
            yield _Event("hello world")

    adk.runner = _Runner()

    async def _consume_and_interrupt():
        stream = adk.run_stream(
            prompt="hello",
            user_id="u-cleanup",
            session_id="s-cleanup",
            context={"timezone": "UTC"},
        )
        first_chunk = await anext(stream)
        assert first_chunk
        await stream.aclose()

    asyncio.run(_consume_and_interrupt())

    assert ("u-cleanup", "s-cleanup") not in adk._task_write_counts
    assert adk.consume_task_write_count(user_id="u-cleanup", session_id="s-cleanup") == 0
