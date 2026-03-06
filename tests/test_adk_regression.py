from __future__ import annotations

import asyncio

from startup_agent.adk import SimpleADK


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
