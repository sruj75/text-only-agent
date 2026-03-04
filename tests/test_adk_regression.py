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
