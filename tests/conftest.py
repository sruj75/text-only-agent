from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

import main


class FakeADK:
    def __init__(self) -> None:
        self.model_name = "fake-adk-model"
        self.run_calls: list[dict[str, Any]] = []
        self.stream_calls: list[dict[str, Any]] = []

    async def run(
        self,
        *,
        prompt: str,
        user_id: str,
        session_id: str,
        context: dict[str, str] | None = None,
    ) -> str:
        self.run_calls.append(
            {
                "prompt": prompt,
                "user_id": user_id,
                "session_id": session_id,
                "context": context or {},
            }
        )
        if prompt == "raise_run_error":
            raise RuntimeError("run exploded")
        if prompt.startswith("Conversation bootstrap:"):
            return "hello world"
        return f"assistant::{prompt}"

    async def run_stream(
        self,
        *,
        prompt: str,
        user_id: str,
        session_id: str,
        context: dict[str, str] | None = None,
    ):
        call = {
            "prompt": prompt,
            "user_id": user_id,
            "session_id": session_id,
            "context": context or {},
        }
        self.stream_calls.append(call)
        if prompt == "raise_stream_error":
            raise RuntimeError("stream exploded")
        for chunk in ["hello", " world"]:
            yield chunk


@pytest.fixture()
def app_client(monkeypatch: pytest.MonkeyPatch):
    fake_agent = FakeADK()
    repo = main.InMemoryRepository()

    monkeypatch.setattr(main, "agent", fake_agent)
    monkeypatch.setattr(main, "repository", repo)

    with TestClient(main.app) as client:
        yield {
            "client": client,
            "agent": fake_agent,
            "repository": repo,
        }
