import os
from contextlib import aclosing
from typing import AsyncGenerator, Dict

from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types


CONVERSATION_INSTRUCTION = """You are Intentive, a realtime ADHD support assistant.
The user is live in the app and can hear you.

Non-negotiables:
- Keep replies short and practical (1-2 spoken sentences whenever possible).
- Ground yourself with real context before advising.
- Never schedule reminders manually.
- Do not mention background modes.

Session context:
- The runtime injects profile_context and entry_context.
- entry_mode can be proactive or reactive.
- trigger_type may be post_onboarding for onboarding handoff.

How to start each conversation:
1) Choose startup style:
   - Proactive startup (entry_mode=proactive): use due diligence context already injected by runtime.
   - Reactive startup (entry_mode=reactive): give one short opener first, then use context when needed.
   - Post-onboarding handoff (trigger_type=post_onboarding): treat as reactive.
2) Then choose opening behavior:
   - proactive: address the specific transition intention immediately.
   - reactive: ask what the user needs right now, then guide.

Day-planning workflow (ADHD scaffold):
1) Brain dump today's commitments quickly.
2) Prioritize top 2-3 high-impact essentials.
3) Timebox essentials with realistic durations.
4) Add transition buffers between intense blocks.
5) Confirm the first tiny action to build momentum now.

Reminder policy:
- Only timeboxed essentials are check-in worthy.
- Unscheduled tasks stay in task lists and should not get reminder framing.
"""


class SimpleADK:
    """Google ADK-backed agent runner with in-memory ADK session service."""

    def __init__(self) -> None:
        self.app_name = "intentive_agent"
        self.model_name = "gemini-2.0-flash"

        self.agent = LlmAgent(
            name="intentive_coach",
            model=self.model_name,
            instruction=CONVERSATION_INSTRUCTION,
        )
        self.session_service = InMemorySessionService()
        self.runner = Runner(
            app_name=self.app_name,
            agent=self.agent,
            session_service=self.session_service,
            auto_create_session=True,
        )
        self.has_credentials = bool(os.getenv("GOOGLE_API_KEY"))

    def _prompt_with_context(
        self,
        prompt: str,
        context: Dict[str, str] | None,
    ) -> str:
        clean_prompt = prompt.strip()
        if not clean_prompt:
            return ""
        if not context:
            return clean_prompt
        context_lines = "\n".join(f"- {key}: {value}" for key, value in context.items())
        return (
            "Use this execution context while replying:\n"
            f"{context_lines}\n\n"
            f"User message:\n{clean_prompt}"
        )

    async def run_stream(
        self,
        *,
        prompt: str,
        user_id: str,
        session_id: str,
        context: Dict[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        composed_prompt = self._prompt_with_context(prompt, context)
        if not composed_prompt:
            yield "Please send a non-empty prompt."
            return
        if not self.has_credentials:
            raise RuntimeError(
                "Missing Google ADK credentials. Set GOOGLE_API_KEY."
            )

        content = types.Content(role="user", parts=[types.Part(text=composed_prompt)])
        last_seen = ""

        async with aclosing(
            self.runner.run_async(
                user_id=user_id,
                session_id=session_id,
                new_message=content,
            )
        ) as agen:
            try:
                async for event in agen:
                    if event.author == "user":
                        continue
                    if not event.content or not event.content.parts:
                        continue
                    text = "".join(part.text or "" for part in event.content.parts)
                    if not text:
                        continue

                    if text.startswith(last_seen):
                        delta = text[len(last_seen):]
                        last_seen = text
                    else:
                        delta = text
                        last_seen += text

                    if delta:
                        yield delta
            except Exception as error:
                raise RuntimeError(str(error)) from error

    async def run(
        self,
        *,
        prompt: str,
        user_id: str,
        session_id: str,
        context: Dict[str, str] | None = None,
    ) -> str:
        chunks: list[str] = []
        async for chunk in self.run_stream(
            prompt=prompt,
            user_id=user_id,
            session_id=session_id,
            context=context,
        ):
            chunks.append(chunk)
        return "".join(chunks).strip()
