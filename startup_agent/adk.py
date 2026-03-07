import logging
import os
import json
from contextlib import aclosing
from typing import Any, AsyncGenerator, Awaitable, Callable, Dict, Tuple

from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools import FunctionTool, ToolContext
from google.genai import types


logger = logging.getLogger(__name__)

CONVERSATION_INSTRUCTION = """You are Intentive, a realtime ADHD support assistant.
The user is live in the app and can hear you.

Non-negotiables:
- Keep replies short and practical (1-2 spoken sentences whenever possible).
- Ground yourself with real context before advising.
- Never schedule reminders manually.
- Do not mention background modes.

Session context:
- The runtime injects profile_context and entry_context.
- The runtime may also inject due diligence task/schedule context from persisted data.
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

GetCurrentTimeToolHandler = Callable[[str | None, Dict[str, str]], Awaitable[Dict[str, Any]]]
TaskManagementToolHandler = Callable[
    [str, Dict[str, Any], str | None, str | None, Dict[str, str]],
    Awaitable[Dict[str, Any]],
]


class SimpleADK:
    """Google ADK-backed agent runner with optional function tool integration."""

    def __init__(
        self,
        *,
        get_current_time_tool: GetCurrentTimeToolHandler | None = None,
        task_management_tool: TaskManagementToolHandler | None = None,
        enable_task_tools: bool | None = None,
    ) -> None:
        self.app_name = "intentive_agent"
        self.model_name = "gemini-3.1-pro-preview"
        self._get_current_time_tool = get_current_time_tool
        self._task_management_tool = task_management_tool
        self._runtime_context: Dict[Tuple[str, str], Dict[str, str]] = {}

        tools = []
        use_tools = enable_task_tools if enable_task_tools is not None else False
        if use_tools and get_current_time_tool and task_management_tool:
            tools.extend([FunctionTool(self.get_current_time), FunctionTool(self.task_management)])

        self.agent = LlmAgent(
            name="intentive_coach",
            model=self.model_name,
            instruction=CONVERSATION_INSTRUCTION,
            tools=tools,
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

    def _tool_runtime_context(self, tool_context: ToolContext | None) -> Dict[str, str]:
        if not tool_context:
            return {}
        key = (tool_context.user_id, tool_context.session.id)
        return dict(self._runtime_context.get(key, {}))

    async def get_current_time(
        self,
        timezone: str | None = None,
        tool_context: ToolContext | None = None,
    ) -> Dict[str, Any]:
        if not self._get_current_time_tool:
            return {"ok": False, "error": "get_current_time tool is unavailable"}
        runtime_context = self._tool_runtime_context(tool_context)
        return await self._get_current_time_tool(timezone, runtime_context)

    async def task_management(
        self,
        action: str,
        payload_json: str | None = None,
        session_id: str | None = None,
        timezone: str | None = None,
        tool_context: ToolContext | None = None,
    ) -> Dict[str, Any]:
        if not self._task_management_tool:
            return {"ok": False, "error": "task_management tool is unavailable"}
        runtime_context = self._tool_runtime_context(tool_context)
        normalized_payload: Dict[str, Any] = {}
        if isinstance(payload_json, str) and payload_json.strip():
            try:
                parsed_payload = json.loads(payload_json)
                if isinstance(parsed_payload, dict):
                    normalized_payload = parsed_payload
                else:
                    logger.error(
                        "task_management payload_json parsed to non-dict (%s); using {}. payload_json=%r",
                        type(parsed_payload).__name__,
                        payload_json,
                    )
            except json.JSONDecodeError as e:
                logger.error(
                    "task_management payload_json JSONDecodeError; using {}. error=%s payload_json=%r",
                    str(e),
                    payload_json,
                )
                normalized_payload = {}
        return await self._task_management_tool(
            action,
            normalized_payload,
            session_id,
            timezone,
            runtime_context,
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
        context_key = (user_id, session_id)
        self._runtime_context[context_key] = dict(context or {})

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
                message = str(error)
                if "no longer available to new users" in message:
                    message = (
                        f"{message} Update startup_agent/adk.py with a currently available model."
                    )
                raise RuntimeError(message) from error
            finally:
                self._runtime_context.pop(context_key, None)

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
