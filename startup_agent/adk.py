import logging
import os
import json
from contextlib import aclosing
from dataclasses import dataclass, field
from typing import Any, AsyncGenerator, Awaitable, Callable, Dict, Tuple

from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools import FunctionTool, ToolContext
from google.genai import types

from startup_agent.task_actions import (
    TASK_WRITE_INTENTS,
    TaskManagementIntent,
    TaskQueryType,
)


logger = logging.getLogger(__name__)

CONVERSATION_INSTRUCTION = """You are Intentive, a realtime ADHD support assistant.
The user is live in the app and can hear you.

Non-negotiables:
- Keep replies short and practical (1-2 spoken sentences whenever possible).
- Ground yourself with real context before advising.
- Never schedule reminders manually.
- Do not mention background modes.
- For task writes (capture, prioritize, timebox, status, delete, reschedule), call task_management before claiming success.
- Use task_query for reads (task list and schedule lookups).
- Do not claim a task is scheduled/rescheduled unless task_management reports schedule writes applied.

Session context:
- The runtime injects profile_context and entry_context.
- The runtime may also inject due diligence task/schedule context from persisted data.
- The runtime injects due_diligence_time, which includes the user's current local time.
- The runtime may inject missed_proactive_count and missed_proactive_events (today-only missed invites).
- entry_mode can be proactive or reactive.

How to start each conversation:
1) Choose startup style:
   - Proactive startup (entry_mode=proactive): use due diligence context already injected by runtime.
   - Reactive startup (entry_mode=reactive): give one short opener first, then use context when needed.
2) Then choose opening behavior:
   - proactive: address the specific transition intention immediately.
   - reactive: ask what the user needs right now, then guide.
3) Missed invite check (once-only):
   - If missed_proactive_count > 0, add one gentle sentence acknowledging missed invites.
   - Ask whether to quickly address that miss or continue the current need.
   - If the user declines or ignores it, continue the current need immediately.

Task tool contracts:
- task_management(intent, entities_json, options_json)
  intents: capture | timebox | prioritize | status | delete | reschedule
- task_query(query, payload_json)
  queries: tasks_overview | schedule_day

Time elicitation policy (required):
1) Lead with time-anchored prompts, not generic prompts.
   - Avoid: "What do you want to do now?"
   - Prefer: "It's around 6 PM now. What is your main focus from 6:00 to 8:00?"
2) Ask in time windows when planning:
   - "From [window A], what do you want to do?"
   - "After that, from [window B], what is next?"
3) If the user names a task with explicit time, schedule from that same conversation turn:
   - Use one task_management call with intent="capture" and explicit entities.
   - If an existing task is being moved, use intent="reschedule".
   - Preferred entities payload:
     {"tasks":[{"title":"Dog walk","start_at":"<ISO>","end_at":"<ISO>"}]}
   - Alternative entities payload:
     {"tasks":[{"title":"Dog walk","at_local":"10:00 PM","duration_minutes":30}]}
4) If timing is ambiguous, ask exactly one targeted follow-up that pushes explicit time:
   - Example: "Do you want this at 10:00 PM or 10:30 PM?"
   - After one follow-up, continue coaching and avoid interrogation loops.
5) Confirm final schedule in one concise line:
   - "Got it: Dog walk 10:00-10:30 PM."

Day-planning workflow (ADHD scaffold):
1) Quick brain dump.
2) Prioritize top 2-3 essentials.
3) Turn essentials into explicit time windows.
4) Add transition buffers between intense blocks.
5) Confirm the first tiny action to build momentum now.

Reminder policy:
- Only timeboxed essentials are check-in worthy.
- Unscheduled tasks stay in task lists and should not get reminder framing.

Few-shot guardrails:
Example A (explicit schedule one-shot):
User: "Take my dog out at 10 PM for 30 mins."
Tool: task_management(intent="capture", entities_json={"tasks":[{"title":"Take my dog out","at_local":"10:00 PM","duration_minutes":30}]})
Assistant: "Great, you're set: Take my dog out from 10:00 to 10:30 PM."

Example B (ambiguous time -> one follow-up):
User: "Remind me to do laundry tonight."
Assistant: "Do you want laundry at 9:00 PM or 10:00 PM?"
User: "10."
Tool: task_management(intent="capture", entities_json={"tasks":[{"title":"Laundry","at_local":"10:00 PM","duration_minutes":30}]})
Assistant: "Done, laundry is scheduled for 10:00-10:30 PM."

Example C (query only):
User: "What is left for today?"
Tool: task_query(query="tasks_overview", payload_json={"scope":"today"})
Assistant: concise summary based on tool response with no write claims.
"""

ONBOARDING_INSTRUCTION = """You are Intentive onboarding assistant.
Your goal is to collect only two values: wake time and bedtime.

Rules:
- Keep responses brief and clear.
- Ask for one missing value at a time.
- Accept 24-hour HH:MM values (example: 07:30, 23:15).
- When both values are available, call onboarding_sleep_schedule once.
- After successful tool call, confirm that onboarding is saved and that tomorrow's morning flow is set.
- Do not use task_management or task_query.
"""

GetCurrentTimeToolHandler = Callable[[str | None, Dict[str, str]], Awaitable[Dict[str, Any]]]
TaskManagementToolHandler = Callable[
    [TaskManagementIntent, Dict[str, Any], Dict[str, Any], str | None, str | None, Dict[str, str]],
    Awaitable[Dict[str, Any]],
]
TaskQueryToolHandler = Callable[
    [TaskQueryType, Dict[str, Any], str | None, str | None, Dict[str, str]],
    Awaitable[Dict[str, Any]],
]
OnboardingSleepScheduleToolHandler = Callable[
    [str, str, str | None, str | None, Dict[str, str]],
    Awaitable[Dict[str, Any]],
]


@dataclass(frozen=True)
class TurnExecutionResult:
    assistant_text: str
    task_action_counts: Dict[str, int]
    task_written: bool
    streamed: bool
    agent_mode: str | None = None


@dataclass
class TurnState:
    runtime_context: Dict[str, str]
    task_write_count: int = 0
    task_action_counts: Dict[str, int] = field(default_factory=dict)


class SimpleADK:
    """Google ADK-backed agent runner with optional function tool integration."""

    def __init__(
        self,
        *,
        instruction: str = CONVERSATION_INSTRUCTION,
        agent_name: str = "intentive_coach",
        get_current_time_tool: GetCurrentTimeToolHandler | None = None,
        task_management_tool: TaskManagementToolHandler | None = None,
        task_query_tool: TaskQueryToolHandler | None = None,
        onboarding_sleep_schedule_tool: OnboardingSleepScheduleToolHandler | None = None,
        enable_task_tools: bool | None = None,
        enable_onboarding_tool: bool | None = None,
    ) -> None:
        self.app_name = f"{agent_name}_app"
        self.model_name = "gemini-3.1-pro-preview"
        self._get_current_time_tool = get_current_time_tool
        self._task_management_tool = task_management_tool
        self._task_query_tool = task_query_tool
        self._onboarding_sleep_schedule_tool = onboarding_sleep_schedule_tool
        self._turn_states: Dict[Tuple[str, str], TurnState] = {}

        tools = []
        use_tools = enable_task_tools if enable_task_tools is not None else False
        if use_tools and get_current_time_tool and task_management_tool:
            tools.extend([FunctionTool(self.get_current_time), FunctionTool(self.task_management)])
            if task_query_tool:
                tools.append(FunctionTool(self.task_query))
        use_onboarding_tool = (
            enable_onboarding_tool if enable_onboarding_tool is not None else False
        )
        if use_onboarding_tool and onboarding_sleep_schedule_tool:
            tools.append(FunctionTool(self.onboarding_sleep_schedule))

        self.agent = LlmAgent(
            name=agent_name,
            model=self.model_name,
            instruction=instruction,
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

    def _runtime_context_map(self, context: Dict[str, str] | None) -> Dict[str, str]:
        return dict(context or {})

    def _prompt_with_context(
        self,
        prompt: str,
        context: Dict[str, str] | None,
    ) -> str:
        clean_prompt = prompt.strip()
        if not clean_prompt:
            return ""
        runtime_context = self._runtime_context_map(context)
        if not runtime_context:
            return clean_prompt
        context_lines = "\n".join(
            f"- {key}: {value}" for key, value in runtime_context.items()
        )
        return (
            "Use this execution context while replying:\n"
            f"{context_lines}\n\n"
            f"User message:\n{clean_prompt}"
        )

    def _turn_key(self, *, user_id: str, session_id: str) -> Tuple[str, str]:
        return (user_id, session_id)

    def _start_turn_state(
        self,
        *,
        user_id: str,
        session_id: str,
        context: Dict[str, str] | None,
    ) -> Tuple[str, str]:
        key = self._turn_key(user_id=user_id, session_id=session_id)
        self._turn_states[key] = TurnState(
            runtime_context=self._runtime_context_map(context)
        )
        return key

    def _finish_turn_state(
        self,
        *,
        key: Tuple[str, str],
        assistant_text: str,
        streamed: bool,
    ) -> TurnExecutionResult:
        state = self._turn_states.pop(key, TurnState(runtime_context={}))
        return TurnExecutionResult(
            assistant_text=assistant_text,
            task_action_counts={
                str(name): int(count)
                for name, count in state.task_action_counts.items()
                if isinstance(count, int)
            },
            task_written=state.task_write_count > 0,
            streamed=streamed,
        )

    def _record_task_output(
        self,
        *,
        key: Tuple[str, str] | None,
        intent: TaskManagementIntent,
        output: Dict[str, Any],
    ) -> None:
        if key is None or output.get("ok") is not True:
            return
        state = self._turn_states.get(key)
        if state is None:
            return
        if intent in TASK_WRITE_INTENTS:
            state.task_write_count += 1
        state.task_action_counts[intent] = state.task_action_counts.get(intent, 0) + 1
        telemetry = output.get("result", {}).get("telemetry", {})
        if isinstance(telemetry, dict):
            timeboxed_count = telemetry.get("tasks_timeboxed")
            if isinstance(timeboxed_count, int) and timeboxed_count > 0:
                state.task_action_counts["timebox"] = (
                    state.task_action_counts.get("timebox", 0) + timeboxed_count
                )
            rescheduled_count = telemetry.get("tasks_rescheduled")
            if isinstance(rescheduled_count, int) and rescheduled_count > 0:
                state.task_action_counts["reschedule"] = (
                    state.task_action_counts.get("reschedule", 0) + rescheduled_count
                )

    def _tool_runtime_context(self, tool_context: ToolContext | None) -> Dict[str, str]:
        if not tool_context:
            return {}
        key = self._turn_key(user_id=tool_context.user_id, session_id=tool_context.session.id)
        state = self._turn_states.get(key)
        return dict(state.runtime_context if state else {})

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
        intent: TaskManagementIntent,
        entities_json: str | None = None,
        options_json: str | None = None,
        session_id: str | None = None,
        timezone: str | None = None,
        tool_context: ToolContext | None = None,
    ) -> Dict[str, Any]:
        """Execute task writes via deterministic intent + entities payload.

        Example:
        - intent="capture", entities_json={"tasks":[{"title":"Dog walk","at_local":"10:00 PM","duration_minutes":30}]}
        """
        if not self._task_management_tool:
            return {"ok": False, "error": "task_management tool is unavailable"}
        context_key: Tuple[str, str] | None = None
        if tool_context:
            context_key = (tool_context.user_id, tool_context.session.id)
        runtime_context = self._tool_runtime_context(tool_context)
        normalized_entities: Dict[str, Any] = {}
        if isinstance(entities_json, str) and entities_json.strip():
            try:
                parsed_payload = json.loads(entities_json)
                if isinstance(parsed_payload, dict):
                    normalized_entities = parsed_payload
                else:
                    logger.error(
                        "task_management entities_json parsed to non-dict (%s); using {}. entities_json=%r",
                        type(parsed_payload).__name__,
                        entities_json,
                    )
            except json.JSONDecodeError as e:
                logger.error(
                    "task_management entities_json JSONDecodeError; using {}. error=%s entities_json=%r",
                    str(e),
                    entities_json,
                )
                normalized_entities = {}
        normalized_options: Dict[str, Any] = {}
        if isinstance(options_json, str) and options_json.strip():
            try:
                parsed_options = json.loads(options_json)
                if isinstance(parsed_options, dict):
                    normalized_options = parsed_options
            except json.JSONDecodeError:
                normalized_options = {}
        output = await self._task_management_tool(
            intent,
            normalized_entities,
            normalized_options,
            session_id,
            timezone,
            runtime_context,
        )
        self._record_task_output(
            key=context_key,
            intent=intent,
            output=output,
        )
        return output

    async def task_query(
        self,
        query: TaskQueryType,
        payload_json: str | None = None,
        session_id: str | None = None,
        timezone: str | None = None,
        tool_context: ToolContext | None = None,
    ) -> Dict[str, Any]:
        """Execute task reads."""
        if not self._task_query_tool:
            return {"ok": False, "error": "task_query tool is unavailable"}
        runtime_context = self._tool_runtime_context(tool_context)
        normalized_payload: Dict[str, Any] = {}
        if isinstance(payload_json, str) and payload_json.strip():
            try:
                parsed_payload = json.loads(payload_json)
                if isinstance(parsed_payload, dict):
                    normalized_payload = parsed_payload
            except json.JSONDecodeError:
                normalized_payload = {}
        return await self._task_query_tool(
            query,
            normalized_payload,
            session_id,
            timezone,
            runtime_context,
        )

    async def onboarding_sleep_schedule(
        self,
        wake_time: str,
        bedtime: str,
        session_id: str | None = None,
        timezone: str | None = None,
        tool_context: ToolContext | None = None,
    ) -> Dict[str, Any]:
        if not self._onboarding_sleep_schedule_tool:
            return {"ok": False, "error": "onboarding_sleep_schedule tool is unavailable"}
        runtime_context = self._tool_runtime_context(tool_context)
        resolved_session_id = session_id
        if not resolved_session_id and tool_context:
            resolved_session_id = tool_context.session.id
        return await self._onboarding_sleep_schedule_tool(
            str(wake_time or ""),
            str(bedtime or ""),
            resolved_session_id,
            timezone,
            runtime_context,
        )

    def _format_model_error(self, error: Exception) -> RuntimeError:
        message = str(error)
        if "no longer available to new users" in message:
            message = (
                f"{message} Update startup_agent/adk.py with a currently available model."
            )
        return RuntimeError(message)

    def _append_model_text(self, last_seen: str, text: str) -> Tuple[str, str]:
        if text.startswith(last_seen):
            return text[len(last_seen):], text
        return text, last_seen + text

    async def _iter_model_deltas(
        self,
        *,
        user_id: str,
        session_id: str,
        content: types.Content,
    ) -> AsyncGenerator[Tuple[str, str], None]:
        last_seen = ""
        try:
            async with aclosing(
                self.runner.run_async(
                    user_id=user_id,
                    session_id=session_id,
                    new_message=content,
                )
            ) as agen:
                async for event in agen:
                    if event.author == "user":
                        continue
                    if not event.content or not event.content.parts:
                        continue
                    text = "".join(part.text or "" for part in event.content.parts)
                    if not text:
                        continue
                    delta, last_seen = self._append_model_text(last_seen, text)
                    yield delta, last_seen
        except Exception as error:
            raise self._format_model_error(error) from error

    async def run_stream(
        self,
        *,
        prompt: str,
        user_id: str,
        session_id: str,
        context: Dict[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        runtime_context = self._runtime_context_map(context)
        composed_prompt = self._prompt_with_context(prompt, runtime_context)
        if not composed_prompt:
            yield "Please send a non-empty prompt."
            return
        if not self.has_credentials:
            raise RuntimeError(
                "Missing Google ADK credentials. Set GOOGLE_API_KEY."
            )

        content = types.Content(role="user", parts=[types.Part(text=composed_prompt)])
        context_key = self._start_turn_state(
            user_id=user_id,
            session_id=session_id,
            context=runtime_context,
        )
        try:
            async for delta, _ in self._iter_model_deltas(
                user_id=user_id,
                session_id=session_id,
                content=content,
            ):
                if delta:
                    yield delta
        finally:
            self._turn_states.pop(context_key, None)

    async def _execute_turn(
        self,
        *,
        prompt: str,
        user_id: str,
        session_id: str,
        context: Dict[str, str] | None,
        streamed: bool,
        on_delta: Callable[[str, str], Awaitable[bool]] | None = None,
    ) -> TurnExecutionResult | None:
        runtime_context = self._runtime_context_map(context)
        composed_prompt = self._prompt_with_context(prompt, runtime_context)
        if not composed_prompt:
            return TurnExecutionResult(
                assistant_text="Please send a non-empty prompt.",
                task_action_counts={},
                task_written=False,
                streamed=streamed,
            )
        if not self.has_credentials:
            raise RuntimeError("Missing Google ADK credentials. Set GOOGLE_API_KEY.")

        content = types.Content(role="user", parts=[types.Part(text=composed_prompt)])
        context_key = self._start_turn_state(
            user_id=user_id,
            session_id=session_id,
            context=runtime_context,
        )
        last_seen = ""
        try:
            async for delta, cumulative in self._iter_model_deltas(
                user_id=user_id,
                session_id=session_id,
                content=content,
            ):
                last_seen = cumulative
                if on_delta and delta and not await on_delta(delta, cumulative):
                    self._turn_states.pop(context_key, None)
                    return None
        except RuntimeError:
            self._turn_states.pop(context_key, None)
            raise

        return self._finish_turn_state(
            key=context_key,
            assistant_text=last_seen,
            streamed=streamed,
        )

    async def run_turn(
        self,
        *,
        prompt: str,
        user_id: str,
        session_id: str,
        context: Dict[str, str] | None = None,
    ) -> TurnExecutionResult:
        result = await self._execute_turn(
            prompt=prompt,
            user_id=user_id,
            session_id=session_id,
            context=context,
            streamed=False,
        )
        assert result is not None
        return result

    async def run_stream_turn(
        self,
        *,
        prompt: str,
        user_id: str,
        session_id: str,
        context: Dict[str, str] | None = None,
        on_delta: Callable[[str, str], Awaitable[bool]],
    ) -> TurnExecutionResult | None:
        return await self._execute_turn(
            prompt=prompt,
            user_id=user_id,
            session_id=session_id,
            context=context,
            streamed=True,
            on_delta=on_delta,
        )

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
