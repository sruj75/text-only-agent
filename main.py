import json
import logging
import os
import re
import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from startup_agent.adk import SimpleADK

logger = logging.getLogger("intentive.agent")

TASK_MGMT_V1_ENABLED = os.getenv("TASK_MGMT_V1", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
TASK_TOOL_CALLING_V1_ENABLED = os.getenv("TASK_TOOL_CALLING_V1", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}

TaskManagementAction = Literal[
    "capture_tasks",
    "get_tasks",
    "set_top_essentials",
    "timebox_task",
    "get_schedule",
    "update_task_status",
]
TaskStatus = Literal["todo", "in_progress", "done", "blocked"]
TaskTriggerType = Literal["before_task", "transition", "after_task"]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return _iso_from_dt(_utc_now())


def _iso_from_dt(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    parsed = datetime.fromisoformat(cleaned)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_timezone(raw_tz: str | None) -> str | None:
    if not raw_tz:
        return None
    try:
        ZoneInfo(raw_tz)
        return raw_tz
    except ZoneInfoNotFoundError:
        return None


def _timezone_from_user_profile(user_profile: Dict[str, Any] | None) -> str | None:
    if not isinstance(user_profile, dict):
        return None
    stored = _normalize_timezone(str(user_profile.get("timezone") or "").strip())
    return stored


def _local_date_key(timezone_name: str) -> str:
    now_local = datetime.now(ZoneInfo(timezone_name))
    return now_local.strftime("%Y-%m-%d")


def _date_key_for_datetime(value: datetime, timezone_name: str) -> str:
    local_value = value.astimezone(ZoneInfo(timezone_name))
    return local_value.strftime("%Y-%m-%d")


def _daily_session_id(device_id: str, timezone_name: str) -> str:
    return f"session_{device_id}_{_local_date_key(timezone_name)}"


def _session_title(session_id: str, date_key: str, state: Dict[str, Any]) -> str:
    configured = state.get("title")
    if isinstance(configured, str) and configured.strip():
        return configured.strip()
    if session_id.endswith(date_key):
        return f"Daily Plan ({date_key})"
    return f"Thread {session_id[-8:]}"


def _target_date_key(raw_date: str | None, timezone_name: str) -> str:
    if not raw_date or raw_date == "today":
        return _local_date_key(timezone_name)
    return raw_date


def _is_valid_hhmm(value: str) -> bool:
    return bool(re.match(r"^(?:[01][0-9]|2[0-3]):[0-5][0-9]$", value))


def _normalize_text_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    output: List[str] = []
    for item in value:
        text = str(item).strip()
        if text:
            output.append(text)
    return output


def _default_health_anchors(wake_time: str, bedtime: str) -> List[str]:
    anchors: List[str] = []
    if _is_valid_hhmm(wake_time):
        anchors.append(f"Morning reset around {wake_time}")
    if _is_valid_hhmm(bedtime):
        anchors.append(f"Night shutdown around {bedtime}")
    if not anchors:
        anchors.append("Daily consistency check-in")
    return anchors


def _get_current_time_context(timezone_name: str) -> Dict[str, str]:
    now_utc = _utc_now()
    now_local = now_utc.astimezone(ZoneInfo(timezone_name))
    return {
        "utc": _iso_from_dt(now_utc),
        "local": now_local.isoformat(),
        "timezone": timezone_name,
        "date": now_local.strftime("%Y-%m-%d"),
        "time": now_local.strftime("%H:%M"),
    }


class EntryContext(BaseModel):
    source: Literal["manual", "push"] = "manual"
    event_id: str | None = None
    trigger_type: str | None = None
    scheduled_time: str | None = None
    calendar_event_id: str | None = None
    entry_mode: Literal["reactive", "proactive"] = "reactive"


class AgentRunRequest(BaseModel):
    prompt: str = Field(..., min_length=1, description="User prompt for the agent")
    context: Dict[str, str] | None = Field(
        default=None,
        description="Optional small context map",
    )


class DeviceBootstrapRequest(BaseModel):
    device_id: str | None = Field(default=None)
    timezone: str | None = Field(default=None)
    session_id: str | None = Field(default=None)
    entry_context: EntryContext | None = Field(default=None)


class ThreadCreateRequest(BaseModel):
    device_id: str
    timezone: str | None = Field(default=None)
    title: str | None = Field(default=None)


class PushTokenRequest(BaseModel):
    device_id: str
    expo_push_token: str = Field(..., min_length=1)
    timezone: str | None = Field(default=None)


class OnboardingCompleteRequest(BaseModel):
    device_id: str
    timezone: str | None = Field(default=None)
    wake_time: str = Field(..., min_length=1)
    bedtime: str = Field(..., min_length=1)
    playbook: str = Field(..., min_length=1)
    health_anchors: List[str] = Field(default_factory=list)


class TaskManagementRequest(BaseModel):
    device_id: str
    timezone: str | None = Field(default=None)
    session_id: str | None = Field(default=None)
    action: TaskManagementAction
    payload: Dict[str, Any] = Field(default_factory=dict)


class MessageRecord(BaseModel):
    id: str
    session_id: str
    user_id: str
    role: Literal["user", "assistant", "system", "event"]
    content: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=_iso_now)


class SessionRecord(BaseModel):
    session_id: str
    user_id: str
    date: str
    state: Dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=_iso_now)
    updated_at: str = Field(default_factory=_iso_now)


class TaskTimebox(BaseModel):
    start_at: str
    end_at: str


class TaskItem(BaseModel):
    task_id: str
    title: str
    priority_rank: int | None = None
    is_essential: bool = False
    status: TaskStatus = "todo"
    created_at: str = Field(default_factory=_iso_now)
    timebox: TaskTimebox | None = None


class TaskStateV1(BaseModel):
    date: str
    timezone: str
    tasks: List[TaskItem] = Field(default_factory=list)
    updated_at: str = Field(default_factory=_iso_now)


class CheckinEventRecord(BaseModel):
    id: str
    user_id: str
    scheduled_time: str
    event_type: str = "checkin"
    payload: Dict[str, Any] = Field(default_factory=dict)
    executed: bool = False
    cron_job_id: int | None = None
    last_error: str | None = None
    attempt_count: int = 0
    created_at: str = Field(default_factory=_iso_now)
    updated_at: str = Field(default_factory=_iso_now)


class InMemoryRepository:
    def __init__(self) -> None:
        self.users: Dict[str, Dict[str, Any]] = {}
        self.sessions: Dict[str, SessionRecord] = {}
        self.messages: Dict[str, List[MessageRecord]] = {}
        self.push_tokens: Dict[str, str] = {}
        self.events: Dict[str, CheckinEventRecord] = {}
        self.next_job_id = 1000

    async def close(self) -> None:
        return None

    async def ensure_user(self, user_id: str, timezone_name: str) -> None:
        existing = self.users.get(user_id, {})
        existing["user_id"] = user_id
        existing["timezone"] = timezone_name
        existing["updated_at"] = _iso_now()
        existing.setdefault("created_at", _iso_now())
        existing.setdefault("wake_time", None)
        existing.setdefault("bedtime", None)
        existing.setdefault("health_anchors", [])
        existing.setdefault("playbook", {})
        existing.setdefault("onboarding_status", "pending")
        existing.setdefault("onboarding_completed_at", None)
        self.users[user_id] = existing

    async def upsert_push_token(self, user_id: str, expo_push_token: str) -> None:
        self.push_tokens[user_id] = expo_push_token

    async def get_user_profile(self, user_id: str) -> Dict[str, Any] | None:
        record = self.users.get(user_id)
        if not record:
            return None
        return dict(record)

    async def has_conversation_history(self, user_id: str) -> bool:
        for bucket in self.messages.values():
            for message in bucket:
                if message.user_id == user_id and message.role in {"user", "assistant"}:
                    return True
        return False

    async def upsert_user_profile(
        self,
        *,
        user_id: str,
        timezone_name: str,
        wake_time: str,
        bedtime: str,
        playbook: Dict[str, Any],
        health_anchors: List[str],
        onboarding_status: str,
        onboarding_completed_at: str | None,
    ) -> Dict[str, Any]:
        await self.ensure_user(user_id=user_id, timezone_name=timezone_name)
        existing = self.users.get(user_id, {})
        existing["timezone"] = timezone_name
        existing["wake_time"] = wake_time
        existing["bedtime"] = bedtime
        existing["playbook"] = playbook
        existing["health_anchors"] = health_anchors
        existing["onboarding_status"] = onboarding_status
        existing["onboarding_completed_at"] = onboarding_completed_at
        existing["updated_at"] = _iso_now()
        self.users[user_id] = existing
        return dict(existing)

    async def get_session(self, session_id: str, user_id: str) -> SessionRecord | None:
        record = self.sessions.get(session_id)
        if not record or record.user_id != user_id:
            return None
        return record

    async def get_session_by_id(self, session_id: str) -> SessionRecord | None:
        return self.sessions.get(session_id)

    async def upsert_session(self, payload: SessionRecord) -> SessionRecord:
        existing = self.sessions.get(payload.session_id)
        merged_state = dict(existing.state if existing else {})
        merged_state.update(payload.state)
        now = _iso_now()
        next_record = SessionRecord(
            session_id=payload.session_id,
            user_id=payload.user_id,
            date=payload.date,
            state=merged_state,
            created_at=existing.created_at if existing else now,
            updated_at=now,
        )
        self.sessions[payload.session_id] = next_record
        return next_record

    async def list_sessions(self, user_id: str) -> List[SessionRecord]:
        rows = [s for s in self.sessions.values() if s.user_id == user_id]
        rows.sort(key=lambda row: row.updated_at, reverse=True)
        return rows

    async def list_messages(self, session_id: str) -> List[MessageRecord]:
        return list(self.messages.get(session_id, []))

    async def message_exists(self, session_id: str, message_id: str) -> bool:
        return any(msg.id == message_id for msg in self.messages.get(session_id, []))

    async def insert_message(self, payload: MessageRecord) -> None:
        bucket = self.messages.setdefault(payload.session_id, [])
        if any(msg.id == payload.id for msg in bucket):
            return
        bucket.append(payload)
        bucket.sort(key=lambda row: row.created_at)

    async def list_future_task_events(
        self,
        *,
        user_id: str,
        date_key: str | None,
        from_time_iso: str,
    ) -> List[CheckinEventRecord]:
        from_time = _parse_iso(from_time_iso)
        rows: List[CheckinEventRecord] = []
        for row in self.events.values():
            if row.user_id != user_id:
                continue
            if row.event_type != "checkin" or row.executed:
                continue
            if row.payload.get("schedule_owner") != "task_management":
                continue
            if date_key and row.payload.get("seed_date") != date_key:
                continue
            if _parse_iso(row.scheduled_time) < from_time:
                continue
            rows.append(row)
        rows.sort(key=lambda row: row.scheduled_time)
        return rows

    async def create_event(self, payload: CheckinEventRecord) -> CheckinEventRecord:
        self.events[payload.id] = payload
        return payload

    async def update_event_cron_job(self, event_id: str, cron_job_id: int) -> None:
        existing = self.events.get(event_id)
        if not existing:
            return
        updated = existing.model_copy(
            update={"cron_job_id": cron_job_id, "updated_at": _iso_now()}
        )
        self.events[event_id] = updated

    async def schedule_event_job(
        self,
        *,
        event_id: str,
        run_at: str,
        timezone_name: str,
    ) -> int:
        _ = event_id
        _ = run_at
        _ = timezone_name
        self.next_job_id += 1
        return self.next_job_id

    async def unschedule_event_job(self, cron_job_id: int) -> bool:
        _ = cron_job_id
        return True

    async def delete_event(self, event_id: str) -> None:
        self.events.pop(event_id, None)


class SupabaseRepository:
    def __init__(self, project_url: str, service_role_key: str) -> None:
        base = project_url.rstrip("/")
        self.base_url = f"{base}/rest/v1"
        self.headers = {
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json",
        }
        self.client = httpx.AsyncClient(timeout=15.0)

    async def close(self) -> None:
        await self.client.aclose()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Dict[str, str] | None = None,
        json_body: Any = None,
        extra_headers: Dict[str, str] | None = None,
    ) -> Any:
        headers = dict(self.headers)
        if extra_headers:
            headers.update(extra_headers)
        response = await self.client.request(
            method=method,
            url=f"{self.base_url}/{path}",
            params=params,
            json=json_body,
            headers=headers,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"Supabase error {response.status_code}: {response.text}")
        if not response.text:
            return None
        return response.json()

    async def _rpc(self, name: str, payload: Dict[str, Any]) -> Any:
        return await self._request("POST", f"rpc/{name}", json_body=payload)

    async def ensure_user(self, user_id: str, timezone_name: str) -> None:
        payload = [{"user_id": user_id, "timezone": timezone_name}]
        await self._request(
            "POST",
            "users",
            params={"on_conflict": "user_id"},
            json_body=payload,
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    async def upsert_push_token(self, user_id: str, expo_push_token: str) -> None:
        await self._request(
            "POST",
            "push_tokens",
            params={"on_conflict": "user_id"},
            json_body=[{"user_id": user_id, "expo_push_token": expo_push_token}],
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    async def get_user_profile(self, user_id: str) -> Dict[str, Any] | None:
        rows = await self._request(
            "GET",
            "users",
            params={
                "user_id": f"eq.{user_id}",
                "select": "user_id,wake_time,bedtime,timezone,health_anchors,onboarding_status,onboarding_completed_at,playbook,created_at,updated_at",
                "limit": "1",
            },
        )
        if not rows:
            return None
        return rows[0]

    async def has_conversation_history(self, user_id: str) -> bool:
        rows = await self._request(
            "GET",
            "session_messages",
            params={
                "user_id": f"eq.{user_id}",
                "role": "in.(user,assistant)",
                "select": "id",
                "limit": "1",
            },
        )
        return bool(rows)

    async def upsert_user_profile(
        self,
        *,
        user_id: str,
        timezone_name: str,
        wake_time: str,
        bedtime: str,
        playbook: Dict[str, Any],
        health_anchors: List[str],
        onboarding_status: str,
        onboarding_completed_at: str | None,
    ) -> Dict[str, Any]:
        rows = await self._request(
            "POST",
            "users",
            params={"on_conflict": "user_id"},
            json_body=[
                {
                    "user_id": user_id,
                    "timezone": timezone_name,
                    "wake_time": wake_time,
                    "bedtime": bedtime,
                    "playbook": playbook,
                    "health_anchors": health_anchors,
                    "onboarding_status": onboarding_status,
                    "onboarding_completed_at": onboarding_completed_at,
                }
            ],
            extra_headers={"Prefer": "resolution=merge-duplicates,return=representation"},
        )
        if not rows:
            raise RuntimeError("User profile upsert returned empty response")
        return rows[0]

    async def get_session(self, session_id: str, user_id: str) -> SessionRecord | None:
        rows = await self._request(
            "GET",
            "sessions",
            params={
                "session_id": f"eq.{session_id}",
                "user_id": f"eq.{user_id}",
                "select": "session_id,user_id,date,state,created_at,updated_at",
                "limit": "1",
            },
        )
        if not rows:
            return None
        return SessionRecord(**rows[0])

    async def get_session_by_id(self, session_id: str) -> SessionRecord | None:
        rows = await self._request(
            "GET",
            "sessions",
            params={
                "session_id": f"eq.{session_id}",
                "select": "session_id,user_id,date,state,created_at,updated_at",
                "limit": "1",
            },
        )
        if not rows:
            return None
        return SessionRecord(**rows[0])

    async def upsert_session(self, payload: SessionRecord) -> SessionRecord:
        rows = await self._request(
            "POST",
            "sessions",
            params={"on_conflict": "session_id"},
            json_body=[payload.model_dump()],
            extra_headers={"Prefer": "resolution=merge-duplicates,return=representation"},
        )
        if not rows:
            raise RuntimeError("Session upsert returned empty response")
        return SessionRecord(**rows[0])

    async def list_sessions(self, user_id: str) -> List[SessionRecord]:
        rows = await self._request(
            "GET",
            "sessions",
            params={
                "user_id": f"eq.{user_id}",
                "select": "session_id,user_id,date,state,created_at,updated_at",
                "order": "updated_at.desc",
                "limit": "50",
            },
        )
        return [SessionRecord(**row) for row in rows or []]

    async def list_messages(self, session_id: str) -> List[MessageRecord]:
        rows = await self._request(
            "GET",
            "session_messages",
            params={
                "session_id": f"eq.{session_id}",
                "select": "id,session_id,user_id,role,content,metadata,created_at",
                "order": "created_at.asc",
                "limit": "500",
            },
        )
        return [MessageRecord(**row) for row in rows or []]

    async def message_exists(self, session_id: str, message_id: str) -> bool:
        rows = await self._request(
            "GET",
            "session_messages",
            params={
                "session_id": f"eq.{session_id}",
                "id": f"eq.{message_id}",
                "select": "id",
                "limit": "1",
            },
        )
        return bool(rows)

    async def insert_message(self, payload: MessageRecord) -> None:
        await self._request(
            "POST",
            "session_messages",
            json_body=[payload.model_dump()],
            extra_headers={"Prefer": "return=minimal"},
        )

    async def list_future_task_events(
        self,
        *,
        user_id: str,
        date_key: str | None,
        from_time_iso: str,
    ) -> List[CheckinEventRecord]:
        params: Dict[str, str] = {
            "user_id": f"eq.{user_id}",
            "event_type": "eq.checkin",
            "executed": "eq.false",
            "scheduled_time": f"gte.{from_time_iso}",
            "payload->>schedule_owner": "eq.task_management",
            "select": "id,user_id,scheduled_time,event_type,payload,executed,cron_job_id,last_error,attempt_count,created_at,updated_at",
            "order": "scheduled_time.asc",
            "limit": "500",
        }
        if date_key:
            params["payload->>seed_date"] = f"eq.{date_key}"

        rows = await self._request(
            "GET",
            "events",
            params=params,
        )
        return [CheckinEventRecord(**row) for row in rows or []]

    async def create_event(self, payload: CheckinEventRecord) -> CheckinEventRecord:
        rows = await self._request(
            "POST",
            "events",
            json_body=[payload.model_dump()],
            extra_headers={"Prefer": "return=representation"},
        )
        if not rows:
            raise RuntimeError("Event insert returned empty response")
        return CheckinEventRecord(**rows[0])

    async def update_event_cron_job(self, event_id: str, cron_job_id: int) -> None:
        await self._request(
            "PATCH",
            "events",
            params={"id": f"eq.{event_id}"},
            json_body={"cron_job_id": cron_job_id, "updated_at": _iso_now()},
            extra_headers={"Prefer": "return=minimal"},
        )

    async def schedule_event_job(
        self,
        *,
        event_id: str,
        run_at: str,
        timezone_name: str,
    ) -> int:
        output = await self._rpc(
            "schedule_event_job",
            {
                "p_event_id": event_id,
                "p_run_at": run_at,
                "p_timezone": timezone_name,
            },
        )
        if isinstance(output, int):
            return output
        if isinstance(output, float):
            return int(output)
        if isinstance(output, str) and output.strip().isdigit():
            return int(output)
        if isinstance(output, list) and output:
            first = output[0]
            if isinstance(first, dict):
                value = first.get("schedule_event_job")
                if isinstance(value, int):
                    return value
                if isinstance(value, float):
                    return int(value)
        if isinstance(output, dict):
            value = output.get("schedule_event_job")
            if isinstance(value, int):
                return value
            if isinstance(value, float):
                return int(value)
        raise RuntimeError(f"Unexpected schedule_event_job response: {output}")

    async def unschedule_event_job(self, cron_job_id: int) -> bool:
        output = await self._rpc("unschedule_event_job", {"p_job_id": int(cron_job_id)})
        if isinstance(output, bool):
            return output
        if isinstance(output, list) and output:
            first = output[0]
            if isinstance(first, dict):
                value = first.get("unschedule_event_job")
                if isinstance(value, bool):
                    return value
        if isinstance(output, dict):
            value = output.get("unschedule_event_job")
            if isinstance(value, bool):
                return value
        return False

    async def delete_event(self, event_id: str) -> None:
        await self._request(
            "DELETE",
            "events",
            params={"id": f"eq.{event_id}"},
            extra_headers={"Prefer": "return=minimal"},
        )


def build_repository() -> InMemoryRepository | SupabaseRepository:
    supabase_url = os.getenv("SUPABASE_URL")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if supabase_url and service_role_key:
        logger.info("Using Supabase repository for chat persistence.")
        return SupabaseRepository(project_url=supabase_url, service_role_key=service_role_key)
    logger.warning(
        "SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY missing; using in-memory repository."
    )
    return InMemoryRepository()


repository = build_repository()


async def _tool_get_current_time(
    timezone: str | None,
    runtime_context: Dict[str, str],
) -> Dict[str, Any]:
    raw_user_id = str(runtime_context.get("user_id") or "").strip()
    if not raw_user_id:
        return {"ok": False, "error": "missing_user_id"}

    try:
        timezone_name = await _resolve_timezone_for_user(
            user_id=raw_user_id,
            provided_timezone=timezone or runtime_context.get("timezone"),
        )
    except HTTPException as error:
        return {"ok": False, "error": str(error.detail)}

    return {"ok": True, "current_time": _get_current_time_context(timezone_name)}


async def _tool_task_management(
    action: str,
    payload: Dict[str, Any],
    session_id: str | None,
    timezone: str | None,
    runtime_context: Dict[str, str],
) -> Dict[str, Any]:
    raw_user_id = str(runtime_context.get("user_id") or "").strip()
    if not raw_user_id:
        return {"ok": False, "error": "missing_user_id"}

    raw_session_id = session_id or runtime_context.get("session_id")
    try:
        timezone_name = await _resolve_timezone_for_user(
            user_id=raw_user_id,
            provided_timezone=timezone or runtime_context.get("timezone"),
        )
    except HTTPException as error:
        return {"ok": False, "error": str(error.detail)}

    try:
        output = await _execute_task_management(
            device_id=raw_user_id,
            timezone_name=timezone_name,
            action=action,  # runtime-validated in _execute_task_management
            payload=payload,
            session_id=raw_session_id,
        )
        return {"ok": True, **output}
    except HTTPException as error:
        return {"ok": False, "error": str(error.detail)}
    except Exception as error:
        return {"ok": False, "error": str(error)}


agent = SimpleADK(
    get_current_time_tool=_tool_get_current_time,
    task_management_tool=_tool_task_management,
    enable_task_tools=TASK_TOOL_CALLING_V1_ENABLED,
)


def _active_adk_model_name() -> str:
    direct = getattr(agent, "model_name", None)
    if isinstance(direct, str) and direct.strip():
        return direct.strip()
    nested = getattr(getattr(agent, "agent", None), "model", None)
    if isinstance(nested, str) and nested.strip():
        return nested.strip()
    return "unknown"


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        yield
    finally:
        await repository.close()


app = FastAPI(title="Startup Agent API", version="0.3.0", lifespan=lifespan)


def _entry_context_from_state(state: Dict[str, Any]) -> EntryContext:
    payload = state.get("entry_context")
    if isinstance(payload, dict):
        try:
            return EntryContext(**payload)
        except Exception:
            return EntryContext()
    return EntryContext()


def _build_profile_context(user_profile: Dict[str, Any] | None) -> Dict[str, Any]:
    profile = user_profile or {}
    wake_time = str(profile.get("wake_time") or "").strip()
    bedtime = str(profile.get("bedtime") or "").strip()

    raw_playbook = profile.get("playbook")
    playbook: Dict[str, Any] = raw_playbook if isinstance(raw_playbook, dict) else {}
    anchors = _normalize_text_list(profile.get("health_anchors"))
    onboarding_status = str(profile.get("onboarding_status") or "pending").strip() or "pending"

    return {
        "wake_time": wake_time or None,
        "bedtime": bedtime or None,
        "playbook": playbook,
        "health_anchors": anchors,
        "onboarding_status": onboarding_status,
    }


def _needs_onboarding(user_profile: Dict[str, Any] | None) -> bool:
    status = str((user_profile or {}).get("onboarding_status") or "pending").strip().lower()
    return status != "completed"


async def _resolve_timezone_for_user(
    *,
    user_id: str,
    provided_timezone: str | None,
) -> str:
    direct = _normalize_timezone(provided_timezone)
    if direct:
        return direct

    user_profile = await repository.get_user_profile(user_id=user_id)
    stored = _timezone_from_user_profile(user_profile)
    if stored:
        return stored

    raise HTTPException(
        status_code=400,
        detail="A valid IANA timezone is required (example: Asia/Kolkata).",
    )


_TASK_REBUILD_LOCKS: Dict[str, asyncio.Lock] = {}


def _task_lock_key(user_id: str, date_key: str) -> str:
    return f"{user_id}:{date_key}"


def _get_task_rebuild_lock(user_id: str, date_key: str) -> asyncio.Lock:
    key = _task_lock_key(user_id, date_key)
    lock = _TASK_REBUILD_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _TASK_REBUILD_LOCKS[key] = lock
    return lock


async def _ensure_session(
    *,
    user_id: str,
    session_id: str,
    timezone_name: str,
    entry_context: EntryContext,
    state_patch: Dict[str, Any] | None = None,
) -> SessionRecord:
    existing = await repository.get_session_by_id(session_id=session_id)
    if existing and existing.user_id != user_id:
        raise HTTPException(status_code=403, detail="Session does not belong to device")
    existing_state = dict(existing.state if existing else {})
    existing_state["user_timezone"] = timezone_name
    existing_state["entry_context"] = entry_context.model_dump()
    existing_state["last_seen_at"] = _iso_now()
    if state_patch:
        existing_state.update(state_patch)
    date_key = existing.date if existing else _local_date_key(timezone_name)
    return await repository.upsert_session(
        SessionRecord(
            session_id=session_id,
            user_id=user_id,
            date=date_key,
            state=existing_state,
            created_at=existing.created_at if existing else _iso_now(),
            updated_at=_iso_now(),
        )
    )


async def _thread_summaries(user_id: str) -> List[Dict[str, Any]]:
    rows = await repository.list_sessions(user_id=user_id)
    output: List[Dict[str, Any]] = []
    for row in rows:
        output.append(
            {
                "session_id": row.session_id,
                "date": row.date,
                "title": _session_title(row.session_id, row.date, row.state),
                "updated_at": row.updated_at,
                "state": row.state,
            }
        )
    return output


def _coerce_task_state(state: Dict[str, Any], timezone_name: str) -> TaskStateV1:
    raw = state.get("task_state_v1")
    if isinstance(raw, dict):
        try:
            parsed = TaskStateV1(**raw)
            if not parsed.timezone:
                parsed.timezone = timezone_name
            if not parsed.date:
                parsed.date = _local_date_key(timezone_name)
            return parsed
        except Exception:
            pass
    return TaskStateV1(date=_local_date_key(timezone_name), timezone=timezone_name, tasks=[])


def _extract_titles(payload: Dict[str, Any]) -> List[str]:
    direct = payload.get("titles")
    if isinstance(direct, list):
        return [str(item).strip() for item in direct if str(item).strip()]

    items = payload.get("tasks")
    if not isinstance(items, list):
        return []

    titles: List[str] = []
    for item in items:
        if isinstance(item, dict):
            title = str(item.get("title", "")).strip()
        else:
            title = str(item).strip()
        if title:
            titles.append(title)
    return titles


def _find_task(task_state: TaskStateV1, task_id: str) -> TaskItem | None:
    for task in task_state.tasks:
        if task.task_id == task_id:
            return task
    return None


def _build_schedule_response(
    *,
    task_state: TaskStateV1,
    timezone_name: str,
    raw_date: str | None,
) -> Dict[str, Any]:
    target_date = _target_date_key(raw_date, timezone_name)
    now_utc = _utc_now()

    items: List[Dict[str, Any]] = []
    for task in task_state.tasks:
        if not task.timebox:
            continue
        start_dt = _parse_iso(task.timebox.start_at)
        end_dt = _parse_iso(task.timebox.end_at)
        if _date_key_for_datetime(start_dt, timezone_name) != target_date:
            continue

        items.append(
            {
                "task_id": task.task_id,
                "title": task.title,
                "status": task.status,
                "priority_rank": task.priority_rank,
                "is_essential": task.is_essential,
                "start_at": task.timebox.start_at,
                "end_at": task.timebox.end_at,
                "start_local": start_dt.astimezone(ZoneInfo(timezone_name)).isoformat(),
                "end_local": end_dt.astimezone(ZoneInfo(timezone_name)).isoformat(),
            }
        )

    items.sort(key=lambda row: row["start_at"])

    next_upcoming = None
    for row in items:
        if _parse_iso(row["end_at"]) >= now_utc:
            next_upcoming = row
            break

    return {
        "date": target_date,
        "timezone": timezone_name,
        "items": items,
        "next_upcoming": next_upcoming,
    }


async def _schedule_task_event(
    *,
    user_id: str,
    timezone_name: str,
    date_key: str,
    trigger_type: TaskTriggerType,
    task: TaskItem,
    run_at: datetime,
) -> Dict[str, Any]:
    event_id = str(uuid.uuid4())
    event_payload = {
        "schedule_owner": "task_management",
        "schedule_policy": "timebox_v1",
        "seed_date": date_key,
        "timezone": timezone_name,
        "reason": trigger_type,
        "trigger_type": trigger_type,
        "task_id": task.task_id,
        "task_title": task.title,
        "timebox_start": task.timebox.start_at if task.timebox else None,
        "timebox_end": task.timebox.end_at if task.timebox else None,
    }

    await repository.create_event(
        CheckinEventRecord(
            id=event_id,
            user_id=user_id,
            scheduled_time=_iso_from_dt(run_at),
            event_type="checkin",
            payload=event_payload,
            executed=False,
            cron_job_id=None,
            created_at=_iso_now(),
            updated_at=_iso_now(),
        )
    )

    try:
        job_id = await repository.schedule_event_job(
            event_id=event_id,
            run_at=_iso_from_dt(run_at),
            timezone_name=timezone_name,
        )
    except Exception:
        await repository.delete_event(event_id)
        raise

    await repository.update_event_cron_job(event_id, int(job_id))

    return {
        "event_id": event_id,
        "trigger_type": trigger_type,
        "scheduled_time": _iso_from_dt(run_at),
        "task_id": task.task_id,
        "task_title": task.title,
        "cron_job_id": int(job_id),
    }


def _event_identity_key(*, task_id: str, trigger_type: str, scheduled_time: str) -> str:
    return f"{task_id}:{trigger_type}:{scheduled_time}"


async def _rebuild_task_events(
    *,
    user_id: str,
    timezone_name: str,
    task_state: TaskStateV1,
) -> List[Dict[str, Any]]:
    lock = _get_task_rebuild_lock(user_id, task_state.date)
    async with lock:
        now_iso = _iso_now()
        existing = await repository.list_future_task_events(
            user_id=user_id,
            date_key=None,
            from_time_iso=now_iso,
        )

        existing_by_key: Dict[str, List[CheckinEventRecord]] = {}
        for event in existing:
            payload = event.payload if isinstance(event.payload, dict) else {}
            existing_task_id = str(payload.get("task_id") or "").strip()
            existing_trigger = str(
                payload.get("trigger_type") or payload.get("reason") or ""
            ).strip()
            if not existing_task_id or not existing_trigger:
                continue
            key = _event_identity_key(
                task_id=existing_task_id,
                trigger_type=existing_trigger,
                scheduled_time=event.scheduled_time,
            )
            existing_by_key.setdefault(key, []).append(event)

        timeboxed: List[TaskItem] = [task for task in task_state.tasks if task.timebox is not None]
        timeboxed.sort(key=lambda task: task.timebox.start_at if task.timebox else "")

        desired_specs: List[Dict[str, Any]] = []
        for index, task in enumerate(timeboxed):
            if not task.timebox:
                continue

            start_dt = _parse_iso(task.timebox.start_at)
            end_dt = _parse_iso(task.timebox.end_at)
            seed_date = _date_key_for_datetime(start_dt, timezone_name)

            before_at = start_dt - timedelta(minutes=5)
            desired_specs.append(
                {
                    "task": task,
                    "trigger_type": "before_task",
                    "run_at": before_at,
                    "seed_date": seed_date,
                }
            )

            next_task = timeboxed[index + 1] if index + 1 < len(timeboxed) else None
            next_start: datetime | None = None
            if next_task and next_task.timebox:
                next_start = _parse_iso(next_task.timebox.start_at)

            end_trigger: TaskTriggerType = "after_task"
            if next_start is not None and next_start - end_dt <= timedelta(minutes=10):
                end_trigger = "transition"

            desired_specs.append(
                {
                    "task": task,
                    "trigger_type": end_trigger,
                    "run_at": end_dt,
                    "seed_date": seed_date,
                }
            )

        scheduled: List[Dict[str, Any]] = []
        retained_event_ids: set[str] = set()

        for spec in desired_specs:
            task: TaskItem = spec["task"]
            trigger_type: TaskTriggerType = spec["trigger_type"]
            run_at: datetime = spec["run_at"]
            scheduled_time = _iso_from_dt(run_at)
            key = _event_identity_key(
                task_id=task.task_id,
                trigger_type=trigger_type,
                scheduled_time=scheduled_time,
            )

            existing_matches = existing_by_key.get(key) or []
            if existing_matches:
                matched = existing_matches.pop(0)
                retained_event_ids.add(matched.id)
                scheduled.append(
                    {
                        "event_id": matched.id,
                        "trigger_type": trigger_type,
                        "scheduled_time": matched.scheduled_time,
                        "task_id": task.task_id,
                        "task_title": task.title,
                        "cron_job_id": matched.cron_job_id,
                    }
                )
                continue

            created = await _schedule_task_event(
                user_id=user_id,
                timezone_name=timezone_name,
                date_key=spec["seed_date"],
                trigger_type=trigger_type,
                task=task,
                run_at=run_at,
            )
            scheduled.append(created)

        obsolete: List[CheckinEventRecord] = []
        for event in existing:
            if event.id in retained_event_ids:
                continue
            obsolete.append(event)

        for event in obsolete:
            if event.cron_job_id is not None:
                await repository.unschedule_event_job(int(event.cron_job_id))
            await repository.delete_event(event.id)

        return scheduled


async def _run_task_management_action(
    *,
    action: TaskManagementAction,
    payload: Dict[str, Any],
    task_state: TaskStateV1,
    timezone_name: str,
    user_id: str,
) -> Dict[str, Any]:
    if action == "capture_tasks":
        titles = _extract_titles(payload)
        if not titles:
            raise HTTPException(status_code=400, detail="capture_tasks requires at least one title")

        created_ids: List[str] = []
        for title in titles:
            new_task = TaskItem(
                task_id=str(uuid.uuid4()),
                title=title,
                priority_rank=None,
                is_essential=False,
                status="todo",
                created_at=_iso_now(),
                timebox=None,
            )
            task_state.tasks.append(new_task)
            created_ids.append(new_task.task_id)
        task_state.updated_at = _iso_now()
        return {
            "action": action,
            "created_task_ids": created_ids,
            "tasks": [task.model_dump() for task in task_state.tasks],
        }

    if action == "get_tasks":
        return {
            "action": action,
            "tasks": [task.model_dump() for task in task_state.tasks],
        }

    if action == "set_top_essentials":
        task_ids = payload.get("task_ids")
        if not isinstance(task_ids, list):
            raise HTTPException(status_code=400, detail="set_top_essentials requires task_ids list")
        cleaned_ids = [str(item) for item in task_ids if str(item).strip()]
        if len(cleaned_ids) > 3:
            raise HTTPException(status_code=400, detail="set_top_essentials allows max 3 tasks")

        seen: set[str] = set()
        for task_id in cleaned_ids:
            if task_id in seen:
                raise HTTPException(status_code=400, detail="task_ids contains duplicates")
            seen.add(task_id)
            if _find_task(task_state, task_id) is None:
                raise HTTPException(status_code=404, detail=f"Task not found: {task_id}")

        for task in task_state.tasks:
            task.priority_rank = None
            task.is_essential = False

        for rank, task_id in enumerate(cleaned_ids, start=1):
            task = _find_task(task_state, task_id)
            if task:
                task.priority_rank = rank
                task.is_essential = True

        task_state.updated_at = _iso_now()

        return {
            "action": action,
            "top_essentials": cleaned_ids,
            "tasks": [task.model_dump() for task in task_state.tasks],
        }

    if action == "timebox_task":
        task_id = str(payload.get("task_id") or "").strip()
        start_at = str(payload.get("start_at") or "").strip()
        end_at = str(payload.get("end_at") or "").strip()

        if not task_id or not start_at or not end_at:
            raise HTTPException(
                status_code=400,
                detail="timebox_task requires task_id, start_at, end_at",
            )

        task = _find_task(task_state, task_id)
        if not task:
            raise HTTPException(status_code=404, detail=f"Task not found: {task_id}")
        if task.timebox is not None:
            raise HTTPException(status_code=409, detail="Timebox is immutable in v1")

        try:
            start_dt = _parse_iso(start_at)
            end_dt = _parse_iso(end_at)
        except Exception as error:
            raise HTTPException(status_code=400, detail=f"Invalid datetime: {error}") from error

        if start_dt >= end_dt:
            raise HTTPException(status_code=400, detail="timebox_task requires start_at < end_at")

        start_date_local = _date_key_for_datetime(start_dt, timezone_name)
        end_date_local = _date_key_for_datetime(end_dt, timezone_name)
        target_date = task_state.date or _local_date_key(timezone_name)
        if start_date_local != target_date or end_date_local != target_date:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Timebox must stay within the session day in your timezone "
                    f"({target_date})."
                ),
            )

        task.timebox = TaskTimebox(start_at=_iso_from_dt(start_dt), end_at=_iso_from_dt(end_dt))
        task_state.updated_at = _iso_now()

        scheduled_events = await _rebuild_task_events(
            user_id=user_id,
            timezone_name=timezone_name,
            task_state=task_state,
        )

        return {
            "action": action,
            "task": task.model_dump(),
            "scheduled_events": scheduled_events,
        }

    if action == "get_schedule":
        date_value = payload.get("date")
        date_text = str(date_value) if isinstance(date_value, str) else "today"
        schedule = _build_schedule_response(
            task_state=task_state,
            timezone_name=timezone_name,
            raw_date=date_text,
        )
        return {
            "action": action,
            "schedule": schedule,
        }

    if action == "update_task_status":
        task_id = str(payload.get("task_id") or "").strip()
        status = str(payload.get("status") or "").strip()
        if status not in {"todo", "in_progress", "done", "blocked"}:
            raise HTTPException(status_code=400, detail="Invalid status")
        task = _find_task(task_state, task_id)
        if not task:
            raise HTTPException(status_code=404, detail=f"Task not found: {task_id}")
        task.status = status  # type: ignore[assignment]
        task_state.updated_at = _iso_now()
        return {
            "action": action,
            "task": task.model_dump(),
        }

    raise HTTPException(status_code=400, detail=f"Unsupported action: {action}")


async def _get_task_state_for_session(
    *,
    user_id: str,
    session_id: str,
    timezone_name: str,
) -> tuple[SessionRecord, TaskStateV1]:
    session = await repository.get_session(session_id=session_id, user_id=user_id)
    if not session:
        default_entry = EntryContext(source="manual", entry_mode="reactive")
        session = await _ensure_session(
            user_id=user_id,
            session_id=session_id,
            timezone_name=timezone_name,
            entry_context=default_entry,
            state_patch={
                "thread_type": "daily"
                if session_id.endswith(_local_date_key(timezone_name))
                else "manual",
            },
        )

    task_state = _coerce_task_state(session.state, timezone_name)
    return session, task_state


async def _execute_task_management(
    *,
    device_id: str,
    timezone_name: str,
    action: TaskManagementAction,
    payload: Dict[str, Any],
    session_id: str | None,
) -> Dict[str, Any]:
    allowed_actions = {
        "capture_tasks",
        "get_tasks",
        "set_top_essentials",
        "timebox_task",
        "get_schedule",
        "update_task_status",
    }
    if action not in allowed_actions:
        raise HTTPException(status_code=400, detail=f"Unsupported action: {action}")

    resolved_session_id = session_id or _daily_session_id(device_id, timezone_name)

    await repository.ensure_user(user_id=device_id, timezone_name=timezone_name)

    session, task_state = await _get_task_state_for_session(
        user_id=device_id,
        session_id=resolved_session_id,
        timezone_name=timezone_name,
    )

    if action in {
        "capture_tasks",
        "set_top_essentials",
        "timebox_task",
        "update_task_status",
    }:
        task_state.date = session.date or _local_date_key(timezone_name)
        task_state.timezone = timezone_name

    result = await _run_task_management_action(
        action=action,
        payload=payload,
        task_state=task_state,
        timezone_name=timezone_name,
        user_id=device_id,
    )

    entry_context = _entry_context_from_state(session.state)
    await _ensure_session(
        user_id=device_id,
        session_id=resolved_session_id,
        timezone_name=timezone_name,
        entry_context=entry_context,
        state_patch={"task_state_v1": task_state.model_dump(), "task_state_version": "v1"},
    )

    return {
        "session_id": resolved_session_id,
        "task_state": task_state.model_dump(),
        "result": result,
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "supabase_url": os.getenv("SUPABASE_URL", ""),
        "repository_mode": "supabase"
        if isinstance(repository, SupabaseRepository)
        else "in_memory",
        "task_mgmt_v1_enabled": TASK_MGMT_V1_ENABLED,
        "task_tool_calling_v1_enabled": TASK_TOOL_CALLING_V1_ENABLED,
        "adk_model": _active_adk_model_name(),
    }


@app.post("/agent/run")
async def run_agent(payload: AgentRunRequest) -> Dict[str, str]:
    context = payload.context or {}
    timezone_name = _normalize_timezone(context.get("timezone")) or "UTC"
    user_id = context.get("user_id", "http_user")
    session_id = context.get("session_id", f"session_http_{_local_date_key(timezone_name)}")
    try:
        output = await agent.run(
            prompt=payload.prompt,
            user_id=user_id,
            session_id=session_id,
            context=context,
        )
    except Exception as error:
        model_name = _active_adk_model_name()
        raise HTTPException(
            status_code=503,
            detail=f"Google ADK unavailable (model={model_name}): {error}",
        ) from error
    return {"output": output}


@app.post("/agent/bootstrap-device")
async def bootstrap_device(payload: DeviceBootstrapRequest) -> Dict[str, Any]:
    device_id = payload.device_id.strip() if payload.device_id else str(uuid.uuid4())
    timezone_name = await _resolve_timezone_for_user(
        user_id=device_id,
        provided_timezone=payload.timezone,
    )
    entry_context = payload.entry_context or EntryContext()
    session_id = payload.session_id or _daily_session_id(device_id, timezone_name)

    await repository.ensure_user(user_id=device_id, timezone_name=timezone_name)
    user_profile = await repository.get_user_profile(user_id=device_id)
    profile_context = _build_profile_context(user_profile)
    needs_onboarding = _needs_onboarding(user_profile)
    await _ensure_session(
        user_id=device_id,
        session_id=session_id,
        timezone_name=timezone_name,
        entry_context=entry_context,
        state_patch={
            "thread_type": "daily" if session_id.endswith(_local_date_key(timezone_name)) else "manual",
            "profile_context": profile_context,
            "lifecycle_state": "needs_onboarding" if needs_onboarding else "active",
        },
    )

    messages = await repository.list_messages(session_id=session_id)
    threads = await _thread_summaries(user_id=device_id)
    return {
        "device_id": device_id,
        "user_id": device_id,
        "timezone": timezone_name,
        "session_id": session_id,
        "threads": threads,
        "messages": [message.model_dump() for message in messages],
        "needs_onboarding": needs_onboarding,
        "profile_context": profile_context,
    }


@app.post("/agent/push-token")
async def register_push_token(payload: PushTokenRequest) -> Dict[str, Any]:
    if not TASK_MGMT_V1_ENABLED:
        raise HTTPException(status_code=404, detail="Task management v1 disabled")

    device_id = payload.device_id.strip()
    token = payload.expo_push_token.strip()
    if not device_id or not token:
        raise HTTPException(status_code=400, detail="device_id and expo_push_token are required")

    timezone_name = await _resolve_timezone_for_user(
        user_id=device_id,
        provided_timezone=payload.timezone,
    )
    await repository.ensure_user(user_id=device_id, timezone_name=timezone_name)
    await repository.upsert_push_token(user_id=device_id, expo_push_token=token)
    return {"status": "ok", "device_id": device_id, "timezone": timezone_name}


@app.post("/agent/onboarding/complete")
async def complete_onboarding(payload: OnboardingCompleteRequest) -> Dict[str, Any]:
    device_id = payload.device_id.strip()
    timezone_name = await _resolve_timezone_for_user(
        user_id=device_id,
        provided_timezone=payload.timezone,
    )
    wake_time = payload.wake_time.strip()
    bedtime = payload.bedtime.strip()
    playbook_text = payload.playbook.strip()
    health_anchors = _normalize_text_list(payload.health_anchors)

    if not device_id:
        raise HTTPException(status_code=400, detail="device_id is required")
    if not _is_valid_hhmm(wake_time):
        raise HTTPException(status_code=400, detail="wake_time must be HH:MM (24h)")
    if not _is_valid_hhmm(bedtime):
        raise HTTPException(status_code=400, detail="bedtime must be HH:MM (24h)")
    if len(playbook_text) < 3:
        raise HTTPException(status_code=400, detail="playbook must have at least 3 characters")
    if not health_anchors:
        health_anchors = _default_health_anchors(wake_time, bedtime)

    playbook = {"notes": playbook_text}
    completed_at = _iso_now()

    await repository.ensure_user(user_id=device_id, timezone_name=timezone_name)
    user_profile = await repository.upsert_user_profile(
        user_id=device_id,
        timezone_name=timezone_name,
        wake_time=wake_time,
        bedtime=bedtime,
        playbook=playbook,
        health_anchors=health_anchors,
        onboarding_status="completed",
        onboarding_completed_at=completed_at,
    )

    profile_context = _build_profile_context(user_profile)
    daily_session_id = _daily_session_id(device_id, timezone_name)
    await _ensure_session(
        user_id=device_id,
        session_id=daily_session_id,
        timezone_name=timezone_name,
        entry_context=EntryContext(source="manual", entry_mode="reactive"),
        state_patch={
            "thread_type": "daily",
            "profile_context": profile_context,
            "lifecycle_state": "active",
        },
    )

    return {
        "status": "ok",
        "device_id": device_id,
        "timezone": timezone_name,
        "needs_onboarding": False,
        "profile_context": profile_context,
    }


@app.post("/agent/task-management")
async def task_management(payload: TaskManagementRequest) -> Dict[str, Any]:
    if not TASK_MGMT_V1_ENABLED:
        raise HTTPException(status_code=404, detail="Task management v1 disabled")

    device_id = payload.device_id.strip()
    if not device_id:
        raise HTTPException(status_code=400, detail="device_id is required")

    timezone_name = await _resolve_timezone_for_user(
        user_id=device_id,
        provided_timezone=payload.timezone,
    )
    try:
        return await _execute_task_management(
            device_id=device_id,
            timezone_name=timezone_name,
            action=payload.action,
            payload=payload.payload,
            session_id=payload.session_id,
        )
    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Task management failed: {error}") from error


@app.get("/agent/threads")
async def list_threads(device_id: str = Query(...)) -> Dict[str, Any]:
    threads = await _thread_summaries(user_id=device_id)
    return {"threads": threads}


@app.get("/agent/threads/{session_id}/messages")
async def get_thread_messages(session_id: str, device_id: str = Query(...)) -> Dict[str, Any]:
    session = await repository.get_session(session_id=session_id, user_id=device_id)
    if not session:
        raise HTTPException(status_code=404, detail="Thread not found")
    messages = await repository.list_messages(session_id=session_id)
    return {"messages": [message.model_dump() for message in messages]}


@app.post("/agent/threads")
async def create_thread(payload: ThreadCreateRequest) -> Dict[str, Any]:
    timezone_name = await _resolve_timezone_for_user(
        user_id=payload.device_id,
        provided_timezone=payload.timezone,
    )
    suffix = uuid.uuid4().hex[:8]
    date_key = _local_date_key(timezone_name)
    session_id = f"session_{payload.device_id}_{date_key}_{suffix}"

    await repository.ensure_user(user_id=payload.device_id, timezone_name=timezone_name)
    user_profile = await repository.get_user_profile(user_id=payload.device_id)
    profile_context = _build_profile_context(user_profile)
    entry_context = EntryContext(source="manual", entry_mode="reactive")
    state_patch = {
        "thread_type": "manual",
        "title": payload.title.strip() if payload.title else f"Thread {suffix}",
        "profile_context": profile_context,
        "lifecycle_state": "needs_onboarding" if _needs_onboarding(user_profile) else "active",
    }
    created = await _ensure_session(
        user_id=payload.device_id,
        session_id=session_id,
        timezone_name=timezone_name,
        entry_context=entry_context,
        state_patch=state_patch,
    )
    return {
        "thread": {
            "session_id": created.session_id,
            "date": created.date,
            "title": _session_title(created.session_id, created.date, created.state),
            "updated_at": created.updated_at,
            "state": created.state,
        }
    }


@app.websocket("/agent/ws")
async def agent_ws(
    websocket: WebSocket,
    device_id: str = Query(...),
    session_id: str = Query(...),
    timezone: str | None = Query(None),
    entry_mode: str = Query("reactive"),
) -> None:
    await websocket.accept()

    try:
        timezone_name = await _resolve_timezone_for_user(
            user_id=device_id,
            provided_timezone=timezone,
        )
    except HTTPException as error:
        detail = error.detail if isinstance(error.detail, str) else "Invalid timezone"
        await websocket.send_json({"type": "error", "code": "invalid_timezone", "detail": detail})
        await websocket.close()
        return
    active_user_id = device_id
    active_session_id = session_id
    initialized = False
    latest_entry_context = EntryContext(entry_mode="proactive" if entry_mode == "proactive" else "reactive")

    logger.info(
        "WebSocket connected device_id=%s session_id=%s timezone=%s",
        active_user_id,
        active_session_id,
        timezone_name,
    )

    def _startup_prompt(entry_context: EntryContext) -> str:
        trigger_type = (entry_context.trigger_type or "").strip().lower()
        if trigger_type == "post_onboarding":
            return (
                "Conversation bootstrap: first post-onboarding handoff. "
                "Generate the first assistant message naturally using context. "
                "Keep it brief, supportive, and action-oriented."
            )
        if entry_context.entry_mode == "proactive":
            return (
                "Conversation bootstrap: proactive check-in open. "
                "Generate the first assistant message naturally using trigger context. "
                "Keep it brief and guide the next concrete step."
            )
        return (
            "Conversation bootstrap: reactive open. "
            "Generate the first assistant message naturally using context. "
            "Keep it brief and focused."
        )

    async def _build_runtime_context() -> Dict[str, str]:
        session_record = await repository.get_session(
            session_id=active_session_id,
            user_id=active_user_id,
        )
        session_state = dict(session_record.state) if session_record else {}
        profile_context = session_state.get("profile_context")
        if not isinstance(profile_context, dict):
            user_profile = await repository.get_user_profile(user_id=active_user_id)
            profile_context = _build_profile_context(user_profile)
            session_state["profile_context"] = profile_context
        task_state = _coerce_task_state(session_state, timezone_name)

        context_map = {
            "session_id": active_session_id,
            "user_id": active_user_id,
            "timezone": timezone_name,
            "entry_mode": latest_entry_context.entry_mode,
            "trigger_type": latest_entry_context.trigger_type or "",
            "entry_context": json.dumps(latest_entry_context.model_dump(), separators=(",", ":")),
            "profile_context": json.dumps(profile_context, separators=(",", ":")),
        }

        should_run_due_diligence = (
            latest_entry_context.entry_mode == "proactive"
            and (latest_entry_context.trigger_type or "") != "post_onboarding"
            and TASK_MGMT_V1_ENABLED
        )
        if should_run_due_diligence:
            due_time = _get_current_time_context(timezone_name)
            due_schedule = _build_schedule_response(
                task_state=task_state,
                timezone_name=timezone_name,
                raw_date="today",
            )
            context_map["due_diligence_time"] = json.dumps(due_time, separators=(",", ":"))
            context_map["due_diligence_schedule"] = json.dumps(due_schedule, separators=(",", ":"))
        return context_map

    async def _run_assistant_turn(
        *,
        prompt: str,
        metadata: Dict[str, Any] | None = None,
    ) -> bool:
        assistant_message_id = str(uuid.uuid4())
        context_map = await _build_runtime_context()

        cumulative = ""
        try:
            async for delta in agent.run_stream(
                prompt=prompt,
                user_id=active_user_id,
                session_id=active_session_id,
                context=context_map,
            ):
                if not delta:
                    continue
                cumulative += delta
                await websocket.send_json(
                    {
                        "type": "assistant_delta",
                        "message_id": assistant_message_id,
                        "delta": delta,
                        "text": cumulative,
                    }
                )
        except Exception as error:
            model_name = _active_adk_model_name()
            await websocket.send_json(
                {
                    "type": "error",
                    "code": "adk_error",
                    "detail": f"Google ADK unavailable (model={model_name}): {error}",
                }
            )
            return False

        assistant_text = cumulative.strip() or "I could not generate a response right now."
        assistant_metadata: Dict[str, Any] = {"model": "google-adk", "streamed": True}
        if metadata:
            assistant_metadata.update(metadata)

        assistant_message = MessageRecord(
            id=assistant_message_id,
            session_id=active_session_id,
            user_id=active_user_id,
            role="assistant",
            content=assistant_text,
            metadata=assistant_metadata,
            created_at=_iso_now(),
        )
        await repository.insert_message(payload=assistant_message)

        await _ensure_session(
            user_id=active_user_id,
            session_id=active_session_id,
            timezone_name=timezone_name,
            entry_context=latest_entry_context,
            state_patch={"last_message_id": assistant_message_id, "last_role": "assistant"},
        )

        await websocket.send_json(
            {
                "type": "assistant_done",
                "message_id": assistant_message_id,
                "text": assistant_text,
            }
        )
        return True

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                frame = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_json({"type": "error", "code": "invalid_json", "detail": "Invalid JSON frame"})
                continue

            frame_type = frame.get("type")
            if frame_type == "ping":
                await websocket.send_json({"type": "pong", "ts": _iso_now()})
                continue

            if frame_type == "init":
                active_user_id = str(frame.get("device_id") or active_user_id)
                active_session_id = str(frame.get("session_id") or active_session_id)
                entry_payload = frame.get("entry_context") or {}
                try:
                    latest_entry_context = EntryContext(**entry_payload)
                except Exception:
                    latest_entry_context = EntryContext()

                # post_onboarding is a one-time lifetime handoff.
                # If the user already has any past conversation, downgrade to reactive.
                is_post_onboarding = (latest_entry_context.trigger_type or "").strip().lower() == "post_onboarding"
                if is_post_onboarding and await repository.has_conversation_history(user_id=active_user_id):
                    latest_entry_context = latest_entry_context.model_copy(
                        update={"entry_mode": "reactive", "trigger_type": None}
                    )
                    logger.info(
                        "Ignoring repeated post_onboarding for device_id=%s session_id=%s",
                        active_user_id,
                        active_session_id,
                    )

                await repository.ensure_user(user_id=active_user_id, timezone_name=timezone_name)
                user_profile = await repository.get_user_profile(user_id=active_user_id)
                profile_context = _build_profile_context(user_profile)
                try:
                    await _ensure_session(
                        user_id=active_user_id,
                        session_id=active_session_id,
                        timezone_name=timezone_name,
                        entry_context=latest_entry_context,
                        state_patch={"profile_context": profile_context},
                    )
                except HTTPException as error:
                    detail = error.detail if isinstance(error.detail, str) else "Session init failed"
                    code = "session_forbidden" if error.status_code == 403 else "session_init_failed"
                    await websocket.send_json({"type": "error", "code": code, "detail": detail})
                    continue
                history = await repository.list_messages(session_id=active_session_id)
                await websocket.send_json(
                    {
                        "type": "session_ready",
                        "session_id": active_session_id,
                        "messages": [message.model_dump() for message in history],
                    }
                )
                initialized = True

                startup_needed = not any(message.role in {"user", "assistant"} for message in history)
                if startup_needed:
                    await _run_assistant_turn(
                        prompt=_startup_prompt(latest_entry_context),
                        metadata={
                            "startup_turn": True,
                            "entry_context": latest_entry_context.model_dump(),
                        },
                    )
                continue

            if frame_type != "user_message":
                await websocket.send_json({"type": "error", "code": "unknown_frame", "detail": "Unknown frame type"})
                continue

            if not initialized:
                await websocket.send_json({"type": "error", "code": "not_initialized", "detail": "Send init first"})
                continue

            message_id = str(frame.get("message_id") or "")
            text = str(frame.get("text") or "").strip()
            if not message_id or not text:
                await websocket.send_json(
                    {
                        "type": "error",
                        "code": "invalid_user_message",
                        "detail": "message_id and text are required",
                    }
                )
                continue

            if await repository.message_exists(session_id=active_session_id, message_id=message_id):
                await websocket.send_json(
                    {
                        "type": "error",
                        "code": "duplicate_message",
                        "detail": f"message_id {message_id} already exists",
                    }
                )
                continue

            user_message = MessageRecord(
                id=message_id,
                session_id=active_session_id,
                user_id=active_user_id,
                role="user",
                content=text,
                metadata={"entry_context": latest_entry_context.model_dump()},
                created_at=_iso_now(),
            )
            await repository.insert_message(payload=user_message)

            if not await _run_assistant_turn(prompt=text):
                continue

    except WebSocketDisconnect:
        logger.info(
            "WebSocket disconnected device_id=%s session_id=%s",
            active_user_id,
            active_session_id,
        )
    except Exception as error:
        logger.exception(
            "WebSocket failure device_id=%s session_id=%s error=%s",
            active_user_id,
            active_session_id,
            error,
        )
        try:
            await websocket.send_json({"type": "error", "code": "server_error", "detail": str(error)})
        except Exception:
            pass
