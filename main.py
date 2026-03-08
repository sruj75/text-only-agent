import json
import logging
import os
import re
import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from startup_agent.adk import SimpleADK


_LOG_RESERVED_FIELDS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "message",
    "module",
    "msecs",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
}


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "severity": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "time": _iso_from_dt(datetime.fromtimestamp(record.created, timezone.utc)),
        }
        for key, value in record.__dict__.items():
            if key in _LOG_RESERVED_FIELDS or key.startswith("_"):
                continue
            if isinstance(value, (str, int, float, bool)) or value is None:
                payload[key] = value
            elif isinstance(value, (dict, list, tuple)):
                payload[key] = value
            else:
                payload[key] = str(value)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=True)


def _configure_logging() -> None:
    if getattr(_configure_logging, "_configured", False):
        return
    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    _configure_logging._configured = True  # type: ignore[attr-defined]


def _log_event(level: int, message: str, **fields: Any) -> None:
    logger.log(level, message, extra=fields)


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _runtime_env() -> str:
    raw = os.getenv("APP_ENV") or os.getenv("ENVIRONMENT") or "production"
    return raw.strip().lower()


def _is_cloud_run_runtime() -> bool:
    return bool(os.getenv("K_SERVICE"))


def _strict_startup_validation_enabled() -> bool:
    default = _is_cloud_run_runtime()
    return _env_flag("STRICT_STARTUP_VALIDATION", default)


def _required_runtime_env_vars() -> List[str]:
    return ["GOOGLE_API_KEY", "SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY"]


def _runtime_config_snapshot() -> Dict[str, Any]:
    missing = [name for name in _required_runtime_env_vars() if not os.getenv(name)]
    return {
        "runtime_env": _runtime_env(),
        "cloud_run_service": os.getenv("K_SERVICE", ""),
        "strict_startup_validation": _strict_startup_validation_enabled(),
        "missing_required_env_vars": missing,
    }


def _validate_runtime_configuration() -> Dict[str, Any]:
    config = _runtime_config_snapshot()
    missing = config["missing_required_env_vars"]
    if missing and config["strict_startup_validation"]:
        raise RuntimeError(
            "Missing required environment variables for production runtime: "
            + ", ".join(missing)
        )
    if missing:
        _log_event(
            logging.WARNING,
            "Runtime configuration incomplete; allowing local fallback",
            **config,
        )
    else:
        _log_event(logging.INFO, "Runtime configuration validated", **config)
    return config


_configure_logging()
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
TaskEventType = Literal[
    "created",
    "status_updated",
    "timebox_updated",
    "top_essential_updated",
    "migrated_from_session_state",
]
TaskPanelListener = Callable[[Dict[str, Any]], Awaitable[bool]]
TASK_INTENT_PATTERN = re.compile(
    r"\b(task|tasks|todo|to-do|priority|priorities|timebox|schedule|plan day|mark done|blocked)\b",
    re.IGNORECASE,
)
TASK_CLAIM_PATTERN = re.compile(
    r"\b(added|captured|saved|scheduled|timeboxed|prioritized|marked)\b",
    re.IGNORECASE,
)
PROACTIVE_ACK_STATE_KEY = "proactive_ack_event_ids_v1"
MISSED_REPORTED_STATE_KEY = "missed_reported_event_ids_v1"
MISSED_PROACTIVE_EVENT_TYPES = {"morning_wake", "checkin", "calendar_reminder"}
MISSED_PROACTIVE_MAX_EVENTS = 5


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


def _session_thread_type(
    *,
    user_id: str,
    session_id: str,
    timezone_name: str,
    entry_context: "EntryContext",
    state: Dict[str, Any] | None = None,
) -> str:
    existing = state or {}
    configured = existing.get("thread_type")
    if isinstance(configured, str) and configured.strip():
        return configured.strip()
    if session_id == _daily_session_id(user_id, timezone_name):
        return "daily"
    if entry_context.entry_mode == "proactive":
        return "daily"
    return "manual"


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


def _normalize_event_id_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    output: List[str] = []
    seen: set[str] = set()
    for item in value:
        event_id = str(item or "").strip()
        if not event_id or event_id in seen:
            continue
        seen.add(event_id)
        output.append(event_id)
    return output


def _append_event_id(existing: List[str], event_id: str) -> List[str]:
    value = event_id.strip()
    if not value:
        return existing
    if value in existing:
        return existing
    return [*existing, value]


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


class TaskRecord(BaseModel):
    task_id: str
    user_id: str
    title: str
    priority_rank: int | None = None
    is_essential: bool = False
    status: TaskStatus = "todo"
    timebox_start_at: str | None = None
    timebox_end_at: str | None = None
    created_at: str = Field(default_factory=_iso_now)
    updated_at: str = Field(default_factory=_iso_now)


class TaskEventRecord(BaseModel):
    id: str
    task_id: str
    user_id: str
    event_type: TaskEventType
    payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=_iso_now)


def _task_timebox_from_record(task: TaskRecord) -> TaskTimebox | None:
    if not task.timebox_start_at or not task.timebox_end_at:
        return None
    return TaskTimebox(start_at=task.timebox_start_at, end_at=task.timebox_end_at)


def _task_item_from_record(task: TaskRecord) -> TaskItem:
    return TaskItem(
        task_id=task.task_id,
        title=task.title,
        priority_rank=task.priority_rank,
        is_essential=task.is_essential,
        status=task.status,
        created_at=task.created_at,
        timebox=_task_timebox_from_record(task),
    )


def _task_state_from_records(
    tasks: List[TaskRecord],
    *,
    timezone_name: str,
    date_key: str | None = None,
) -> TaskStateV1:
    latest_update = max((task.updated_at for task in tasks), default=_iso_now())
    return TaskStateV1(
        date=date_key or _local_date_key(timezone_name),
        timezone=timezone_name,
        tasks=[_task_item_from_record(task) for task in tasks],
        updated_at=latest_update,
    )


def _task_is_open(task: TaskRecord) -> bool:
    return task.status != "done"


def _task_sort_key(task: TaskRecord) -> tuple[int, int, int, str, str]:
    done_rank = 1 if task.status == "done" else 0
    essential_rank = task.priority_rank if task.is_essential and task.priority_rank is not None else 999
    timebox_rank = 0 if task.timebox_start_at else 1
    timebox_value = task.timebox_start_at or ""
    return (done_rank, essential_rank, timebox_rank, timebox_value, task.created_at)


def _day_window_bounds(raw_date: str | None, timezone_name: str) -> tuple[str, str, str]:
    target_date = _target_date_key(raw_date, timezone_name)
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", target_date):
        raise HTTPException(status_code=400, detail="date must be 'today' or YYYY-MM-DD")
    try:
        start_local = datetime.strptime(target_date, "%Y-%m-%d").replace(
            tzinfo=ZoneInfo(timezone_name)
        )
    except ValueError as error:
        raise HTTPException(
            status_code=400,
            detail="date must be 'today' or YYYY-MM-DD",
        ) from error
    end_local = start_local + timedelta(days=1)
    return (
        target_date,
        _iso_from_dt(start_local.astimezone(timezone.utc)),
        _iso_from_dt(end_local.astimezone(timezone.utc)),
    )


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
        self.tasks: Dict[str, TaskRecord] = {}
        self.task_events: Dict[str, TaskEventRecord] = {}
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

    async def list_tasks(self, user_id: str) -> List[TaskRecord]:
        rows = [task for task in self.tasks.values() if task.user_id == user_id]
        rows.sort(key=_task_sort_key)
        return [task.model_copy() for task in rows]

    async def get_task(self, task_id: str, user_id: str) -> TaskRecord | None:
        task = self.tasks.get(task_id)
        if not task or task.user_id != user_id:
            return None
        return task.model_copy()

    async def create_tasks(self, user_id: str, titles: List[str]) -> List[TaskRecord]:
        created: List[TaskRecord] = []
        for title in titles:
            now = _iso_now()
            task = TaskRecord(
                task_id=str(uuid.uuid4()),
                user_id=user_id,
                title=title,
                created_at=now,
                updated_at=now,
            )
            self.tasks[task.task_id] = task
            created.append(task.model_copy())
        return created

    async def update_task_status(self, task_id: str, user_id: str, status: TaskStatus) -> TaskRecord | None:
        task = self.tasks.get(task_id)
        if not task or task.user_id != user_id:
            return None
        updated = task.model_copy(update={"status": status, "updated_at": _iso_now()})
        self.tasks[task_id] = updated
        return updated.model_copy()

    async def set_task_timebox(
        self,
        task_id: str,
        user_id: str,
        start_at: str,
        end_at: str,
    ) -> TaskRecord | None:
        task = self.tasks.get(task_id)
        if not task or task.user_id != user_id:
            return None
        updated = task.model_copy(
            update={
                "timebox_start_at": start_at,
                "timebox_end_at": end_at,
                "updated_at": _iso_now(),
            }
        )
        self.tasks[task_id] = updated
        return updated.model_copy()

    async def replace_top_essentials(self, user_id: str, task_ids: List[str]) -> List[TaskRecord]:
        updated: List[TaskRecord] = []
        selected = {task_id: index for index, task_id in enumerate(task_ids, start=1)}
        for task_id, task in list(self.tasks.items()):
            if task.user_id != user_id:
                continue
            next_rank = selected.get(task_id)
            next_task = task.model_copy(
                update={
                    "priority_rank": next_rank,
                    "is_essential": next_rank is not None,
                    "updated_at": _iso_now(),
                }
            )
            self.tasks[task_id] = next_task
            updated.append(next_task.model_copy())
        updated.sort(key=_task_sort_key)
        return updated

    async def list_tasks_in_window(
        self,
        user_id: str,
        start_at_utc: str,
        end_at_utc: str,
    ) -> List[TaskRecord]:
        start_dt = _parse_iso(start_at_utc)
        end_dt = _parse_iso(end_at_utc)
        rows: List[TaskRecord] = []
        for task in self.tasks.values():
            if task.user_id != user_id or not task.timebox_start_at:
                continue
            start_value = _parse_iso(task.timebox_start_at)
            if start_value < start_dt or start_value >= end_dt:
                continue
            rows.append(task.model_copy())
        rows.sort(key=_task_sort_key)
        return rows

    async def append_task_event(self, payload: TaskEventRecord) -> TaskEventRecord:
        self.task_events[payload.id] = payload
        return payload.model_copy()

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

    async def list_future_events(
        self,
        *,
        user_id: str,
        from_time_iso: str,
        event_type: str | None = None,
    ) -> List[CheckinEventRecord]:
        from_time = _parse_iso(from_time_iso)
        rows: List[CheckinEventRecord] = []
        for row in self.events.values():
            if row.user_id != user_id or row.executed:
                continue
            if event_type and row.event_type != event_type:
                continue
            if _parse_iso(row.scheduled_time) < from_time:
                continue
            rows.append(row)
        rows.sort(key=lambda row: row.scheduled_time)
        return rows

    async def list_events_in_window(
        self,
        *,
        user_id: str,
        start_time_iso: str,
        end_time_iso: str,
        executed: bool | None = None,
        event_types: List[str] | None = None,
    ) -> List[CheckinEventRecord]:
        start_time = _parse_iso(start_time_iso)
        end_time = _parse_iso(end_time_iso)
        allowed_types = set(event_types or [])
        rows: List[CheckinEventRecord] = []
        for row in self.events.values():
            if row.user_id != user_id:
                continue
            row_time = _parse_iso(row.scheduled_time)
            if row_time < start_time or row_time >= end_time:
                continue
            if executed is not None and bool(row.executed) is not executed:
                continue
            if allowed_types and row.event_type not in allowed_types:
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

    async def _list_task_rows(
        self,
        *,
        user_id: str,
        extra_params: Dict[str, str] | None = None,
    ) -> List[Dict[str, Any]]:
        page_size = 500
        offset = 0
        rows: List[Dict[str, Any]] = []

        while True:
            params = {
                "user_id": f"eq.{user_id}",
                "select": (
                    "task_id,user_id,title,status,priority_rank,is_essential,"
                    "timebox_start_at,timebox_end_at,created_at,updated_at"
                ),
                "order": "created_at.asc,task_id.asc",
                "limit": str(page_size),
                "offset": str(offset),
            }
            if extra_params:
                params.update(extra_params)

            page = await self._request(
                "GET",
                "tasks",
                params=params,
            )
            page_rows = page or []
            rows.extend(page_rows)
            if len(page_rows) < page_size:
                break
            offset += page_size

        return rows

    async def list_tasks(self, user_id: str) -> List[TaskRecord]:
        rows = await self._list_task_rows(user_id=user_id)
        tasks = [TaskRecord(**row) for row in rows or []]
        tasks.sort(key=_task_sort_key)
        return tasks

    async def get_task(self, task_id: str, user_id: str) -> TaskRecord | None:
        rows = await self._request(
            "GET",
            "tasks",
            params={
                "task_id": f"eq.{task_id}",
                "user_id": f"eq.{user_id}",
                "select": (
                    "task_id,user_id,title,status,priority_rank,is_essential,"
                    "timebox_start_at,timebox_end_at,created_at,updated_at"
                ),
                "limit": "1",
            },
        )
        if not rows:
            return None
        return TaskRecord(**rows[0])

    async def create_tasks(self, user_id: str, titles: List[str]) -> List[TaskRecord]:
        now = _iso_now()
        payload = [
            {
                "task_id": str(uuid.uuid4()),
                "user_id": user_id,
                "title": title,
                "created_at": now,
                "updated_at": now,
            }
            for title in titles
        ]
        rows = await self._request(
            "POST",
            "tasks",
            json_body=payload,
            extra_headers={"Prefer": "return=representation"},
        )
        return [TaskRecord(**row) for row in rows or []]

    async def update_task_status(
        self,
        task_id: str,
        user_id: str,
        status: TaskStatus,
    ) -> TaskRecord | None:
        rows = await self._request(
            "PATCH",
            "tasks",
            params={"task_id": f"eq.{task_id}", "user_id": f"eq.{user_id}"},
            json_body={"status": status, "updated_at": _iso_now()},
            extra_headers={"Prefer": "return=representation"},
        )
        if not rows:
            return None
        return TaskRecord(**rows[0])

    async def set_task_timebox(
        self,
        task_id: str,
        user_id: str,
        start_at: str,
        end_at: str,
    ) -> TaskRecord | None:
        rows = await self._request(
            "PATCH",
            "tasks",
            params={"task_id": f"eq.{task_id}", "user_id": f"eq.{user_id}"},
            json_body={
                "timebox_start_at": start_at,
                "timebox_end_at": end_at,
                "updated_at": _iso_now(),
            },
            extra_headers={"Prefer": "return=representation"},
        )
        if not rows:
            return None
        return TaskRecord(**rows[0])

    async def replace_top_essentials(self, user_id: str, task_ids: List[str]) -> List[TaskRecord]:
        tasks = await self.list_tasks(user_id=user_id)
        ordered_lookup = {task_id: index for index, task_id in enumerate(task_ids, start=1)}
        updated_rows: List[TaskRecord] = []
        for task in tasks:
            next_rank = ordered_lookup.get(task.task_id)
            rows = await self._request(
                "PATCH",
                "tasks",
                params={"task_id": f"eq.{task.task_id}", "user_id": f"eq.{user_id}"},
                json_body={
                    "priority_rank": next_rank,
                    "is_essential": next_rank is not None,
                    "updated_at": _iso_now(),
                },
                extra_headers={"Prefer": "return=representation"},
            )
            if rows:
                updated_rows.append(TaskRecord(**rows[0]))
        updated_rows.sort(key=_task_sort_key)
        return updated_rows

    async def list_tasks_in_window(
        self,
        user_id: str,
        start_at_utc: str,
        end_at_utc: str,
    ) -> List[TaskRecord]:
        rows = await self._list_task_rows(
            user_id=user_id,
            extra_params={
                "timebox_start_at": "not.is.null",
                "and": f"(timebox_start_at.gte.{start_at_utc},timebox_start_at.lt.{end_at_utc})",
            },
        )
        tasks = [TaskRecord(**row) for row in rows or []]
        tasks.sort(key=_task_sort_key)
        return tasks

    async def append_task_event(self, payload: TaskEventRecord) -> TaskEventRecord:
        rows = await self._request(
            "POST",
            "task_events",
            json_body=[payload.model_dump()],
            extra_headers={"Prefer": "return=representation"},
        )
        if not rows:
            raise RuntimeError("Task event insert returned empty response")
        return TaskEventRecord(**rows[0])

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

    async def list_future_events(
        self,
        *,
        user_id: str,
        from_time_iso: str,
        event_type: str | None = None,
    ) -> List[CheckinEventRecord]:
        params: Dict[str, str] = {
            "user_id": f"eq.{user_id}",
            "executed": "eq.false",
            "scheduled_time": f"gte.{from_time_iso}",
            "select": "id,user_id,scheduled_time,event_type,payload,executed,cron_job_id,last_error,attempt_count,created_at,updated_at",
            "order": "scheduled_time.asc",
            "limit": "500",
        }
        if event_type:
            params["event_type"] = f"eq.{event_type}"
        rows = await self._request(
            "GET",
            "events",
            params=params,
        )
        return [CheckinEventRecord(**row) for row in rows or []]

    async def list_events_in_window(
        self,
        *,
        user_id: str,
        start_time_iso: str,
        end_time_iso: str,
        executed: bool | None = None,
        event_types: List[str] | None = None,
    ) -> List[CheckinEventRecord]:
        params: Dict[str, str] = {
            "user_id": f"eq.{user_id}",
            "scheduled_time": f"gte.{start_time_iso}",
            "and": f"(scheduled_time.lt.{end_time_iso})",
            "select": "id,user_id,scheduled_time,event_type,payload,executed,cron_job_id,last_error,attempt_count,created_at,updated_at",
            "order": "scheduled_time.asc",
            "limit": "500",
        }
        if executed is not None:
            params["executed"] = f"eq.{str(executed).lower()}"
        if event_types:
            params["event_type"] = f"in.({','.join(event_types)})"
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
    runtime_config = _validate_runtime_configuration()
    supabase_url = os.getenv("SUPABASE_URL")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if supabase_url and service_role_key:
        _log_event(
            logging.INFO,
            "Using Supabase repository for chat persistence",
            repository_mode="supabase",
            runtime_env=runtime_config["runtime_env"],
            cloud_run_service=runtime_config["cloud_run_service"],
        )
        return SupabaseRepository(project_url=supabase_url, service_role_key=service_role_key)
    _log_event(
        logging.WARNING,
        "Using in-memory repository fallback",
        repository_mode="in_memory",
        runtime_env=runtime_config["runtime_env"],
    )
    return InMemoryRepository()


repository = build_repository()
task_panel_subscribers: Dict[tuple[str, str], List[TaskPanelListener]] = {}


def _subscribe_task_panel(
    user_id: str,
    session_id: str,
    listener: TaskPanelListener,
) -> Any:
    key = (user_id, session_id)
    bucket = task_panel_subscribers.setdefault(key, [])
    bucket.append(listener)

    def _unsubscribe() -> None:
        listeners = task_panel_subscribers.get(key)
        if not listeners:
            return
        try:
            listeners.remove(listener)
        except ValueError:
            return
        if not listeners:
            task_panel_subscribers.pop(key, None)

    return _unsubscribe


async def _broadcast_task_panel(
    user_id: str,
    session_id: str,
    snapshot: Dict[str, Any],
) -> None:
    listeners = list(task_panel_subscribers.get((user_id, session_id), []))
    stale: List[TaskPanelListener] = []

    for listener in listeners:
        try:
            delivered = await listener(snapshot)
        except Exception:
            delivered = False
        if not delivered:
            stale.append(listener)

    if stale:
        current = task_panel_subscribers.get((user_id, session_id), [])
        task_panel_subscribers[(user_id, session_id)] = [
            listener for listener in current if listener not in stale
        ]
        if not task_panel_subscribers[(user_id, session_id)]:
            task_panel_subscribers.pop((user_id, session_id), None)


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
    if not TASK_MGMT_V1_ENABLED:
        return {"ok": False, "error": "task_management_v1_disabled"}

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

    target_session_id = raw_session_id or _daily_session_id(raw_user_id, timezone_name)

    try:
        session_record = await _ensure_task_session(
            user_id=raw_user_id,
            session_id=target_session_id,
            timezone_name=timezone_name,
        )
        task_state = await _load_task_state_for_user(
            user_id=raw_user_id,
            timezone_name=timezone_name,
        )
        target_session_id = session_record.session_id
        await _broadcast_task_panel(
            raw_user_id,
            target_session_id,
            _build_task_panel_state(
                task_state=task_state,
                timezone_name=timezone_name,
                run_status="running",
                action=action,
                payload=payload,
            ),
        )

        output = await _execute_task_management(
            device_id=raw_user_id,
            timezone_name=timezone_name,
            action=action,  # runtime-validated in _execute_task_management
            payload=payload,
            session_id=target_session_id,
        )
        await _broadcast_task_panel(
            raw_user_id,
            output["session_id"],
            _build_task_panel_state(
                task_state=TaskStateV1(**output["task_state"]),
                timezone_name=timezone_name,
                run_status="complete",
                action=action,
                payload=payload,
                result=output.get("result"),
            ),
        )
        return {"ok": True, **output}
    except HTTPException as error:
        _log_event(
            logging.WARNING,
            "Task tool action rejected",
            device_id=raw_user_id,
            session_id=target_session_id,
            timezone=timezone_name,
            action=action,
            detail=str(error.detail),
        )
        if target_session_id:
            error_state = await _load_task_state_for_user(
                user_id=raw_user_id,
                timezone_name=timezone_name,
            )
            await _broadcast_task_panel(
                raw_user_id,
                target_session_id,
                _build_task_panel_state(
                    task_state=error_state,
                    timezone_name=timezone_name,
                    run_status="error",
                    action=action,
                    payload=payload,
                    error_message=str(error.detail),
                ),
            )
        return {"ok": False, "error": str(error.detail)}
    except Exception as error:
        logger.exception(
            "Task tool action failed",
            extra={
                "device_id": raw_user_id,
                "session_id": target_session_id,
                "timezone": timezone_name,
                "action": action,
                "error": str(error),
            },
        )
        if target_session_id:
            error_state = await _load_task_state_for_user(
                user_id=raw_user_id,
                timezone_name=timezone_name,
            )
            await _broadcast_task_panel(
                raw_user_id,
                target_session_id,
                _build_task_panel_state(
                    task_state=error_state,
                    timezone_name=timezone_name,
                    run_status="error",
                    action=action,
                    payload=payload,
                    error_message=str(error),
                ),
            )
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


async def _validate_scheduler_runtime_dependencies() -> None:
    if not isinstance(repository, SupabaseRepository):
        return

    scheduler_secret_output = await repository._rpc("get_scheduler_secret_for_execution", {})
    scheduler_secret = scheduler_secret_output if isinstance(scheduler_secret_output, str) else ""
    if not scheduler_secret.strip():
        raise RuntimeError("scheduler_secret is missing from Supabase vault")

    # This RPC exists only in the scheduler source-of-truth migration.
    await repository._rpc("ensure_next_morning_wake_event", {"p_event_id": "__healthcheck__"})


@asynccontextmanager
async def lifespan(_: FastAPI):
    startup_config = _runtime_config_snapshot()
    strict_validation = bool(startup_config["strict_startup_validation"])
    if strict_validation:
        await _validate_scheduler_runtime_dependencies()
    elif isinstance(repository, SupabaseRepository):
        try:
            await _validate_scheduler_runtime_dependencies()
        except Exception as error:
            _log_event(
                logging.WARNING,
                "Scheduler runtime validation skipped in non-strict mode",
                error=str(error),
            )
    _log_event(
        logging.INFO,
        "Application startup",
        **startup_config,
        repository_mode="supabase" if isinstance(repository, SupabaseRepository) else "in_memory",
        adk_model=_active_adk_model_name(),
    )
    try:
        yield
    finally:
        _log_event(logging.INFO, "Application shutdown")
        await repository.close()


app = FastAPI(title="Startup Agent API", version="0.3.0", lifespan=lifespan)


@app.middleware("http")
async def log_http_requests(request: Request, call_next):
    started_at = _utc_now()
    try:
        response = await call_next(request)
    except Exception as error:
        duration_ms = int((_utc_now() - started_at).total_seconds() * 1000)
        logger.exception(
            "HTTP request failed",
            extra={
                "method": request.method,
                "path": request.url.path,
                "query": str(request.url.query),
                "duration_ms": duration_ms,
                "error": str(error),
            },
        )
        raise

    duration_ms = int((_utc_now() - started_at).total_seconds() * 1000)
    if request.url.path != "/health":
        _log_event(
            logging.INFO,
            "HTTP request completed",
            method=request.method,
            path=request.url.path,
            query=str(request.url.query),
            status_code=response.status_code,
            duration_ms=duration_ms,
        )
    return response


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


def _next_wake_run_at_utc(*, timezone_name: str, wake_time_hhmm: str) -> datetime:
    if not _is_valid_hhmm(wake_time_hhmm):
        raise HTTPException(status_code=400, detail="wake_time must be HH:MM (24h)")
    wake_hour, wake_minute = [int(part) for part in wake_time_hhmm.split(":")]
    now_local = datetime.now(ZoneInfo(timezone_name))
    target_local = now_local.replace(
        hour=wake_hour,
        minute=wake_minute,
        second=0,
        microsecond=0,
    )
    if target_local <= now_local:
        target_local = target_local + timedelta(days=1)
    return target_local.astimezone(timezone.utc)


async def _seed_next_morning_wake_event(
    *,
    user_id: str,
    timezone_name: str,
    wake_time: str,
) -> Dict[str, Any]:
    existing = await repository.list_future_events(
        user_id=user_id,
        from_time_iso=_iso_now(),
        event_type="morning_wake",
    )
    if existing:
        earliest = existing[0]
        return {
            "seeded": False,
            "reason": "existing_pending_morning_wake",
            "event_id": earliest.id,
            "cron_job_id": earliest.cron_job_id,
            "scheduled_time": earliest.scheduled_time,
        }

    run_at = _next_wake_run_at_utc(timezone_name=timezone_name, wake_time_hhmm=wake_time)
    seed_date = _date_key_for_datetime(run_at, timezone_name)
    event_payload = {
        "schedule_owner": "system",
        "schedule_policy": "morning_bootstrap",
        "seed_date": seed_date,
        "timezone": timezone_name,
        "reason": "daily_bootstrap",
        "trigger_type": "morning_wake",
    }
    event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"morning_wake:{user_id}:{seed_date}"))
    try:
        created_event = await repository.create_event(
            CheckinEventRecord(
                id=event_id,
                user_id=user_id,
                scheduled_time=_iso_from_dt(run_at),
                event_type="morning_wake",
                payload=event_payload,
                executed=False,
                cron_job_id=None,
                attempt_count=0,
                created_at=_iso_now(),
                updated_at=_iso_now(),
            )
        )
    except RuntimeError as error:
        # Supabase conflict means another concurrent request seeded this day already.
        if "Supabase error 409" not in str(error):
            raise
        existing = await repository.list_future_events(
            user_id=user_id,
            from_time_iso=_iso_now(),
            event_type="morning_wake",
        )
        matching = next(
            (
                event
                for event in existing
                if isinstance(event.payload, dict)
                and str(event.payload.get("seed_date") or "").strip() == seed_date
            ),
            existing[0] if existing else None,
        )
        if not matching:
            raise
        return {
            "seeded": False,
            "reason": "existing_pending_morning_wake",
            "event_id": matching.id,
            "cron_job_id": matching.cron_job_id,
            "scheduled_time": matching.scheduled_time,
        }
    try:
        job_id = await repository.schedule_event_job(
            event_id=created_event.id,
            run_at=created_event.scheduled_time,
            timezone_name=timezone_name,
        )
        await repository.update_event_cron_job(created_event.id, int(job_id))
    except Exception:
        await repository.delete_event(created_event.id)
        raise

    return {
        "seeded": True,
        "event_id": created_event.id,
        "cron_job_id": int(job_id),
        "scheduled_time": created_event.scheduled_time,
    }


async def _list_missed_proactive_events_for_today(
    *,
    user_id: str,
    timezone_name: str,
    current_event_id: str | None,
    acknowledged_ids: List[str],
    reported_ids: List[str],
) -> List[Dict[str, Any]]:
    _, start_at_utc, end_at_utc = _day_window_bounds("today", timezone_name)
    events = await repository.list_events_in_window(
        user_id=user_id,
        start_time_iso=start_at_utc,
        end_time_iso=end_at_utc,
        executed=True,
        event_types=sorted(MISSED_PROACTIVE_EVENT_TYPES),
    )
    now = _utc_now()
    acknowledged = set(acknowledged_ids)
    reported = set(reported_ids)
    current_id = str(current_event_id or "").strip()
    missed: List[Dict[str, Any]] = []

    for event in events:
        event_id = str(event.id).strip()
        if not event_id:
            continue
        if event_id == current_id:
            continue
        if event_id in acknowledged or event_id in reported:
            continue
        try:
            scheduled_dt = _parse_iso(event.scheduled_time)
        except Exception:
            continue
        if scheduled_dt >= now:
            continue

        payload = event.payload if isinstance(event.payload, dict) else {}
        local_time = scheduled_dt.astimezone(ZoneInfo(timezone_name))
        trigger_type = str(payload.get("trigger_type") or payload.get("reason") or event.event_type)
        task_title = str(payload.get("task_title") or "").strip() or None

        missed.append(
            {
                "event_id": event_id,
                "event_type": event.event_type,
                "trigger_type": trigger_type,
                "scheduled_time": event.scheduled_time,
                "scheduled_local_time": local_time.strftime("%H:%M"),
                "task_title": task_title,
            }
        )
        if len(missed) >= MISSED_PROACTIVE_MAX_EVENTS:
            break

    return missed


def _looks_like_task_intent(text: str) -> bool:
    return bool(TASK_INTENT_PATTERN.search(text or ""))


def _looks_like_task_claim(text: str) -> bool:
    return bool(TASK_CLAIM_PATTERN.search(text or "")) and bool(
        TASK_INTENT_PATTERN.search(text or "")
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


async def _load_task_state_for_user(
    *,
    user_id: str,
    timezone_name: str,
    date_key: str | None = None,
) -> TaskStateV1:
    if not TASK_MGMT_V1_ENABLED:
        return _task_state_from_records([], timezone_name=timezone_name, date_key=date_key)
    tasks = await repository.list_tasks(user_id=user_id)
    return _task_state_from_records(tasks, timezone_name=timezone_name, date_key=date_key)


async def _build_schedule_for_user(
    *,
    user_id: str,
    timezone_name: str,
    raw_date: str | None,
) -> Dict[str, Any]:
    target_date, start_at_utc, end_at_utc = _day_window_bounds(raw_date, timezone_name)
    tasks = await repository.list_tasks_in_window(
        user_id=user_id,
        start_at_utc=start_at_utc,
        end_at_utc=end_at_utc,
    )
    return _build_schedule_response(
        task_state=_task_state_from_records(tasks, timezone_name=timezone_name, date_key=target_date),
        timezone_name=timezone_name,
        raw_date=target_date,
    )


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


def _task_action_label(action: str | None) -> str | None:
    labels = {
        "capture_tasks": "Capturing tasks",
        "get_tasks": "Refreshing tasks",
        "set_top_essentials": "Updating top essentials",
        "timebox_task": "Updating schedule",
        "get_schedule": "Loading schedule",
        "update_task_status": "Updating task status",
    }
    if not action:
        return None
    return labels.get(action, action.replace("_", " ").strip().title())


def _task_time_label(task: TaskItem, timezone_name: str) -> str | None:
    if not task.timebox:
        return None
    start_dt = _parse_iso(task.timebox.start_at).astimezone(ZoneInfo(timezone_name))
    end_dt = _parse_iso(task.timebox.end_at).astimezone(ZoneInfo(timezone_name))
    return f"{start_dt.strftime('%I:%M %p').lstrip('0')} - {end_dt.strftime('%I:%M %p').lstrip('0')}"


def _active_task_ids_for_action(
    *,
    action: str | None,
    payload: Dict[str, Any],
    result: Dict[str, Any] | None,
) -> set[str]:
    if not action:
        return set()
    if action == "capture_tasks" and isinstance(result, dict):
        created = result.get("created_task_ids")
        if isinstance(created, list):
            return {str(item) for item in created if str(item).strip()}
    if action in {"timebox_task", "update_task_status"}:
        task_id = str(payload.get("task_id") or "").strip()
        return {task_id} if task_id else set()
    if action == "set_top_essentials":
        task_ids = payload.get("task_ids")
        if isinstance(task_ids, list):
            return {str(item) for item in task_ids if str(item).strip()}
    return set()


def _build_task_panel_state(
    *,
    task_state: TaskStateV1,
    timezone_name: str,
    run_status: Literal["idle", "running", "complete", "error"] = "idle",
    action: str | None = None,
    payload: Dict[str, Any] | None = None,
    result: Dict[str, Any] | None = None,
    error_message: str | None = None,
) -> Dict[str, Any]:
    safe_payload = payload or {}
    active_task_ids = _active_task_ids_for_action(
        action=action,
        payload=safe_payload,
        result=result,
    )

    top_essential_tasks = [
        task for task in task_state.tasks if task.is_essential
    ]
    top_essential_tasks.sort(key=lambda task: (task.priority_rank or 999, task.created_at))

    tasks: List[Dict[str, Any]] = []
    for task in task_state.tasks:
        tasks.append(
            {
                "id": task.task_id,
                "title": task.title,
                "status": task.status,
                "time_label": _task_time_label(task, timezone_name),
                "is_active": task.task_id in active_task_ids,
                "is_top_essential": task.is_essential,
            }
        )

    schedule: List[Dict[str, Any]] = []
    for item in _build_schedule_response(
        task_state=task_state,
        timezone_name=timezone_name,
        raw_date=task_state.date or "today",
    )["items"]:
        schedule.append(
            {
                "id": item["task_id"],
                "title": item["title"],
                "start_label": _parse_iso(item["start_at"])
                .astimezone(ZoneInfo(timezone_name))
                .strftime("%I:%M %p")
                .lstrip("0"),
                "end_label": _parse_iso(item["end_at"])
                .astimezone(ZoneInfo(timezone_name))
                .strftime("%I:%M %p")
                .lstrip("0"),
                "task_id": item["task_id"],
                "status": item["status"],
            }
        )

    action_label = _task_action_label(action)
    if error_message:
        headline = error_message
    elif run_status == "running" and action_label:
        headline = f"{action_label}..."
    elif run_status == "complete" and action_label:
        headline = f"{action_label} done."
    else:
        headline = None

    return {
        "run_status": run_status,
        "active_action": action_label,
        "headline": headline,
        "tasks": tasks,
        "top_essentials": [task.title for task in top_essential_tasks],
        "schedule": schedule,
        "updated_at": task_state.updated_at,
        "error_message": error_message,
    }


async def _schedule_task_event(
    *,
    user_id: str,
    timezone_name: str,
    date_key: str,
    trigger_type: TaskTriggerType,
    task: TaskRecord,
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
        "timebox_start": task.timebox_start_at,
        "timebox_end": task.timebox_end_at,
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


async def _ensure_task_session(
    *,
    user_id: str,
    session_id: str,
    timezone_name: str,
) -> SessionRecord:
    existing = await repository.get_session(session_id=session_id, user_id=user_id)
    if existing:
        return existing
    default_entry = EntryContext(source="manual", entry_mode="reactive")
    return await _ensure_session(
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


def _task_date_from_record(task: TaskRecord, timezone_name: str) -> str | None:
    if not task.timebox_start_at:
        return None
    return _date_key_for_datetime(_parse_iso(task.timebox_start_at), timezone_name)


async def _append_task_event(
    *,
    task_id: str,
    user_id: str,
    event_type: TaskEventType,
    payload: Dict[str, Any],
) -> None:
    await repository.append_task_event(
        TaskEventRecord(
            id=str(uuid.uuid4()),
            task_id=task_id,
            user_id=user_id,
            event_type=event_type,
            payload=payload,
            created_at=_iso_now(),
        )
    )


async def _remove_task_events_for_task(
    *,
    user_id: str,
    task_id: str,
) -> None:
    existing = await repository.list_future_task_events(
        user_id=user_id,
        date_key=None,
        from_time_iso=_iso_now(),
    )
    for event in existing:
        payload = event.payload if isinstance(event.payload, dict) else {}
        if str(payload.get("task_id") or "").strip() != task_id:
            continue
        if event.cron_job_id is not None:
            await repository.unschedule_event_job(int(event.cron_job_id))
        await repository.delete_event(event.id)


async def _rebuild_task_events_for_date(
    *,
    user_id: str,
    timezone_name: str,
    date_key: str,
) -> List[Dict[str, Any]]:
    lock = _get_task_rebuild_lock(user_id, date_key)
    async with lock:
        target_date, start_at_utc, end_at_utc = _day_window_bounds(date_key, timezone_name)
        now_dt = _utc_now()
        now_iso = _iso_now()
        existing = await repository.list_future_task_events(
            user_id=user_id,
            date_key=target_date,
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

        timeboxed = [
            task
            for task in await repository.list_tasks_in_window(
                user_id=user_id,
                start_at_utc=start_at_utc,
                end_at_utc=end_at_utc,
            )
            if task.timebox_start_at and task.timebox_end_at and task.status != "done"
        ]
        timeboxed.sort(key=lambda task: task.timebox_start_at or "")

        desired_specs: List[Dict[str, Any]] = []
        for index, task in enumerate(timeboxed):
            start_dt = _parse_iso(task.timebox_start_at or "")
            end_dt = _parse_iso(task.timebox_end_at or "")

            before_at = start_dt - timedelta(minutes=5)
            if before_at >= now_dt:
                desired_specs.append(
                    {
                        "task": task,
                        "trigger_type": "before_task",
                        "run_at": before_at,
                        "seed_date": target_date,
                    }
                )

            next_task = timeboxed[index + 1] if index + 1 < len(timeboxed) else None
            next_start: datetime | None = None
            if next_task and next_task.timebox_start_at:
                next_start = _parse_iso(next_task.timebox_start_at)

            end_trigger: TaskTriggerType = "after_task"
            if next_start is not None and next_start - end_dt <= timedelta(minutes=10):
                end_trigger = "transition"

            if end_dt >= now_dt:
                desired_specs.append(
                    {
                        "task": task,
                        "trigger_type": end_trigger,
                        "run_at": end_dt,
                        "seed_date": target_date,
                    }
                )

        scheduled: List[Dict[str, Any]] = []
        retained_event_ids: set[str] = set()

        for spec in desired_specs:
            task: TaskRecord = spec["task"]
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
    timezone_name: str,
    user_id: str,
) -> Dict[str, Any]:
    if action == "capture_tasks":
        titles = _extract_titles(payload)
        if not titles:
            raise HTTPException(status_code=400, detail="capture_tasks requires at least one title")

        created = await repository.create_tasks(user_id=user_id, titles=titles)
        for task in created:
            await _append_task_event(
                task_id=task.task_id,
                user_id=user_id,
                event_type="created",
                payload={"title": task.title},
            )
        all_tasks = await repository.list_tasks(user_id=user_id)
        return {
            "action": action,
            "created_task_ids": [task.task_id for task in created],
            "tasks": [_task_item_from_record(task).model_dump() for task in all_tasks],
        }

    if action == "get_tasks":
        tasks = await repository.list_tasks(user_id=user_id)
        return {
            "action": action,
            "tasks": [_task_item_from_record(task).model_dump() for task in tasks],
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
        existing_tasks = await repository.list_tasks(user_id=user_id)
        existing_lookup = {task.task_id: task for task in existing_tasks}
        for task_id in cleaned_ids:
            if task_id not in existing_lookup:
                raise HTTPException(status_code=404, detail=f"Task not found: {task_id}")

        updated_tasks = await repository.replace_top_essentials(user_id=user_id, task_ids=cleaned_ids)
        for task in updated_tasks:
            previous = existing_lookup.get(task.task_id)
            before = (
                previous.priority_rank if previous else None,
                previous.is_essential if previous else False,
            )
            after = (task.priority_rank, task.is_essential)
            if before == after:
                continue
            await _append_task_event(
                task_id=task.task_id,
                user_id=user_id,
                event_type="top_essential_updated",
                payload={
                    "priority_rank": task.priority_rank,
                    "is_essential": task.is_essential,
                },
            )

        return {
            "action": action,
            "top_essentials": cleaned_ids,
            "tasks": [_task_item_from_record(task).model_dump() for task in updated_tasks],
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

        existing_task = await repository.get_task(task_id=task_id, user_id=user_id)
        if not existing_task:
            raise HTTPException(status_code=404, detail=f"Task not found: {task_id}")

        try:
            start_dt = _parse_iso(start_at)
            end_dt = _parse_iso(end_at)
        except Exception as error:
            raise HTTPException(status_code=400, detail=f"Invalid datetime: {error}") from error

        if start_dt >= end_dt:
            raise HTTPException(status_code=400, detail="timebox_task requires start_at < end_at")

        start_date_local = _date_key_for_datetime(start_dt, timezone_name)
        end_date_local = _date_key_for_datetime(end_dt, timezone_name)
        if start_date_local != end_date_local:
            raise HTTPException(
                status_code=400,
                detail="Timebox must stay within one local calendar day in your timezone.",
            )

        normalized_start = _iso_from_dt(start_dt)
        normalized_end = _iso_from_dt(end_dt)
        updated_task = await repository.set_task_timebox(
            task_id=task_id,
            user_id=user_id,
            start_at=normalized_start,
            end_at=normalized_end,
        )
        if not updated_task:
            raise HTTPException(status_code=404, detail=f"Task not found: {task_id}")

        previous_date = _task_date_from_record(existing_task, timezone_name)
        next_date = _task_date_from_record(updated_task, timezone_name)
        await _append_task_event(
            task_id=task_id,
            user_id=user_id,
            event_type="timebox_updated",
            payload={
                "previous_start_at": existing_task.timebox_start_at,
                "previous_end_at": existing_task.timebox_end_at,
                "start_at": normalized_start,
                "end_at": normalized_end,
            },
        )

        scheduled_events: List[Dict[str, Any]] = []
        rebuild_dates = {date for date in [previous_date, next_date] if date}
        for rebuild_date in sorted(rebuild_dates):
            scheduled_events.extend(
                await _rebuild_task_events_for_date(
                    user_id=user_id,
                    timezone_name=timezone_name,
                    date_key=rebuild_date,
                )
            )
        scheduled_events.sort(key=lambda row: row["scheduled_time"])

        return {
            "action": action,
            "task": _task_item_from_record(updated_task).model_dump(),
            "scheduled_events": scheduled_events,
        }

    if action == "get_schedule":
        date_value = payload.get("date")
        date_text = str(date_value) if isinstance(date_value, str) else "today"
        schedule = await _build_schedule_for_user(
            user_id=user_id,
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
        existing_task = await repository.get_task(task_id=task_id, user_id=user_id)
        if not existing_task:
            raise HTTPException(status_code=404, detail=f"Task not found: {task_id}")
        updated_task = await repository.update_task_status(
            task_id=task_id,
            user_id=user_id,
            status=status,  # type: ignore[arg-type]
        )
        if not updated_task:
            raise HTTPException(status_code=404, detail=f"Task not found: {task_id}")
        await _append_task_event(
            task_id=task_id,
            user_id=user_id,
            event_type="status_updated",
            payload={"from_status": existing_task.status, "to_status": status},
        )
        task_date = _task_date_from_record(updated_task, timezone_name)
        if status == "done":
            await _remove_task_events_for_task(user_id=user_id, task_id=task_id)
        elif (
            existing_task.status == "done"
            and task_date
            and updated_task.timebox_end_at
            and _parse_iso(updated_task.timebox_end_at) >= _utc_now()
        ):
            await _rebuild_task_events_for_date(
                user_id=user_id,
                timezone_name=timezone_name,
                date_key=task_date,
            )
        return {
            "action": action,
            "task": _task_item_from_record(updated_task).model_dump(),
        }

    raise HTTPException(status_code=400, detail=f"Unsupported action: {action}")


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
    await _ensure_task_session(
        user_id=device_id,
        session_id=resolved_session_id,
        timezone_name=timezone_name,
    )

    result = await _run_task_management_action(
        action=action,
        payload=payload,
        timezone_name=timezone_name,
        user_id=device_id,
    )
    task_state = await _load_task_state_for_user(
        user_id=device_id,
        timezone_name=timezone_name,
    )

    task_panel_state = _build_task_panel_state(
        task_state=task_state,
        timezone_name=timezone_name,
        run_status="idle",
        action=action,
        payload=payload,
        result=result,
    )

    return {
        "session_id": resolved_session_id,
        "task_state": task_state.model_dump(),
        "result": result,
        "task_panel_state": task_panel_state,
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    runtime_config = _runtime_config_snapshot()
    return {
        "status": "ok",
        "supabase_url": os.getenv("SUPABASE_URL", ""),
        "repository_mode": "supabase"
        if isinstance(repository, SupabaseRepository)
        else "in_memory",
        "task_mgmt_v1_enabled": TASK_MGMT_V1_ENABLED,
        "task_tool_calling_v1_enabled": TASK_TOOL_CALLING_V1_ENABLED,
        "adk_model": _active_adk_model_name(),
        "runtime_env": runtime_config["runtime_env"],
        "cloud_run_service": runtime_config["cloud_run_service"],
        "strict_startup_validation": runtime_config["strict_startup_validation"],
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
        logger.exception(
            "HTTP ADK run failed",
            extra={
                "user_id": user_id,
                "session_id": session_id,
                "timezone": timezone_name,
                "model": model_name,
                "error": str(error),
            },
        )
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
    morning_seed: Dict[str, Any] | None = None
    wake_time = str(profile_context.get("wake_time") or "").strip()
    if not needs_onboarding and wake_time:
        try:
            morning_seed = await _seed_next_morning_wake_event(
                user_id=device_id,
                timezone_name=timezone_name,
                wake_time=wake_time,
            )
        except Exception as error:
            _log_event(
                logging.WARNING,
                "Morning wake seed skipped during bootstrap",
                device_id=device_id,
                timezone=timezone_name,
                error=str(error),
            )
    existing_session = await repository.get_session_by_id(session_id=session_id)
    thread_type = _session_thread_type(
        user_id=device_id,
        session_id=session_id,
        timezone_name=timezone_name,
        entry_context=entry_context,
        state=existing_session.state if existing_session else None,
    )
    await _ensure_session(
        user_id=device_id,
        session_id=session_id,
        timezone_name=timezone_name,
        entry_context=entry_context,
        state_patch={
            "thread_type": thread_type,
            "profile_context": profile_context,
            "lifecycle_state": "needs_onboarding" if needs_onboarding else "active",
        },
    )

    messages = await repository.list_messages(session_id=session_id)
    threads = await _thread_summaries(user_id=device_id)
    _log_event(
        logging.INFO,
        "Device bootstrap completed",
        device_id=device_id,
        session_id=session_id,
        timezone=timezone_name,
        needs_onboarding=needs_onboarding,
        thread_type=thread_type,
        seeded=morning_seed.get("seeded") if morning_seed else None,
        scheduled=bool(morning_seed.get("cron_job_id")) if morning_seed else None,
    )
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
    _log_event(
        logging.INFO,
        "Push token registered",
        device_id=device_id,
        timezone=timezone_name,
    )
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
    morning_seed: Dict[str, Any] | None = None
    try:
        morning_seed = await _seed_next_morning_wake_event(
            user_id=device_id,
            timezone_name=timezone_name,
            wake_time=wake_time,
        )
    except Exception as error:
        _log_event(
            logging.WARNING,
            "Morning wake seed skipped during onboarding",
            device_id=device_id,
            timezone=timezone_name,
            error=str(error),
        )
    _log_event(
        logging.INFO,
        "Onboarding completed",
        device_id=device_id,
        session_id=daily_session_id,
        timezone=timezone_name,
        seeded=morning_seed.get("seeded") if morning_seed else None,
        scheduled=bool(morning_seed.get("cron_job_id")) if morning_seed else None,
        event_id=morning_seed.get("event_id") if morning_seed else None,
        scheduled_time=morning_seed.get("scheduled_time") if morning_seed else None,
        reason=morning_seed.get("reason") if morning_seed else "seed_failed",
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
        output = await _execute_task_management(
            device_id=device_id,
            timezone_name=timezone_name,
            action=payload.action,
            payload=payload.payload,
            session_id=payload.session_id,
        )
        await _broadcast_task_panel(device_id, output["session_id"], output["task_panel_state"])
        _log_event(
            logging.INFO,
            "Task management action completed",
            device_id=device_id,
            session_id=output["session_id"],
            timezone=timezone_name,
            action=payload.action,
            task_written=payload.action
            in {"capture_tasks", "set_top_essentials", "timebox_task", "update_task_status"},
        )
        return output
    except HTTPException as error:
        _log_event(
            logging.WARNING,
            "Task management action rejected",
            device_id=device_id,
            session_id=payload.session_id,
            timezone=timezone_name,
            action=payload.action,
            status_code=error.status_code,
            detail=str(error.detail),
        )
        raise
    except Exception as error:
        logger.exception(
            "Task management action failed",
            extra={
                "device_id": device_id,
                "session_id": payload.session_id,
                "timezone": timezone_name,
                "action": payload.action,
                "error": str(error),
            },
        )
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
    socket_closed = False

    async def _safe_send_json(payload: Dict[str, Any]) -> bool:
        nonlocal socket_closed
        if socket_closed:
            return False
        try:
            await websocket.send_json(payload)
            return True
        except WebSocketDisconnect:
            socket_closed = True
            return False
        except RuntimeError as error:
            # Starlette raises RuntimeError after the close handshake starts.
            if 'Cannot call "send" once a close message has been sent.' in str(error):
                socket_closed = True
                return False
            raise

    try:
        timezone_name = await _resolve_timezone_for_user(
            user_id=device_id,
            provided_timezone=timezone,
        )
    except HTTPException as error:
        detail = error.detail if isinstance(error.detail, str) else "Invalid timezone"
        await _safe_send_json({"type": "error", "code": "invalid_timezone", "detail": detail})
        try:
            await websocket.close()
        except Exception:
            pass
        return
    active_user_id = device_id
    active_session_id = session_id
    unsubscribe_task_panel = None
    initialized = False
    suppress_startup_on_init = False
    latest_entry_context = EntryContext(entry_mode="proactive" if entry_mode == "proactive" else "reactive")
    latest_missed_proactive_events: List[Dict[str, Any]] = []
    latest_proactive_ack_ids: List[str] = []
    latest_missed_reported_ids: List[str] = []

    _log_event(
        logging.INFO,
        "WebSocket connected",
        device_id=active_user_id,
        session_id=active_session_id,
        timezone=timezone_name,
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
        context_map = {
            "session_id": active_session_id,
            "user_id": active_user_id,
            "timezone": timezone_name,
            "entry_mode": latest_entry_context.entry_mode,
            "trigger_type": latest_entry_context.trigger_type or "",
            "entry_context": json.dumps(latest_entry_context.model_dump(), separators=(",", ":")),
            "profile_context": json.dumps(profile_context, separators=(",", ":")),
            "missed_proactive_count": str(len(latest_missed_proactive_events)),
            "missed_proactive_events": json.dumps(
                latest_missed_proactive_events, separators=(",", ":")
            ),
        }

        if TASK_MGMT_V1_ENABLED:
            open_tasks = [
                task
                for task in await repository.list_tasks(user_id=active_user_id)
                if _task_is_open(task)
            ]
            due_time = _get_current_time_context(timezone_name)
            due_schedule = await _build_schedule_for_user(
                user_id=active_user_id,
                timezone_name=timezone_name,
                raw_date="today",
            )
            context_map["due_diligence_time"] = json.dumps(due_time, separators=(",", ":"))
            context_map["due_diligence_schedule"] = json.dumps(due_schedule, separators=(",", ":"))
            context_map["due_diligence_tasks"] = json.dumps(
                [
                    {
                        "id": task.task_id,
                        "title": task.title,
                        "status": task.status,
                        "is_essential": task.is_essential,
                        "priority_rank": task.priority_rank,
                        "timebox_start_at": task.timebox_start_at,
                        "timebox_end_at": task.timebox_end_at,
                    }
                    for task in open_tasks
                ],
                separators=(",", ":"),
            )
        return context_map

    async def _run_assistant_turn(
        *,
        prompt: str,
        metadata: Dict[str, Any] | None = None,
        user_task_intent: bool = False,
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
                if not await _safe_send_json(
                    {
                        "type": "assistant_delta",
                        "message_id": assistant_message_id,
                        "delta": delta,
                        "text": cumulative,
                    }
                ):
                    return False
        except WebSocketDisconnect:
            return False
        except Exception as error:
            model_name = _active_adk_model_name()
            logger.exception(
                "WebSocket ADK stream failed",
                extra={
                    "device_id": active_user_id,
                    "session_id": active_session_id,
                    "timezone": timezone_name,
                    "model": model_name,
                    "error": str(error),
                },
            )
            await _safe_send_json(
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
        startup_turn = bool(assistant_metadata.get("startup_turn"))
        task_write_count = 0
        consume_write_count = getattr(agent, "consume_task_write_count", None)
        if callable(consume_write_count):
            try:
                task_write_count = int(
                    consume_write_count(user_id=active_user_id, session_id=active_session_id)
                )
            except Exception:
                task_write_count = 0
        task_written = task_write_count > 0

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

        assistant_claimed_task_action = _looks_like_task_claim(assistant_text)
        if (user_task_intent or assistant_claimed_task_action) and not task_written and not startup_turn:
            _log_event(
                logging.WARNING,
                "Task intent detected without task write",
                device_id=active_user_id,
                session_id=active_session_id,
                user_task_intent=user_task_intent,
                assistant_claimed_task_action=assistant_claimed_task_action,
                task_written=False,
            )
        else:
            _log_event(
                logging.INFO,
                "Assistant turn completed",
                device_id=active_user_id,
                session_id=active_session_id,
                startup_turn=startup_turn,
                task_written=task_written,
                task_write_count=task_write_count,
            )

        return await _safe_send_json(
            {
                "type": "assistant_done",
                "message_id": assistant_message_id,
                "text": assistant_text,
            }
        )

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                frame = json.loads(raw)
            except json.JSONDecodeError:
                if not await _safe_send_json(
                    {"type": "error", "code": "invalid_json", "detail": "Invalid JSON frame"}
                ):
                    break
                continue

            frame_type = frame.get("type")
            if frame_type == "ping":
                if not await _safe_send_json({"type": "pong", "ts": _iso_now()}):
                    break
                continue

            if frame_type == "init":
                active_user_id = str(frame.get("device_id") or active_user_id)
                active_session_id = str(frame.get("session_id") or active_session_id)
                suppress_startup_on_init = bool(frame.get("suppress_startup_on_init"))
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
                    _log_event(
                        logging.INFO,
                        "Ignoring repeated post_onboarding",
                        device_id=active_user_id,
                        session_id=active_session_id,
                    )

                await repository.ensure_user(user_id=active_user_id, timezone_name=timezone_name)
                user_profile = await repository.get_user_profile(user_id=active_user_id)
                profile_context = _build_profile_context(user_profile)
                try:
                    session_record = await _ensure_session(
                        user_id=active_user_id,
                        session_id=active_session_id,
                        timezone_name=timezone_name,
                        entry_context=latest_entry_context,
                        state_patch={"profile_context": profile_context},
                    )
                except HTTPException as error:
                    detail = error.detail if isinstance(error.detail, str) else "Session init failed"
                    code = "session_forbidden" if error.status_code == 403 else "session_init_failed"
                    if not await _safe_send_json({"type": "error", "code": code, "detail": detail}):
                        break
                    continue
                session_state = dict(session_record.state if session_record else {})
                latest_proactive_ack_ids = _normalize_event_id_list(
                    session_state.get(PROACTIVE_ACK_STATE_KEY)
                )
                latest_missed_reported_ids = _normalize_event_id_list(
                    session_state.get(MISSED_REPORTED_STATE_KEY)
                )

                current_proactive_event_id: str | None = None
                if latest_entry_context.entry_mode == "proactive":
                    proactive_event_id = str(latest_entry_context.event_id or "").strip()
                    if proactive_event_id:
                        current_proactive_event_id = proactive_event_id
                        latest_proactive_ack_ids = _append_event_id(
                            latest_proactive_ack_ids, proactive_event_id
                        )

                latest_missed_proactive_events = await _list_missed_proactive_events_for_today(
                    user_id=active_user_id,
                    timezone_name=timezone_name,
                    current_event_id=current_proactive_event_id,
                    acknowledged_ids=latest_proactive_ack_ids,
                    reported_ids=latest_missed_reported_ids,
                )

                state_patch: Dict[str, Any] = {}
                if latest_proactive_ack_ids != _normalize_event_id_list(
                    session_state.get(PROACTIVE_ACK_STATE_KEY)
                ):
                    state_patch[PROACTIVE_ACK_STATE_KEY] = latest_proactive_ack_ids
                if latest_missed_reported_ids != _normalize_event_id_list(
                    session_state.get(MISSED_REPORTED_STATE_KEY)
                ):
                    state_patch[MISSED_REPORTED_STATE_KEY] = latest_missed_reported_ids
                if state_patch:
                    try:
                        await _ensure_session(
                            user_id=active_user_id,
                            session_id=active_session_id,
                            timezone_name=timezone_name,
                            entry_context=latest_entry_context,
                            state_patch=state_patch,
                        )
                    except HTTPException:
                        pass
                history = await repository.list_messages(session_id=active_session_id)
                current_task_state = await _load_task_state_for_user(
                    user_id=active_user_id,
                    timezone_name=timezone_name,
                )
                if unsubscribe_task_panel:
                    unsubscribe_task_panel()

                async def _task_panel_listener(snapshot: Dict[str, Any]) -> bool:
                    return await _safe_send_json({"type": "task_panel_state", "state": snapshot})

                unsubscribe_task_panel = _subscribe_task_panel(
                    active_user_id,
                    active_session_id,
                    _task_panel_listener,
                )
                if not await _safe_send_json(
                    {
                        "type": "session_ready",
                        "session_id": active_session_id,
                        "messages": [message.model_dump() for message in history],
                    }
                ):
                    break
                if not await _safe_send_json(
                    {
                        "type": "task_panel_state",
                        "state": _build_task_panel_state(
                            task_state=current_task_state,
                            timezone_name=timezone_name,
                        ),
                    }
                ):
                    break
                initialized = True
                _log_event(
                    logging.INFO,
                    "WebSocket session initialized",
                    device_id=active_user_id,
                    session_id=active_session_id,
                    ws_init=True,
                    startup_suppressed=suppress_startup_on_init,
                    message_count=len(history),
                    entry_mode=latest_entry_context.entry_mode,
                    trigger_type=latest_entry_context.trigger_type or "",
                )

                startup_needed = not suppress_startup_on_init
                if startup_needed:
                    startup_sent = await _run_assistant_turn(
                        prompt=_startup_prompt(latest_entry_context),
                        metadata={
                            "startup_turn": True,
                            "entry_context": latest_entry_context.model_dump(),
                        },
                    )
                    surfaced_ids = [
                        str(item.get("event_id") or "").strip()
                        for item in latest_missed_proactive_events
                        if str(item.get("event_id") or "").strip()
                    ]
                    if startup_sent and surfaced_ids:
                        for surfaced_id in surfaced_ids:
                            latest_missed_reported_ids = _append_event_id(
                                latest_missed_reported_ids, surfaced_id
                            )
                        try:
                            await _ensure_session(
                                user_id=active_user_id,
                                session_id=active_session_id,
                                timezone_name=timezone_name,
                                entry_context=latest_entry_context,
                                state_patch={
                                    MISSED_REPORTED_STATE_KEY: latest_missed_reported_ids
                                },
                            )
                        except HTTPException:
                            pass
                        latest_missed_proactive_events = []
                    _log_event(
                        logging.INFO,
                        "Startup turn evaluated",
                        device_id=active_user_id,
                        session_id=active_session_id,
                        ws_init=True,
                        startup_sent=startup_sent,
                    )
                continue

            if frame_type != "user_message":
                if not await _safe_send_json(
                    {"type": "error", "code": "unknown_frame", "detail": "Unknown frame type"}
                ):
                    break
                continue

            if not initialized:
                if not await _safe_send_json(
                    {"type": "error", "code": "not_initialized", "detail": "Send init first"}
                ):
                    break
                continue

            message_id = str(frame.get("message_id") or "")
            text = str(frame.get("text") or "").strip()
            if not message_id or not text:
                if not await _safe_send_json(
                    {
                        "type": "error",
                        "code": "invalid_user_message",
                        "detail": "message_id and text are required",
                    }
                ):
                    break
                continue

            if await repository.message_exists(session_id=active_session_id, message_id=message_id):
                if not await _safe_send_json(
                    {
                        "type": "error",
                        "code": "duplicate_message",
                        "detail": f"message_id {message_id} already exists",
                    }
                ):
                    break
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

            if not await _run_assistant_turn(
                prompt=text,
                user_task_intent=_looks_like_task_intent(text),
            ):
                if socket_closed:
                    break
                continue

    except WebSocketDisconnect:
        _log_event(
            logging.INFO,
            "WebSocket disconnected",
            device_id=active_user_id,
            session_id=active_session_id,
        )
    except Exception as error:
        logger.exception(
            "WebSocket failure",
            extra={
                "device_id": active_user_id,
                "session_id": active_session_id,
                "error": str(error),
            },
        )
        try:
            await _safe_send_json({"type": "error", "code": "server_error", "detail": str(error)})
        except Exception:
            pass
    finally:
        if unsubscribe_task_panel:
            unsubscribe_task_panel()
