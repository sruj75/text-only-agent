import json
import logging
import os
import re
import asyncio
import base64
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from startup_agent.adk import ONBOARDING_INSTRUCTION, SimpleADK
from startup_agent.task_actions import (
    TASK_INTENT_LABELS,
    TASK_MANAGEMENT_INTENTS,
    TASK_QUERY_TYPES,
    TASK_STATUS_VALUES,
    TASK_WRITE_INTENTS,
    TaskManagementIntent,
    TaskQueryType,
)


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
        "release_id": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
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
CONTRACT_VERSION = "2026-03-09"
RELEASE_ID = (os.getenv("K_REVISION") or "dev-local").strip() or "dev-local"
SCHEDULER_SECRET_HEADER = "x-scheduler-secret"
EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"
MAX_DELIVERY_ATTEMPTS = 4
RETRY_BASE_SECONDS = 60
RETRY_MAX_SECONDS = 30 * 60

StartupWorkflowState = Literal[
    "succeeded",
    "failed",
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
    r"\b(task|tasks|todo|to-do|priority|priorities|timebox|schedule|reschedule|delete|plan day|mark done|blocked)\b",
    re.IGNORECASE,
)
TASK_CLAIM_PATTERN = re.compile(
    r"\b(added|captured|saved|scheduled|timeboxed|prioritized|marked)\b",
    re.IGNORECASE,
)
TASK_SCHEDULE_CLAIM_PATTERN = re.compile(
    r"\b(scheduled|timeboxed|rescheduled|moved to|set for)\b",
    re.IGNORECASE,
)
PROACTIVE_ACK_STATE_KEY = "proactive_ack_event_ids_v1"
MISSED_REPORTED_STATE_KEY = "missed_reported_event_ids_v1"
MISSED_PROACTIVE_EVENT_TYPES = {"morning_wake", "checkin", "calendar_reminder"}
MISSED_PROACTIVE_MAX_EVENTS = 5
ONBOARDING_STATUS_PENDING = "pending"
ONBOARDING_STATUS_IN_PROGRESS = "in_progress"
ONBOARDING_STATUS_READY_FOR_MAIN = "ready_for_main"
ONBOARDING_STATUS_COMPLETED = "completed"
ONBOARDING_AGENT_STATUSES = {
    ONBOARDING_STATUS_IN_PROGRESS,
    ONBOARDING_STATUS_READY_FOR_MAIN,
}
WAKE_PURPOSE_DAILY_LOOP = "daily_loop"
WAKE_PURPOSE_ONBOARDING_UNLOCK = "onboarding_unlock"


@dataclass
class PreparedSessionState:
    user_id: str
    session_id: str
    timezone_name: str
    entry_context: "EntryContext"
    profile_context: Dict[str, Any]
    needs_onboarding: bool
    missed_proactive_events: List[Dict[str, Any]]
    proactive_ack_ids: List[str]
    missed_reported_ids: List[str]
    messages: List["MessageRecord"]
    task_panel_state: Dict[str, Any]


@dataclass(frozen=True)
class SessionOpenContract:
    open_id: str
    client_version: str
    contract_version: str
    contract_match: bool


@dataclass(frozen=True)
class ResolvedEventExecutionContext:
    event: "CheckinEventRecord"
    event_id: str
    event_type: str
    event_user_id: str
    event_payload: Dict[str, Any]
    calendar_timezone: str | None
    session_timezone: str | None
    session_id: str
    trigger_type: str
    calendar_event_id: str | None
    title: str
    body: str
    notification_data: Dict[str, Any]
    attempted_at: str
    next_attempt_count: int


@dataclass(frozen=True)
class DeliveryAttemptResult:
    push_sent: bool
    session_ready: bool
    delivery_succeeded: bool
    last_error: str | None
    next_morning_wake: Dict[str, Any] | None
    continuation_failed: bool


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


def _new_correlation_id() -> str:
    return uuid.uuid4().hex


def _new_open_id() -> str:
    return f"open_{uuid.uuid4().hex}"


def _message_cursor_for(message: "MessageRecord") -> str:
    return f"{message.created_at}|{message.id}"


def _conversation_start_idempotency_key(
    *,
    session_id: str,
    open_id: str,
) -> str:
    return f"{session_id}|{open_id}"


def _startup_message_id_for_key(idempotency_key: str) -> str:
    token = uuid.uuid5(uuid.NAMESPACE_URL, idempotency_key).hex[:24]
    return f"startup_{token}"


def _normalize_version_token(raw_value: str | None, fallback: str) -> str:
    value = str(raw_value or "").strip()
    return value or fallback


def _cloud_tasks_parent_path() -> str:
    project_id = (
        os.getenv("CLOUD_TASKS_PROJECT_ID")
        or os.getenv("GOOGLE_CLOUD_PROJECT")
        or os.getenv("GCP_PROJECT")
        or ""
    ).strip()
    location = (os.getenv("CLOUD_TASKS_LOCATION") or "").strip()
    queue = (os.getenv("CLOUD_TASKS_QUEUE") or "").strip()
    if not project_id or not location or not queue:
        raise RuntimeError(
            "Cloud Tasks configuration missing: set CLOUD_TASKS_PROJECT_ID (or GOOGLE_CLOUD_PROJECT), "
            "CLOUD_TASKS_LOCATION, and CLOUD_TASKS_QUEUE"
        )
    return f"projects/{project_id}/locations/{location}/queues/{queue}"


def _cloud_tasks_dispatch_url() -> str:
    direct = (os.getenv("CLOUD_TASKS_DISPATCH_URL") or "").strip()
    if direct:
        return direct
    cloud_run_url = (os.getenv("CLOUD_RUN_URL") or "").strip().rstrip("/")
    if cloud_run_url:
        return f"{cloud_run_url}/agent/events/execute"
    raise RuntimeError("Cloud Tasks dispatch URL missing: set CLOUD_TASKS_DISPATCH_URL or CLOUD_RUN_URL")


def _scheduler_shared_secret() -> str:
    value = str(os.getenv("SCHEDULER_SECRET") or os.getenv("SCHEDULER_EXECUTION_SECRET") or "").strip()
    if not value:
        raise RuntimeError("Scheduler secret missing: set SCHEDULER_SECRET")
    return value


async def _cloud_tasks_access_token() -> str:
    explicit = (os.getenv("CLOUD_TASKS_ACCESS_TOKEN") or "").strip()
    if explicit:
        return explicit
    metadata_url = (
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
    )
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(
                metadata_url,
                headers={"Metadata-Flavor": "Google"},
            )
    except Exception as error:
        raise RuntimeError(f"Unable to fetch Cloud Tasks access token from metadata server: {error}") from error
    if response.status_code >= 400:
        raise RuntimeError(f"Metadata token endpoint failed: {response.status_code} {response.text}")
    try:
        payload = response.json()
    except Exception as error:
        raise RuntimeError(f"Invalid metadata token payload: {error}") from error
    token = str(payload.get("access_token") or "").strip()
    if not token:
        raise RuntimeError("Metadata token payload missing access_token")
    return token


async def _create_cloud_task_for_event(*, event_id: str, run_at: str) -> str:
    parent = _cloud_tasks_parent_path()
    dispatch_url = _cloud_tasks_dispatch_url()
    scheduler_secret = _scheduler_shared_secret()
    token = await _cloud_tasks_access_token()
    schedule_time = _iso_from_dt(_parse_iso(run_at))
    request_body = {
        "task": {
            "scheduleTime": schedule_time,
            "httpRequest": {
                "httpMethod": "POST",
                "url": dispatch_url,
                "headers": {
                    "Content-Type": "application/json",
                    SCHEDULER_SECRET_HEADER: scheduler_secret,
                },
                "body": base64.b64encode(
                    json.dumps({"event_id": event_id}, separators=(",", ":")).encode("utf-8")
                ).decode("ascii"),
            },
        }
    }
    endpoint = f"https://cloudtasks.googleapis.com/v2/{parent}/tasks"
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            endpoint,
            json=request_body,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
    if response.status_code >= 400:
        raise RuntimeError(f"Cloud Tasks create failed: {response.status_code} {response.text}")
    try:
        payload = response.json()
    except Exception as error:
        raise RuntimeError(f"Cloud Tasks create returned invalid JSON: {error}") from error
    task_name = str(payload.get("name") or "").strip()
    if not task_name:
        raise RuntimeError("Cloud Tasks create response missing task name")
    return task_name


async def _delete_cloud_task(task_name: str) -> bool:
    cleaned = str(task_name or "").strip()
    if not cleaned:
        return True
    token = await _cloud_tasks_access_token()
    endpoint = f"https://cloudtasks.googleapis.com/v2/{cleaned}"
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.delete(
            endpoint,
            headers={"Authorization": f"Bearer {token}"},
        )
    if response.status_code in {200, 204, 404}:
        return True
    raise RuntimeError(f"Cloud Tasks delete failed: {response.status_code} {response.text}")


def _task_error(code: str, message: str, *, status_code: int = 400) -> HTTPException:
    return HTTPException(
        status_code=status_code,
        detail={
            "code": code,
            "message": message,
        },
    )


def _http_error(status_code: int, code: str, message: str) -> HTTPException:
    return HTTPException(
        status_code=status_code,
        detail={
            "code": code,
            "message": message,
        },
    )


def _error_code_for_status(status_code: int) -> str:
    status_map = {
        400: "BAD_REQUEST",
        401: "UNAUTHORIZED",
        403: "FORBIDDEN",
        404: "NOT_FOUND",
        409: "CONFLICT",
        426: "UPGRADE_REQUIRED",
        422: "INVALID_REQUEST",
        500: "INTERNAL_ERROR",
        503: "SERVICE_UNAVAILABLE",
    }
    return status_map.get(status_code, "HTTP_ERROR")


def _error_envelope(*, status_code: int, detail: Any) -> Dict[str, Any]:
    if isinstance(detail, dict):
        raw_code = str(detail.get("code") or "").strip()
        raw_message = str(detail.get("message") or detail.get("detail") or "").strip()
        code = raw_code or _error_code_for_status(status_code)
        message = raw_message or "Request failed"
        return {"error": {"code": code, "message": message}}
    if isinstance(detail, str) and detail.strip():
        return {"error": {"code": _error_code_for_status(status_code), "message": detail.strip()}}
    return {"error": {"code": _error_code_for_status(status_code), "message": "Request failed"}}


def _task_error_code_from_http_error(error: HTTPException) -> str:
    detail = error.detail
    if isinstance(detail, dict):
        raw = detail.get("code")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return "TASK_ACTION_FAILED"


def _task_error_message_from_http_error(error: HTTPException) -> str:
    detail = error.detail
    if isinstance(detail, dict):
        raw = detail.get("message")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    if isinstance(detail, str) and detail.strip():
        return detail
    return "Task action failed"


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


class SessionOpenRequest(BaseModel):
    device_id: str
    timezone: str | None = Field(default=None)
    session_id: str | None = Field(default=None)
    entry_context: EntryContext | None = Field(default=None)
    source: Literal["manual", "push"] = "manual"
    open_id: str = Field(..., min_length=1)
    client_version: str = Field(..., min_length=1)
    contract_version: str = Field(..., min_length=1)


class LegacyBootstrapRequest(BaseModel):
    device_id: str
    timezone: str | None = Field(default=None)
    session_id: str | None = Field(default=None)
    entry_context: EntryContext | None = Field(default=None)


class PushTokenRequest(BaseModel):
    device_id: str
    expo_push_token: str = Field(..., min_length=1)
    timezone: str | None = Field(default=None)


class OnboardingStartRequest(BaseModel):
    device_id: str
    timezone: str | None = Field(default=None)


class OnboardingSleepScheduleRequest(BaseModel):
    device_id: str
    timezone: str | None = Field(default=None)
    wake_time: str = Field(..., min_length=1)
    bedtime: str = Field(..., min_length=1)


class TaskManagementRequest(BaseModel):
    device_id: str
    timezone: str | None = Field(default=None)
    session_id: str | None = Field(default=None)
    intent: TaskManagementIntent
    entities: Dict[str, Any] = Field(default_factory=dict)
    options: Dict[str, Any] = Field(default_factory=dict)


class TaskQueryRequest(BaseModel):
    device_id: str
    timezone: str | None = Field(default=None)
    session_id: str | None = Field(default=None)
    query: TaskQueryType
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


class TaskOperationRecord(BaseModel):
    operation_id: str
    user_id: str
    intent: str
    session_id: str | None = None
    entities: Dict[str, Any] = Field(default_factory=dict)
    options: Dict[str, Any] = Field(default_factory=dict)
    apply_result: Dict[str, Any] = Field(default_factory=dict)
    validation_errors: List[str] = Field(default_factory=list)
    correlation_id: str | None = None
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
        raise _http_error(400, "INVALID_DATE", "date must be 'today' or YYYY-MM-DD")
    try:
        start_local = datetime.strptime(target_date, "%Y-%m-%d").replace(
            tzinfo=ZoneInfo(timezone_name)
        )
    except ValueError as error:
        raise _http_error(400, "INVALID_DATE", "date must be 'today' or YYYY-MM-DD") from error
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
    cloud_task_name: str | None = None
    last_error: str | None = None
    last_attempt_at: str | None = None
    next_retry_at: str | None = None
    dead_lettered_at: str | None = None
    workflow_state: str = "requested"
    attempt_count: int = 0
    created_at: str = Field(default_factory=_iso_now)
    updated_at: str = Field(default_factory=_iso_now)


class EventExecuteRequest(BaseModel):
    event_id: str = Field(..., min_length=1)


class InMemoryRepository:
    def __init__(self) -> None:
        self.users: Dict[str, Dict[str, Any]] = {}
        self.sessions: Dict[str, SessionRecord] = {}
        self.messages: Dict[str, List[MessageRecord]] = {}
        self.push_tokens: Dict[str, str] = {}
        self.events: Dict[str, CheckinEventRecord] = {}
        self.tasks: Dict[str, TaskRecord] = {}
        self.task_events: Dict[str, TaskEventRecord] = {}
        self.task_operation_log: Dict[str, TaskOperationRecord] = {}
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
        existing.setdefault("onboarding_status", ONBOARDING_STATUS_PENDING)
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
        wake_time: str | None,
        bedtime: str | None,
        onboarding_status: str,
    ) -> Dict[str, Any]:
        await self.ensure_user(user_id=user_id, timezone_name=timezone_name)
        existing = self.users.get(user_id, {})
        existing["timezone"] = timezone_name
        existing["wake_time"] = wake_time
        existing["bedtime"] = bedtime
        existing["onboarding_status"] = onboarding_status
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

    async def delete_tasks(self, user_id: str, task_ids: List[str]) -> int:
        deleted = 0
        for task_id in task_ids:
            existing = self.tasks.get(task_id)
            if not existing or existing.user_id != user_id:
                continue
            self.tasks.pop(task_id, None)
            deleted += 1
        return deleted

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

    async def append_task_operation(self, payload: TaskOperationRecord) -> TaskOperationRecord:
        self.task_operation_log[payload.operation_id] = payload
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

    async def update_event_cloud_task_name(self, event_id: str, cloud_task_name: str | None) -> None:
        existing = self.events.get(event_id)
        if not existing:
            return
        updated = existing.model_copy(
            update={"cloud_task_name": cloud_task_name, "updated_at": _iso_now()}
        )
        self.events[event_id] = updated

    async def schedule_event_job(
        self,
        *,
        event_id: str,
        run_at: str,
        timezone_name: str,
    ) -> str:
        _ = event_id
        _ = run_at
        _ = timezone_name
        self.next_job_id += 1
        return f"projects/local/locations/local/queues/default/tasks/{self.next_job_id}"

    async def unschedule_event_job(self, cron_job_id: int | str) -> bool:
        _ = cron_job_id
        return True

    async def delete_event(self, event_id: str) -> None:
        self.events.pop(event_id, None)

    async def get_event_for_execution(self, event_id: str) -> CheckinEventRecord | None:
        event = self.events.get(event_id)
        if not event:
            return None
        return event.model_copy()

    async def finalize_event_execution(
        self,
        *,
        event_id: str,
        last_error: str | None,
        attempted_at: str,
        executed: bool,
        attempt_count: int | None = None,
    ) -> bool:
        existing = self.events.get(event_id)
        if not existing:
            return False
        next_attempt_count = (
            int(attempt_count)
            if attempt_count is not None
            else int(existing.attempt_count or 0) + 1
        )
        self.events[event_id] = existing.model_copy(
            update={
                "executed": executed,
                "last_error": last_error,
                "last_attempt_at": attempted_at,
                "attempt_count": next_attempt_count,
                "updated_at": _iso_now(),
            }
        )
        return True

    async def schedule_event_retry(
        self,
        *,
        event_id: str,
        next_run_at: str,
        timezone_name: str,
        last_error: str,
        attempted_at: str,
    ) -> str:
        existing = self.events.get(event_id)
        if not existing:
            raise RuntimeError(f"Event not found: {event_id}")
        if existing.cloud_task_name:
            await self.unschedule_event_job(existing.cloud_task_name)
        task_name = await self.schedule_event_job(
            event_id=event_id,
            run_at=next_run_at,
            timezone_name=timezone_name,
        )
        self.events[event_id] = existing.model_copy(
            update={
                "executed": False,
                "scheduled_time": next_run_at,
                "cron_job_id": None,
                "cloud_task_name": task_name,
                "last_error": last_error,
                "last_attempt_at": attempted_at,
                "updated_at": _iso_now(),
            }
        )
        return task_name

    async def update_event_execution_state(
        self,
        *,
        event_id: str,
        attempt_count: int,
        attempted_at: str,
        last_error: str | None,
        next_retry_at: str | None,
        workflow_state: Literal["succeeded", "retry_scheduled", "dead_letter"],
    ) -> None:
        existing = self.events.get(event_id)
        if not existing:
            raise RuntimeError(f"Event not found: {event_id}")
        self.events[event_id] = existing.model_copy(
            update={
                "attempt_count": int(attempt_count),
                "last_attempt_at": attempted_at,
                "last_error": last_error,
                "next_retry_at": next_retry_at,
                "workflow_state": workflow_state,
                "dead_lettered_at": _iso_now() if workflow_state == "dead_letter" else None,
                "updated_at": _iso_now(),
            }
        )

    async def get_scheduler_secret_for_execution(self) -> str | None:
        raw = os.getenv("SCHEDULER_SECRET") or os.getenv("SCHEDULER_EXECUTION_SECRET")
        value = str(raw or "").strip()
        return value or None

    async def get_user_timezone_for_execution(self, user_id: str) -> str | None:
        user = self.users.get(user_id)
        if not isinstance(user, dict):
            return None
        timezone_name = _normalize_timezone(str(user.get("timezone") or "").strip())
        return timezone_name

    async def get_push_token_for_execution(self, user_id: str) -> str | None:
        token = str(self.push_tokens.get(user_id) or "").strip()
        return token or None

    async def delete_push_token_for_execution(self, user_id: str) -> bool:
        self.push_tokens.pop(user_id, None)
        return True

    async def ensure_next_morning_wake_event(self, event_id: str) -> Dict[str, Any]:
        event = self.events.get(event_id)
        if not event:
            return {"status": "skipped", "reason": "event_not_found"}
        if event.event_type != "morning_wake":
            return {"status": "skipped", "reason": "not_morning_wake"}
        timezone_name = await self.get_user_timezone_for_execution(event.user_id)
        user = self.users.get(event.user_id, {})
        wake_time = str(user.get("wake_time") or "").strip()
        if not timezone_name or not _is_valid_hhmm(wake_time):
            return {"status": "skipped", "reason": "invalid_user_profile"}
        seed = await _seed_next_morning_wake_event(
            user_id=event.user_id,
            timezone_name=timezone_name,
            wake_time=wake_time,
        )
        return {"status": "ok", "detail": seed}


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

    @staticmethod
    def _rpc_int(output: Any, key: str) -> int | None:
        if isinstance(output, int):
            return output
        if isinstance(output, float):
            return int(output)
        if isinstance(output, str) and output.strip().isdigit():
            return int(output)
        if isinstance(output, list) and output:
            first = output[0]
            if isinstance(first, dict):
                value = first.get(key)
                if isinstance(value, int):
                    return value
                if isinstance(value, float):
                    return int(value)
        if isinstance(output, dict):
            value = output.get(key)
            if isinstance(value, int):
                return value
            if isinstance(value, float):
                return int(value)
        return None

    @staticmethod
    def _rpc_bool(output: Any, key: str) -> bool:
        if isinstance(output, bool):
            return output
        if isinstance(output, list) and output:
            first = output[0]
            if isinstance(first, dict):
                value = first.get(key)
                if isinstance(value, bool):
                    return value
        if isinstance(output, dict):
            value = output.get(key)
            if isinstance(value, bool):
                return value
        return False

    @staticmethod
    def _rpc_text(output: Any, key: str | None = None) -> str | None:
        if isinstance(output, str):
            value = output.strip()
            return value or None
        if isinstance(output, list) and output:
            first = output[0]
            if isinstance(first, dict) and key:
                value = str(first.get(key) or "").strip()
                return value or None
        if isinstance(output, dict):
            if key:
                value = str(output.get(key) or "").strip()
                return value or None
            for candidate in output.values():
                if isinstance(candidate, str) and candidate.strip():
                    return candidate.strip()
        return None

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
                "select": "user_id,wake_time,bedtime,timezone,onboarding_status,created_at,updated_at",
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
        wake_time: str | None,
        bedtime: str | None,
        onboarding_status: str,
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
                    "onboarding_status": onboarding_status,
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

    async def _list_task_item_rows(
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
                    "created_at,updated_at"
                ),
                "order": "created_at.asc,task_id.asc",
                "limit": str(page_size),
                "offset": str(offset),
            }
            if extra_params:
                params.update(extra_params)

            page = await self._request(
                "GET",
                "task_items",
                params=params,
            )
            page_rows = page or []
            rows.extend(page_rows)
            if len(page_rows) < page_size:
                break
            offset += page_size

        return rows

    async def _list_task_timebox_rows(
        self,
        *,
        user_id: str,
        extra_params: Dict[str, str] | None = None,
    ) -> List[Dict[str, Any]]:
        page_size = 500
        offset = 0
        rows: List[Dict[str, Any]] = []

        while True:
            params: Dict[str, str] = {
                "user_id": f"eq.{user_id}",
                "select": "task_id,user_id,start_at,end_at,source,created_at,updated_at",
                "order": "start_at.asc",
                "limit": str(page_size),
                "offset": str(offset),
            }
            if extra_params:
                params.update(extra_params)
            page = await self._request(
                "GET",
                "task_timeboxes",
                params=params,
            )
            page_rows = page or []
            rows.extend(page_rows)
            if len(page_rows) < page_size:
                break
            offset += page_size

        return rows

    @staticmethod
    def _task_record_from_rows(
        item_row: Dict[str, Any],
        timebox_row: Dict[str, Any] | None,
    ) -> TaskRecord:
        return TaskRecord(
            task_id=str(item_row.get("task_id") or ""),
            user_id=str(item_row.get("user_id") or ""),
            title=str(item_row.get("title") or ""),
            status=str(item_row.get("status") or "todo"),  # type: ignore[arg-type]
            priority_rank=item_row.get("priority_rank"),
            is_essential=bool(item_row.get("is_essential") or False),
            timebox_start_at=(str(timebox_row.get("start_at") or "").strip() or None)
            if isinstance(timebox_row, dict)
            else None,
            timebox_end_at=(str(timebox_row.get("end_at") or "").strip() or None)
            if isinstance(timebox_row, dict)
            else None,
            created_at=str(item_row.get("created_at") or _iso_now()),
            updated_at=str(item_row.get("updated_at") or _iso_now()),
        )

    async def list_tasks(self, user_id: str) -> List[TaskRecord]:
        item_rows = await self._list_task_item_rows(user_id=user_id)
        timebox_rows = await self._list_task_timebox_rows(user_id=user_id)
        timebox_map = {
            str(row.get("task_id") or ""): row
            for row in timebox_rows
            if str(row.get("task_id") or "").strip()
        }
        tasks = [self._task_record_from_rows(row, timebox_map.get(str(row.get("task_id") or ""))) for row in item_rows]
        tasks.sort(key=_task_sort_key)
        return tasks

    async def get_task(self, task_id: str, user_id: str) -> TaskRecord | None:
        rows = await self._request(
            "GET",
            "task_items",
            params={
                "task_id": f"eq.{task_id}",
                "user_id": f"eq.{user_id}",
                "select": "task_id,user_id,title,status,priority_rank,is_essential,created_at,updated_at",
                "limit": "1",
            },
        )
        if not rows:
            return None
        timeboxes = await self._request(
            "GET",
            "task_timeboxes",
            params={
                "task_id": f"eq.{task_id}",
                "user_id": f"eq.{user_id}",
                "select": "task_id,user_id,start_at,end_at,source,created_at,updated_at",
                "limit": "1",
            },
        )
        timebox_row = (timeboxes or [None])[0]
        return self._task_record_from_rows(rows[0], timebox_row)

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
            "task_items",
            json_body=payload,
            extra_headers={"Prefer": "return=representation"},
        )
        return [self._task_record_from_rows(row, None) for row in rows or []]

    async def delete_tasks(self, user_id: str, task_ids: List[str]) -> int:
        if not task_ids:
            return 0
        deleted = 0
        for task_id in task_ids:
            rows = await self._request(
                "DELETE",
                "task_items",
                params={"task_id": f"eq.{task_id}", "user_id": f"eq.{user_id}"},
                extra_headers={"Prefer": "return=representation"},
            )
            if isinstance(rows, list):
                deleted += len(rows)
        return deleted

    async def update_task_status(
        self,
        task_id: str,
        user_id: str,
        status: TaskStatus,
    ) -> TaskRecord | None:
        rows = await self._request(
            "PATCH",
            "task_items",
            params={"task_id": f"eq.{task_id}", "user_id": f"eq.{user_id}"},
            json_body={"status": status, "updated_at": _iso_now()},
            extra_headers={"Prefer": "return=representation"},
        )
        if not rows:
            return None
        return await self.get_task(task_id=task_id, user_id=user_id)

    async def set_task_timebox(
        self,
        task_id: str,
        user_id: str,
        start_at: str,
        end_at: str,
    ) -> TaskRecord | None:
        rows = await self._request(
            "POST",
            "task_timeboxes",
            params={"on_conflict": "task_id"},
            json_body={
                "task_id": task_id,
                "user_id": user_id,
                "start_at": start_at,
                "end_at": end_at,
                "source": "agent",
                "updated_at": _iso_now(),
            },
            extra_headers={"Prefer": "resolution=merge-duplicates,return=representation"},
        )
        if not rows:
            return await self.get_task(task_id=task_id, user_id=user_id)
        return await self.get_task(task_id=task_id, user_id=user_id)

    async def replace_top_essentials(self, user_id: str, task_ids: List[str]) -> List[TaskRecord]:
        tasks = await self.list_tasks(user_id=user_id)
        ordered_lookup = {task_id: index for index, task_id in enumerate(task_ids, start=1)}
        updated_rows: List[TaskRecord] = []
        for task in tasks:
            next_rank = ordered_lookup.get(task.task_id)
            rows = await self._request(
                "PATCH",
                "task_items",
                params={"task_id": f"eq.{task.task_id}", "user_id": f"eq.{user_id}"},
                json_body={
                    "priority_rank": next_rank,
                    "is_essential": next_rank is not None,
                    "updated_at": _iso_now(),
                },
                extra_headers={"Prefer": "return=representation"},
            )
            if rows:
                updated_rows.append(await self.get_task(task_id=task.task_id, user_id=user_id) or task)
        updated_rows.sort(key=_task_sort_key)
        return updated_rows

    async def list_tasks_in_window(
        self,
        user_id: str,
        start_at_utc: str,
        end_at_utc: str,
    ) -> List[TaskRecord]:
        timebox_rows = await self._list_task_timebox_rows(
            user_id=user_id,
            extra_params={"and": f"(start_at.gte.{start_at_utc},start_at.lt.{end_at_utc})"},
        )
        task_ids = [
            str(row.get("task_id") or "").strip()
            for row in timebox_rows
            if str(row.get("task_id") or "").strip()
        ]
        all_tasks = await self.list_tasks(user_id=user_id)
        tasks = [task for task in all_tasks if task.task_id in set(task_ids)]
        tasks.sort(key=_task_sort_key)
        return tasks

    async def append_task_event(self, payload: TaskEventRecord) -> TaskEventRecord:
        await self.append_task_operation(
            TaskOperationRecord(
                operation_id=payload.id,
                user_id=payload.user_id,
                intent=payload.event_type,
                entities=payload.payload,
                apply_result={"task_id": payload.task_id},
            )
        )
        return payload

    async def append_task_operation(self, payload: TaskOperationRecord) -> TaskOperationRecord:
        rows = await self._request(
            "POST",
            "task_operation_log",
            json_body=[payload.model_dump()],
            extra_headers={"Prefer": "return=representation"},
        )
        if not rows:
            raise RuntimeError("Task operation insert returned empty response")
        return TaskOperationRecord(**rows[0])

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
            "select": (
                "id,user_id,scheduled_time,event_type,payload,executed,cron_job_id,cloud_task_name,last_error,"
                "last_attempt_at,next_retry_at,dead_lettered_at,workflow_state,attempt_count,"
                "created_at,updated_at"
            ),
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
            "select": (
                "id,user_id,scheduled_time,event_type,payload,executed,cron_job_id,cloud_task_name,last_error,"
                "last_attempt_at,next_retry_at,dead_lettered_at,workflow_state,attempt_count,"
                "created_at,updated_at"
            ),
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
            "select": "id,user_id,scheduled_time,event_type,payload,executed,cron_job_id,cloud_task_name,last_error,last_attempt_at,next_retry_at,dead_lettered_at,workflow_state,attempt_count,created_at,updated_at",
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

    async def update_event_cloud_task_name(self, event_id: str, cloud_task_name: str | None) -> None:
        await self._request(
            "PATCH",
            "events",
            params={"id": f"eq.{event_id}"},
            json_body={"cloud_task_name": cloud_task_name, "updated_at": _iso_now()},
            extra_headers={"Prefer": "return=minimal"},
        )

    async def schedule_event_job(
        self,
        *,
        event_id: str,
        run_at: str,
        timezone_name: str,
    ) -> str:
        _ = timezone_name
        return await _create_cloud_task_for_event(event_id=event_id, run_at=run_at)

    async def unschedule_event_job(self, cron_job_id: int | str) -> bool:
        task_name = str(cron_job_id or "").strip()
        if not task_name:
            return False
        return await _delete_cloud_task(task_name)

    async def delete_event(self, event_id: str) -> None:
        await self._request(
            "DELETE",
            "events",
            params={"id": f"eq.{event_id}"},
            extra_headers={"Prefer": "return=minimal"},
        )

    async def get_event_for_execution(self, event_id: str) -> CheckinEventRecord | None:
        output = await self._rpc("get_event_for_execution", {"p_event_id": event_id})
        row: Dict[str, Any] | None = None
        if isinstance(output, dict):
            row = output
        elif isinstance(output, list) and output and isinstance(output[0], dict):
            row = output[0]
        if not row:
            return None
        return CheckinEventRecord(**row)

    async def finalize_event_execution(
        self,
        *,
        event_id: str,
        last_error: str | None,
        attempted_at: str,
        executed: bool,
        attempt_count: int | None = None,
    ) -> bool:
        output = await self._rpc(
            "finalize_event_execution",
            {
                "p_event_id": event_id,
                "p_last_error": last_error,
                "p_attempted_at": attempted_at,
                "p_executed": executed,
                "p_attempt_count": attempt_count,
            },
        )
        return self._rpc_bool(output, "finalize_event_execution")

    async def schedule_event_retry(
        self,
        *,
        event_id: str,
        next_run_at: str,
        timezone_name: str,
        last_error: str,
        attempted_at: str,
    ) -> str:
        existing = await self.get_event_for_execution(event_id)
        old_task_name = str((existing.cloud_task_name if existing else "") or "").strip()
        await self._rpc(
            "schedule_event_retry",
            {
                "p_event_id": event_id,
                "p_next_run_at": next_run_at,
                "p_timezone": timezone_name,
                "p_last_error": last_error,
                "p_attempted_at": attempted_at,
            },
        )
        if old_task_name:
            await self.unschedule_event_job(old_task_name)
        new_task_name = await self.schedule_event_job(
            event_id=event_id,
            run_at=next_run_at,
            timezone_name=timezone_name,
        )
        await self.update_event_cloud_task_name(event_id, new_task_name)
        return new_task_name

    async def update_event_execution_state(
        self,
        *,
        event_id: str,
        attempt_count: int,
        attempted_at: str,
        last_error: str | None,
        next_retry_at: str | None,
        workflow_state: Literal["succeeded", "retry_scheduled", "dead_letter"],
    ) -> None:
        await self._request(
            "PATCH",
            "events",
            params={"id": f"eq.{event_id}"},
            json_body={
                "attempt_count": attempt_count,
                "last_error": last_error,
                "last_attempt_at": attempted_at,
                "next_retry_at": next_retry_at,
                "workflow_state": workflow_state,
                "dead_lettered_at": _iso_now() if workflow_state == "dead_letter" else None,
                "updated_at": _iso_now(),
            },
            extra_headers={"Prefer": "return=minimal"},
        )

    async def get_scheduler_secret_for_execution(self) -> str | None:
        raw = os.getenv("SCHEDULER_SECRET") or os.getenv("SCHEDULER_EXECUTION_SECRET")
        value = str(raw or "").strip()
        return value or None

    async def get_user_timezone_for_execution(self, user_id: str) -> str | None:
        output = await self._rpc("get_user_timezone_for_execution", {"p_user_id": user_id})
        return self._rpc_text(output)

    async def get_push_token_for_execution(self, user_id: str) -> str | None:
        output = await self._rpc("get_push_token_for_execution", {"p_user_id": user_id})
        return self._rpc_text(output)

    async def delete_push_token_for_execution(self, user_id: str) -> bool:
        output = await self._rpc("delete_push_token_for_execution", {"p_user_id": user_id})
        return self._rpc_bool(output, "delete_push_token_for_execution")

    async def ensure_next_morning_wake_event(self, event_id: str) -> Dict[str, Any]:
        output = await self._rpc("ensure_next_morning_wake_event", {"p_event_id": event_id})
        if isinstance(output, dict):
            payload = dict(output)
        elif isinstance(output, list) and output and isinstance(output[0], dict):
            payload = dict(output[0])
        else:
            return {"status": "unknown"}

        status = str(payload.get("status") or "").strip().lower()
        continuation_event_id = str(payload.get("event_id") or "").strip()
        if status != "ok" or not continuation_event_id:
            return payload

        continuation = await self.get_event_for_execution(continuation_event_id)
        if not continuation:
            return {
                **payload,
                "status": "error",
                "reason": "continuation_event_missing",
            }
        if continuation.cloud_task_name:
            return {
                **payload,
                "cloud_task_name": continuation.cloud_task_name,
            }

        run_at = str(continuation.scheduled_time or "").strip()
        if not run_at:
            return {
                **payload,
                "status": "error",
                "reason": "continuation_schedule_time_missing",
            }
        continuation_timezone = _coerce_string(continuation.payload.get("timezone")) or "UTC"

        try:
            task_name = await self.schedule_event_job(
                event_id=continuation_event_id,
                run_at=run_at,
                timezone_name=continuation_timezone,
            )
            await self.update_event_cloud_task_name(continuation_event_id, task_name)
        except Exception as error:
            return {
                **payload,
                "status": "error",
                "reason": "continuation_enqueue_failed",
                "error": str(error),
            }

        return {
            **payload,
            "queued": True,
            "cloud_task_name": task_name,
        }


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
        error_message = _error_envelope(status_code=error.status_code, detail=error.detail)["error"]["message"]
        return {"ok": False, "error": error_message}

    return {"ok": True, "current_time": _get_current_time_context(timezone_name)}


async def _tool_task_management(
    intent: TaskManagementIntent,
    entities: Dict[str, Any],
    options: Dict[str, Any],
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
        error_message = _error_envelope(status_code=error.status_code, detail=error.detail)["error"]["message"]
        return {"ok": False, "error": error_message}

    target_session_id = raw_session_id or _daily_session_id(raw_user_id, timezone_name)
    normalized_entities = dict(entities or {})
    normalized_options = dict(options or {})

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
                action=intent,
                payload=normalized_entities,
            ),
        )

        output = await _execute_task_management(
            device_id=raw_user_id,
            timezone_name=timezone_name,
            intent=intent,
            entities=normalized_entities,
            options=normalized_options,
            session_id=target_session_id,
        )
        await _broadcast_task_panel(
            raw_user_id,
            output["session_id"],
            _build_task_panel_state(
                task_state=TaskStateV1(**output["task_state"]),
                timezone_name=timezone_name,
                run_status="complete",
                action=intent,
                payload=normalized_entities,
                result=output.get("result"),
            ),
        )
        return {"ok": True, **output}
    except HTTPException as error:
        error_code = _task_error_code_from_http_error(error)
        error_message = _task_error_message_from_http_error(error)
        _log_event(
            logging.WARNING,
            "Task tool action rejected",
            device_id=raw_user_id,
            session_id=target_session_id,
            timezone=timezone_name,
            intent=intent,
            error_code=error_code,
            detail=error_message,
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
                    action=intent,
                    payload=normalized_entities,
                    error_message=error_message,
                ),
            )
        return {
            "ok": False,
            "error": {
                "code": error_code,
                "message": error_message,
            },
        }
    except Exception as error:
        logger.exception(
            "Task tool action failed",
            extra={
                "device_id": raw_user_id,
                "session_id": target_session_id,
                "timezone": timezone_name,
                "intent": intent,
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
                    action=intent,
                    payload=normalized_entities,
                    error_message=str(error),
                ),
            )
        return {"ok": False, "error": {"code": "TASK_ACTION_FAILED", "message": str(error)}}


async def _tool_task_query(
    query: TaskQueryType,
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
        error_message = _error_envelope(status_code=error.status_code, detail=error.detail)["error"]["message"]
        return {"ok": False, "error": error_message}
    target_session_id = raw_session_id or _daily_session_id(raw_user_id, timezone_name)
    try:
        output = await _execute_task_query(
            device_id=raw_user_id,
            timezone_name=timezone_name,
            query=query,
            payload=dict(payload or {}),
            session_id=target_session_id,
        )
        await _broadcast_task_panel(raw_user_id, output["session_id"], output["task_panel_state"])
        return {"ok": True, **output}
    except HTTPException as error:
        return {
            "ok": False,
            "error": {
                "code": _task_error_code_from_http_error(error),
                "message": _task_error_message_from_http_error(error),
            },
        }


async def _tool_onboarding_sleep_schedule(
    wake_time: str,
    bedtime: str,
    session_id: str | None,
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
        target_session_id = session_id or runtime_context.get("session_id")
        output = await _set_onboarding_sleep_schedule(
            device_id=raw_user_id,
            timezone_name=timezone_name,
            wake_time=wake_time,
            bedtime=bedtime,
            session_id=target_session_id,
        )
        return {"ok": True, **output}
    except HTTPException as error:
        error_message = _error_envelope(status_code=error.status_code, detail=error.detail)["error"]["message"]
        return {"ok": False, "error": error_message}
    except Exception as error:
        return {"ok": False, "error": str(error)}


agent = SimpleADK(
    get_current_time_tool=_tool_get_current_time,
    task_management_tool=_tool_task_management,
    task_query_tool=_tool_task_query,
    enable_task_tools=TASK_TOOL_CALLING_V1_ENABLED,
)
onboarding_agent = SimpleADK(
    instruction=ONBOARDING_INSTRUCTION,
    agent_name="intentive_onboarding",
    onboarding_sleep_schedule_tool=_tool_onboarding_sleep_schedule,
    enable_onboarding_tool=True,
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

    scheduler_secret = await repository.get_scheduler_secret_for_execution()
    if not str(scheduler_secret or "").strip():
        raise RuntimeError("SCHEDULER_SECRET is missing from runtime environment")

    _cloud_tasks_parent_path()
    _cloud_tasks_dispatch_url()

    # RPC exists only in the scheduler source-of-truth migration and validates availability.
    await repository.ensure_next_morning_wake_event("__healthcheck__")


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


@app.exception_handler(HTTPException)
async def handle_http_exception(request: Request, error: HTTPException) -> JSONResponse:
    envelope = _error_envelope(status_code=error.status_code, detail=error.detail)
    payload = envelope["error"]
    if request.url.path != "/health":
        _log_event(
            logging.WARNING if error.status_code < 500 else logging.ERROR,
            "HTTP exception returned",
            method=request.method,
            path=request.url.path,
            query=str(request.url.query),
            status_code=error.status_code,
            error_code=payload.get("code"),
            error_message=payload.get("message"),
        )
    return JSONResponse(
        status_code=error.status_code,
        content=envelope,
    )


@app.exception_handler(RequestValidationError)
async def handle_validation_exception(request: Request, error: RequestValidationError) -> JSONResponse:
    message = "Invalid request"
    issues = error.errors()
    if issues:
        candidate = str(issues[0].get("msg") or "").strip()
        if candidate:
            message = candidate
    if request.url.path != "/health":
        _log_event(
            logging.WARNING,
            "HTTP validation failed",
            method=request.method,
            path=request.url.path,
            query=str(request.url.query),
            status_code=422,
            error_code="INVALID_REQUEST",
            error_message=message,
            issue_count=len(issues),
        )
    return JSONResponse(
        status_code=422,
        content={
            "error": {
                "code": "INVALID_REQUEST",
                "message": message,
            }
        },
    )


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
    onboarding_status = _normalize_onboarding_status(profile.get("onboarding_status"))

    return {
        "wake_time": wake_time or None,
        "bedtime": bedtime or None,
        "onboarding_status": onboarding_status,
    }


def _normalize_onboarding_status(raw_status: Any) -> str:
    status = str(raw_status or "").strip().lower()
    if status in {
        ONBOARDING_STATUS_PENDING,
        ONBOARDING_STATUS_IN_PROGRESS,
        ONBOARDING_STATUS_READY_FOR_MAIN,
        ONBOARDING_STATUS_COMPLETED,
    }:
        return status
    return ONBOARDING_STATUS_PENDING


def _needs_onboarding(user_profile: Dict[str, Any] | None) -> bool:
    status = _normalize_onboarding_status((user_profile or {}).get("onboarding_status"))
    return status == ONBOARDING_STATUS_PENDING


def _onboarding_agent_enabled(profile_context: Dict[str, Any] | None) -> bool:
    status = _normalize_onboarding_status((profile_context or {}).get("onboarding_status"))
    return status in ONBOARDING_AGENT_STATUSES


def _wake_purpose_from_payload(payload: Dict[str, Any] | None) -> str:
    if not isinstance(payload, dict):
        return WAKE_PURPOSE_DAILY_LOOP
    raw_purpose = str(payload.get("wake_purpose") or "").strip().lower()
    if raw_purpose == WAKE_PURPOSE_ONBOARDING_UNLOCK:
        return WAKE_PURPOSE_ONBOARDING_UNLOCK
    if raw_purpose == WAKE_PURPOSE_DAILY_LOOP:
        return WAKE_PURPOSE_DAILY_LOOP
    if bool(payload.get("onboarding_unlock")):
        return WAKE_PURPOSE_ONBOARDING_UNLOCK
    return WAKE_PURPOSE_DAILY_LOOP


def _startup_prompt(entry_context: EntryContext, *, onboarding_agent_enabled: bool) -> str:
    if onboarding_agent_enabled:
        return (
            "Conversation bootstrap: onboarding open. "
            "Greet the user and ask for wake time first, then bedtime. "
            "After both are collected, call onboarding_sleep_schedule and confirm setup."
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


def _find_startup_message(
    messages: List[MessageRecord],
    *,
    open_id: str | None = None,
) -> MessageRecord | None:
    for message in messages:
        if message.role != "assistant":
            continue
        metadata = message.metadata if isinstance(message.metadata, dict) else {}
        if not bool(metadata.get("startup_turn")):
            continue
        if open_id is not None and str(metadata.get("startup_open_id") or "").strip() != open_id:
            continue
        return message
    return None


async def _build_runtime_context_for_session(
    *,
    user_id: str,
    session_id: str,
    timezone_name: str,
    entry_context: EntryContext,
    missed_proactive_events: List[Dict[str, Any]] | None = None,
) -> Dict[str, str]:
    session_record = await repository.get_session(
        session_id=session_id,
        user_id=user_id,
    )
    session_state = dict(session_record.state) if session_record else {}
    profile_context = session_state.get("profile_context")
    if not isinstance(profile_context, dict):
        user_profile = await repository.get_user_profile(user_id=user_id)
        profile_context = _build_profile_context(user_profile)
    context_map = {
        "session_id": session_id,
        "user_id": user_id,
        "timezone": timezone_name,
        "entry_mode": entry_context.entry_mode,
        "trigger_type": entry_context.trigger_type or "",
        "entry_context": json.dumps(entry_context.model_dump(), separators=(",", ":")),
        "profile_context": json.dumps(profile_context, separators=(",", ":")),
    }
    missed_events = missed_proactive_events or []
    context_map["missed_proactive_count"] = str(len(missed_events))
    context_map["missed_proactive_events"] = json.dumps(missed_events, separators=(",", ":"))

    if TASK_MGMT_V1_ENABLED:
        open_tasks = [
            task
            for task in await repository.list_tasks(user_id=user_id)
            if _task_is_open(task)
        ]
        due_time = _get_current_time_context(timezone_name)
        due_schedule = await _build_schedule_for_user(
            user_id=user_id,
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


async def _ensure_startup_message_for_session(
    *,
    user_id: str,
    session_id: str,
    open_id: str,
    timezone_name: str,
    entry_context: EntryContext,
    profile_context: Dict[str, Any],
    source: Literal["manual", "push"],
    missed_proactive_events: List[Dict[str, Any]] | None = None,
) -> tuple[StartupWorkflowState, str | None, bool]:
    existing_messages = await repository.list_messages(session_id=session_id)
    startup_message = _find_startup_message(existing_messages, open_id=open_id)
    if startup_message:
        return "succeeded", startup_message.id, False

    idempotency_key = _conversation_start_idempotency_key(
        session_id=session_id,
        open_id=open_id,
    )
    startup_message_id = _startup_message_id_for_key(idempotency_key)
    generated_at = _iso_now()

    try:
        inserted_by_this_call = False
        use_onboarding_agent = _onboarding_agent_enabled(profile_context)
        selected_agent = onboarding_agent if use_onboarding_agent else agent
        context_map = await _build_runtime_context_for_session(
            user_id=user_id,
            session_id=session_id,
            timezone_name=timezone_name,
            entry_context=entry_context,
            missed_proactive_events=missed_proactive_events,
        )
        output = await selected_agent.run(
            prompt=_startup_prompt(
                entry_context,
                onboarding_agent_enabled=use_onboarding_agent,
            ),
            user_id=user_id,
            session_id=session_id,
            context=context_map,
        )
        assistant_text = output.strip() or "Let's begin."
        assistant_message = MessageRecord(
            id=startup_message_id,
            session_id=session_id,
            user_id=user_id,
            role="assistant",
            content=assistant_text,
            metadata={
                "model": "google-adk",
                "startup_turn": True,
                "startup_open_id": open_id,
                "startup_generation_key": idempotency_key,
                "entry_context": entry_context.model_dump(),
                "source": source,
                "agent_mode": "onboarding" if use_onboarding_agent else "main",
                "correlation_id": _new_correlation_id(),
            },
            created_at=generated_at,
        )
        try:
            await repository.insert_message(payload=assistant_message)
            inserted_by_this_call = True
        except RuntimeError as error:
            if "duplicate key" not in str(error).lower():
                raise
            _log_event(
                logging.INFO,
                "Startup message insert already applied",
                device_id=user_id,
                session_id=session_id,
                startup_message_id=startup_message_id,
            )

        refreshed_messages = await repository.list_messages(session_id=session_id)
        startup_after_insert = _find_startup_message(refreshed_messages, open_id=open_id)
        if not startup_after_insert:
            return "failed", None, False

        await _ensure_session(
            user_id=user_id,
            session_id=session_id,
            timezone_name=timezone_name,
            entry_context=entry_context,
            state_patch={
                "last_message_id": startup_after_insert.id,
                "last_role": "assistant",
                "startup_open_id": open_id,
                "startup_generation_key": idempotency_key,
                "startup_generated_at": startup_after_insert.created_at,
            },
        )
        return "succeeded", startup_after_insert.id, inserted_by_this_call
    except Exception as error:
        _log_event(
            logging.WARNING,
            "Session startup generation failed",
            device_id=user_id,
            session_id=session_id,
            startup_state="failed",
            error=str(error),
        )
        return "failed", None, False


def _resolve_session_open_contract(payload: SessionOpenRequest) -> SessionOpenContract:
    open_id = _normalize_version_token(payload.open_id, _new_open_id())
    client_version = _normalize_version_token(payload.client_version, "unknown")
    contract_version = _normalize_version_token(payload.contract_version, CONTRACT_VERSION)
    contract_match = contract_version == CONTRACT_VERSION
    return SessionOpenContract(
        open_id=open_id,
        client_version=client_version,
        contract_version=contract_version,
        contract_match=contract_match,
    )


def _enforce_session_open_contract(contract: SessionOpenContract) -> None:
    if not contract.contract_match:
        raise _http_error(
            409,
            "CONTRACT_VERSION_MISMATCH",
            f"client contract {contract.contract_version} does not match server {CONTRACT_VERSION}",
        )


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

    raise _http_error(
        400,
        "INVALID_TIMEZONE",
        "A valid IANA timezone is required (example: Asia/Kolkata).",
    )


def _next_wake_run_at_utc(
    *,
    timezone_name: str,
    wake_time_hhmm: str,
    force_tomorrow: bool = False,
) -> datetime:
    if not _is_valid_hhmm(wake_time_hhmm):
        raise _http_error(400, "INVALID_WAKE_TIME", "wake_time must be HH:MM (24h)")
    wake_hour, wake_minute = [int(part) for part in wake_time_hhmm.split(":")]
    now_local = datetime.now(ZoneInfo(timezone_name))
    target_local = now_local.replace(
        hour=wake_hour,
        minute=wake_minute,
        second=0,
        microsecond=0,
    )
    if force_tomorrow or target_local <= now_local:
        target_local = target_local + timedelta(days=1)
    return target_local.astimezone(timezone.utc)


async def _seed_next_morning_wake_event(
    *,
    user_id: str,
    timezone_name: str,
    wake_time: str,
    reason: str = "daily_bootstrap",
    wake_purpose: str = WAKE_PURPOSE_DAILY_LOOP,
    force_tomorrow: bool = False,
) -> Dict[str, Any]:
    if wake_purpose not in {WAKE_PURPOSE_DAILY_LOOP, WAKE_PURPOSE_ONBOARDING_UNLOCK}:
        wake_purpose = WAKE_PURPOSE_DAILY_LOOP

    run_at = _next_wake_run_at_utc(
        timezone_name=timezone_name,
        wake_time_hhmm=wake_time,
        force_tomorrow=force_tomorrow,
    )
    seed_date = _date_key_for_datetime(run_at, timezone_name)
    existing = await repository.list_future_events(
        user_id=user_id,
        from_time_iso=_iso_now(),
        event_type="morning_wake",
    )
    matching_existing = [
        event
        for event in existing
        if _wake_purpose_from_payload(event.payload) == wake_purpose
    ]
    if matching_existing:
        earliest = matching_existing[0]
        return {
            "seeded": False,
            "reason": "existing_pending_morning_wake",
            "event_id": earliest.id,
            "cloud_task_name": earliest.cloud_task_name,
            "scheduled_time": earliest.scheduled_time,
        }

    event_payload = {
        "schedule_owner": "system",
        "schedule_policy": "morning_bootstrap",
        "seed_date": seed_date,
        "timezone": timezone_name,
        "reason": reason,
        "trigger_type": "morning_wake",
        "wake_purpose": wake_purpose,
    }
    if wake_purpose == WAKE_PURPOSE_ONBOARDING_UNLOCK:
        event_payload["onboarding_unlock"] = True
    event_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"morning_wake:{wake_purpose}:{user_id}:{seed_date}",
        )
    )
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
                and _wake_purpose_from_payload(event.payload) == wake_purpose
            ),
            None,
        )
        if not matching:
            raise
        return {
            "seeded": False,
            "reason": "existing_pending_morning_wake",
            "event_id": matching.id,
            "cloud_task_name": matching.cloud_task_name,
            "scheduled_time": matching.scheduled_time,
        }
    try:
        task_name = await repository.schedule_event_job(
            event_id=created_event.id,
            run_at=created_event.scheduled_time,
            timezone_name=timezone_name,
        )
        await repository.update_event_cloud_task_name(created_event.id, task_name)
    except Exception:
        await repository.delete_event(created_event.id)
        raise

    return {
        "seeded": True,
        "event_id": created_event.id,
        "cloud_task_name": task_name,
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


def _looks_like_schedule_claim(text: str) -> bool:
    return bool(TASK_SCHEDULE_CLAIM_PATTERN.search(text or ""))


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
        raise _http_error(403, "SESSION_FORBIDDEN", "Session does not belong to device")
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


async def _prepare_session_state(
    *,
    user_id: str,
    session_id: str,
    timezone_name: str,
    entry_context: EntryContext,
    persist_event_state: bool,
) -> PreparedSessionState:
    await repository.ensure_user(user_id=user_id, timezone_name=timezone_name)
    user_profile = await repository.get_user_profile(user_id=user_id)
    profile_context = _build_profile_context(user_profile)
    needs_onboarding = _needs_onboarding(user_profile)
    onboarding_status = _normalize_onboarding_status(profile_context.get("onboarding_status"))
    lifecycle_state = "active"
    if needs_onboarding:
        lifecycle_state = "needs_onboarding"
    elif onboarding_status in ONBOARDING_AGENT_STATUSES:
        lifecycle_state = "onboarding_active"
    session_record = await _ensure_session(
        user_id=user_id,
        session_id=session_id,
        timezone_name=timezone_name,
        entry_context=entry_context,
        state_patch={
            "profile_context": profile_context,
            "lifecycle_state": lifecycle_state,
        },
    )
    session_state = dict(session_record.state if session_record else {})
    proactive_ack_ids = _normalize_event_id_list(session_state.get(PROACTIVE_ACK_STATE_KEY))
    missed_reported_ids = _normalize_event_id_list(session_state.get(MISSED_REPORTED_STATE_KEY))

    current_proactive_event_id: str | None = None
    if entry_context.entry_mode == "proactive":
        proactive_event_id = str(entry_context.event_id or "").strip()
        if proactive_event_id:
            current_proactive_event_id = proactive_event_id
            proactive_ack_ids = _append_event_id(proactive_ack_ids, proactive_event_id)

    missed_proactive_events = await _list_missed_proactive_events_for_today(
        user_id=user_id,
        timezone_name=timezone_name,
        current_event_id=current_proactive_event_id,
        acknowledged_ids=proactive_ack_ids,
        reported_ids=missed_reported_ids,
    )

    if persist_event_state:
        state_patch: Dict[str, Any] = {}
        if proactive_ack_ids != _normalize_event_id_list(session_state.get(PROACTIVE_ACK_STATE_KEY)):
            state_patch[PROACTIVE_ACK_STATE_KEY] = proactive_ack_ids
        if missed_reported_ids != _normalize_event_id_list(session_state.get(MISSED_REPORTED_STATE_KEY)):
            state_patch[MISSED_REPORTED_STATE_KEY] = missed_reported_ids
        if state_patch:
            await _ensure_session(
                user_id=user_id,
                session_id=session_id,
                timezone_name=timezone_name,
                entry_context=entry_context,
                state_patch=state_patch,
            )

    messages = await repository.list_messages(session_id=session_id)
    task_state = await _load_task_state_for_user(
        user_id=user_id,
        timezone_name=timezone_name,
    )
    return PreparedSessionState(
        user_id=user_id,
        session_id=session_id,
        timezone_name=timezone_name,
        entry_context=entry_context,
        profile_context=profile_context,
        needs_onboarding=needs_onboarding,
        missed_proactive_events=missed_proactive_events,
        proactive_ack_ids=proactive_ack_ids,
        missed_reported_ids=missed_reported_ids,
        messages=messages,
        task_panel_state=_build_task_panel_state(
            task_state=task_state,
            timezone_name=timezone_name,
        ),
    )


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
    if not action:
        return None
    return TASK_INTENT_LABELS.get(action, action.replace("_", " ").strip().title())


def _task_time_label(task: TaskItem, timezone_name: str) -> str | None:
    if not task.timebox:
        return None
    start_dt = _parse_iso(task.timebox.start_at).astimezone(ZoneInfo(timezone_name))
    end_dt = _parse_iso(task.timebox.end_at).astimezone(ZoneInfo(timezone_name))
    return f"{start_dt.strftime('%I:%M %p').lstrip('0')} - {end_dt.strftime('%I:%M %p').lstrip('0')}"


def _resolve_temporal_focus_task_id(task_state: TaskStateV1) -> str | None:
    now = _utc_now()
    scheduled_tasks = [
        task
        for task in task_state.tasks
        if task.timebox and task.status != "done"
    ]
    scheduled_tasks.sort(key=lambda task: task.timebox.start_at if task.timebox else "")

    for task in scheduled_tasks:
        if not task.timebox:
            continue
        start_dt = _parse_iso(task.timebox.start_at)
        end_dt = _parse_iso(task.timebox.end_at)
        if start_dt <= now < end_dt:
            return task.task_id

    for task in scheduled_tasks:
        if not task.timebox:
            continue
        start_dt = _parse_iso(task.timebox.start_at)
        if start_dt > now:
            return task.task_id

    return None


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
    _ = payload
    active_task_id = _resolve_temporal_focus_task_id(task_state)

    tasks: List[Dict[str, Any]] = []
    for task in task_state.tasks:
        tasks.append(
            {
                "id": task.task_id,
                "title": task.title,
                "status": task.status,
                "time_label": _task_time_label(task, timezone_name),
                "is_active": task.task_id == active_task_id,
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

    last_action_summary = None
    if isinstance(result, dict):
        applied = result.get("applied")
        if isinstance(applied, dict):
            fragments: List[str] = []
            for key in [
                "created",
                "timeboxed",
                "rescheduled",
                "deleted",
                "status_changed",
                "prioritized",
            ]:
                value = applied.get(key)
                if isinstance(value, int) and value > 0:
                    fragments.append(f"{key.replace('_', ' ')}: {value}")
            if fragments:
                last_action_summary = ", ".join(fragments)

    return {
        "run_status": run_status,
        "active_action": action_label,
        "headline": headline,
        "tasks": tasks,
        "schedule": schedule,
        "updated_at": task_state.updated_at,
        "error_message": error_message,
        "last_action_summary": last_action_summary,
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
        task_name = await repository.schedule_event_job(
            event_id=event_id,
            run_at=_iso_from_dt(run_at),
            timezone_name=timezone_name,
        )
    except Exception:
        await repository.delete_event(event_id)
        raise

    await repository.update_event_cloud_task_name(event_id, task_name)

    return {
        "event_id": event_id,
        "trigger_type": trigger_type,
        "scheduled_time": _iso_from_dt(run_at),
        "task_id": task.task_id,
        "task_title": task.title,
        "cloud_task_name": task_name,
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
        if event.cloud_task_name:
            await repository.unschedule_event_job(event.cloud_task_name)
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
            previous_task = timeboxed[index - 1] if index > 0 else None
            previous_end: datetime | None = None
            if previous_task and previous_task.timebox_end_at:
                previous_end = _parse_iso(previous_task.timebox_end_at)

            before_at = start_dt - timedelta(minutes=5)
            suppress_before_trigger = (
                previous_end is not None and start_dt - previous_end <= timedelta(minutes=10)
            )
            if before_at >= now_dt and not suppress_before_trigger:
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
                        "cloud_task_name": matched.cloud_task_name,
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
            if event.cloud_task_name:
                await repository.unschedule_event_job(event.cloud_task_name)
            await repository.delete_event(event.id)

        return scheduled


def _parse_local_time_to_utc(
    *,
    at_local: str,
    timezone_name: str,
    date_key: str,
) -> datetime:
    cleaned = at_local.strip().upper()
    parsed: datetime | None = None
    for fmt in ("%I:%M %p", "%I %p", "%H:%M"):
        try:
            parsed = datetime.strptime(cleaned, fmt)
            break
        except ValueError:
            continue
    if parsed is None:
        raise _task_error("INVALID_DATETIME", "Unsupported at_local format")
    local_date = datetime.strptime(date_key, "%Y-%m-%d")
    local_value = local_date.replace(
        hour=parsed.hour,
        minute=parsed.minute,
        second=0,
        microsecond=0,
        tzinfo=ZoneInfo(timezone_name),
    )
    return local_value.astimezone(timezone.utc)


def _resolve_timebox_window(
    *,
    task_payload: Dict[str, Any],
    timezone_name: str,
    date_key: str,
) -> tuple[str, str]:
    start_at = str(task_payload.get("start_at") or "").strip()
    end_at = str(task_payload.get("end_at") or "").strip()
    if start_at and end_at:
        try:
            start_dt = _parse_iso(start_at)
            end_dt = _parse_iso(end_at)
        except Exception as error:
            raise _task_error("INVALID_DATETIME", f"Invalid datetime: {error}") from error
        if start_dt >= end_dt:
            raise _task_error("INVALID_PAYLOAD", "start_at must be earlier than end_at")
        if _date_key_for_datetime(start_dt, timezone_name) != _date_key_for_datetime(end_dt, timezone_name):
            raise _task_error(
                "INVALID_PAYLOAD",
                "Timebox must stay within one local calendar day in your timezone.",
            )
        return _iso_from_dt(start_dt), _iso_from_dt(end_dt)

    at_local = str(task_payload.get("at_local") or "").strip()
    duration = task_payload.get("duration_minutes")
    if at_local and isinstance(duration, int) and duration > 0:
        start_dt = _parse_local_time_to_utc(at_local=at_local, timezone_name=timezone_name, date_key=date_key)
        end_dt = start_dt + timedelta(minutes=duration)
        if _date_key_for_datetime(start_dt, timezone_name) != _date_key_for_datetime(end_dt, timezone_name):
            raise _task_error(
                "INVALID_PAYLOAD",
                "Timebox must stay within one local calendar day in your timezone.",
            )
        return _iso_from_dt(start_dt), _iso_from_dt(end_dt)

    raise _task_error(
        "INVALID_TIME_INPUT",
        "Provide start_at/end_at (ISO) or at_local + duration_minutes.",
    )


async def _apply_schedule_rebuild(
    *,
    timezone_name: str,
    user_id: str,
    candidate_dates: List[str | None],
) -> List[Dict[str, Any]]:
    scheduled_events: List[Dict[str, Any]] = []
    rebuild_dates = {value for value in candidate_dates if value}
    try:
        for rebuild_date in sorted(rebuild_dates):
            scheduled_events.extend(
                await _rebuild_task_events_for_date(
                    user_id=user_id,
                    timezone_name=timezone_name,
                    date_key=rebuild_date,
                )
            )
    except Exception as error:
        raise _task_error(
            "SCHEDULER_UNAVAILABLE",
            "Task was updated, but reminder scheduling failed. Notifications may not arrive until the scheduler recovers.",
            status_code=503,
        ) from error
    scheduled_events.sort(key=lambda row: row["scheduled_time"])
    return scheduled_events


async def _run_task_management_intent(
    *,
    intent: TaskManagementIntent,
    entities: Dict[str, Any],
    options: Dict[str, Any],
    timezone_name: str,
    user_id: str,
    session_id: str,
) -> Dict[str, Any]:
    applied = {
        "created": 0,
        "timeboxed": 0,
        "rescheduled": 0,
        "deleted": 0,
        "status_changed": 0,
        "prioritized": 0,
    }
    rejected: List[Dict[str, Any]] = []
    now_date = _local_date_key(timezone_name)
    target_date = str(options.get("date") or entities.get("date") or now_date).strip() or now_date
    scheduled_events: List[Dict[str, Any]] = []

    if intent == "capture":
        raw_tasks = entities.get("tasks")
        if not isinstance(raw_tasks, list) or not raw_tasks:
            raise _task_error("MISSING_FIELD", "capture intent requires entities.tasks")
        task_payloads: List[Dict[str, Any]] = []
        for row in raw_tasks:
            if not isinstance(row, dict):
                continue
            title = str(row.get("title") or "").strip()
            if not title:
                continue
            task_payloads.append({**row, "title": title})
        titles = [row["title"] for row in task_payloads]
        if not titles:
            raise _task_error("MISSING_FIELD", "capture requires at least one task title")
        created = await repository.create_tasks(user_id=user_id, titles=titles)
        applied["created"] = len(created)
        for task, payload in zip(created, task_payloads):
            try:
                start_at, end_at = _resolve_timebox_window(
                    task_payload=payload,
                    timezone_name=timezone_name,
                    date_key=target_date,
                )
            except HTTPException as error:
                error_code = _task_error_code_from_http_error(error)
                if error_code == "INVALID_TIME_INPUT":
                    continue
                raise
            previous = await repository.get_task(task_id=task.task_id, user_id=user_id)
            updated = await repository.set_task_timebox(
                task_id=task.task_id,
                user_id=user_id,
                start_at=start_at,
                end_at=end_at,
            )
            if updated:
                applied["timeboxed"] += 1
                scheduled_events.extend(
                    await _apply_schedule_rebuild(
                        timezone_name=timezone_name,
                        user_id=user_id,
                        candidate_dates=[
                            _task_date_from_record(previous, timezone_name) if previous else None,
                            _task_date_from_record(updated, timezone_name),
                        ],
                    )
                )

    elif intent in {"timebox", "reschedule"}:
        raw_tasks = entities.get("tasks")
        if isinstance(raw_tasks, list):
            task_payloads = [row for row in raw_tasks if isinstance(row, dict)]
        else:
            task_payloads = [entities]
        if not task_payloads:
            raise _task_error("MISSING_FIELD", f"{intent} requires entities with task references")
        all_tasks = await repository.list_tasks(user_id=user_id)
        by_title = {task.title.lower(): task for task in all_tasks}
        counter_key = "rescheduled" if intent == "reschedule" else "timeboxed"
        for payload in task_payloads:
            task_id = str(payload.get("task_id") or "").strip()
            existing_task = None
            if task_id:
                existing_task = await repository.get_task(task_id=task_id, user_id=user_id)
            if not existing_task:
                title = str(payload.get("title") or "").strip().lower()
                existing_task = by_title.get(title)
            if not existing_task:
                rejected.append({"code": "TASK_NOT_FOUND", "payload": payload})
                continue
            start_at, end_at = _resolve_timebox_window(
                task_payload=payload,
                timezone_name=timezone_name,
                date_key=target_date,
            )
            previous = existing_task
            updated = await repository.set_task_timebox(
                task_id=existing_task.task_id,
                user_id=user_id,
                start_at=start_at,
                end_at=end_at,
            )
            if not updated:
                rejected.append({"code": "TASK_UPDATE_FAILED", "task_id": existing_task.task_id})
                continue
            applied[counter_key] += 1
            scheduled_events.extend(
                await _apply_schedule_rebuild(
                    timezone_name=timezone_name,
                    user_id=user_id,
                    candidate_dates=[
                        _task_date_from_record(previous, timezone_name),
                        _task_date_from_record(updated, timezone_name),
                    ],
                )
            )

    elif intent == "prioritize":
        task_ids = entities.get("task_ids")
        if not isinstance(task_ids, list):
            raise _task_error("MISSING_FIELD", "prioritize requires entities.task_ids")
        cleaned_ids = [str(item).strip() for item in task_ids if str(item).strip()]
        if len(cleaned_ids) > 3:
            raise _task_error("INVALID_PAYLOAD", "prioritize supports max 3 task_ids")
        updated = await repository.replace_top_essentials(user_id=user_id, task_ids=cleaned_ids)
        applied["prioritized"] = len([task for task in updated if task.is_essential])

    elif intent == "status":
        updates = entities.get("updates")
        if isinstance(updates, list):
            items = [row for row in updates if isinstance(row, dict)]
        else:
            items = [entities]
        if not items:
            raise _task_error("MISSING_FIELD", "status requires entities with task_id/status")
        for payload in items:
            task_id = str(payload.get("task_id") or "").strip()
            status = str(payload.get("status") or "").strip()
            if not task_id or status not in TASK_STATUS_VALUES:
                rejected.append({"code": "INVALID_STATUS_PAYLOAD", "payload": payload})
                continue
            existing_task = await repository.get_task(task_id=task_id, user_id=user_id)
            if not existing_task:
                rejected.append({"code": "TASK_NOT_FOUND", "task_id": task_id})
                continue
            updated = await repository.update_task_status(
                task_id=task_id,
                user_id=user_id,
                status=status,  # type: ignore[arg-type]
            )
            if not updated:
                rejected.append({"code": "TASK_UPDATE_FAILED", "task_id": task_id})
                continue
            applied["status_changed"] += 1
            task_date = _task_date_from_record(updated, timezone_name)
            if status == "done":
                await _remove_task_events_for_task(user_id=user_id, task_id=task_id)
            elif (
                existing_task.status == "done"
                and task_date
                and updated.timebox_end_at
                and _parse_iso(updated.timebox_end_at) >= _utc_now()
            ):
                scheduled_events.extend(
                    await _apply_schedule_rebuild(
                        timezone_name=timezone_name,
                        user_id=user_id,
                        candidate_dates=[task_date],
                    )
                )

    elif intent == "delete":
        task_ids = entities.get("task_ids")
        if not isinstance(task_ids, list):
            raise _task_error("MISSING_FIELD", "delete requires entities.task_ids")
        cleaned_ids = [str(item).strip() for item in task_ids if str(item).strip()]
        for task_id in cleaned_ids:
            await _remove_task_events_for_task(user_id=user_id, task_id=task_id)
        applied["deleted"] = await repository.delete_tasks(user_id=user_id, task_ids=cleaned_ids)

    else:
        raise _task_error("INVALID_INTENT", f"Unsupported intent: {intent}")

    tasks = await repository.list_tasks(user_id=user_id)
    telemetry = {
        "tasks_created": applied["created"],
        "tasks_timeboxed": applied["timeboxed"],
        "tasks_rescheduled": applied["rescheduled"],
        "tasks_deleted": applied["deleted"],
        "tasks_status_changed": applied["status_changed"],
        "tasks_prioritized": applied["prioritized"],
    }
    operation_payload = TaskOperationRecord(
        operation_id=str(uuid.uuid4()),
        user_id=user_id,
        intent=intent,
        session_id=session_id,
        entities=entities,
        options=options,
        apply_result={"applied": applied, "rejected": rejected, "telemetry": telemetry},
        validation_errors=[],
        correlation_id=_new_correlation_id(),
    )
    append_task_operation = getattr(repository, "append_task_operation", None)
    if callable(append_task_operation):
        await append_task_operation(operation_payload)
    return {
        "intent": intent,
        "applied": applied,
        "rejected": rejected,
        "scheduled_events": scheduled_events,
        "tasks": [_task_item_from_record(task).model_dump() for task in tasks],
        "telemetry": telemetry,
    }


async def _execute_task_management(
    *,
    device_id: str,
    timezone_name: str,
    intent: TaskManagementIntent,
    entities: Dict[str, Any],
    options: Dict[str, Any],
    session_id: str | None,
) -> Dict[str, Any]:
    if intent not in TASK_MANAGEMENT_INTENTS:
        raise _task_error("INVALID_INTENT", f"Unsupported intent: {intent}")

    resolved_session_id = session_id or _daily_session_id(device_id, timezone_name)
    await repository.ensure_user(user_id=device_id, timezone_name=timezone_name)
    await _ensure_task_session(
        user_id=device_id,
        session_id=resolved_session_id,
        timezone_name=timezone_name,
    )

    result = await _run_task_management_intent(
        intent=intent,
        entities=entities,
        options=options,
        timezone_name=timezone_name,
        user_id=device_id,
        session_id=resolved_session_id,
    )
    task_state = await _load_task_state_for_user(user_id=device_id, timezone_name=timezone_name)
    task_panel_state = _build_task_panel_state(
        task_state=task_state,
        timezone_name=timezone_name,
        run_status="idle",
        action=intent,
        payload=entities,
        result=result,
    )
    return {
        "session_id": resolved_session_id,
        "task_state": task_state.model_dump(),
        "result": result,
        "task_panel_state": task_panel_state,
    }


async def _execute_task_query(
    *,
    device_id: str,
    timezone_name: str,
    query: TaskQueryType,
    payload: Dict[str, Any],
    session_id: str | None,
) -> Dict[str, Any]:
    if query not in TASK_QUERY_TYPES:
        raise _task_error("INVALID_QUERY", f"Unsupported query: {query}")
    resolved_session_id = session_id or _daily_session_id(device_id, timezone_name)
    await repository.ensure_user(user_id=device_id, timezone_name=timezone_name)
    await _ensure_task_session(
        user_id=device_id,
        session_id=resolved_session_id,
        timezone_name=timezone_name,
    )
    task_state = await _load_task_state_for_user(user_id=device_id, timezone_name=timezone_name)
    if query == "schedule_day":
        raw_date = str(payload.get("date") or "today")
        result: Dict[str, Any] = {
            "query": query,
            "schedule": await _build_schedule_for_user(
                user_id=device_id,
                timezone_name=timezone_name,
                raw_date=raw_date,
            ),
        }
    else:
        result = {
            "query": query,
            "tasks": [_task_item_from_record(task).model_dump() for task in await repository.list_tasks(user_id=device_id)],
        }
    return {
        "session_id": resolved_session_id,
        "task_state": task_state.model_dump(),
        "result": result,
        "task_panel_state": _build_task_panel_state(task_state=task_state, timezone_name=timezone_name),
    }


def _coerce_string(value: Any) -> str | None:
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    return None


def _coerce_number(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def _local_date_key_at(timezone_name: str, at_iso: str | None = None) -> str:
    try:
        zone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return ""
    if at_iso:
        try:
            reference = _parse_iso(at_iso)
        except Exception:
            return ""
    else:
        reference = _utc_now()
    return reference.astimezone(zone).strftime("%Y-%m-%d")


def _daily_session_id_at(user_id: str, timezone_name: str, at_iso: str | None = None) -> str:
    date_key = _local_date_key_at(timezone_name, at_iso)
    if not date_key:
        return ""
    return f"session_{user_id}_{date_key}"


def _next_retry_run_at_iso(attempt_count: int) -> str:
    exponent = max(0, int(attempt_count) - 1)
    delay_seconds = min(RETRY_MAX_SECONDS, RETRY_BASE_SECONDS * (2**exponent))
    return _iso_from_dt(_utc_now() + timedelta(seconds=delay_seconds))


def _format_calendar_reminder_body(payload: Dict[str, Any], timezone_name: str) -> str:
    event_title = _coerce_string(payload.get("event_title")) or "upcoming event"
    raw_start = _coerce_string(payload.get("event_start_time"))
    if not raw_start:
        return f"{event_title} starts soon."
    try:
        start_dt = _parse_iso(raw_start)
    except Exception:
        return f"{event_title} starts soon."
    try:
        local_dt = start_dt.astimezone(ZoneInfo(timezone_name))
    except ZoneInfoNotFoundError:
        return f"{event_title} starts soon."
    return f"{event_title} starts at {local_dt.strftime('%I:%M %p').lstrip('0')}."


def _format_local_clock_label(at_iso: str | None, timezone_name: str | None) -> str | None:
    if not at_iso or not timezone_name:
        return None
    try:
        local_dt = _parse_iso(at_iso).astimezone(ZoneInfo(timezone_name))
    except Exception:
        return None
    return local_dt.strftime("%I:%M %p").lstrip("0")


def _format_task_checkin_notification(
    *,
    payload: Dict[str, Any],
    trigger_type: str,
    timezone_name: str | None,
) -> tuple[str, str]:
    task_title = _coerce_string(payload.get("task_title")) or "your task"
    start_label = _format_local_clock_label(_coerce_string(payload.get("timebox_start")), timezone_name)
    end_label = _format_local_clock_label(_coerce_string(payload.get("timebox_end")), timezone_name)

    if trigger_type == "before_task":
        title = f"Up next: {task_title}"
        body = (
            f"{task_title} starts at {start_label}. Ready to begin?"
            if start_label
            else f"{task_title} starts soon. Ready to begin?"
        )
        return title, body

    if trigger_type == "transition":
        title = f"Next up: {task_title}"
        body = (
            f"Time to switch over. {task_title} starts at {start_label}."
            if start_label
            else f"Time to switch over to {task_title}."
        )
        return title, body

    if trigger_type == "after_task":
        title = f"Wrap up: {task_title}"
        body = (
            f"{task_title} was timeboxed until {end_label}. Ready to wrap up or extend it?"
            if end_label
            else f"{task_title} just reached the end of its timebox. Ready to wrap up or extend it?"
        )
        return title, body

    return "Check-in", f"It is time for your check-in ({trigger_type or 'scheduled'})."


async def _send_push_notification(
    *,
    user_id: str,
    title: str,
    body: str,
    data: Dict[str, Any],
) -> bool:
    push_token = await repository.get_push_token_for_execution(user_id)
    if not push_token:
        return False

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                EXPO_PUSH_URL,
                json={
                    "to": push_token,
                    "title": title,
                    "body": body,
                    "data": data,
                    "sound": "default",
                    "priority": "high",
                },
                headers={"Content-Type": "application/json"},
            )
    except Exception:
        return False

    if response.status_code >= 400:
        return False

    try:
        payload = response.json()
    except Exception:
        return False

    raw_items = payload.get("data")
    if isinstance(raw_items, list):
        items = raw_items
    elif isinstance(raw_items, dict):
        items = [raw_items]
    else:
        items = []

    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("status") != "error":
            continue
        details = item.get("details") if isinstance(item.get("details"), dict) else {}
        if details.get("error") == "DeviceNotRegistered":
            await repository.delete_push_token_for_execution(user_id)
        return False

    return True


async def _unschedule_if_present(task_name: str | None) -> None:
    if not task_name:
        return
    try:
        await repository.unschedule_event_job(task_name)
    except Exception:
        return


async def _resolve_session_timezone_for_event(
    *,
    user_id: str,
    payload: Dict[str, Any],
    calendar_timezone: str | None,
) -> str | None:
    if calendar_timezone:
        return calendar_timezone
    payload_timezone = _coerce_string(payload.get("timezone"))
    if payload_timezone:
        return payload_timezone
    return await repository.get_user_timezone_for_execution(user_id)


async def _ensure_proactive_session_for_event(
    *,
    user_id: str,
    session_id: str,
    timezone_name: str,
    scheduled_time: str | None,
    title: str,
    entry_context: EntryContext,
) -> bool:
    if not session_id:
        return False
    try:
        await repository.ensure_user(user_id=user_id, timezone_name=timezone_name)
        session_state_patch = {
            "title": title,
            "thread_type": "daily",
            "lifecycle_state": "active",
        }
        date_key = _local_date_key_at(timezone_name, scheduled_time)
        if date_key:
            session_state_patch["date_key"] = date_key
        await _ensure_session(
            user_id=user_id,
            session_id=session_id,
            timezone_name=timezone_name,
            entry_context=entry_context,
            state_patch=session_state_patch,
        )
        return True
    except Exception:
        return False


async def _resolve_event_execution_context(
    event: CheckinEventRecord,
) -> ResolvedEventExecutionContext:
    event_id = _coerce_string(event.id) or ""
    event_user_id = _coerce_string(event.user_id) or ""
    event_type = _coerce_string(event.event_type) or "checkin"
    raw_payload = event.payload if isinstance(event.payload, dict) else {}
    event_payload: Dict[str, Any] = dict(raw_payload)

    calendar_timezone: str | None = None
    if event_type == "calendar_reminder":
        calendar_timezone = _coerce_string(event_payload.get("timezone"))
        if not calendar_timezone:
            calendar_timezone = await repository.get_user_timezone_for_execution(event_user_id)
        if calendar_timezone:
            event_payload["timezone"] = calendar_timezone

    session_timezone = await _resolve_session_timezone_for_event(
        user_id=event_user_id,
        payload=event_payload,
        calendar_timezone=calendar_timezone,
    )
    reason = _coerce_string(event_payload.get("reason")) or "scheduled_checkin"
    trigger_type = _coerce_string(event_payload.get("trigger_type")) or event_type
    calendar_event_id = _coerce_string(event_payload.get("calendar_event_id"))
    scheduled_time = _coerce_string(event.scheduled_time)

    title = "Check-in"
    body = "You have a scheduled check-in."
    if event_type == "morning_wake":
        title = "Good morning"
        body = "Ready to plan your day?"
    elif event_type == "calendar_reminder":
        title = f"Upcoming: {_coerce_string(event_payload.get('event_title')) or 'Event'}"
        if calendar_timezone:
            body = _format_calendar_reminder_body(event_payload, calendar_timezone)
        else:
            body = "Reminder unavailable: missing timezone."
    elif event_type == "checkin":
        title, body = _format_task_checkin_notification(
            payload=event_payload,
            trigger_type=trigger_type,
            timezone_name=session_timezone,
        )

    session_id = (
        _daily_session_id_at(event_user_id, session_timezone, scheduled_time)
        if session_timezone
        else ""
    )
    notification_data: Dict[str, Any] = {
        "session_id": session_id,
        "type": event_type,
        "trigger_type": trigger_type,
        "event_id": event_id,
        "user_id": event_user_id,
        "source": "push",
        "entry_mode": "proactive",
        "scheduled_time": scheduled_time,
    }
    task_title = _coerce_string(event_payload.get("task_title"))
    if task_title:
        notification_data["task_title"] = task_title
    timebox_start = _coerce_string(event_payload.get("timebox_start"))
    if timebox_start:
        notification_data["timebox_start"] = timebox_start
    timebox_end = _coerce_string(event_payload.get("timebox_end"))
    if timebox_end:
        notification_data["timebox_end"] = timebox_end
    if calendar_event_id:
        notification_data["calendar_event_id"] = calendar_event_id

    attempted_at = _iso_now()
    existing_attempt_count = max(0, _coerce_number(event.attempt_count) or 0)

    return ResolvedEventExecutionContext(
        event=event,
        event_id=event_id,
        event_type=event_type,
        event_user_id=event_user_id,
        event_payload=event_payload,
        calendar_timezone=calendar_timezone,
        session_timezone=session_timezone,
        session_id=session_id,
        trigger_type=trigger_type,
        calendar_event_id=calendar_event_id,
        title=title,
        body=body,
        notification_data=notification_data,
        attempted_at=attempted_at,
        next_attempt_count=existing_attempt_count + 1,
    )


async def _attempt_event_delivery(
    context: ResolvedEventExecutionContext,
) -> DeliveryAttemptResult:
    push_sent = False
    session_ready = False
    if context.session_timezone and context.session_id:
        if not (context.event_type == "calendar_reminder" and not context.calendar_timezone):
            session_ready = await _ensure_proactive_session_for_event(
                user_id=context.event_user_id,
                session_id=context.session_id,
                timezone_name=context.session_timezone,
                scheduled_time=_coerce_string(context.event.scheduled_time),
                title=context.title,
                entry_context=EntryContext(
                    source="push",
                    event_id=context.event_id,
                    trigger_type=context.trigger_type,
                    scheduled_time=_coerce_string(context.event.scheduled_time),
                    calendar_event_id=context.calendar_event_id,
                    entry_mode="proactive",
                ),
            )
            if session_ready:
                push_sent = await _send_push_notification(
                    user_id=context.event_user_id,
                    title=context.title,
                    body=context.body,
                    data=context.notification_data,
                )

    last_error: str | None = None if push_sent else "push_failed_or_missing_token"
    if not context.session_timezone or not context.session_id:
        last_error = "missing_timezone"
    elif not session_ready:
        last_error = "session_persist_failed"
    elif context.event_type == "calendar_reminder" and not context.calendar_timezone:
        last_error = "missing_timezone"

    delivery_succeeded = bool(
        push_sent and session_ready and context.session_timezone and context.session_id
    )

    next_morning_wake: Dict[str, Any] | None = None
    continuation_failed = False
    if context.event_type == "morning_wake":
        next_morning_wake = await repository.ensure_next_morning_wake_event(context.event_id)
        continuation_failed = str(next_morning_wake.get("status") or "").strip().lower() != "ok"
        if continuation_failed and not push_sent:
            last_error = "morning_wake_continuation_failed"

    return DeliveryAttemptResult(
        push_sent=push_sent,
        session_ready=session_ready,
        delivery_succeeded=delivery_succeeded,
        last_error=last_error,
        next_morning_wake=next_morning_wake,
        continuation_failed=continuation_failed,
    )


async def _write_event_attempt_state(
    *,
    event_id: str,
    attempt_count: int,
    attempted_at: str,
    last_error: str | None,
    next_retry_at: str | None,
    workflow_state: Literal["succeeded", "retry_scheduled", "dead_letter"],
) -> None:
    await repository.update_event_execution_state(
        event_id=event_id,
        attempt_count=attempt_count,
        attempted_at=attempted_at,
        last_error=last_error,
        next_retry_at=next_retry_at,
        workflow_state=workflow_state,
    )


async def _finalize_event_execution_attempt(
    context: ResolvedEventExecutionContext,
    attempt: DeliveryAttemptResult,
) -> tuple[int, Dict[str, Any]]:
    if attempt.delivery_succeeded:
        finalized = await repository.finalize_event_execution(
            event_id=context.event_id,
            last_error="morning_wake_continuation_failed" if attempt.continuation_failed else None,
            attempted_at=context.attempted_at,
            executed=True,
            attempt_count=context.next_attempt_count,
        )
        if not finalized:
            raise RuntimeError("finalize_event_execution failed")
        await _unschedule_if_present(context.event.cloud_task_name)
        await _write_event_attempt_state(
            event_id=context.event_id,
            attempt_count=context.next_attempt_count,
            attempted_at=context.attempted_at,
            last_error="morning_wake_continuation_failed" if attempt.continuation_failed else None,
            next_retry_at=None,
            workflow_state="succeeded",
        )
        if context.event_type == "morning_wake" and bool(context.event_payload.get("onboarding_unlock")):
            existing_profile = await repository.get_user_profile(user_id=context.event_user_id)
            wake_time = str((existing_profile or {}).get("wake_time") or "").strip() or None
            bedtime = str((existing_profile or {}).get("bedtime") or "").strip() or None
            timezone_name = (
                context.session_timezone
                or _timezone_from_user_profile(existing_profile)
                or "UTC"
            )
            user_profile = await repository.upsert_user_profile(
                user_id=context.event_user_id,
                timezone_name=timezone_name,
                wake_time=wake_time,
                bedtime=bedtime,
                onboarding_status=ONBOARDING_STATUS_COMPLETED,
            )
            profile_context = _build_profile_context(user_profile)
            await _ensure_session(
                user_id=context.event_user_id,
                session_id=context.session_id,
                timezone_name=timezone_name,
                entry_context=EntryContext(
                    source="push",
                    event_id=context.event_id,
                    trigger_type=context.trigger_type,
                    scheduled_time=_coerce_string(context.event.scheduled_time),
                    calendar_event_id=context.calendar_event_id,
                    entry_mode="proactive",
                ),
                state_patch={
                    "profile_context": profile_context,
                    "lifecycle_state": "active",
                },
            )
        return (
            200,
            {
                "status": "executed",
                "event_id": context.event_id,
                "event_type": context.event_type,
                "attempt_count": context.next_attempt_count,
                "reason": "delivery_completed",
                "push_sent": attempt.push_sent,
                "next_morning_wake": attempt.next_morning_wake,
                "continuation_failed": attempt.continuation_failed,
            },
        )

    if context.next_attempt_count >= MAX_DELIVERY_ATTEMPTS:
        finalized = await repository.finalize_event_execution(
            event_id=context.event_id,
            last_error=attempt.last_error,
            attempted_at=context.attempted_at,
            executed=False,
            attempt_count=context.next_attempt_count,
        )
        if not finalized:
            raise RuntimeError("finalize_event_execution failed")
        await _unschedule_if_present(context.event.cloud_task_name)
        await _write_event_attempt_state(
            event_id=context.event_id,
            attempt_count=context.next_attempt_count,
            attempted_at=context.attempted_at,
            last_error=attempt.last_error,
            next_retry_at=None,
            workflow_state="dead_letter",
        )
        return (
            200,
            {
                "status": "dead_letter",
                "event_id": context.event_id,
                "event_type": context.event_type,
                "attempt_count": context.next_attempt_count,
                "reason": "max_delivery_attempts_reached",
                "error": attempt.last_error,
            },
        )

    retry_at = _next_retry_run_at_iso(context.next_attempt_count)
    retry_error = attempt.last_error or "push_failed_or_missing_token"
    await repository.schedule_event_retry(
        event_id=context.event_id,
        next_run_at=retry_at,
        timezone_name=context.session_timezone or "UTC",
        last_error=retry_error,
        attempted_at=context.attempted_at,
    )
    await _write_event_attempt_state(
        event_id=context.event_id,
        attempt_count=context.next_attempt_count,
        attempted_at=context.attempted_at,
        last_error=retry_error,
        next_retry_at=retry_at,
        workflow_state="retry_scheduled",
    )
    return (
        202,
        {
            "status": "retry_scheduled",
            "event_id": context.event_id,
            "event_type": context.event_type,
            "attempt_count": context.next_attempt_count,
            "reason": "delivery_failed_retry_scheduled",
            "retry_at": retry_at,
            "error": retry_error,
        },
    )


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
        "release_id": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
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
        raise _http_error(
            503,
            "ADK_UNAVAILABLE",
            f"Google ADK unavailable (model={model_name}): {error}",
        ) from error
    return {"output": output}


@app.post("/agent/bootstrap-device")
async def bootstrap_device(payload: LegacyBootstrapRequest) -> Dict[str, Any]:
    device_id = payload.device_id.strip()
    if not device_id:
        raise _http_error(400, "INVALID_DEVICE_ID", "device_id is required")

    entry_context = payload.entry_context or EntryContext()
    source = entry_context.source or "manual"
    session_payload = SessionOpenRequest(
        device_id=device_id,
        timezone=payload.timezone,
        session_id=payload.session_id,
        entry_context=entry_context,
        source=source,
        open_id=_new_open_id(),
        client_version="legacy-bootstrap",
        contract_version=CONTRACT_VERSION,
    )
    opened = await open_session(session_payload)
    timezone_name = await _resolve_timezone_for_user(
        user_id=device_id,
        provided_timezone=payload.timezone,
    )
    _log_event(
        logging.WARNING,
        "Legacy bootstrap route used",
        device_id=device_id,
        session_id=opened["session_id"],
        timezone=timezone_name,
        source=source,
        entry_mode=entry_context.entry_mode,
        trigger_type=entry_context.trigger_type or "",
    )
    return {
        "device_id": device_id,
        "user_id": device_id,
        "timezone": timezone_name,
        "session_id": opened["session_id"],
        "threads": [],
        "messages": opened["messages"],
        "needs_onboarding": opened["needs_onboarding"],
        "profile_context": opened["profile_context"],
    }


@app.post("/agent/session/open")
async def open_session(payload: SessionOpenRequest) -> Dict[str, Any]:
    device_id = payload.device_id.strip()
    if not device_id:
        raise _http_error(400, "INVALID_DEVICE_ID", "device_id is required")

    correlation_id = _new_correlation_id()
    contract = _resolve_session_open_contract(payload)
    requested_entry_context = payload.entry_context or EntryContext(source=payload.source, entry_mode="reactive")
    _log_event(
        logging.INFO,
        "Session open requested",
        device_id=device_id,
        session_id=payload.session_id or "",
        timezone=payload.timezone or "",
        correlation_id=correlation_id,
        open_id=contract.open_id,
        source=payload.source,
        client_version=contract.client_version,
        contract_version=contract.contract_version,
        entry_mode=requested_entry_context.entry_mode,
        trigger_type=requested_entry_context.trigger_type or "",
        event_id=requested_entry_context.event_id or "",
    )
    _enforce_session_open_contract(contract)
    timezone_name = await _resolve_timezone_for_user(
        user_id=device_id,
        provided_timezone=payload.timezone,
    )
    base_entry_context = payload.entry_context or EntryContext(source=payload.source, entry_mode="reactive")
    entry_context = base_entry_context.model_copy(
        update={"source": payload.source or base_entry_context.source}
    )
    session_id = payload.session_id or _daily_session_id(device_id, timezone_name)

    prepared = await _prepare_session_state(
        user_id=device_id,
        session_id=session_id,
        timezone_name=timezone_name,
        entry_context=entry_context,
        persist_event_state=True,
    )

    startup_status: StartupWorkflowState = "failed"
    startup_message_id: str | None = None
    startup_generated = False
    morning_seed: Dict[str, Any] | None = None
    onboarding_status = _normalize_onboarding_status(prepared.profile_context.get("onboarding_status"))
    if not prepared.needs_onboarding:
        wake_time = str(prepared.profile_context.get("wake_time") or "").strip()
        if onboarding_status == ONBOARDING_STATUS_COMPLETED and wake_time:
            try:
                morning_seed = await _seed_next_morning_wake_event(
                    user_id=device_id,
                    timezone_name=timezone_name,
                    wake_time=wake_time,
                )
            except Exception as error:
                _log_event(
                    logging.WARNING,
                    "Morning wake seed skipped during session open",
                    device_id=device_id,
                    timezone=timezone_name,
                    error=str(error),
                )
        startup_status, startup_message_id, startup_generated = await _ensure_startup_message_for_session(
            user_id=device_id,
            session_id=session_id,
            open_id=contract.open_id,
            timezone_name=timezone_name,
            entry_context=entry_context,
            profile_context=prepared.profile_context,
            source=payload.source,
            missed_proactive_events=prepared.missed_proactive_events,
        )
        surfaced_ids = [
            str(item.get("event_id") or "").strip()
            for item in prepared.missed_proactive_events
            if str(item.get("event_id") or "").strip()
        ]
        if startup_status == "succeeded" and startup_generated and surfaced_ids:
            latest_missed_reported_ids = list(prepared.missed_reported_ids)
            for surfaced_id in surfaced_ids:
                latest_missed_reported_ids = _append_event_id(latest_missed_reported_ids, surfaced_id)
            await _ensure_session(
                user_id=device_id,
                session_id=session_id,
                timezone_name=timezone_name,
                entry_context=entry_context,
                state_patch={MISSED_REPORTED_STATE_KEY: latest_missed_reported_ids},
            )

    messages = list(prepared.messages)
    if startup_generated:
        messages = await repository.list_messages(session_id=session_id)
    _log_event(
        logging.INFO,
        "Session open completed",
        device_id=device_id,
        session_id=session_id,
        timezone=timezone_name,
        correlation_id=correlation_id,
        open_id=contract.open_id,
        client_version=contract.client_version,
        contract_version=contract.contract_version,
        startup_status=startup_status,
        startup_message_id=startup_message_id,
        startup_generated=startup_generated,
        morning_seeded=morning_seed.get("seeded") if morning_seed else None,
        morning_seed_reason=morning_seed.get("reason") if morning_seed else None,
        message_count=len(messages),
        needs_onboarding=prepared.needs_onboarding,
    )
    return {
        "session_id": session_id,
        "startup_status": startup_status,
        "messages": [message.model_dump() for message in messages],
        "needs_onboarding": prepared.needs_onboarding,
        "profile_context": prepared.profile_context,
        "task_panel_state": prepared.task_panel_state,
        "release_id": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
    }


@app.post("/agent/events/execute")
async def execute_event(payload: EventExecuteRequest, request: Request) -> JSONResponse:
    expected_secret = await repository.get_scheduler_secret_for_execution()
    provided_secret = _coerce_string(request.headers.get(SCHEDULER_SECRET_HEADER))
    if not expected_secret or provided_secret != expected_secret:
        raise _http_error(401, "UNAUTHORIZED", "Scheduler authentication failed")

    event_id = payload.event_id.strip()
    event = await repository.get_event_for_execution(event_id)
    if not event:
        raise _http_error(404, "EVENT_NOT_FOUND", "Event not found")
    if event.executed:
        await _unschedule_if_present(event.cloud_task_name)
        return JSONResponse(
            status_code=200,
            content={
                "status": "executed",
                "event_id": event_id,
                "reason": "already_executed",
            },
        )

    event_user_id = _coerce_string(event.user_id)
    if not event_user_id:
        raise _http_error(500, "EVENT_INVALID", "Event missing user_id")

    try:
        context = await _resolve_event_execution_context(event)
        attempt = await _attempt_event_delivery(context)
        status_code, response_payload = await _finalize_event_execution_attempt(context, attempt)
        _log_event(
            logging.INFO,
            "Scheduled event executed",
            event_id=context.event_id,
            user_id=context.event_user_id,
            status=response_payload.get("status"),
            attempt_count=response_payload.get("attempt_count"),
            reason=response_payload.get("reason"),
        )
        return JSONResponse(status_code=status_code, content=response_payload)
    except HTTPException:
        raise
    except Exception as error:
        _log_event(
            logging.ERROR,
            "Scheduled event execution failed",
            event_id=event_id,
            error=str(error),
        )
        raise _http_error(500, "EVENT_EXECUTION_FAILED", str(error)) from error


@app.post("/agent/push-token")
async def register_push_token(payload: PushTokenRequest) -> Dict[str, Any]:
    device_id = payload.device_id.strip()
    token = payload.expo_push_token.strip()
    if not device_id or not token:
        raise _http_error(
            400,
            "INVALID_PUSH_TOKEN_REQUEST",
            "device_id and expo_push_token are required",
        )

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


async def _set_onboarding_started(
    *,
    device_id: str,
    timezone_name: str,
) -> Dict[str, Any]:
    await repository.ensure_user(user_id=device_id, timezone_name=timezone_name)
    existing_profile = await repository.get_user_profile(user_id=device_id)
    existing_status = _normalize_onboarding_status((existing_profile or {}).get("onboarding_status"))
    next_status = (
        ONBOARDING_STATUS_IN_PROGRESS
        if existing_status == ONBOARDING_STATUS_PENDING
        else existing_status
    )
    wake_time = str((existing_profile or {}).get("wake_time") or "").strip() or None
    bedtime = str((existing_profile or {}).get("bedtime") or "").strip() or None
    user_profile = await repository.upsert_user_profile(
        user_id=device_id,
        timezone_name=timezone_name,
        wake_time=wake_time,
        bedtime=bedtime,
        onboarding_status=next_status,
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
            "lifecycle_state": "onboarding_active"
            if _onboarding_agent_enabled(profile_context)
            else "active",
        },
    )
    return {
        "status": "ok",
        "device_id": device_id,
        "timezone": timezone_name,
        "session_id": daily_session_id,
        "needs_onboarding": _needs_onboarding(user_profile),
        "profile_context": profile_context,
    }


async def _set_onboarding_sleep_schedule(
    *,
    device_id: str,
    timezone_name: str,
    wake_time: str,
    bedtime: str,
    session_id: str | None = None,
) -> Dict[str, Any]:
    normalized_wake = wake_time.strip()
    normalized_bedtime = bedtime.strip()
    if not _is_valid_hhmm(normalized_wake):
        raise _http_error(400, "INVALID_WAKE_TIME", "wake_time must be HH:MM (24h)")
    if not _is_valid_hhmm(normalized_bedtime):
        raise _http_error(400, "INVALID_BEDTIME", "bedtime must be HH:MM (24h)")
    await repository.ensure_user(user_id=device_id, timezone_name=timezone_name)
    user_profile = await repository.upsert_user_profile(
        user_id=device_id,
        timezone_name=timezone_name,
        wake_time=normalized_wake,
        bedtime=normalized_bedtime,
        onboarding_status=ONBOARDING_STATUS_READY_FOR_MAIN,
    )
    profile_context = _build_profile_context(user_profile)
    target_session_id = session_id or _daily_session_id(device_id, timezone_name)
    await _ensure_session(
        user_id=device_id,
        session_id=target_session_id,
        timezone_name=timezone_name,
        entry_context=EntryContext(source="manual", entry_mode="reactive"),
        state_patch={
            "thread_type": "daily",
            "profile_context": profile_context,
            "lifecycle_state": "onboarding_active",
        },
    )
    morning_seed = await _seed_next_morning_wake_event(
        user_id=device_id,
        timezone_name=timezone_name,
        wake_time=normalized_wake,
        reason="onboarding_unlock",
        wake_purpose=WAKE_PURPOSE_ONBOARDING_UNLOCK,
        force_tomorrow=True,
    )
    _log_event(
        logging.INFO,
        "Onboarding sleep schedule saved",
        device_id=device_id,
        session_id=target_session_id,
        timezone=timezone_name,
        status=ONBOARDING_STATUS_READY_FOR_MAIN,
        seeded=morning_seed.get("seeded"),
        reason=morning_seed.get("reason"),
        event_id=morning_seed.get("event_id"),
        scheduled_time=morning_seed.get("scheduled_time"),
    )
    return {
        "status": "ok",
        "device_id": device_id,
        "timezone": timezone_name,
        "session_id": target_session_id,
        "needs_onboarding": False,
        "profile_context": profile_context,
        "morning_seed": morning_seed,
    }


@app.post("/agent/onboarding/start")
async def onboarding_start(payload: OnboardingStartRequest) -> Dict[str, Any]:
    device_id = payload.device_id.strip()
    if not device_id:
        raise _http_error(400, "INVALID_DEVICE_ID", "device_id is required")
    timezone_name = await _resolve_timezone_for_user(
        user_id=device_id,
        provided_timezone=payload.timezone,
    )
    output = await _set_onboarding_started(device_id=device_id, timezone_name=timezone_name)
    _log_event(
        logging.INFO,
        "Onboarding started",
        device_id=device_id,
        session_id=output.get("session_id"),
        timezone=timezone_name,
    )
    return output


@app.post("/agent/onboarding/sleep-schedule")
async def onboarding_sleep_schedule(payload: OnboardingSleepScheduleRequest) -> Dict[str, Any]:
    device_id = payload.device_id.strip()
    if not device_id:
        raise _http_error(400, "INVALID_DEVICE_ID", "device_id is required")
    timezone_name = await _resolve_timezone_for_user(
        user_id=device_id,
        provided_timezone=payload.timezone,
    )
    return await _set_onboarding_sleep_schedule(
        device_id=device_id,
        timezone_name=timezone_name,
        wake_time=payload.wake_time,
        bedtime=payload.bedtime,
    )


@app.post("/agent/task-management")
async def task_management(payload: TaskManagementRequest) -> Dict[str, Any]:
    if not TASK_MGMT_V1_ENABLED:
        raise _http_error(404, "TASK_MGMT_DISABLED", "Task management v1 disabled")

    device_id = payload.device_id.strip()
    if not device_id:
        raise _http_error(400, "INVALID_DEVICE_ID", "device_id is required")

    timezone_name = await _resolve_timezone_for_user(
        user_id=device_id,
        provided_timezone=payload.timezone,
    )
    try:
        output = await _execute_task_management(
            device_id=device_id,
            timezone_name=timezone_name,
            intent=payload.intent,
            entities=payload.entities,
            options=payload.options,
            session_id=payload.session_id,
        )
        await _broadcast_task_panel(device_id, output["session_id"], output["task_panel_state"])
        _log_event(
            logging.INFO,
            "Task management intent completed",
            device_id=device_id,
            session_id=output["session_id"],
            timezone=timezone_name,
            intent=payload.intent,
            task_written=payload.intent in TASK_WRITE_INTENTS,
        )
        return output
    except HTTPException as error:
        error_code = _task_error_code_from_http_error(error)
        error_message = _task_error_message_from_http_error(error)
        _log_event(
            logging.WARNING,
            "Task management intent rejected",
            device_id=device_id,
            session_id=payload.session_id,
            timezone=timezone_name,
            intent=payload.intent,
            error_code=error_code,
            status_code=error.status_code,
            detail=error_message,
        )
        raise
    except Exception as error:
        logger.exception(
            "Task management intent failed",
            extra={
                "device_id": device_id,
                "session_id": payload.session_id,
                "timezone": timezone_name,
                "intent": payload.intent,
                "error": str(error),
            },
        )
        raise _http_error(500, "TASK_MANAGEMENT_FAILED", f"Task management failed: {error}") from error


@app.post("/agent/task-query")
async def task_query(payload: TaskQueryRequest) -> Dict[str, Any]:
    if not TASK_MGMT_V1_ENABLED:
        raise _http_error(404, "TASK_MGMT_DISABLED", "Task management v1 disabled")
    device_id = payload.device_id.strip()
    if not device_id:
        raise _http_error(400, "INVALID_DEVICE_ID", "device_id is required")
    timezone_name = await _resolve_timezone_for_user(
        user_id=device_id,
        provided_timezone=payload.timezone,
    )
    output = await _execute_task_query(
        device_id=device_id,
        timezone_name=timezone_name,
        query=payload.query,
        payload=payload.payload,
        session_id=payload.session_id,
    )
    await _broadcast_task_panel(device_id, output["session_id"], output["task_panel_state"])
    _log_event(
        logging.INFO,
        "Task query completed",
        device_id=device_id,
        session_id=output["session_id"],
        timezone=timezone_name,
        query=payload.query,
    )
    return output


@app.websocket("/agent/ws")
async def agent_ws(
    websocket: WebSocket,
    device_id: str = Query(...),
    timezone: str | None = Query(None),
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
        detail = _error_envelope(status_code=error.status_code, detail=error.detail)["error"]["message"]
        await _safe_send_json({"type": "error", "code": "invalid_timezone", "detail": detail})
        try:
            await websocket.close()
        except Exception:
            pass
        return
    active_user_id = device_id
    active_session_id: str | None = None
    unsubscribe_task_panel = None
    initialized = False
    latest_entry_context = EntryContext()
    latest_missed_proactive_events: List[Dict[str, Any]] = []
    latest_profile_context: Dict[str, Any] = {}
    ws_correlation_id = _new_correlation_id()

    _log_event(
        logging.INFO,
        "WebSocket connected",
        device_id=active_user_id,
        session_id="",
        timezone=timezone_name,
        correlation_id=ws_correlation_id,
    )

    async def _build_runtime_context() -> Dict[str, str]:
        if not active_session_id:
            raise RuntimeError("Session not initialized")
        return await _build_runtime_context_for_session(
            user_id=active_user_id,
            session_id=active_session_id,
            timezone_name=timezone_name,
            entry_context=latest_entry_context,
            missed_proactive_events=latest_missed_proactive_events,
        )

    async def _run_assistant_turn(
        *,
        prompt: str,
        metadata: Dict[str, Any] | None = None,
        user_task_intent: bool = False,
    ) -> bool:
        if not active_session_id:
            return False
        assistant_message_id = str(uuid.uuid4())
        context_map = await _build_runtime_context()
        use_onboarding_agent = _onboarding_agent_enabled(latest_profile_context)
        selected_agent = onboarding_agent if use_onboarding_agent else agent

        cumulative = ""
        try:
            async for delta in selected_agent.run_stream(
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
        assistant_metadata["agent_mode"] = "onboarding" if use_onboarding_agent else "main"
        startup_turn = bool(assistant_metadata.get("startup_turn"))
        task_write_count = 0
        consume_write_count = getattr(selected_agent, "consume_task_write_count", None)
        if callable(consume_write_count):
            try:
                task_write_count = int(
                    consume_write_count(user_id=active_user_id, session_id=active_session_id)
                )
            except Exception:
                task_write_count = 0
        task_written = task_write_count > 0
        task_action_counts: Dict[str, int] = {}
        consume_action_counts = getattr(selected_agent, "consume_task_action_counts", None)
        if callable(consume_action_counts):
            try:
                raw_counts = consume_action_counts(user_id=active_user_id, session_id=active_session_id)
                if isinstance(raw_counts, dict):
                    task_action_counts = {
                        str(key): int(value) for key, value in raw_counts.items() if isinstance(value, int)
                    }
            except Exception:
                task_action_counts = {}
        assistant_claimed_task_action = _looks_like_task_claim(assistant_text)
        assistant_claimed_schedule = _looks_like_schedule_claim(assistant_text)
        schedule_write_count = int(task_action_counts.get("timebox", 0)) + int(
            task_action_counts.get("reschedule", 0)
        )

        if assistant_claimed_schedule and schedule_write_count <= 0 and not startup_turn:
            assistant_text = (
                "I could not safely schedule that yet. "
                "Please share an explicit time window and I will set it now."
            )
        elif (user_task_intent or assistant_claimed_task_action) and not task_written and not startup_turn:
            assistant_text = (
                "I could not safely apply that task change yet. "
                "Please confirm the task details and I will retry."
            )

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

        if assistant_claimed_schedule and schedule_write_count <= 0 and not startup_turn:
            _log_event(
                logging.WARNING,
                "Schedule claim detected without schedule write",
                device_id=active_user_id,
                session_id=active_session_id,
                schedule_write_count=schedule_write_count,
                task_action_counts=task_action_counts,
            )
        elif (user_task_intent or assistant_claimed_task_action) and not task_written and not startup_turn:
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
                task_action_counts=task_action_counts,
            )

        return await _safe_send_json(
            {
                "type": "assistant_done",
                "message_id": assistant_message_id,
                "text": assistant_text,
                "created_at": assistant_message.created_at,
                "cursor": _message_cursor_for(assistant_message),
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
                next_session_id = str(frame.get("session_id") or "").strip()
                if not next_session_id:
                    if not await _safe_send_json(
                        {
                            "type": "error",
                            "code": "invalid_init",
                            "detail": "session_id is required",
                        }
                    ):
                        break
                    continue
                active_session_id = next_session_id
                try:
                    existing_session = await repository.get_session_by_id(session_id=active_session_id)
                    if existing_session and existing_session.user_id != active_user_id:
                        raise _http_error(403, "SESSION_FORBIDDEN", "Session does not belong to device")
                    session_entry_context = _entry_context_from_state(
                        existing_session.state if existing_session else {}
                    )
                    prepared = await _prepare_session_state(
                        user_id=active_user_id,
                        session_id=active_session_id,
                        timezone_name=timezone_name,
                        entry_context=session_entry_context,
                        persist_event_state=False,
                    )
                except HTTPException as error:
                    detail = _error_envelope(status_code=error.status_code, detail=error.detail)["error"][
                        "message"
                    ]
                    code = "session_forbidden" if error.status_code == 403 else "session_init_failed"
                    _log_event(
                        logging.WARNING,
                        "WebSocket session init rejected",
                        device_id=active_user_id,
                        session_id=active_session_id,
                        correlation_id=ws_correlation_id,
                        status_code=error.status_code,
                        error_code=code,
                        error_message=detail,
                    )
                    if not await _safe_send_json({"type": "error", "code": code, "detail": detail}):
                        break
                    continue
                latest_entry_context = prepared.entry_context
                latest_missed_proactive_events = prepared.missed_proactive_events
                latest_profile_context = prepared.profile_context
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
                    }
                ):
                    break
                if not await _safe_send_json(
                    {
                        "type": "task_panel_state",
                        "state": prepared.task_panel_state,
                    }
                ):
                    break
                initialized = True
                _log_event(
                    logging.INFO,
                    "WebSocket session initialized",
                    device_id=active_user_id,
                    session_id=active_session_id,
                    correlation_id=ws_correlation_id,
                    ws_init=True,
                    message_count=len(prepared.messages),
                    entry_mode=latest_entry_context.entry_mode,
                    trigger_type=latest_entry_context.trigger_type or "",
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
