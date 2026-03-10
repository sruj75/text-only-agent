from typing import Literal

TaskManagementIntent = Literal[
    "capture",
    "timebox",
    "prioritize",
    "status",
    "delete",
    "reschedule",
]

TaskQueryType = Literal[
    "tasks_overview",
    "schedule_day",
]

TASK_MANAGEMENT_INTENTS: tuple[TaskManagementIntent, ...] = (
    "capture",
    "timebox",
    "prioritize",
    "status",
    "delete",
    "reschedule",
)

TASK_QUERY_TYPES: tuple[TaskQueryType, ...] = (
    "tasks_overview",
    "schedule_day",
)

TASK_INTENT_LABELS: dict[TaskManagementIntent, str] = {
    "capture": "Capturing tasks",
    "timebox": "Scheduling tasks",
    "prioritize": "Updating priorities",
    "status": "Updating task status",
    "delete": "Deleting tasks",
    "reschedule": "Rescheduling tasks",
}

TASK_WRITE_INTENTS: frozenset[TaskManagementIntent] = frozenset(
    {
        "capture",
        "timebox",
        "prioritize",
        "status",
        "delete",
        "reschedule",
    }
)

TASK_STATUS_VALUES: frozenset[str] = frozenset({"todo", "in_progress", "done", "blocked"})
