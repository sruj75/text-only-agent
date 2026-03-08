from typing import Literal

TaskManagementAction = Literal[
    "capture_tasks",
    "get_tasks",
    "set_top_essentials",
    "timebox_task",
    "get_schedule",
    "update_task_status",
]

TASK_MANAGEMENT_ACTIONS: tuple[TaskManagementAction, ...] = (
    "capture_tasks",
    "get_tasks",
    "set_top_essentials",
    "timebox_task",
    "get_schedule",
    "update_task_status",
)

TASK_ACTION_LABELS: dict[TaskManagementAction, str] = {
    "capture_tasks": "Capturing tasks",
    "get_tasks": "Refreshing tasks",
    "set_top_essentials": "Updating top essentials",
    "timebox_task": "Updating schedule",
    "get_schedule": "Loading schedule",
    "update_task_status": "Updating task status",
}

TASK_WRITE_ACTIONS: frozenset[TaskManagementAction] = frozenset(
    {
        "capture_tasks",
        "set_top_essentials",
        "timebox_task",
        "update_task_status",
    }
)

TASK_STATUS_VALUES: frozenset[str] = frozenset({"todo", "in_progress", "done", "blocked"})
