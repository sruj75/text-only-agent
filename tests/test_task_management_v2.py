from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import main


def test_task_management_v2_capture_status_and_query(app_client):
    client = app_client["client"]

    captured = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-v2-1",
            "timezone": "UTC",
            "intent": "capture",
            "entities": {"tasks": [{"title": "Write design brief"}]},
            "options": {},
        },
    )
    assert captured.status_code == 200
    assert captured.json()["result"]["applied"]["created"] == 1

    task_id = captured.json()["result"]["tasks"][0]["task_id"]
    status_update = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-v2-1",
            "timezone": "UTC",
            "intent": "status",
            "entities": {"updates": [{"task_id": task_id, "status": "done"}]},
            "options": {},
        },
    )
    assert status_update.status_code == 200
    assert status_update.json()["result"]["applied"]["status_changed"] == 1

    queried = client.post(
        "/agent/task-query",
        json={
            "device_id": "device-v2-1",
            "timezone": "UTC",
            "query": "tasks_overview",
            "payload": {"scope": "today"},
        },
    )
    assert queried.status_code == 200
    assert queried.json()["result"]["query"] == "tasks_overview"
    assert queried.json()["result"]["tasks"][0]["status"] == "done"


def test_task_management_v2_capture_with_explicit_timeboxes_schedules_events(app_client):
    client = app_client["client"]

    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = now + timedelta(minutes=20)
    end = start + timedelta(minutes=30)

    captured = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-v2-2",
            "timezone": "UTC",
            "intent": "capture",
            "entities": {
                "tasks": [
                    {
                        "title": "Take dog for walk",
                        "start_at": start.isoformat().replace("+00:00", "Z"),
                        "end_at": end.isoformat().replace("+00:00", "Z"),
                    }
                ]
            },
            "options": {},
        },
    )
    assert captured.status_code == 200
    result = captured.json()["result"]
    assert result["applied"]["created"] == 1
    assert result["applied"]["timeboxed"] == 1
    assert len(result["scheduled_events"]) == 2

    scheduled = client.post(
        "/agent/task-query",
        json={
            "device_id": "device-v2-2",
            "timezone": "UTC",
            "query": "schedule_day",
            "payload": {"date": "today"},
        },
    )
    assert scheduled.status_code == 200
    assert scheduled.json()["result"]["query"] == "schedule_day"
    assert len(scheduled.json()["result"]["schedule"]["items"]) == 1


def test_task_management_v2_capture_keeps_timeboxes_aligned_with_valid_titles(app_client):
    client = app_client["client"]
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    first_start = now + timedelta(minutes=20)
    first_end = first_start + timedelta(minutes=15)
    second_start = now + timedelta(minutes=90)
    second_end = second_start + timedelta(minutes=20)

    captured = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-v2-alignment",
            "timezone": "UTC",
            "intent": "capture",
            "entities": {
                "tasks": [
                    {
                        "title": "   ",
                        "start_at": first_start.isoformat().replace("+00:00", "Z"),
                        "end_at": first_end.isoformat().replace("+00:00", "Z"),
                    },
                    {"title": "Task without timebox"},
                    {
                        "title": "Task with timebox",
                        "start_at": second_start.isoformat().replace("+00:00", "Z"),
                        "end_at": second_end.isoformat().replace("+00:00", "Z"),
                    },
                ]
            },
            "options": {},
        },
    )
    assert captured.status_code == 200
    result = captured.json()["result"]
    assert result["applied"]["created"] == 2
    assert result["applied"]["timeboxed"] == 1

    queried = client.post(
        "/agent/task-query",
        json={
            "device_id": "device-v2-alignment",
            "timezone": "UTC",
            "query": "tasks_overview",
            "payload": {"scope": "today"},
        },
    )
    assert queried.status_code == 200
    tasks = {item["title"]: item for item in queried.json()["result"]["tasks"]}
    assert tasks["Task without timebox"]["timebox"] is None
    assert tasks["Task with timebox"]["timebox"] is not None
    assert tasks["Task with timebox"]["timebox"]["start_at"] == second_start.isoformat().replace("+00:00", "Z")
    assert tasks["Task with timebox"]["timebox"]["end_at"] == second_end.isoformat().replace("+00:00", "Z")


def test_supabase_task_timebox_listing_paginates_past_500_rows():
    repository = main.SupabaseRepository(
        project_url="https://example.supabase.co",
        service_role_key="service-role-key",
    )
    item_offsets: list[str] = []
    timebox_offsets: list[str] = []

    async def fake_request(
        method: str,
        path: str,
        *,
        params=None,
        json_body=None,
        extra_headers=None,
    ):
        assert method == "GET"
        assert json_body is None
        assert extra_headers is None
        total_rows = 501
        page_size = int(params["limit"])
        offset = int(params["offset"])
        remaining = max(total_rows - offset, 0)
        count = min(page_size, remaining)

        if path == "task_items":
            item_offsets.append(params["offset"])
            return [
                {
                    "task_id": f"task-{offset + index:03d}",
                    "user_id": "device-timebox-paged",
                    "title": f"Task {offset + index:03d}",
                    "status": "todo",
                    "priority_rank": None,
                    "is_essential": False,
                    "created_at": f"2026-03-07T00:{(offset + index) // 60:02d}:{(offset + index) % 60:02d}Z",
                    "updated_at": f"2026-03-07T00:{(offset + index) // 60:02d}:{(offset + index) % 60:02d}Z",
                }
                for index in range(count)
            ]
        assert path == "task_timeboxes"
        timebox_offsets.append(params["offset"])
        return [
            {
                "task_id": f"task-{offset + index:03d}",
                "user_id": "device-timebox-paged",
                "start_at": "2026-03-07T08:00:00Z",
                "end_at": "2026-03-07T08:30:00Z",
                "source": "agent",
                "created_at": "2026-03-07T00:00:00Z",
                "updated_at": "2026-03-07T00:00:00Z",
            }
            for index in range(count)
        ]

    repository._request = fake_request  # type: ignore[assignment]
    try:
        rows = asyncio.run(repository.list_tasks(user_id="device-timebox-paged"))
    finally:
        asyncio.run(repository.close())

    assert len(rows) == 501
    assert item_offsets == ["0", "500"]
    assert timebox_offsets == ["0", "500"]
    assert rows[-1].timebox_start_at == "2026-03-07T08:00:00Z"


def test_supabase_delete_tasks_returns_actual_deleted_count():
    repository = main.SupabaseRepository(
        project_url="https://example.supabase.co",
        service_role_key="service-role-key",
    )
    delete_calls: list[tuple[str, str]] = []

    async def fake_request(
        method: str,
        path: str,
        *,
        params=None,
        json_body=None,
        extra_headers=None,
    ):
        assert method == "DELETE"
        assert path == "task_items"
        assert json_body is None
        assert extra_headers == {"Prefer": "return=representation"}
        task_id = params["task_id"].removeprefix("eq.")
        user_id = params["user_id"].removeprefix("eq.")
        delete_calls.append((task_id, user_id))
        if task_id == "task-found":
            return [{"task_id": task_id}]
        return []

    repository._request = fake_request  # type: ignore[assignment]
    try:
        deleted = asyncio.run(
            repository.delete_tasks(
                user_id="device-delete-count",
                task_ids=["task-missing", "task-found"],
            )
        )
    finally:
        asyncio.run(repository.close())

    assert deleted == 1
    assert delete_calls == [
        ("task-missing", "device-delete-count"),
        ("task-found", "device-delete-count"),
    ]
