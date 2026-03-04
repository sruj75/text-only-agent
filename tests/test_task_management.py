from __future__ import annotations

from datetime import datetime, timedelta, timezone


def _capture(client, device_id: str, timezone_name: str, titles: list[str]):
    response = client.post(
        "/agent/task-management",
        json={
            "device_id": device_id,
            "timezone": timezone_name,
            "action": "capture_tasks",
            "payload": {"titles": titles},
        },
    )
    assert response.status_code == 200
    return response.json()


def test_push_token_registration(app_client):
    client = app_client["client"]
    repository = app_client["repository"]

    response = client.post(
        "/agent/push-token",
        json={
            "device_id": "device-token",
            "expo_push_token": "ExponentPushToken[token]",
            "timezone": "Asia/Kolkata",
        },
    )

    assert response.status_code == 200
    assert repository.push_tokens["device-token"] == "ExponentPushToken[token]"
    assert repository.users["device-token"]["timezone"] == "Asia/Kolkata"


def test_task_management_happy_path(app_client):
    client = app_client["client"]

    captured = _capture(client, "device-task-1", "UTC", ["Deep work", "Email"])
    task_ids = captured["result"]["created_task_ids"]
    assert len(task_ids) == 2

    prioritized = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-task-1",
            "timezone": "UTC",
            "session_id": captured["session_id"],
            "action": "set_top_essentials",
            "payload": {"task_ids": [task_ids[0], task_ids[1]]},
        },
    )
    assert prioritized.status_code == 200
    ranked = prioritized.json()["result"]["tasks"]
    assert ranked[0]["priority_rank"] == 1
    assert ranked[1]["priority_rank"] == 2

    now = datetime.now(timezone.utc)
    start = (now + timedelta(hours=1)).replace(microsecond=0)
    end = (start + timedelta(minutes=45)).replace(microsecond=0)

    timeboxed = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-task-1",
            "timezone": "UTC",
            "session_id": captured["session_id"],
            "action": "timebox_task",
            "payload": {
                "task_id": task_ids[0],
                "start_at": start.isoformat().replace("+00:00", "Z"),
                "end_at": end.isoformat().replace("+00:00", "Z"),
            },
        },
    )
    assert timeboxed.status_code == 200
    scheduled = timeboxed.json()["result"]["scheduled_events"]
    assert [item["trigger_type"] for item in scheduled] == ["before_task", "after_task"]

    schedule = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-task-1",
            "timezone": "UTC",
            "session_id": captured["session_id"],
            "action": "get_schedule",
            "payload": {"date": "today"},
        },
    )
    assert schedule.status_code == 200
    items = schedule.json()["result"]["schedule"]["items"]
    assert len(items) == 1
    assert items[0]["task_id"] == task_ids[0]

    status_update = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-task-1",
            "timezone": "UTC",
            "session_id": captured["session_id"],
            "action": "update_task_status",
            "payload": {"task_id": task_ids[0], "status": "in_progress"},
        },
    )
    assert status_update.status_code == 200
    assert status_update.json()["result"]["task"]["status"] == "in_progress"


def test_timebox_is_immutable_in_v1(app_client):
    client = app_client["client"]
    captured = _capture(client, "device-task-immutable", "UTC", ["Task"])
    task_id = captured["result"]["created_task_ids"][0]

    now = datetime.now(timezone.utc)
    start = (now + timedelta(hours=2)).replace(microsecond=0)
    end = (start + timedelta(minutes=25)).replace(microsecond=0)

    first = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-task-immutable",
            "timezone": "UTC",
            "session_id": captured["session_id"],
            "action": "timebox_task",
            "payload": {
                "task_id": task_id,
                "start_at": start.isoformat().replace("+00:00", "Z"),
                "end_at": end.isoformat().replace("+00:00", "Z"),
            },
        },
    )
    assert first.status_code == 200

    second = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-task-immutable",
            "timezone": "UTC",
            "session_id": captured["session_id"],
            "action": "timebox_task",
            "payload": {
                "task_id": task_id,
                "start_at": (start + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
                "end_at": (end + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
            },
        },
    )
    assert second.status_code == 409


def test_transition_rule_uses_gap_threshold(app_client):
    client = app_client["client"]
    repository = app_client["repository"]

    captured = _capture(client, "device-transition", "UTC", ["Task A", "Task B", "Task C"])
    task_a, task_b, task_c = captured["result"]["created_task_ids"]

    now = datetime.now(timezone.utc).replace(microsecond=0)

    a_start = now + timedelta(hours=3)
    a_end = a_start + timedelta(minutes=30)
    b_start = a_end + timedelta(minutes=10)  # transition
    b_end = b_start + timedelta(minutes=30)
    c_start = b_end + timedelta(minutes=11)  # after_task
    c_end = c_start + timedelta(minutes=30)

    for task_id, start, end in [
        (task_a, a_start, a_end),
        (task_b, b_start, b_end),
        (task_c, c_start, c_end),
    ]:
        response = client.post(
            "/agent/task-management",
            json={
                "device_id": "device-transition",
                "timezone": "UTC",
                "session_id": captured["session_id"],
                "action": "timebox_task",
                "payload": {
                    "task_id": task_id,
                    "start_at": start.isoformat().replace("+00:00", "Z"),
                    "end_at": end.isoformat().replace("+00:00", "Z"),
                },
            },
        )
        assert response.status_code == 200

    owned_events = [
        event
        for event in repository.events.values()
        if event.payload.get("schedule_owner") == "task_management"
    ]

    # One before + one end trigger per task.
    assert len(owned_events) == 6

    task_a_end_trigger = [
        event
        for event in owned_events
        if event.payload.get("task_id") == task_a and event.payload.get("reason") != "before_task"
    ][0]
    task_b_end_trigger = [
        event
        for event in owned_events
        if event.payload.get("task_id") == task_b and event.payload.get("reason") != "before_task"
    ][0]

    assert task_a_end_trigger.payload.get("reason") == "transition"
    assert task_b_end_trigger.payload.get("reason") == "after_task"
