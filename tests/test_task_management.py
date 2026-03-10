from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import main


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

    another_session = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-task-1",
            "timezone": "UTC",
            "session_id": "session_device-task-1_cross_session",
            "action": "get_tasks",
        },
    )
    assert another_session.status_code == 200
    assert [task["task_id"] for task in another_session.json()["result"]["tasks"]] == task_ids

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
    start = (now + timedelta(minutes=10)).replace(second=0, microsecond=0)
    if start.date() != now.date():
        start = now.replace(hour=20, minute=0, second=0, microsecond=0)
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


def test_timebox_is_editable_for_persistent_tasks(app_client):
    client = app_client["client"]
    captured = _capture(client, "device-task-editable", "UTC", ["Task"])
    task_id = captured["result"]["created_task_ids"][0]

    now = datetime.now(timezone.utc)
    start = (now + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
    end = (start + timedelta(minutes=25)).replace(microsecond=0)

    first = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-task-editable",
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
    assert len(first.json()["result"]["scheduled_events"]) == 2

    second = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-task-editable",
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
    assert second.status_code == 200
    assert second.json()["result"]["task"]["timebox"]["start_at"] == (
        start + timedelta(hours=1)
    ).isoformat().replace("+00:00", "Z")


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


def test_timebox_must_stay_within_one_local_calendar_day(app_client):
    client = app_client["client"]
    timezone_name = "Asia/Kolkata"

    captured = _capture(client, "device-day-boundary", timezone_name, ["Task"])
    task_id = captured["result"]["created_task_ids"][0]

    # Session date in local timezone.
    local_day = datetime.now(ZoneInfo(timezone_name)).strftime("%Y-%m-%d")
    start = datetime.fromisoformat(f"{local_day}T23:50:00+05:30").astimezone(timezone.utc)
    end = start + timedelta(minutes=20)  # spills into next local day

    response = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-day-boundary",
            "timezone": timezone_name,
            "session_id": captured["session_id"],
            "action": "timebox_task",
            "payload": {
                "task_id": task_id,
                "start_at": start.isoformat().replace("+00:00", "Z"),
                "end_at": end.isoformat().replace("+00:00", "Z"),
            },
        },
    )

    assert response.status_code == 400
    detail = response.json()["error"]
    assert detail["code"] == "INVALID_PAYLOAD"
    assert "Timebox must stay within one local calendar day" in detail["message"]


def test_task_panel_uses_requested_day_for_schedule_snapshot():
    timezone_name = "UTC"
    task_state = main.TaskStateV1(
        date="2026-03-04",
        timezone=timezone_name,
        tasks=[
            main.TaskItem(
                task_id="task-1",
                title="Deep work",
                timebox=main.TaskTimebox(
                    start_at="2026-03-04T09:00:00Z",
                    end_at="2026-03-04T10:00:00Z",
                ),
            )
        ],
    )

    snapshot = main._build_task_panel_state(
        task_state=task_state,
        timezone_name=timezone_name,
    )

    assert [item["task_id"] for item in snapshot["schedule"]] == ["task-1"]


def test_idle_task_panel_state_omits_placeholder_headline():
    task_state = main.TaskStateV1(
        user_id="device-headline",
        date="today",
        timezone="UTC",
        updated_at="2026-03-07T12:00:00Z",
        tasks=[],
    )

    snapshot = main._build_task_panel_state(
        task_state=task_state,
        timezone_name="UTC",
    )

    assert snapshot["headline"] is None
    assert snapshot["active_action"] is None


def test_task_panel_now_is_strictly_time_derived(monkeypatch):
    task_state = main.TaskStateV1(
        date="2026-03-04",
        timezone="UTC",
        tasks=[
            main.TaskItem(
                task_id="task-early",
                title="Work on FRAU app",
                timebox=main.TaskTimebox(
                    start_at="2026-03-04T13:42:00Z",
                    end_at="2026-03-04T13:45:00Z",
                ),
            ),
            main.TaskItem(
                task_id="task-late",
                title="Start working",
                timebox=main.TaskTimebox(
                    start_at="2026-03-04T13:45:00Z",
                    end_at="2026-03-04T14:00:00Z",
                ),
            ),
        ],
    )

    monkeypatch.setattr(main, "_utc_now", lambda: main._parse_iso("2026-03-04T13:42:30Z"))
    first = main._build_task_panel_state(task_state=task_state, timezone_name="UTC")
    first_active = [task["id"] for task in first["tasks"] if task["is_active"]]
    assert first_active == ["task-early"]

    monkeypatch.setattr(main, "_utc_now", lambda: main._parse_iso("2026-03-04T13:45:00Z"))
    boundary = main._build_task_panel_state(task_state=task_state, timezone_name="UTC")
    boundary_active = [task["id"] for task in boundary["tasks"] if task["is_active"]]
    assert boundary_active == ["task-late"]

    monkeypatch.setattr(main, "_utc_now", lambda: main._parse_iso("2026-03-04T13:40:00Z"))
    upcoming = main._build_task_panel_state(task_state=task_state, timezone_name="UTC")
    upcoming_active = [task["id"] for task in upcoming["tasks"] if task["is_active"]]
    assert upcoming_active == ["task-early"]

    monkeypatch.setattr(main, "_utc_now", lambda: main._parse_iso("2026-03-04T14:05:00Z"))
    none_active = main._build_task_panel_state(task_state=task_state, timezone_name="UTC")
    assert [task["id"] for task in none_active["tasks"] if task["is_active"]] == []


def test_done_task_unschedules_and_reopen_rebuilds_future_events(app_client):
    client = app_client["client"]
    repository = app_client["repository"]

    captured = _capture(client, "device-status-events", "UTC", ["Task"])
    task_id = captured["result"]["created_task_ids"][0]

    now = datetime.now(timezone.utc).replace(microsecond=0)
    start = (now + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
    end = start + timedelta(minutes=30)

    timeboxed = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-status-events",
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
    assert timeboxed.status_code == 200
    assert len(repository.events) == 2

    done = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-status-events",
            "timezone": "UTC",
            "session_id": captured["session_id"],
            "action": "update_task_status",
            "payload": {"task_id": task_id, "status": "done"},
        },
    )
    assert done.status_code == 200
    assert repository.events == {}

    reopened = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-status-events",
            "timezone": "UTC",
            "session_id": captured["session_id"],
            "action": "update_task_status",
            "payload": {"task_id": task_id, "status": "todo"},
        },
    )
    assert reopened.status_code == 200
    assert len(repository.events) == 2
    assert any(event.event_type == "status_updated" for event in repository.task_events.values())


def test_get_schedule_rejects_invalid_date_format(app_client):
    client = app_client["client"]

    response = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-invalid-date",
            "timezone": "UTC",
            "action": "get_schedule",
            "payload": {"date": "tomorrow"},
        },
    )

    assert response.status_code == 400
    assert response.json()["error"]["message"] == "date must be 'today' or YYYY-MM-DD"


def test_supabase_task_list_paginates_past_500_rows():
    repository = main.SupabaseRepository(
        project_url="https://example.supabase.co",
        service_role_key="service-role-key",
    )
    request_offsets: list[str] = []

    async def fake_request(
        method: str,
        path: str,
        *,
        params=None,
        json_body=None,
        extra_headers=None,
    ):
        assert method == "GET"
        assert path == "tasks"
        assert json_body is None
        assert extra_headers is None
        request_offsets.append(params["offset"])

        offset = int(params["offset"])
        page_size = int(params["limit"])
        total_rows = 501
        remaining = max(total_rows - offset, 0)
        count = min(page_size, remaining)
        return [
            {
                "task_id": f"task-{offset + index:03d}",
                "user_id": "device-paged",
                "title": f"Task {offset + index:03d}",
                "status": "todo",
                "priority_rank": None,
                "is_essential": False,
                "timebox_start_at": None,
                "timebox_end_at": None,
                "created_at": f"2026-03-07T00:{(offset + index) // 60:02d}:{(offset + index) % 60:02d}Z",
                "updated_at": f"2026-03-07T00:{(offset + index) // 60:02d}:{(offset + index) % 60:02d}Z",
            }
            for index in range(count)
        ]

    repository._request = fake_request  # type: ignore[assignment]
    try:
        rows = asyncio.run(repository.list_tasks(user_id="device-paged"))
    finally:
        asyncio.run(repository.close())

    assert len(rows) == 501
    assert request_offsets == ["0", "500"]
    assert rows[0].task_id == "task-000"
    assert rows[-1].task_id == "task-500"


def test_supabase_morning_wake_continuation_enqueues_cloud_task_when_missing():
    repository = main.SupabaseRepository(
        project_url="https://example.supabase.co",
        service_role_key="service-role-key",
    )
    captured: dict[str, str | None] = {"event_id": None, "run_at": None, "timezone_name": None}
    updated: dict[str, str | None] = {"event_id": None, "cloud_task_name": None}

    async def fake_rpc(name: str, payload: dict):
        assert name == "ensure_next_morning_wake_event"
        assert payload == {"p_event_id": "event-current"}
        return {"status": "ok", "action": "created", "event_id": "event-next"}

    async def fake_get_event_for_execution(event_id: str):
        assert event_id == "event-next"
        return main.CheckinEventRecord(
            id="event-next",
            user_id="device-next",
            scheduled_time="2026-03-10T01:00:00Z",
            event_type="morning_wake",
            payload={"timezone": "Asia/Kolkata"},
            executed=False,
            cloud_task_name=None,
        )

    async def fake_schedule_event_job(*, event_id: str, run_at: str, timezone_name: str) -> str:
        captured["event_id"] = event_id
        captured["run_at"] = run_at
        captured["timezone_name"] = timezone_name
        return "projects/p/locations/l/queues/q/tasks/42"

    async def fake_update_event_cloud_task_name(event_id: str, cloud_task_name: str | None) -> None:
        updated["event_id"] = event_id
        updated["cloud_task_name"] = cloud_task_name

    repository._rpc = fake_rpc  # type: ignore[assignment]
    repository.get_event_for_execution = fake_get_event_for_execution  # type: ignore[assignment]
    repository.schedule_event_job = fake_schedule_event_job  # type: ignore[assignment]
    repository.update_event_cloud_task_name = fake_update_event_cloud_task_name  # type: ignore[assignment]

    try:
        result = asyncio.run(repository.ensure_next_morning_wake_event("event-current"))
    finally:
        asyncio.run(repository.close())

    assert result["status"] == "ok"
    assert result["event_id"] == "event-next"
    assert result["cloud_task_name"] == "projects/p/locations/l/queues/q/tasks/42"
    assert captured == {
        "event_id": "event-next",
        "run_at": "2026-03-10T01:00:00Z",
        "timezone_name": "Asia/Kolkata",
    }
    assert updated == {
        "event_id": "event-next",
        "cloud_task_name": "projects/p/locations/l/queues/q/tasks/42",
    }


def test_supabase_morning_wake_continuation_reuses_existing_cloud_task():
    repository = main.SupabaseRepository(
        project_url="https://example.supabase.co",
        service_role_key="service-role-key",
    )

    async def fake_rpc(name: str, payload: dict):
        assert name == "ensure_next_morning_wake_event"
        assert payload == {"p_event_id": "event-current"}
        return {"status": "ok", "action": "rescheduled_existing", "event_id": "event-next"}

    async def fake_get_event_for_execution(event_id: str):
        assert event_id == "event-next"
        return main.CheckinEventRecord(
            id="event-next",
            user_id="device-next",
            scheduled_time="2026-03-10T01:00:00Z",
            event_type="morning_wake",
            payload={"timezone": "Asia/Kolkata"},
            executed=False,
            cloud_task_name="projects/p/locations/l/queues/q/tasks/existing",
        )

    async def fail_schedule_event_job(*, event_id: str, run_at: str, timezone_name: str) -> str:
        raise AssertionError("schedule_event_job should not be called when cloud_task_name exists")

    async def fail_update_event_cloud_task_name(event_id: str, cloud_task_name: str | None) -> None:
        raise AssertionError("update_event_cloud_task_name should not be called when cloud_task_name exists")

    repository._rpc = fake_rpc  # type: ignore[assignment]
    repository.get_event_for_execution = fake_get_event_for_execution  # type: ignore[assignment]
    repository.schedule_event_job = fail_schedule_event_job  # type: ignore[assignment]
    repository.update_event_cloud_task_name = fail_update_event_cloud_task_name  # type: ignore[assignment]

    try:
        result = asyncio.run(repository.ensure_next_morning_wake_event("event-current"))
    finally:
        asyncio.run(repository.close())

    assert result["status"] == "ok"
    assert result["event_id"] == "event-next"
    assert result["cloud_task_name"] == "projects/p/locations/l/queues/q/tasks/existing"


def test_rebuild_keeps_existing_events_when_new_schedule_fails(app_client):
    client = app_client["client"]
    repository = app_client["repository"]

    captured = _capture(client, "device-rebuild-safe", "UTC", ["Task A", "Task B"])
    task_a, task_b = captured["result"]["created_task_ids"]

    now = (datetime.now(timezone.utc) + timedelta(days=1)).replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0,
    )
    a_start = now
    a_end = a_start + timedelta(minutes=30)

    first = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-rebuild-safe",
            "timezone": "UTC",
            "session_id": captured["session_id"],
            "action": "timebox_task",
            "payload": {
                "task_id": task_a,
                "start_at": a_start.isoformat().replace("+00:00", "Z"),
                "end_at": a_end.isoformat().replace("+00:00", "Z"),
            },
        },
    )
    assert first.status_code == 200
    old_event_ids = set(repository.events.keys())
    assert len(old_event_ids) == 2

    b_start = a_end + timedelta(minutes=5)
    b_end = b_start + timedelta(minutes=25)

    original_schedule = repository.schedule_event_job
    call_count = {"value": 0}

    async def failing_schedule_event_job(*, event_id: str, run_at: str, timezone_name: str) -> int:
        call_count["value"] += 1
        if call_count["value"] == 1:
            raise RuntimeError("scheduler down")
        return await original_schedule(event_id=event_id, run_at=run_at, timezone_name=timezone_name)

    repository.schedule_event_job = failing_schedule_event_job  # type: ignore[assignment]
    failed = client.post(
        "/agent/task-management",
        json={
            "device_id": "device-rebuild-safe",
            "timezone": "UTC",
            "session_id": captured["session_id"],
            "action": "timebox_task",
            "payload": {
                "task_id": task_b,
                "start_at": b_start.isoformat().replace("+00:00", "Z"),
                "end_at": b_end.isoformat().replace("+00:00", "Z"),
            },
        },
    )
    repository.schedule_event_job = original_schedule  # type: ignore[assignment]

    assert failed.status_code == 503
    assert failed.json()["error"]["code"] == "SCHEDULER_UNAVAILABLE"
    assert "Task was timeboxed, but reminder scheduling failed" in failed.json()["error"]["message"]
    assert old_event_ids.issubset(set(repository.events.keys()))


def test_tool_task_management_invalid_action_fails_fast(app_client):
    client = app_client["client"]

    opened = client.post(
        "/agent/session/open",
        json={
            "device_id": "device-invalid-action",
            "timezone": "UTC",
            "source": "manual",
            "open_id": "open-invalid-action",
            "client_version": main.RELEASE_ID,
            "contract_version": main.CONTRACT_VERSION,
        },
    )
    assert opened.status_code == 200
    session_id = opened.json()["session_id"]

    result = asyncio.run(
        main._tool_task_management(
            "invalid_action",
            {},
            session_id,
            "UTC",
            {
                "user_id": "device-invalid-action",
                "session_id": session_id,
                "timezone": "UTC",
            },
        )
    )

    assert result["ok"] is False
    assert result["error"]["code"] == "INVALID_ACTION"
