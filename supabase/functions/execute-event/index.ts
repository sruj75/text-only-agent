import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";

const EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send";
const MAX_DELIVERY_ATTEMPTS = 4;
const RETRY_BASE_SECONDS = 60;
const RETRY_MAX_SECONDS = 30 * 60;

type EventRow = {
  id: string;
  user_id: string;
  event_type: string;
  payload: Record<string, unknown>;
  executed: boolean;
  cron_job_id: number | null;
  attempt_count?: number | null;
  scheduled_time?: string | null;
};

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function localDateKey(timezoneName: string | null, atIso?: string | null): string {
  if (!timezoneName) return "";
  const referenceDate = atIso ? new Date(atIso) : new Date();
  if (Number.isNaN(referenceDate.getTime())) return "";
  try {
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone: timezoneName,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).formatToParts(referenceDate);
    const year = parts.find((p) => p.type === "year")?.value;
    const month = parts.find((p) => p.type === "month")?.value;
    const day = parts.find((p) => p.type === "day")?.value;
    if (year && month && day) return `${year}-${month}-${day}`;
  } catch {
    return "";
  }
  return "";
}

function getDailySessionId(userId: string, timezoneName: string, atIso?: string | null): string {
  const dateKey = localDateKey(timezoneName, atIso);
  if (!dateKey) return "";
  return `session_${userId}_${dateKey}`;
}

function asString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function nextRetryRunAtIso(attemptCount: number): string {
  const exponent = Math.max(0, attemptCount - 1);
  const delaySeconds = Math.min(RETRY_MAX_SECONDS, RETRY_BASE_SECONDS * 2 ** exponent);
  return new Date(Date.now() + delaySeconds * 1000).toISOString();
}

function formatCalendarReminderBody(payload: Record<string, unknown>, timezoneName: string): string {
  const eventTitle = asString(payload.event_title) ?? "upcoming event";
  const raw = asString(payload.event_start_time);
  if (!raw) return `${eventTitle} starts soon.`;

  const dt = new Date(raw);
  if (Number.isNaN(dt.getTime())) return `${eventTitle} starts soon.`;

  try {
    const formatted = new Intl.DateTimeFormat("en-US", {
      hour: "2-digit",
      minute: "2-digit",
      hour12: true,
      timeZone: timezoneName,
    }).format(dt);
    return `${eventTitle} starts at ${formatted}.`;
  } catch {
    return `${eventTitle} starts soon.`;
  }
}

async function sendPushNotification(
  supabase: ReturnType<typeof createClient>,
  userId: string,
  title: string,
  body: string,
  data: Record<string, unknown>,
): Promise<boolean> {
  const tokenResult = await supabase.rpc("get_push_token_for_execution", {
    p_user_id: userId,
  });
  if (tokenResult.error) {
    console.error("get_push_token_for_execution failed", tokenResult.error.message);
    return false;
  }

  const pushToken = asString(tokenResult.data);
  if (!pushToken) return false;

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 10000);
  let response: Response;
  try {
    response = await fetch(EXPO_PUSH_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        to: pushToken,
        title,
        body,
        data,
        sound: "default",
        priority: "high",
      }),
      signal: controller.signal,
    });
  } catch (error) {
    console.error("Expo push request failed", error);
    return false;
  } finally {
    clearTimeout(timeoutId);
  }

  if (!response.ok) return false;

  let result: unknown;
  try {
    result = await response.json();
  } catch (error) {
    console.error("Failed to parse Expo push response JSON", error);
    return false;
  }
  const data = (result as { data?: unknown } | null)?.data;
  const items = Array.isArray(data) ? data : (data ? [data] : []);
  for (const item of items) {
    if (item?.status === "error") {
      const errorCode = item?.details?.error;
      if (errorCode === "DeviceNotRegistered") {
        const deleteResult = await supabase.rpc("delete_push_token_for_execution", {
          p_user_id: userId,
        });
        if (deleteResult.error) {
          console.warn(
            "delete_push_token_for_execution failed",
            userId,
            deleteResult.error.message,
          );
        }
      }
      return false;
    }
  }

  return true;
}

async function unscheduleIfPresent(
  supabase: ReturnType<typeof createClient>,
  jobId: number | null,
): Promise<void> {
  if (!jobId || Number.isNaN(Number(jobId))) return;
  const result = await supabase.rpc("unschedule_event_job", { p_job_id: Number(jobId) });
  if (result.error) {
    console.warn("unschedule_event_job failed", result.error.message);
  }
}

async function ensureNextMorningWake(
  supabase: ReturnType<typeof createClient>,
  eventId: string,
): Promise<{ ok: boolean; detail?: unknown; error?: string }> {
  const result = await supabase.rpc("ensure_next_morning_wake_event", {
    p_event_id: eventId,
  });
  if (result.error) {
    console.warn("ensure_next_morning_wake_event failed", result.error.message);
    return { ok: false, error: result.error.message };
  }
  return { ok: true, detail: result.data ?? null };
}

async function resolveSessionTimezone(
  supabase: ReturnType<typeof createClient>,
  userId: string,
  payload: Record<string, unknown>,
  calendarTimezone: string | null,
): Promise<string | null> {
  if (calendarTimezone) return calendarTimezone;
  const payloadTimezone = asString(payload.timezone);
  if (payloadTimezone) return payloadTimezone;
  const tzResult = await supabase.rpc("get_user_timezone_for_execution", {
    p_user_id: userId,
  });
  if (tzResult.error) return null;
  return asString(tzResult.data);
}

async function ensureProactiveSession(
  supabase: ReturnType<typeof createClient>,
  {
    userId,
    sessionId,
    timezoneName,
    scheduledTime,
    title,
    entryContext,
  }: {
    userId: string;
    sessionId: string;
    timezoneName: string;
    scheduledTime: string | null;
    title: string;
    entryContext: Record<string, unknown>;
  },
): Promise<boolean> {
  const dateKey = localDateKey(timezoneName, scheduledTime);
  if (!dateKey) return false;

  const existingSessionResult = await supabase
    .from("sessions")
    .select("date,state")
    .eq("session_id", sessionId)
    .eq("user_id", userId)
    .maybeSingle();
  if (existingSessionResult.error) {
    console.error("sessions select failed", existingSessionResult.error.message);
    return false;
  }

  const existingState =
    existingSessionResult.data?.state &&
    typeof existingSessionResult.data.state === "object" &&
    !Array.isArray(existingSessionResult.data.state)
      ? existingSessionResult.data.state
      : {};

  const now = new Date().toISOString();
  const sessionState = {
    ...existingState,
    title,
    thread_type: "daily",
    user_timezone: timezoneName,
    entry_context: entryContext,
    lifecycle_state: "active",
    last_seen_at: now,
  };

  const result = await supabase
    .from("sessions")
    .upsert(
      {
        session_id: sessionId,
        user_id: userId,
        date: existingSessionResult.data?.date ?? dateKey,
        state: sessionState,
        updated_at: now,
      },
      {
        onConflict: "session_id",
        ignoreDuplicates: false,
      },
    );

  if (result.error) {
    console.error("sessions upsert failed", result.error.message);
    return false;
  }

  return true;
}

Deno.serve(async (req: Request): Promise<Response> => {
  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
    if (!supabaseUrl || !serviceRoleKey) {
      return new Response(
        JSON.stringify({ error: "Missing Supabase runtime env" }),
        { status: 500, headers: { "Content-Type": "application/json" } },
      );
    }

    const supabase = createClient(supabaseUrl, serviceRoleKey, {
      auth: { persistSession: false, autoRefreshToken: false },
    });

    const secretResult = await supabase.rpc("get_scheduler_secret_for_execution");
    const expectedSecret = asString(secretResult.data);
    const providedSecret = asString(req.headers.get("x-scheduler-secret"));
    if (secretResult.error || !expectedSecret || providedSecret !== expectedSecret) {
      return new Response(
        JSON.stringify({ error: "Unauthorized" }),
        { status: 401, headers: { "Content-Type": "application/json" } },
      );
    }

    let payload: Record<string, unknown>;
    try {
      payload = await req.json();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Invalid JSON body";
      return new Response(
        JSON.stringify({ error: `Malformed JSON body: ${message}` }),
        { status: 400, headers: { "Content-Type": "application/json" } },
      );
    }
    const eventId = asString(payload?.event_id);
    if (!eventId) {
      return new Response(
        JSON.stringify({ error: "event_id is required" }),
        { status: 400, headers: { "Content-Type": "application/json" } },
      );
    }

    const eventResult = await supabase.rpc("get_event_for_execution", { p_event_id: eventId });
    if (eventResult.error) {
      return new Response(
        JSON.stringify({ error: `event fetch failed: ${eventResult.error.message}` }),
        { status: 500, headers: { "Content-Type": "application/json" } },
      );
    }

    const event = eventResult.data as EventRow | null;
    if (!event) {
      return new Response(
        JSON.stringify({ error: "Event not found" }),
        { status: 404, headers: { "Content-Type": "application/json" } },
      );
    }

    if (event.executed) {
      await unscheduleIfPresent(supabase, event.cron_job_id ?? null);
      return new Response(
        JSON.stringify({ status: "already_executed", event_id: eventId }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    }

    const eventType = asString(event.event_type) ?? "checkin";
    const eventUserId = asString(event.user_id);
    if (!eventUserId) {
      return new Response(
        JSON.stringify({ error: "Event missing user_id" }),
        { status: 500, headers: { "Content-Type": "application/json" } },
      );
    }

    const rawPayload =
      event.payload && typeof event.payload === "object" && !Array.isArray(event.payload)
        ? event.payload
        : {};
    const eventPayload = { ...rawPayload };

    let calendarTimezone: string | null = null;
    if (eventType === "calendar_reminder") {
      calendarTimezone = asString(eventPayload.timezone);
      if (!calendarTimezone) {
        const tzResult = await supabase.rpc("get_user_timezone_for_execution", {
          p_user_id: eventUserId,
        });
        if (!tzResult.error) {
          calendarTimezone = asString(tzResult.data);
        }
      }
      if (calendarTimezone) {
        eventPayload.timezone = calendarTimezone;
      }
    }

    const sessionTimezone = await resolveSessionTimezone(
      supabase,
      eventUserId,
      eventPayload,
      calendarTimezone,
    );
    const reason = asString(eventPayload.reason) ?? "scheduled_checkin";
    const triggerType = asString(eventPayload.trigger_type) ?? eventType;
    const calendarEventId = asString(eventPayload.calendar_event_id);

    let title = "Check-in";
    let body = "You have a scheduled check-in.";
    if (eventType === "morning_wake") {
      title = "Good morning";
      body = "Ready to plan your day?";
    } else if (eventType === "calendar_reminder") {
      title = `Upcoming: ${asString(eventPayload.event_title) ?? "Event"}`;
      body = calendarTimezone
        ? formatCalendarReminderBody(eventPayload, calendarTimezone)
        : "Reminder unavailable: missing timezone.";
    } else if (eventType === "checkin") {
      body = `It is time for your check-in (${reason}).`;
    }

    const sessionId = sessionTimezone
      ? getDailySessionId(eventUserId, sessionTimezone, asString(event.scheduled_time))
      : "";

    const notificationData: Record<string, unknown> = {
      session_id: sessionId,
      type: eventType,
      trigger_type: triggerType,
      event_id: eventId,
      user_id: eventUserId,
      source: "push",
      entry_mode: "proactive",
      scheduled_time: asString(event.scheduled_time),
    };
    if (calendarEventId) notificationData.calendar_event_id = calendarEventId;

    const attemptedAt = new Date().toISOString();
    const existingAttemptCount = Math.max(0, asNumber(event.attempt_count) ?? 0);
    const nextAttemptCount = existingAttemptCount + 1;

    let pushSent = false;
    let sessionReady = false;
    if (!sessionTimezone || !sessionId) {
      pushSent = false;
    } else if (!(eventType === "calendar_reminder" && !calendarTimezone)) {
      sessionReady = await ensureProactiveSession(supabase, {
        userId: eventUserId,
        sessionId,
        timezoneName: sessionTimezone,
        scheduledTime: asString(event.scheduled_time),
        title,
        entryContext: {
          source: "push",
          event_id: eventId,
          trigger_type: triggerType,
          scheduled_time: asString(event.scheduled_time),
          calendar_event_id: calendarEventId,
          entry_mode: "proactive",
        },
      });
      if (!sessionReady) {
        pushSent = false;
      } else {
        pushSent = await sendPushNotification(supabase, eventUserId, title, body, notificationData);
      }
    }

    let lastError: string | null = pushSent ? null : "push_failed_or_missing_token";
    if (!sessionTimezone || !sessionId) {
      lastError = "missing_timezone";
    } else if (!sessionReady) {
      lastError = "session_persist_failed";
    } else if (eventType === "calendar_reminder" && !calendarTimezone) {
      lastError = "missing_timezone";
    }
    let deliverySucceeded = pushSent && !!sessionReady && !!sessionTimezone && !!sessionId;

    let nextMorningWake: { ok: boolean; detail?: unknown; error?: string } | null = null;
    let continuationFailed = false;
    if (eventType === "morning_wake") {
      nextMorningWake = await ensureNextMorningWake(supabase, eventId);
      if (!nextMorningWake.ok) {
        continuationFailed = true;
        if (!pushSent) {
          lastError = "morning_wake_continuation_failed";
        }
      }
    }

    if (deliverySucceeded) {
      const finalize = await supabase.rpc("finalize_event_execution", {
        p_event_id: eventId,
        p_last_error: continuationFailed ? "morning_wake_continuation_failed" : null,
        p_attempted_at: attemptedAt,
        p_executed: true,
        p_attempt_count: nextAttemptCount,
      });
      if (finalize.error) {
        return new Response(
          JSON.stringify({ error: `finalize failed: ${finalize.error.message}` }),
          { status: 500, headers: { "Content-Type": "application/json" } },
        );
      }
      await unscheduleIfPresent(supabase, event.cron_job_id ?? null);
      await supabase.from("events").update({
        workflow_state: "succeeded",
        next_retry_at: null,
        dead_lettered_at: null,
        updated_at: new Date().toISOString(),
      }).eq("id", eventId);
      return new Response(
        JSON.stringify({
          status: "executed",
          event_id: eventId,
          event_type: eventType,
          push_sent: pushSent,
          attempt_count: nextAttemptCount,
          next_morning_wake: nextMorningWake,
          continuation_failed: continuationFailed,
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    }

    if (nextAttemptCount >= MAX_DELIVERY_ATTEMPTS) {
      const finalize = await supabase.rpc("finalize_event_execution", {
        p_event_id: eventId,
        p_last_error: lastError,
        p_attempted_at: attemptedAt,
        p_executed: false,
        p_attempt_count: nextAttemptCount,
      });
      if (finalize.error) {
        return new Response(
          JSON.stringify({ error: `finalize failed: ${finalize.error.message}` }),
          { status: 500, headers: { "Content-Type": "application/json" } },
        );
      }
      await unscheduleIfPresent(supabase, event.cron_job_id ?? null);
      await supabase.from("events").update({
        workflow_state: "dead_letter",
        dead_lettered_at: new Date().toISOString(),
        next_retry_at: null,
        updated_at: new Date().toISOString(),
      }).eq("id", eventId);
      return new Response(
        JSON.stringify({
          status: "dead_letter",
          event_id: eventId,
          event_type: eventType,
          attempt_count: nextAttemptCount,
          last_error: lastError,
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    }

    const retryAt = nextRetryRunAtIso(nextAttemptCount);
    const retryTimezone = sessionTimezone || "UTC";
    const retryResult = await supabase.rpc("schedule_event_retry", {
      p_event_id: eventId,
      p_next_run_at: retryAt,
      p_timezone: retryTimezone,
      p_last_error: lastError || "push_failed_or_missing_token",
      p_attempted_at: attemptedAt,
    });
    if (retryResult.error) {
      return new Response(
        JSON.stringify({ error: `retry schedule failed: ${retryResult.error.message}` }),
        { status: 500, headers: { "Content-Type": "application/json" } },
      );
    }
    await supabase.from("events").update({
      workflow_state: "retry_scheduled",
      next_retry_at: retryAt,
      updated_at: new Date().toISOString(),
    }).eq("id", eventId);
    return new Response(
      JSON.stringify({
        status: "retry_scheduled",
        event_id: eventId,
        event_type: eventType,
        attempt_count: nextAttemptCount,
        retry_at: retryAt,
        last_error: lastError,
      }),
      { status: 202, headers: { "Content-Type": "application/json" } },
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return new Response(
      JSON.stringify({ error: message }),
      { status: 500, headers: { "Content-Type": "application/json" } },
    );
  }
});
