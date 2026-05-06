import { settings } from "./config.js";
import * as db from "./db.js";
import { broadcaster } from "./logStream.js";
import { sendAll } from "./pipeline.js";

type SchedulerConfig = {
  timeZone: string;
  hour: number;
  minute: number;
};

function getZonedParts(date: Date, timeZone: string) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(date);

  const get = (type: string) => parts.find((part) => part.type === type)?.value ?? "";

  return {
    year: Number(get("year")),
    month: Number(get("month")),
    day: Number(get("day")),
    hour: Number(get("hour")),
    minute: Number(get("minute")),
    localDate: `${get("year")}-${get("month")}-${get("day")}`,
  };
}

function findNextRun(now: Date, timeZone: string, hour: number, minute: number): Date {
  for (let offsetMinutes = 0; offsetMinutes <= 60 * 48; offsetMinutes += 1) {
    const candidate = new Date(now.getTime() + offsetMinutes * 60_000);
    const parts = getZonedParts(candidate, timeZone);
    if (parts.hour === hour && parts.minute === minute) {
      return candidate;
    }
  }

  return new Date(now.getTime() + 24 * 60 * 60_000);
}

function clampInteger(value: string, fallback: number, min: number, max: number): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, parsed));
}

export function getSchedulerConfig(): SchedulerConfig {
  return {
    timeZone:
      db.getSetting("scheduler_timezone", "").trim() ||
      settings.schedulerTimezone,
    hour: clampInteger(
      db.getSetting("scheduler_send_hour", ""),
      settings.sendHour,
      0,
      23,
    ),
    minute: clampInteger(
      db.getSetting("scheduler_send_minute", ""),
      settings.sendMinute,
      0,
      59,
    ),
  };
}

export function setSchedulerConfig(next: Partial<SchedulerConfig>): SchedulerConfig {
  const current = getSchedulerConfig();
  const merged = {
    timeZone: next.timeZone?.trim() || current.timeZone,
    hour:
      typeof next.hour === "number"
        ? Math.min(23, Math.max(0, Math.trunc(next.hour)))
        : current.hour,
    minute:
      typeof next.minute === "number"
        ? Math.min(59, Math.max(0, Math.trunc(next.minute)))
        : current.minute,
  };

  db.setSetting("scheduler_timezone", merged.timeZone);
  db.setSetting("scheduler_send_hour", String(merged.hour));
  db.setSetting("scheduler_send_minute", String(merged.minute));
  return merged;
}

export function getSchedulerState() {
  const isPaused     = db.getSetting("scheduler_paused", "false") === "true";
  const lastRunDate  = db.getSetting("scheduler_last_date", "");
  const lastRunStatus = db.getSetting("scheduler_last_status", "");
  const schedulerConfig = getSchedulerConfig();

  const now = new Date();
  const next = findNextRun(
    new Date(now.getTime() + 60_000),
    schedulerConfig.timeZone,
    schedulerConfig.hour,
    schedulerConfig.minute,
  );

  const countdownSeconds = Math.max(0, Math.floor((next.getTime() - now.getTime()) / 1000));
  const nowParts = getZonedParts(now, schedulerConfig.timeZone);
  const nextParts = getZonedParts(next, schedulerConfig.timeZone);
  const isToday = nextParts.localDate === nowParts.localDate;
  const timeLabel = next.toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
    timeZone: schedulerConfig.timeZone,
    timeZoneName: "short",
  });

  return {
    is_paused:        isPaused,
    last_run_date:    lastRunDate || null,
    last_run_status:  lastRunStatus || null,
    schedule_label:   `Daily at ${timeLabel}`,
    next_run_label:   `${isToday ? "Today" : "Tomorrow"} at ${timeLabel}`,
    countdown_seconds: countdownSeconds,
  };
}

export function setPaused(paused: boolean): void {
  db.setSetting("scheduler_paused", paused ? "true" : "false");
}

function recordRun(status: string): void {
  const now = new Date();
  db.setSetting("scheduler_last_date",   now.toISOString().slice(0, 10));
  db.setSetting("scheduler_last_status", status);
}

// Poll every 30s — fire pipeline once per day at the configured local scheduler time.
export function startSchedulerLoop(): void {
  let lastFiredLocalDate = "";

  setInterval(async () => {
    try {
      const state = getSchedulerState();
      if (state.is_paused) return;

      const schedulerConfig = getSchedulerConfig();
      const now = new Date();
      const zoned = getZonedParts(now, schedulerConfig.timeZone);
      if (
        zoned.hour === schedulerConfig.hour &&
        zoned.minute === schedulerConfig.minute &&
        lastFiredLocalDate !== zoned.localDate
      ) {
        lastFiredLocalDate = zoned.localDate;
        broadcaster.emit("Scheduler: daily pipeline starting…", "info");
        const results = await sendAll(undefined, "scheduler");
        const sent    = results.filter(r => r.status === "sent").length;
        const failed  = results.filter(r => r.status === "failed").length;
        const skipped = results.filter(r => r.status === "skipped").length;
        recordRun(`${sent} sent, ${skipped} skipped, ${failed} failed`);
      }
    } catch (e: any) {
      console.error("[scheduler] error:", e?.message);
    }
  }, 30_000);
}
