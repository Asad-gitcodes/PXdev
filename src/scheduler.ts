import { settings } from "./config.js";
import * as db from "./db.js";
import { broadcaster } from "./logStream.js";
import { sendAll } from "./pipeline.js";

export function getSchedulerState() {
  const isPaused     = db.getSetting("scheduler_paused", "false") === "true";
  const lastRunDate  = db.getSetting("scheduler_last_date", "");
  const lastRunStatus = db.getSetting("scheduler_last_status", "");

  const now = new Date();
  const next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), settings.sendHour, settings.sendMinute, 0));
  if (next <= now) next.setUTCDate(next.getUTCDate() + 1);

  const countdownSeconds = Math.max(0, Math.floor((next.getTime() - now.getTime()) / 1000));
  const isToday = next.getUTCDate() === now.getUTCDate();
  const timeLabel = next.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit", hour12: true, timeZone: "UTC" });

  return {
    is_paused:        isPaused,
    last_run_date:    lastRunDate || null,
    last_run_status:  lastRunStatus || null,
    schedule_label:   `Daily at ${timeLabel} UTC`,
    next_run_label:   `${isToday ? "Today" : "Tomorrow"} at ${timeLabel} UTC`,
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

// Poll every 30s — fire pipeline once per day at configured hour:minute UTC
export function startSchedulerLoop(): void {
  let lastFiredDate = "";

  setInterval(async () => {
    try {
      const state = getSchedulerState();
      if (state.is_paused) return;

      const now = new Date();
      const today = now.toISOString().slice(0, 10);
      if (now.getUTCHours() === settings.sendHour && now.getUTCMinutes() === settings.sendMinute && lastFiredDate !== today) {
        lastFiredDate = today;
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
