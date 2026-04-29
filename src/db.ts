import Database from "better-sqlite3";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// DB_PATH env var lets Render's persistent disk keep data across redeploys.
// Falls back to jobs.db next to the package root for local dev.
const DB_PATH = process.env.DB_PATH ?? path.join(__dirname, "..", "jobs.db");

let _db: Database.Database | null = null;

function db(): Database.Database {
  if (!_db) {
    _db = new Database(DB_PATH);
    _db.pragma("journal_mode = WAL");
  }
  return _db;
}

export function initDb(): void {
  db().exec(`
    CREATE TABLE IF NOT EXISTS settings (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL DEFAULT ''
    );
    CREATE TABLE IF NOT EXISTS jobs (
      id                INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id            TEXT    NOT NULL UNIQUE,
      license_key       TEXT    NOT NULL,
      clinic_name       TEXT    NOT NULL DEFAULT '',
      report_date       TEXT    NOT NULL,
      triggered_by      TEXT    NOT NULL DEFAULT 'manual',
      status            TEXT    NOT NULL DEFAULT 'running',
      records_found     INTEGER          DEFAULT 0,
      records_passed    INTEGER          DEFAULT 0,
      analytics_records INTEGER          DEFAULT 0,
      email_to          TEXT             DEFAULT '',
      email_subject     TEXT             DEFAULT '',
      error_message     TEXT             DEFAULT '',
      started_at        TEXT    NOT NULL,
      completed_at      TEXT             DEFAULT NULL
    );
  `);
}

export function createJob(licenseKey: string, reportDate: string, triggeredBy = "manual"): string {
  const jobId = randomUUID();
  const now = new Date().toISOString();
  db().prepare(
    `INSERT INTO jobs (job_id, license_key, report_date, triggered_by, status, started_at)
     VALUES (?, ?, ?, ?, 'running', ?)`
  ).run(jobId, licenseKey, reportDate, triggeredBy, now);
  return jobId;
}

export function updateJob(jobId: string, fields: Record<string, unknown>): void {
  if (!Object.keys(fields).length) return;
  const data = { ...fields, completed_at: new Date().toISOString() };
  const set = Object.keys(data).map(k => `${k} = ?`).join(", ");
  db().prepare(`UPDATE jobs SET ${set} WHERE job_id = ?`).run(...Object.values(data), jobId);
}

export function getJobs(limit = 50, offset = 0): Record<string, unknown>[] {
  return db().prepare(
    `SELECT * FROM jobs ORDER BY started_at DESC LIMIT ? OFFSET ?`
  ).all(limit, offset) as Record<string, unknown>[];
}

export function getJobsFiltered(opts: {
  status?: string; reportDate?: string; limit?: number; offset?: number;
}): Record<string, unknown>[] {
  const { status, reportDate, limit = 20, offset = 0 } = opts;
  const clauses: string[] = [];
  const params: unknown[] = [];
  if (status && status !== "all") { clauses.push("status = ?"); params.push(status); }
  if (reportDate) { clauses.push("report_date = ?"); params.push(reportDate); }
  const where = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
  return db().prepare(
    `SELECT * FROM jobs ${where} ORDER BY started_at DESC LIMIT ? OFFSET ?`
  ).all(...params, limit, offset) as Record<string, unknown>[];
}

export function countJobsFiltered(opts: { status?: string; reportDate?: string }): number {
  const { status, reportDate } = opts;
  const clauses: string[] = [];
  const params: unknown[] = [];
  if (status && status !== "all") { clauses.push("status = ?"); params.push(status); }
  if (reportDate) { clauses.push("report_date = ?"); params.push(reportDate); }
  const where = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
  const row = db().prepare(`SELECT COUNT(*) as n FROM jobs ${where}`).get(...params) as { n: number };
  return row.n;
}

export function getSetting(key: string, fallback = ""): string {
  const row = db().prepare("SELECT value FROM settings WHERE key = ?").get(key) as { value: string } | undefined;
  return row ? row.value : fallback;
}

export function setSetting(key: string, value: string): void {
  db().prepare(
    `INSERT INTO settings (key, value) VALUES (?, ?)
     ON CONFLICT(key) DO UPDATE SET value = excluded.value`
  ).run(key, value);
}
