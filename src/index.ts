import express from "express";
import cors from "cors";
import { WebSocketServer } from "ws";
import http from "http";
import { randomUUID } from "crypto";

import { initDb } from "./db.js";
import * as db from "./db.js";
import { broadcaster } from "./logStream.js";
import { settings } from "./config.js";
import {
  getClinicsStatus,
  previewClinic,
  sendClinic,
  sendAll,
} from "./pipeline.js";
import {
  getSchedulerState,
  setPaused,
  startSchedulerLoop,
} from "./scheduler.js";

// ── Init DB ───────────────────────────────────────────────────────────────────
initDb();

// ── In-memory batch tracking ──────────────────────────────────────────────────
const batches = new Map<string, { status: string; results: unknown[] }>();

// ── CORS ──────────────────────────────────────────────────────────────────────
// In production set ALLOWED_ORIGINS to your Netlify URL, e.g.:
//   ALLOWED_ORIGINS=https://your-site.netlify.app
// Multiple origins can be comma-separated.
const rawOrigins = process.env.ALLOWED_ORIGINS ?? "";
const allowedOrigins = rawOrigins
  .split(",")
  .map(o => o.trim())
  .filter(Boolean);

const corsOptions: cors.CorsOptions = {
  origin: allowedOrigins.length
    ? (origin, cb) => {
        // Allow requests with no origin (server-to-server, curl, etc.)
        if (!origin || allowedOrigins.includes(origin)) return cb(null, true);
        cb(null, false); // reject — browser will see CORS block, not 500
      }
    : true, // allow all when ALLOWED_ORIGINS is not set (local dev)
  credentials: true,
};

// ── Express app ───────────────────────────────────────────────────────────────
const app = express();
app.use(cors(corsOptions));
app.use(express.json());

// GET /config
app.get("/config", (_req, res) => {
  res.json({
    send_hour:         settings.sendHour,
    send_minute:       settings.sendMinute,
    timezone:          settings.timezone,
    fetch_license_key: settings.fetchLicenseKey,
    test_email:        settings.testEmail,
  });
});

// GET /scheduler
app.get("/scheduler", (_req, res) => {
  res.json(getSchedulerState());
});

// POST /scheduler/pause
app.post("/scheduler/pause", (_req, res) => {
  setPaused(true);
  res.json(getSchedulerState());
});

// POST /scheduler/resume
app.post("/scheduler/resume", (_req, res) => {
  setPaused(false);
  res.json(getSchedulerState());
});

// POST /scheduler/trigger
app.post("/scheduler/trigger", async (req, res) => {
  const { target_date } = req.body ?? {};
  const batchId = randomUUID();
  batches.set(batchId, { status: "running", results: [] });

  sendAll(target_date, "scheduler").then(results => {
    batches.set(batchId, { status: "done", results });
  }).catch(e => {
    batches.set(batchId, { status: "failed", results: [{ error: String(e?.message ?? e) }] });
  });

  res.json({ batch_id: batchId, status: "running" });
});

// GET /clinics
app.get("/clinics", async (req, res) => {
  try {
    const targetDate = String(req.query.target_date ?? "").trim() || undefined;
    const clinics = await getClinicsStatus(targetDate);
    const report_date = clinics[0]?.report_date ?? null;
    res.json({ clinics, report_date });
  } catch (e: any) {
    res.status(500).json({ detail: e?.message ?? "Internal error" });
  }
});

// GET /clinics/:licenseKey/preview
app.get("/clinics/:licenseKey/preview", async (req, res) => {
  try {
    const targetDate = String(req.query.target_date ?? "").trim() || undefined;
    const preview = await previewClinic(req.params.licenseKey, targetDate);
    res.json(preview);
  } catch (e: any) {
    res.status(500).json({ detail: e?.message ?? "Internal error" });
  }
});

// POST /clinics/:licenseKey/send
app.post("/clinics/:licenseKey/send", async (req, res) => {
  try {
    const { target_date, override_email } = req.body ?? {};
    const result = await sendClinic(
      req.params.licenseKey,
      target_date,
      "manual",
      override_email,
    );
    res.json(result);
  } catch (e: any) {
    res.status(500).json({ detail: e?.message ?? "Internal error" });
  }
});

// POST /send-all
app.post("/send-all", async (req, res) => {
  const { target_date, override_email } = req.body ?? {};
  const batchId = randomUUID();
  batches.set(batchId, { status: "running", results: [] });

  sendAll(target_date, "manual", override_email).then(results => {
    batches.set(batchId, { status: "done", results });
  }).catch(e => {
    batches.set(batchId, { status: "failed", results: [{ error: String(e?.message ?? e) }] });
  });

  res.json({ batch_id: batchId, status: "running" });
});

// GET /send-all/:batchId
app.get("/send-all/:batchId", (req, res) => {
  const batch = batches.get(req.params.batchId);
  if (!batch) return res.status(404).json({ detail: "Batch not found" });
  res.json(batch);
});

// GET /jobs
app.get("/jobs", (req, res) => {
  try {
    const limit      = Math.min(parseInt(String(req.query.limit  ?? 20)), 200);
    const offset     = parseInt(String(req.query.offset ?? 0));
    const status     = String(req.query.status      ?? "").trim() || undefined;
    const reportDate = String(req.query.report_date ?? "").trim() || undefined;

    const jobs  = db.getJobsFiltered({ status, reportDate, limit, offset });
    const total = db.countJobsFiltered({ status, reportDate });
    res.json({ jobs, total, limit, offset });
  } catch (e: any) {
    res.status(500).json({ detail: e?.message ?? "Internal error" });
  }
});

// ── HTTP + WebSocket server ───────────────────────────────────────────────────
// Render sets $PORT automatically; fall back to EMAIL_AGENT_PORT for local dev.
const PORT = parseInt(process.env.PORT ?? process.env.EMAIL_AGENT_PORT ?? "8000");
const server = http.createServer(app);

const wss = new WebSocketServer({ server, path: "/ws/logs" });
wss.on("connection", ws => broadcaster.connect(ws));

server.listen(PORT, () => {
  console.log(`[email-agent] Server running on port ${PORT}`);
  startSchedulerLoop();
});
