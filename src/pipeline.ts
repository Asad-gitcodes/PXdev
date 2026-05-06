import { settings } from "./config.js";
import * as db from "./db.js";
import { broadcaster } from "./logStream.js";
import { getPatientRecords, getPatientRecordsForRange } from "./clients/patientClient.js";
import { getClinicByLicenseKey } from "./clients/clinicClient.js";
import { getAppointments, getAppointmentsForRange } from "./clients/queryClient.js";
import { sendEmail } from "./clients/emailClient.js";
import { filterRecordsForDay, filterRecordsForRange } from "./processing/filter.js";
import { buildAnalytics } from "./processing/analytics.js";
import { renderReport } from "./reports/generator.js";

// ── In-memory record cache (5 min TTL, one date at a time) ──────────────────
type CacheEntry = { records: any[]; fetchedAt: number };
const recordCache = new Map<string, CacheEntry>();
const CACHE_TTL = 300_000;

function getCached(cacheKey: string): any[] | null {
  const entry = recordCache.get(cacheKey);
  if (entry && Date.now() - entry.fetchedAt < CACHE_TTL) {
    return entry.records;
  }
  return null;
}
function setCached(cacheKey: string, records: any[]): void {
  recordCache.set(cacheKey, { records, fetchedAt: Date.now() });
}

// ── Date helpers ─────────────────────────────────────────────────────────────
function reportDate(targetDate?: string): string {
  const run = targetDate ? new Date(targetDate + "T12:00:00Z") : new Date();
  run.setUTCDate(run.getUTCDate() - 1);
  return run.toISOString().slice(0, 10);
}

function previousFullMonthRange(anchorDate?: string): { monthStart: string; monthEnd: string } {
  const anchor = anchorDate ? new Date(`${anchorDate}T12:00:00Z`) : new Date();
  const previousMonthStart = new Date(Date.UTC(anchor.getUTCFullYear(), anchor.getUTCMonth() - 1, 1));
  const previousMonthEnd = new Date(Date.UTC(anchor.getUTCFullYear(), anchor.getUTCMonth(), 0));
  return {
    monthStart: previousMonthStart.toISOString().slice(0, 10),
    monthEnd: previousMonthEnd.toISOString().slice(0, 10),
  };
}

function formatDateLabel(ymd: string): string {
  return new Date(`${ymd}T12:00:00Z`).toLocaleDateString("en-US", {
    month: "long",
    day: "2-digit",
    year: "numeric",
    timeZone: "UTC",
  });
}

async function fetchRecords(repDate: string): Promise<any[]> {
  const cached = getCached(repDate);
  if (cached) return cached;
  const records = await getPatientRecords(repDate, settings.fetchLicenseKey);
  setCached(repDate, records);
  return records;
}

async function fetchRecordsForRange(startDate: string, endDate: string): Promise<any[]> {
  const cacheKey = `${startDate}:${endDate}`;
  const cached = getCached(cacheKey);
  if (cached) return cached;
  const records = await getPatientRecordsForRange(startDate, endDate, settings.fetchLicenseKey);
  setCached(cacheKey, records);
  return records;
}

// ── Clinic listing ────────────────────────────────────────────────────────────
export async function getClinicsStatus(targetDate?: string) {
  const runDate = targetDate ?? new Date().toISOString().slice(0, 10);
  const repDate = reportDate(targetDate);

  let allRecords: any[] = [];
  try { allRecords = await fetchRecords(repDate); } catch { /* return empty list on error */ }

  const seen = new Set<string>();
  const licenseKeys: string[] = [];
  for (const r of allRecords) {
    const k = String(r.license_key ?? "").trim();
    if (k && !seen.has(k)) { seen.add(k); licenseKeys.push(k); }
  }

  return Promise.all(licenseKeys.map(async lk => {
    const clinicRecords = allRecords.filter(r => r.license_key === lk);
    const filtered = filterRecordsForDay(clinicRecords, runDate, settings.timezone);
    const clinic = await getClinicByLicenseKey(lk);
    return {
      license_key:    lk,
      clinic_name:    clinic?.clinic_name  ?? "Unknown",
      email:          clinic?.email        ?? "",
      is_active:      clinic?.is_active    ?? false,
      records_found:  clinicRecords.length,
      records_passed: filtered.length,
      report_date:    repDate,
    };
  }));
}

// ── Preview ──────────────────────────────────────────────────────────────────
export async function previewClinic(licenseKey: string, targetDate?: string) {
  const runDate = targetDate ?? new Date().toISOString().slice(0, 10);
  const repDate = reportDate(targetDate);

  const [allRecords, clinic, rawQuery] = await Promise.all([
    fetchRecords(repDate),
    getClinicByLicenseKey(licenseKey),
    getAppointments(licenseKey, repDate),
  ]);

  if (!clinic) throw new Error("Clinic not found");

  const clinicRecords = allRecords.filter(r => r.license_key === licenseKey);
  const filtered = filterRecordsForDay(clinicRecords, runDate, settings.timezone);
  const analytics = buildAnalytics(rawQuery);
  const { html } = renderReport(clinic.clinic_name, filtered, repDate, analytics);

  return {
    license_key:       licenseKey,
    clinic_name:       clinic.clinic_name,
    email:             clinic.email,
    report_date:       repDate,
    records_found:     clinicRecords.length,
    records_passed:    filtered.length,
    analytics_records: analytics.total_records,
    email_subject:     `Daily Patient Report - ${new Date(repDate + "T12:00:00Z").toLocaleDateString("en-US", { month: "long", day: "2-digit", year: "numeric", timeZone: "UTC" })}`,
    html,
  };
}

export async function previewClinicMonthly(
  licenseKey: string,
  monthStart?: string,
  monthEnd?: string,
  anchorDate?: string,
) {
  const range = monthStart && monthEnd ? { monthStart, monthEnd } : previousFullMonthRange(anchorDate);
  const { monthStart: startDate, monthEnd: endDate } = range;

  const [allRecords, clinic, rawQuery] = await Promise.all([
    fetchRecordsForRange(startDate, endDate),
    getClinicByLicenseKey(licenseKey),
    getAppointmentsForRange(licenseKey, startDate, endDate),
  ]);

  if (!clinic) throw new Error("Clinic not found");

  const clinicRecords = allRecords.filter(r => r.license_key === licenseKey);
  const filtered = filterRecordsForRange(clinicRecords, startDate, endDate, settings.timezone);
  const analytics = buildAnalytics(rawQuery);
  const formattedRange = `${formatDateLabel(startDate)} - ${formatDateLabel(endDate)}`;
  const { html } = renderReport(clinic.clinic_name, filtered, endDate, analytics, {
    title: "Monthly Patient Report",
    reportDateLabel: formattedRange,
    analyticsHeading: "Voice AI Monthly Analytics",
    analyticsSubheading: "Previous Full Month",
    analyticsEmptyMessage: "No appointment data available for the previous full month.",
    windowDescription:
      `This is an automated monthly report generated by PatientXpress AI Voice, covering activity between <strong>${formatDateLabel(startDate)}</strong> and <strong>${formatDateLabel(endDate)}</strong>.`,
  });

  return {
    license_key: licenseKey,
    clinic_name: clinic.clinic_name,
    email: clinic.email,
    report_date: endDate,
    records_found: clinicRecords.length,
    records_passed: filtered.length,
    analytics_records: analytics.total_records,
    email_subject: `Monthly Patient Report - ${new Date(`${endDate}T12:00:00Z`).toLocaleDateString("en-US", {
      month: "long",
      year: "numeric",
      timeZone: "UTC",
    })}`,
    html,
  };
}

// ── Send single clinic ────────────────────────────────────────────────────────
export async function sendClinic(
  licenseKey: string,
  targetDate?: string,
  triggeredBy = "manual",
  overrideEmail?: string,
): Promise<Record<string, unknown>> {
  const runDate = targetDate ?? new Date().toISOString().slice(0, 10);
  const repDate = reportDate(targetDate);
  const jobId = db.createJob(licenseKey, repDate, triggeredBy);

  broadcaster.emitStatus("pipeline_start", { license_key: licenseKey, report_date: repDate });

  try {
    broadcaster.emit(`Fetching records for ${repDate}…`);
    const allRecords = await fetchRecords(repDate);
    const clinicRecords = allRecords.filter(r => r.license_key === licenseKey);
    broadcaster.emit(`Found ${clinicRecords.length} records for this clinic`);

    const filtered = filterRecordsForDay(clinicRecords, runDate, settings.timezone);
    broadcaster.emit(`Filter: ${filtered.length} of ${clinicRecords.length} records in window`, filtered.length ? "info" : "warning");

    const clinic = await getClinicByLicenseKey(licenseKey);
    if (!clinic) {
      broadcaster.emit("Clinic not found in registry", "error");
      broadcaster.emitStatus("pipeline_end", { result: "failed" });
      db.updateJob(jobId, { status: "failed", error_message: "Clinic not found" });
      return { job_id: jobId, status: "failed", reason: "Clinic not found" };
    }
    broadcaster.emit(`Clinic: ${clinic.clinic_name}`, "success");

    if (!filtered.length) {
      broadcaster.emit("No records in window — skipping", "warning");
      broadcaster.emitStatus("pipeline_end", { result: "skipped" });
      db.updateJob(jobId, { status: "skipped", clinic_name: clinic.clinic_name, records_found: clinicRecords.length, records_passed: 0, error_message: "No records in window" });
      return { job_id: jobId, status: "skipped", clinic_name: clinic.clinic_name, reason: "No records in window" };
    }

    broadcaster.emit("Fetching Voice AI analytics…");
    const rawQuery = await getAppointments(licenseKey, repDate);
    const analytics = buildAnalytics(rawQuery);
    broadcaster.emit(`Analytics: ${analytics.total_records} records across 3 tables`);

    broadcaster.emit("Rendering HTML email report…");
    const subject = `Daily Patient Report - ${new Date(repDate + "T12:00:00Z").toLocaleDateString("en-US", { month: "long", day: "2-digit", year: "numeric", timeZone: "UTC" })}`;
    const { html, text } = renderReport(clinic.clinic_name, filtered, repDate, analytics);
    broadcaster.emit(`Report rendered — ${html.length.toLocaleString()} bytes`, "success");

    const sendTo = overrideEmail ?? clinic.email;
    broadcaster.emit(`Sending email → ${sendTo}`);
    const result = await sendEmail({ to: sendTo, subject, html, text });

    if (result.success) {
      broadcaster.emit("Email sent successfully ✓", "success");
      broadcaster.emitStatus("pipeline_end", { result: "sent", clinic_name: clinic.clinic_name });
      db.updateJob(jobId, { status: "sent", clinic_name: clinic.clinic_name, records_found: clinicRecords.length, records_passed: filtered.length, analytics_records: analytics.total_records, email_to: sendTo, email_subject: subject });
      return { job_id: jobId, status: "sent", clinic_name: clinic.clinic_name, email_to: sendTo };
    }

    broadcaster.emit(`Send failed: ${result.error}`, "error");
    broadcaster.emitStatus("pipeline_end", { result: "failed" });
    db.updateJob(jobId, { status: "failed", clinic_name: clinic.clinic_name, records_found: clinicRecords.length, records_passed: filtered.length, error_message: result.error });
    return { job_id: jobId, status: "failed", reason: result.error };

  } catch (e: any) {
    const msg = String(e?.message ?? e).slice(0, 400);
    broadcaster.emit(`Unexpected error: ${msg}`, "error");
    broadcaster.emitStatus("pipeline_end", { result: "failed" });
    db.updateJob(jobId, { status: "failed", error_message: msg });
    return { job_id: jobId, status: "failed", reason: msg };
  }
}

export async function sendClinicMonthly(
  licenseKey: string,
  monthStart?: string,
  monthEnd?: string,
  anchorDate?: string,
  triggeredBy = "manual",
  overrideEmail?: string,
): Promise<Record<string, unknown>> {
  const range = monthStart && monthEnd ? { monthStart, monthEnd } : previousFullMonthRange(anchorDate);
  const { monthStart: startDate, monthEnd: endDate } = range;
  const jobId = db.createJob(licenseKey, endDate, triggeredBy);

  broadcaster.emitStatus("pipeline_start", {
    license_key: licenseKey,
    report_date: endDate,
    month_start: startDate,
    month_end: endDate,
  });

  try {
    broadcaster.emit(`Fetching monthly records for ${startDate} through ${endDate}…`);
    const allRecords = await fetchRecordsForRange(startDate, endDate);
    const clinicRecords = allRecords.filter(r => r.license_key === licenseKey);
    broadcaster.emit(`Found ${clinicRecords.length} monthly records for this clinic`);

    const filtered = filterRecordsForRange(clinicRecords, startDate, endDate, settings.timezone);
    broadcaster.emit(
      `Monthly filter: ${filtered.length} of ${clinicRecords.length} records in window`,
      filtered.length ? "info" : "warning",
    );

    const clinic = await getClinicByLicenseKey(licenseKey);
    if (!clinic) {
      broadcaster.emit("Clinic not found in registry", "error");
      broadcaster.emitStatus("pipeline_end", { result: "failed" });
      db.updateJob(jobId, { status: "failed", error_message: "Clinic not found" });
      return { job_id: jobId, status: "failed", reason: "Clinic not found" };
    }
    broadcaster.emit(`Clinic: ${clinic.clinic_name}`, "success");

    if (!filtered.length) {
      broadcaster.emit("No monthly records in window — skipping", "warning");
      broadcaster.emitStatus("pipeline_end", { result: "skipped" });
      db.updateJob(jobId, {
        status: "skipped",
        clinic_name: clinic.clinic_name,
        records_found: clinicRecords.length,
        records_passed: 0,
        error_message: `No records in monthly window (${startDate} to ${endDate})`,
      });
      return { job_id: jobId, status: "skipped", clinic_name: clinic.clinic_name, reason: "No records in monthly window" };
    }

    broadcaster.emit("Fetching monthly Voice AI analytics…");
    const rawQuery = await getAppointmentsForRange(licenseKey, startDate, endDate);
    const analytics = buildAnalytics(rawQuery);
    broadcaster.emit(`Monthly analytics: ${analytics.total_records} records across 3 tables`);

    broadcaster.emit("Rendering monthly HTML email report…");
    const formattedRange = `${formatDateLabel(startDate)} - ${formatDateLabel(endDate)}`;
    const subject = `Monthly Patient Report - ${new Date(`${endDate}T12:00:00Z`).toLocaleDateString("en-US", {
      month: "long",
      year: "numeric",
      timeZone: "UTC",
    })}`;
    const { html, text } = renderReport(clinic.clinic_name, filtered, endDate, analytics, {
      title: "Monthly Patient Report",
      reportDateLabel: formattedRange,
      analyticsHeading: "Voice AI Monthly Analytics",
      analyticsSubheading: "Previous Full Month",
      analyticsEmptyMessage: "No appointment data available for the previous full month.",
      windowDescription:
        `This is an automated monthly report generated by PatientXpress AI Voice, covering activity between <strong>${formatDateLabel(startDate)}</strong> and <strong>${formatDateLabel(endDate)}</strong>.`,
    });
    broadcaster.emit(`Monthly report rendered — ${html.length.toLocaleString()} bytes`, "success");

    const sendTo = overrideEmail ?? clinic.email;
    broadcaster.emit(`Sending monthly email → ${sendTo}`);
    const result = await sendEmail({ to: sendTo, subject, html, text });

    if (result.success) {
      broadcaster.emit("Monthly email sent successfully ✓", "success");
      broadcaster.emitStatus("pipeline_end", { result: "sent", clinic_name: clinic.clinic_name });
      db.updateJob(jobId, {
        status: "sent",
        clinic_name: clinic.clinic_name,
        records_found: clinicRecords.length,
        records_passed: filtered.length,
        analytics_records: analytics.total_records,
        email_to: sendTo,
        email_subject: subject,
      });
      return { job_id: jobId, status: "sent", clinic_name: clinic.clinic_name, email_to: sendTo };
    }

    broadcaster.emit(`Monthly send failed: ${result.error}`, "error");
    broadcaster.emitStatus("pipeline_end", { result: "failed" });
    db.updateJob(jobId, {
      status: "failed",
      clinic_name: clinic.clinic_name,
      records_found: clinicRecords.length,
      records_passed: filtered.length,
      error_message: result.error,
    });
    return { job_id: jobId, status: "failed", reason: result.error };

  } catch (e: any) {
    const msg = String(e?.message ?? e).slice(0, 400);
    broadcaster.emit(`Unexpected monthly error: ${msg}`, "error");
    broadcaster.emitStatus("pipeline_end", { result: "failed" });
    db.updateJob(jobId, { status: "failed", error_message: msg });
    return { job_id: jobId, status: "failed", reason: msg };
  }
}

// ── Send all clinics ──────────────────────────────────────────────────────────
export async function sendAll(
  targetDate?: string,
  triggeredBy = "manual",
  overrideEmail?: string,
): Promise<Record<string, unknown>[]> {
  const repDate = reportDate(targetDate);
  let allRecords: any[] = [];
  try { allRecords = await fetchRecords(repDate); } catch (e: any) {
    broadcaster.emit(`Failed to fetch records: ${e?.message}`, "error");
    return [];
  }

  const seen = new Set<string>();
  const licenseKeys: string[] = [];
  for (const r of allRecords) {
    const k = String(r.license_key ?? "").trim();
    if (k && !seen.has(k)) { seen.add(k); licenseKeys.push(k); }
  }

  if (overrideEmail) broadcaster.emit(`TEST MODE — all emails → ${overrideEmail}`, "warning");
  broadcaster.emit(`Discovered ${licenseKeys.length} clinic(s) from live data…`);

  const results: Record<string, unknown>[] = [];
  for (let i = 0; i < licenseKeys.length; i++) {
    broadcaster.emit(`── Clinic ${i + 1} of ${licenseKeys.length} ──`);
    results.push(await sendClinic(licenseKeys[i], targetDate, triggeredBy, overrideEmail));
  }

  const sent    = results.filter(r => r.status === "sent").length;
  const failed  = results.filter(r => r.status === "failed").length;
  const skipped = results.filter(r => r.status === "skipped").length;
  broadcaster.emit(`All done — ${sent} sent, ${skipped} skipped, ${failed} failed`, failed === 0 ? "success" : "warning");
  return results;
}

export async function sendAllMonthly(
  monthStart?: string,
  monthEnd?: string,
  anchorDate?: string,
  triggeredBy = "manual",
  overrideEmail?: string,
): Promise<Record<string, unknown>[]> {
  const range = monthStart && monthEnd ? { monthStart, monthEnd } : previousFullMonthRange(anchorDate);
  const { monthStart: startDate, monthEnd: endDate } = range;

  let allRecords: any[] = [];
  try {
    allRecords = await fetchRecordsForRange(startDate, endDate);
  } catch (e: any) {
    broadcaster.emit(`Failed to fetch monthly records: ${e?.message}`, "error");
    return [];
  }

  const seen = new Set<string>();
  const licenseKeys: string[] = [];
  for (const r of allRecords) {
    const k = String(r.license_key ?? "").trim();
    if (k && !seen.has(k)) { seen.add(k); licenseKeys.push(k); }
  }

  if (overrideEmail) broadcaster.emit(`TEST MODE — all monthly emails → ${overrideEmail}`, "warning");
  broadcaster.emit(`Discovered ${licenseKeys.length} clinic(s) from monthly live data…`);

  const results: Record<string, unknown>[] = [];
  for (let i = 0; i < licenseKeys.length; i++) {
    broadcaster.emit(`── Monthly clinic ${i + 1} of ${licenseKeys.length} ──`);
    results.push(await sendClinicMonthly(
      licenseKeys[i],
      startDate,
      endDate,
      anchorDate,
      triggeredBy,
      overrideEmail,
    ));
  }

  const sent = results.filter(r => r.status === "sent").length;
  const failed = results.filter(r => r.status === "failed").length;
  const skipped = results.filter(r => r.status === "skipped").length;
  broadcaster.emit(
    `Monthly send complete — ${sent} sent, ${skipped} skipped, ${failed} failed`,
    failed === 0 ? "success" : "warning",
  );
  return results;
}
