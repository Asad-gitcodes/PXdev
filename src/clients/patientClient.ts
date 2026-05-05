import { settings } from "../config.js";
import type { PatientRecord } from "../processing/filter.js";

const PAGE_SIZE = 100;

async function fetchPage(
  startDate: string,
  endDate: string,
  page: number,
  licenseKey = "",
): Promise<{ data: any[]; pagination: { totalPages: number } }> {
  const params = new URLSearchParams({
    startDate,
    endDate,
    size: String(PAGE_SIZE),
    page: String(page),
  });
  if (licenseKey) params.set("licenseKey", licenseKey);
  const res = await fetch(`${settings.patientApiBaseUrl}?${params}`, {
    headers: { Authorization: `Bearer ${settings.patientApiToken}` },
    signal: AbortSignal.timeout(settings.httpTimeout),
  });
  if (!res.ok) throw new Error(`Patient API ${res.status}: ${res.statusText}`);
  return res.json();
}

function parseRecord(raw: Record<string, unknown>): PatientRecord {
  const safeBool = (v: unknown): boolean => {
    if (typeof v === "boolean") return v;
    if (typeof v === "number") return v === 1;
    return String(v ?? "").trim() === "1" || String(v ?? "").trim().toLowerCase() === "true";
  };
  return {
    // spread raw first so explicit mappings below take precedence
    ...raw,
    license_key:            String(raw["licenseKey"] ?? ""),
    created_at:             String(raw["createdAt"] ?? ""),
    call_direction:         String(raw["callDirection"] ?? ""),
    duration_ms:            parseInt(String(raw["duration_ms"] ?? "0")) || 0,
    appt_opportunity:       String(raw["appt_opportunity"] ?? "0"),
    is_appointment_booked:  safeBool(raw["isAppointmentBooked"]),
    is_confirmation:        String(raw["is_confirmation"] ?? "0"),
    confirmation_success:   String(raw["confirmation_success"] ?? "0"),
    reschedule_success:     String(raw["reschedule_success"] ?? "0"),
  } as PatientRecord;
}

export async function getPatientRecords(targetDate: string, licenseKey = ""): Promise<PatientRecord[]> {
  return getPatientRecordsForRange(targetDate, targetDate, licenseKey);
}

export async function getPatientRecordsForRange(
  startDate: string,
  endDate: string,
  licenseKey = "",
): Promise<PatientRecord[]> {
  const all: PatientRecord[] = [];
  let page = 1;
  while (true) {
    const raw = await fetchPage(startDate, endDate, page, licenseKey);
    const items: unknown[] = raw.data ?? [];
    for (const item of items) {
      try { all.push(parseRecord(item as Record<string, unknown>)); } catch { /* skip bad record */ }
    }
    if (page >= (raw.pagination?.totalPages ?? 1) || !items.length) break;
    page++;
  }
  return all;
}
