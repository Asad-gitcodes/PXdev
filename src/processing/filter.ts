// Filter patient records to the full previous day (00:00–23:59) in the given timezone.

export interface PatientRecord {
  license_key: string;
  created_at: string;
  call_direction: string;
  duration_ms: number;
  appt_opportunity: string;
  is_appointment_booked: boolean;
  is_confirmation: string;
  confirmation_success: string;
  reschedule_success: string;
  [key: string]: unknown;
}

function localDateStr(date: Date, timezone: string): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date);
}

function reportDateStr(targetDate: string): string {
  // Report date = one day before targetDate
  const [y, m, d] = targetDate.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d - 1));
  return dt.toISOString().slice(0, 10);
}

/**
 * Extract a YYYY-MM-DD date string from the API's startTime field.
 * Format: "MM/DD/YYYY, HH:MM:SS AM/PM"  (clinic local time — same TZ the API uses for startDate/endDate)
 */
function parseDateFromStartTime(raw: unknown): string | null {
  const m = String(raw ?? "").match(/^(\d{2})\/(\d{2})\/(\d{4})/);
  if (!m) return null;
  return `${m[3]}-${m[1]}-${m[2]}`; // → YYYY-MM-DD
}

export function filterRecordsForDay(
  records: PatientRecord[],
  targetDate: string,
  timezone = "UTC"
): PatientRecord[] {
  if (!records.length) return [];
  const report = reportDateStr(targetDate);
  return records.filter((r) => {
    // Prefer startTime — the API stores it in clinic local time and uses it for
    // its own startDate/endDate filtering, so it's always the correct date.
    const startTimeDate = parseDateFromStartTime(r["startTime"]);
    if (startTimeDate) return startTimeDate === report;

    // Fall back to createdAt converted to the configured timezone.
    if (!r.created_at) return false;
    try {
      return localDateStr(new Date(r.created_at), timezone) === report;
    } catch {
      return false;
    }
  });
}
