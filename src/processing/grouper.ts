import type { PatientRecord } from "./filter.js";

function safePct(part: number, total: number): number {
  return total === 0 ? 0 : Math.round((part / total) * 100);
}

function formatDuration(ms: number): string {
  if (ms <= 0) return "0s";
  const secs = Math.floor(ms / 1000);
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = secs % 60;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

function flagCount(records: PatientRecord[], field: string): number {
  return records.filter(r => {
    const v = r[field];
    if (typeof v === "boolean") return v;
    return String(v) === "1" || String(v) === "true";
  }).length;
}

export function summariseGroup(records: PatientRecord[]): Record<string, unknown> {
  if (!records.length) return {};
  const total = records.length;
  const inbound  = records.filter(r => r.call_direction === "inbound");
  const outbound = records.filter(r => r.call_direction === "outbound");
  const inboundMs  = inbound.reduce((s, r)  => s + (Number(r.duration_ms) || 0), 0);
  const outboundMs = outbound.reduce((s, r) => s + (Number(r.duration_ms) || 0), 0);

  const apptOpportunity     = flagCount(records, "appt_opportunity");
  const apptBooked          = flagCount(records, "is_appointment_booked");
  const confirmations       = flagCount(records, "is_confirmation");
  const confirmationSuccess = flagCount(records, "confirmation_success");
  const rescheduleSuccess   = flagCount(records, "reschedule_success");

  const inboundPct  = safePct(inbound.length, total);
  const funnel      = Math.max(apptOpportunity, 1);

  return {
    total_calls:               total,
    inbound_calls:             inbound.length,
    outbound_calls:            outbound.length,
    inbound_duration:          formatDuration(inboundMs),
    outbound_duration:         formatDuration(outboundMs),
    appt_opportunity:          apptOpportunity,
    appt_booked:               apptBooked,
    confirmations,
    confirmation_success:      confirmationSuccess,
    reschedule_success:        rescheduleSuccess,
    inbound_pct:               inboundPct,
    outbound_pct:              100 - inboundPct,
    appt_booked_pct:           Math.min(safePct(apptBooked,          funnel), 100),
    confirmations_pct:         Math.min(safePct(confirmations,       funnel), 100),
    confirmation_success_pct:  Math.min(safePct(confirmationSuccess, funnel), 100),
  };
}
