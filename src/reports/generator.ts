import type { PatientRecord } from "../processing/filter.js";
import { summariseGroup } from "../processing/grouper.js";

type Analytics = { table_a: any[]; table_b: any[]; table_c: any[]; total_records: number };

function tableARows(rows: any[]): string {
  return rows.map(r => `
    <tr>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;">${r.date}</td>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;text-align:right;">${r.hygiene}</td>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;text-align:right;">${r.restorative}</td>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;text-align:right;font-weight:700;">${r.total}</td>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;text-align:right;">${r.reschedule_count}</td>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;text-align:right;">${r.new_appt_count}</td>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;text-align:right;">${r.completed}</td>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;text-align:right;">${r.other}</td>
    </tr>`).join("");
}

function tableBRows(rows: any[]): string {
  return rows.map(r => `
    <tr>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;">${r.category}</td>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;text-align:right;font-weight:700;">${r.production}</td>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;text-align:right;">${r.reschedule_count}</td>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;text-align:right;">${r.new_appt_count}</td>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;text-align:right;">${r.completed}</td>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;text-align:right;">${r.other}</td>
    </tr>`).join("");
}

function tableCRows(rows: any[]): string {
  return rows.map(r => `
    <tr>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;">${r.date}</td>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;text-align:right;font-weight:700;">${r.new_prod}</td>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;text-align:right;font-weight:700;">${r.reschedule_prod}</td>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;text-align:right;">${r.new_count}</td>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;text-align:right;">${r.reschedule_count}</td>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;text-align:right;">${r.new_completed}</td>
      <td style="padding:7px 10px;border:1px solid #e4e4e7;text-align:right;">${r.reschedule_completed}</td>
    </tr>`).join("");
}

const TH = `padding:8px 10px;border:1px solid #e4e4e7;font-weight:700;background:#f4f4f5;`;

function analyticsSection(analytics: Analytics): string {
  if (analytics.total_records === 0) {
    return `<p style="color:#71717a;font-size:13px;">No appointment data available for the previous day.</p>`;
  }
  const a = analytics.table_a.length
    ? `<table width="100%" cellpadding="0" cellspacing="0" border="0" style="font-size:13px;border-collapse:collapse;margin-bottom:8px;">
        <tr><th style="${TH}text-align:left;">Date</th><th style="${TH}text-align:right;">Hygiene</th><th style="${TH}text-align:right;">Restorative</th><th style="${TH}text-align:right;">Total</th><th style="${TH}text-align:right;">Reschedules</th><th style="${TH}text-align:right;">New Appts</th><th style="${TH}text-align:right;">Completed</th><th style="${TH}text-align:right;">Other</th></tr>
        ${tableARows(analytics.table_a)}
       </table>`
    : `<p style="font-size:13px;color:#71717a;">No data available.</p>`;

  const b = analytics.table_b.length
    ? `<table width="100%" cellpadding="0" cellspacing="0" border="0" style="font-size:13px;border-collapse:collapse;margin-bottom:8px;">
        <tr><th style="${TH}text-align:left;">Procedure Category</th><th style="${TH}text-align:right;">Net Production</th><th style="${TH}text-align:right;">Reschedules</th><th style="${TH}text-align:right;">New Appts</th><th style="${TH}text-align:right;">Completed</th><th style="${TH}text-align:right;">Other</th></tr>
        ${tableBRows(analytics.table_b)}
       </table>`
    : `<p style="font-size:13px;color:#71717a;">No data available.</p>`;

  const c = analytics.table_c.length
    ? `<table width="100%" cellpadding="0" cellspacing="0" border="0" style="font-size:13px;border-collapse:collapse;margin-bottom:8px;">
        <tr><th style="${TH}text-align:left;">Date</th><th style="${TH}text-align:right;">New Bookings</th><th style="${TH}text-align:right;">Reschedules</th><th style="${TH}text-align:right;">New Count</th><th style="${TH}text-align:right;">Reschedule Count</th><th style="${TH}text-align:right;">New Completed</th><th style="${TH}text-align:right;">Reschedule Completed</th></tr>
        ${tableCRows(analytics.table_c)}
       </table>`
    : `<p style="font-size:13px;color:#71717a;">No data available.</p>`;

  return `
    <p style="margin:20px 0 8px;"><strong>A. Adjusted Net Production — Hygiene vs. Restorative by Day</strong></p>${a}
    <p style="margin:20px 0 8px;"><strong>B. Adjusted Net Production by Procedure Category</strong></p>${b}
    <p style="margin:20px 0 8px;"><strong>C. Net Production — Reschedule vs. New Booking by Day</strong></p>${c}`;
}

export function renderReport(
  clinicName: string,
  records: PatientRecord[],
  reportDate: string,
  analytics: Analytics,
): { html: string; text: string } {
  const s = summariseGroup(records);
  const formattedDate = new Date(reportDate + "T12:00:00Z").toLocaleDateString("en-US", {
    month: "long", day: "2-digit", year: "numeric", timeZone: "UTC",
  });

  const html = `<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"><title>Daily Patient Report</title></head>
<body style="margin:0;padding:0;background:#fff;font-family:Arial,Helvetica,sans-serif;font-size:15px;line-height:1.8;color:#1a1a1a;">
<div style="max-width:640px;margin:0 auto;padding:40px 24px;">
<p>Good evening,</p>
<p>Please find below the summary of patient communication activity and appointment performance for <strong>${formattedDate}</strong>.</p>
<hr style="border:none;border-top:1px solid #d4d4d8;margin:24px 0;">
<p><strong>Call Metrics</strong><br>Total Calls: ${s.total_calls}<br>Inbound: ${s.inbound_calls}<br>Outbound: ${s.outbound_calls}</p>
<p><strong>Call Duration</strong><br>Inbound Duration: ${s.inbound_duration}<br>Outbound Duration: ${s.outbound_duration}</p>
<p><strong>Appointment Performance</strong><br>Opportunities Identified: ${s.appt_opportunity}<br>Appointments Booked: ${s.appt_booked}<br>Confirmations Initiated: ${s.confirmations}<br>Successful Confirmations: ${s.confirmation_success}<br>Successful Reschedules: ${s.reschedule_success}</p>
<hr style="border:none;border-top:1px solid #d4d4d8;margin:24px 0;">
<p style="margin:0 0 4px;"><strong>Voice AI Daily Analytics</strong> <span style="font-size:13px;color:#71717a;">— Previous Day</span></p>
${analyticsSection(analytics)}
<hr style="border:none;border-top:1px solid #d4d4d8;margin:24px 0;">
<p style="color:#52525b;">This is an automated report generated by PatientXpress AI Voice, covering activity between <strong>12:00 AM and 11:59 PM</strong>.<br><br>If you have any questions or would like additional insights, please let us know.</p>
<p>Best regards,<br><strong>PatientXpress Team</strong><br><a href="mailto:info@patientxpress.us" style="color:#1a1a1a;">info@patientxpress.us</a></p>
<p style="color:#71717a;font-size:13px;">PatientXpress<br>30021 Alicia Parkway<br>Laguna Niguel, CA 92677<br>United States</p>
</div></body></html>`;

  const text = `Good evening,\n\nPlease find below the summary for ${formattedDate}.\n\nTotal Calls: ${s.total_calls}\nInbound: ${s.inbound_calls} | Outbound: ${s.outbound_calls}\nOpportunities: ${s.appt_opportunity} | Booked: ${s.appt_booked}\nConfirmations: ${s.confirmations} | Successful: ${s.confirmation_success}\nReschedules: ${s.reschedule_success}\n\nBest regards,\nPatientXpress Team`;

  return { html, text };
}
