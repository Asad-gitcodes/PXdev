import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, "..", ".env") });

export const settings = {
  patientApiBaseUrl: process.env.PATIENT_API_BASE_URL ?? "",
  patientApiToken:   process.env.PATIENT_API_TOKEN   ?? "",
  clinicApiBaseUrl:  process.env.CLINIC_API_BASE_URL ?? "",
  clinicApiToken:    process.env.CLINIC_API_TOKEN    ?? "",
  queryApiUrl:       process.env.QUERY_API_URL       ?? "",
  queryApiToken:     process.env.QUERY_API_TOKEN     ?? "",
  emailApiUrl:       process.env.EMAIL_API_URL       ?? "",
  emailApiToken:     process.env.EMAIL_API_TOKEN     ?? "",
  fetchLicenseKey:   process.env.FETCH_LICENSE_KEY   ?? "",
  testEmail:         process.env.TEST_EMAIL          ?? "",
  httpTimeout:       parseInt(process.env.HTTP_TIMEOUT  ?? "10") * 1000,
  timezone:          process.env.TIMEZONE             ?? "UTC",
  schedulerTimezone: process.env.SCHEDULER_TIMEZONE   ?? "America/Los_Angeles",
  sendHour:          parseInt(process.env.SEND_HOUR   ?? "20"),
  sendMinute:        parseInt(process.env.SEND_MINUTE ?? "0"),
};
