import { settings } from "../config.js";

export interface ClinicInfo {
  license_key: string;
  clinic_name: string;
  email: string;
  is_active: boolean;
  is_sendable: boolean;
}

function parseClinic(raw: Record<string, unknown>): ClinicInfo {
  const safeBool = (v: unknown) => {
    if (typeof v === "boolean") return v;
    if (typeof v === "number") return v === 1;
    return ["1", "true"].includes(String(v ?? "").toLowerCase().trim());
  };
  const first = String(raw["FirstName"] ?? "").trim();
  const last  = String(raw["LastName"]  ?? "").trim();
  const name  = first && last && first !== last ? `${first} / ${last}` : first || last || "Unknown Clinic";
  const email = String(raw["Email"] ?? "").trim();
  const active = safeBool(raw["IsActive"]);
  return { license_key: String(raw["licenseKey"] ?? ""), clinic_name: name, email, is_active: active, is_sendable: active && !!email };
}

export async function getClinicByLicenseKey(licenseKey: string): Promise<ClinicInfo | null> {
  const filter = JSON.stringify({ Email: "", FirstName: "", LastName: "", licenseKey, Phone: "" });
  const params = new URLSearchParams({ page: "1", size: "100", searchFilter: filter });
  try {
    const res = await fetch(`${settings.clinicApiBaseUrl}?${params}`, {
      headers: { Authorization: settings.clinicApiToken },
      signal: AbortSignal.timeout(settings.httpTimeout),
    });
    if (!res.ok) throw new Error(`Clinic API ${res.status}`);
    const data = await res.json();
    const records: unknown[] = data.records ?? [];
    if (!records.length) return null;
    return parseClinic(records[0] as Record<string, unknown>);
  } catch {
    return null;
  }
}
