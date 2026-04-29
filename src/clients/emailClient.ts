import { settings } from "../config.js";

export interface EmailPayload { to: string; subject: string; html: string; text: string; }
export interface SendResult   { success: boolean; error?: string; status_code?: number; }

export async function sendEmail(payload: EmailPayload): Promise<SendResult> {
  if (!payload.to.trim())      return { success: false, error: "'to' address is empty" };
  if (!payload.subject.trim()) return { success: false, error: "'subject' is empty" };

  const maxRetries = parseInt(process.env.EMAIL_MAX_RETRIES ?? "3");
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const res = await fetch(settings.emailApiUrl, {
        method: "POST",
        headers: { token: settings.emailApiToken, "Content-Type": "application/json" },
        body: JSON.stringify({ to: payload.to, subject: payload.subject, text: payload.html }),
        signal: AbortSignal.timeout(settings.httpTimeout),
      });
      if (res.ok) return { success: true, status_code: res.status };
      const msg = `HTTP ${res.status}: ${(await res.text()).slice(0, 200)}`;
      if (attempt === maxRetries) return { success: false, error: msg, status_code: res.status };
    } catch (e: any) {
      if (attempt === maxRetries) return { success: false, error: String(e?.message ?? e) };
    }
    await new Promise(r => setTimeout(r, Math.pow(2, attempt) * 1000));
  }
  return { success: false, error: "Max retries exceeded" };
}
