import type { WebSocket } from "ws";

type LogMsg =
  | { type: "log"; level: string; event: string; timestamp: string; data?: unknown }
  | { type: "status"; status: string; timestamp: string; data?: unknown };

class LogBroadcaster {
  private clients: Set<WebSocket> = new Set();

  connect(ws: WebSocket): void {
    this.clients.add(ws);
    ws.on("close", () => this.clients.delete(ws));
    ws.on("error", () => this.clients.delete(ws));
  }

  private send(msg: LogMsg): void {
    const payload = JSON.stringify(msg);
    for (const ws of this.clients) {
      try {
        if (ws.readyState === 1 /* OPEN */) ws.send(payload);
      } catch {
        this.clients.delete(ws);
      }
    }
  }

  emit(event: string, level = "info"): void {
    this.send({ type: "log", level, event, timestamp: now() });
  }

  emitStatus(status: string, data?: Record<string, unknown>): void {
    this.send({ type: "status", status, timestamp: now(), data });
  }
}

function now(): string {
  return new Date().toISOString().slice(11, 19);
}

export const broadcaster = new LogBroadcaster();
