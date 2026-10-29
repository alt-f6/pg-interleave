import type { Client } from "pg";

export interface InterceptOptions {
  client?: Client;
  clientPid?: number;
  onQueryMatch: string | RegExp;
  action?: "PAUSE_BEFORE_QUERY" | "PAUSE_AFTER_RESPONSE";
}

export class BarrierHandle {
  constructor(
    private readonly controlUrl: string,
    public readonly barrierId?: string
  ) {}

  async wait(timeoutMs = 5000): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const res = await fetch(`${this.controlUrl}/barriers`);
      if (res.ok) {
        const barriers = (await res.json()) as string[];
        if (this.barrierId) {
          if (barriers.includes(this.barrierId)) return;
        } else if (barriers.length > 0) {
          return;
        }
      }
      await new Promise((r) => setTimeout(r, 20));
    }
    throw new Error(`Timeout waiting for barrier to be reached (${timeoutMs}ms)`);
  }

  async resume(): Promise<void> {
    const url = this.barrierId
      ? `${this.controlUrl}/resume/${this.barrierId}`
      : `${this.controlUrl}/resume-next`;

    const res = await fetch(url, { method: "POST" });
    if (!res.ok) {
      throw new Error(`Failed to resume barrier: HTTP ${res.status}`);
    }
  }
}

export class PgInterleaveClient {
  constructor(private readonly controlUrl = "http://127.0.0.1:9090") {}

  async reset(): Promise<void> {
    const res = await fetch(`${this.controlUrl}/reset`, { method: "POST" });
    if (!res.ok) {
      throw new Error(`Failed to reset proxy: HTTP ${res.status}`);
    }
  }

  async getPid(client: Client): Promise<number> {
    const res = await client.query<{ pid: number }>("SELECT pg_backend_pid() AS pid;");
    return res.rows[0].pid;
  }

  async intercept(options: InterceptOptions): Promise<BarrierHandle> {
    let pid = options.clientPid;
    if (!pid && options.client) {
      pid = await this.getPid(options.client);
    }

    const pattern =
      typeof options.onQueryMatch === "string"
        ? options.onQueryMatch
        : options.onQueryMatch.source;

    const res = await fetch(`${this.controlUrl}/intercept`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        target_pid: pid ?? null,
        query_pattern: pattern,
        action: options.action ?? "pause_before_query",
      }),
    });

    if (!res.ok) {
      throw new Error(`Failed to register interceptor: HTTP ${res.status}`);
    }

    return new BarrierHandle(this.controlUrl);
  }

  async injectTornCommit(client: Client): Promise<void> {
    const pid = await this.getPid(client);
    const res = await fetch(`${this.controlUrl}/intercept`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        target_pid: pid,
        query_pattern: "COMMIT",
        action: "drop_connection_on_commit",
      }),
    });

    if (!res.ok) {
      throw new Error(`Failed to inject torn commit: HTTP ${res.status}`);
    }
  }

  /**
   * Injects a synthetic PostgreSQL 40P01 (Deadlock Detected) error frame
   * when matching the given query pattern without locking real rows.
   */
  async injectDeadlock(options: {
    client?: Client;
    clientPid?: number;
    onQueryMatch: string | RegExp;
  }): Promise<void> {
    let pid = options.clientPid;
    if (!pid && options.client) {
      pid = await this.getPid(options.client);
    }

    const pattern =
      typeof options.onQueryMatch === "string"
        ? options.onQueryMatch
        : options.onQueryMatch.source;

    const res = await fetch(`${this.controlUrl}/intercept`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        target_pid: pid ?? null,
        query_pattern: pattern,
        action: "synthetic_deadlock",
      }),
    });

    if (!res.ok) {
      throw new Error(`Failed to inject deadlock rule: HTTP ${res.status}`);
    }
  }
}