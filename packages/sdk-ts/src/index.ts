import type { Client } from "pg";

export type SqlOperation =
  | "SELECT"
  | "INSERT"
  | "UPDATE"
  | "DELETE"
  | "select"
  | "insert"
  | "update"
  | "delete";

export interface AstSelector {
  operation?: SqlOperation;
  table?: string;
}

export type BarrierAction =
  | "PAUSE_BEFORE_QUERY"
  | "PAUSE_AFTER_RESPONSE"
  | "DROP_CONNECTION_ON_COMMIT"
  | "SYNTHETIC_DEADLOCK"
  | "pause_before_query"
  | "pause_after_response"
  | "drop_connection_on_commit"
  | "synthetic_deadlock";

export type ParamValue = string | number | boolean | null;

export interface ActiveBarrierInfo {
  id: string;
  rule_id: string;
}

export interface InterceptOptions {
  client?: Client;
  clientPid?: number;
  sessionTag?: string;
  onQueryMatch?: RegExp | string;
  ast?: AstSelector;
  withParams?: (ParamValue | "*")[];
  action?: BarrierAction;
  times?: number;
}

export interface RuleStatusDto {
  id: string;
  fired_count: number;
  times_remaining?: number | null;
}

export function regexToRustPattern(pattern: RegExp | string): string {
  if (typeof pattern === "string") {
    if (pattern.startsWith("(?")) {
      return pattern;
    }
    return `(?s)${pattern}`;
  }

  const flags = pattern.flags;
  const isCaseInsensitive = flags.includes("i");
  const rustFlags = isCaseInsensitive ? "(?is)" : "(?s)";

  return `${rustFlags}${pattern.source}`;
}

export class InterceptHandle {
  constructor(
    public readonly id: string,
    private readonly client: PgInterleaveClient,
    public readonly barrierId?: string
  ) {}

  async getFiredCount(): Promise<number> {
    const stats = await this.client.getRuleStats(this.id);
    return stats?.fired_count ?? 0;
  }

  async assertFired(expectedCount = 1): Promise<void> {
    const actual = await this.getFiredCount();
    if (actual !== expectedCount) {
      throw new Error(
        `[pg-interleave] Rule assertion failed for rule "${this.id}": ` +
        `expected to fire ${expectedCount} time(s), but fired ${actual} time(s). ` +
        `Check your SQL regex pattern or verify whether the query actually reached the proxy.`
      );
    }
  }

  async resume(): Promise<void> {
    await this.client.resume(this.barrierId ?? this.id);
  }

  async wait(timeoutMs = 5000): Promise<void> {
    await this.client.waitForBarrier(this.barrierId ?? this.id, timeoutMs);
  }
}

export class PgInterleaveClient {
  constructor(public readonly controlUrl = "http://127.0.0.1:9090") {}

  static tag(sql: string, sessionTag: string): string {
    return `/* interleave: session=${sessionTag} */ ${sql}`;
  }

  async reset(): Promise<void> {
    const res = await fetch(`${this.controlUrl}/reset`, { method: "POST" });
    if (!res.ok) {
      throw new Error(`Failed to reset proxy: HTTP ${res.status}`);
    }
  }

  async resume(targetId: string): Promise<void> {
    const res = await fetch(`${this.controlUrl}/resume/${targetId}`, {
      method: "POST",
    });
    if (!res.ok) {
      throw new Error(`Failed to resume target '${targetId}': HTTP ${res.status}`);
    }
  }

  async resumeNext(): Promise<{ status: string; barrier_id?: string }> {
    const res = await fetch(`${this.controlUrl}/resume-next`, {
      method: "POST",
    });
    if (!res.ok) {
      throw new Error(`Failed to resume next barrier: HTTP ${res.status}`);
    }
    return (await res.json()) as { status: string; barrier_id?: string };
  }

  async barriers(): Promise<ActiveBarrierInfo[]> {
    const res = await fetch(`${this.controlUrl}/barriers`);
    if (!res.ok) {
      throw new Error(`Failed to fetch active barriers: HTTP ${res.status}`);
    }
    const data = await res.json();
    return data.map((b: any) =>
      typeof b === "string" ? { id: b, rule_id: b } : b
    );
  }

  async waitForBarrier(targetId: string, timeoutMs = 5000): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const list = await this.barriers();
      const found = list.some(
        (b) => b.id === targetId || b.rule_id === targetId
      );
      if (found) return;
      await new Promise((r) => setTimeout(r, 20));
    }
    throw new Error(`Timeout waiting for barrier '${targetId}' to be reached (${timeoutMs}ms)`);
  }

  async getRuleStats(ruleId: string): Promise<RuleStatusDto | null> {
    const res = await fetch(`${this.controlUrl}/rules`);
    if (!res.ok) return null;
    const rules = (await res.json()) as RuleStatusDto[];
    return rules.find((r) => r.id === ruleId) ?? null;
  }

  async getRuleFiredCount(ruleId: string): Promise<number> {
    const stats = await this.getRuleStats(ruleId);
    return stats?.fired_count ?? 0;
  }

  async hasTimeouts(): Promise<boolean> {
    const res = await fetch(`${this.controlUrl}/events`);
    if (!res.ok) {
      throw new Error(`Failed to fetch events: HTTP ${res.status}`);
    }
    const data = (await res.json()) as { had_timeouts: boolean };
    return data.had_timeouts;
  }

  async getPid(client: Client): Promise<number> {
    const res = await client.query<{ pid: number }>("SELECT pg_backend_pid() AS pid;");
    return res.rows[0].pid;
  }

  async intercept(options: InterceptOptions): Promise<InterceptHandle> {
    if (!options.onQueryMatch && !options.ast) {
      throw new Error("Either 'onQueryMatch' or 'ast' selector must be provided");
    }

    let pid = options.clientPid;
    if (!pid && options.client) {
      pid = await this.getPid(options.client);
    }

    const payload: Record<string, unknown> = {
      targetPid: pid,
      sessionTag: options.sessionTag,
      action: options.action ?? "pause_before_query",
      times: options.times ?? 1,
    };

    if (options.onQueryMatch) {
      payload.queryPattern = regexToRustPattern(options.onQueryMatch);
    }

    if (options.ast) {
      payload.ast = options.ast;
    }

    if (options.withParams) {
      payload.withParams = options.withParams.map((p) =>
        typeof p === "number" ? String(p) : p
      );
    }

    const res = await fetch(`${this.controlUrl}/intercept`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const errBody = await res.text();
      throw new Error(`Failed to register interceptor: HTTP ${res.status} - ${errBody}`);
    }

    const data = (await res.json()) as { rule_id?: string; barrier_id?: string };
    const id = data.rule_id || data.barrier_id || "rule";
    return new InterceptHandle(id, this, data.barrier_id);
  }

  async injectTornCommit(client: Client, times = 1): Promise<InterceptHandle> {
    const pid = await this.getPid(client);
    const res = await fetch(`${this.controlUrl}/intercept`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        target_pid: pid,
        query_pattern: regexToRustPattern("COMMIT"),
        action: "drop_connection_on_commit",
        times,
      }),
    });

    if (!res.ok) {
      throw new Error(`Failed to inject torn commit: HTTP ${res.status}`);
    }

    const data = (await res.json()) as { rule_id?: string };
    const id = data.rule_id || "rule";
    return new InterceptHandle(id, this);
  }

  async flushTrace(): Promise<void> {
    const res = await fetch(`${this.controlUrl}/flush-trace`, {
      method: "POST",
    });
    if (!res.ok) {
      throw new Error(`Failed to flush trace: HTTP ${res.status}`);
    }
  }

  async injectDeadlock(options: {
    client?: Client;
    clientPid?: number;
    onQueryMatch: string | RegExp;
    times?: number;
  }): Promise<InterceptHandle> {
    let pid = options.clientPid;
    if (!pid && options.client) {
      pid = await this.getPid(options.client);
    }

    const pattern = regexToRustPattern(options.onQueryMatch);

    const res = await fetch(`${this.controlUrl}/intercept`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        target_pid: pid ?? null,
        query_pattern: pattern,
        action: "synthetic_deadlock",
        times: options.times ?? 1,
      }),
    });

    if (!res.ok) {
      throw new Error(`Failed to inject deadlock rule: HTTP ${res.status}`);
    }

    const data = (await res.json()) as { rule_id?: string };
    const id = data.rule_id || "rule";
    return new InterceptHandle(id, this);
  }
}