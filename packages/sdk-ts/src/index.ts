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
  | "pause_before_query"
  | "pause_after_response";

export type ParamValue = string | number | boolean | null;

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

export function regexToRustPattern(regex: RegExp | string): string {
  if (typeof regex === "string") {
    return regex.startsWith("(?") ? regex : `(?s)${regex}`;
  }

  let flags = "";
  if (regex.ignoreCase) flags += "i";
  if (regex.multiline) flags += "m";
  flags += "s";

  const uniqueFlags = Array.from(new Set(flags.split(""))).join("");
  return `(?${uniqueFlags})${regex.source}`;
}

export interface RuleHandle {
  id: string;
  assertFired(minTimes?: number): Promise<void>;
  getFiredCount(): Promise<number>;
}

export class DefaultRuleHandle implements RuleHandle {
  constructor(
    protected readonly client: PgInterleaveClient,
    public readonly id: string
  ) {}

  async getFiredCount(): Promise<number> {
    return this.client.getRuleFiredCount(this.id);
  }

  async assertFired(minTimes = 1): Promise<void> {
    const count = await this.getFiredCount();
    if (count < minTimes) {
      throw new Error(
        `Assertion failed: Rule '${this.id}' was expected to fire at least ${minTimes} time(s), but fired ${count} time(s). Check your SQL pattern and newlines.`
      );
    }
  }
}

export class BarrierHandle implements RuleHandle {
  constructor(
    private readonly client: PgInterleaveClient,
    public readonly id: string,
    public readonly barrierId?: string
  ) {}

  async wait(timeoutMs = 5000): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const active = await this.client.barriers();
      if (this.barrierId) {
        if (active.includes(this.barrierId)) return;
      } else if (active.length > 0) {
        return;
      }
      await new Promise((r) => setTimeout(r, 20));
    }
    throw new Error(`Timeout waiting for barrier to be reached (${timeoutMs}ms)`);
  }

  async resume(): Promise<void> {
    if (this.barrierId) {
      await this.client.resume(this.barrierId);
    } else {
      await this.client.resumeNext();
    }
  }

  async getFiredCount(): Promise<number> {
    return this.client.getRuleFiredCount(this.id);
  }

  async assertFired(minTimes = 1): Promise<void> {
    const count = await this.getFiredCount();
    if (count < minTimes) {
      throw new Error(
        `Assertion failed: Rule '${this.id}' was expected to fire at least ${minTimes} time(s), but fired ${count} time(s). Check your SQL pattern and newlines.`
      );
    }
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

  async resume(barrierId: string): Promise<void> {
    const res = await fetch(`${this.controlUrl}/resume/${barrierId}`, {
      method: "POST",
    });
    if (!res.ok) {
      throw new Error(`Failed to resume barrier '${barrierId}': HTTP ${res.status}`);
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

  async barriers(): Promise<string[]> {
    const res = await fetch(`${this.controlUrl}/barriers`);
    if (!res.ok) {
      throw new Error(`Failed to fetch active barriers: HTTP ${res.status}`);
    }
    return (await res.json()) as string[];
  }

  async getRuleFiredCount(ruleId: string): Promise<number> {
    const res = await fetch(`${this.controlUrl}/rules`);
    if (!res.ok) return 0;
    const rules = (await res.json()) as Array<{ id: string; fired_count: number }>;
    const rule = rules.find((r) => r.id === ruleId);
    return rule ? rule.fired_count : 0;
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

  async intercept(options: InterceptOptions): Promise<BarrierHandle> {
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
    return new BarrierHandle(this, id, data.barrier_id);
  }

  async injectTornCommit(client: Client, times = 1): Promise<RuleHandle> {
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
    return new DefaultRuleHandle(this, id);
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
  }): Promise<RuleHandle> {
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
    return new DefaultRuleHandle(this, id);
  }
}