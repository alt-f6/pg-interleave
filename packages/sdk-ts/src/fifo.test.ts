import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Client } from "pg";
import { PgInterleaveClient } from "./index";

describe("Deterministic FIFO Queue Ordering", () => {
  const chaos = new PgInterleaveClient("http://127.0.0.1:9090");
  let c1: Client;
  let c2: Client;
  let c3: Client;

  beforeEach(async () => {
    await chaos.reset();
    const connStr = "postgres://postgres:postgres@127.0.0.1:5433/postgres";
    c1 = new Client({ connectionString: connStr });
    c2 = new Client({ connectionString: connStr });
    c3 = new Client({ connectionString: connStr });

    await c1.connect();
    await c2.connect();
    await c3.connect();
  });

  afterEach(async () => {
    const timedOut = await chaos.hasTimeouts();
    expect(timedOut).toBe(false);
    await c1.end();
    await c2.end();
    await c3.end();
  });

  it("releases 3 barriers strictly in creation order via resume-next", async () => {
    await chaos.intercept({ client: c1, onQueryMatch: /SELECT 101/ });
    await chaos.intercept({ client: c2, onQueryMatch: /SELECT 202/ });
    await chaos.intercept({ client: c3, onQueryMatch: /SELECT 303/ });

    let p1Done = false;
    let p2Done = false;
    let p3Done = false;

    const p1 = c1.query("SELECT 101").then(() => { p1Done = true; });
    while ((await chaos.barriers()).length < 1) {
      await new Promise((r) => setTimeout(r, 10));
    }

    const p2 = c2.query("SELECT 202").then(() => { p2Done = true; });
    while ((await chaos.barriers()).length < 2) {
      await new Promise((r) => setTimeout(r, 10));
    }

    const p3 = c3.query("SELECT 303").then(() => { p3Done = true; });
    while ((await chaos.barriers()).length < 3) {
      await new Promise((r) => setTimeout(r, 10));
    }

    expect(p1Done).toBe(false);
    expect(p2Done).toBe(false);
    expect(p3Done).toBe(false);

    const r1 = await chaos.resumeNext();
    expect(r1.status).toBe("resumed");
    await p1;
    expect(p1Done).toBe(true);
    expect(p2Done).toBe(false);
    expect(p3Done).toBe(false);

    const r2 = await chaos.resumeNext();
    expect(r2.status).toBe("resumed");
    await p2;
    expect(p2Done).toBe(true);
    expect(p3Done).toBe(false);

    const r3 = await chaos.resumeNext();
    expect(r3.status).toBe("resumed");
    await p3;
    expect(p3Done).toBe(true);
  });
});