import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Client } from "pg";
import { PgInterleaveClient } from "./index";

describe("Schedule Fuzzer & Jitter Injection", () => {
  const chaos = new PgInterleaveClient("http://127.0.0.1:9090");
  let c1: Client;
  let c2: Client;

  beforeEach(async () => {
    await chaos.reset();
    const connStr = "postgres://postgres:postgres@127.0.0.1:5433/postgres";
    c1 = new Client({ connectionString: connStr });
    c2 = new Client({ connectionString: connStr });

    await c1.connect();
    await c2.connect();

    await c1.query(`
      CREATE TABLE IF NOT EXISTS fuzz_counter (
        id INT PRIMARY KEY,
        val INT NOT NULL
      );
      INSERT INTO fuzz_counter (id, val) VALUES (1, 0)
      ON CONFLICT (id) DO UPDATE SET val = 0;
    `);
  });

  afterEach(async () => {
    const timedOut = await chaos.hasTimeouts();
    expect(timedOut).toBe(false);
    await c1.end();
    await c2.end();
  });

  it("completes concurrent transactions reliably despite randomized query jitters", async () => {
    const iterations = 5;

    for (let i = 0; i < iterations; i++) {
      const p1 = (async () => {
        await c1.query("BEGIN");
        await c1.query("UPDATE fuzz_counter SET val = val + 1 WHERE id = 1");
        await c1.query("COMMIT");
      })();

      const p2 = (async () => {
        await c2.query("BEGIN");
        await c2.query("UPDATE fuzz_counter SET val = val + 1 WHERE id = 1");
        await c2.query("COMMIT");
      })();

      await Promise.all([p1, p2]);
    }

    const res = await c1.query<{ val: number }>("SELECT val FROM fuzz_counter WHERE id = 1");
    expect(res.rows[0].val).toBe(iterations * 2);
  });
});