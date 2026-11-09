import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Client } from "pg";
import { PgInterleaveClient } from "./index";
import * as fs from "fs";
import * as path from "path";

describe("Trace Recorder & Replay", () => {
  const chaos = new PgInterleaveClient("http://127.0.0.1:9090");
  const connStr = "postgres://postgres:postgres@127.0.0.1:5433/postgres";
  let c1: Client;
  let c2: Client;
  const tracePath = path.resolve(process.cwd(), "trace.json");

  beforeEach(async () => {
    await chaos.reset();
    c1 = new Client({ connectionString: connStr });
    c2 = new Client({ connectionString: connStr });
    await Promise.all([c1.connect(), c2.connect()]);

    await c1.query(`
      DROP TABLE IF EXISTS replay_bank CASCADE;
      CREATE TABLE replay_bank (
        id INT PRIMARY KEY,
        balance INT NOT NULL
      );
      INSERT INTO replay_bank (id, balance) VALUES (1, 100);
    `);
  });

  afterEach(async () => {
    const closes: Promise<void>[] = [];
    if (c1) closes.push(c1.end());
    if (c2) closes.push(c2.end());
    await Promise.all(closes);
    if (fs.existsSync(tracePath)) {
      try {
        fs.unlinkSync(tracePath);
      } catch {}
    }
  });

  it("records interleaving schedule, flushes trace to disk and verifies step ordering", async () => {
    await c1.query("BEGIN");
    await c2.query("BEGIN");

    await c1.query("SELECT balance FROM replay_bank WHERE id = 1");
    await c2.query("SELECT balance FROM replay_bank WHERE id = 1");

    await c1.query("UPDATE replay_bank SET balance = balance - 10 WHERE id = 1");
    await c2.query("COMMIT");

    await c1.query("UPDATE replay_bank SET balance = balance - 20 WHERE id = 1");
    await c1.query("COMMIT");

    await chaos.flushTrace();

    // Проверяем наличие файла в текущей папке или корне
    const possiblePaths = [
      tracePath,
      path.resolve(process.cwd(), "../../trace.json"),
      path.resolve(process.cwd(), "../trace.json"),
    ];
    const resolvedPath = possiblePaths.find((p) => fs.existsSync(p));

    expect(
      resolvedPath,
      "trace.json should exist after calling chaos.flushTrace() when --record is configured"
    ).toBeDefined();

    if (resolvedPath) {
      const raw = fs.readFileSync(resolvedPath, "utf-8");
      const trace = JSON.parse(raw);
      expect(Array.isArray(trace.steps)).toBe(true);
      expect(trace.steps.length).toBeGreaterThanOrEqual(4);

      for (let i = 0; i < trace.steps.length; i++) {
        expect(trace.steps[i].step).toBe(i);
        expect(typeof trace.steps[i].conn_id).toBe("number");
        expect(typeof trace.steps[i].query).toBe("string");
      }

      const hasUpdate = trace.steps.some((s: { query: string }) =>
        s.query.includes("UPDATE replay_bank")
      );
      expect(hasUpdate).toBe(true);
    }
  });
});