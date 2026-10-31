import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Client } from "pg";
import { PgInterleaveClient } from "./index";

describe("Chaos Engine: Synthetic Deadlock (40P01)", () => {
  const chaos = new PgInterleaveClient("http://127.0.0.1:9090");
  let client: Client;

  beforeEach(async () => {
    await chaos.reset();
    client = new Client({
      connectionString: "postgres://postgres:postgres@127.0.0.1:5433/postgres",
    });
    await client.connect();

    await client.query(`
      CREATE TABLE IF NOT EXISTS seats (
        id INT PRIMARY KEY,
        reserved BOOLEAN NOT NULL
      );
      INSERT INTO seats (id, reserved) VALUES (1, false)
      ON CONFLICT (id) DO UPDATE SET reserved = false;
    `);
  });

  afterEach(async () => {
    const timedOut = await chaos.hasTimeouts();
    expect(timedOut).toBe(false);
    await client.end();
  });

  it("triggers 40P01 error and verifies retry loop behavior without manual reset", async () => {
    await chaos.injectDeadlock({
      client,
      onQueryMatch: /UPDATE seats SET reserved = true/,
      times: 1,
    });

    let attempts = 0;
    const reserveWithRetry = async () => {
      for (let i = 0; i < 3; i++) {
        attempts++;
        try {
          await client.query("BEGIN");
          await client.query("UPDATE seats SET reserved = true WHERE id = 1");
          await client.query("COMMIT");
          return;
        } catch (err: any) {
          await client.query("ROLLBACK");
          if (err.code === "40P01") {
            continue;
          }
          throw err;
        }
      }
    };

    await reserveWithRetry();

    expect(attempts).toBe(2);

    const check = await client.query<{ reserved: boolean }>(
      "SELECT reserved FROM seats WHERE id = 1"
    );
    expect(check.rows[0].reserved).toBe(true);
  });
});