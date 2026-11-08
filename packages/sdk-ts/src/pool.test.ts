import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Pool } from "pg";
import { PgInterleaveClient } from "./index.js";

describe("SQL Comment Session Routing (Connection Pools)", () => {
  const chaos = new PgInterleaveClient("http://127.0.0.1:9090");
  let pool: Pool;

  beforeEach(async () => {
    await chaos.reset();

    pool = new Pool({
      connectionString: "postgres://postgres:postgres@127.0.0.1:5433/postgres",
      max: 5,
    });

    await pool.query(`
      DROP TABLE IF EXISTS inventory CASCADE;
      CREATE TABLE IF NOT EXISTS inventory (
        id INT PRIMARY KEY,
        stock INT NOT NULL
      );
      INSERT INTO inventory (id, stock) VALUES (1, 10)
      ON CONFLICT (id) DO UPDATE SET stock = 10;
    `);
  });

  afterEach(async () => {
    await pool.end();
  });

  it("routes barrier via SQL comment tag without explicit client PID", async () => {
    const barrier = await chaos.intercept({
      sessionTag: "workerA",
      onQueryMatch: /UPDATE inventory SET stock = stock - 1/,
    });

    let txAFinished = false;
    const workerAPromise = (async () => {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        const taggedSql = PgInterleaveClient.tag(
          "UPDATE inventory SET stock = stock - 1 WHERE id = 1",
          "workerA"
        );
        await client.query(taggedSql);
        await client.query("COMMIT");
        txAFinished = true;
      } finally {
        client.release();
      }
    })();

    await barrier.wait();
    expect(txAFinished).toBe(false);

    const clientB = await pool.connect();
    try {
      const res = await clientB.query<{ stock: number }>(
        "SELECT stock FROM inventory WHERE id = 1"
      );
      expect(res.rows[0].stock).toBe(10);
    } finally {
      clientB.release();
    }

    await barrier.resume();
    await workerAPromise;
    expect(txAFinished).toBe(true);

    const finalCheck = await pool.query<{ stock: number }>(
      "SELECT stock FROM inventory WHERE id = 1"
    );
    expect(finalCheck.rows[0].stock).toBe(9);
  });
});