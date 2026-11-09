import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Client } from "pg";
import { PgInterleaveClient } from "./index";

describe("Extended Protocol: Pipelined Execution & Synthetic Deadlock", () => {
  const chaos = new PgInterleaveClient("http://127.0.0.1:9090");
  let client: Client;

  beforeEach(async () => {
    await chaos.reset();
    client = new Client({
      connectionString: "postgres://postgres:postgres@127.0.0.1:5433/postgres",
    });
    await client.connect();

    await client.query(`
      CREATE TABLE IF NOT EXISTS test_items (
        id INT PRIMARY KEY,
        val INT NOT NULL
      );
      INSERT INTO test_items (id, val) VALUES (1, 10), (2, 20), (3, 30)
      ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val;
    `);
  });

  afterEach(async () => {
    await client.end().catch(() => {});
  });

  it("handles synthetic deadlock on prepared statement without corrupting stream for subsequent queries", async () => {
    await chaos.injectDeadlock({
      client,
      onQueryMatch: /SELECT val FROM test_items WHERE id = \$1/,
      times: 1,
    });

    const queryConfig = {
      name: "fetch_item",
      text: "SELECT val FROM test_items WHERE id = $1",
      values: [1],
    };

    let caught40P01 = false;
    try {
      await client.query(queryConfig);
    } catch (err: any) {
      if (err.code === "40P01") {
        caught40P01 = true;
      } else {
        throw err;
      }
    }
    expect(caught40P01).toBe(true);

    const res2 = await client.query({
      name: "fetch_item",
      text: "SELECT val FROM test_items WHERE id = $1",
      values: [1],
    });
    expect(res2.rows[0].val).toBe(10);

    const res3 = await client.query({
      name: "fetch_item",
      text: "SELECT val FROM test_items WHERE id = $1",
      values: [2],
    });
    expect(res3.rows[0].val).toBe(20);
  });
});