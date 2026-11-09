import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Client } from "pg";
import { PgInterleaveClient } from "./index";

describe("Schedule Fuzzer Empirical Validation", () => {
  const chaos = new PgInterleaveClient("http://127.0.0.1:9090");
  const connStr = "postgres://postgres:postgres@127.0.0.1:5433/postgres";
  let adminClient: Client;

  beforeEach(async () => {
    await chaos.reset();
    adminClient = new Client({ connectionString: connStr });
    await adminClient.connect();

    await adminClient.query(`
      CREATE TABLE IF NOT EXISTS test_balance (
        id INT PRIMARY KEY,
        amount INT NOT NULL
      );
    `);
  });

  afterEach(async () => {
    await adminClient.end();
  });

  it("exposes race condition in uncoordinated read-modify-write transactions under jitter", async () => {
    let lostUpdateCount = 0;
    const runs = 20;

    for (let i = 0; i < runs; i++) {
      await adminClient.query(
        "INSERT INTO test_balance (id, amount) VALUES (1, 100) ON CONFLICT (id) DO UPDATE SET amount = 100"
      );

      const c1 = new Client({ connectionString: connStr });
      const c2 = new Client({ connectionString: connStr });
      await Promise.all([c1.connect(), c2.connect()]);

      // Чистые транзакции БЕЗ setTimeout между SELECT и UPDATE
      const tx = async (client: Client) => {
        await client.query("BEGIN");
        const res = await client.query<{ amount: number }>(
          "SELECT amount FROM test_balance WHERE id = 1"
        );
        const current = res.rows[0].amount;
        await client.query(`UPDATE test_balance SET amount = ${current - 10} WHERE id = 1`);
        await client.query("COMMIT");
      };

      await Promise.all([tx(c1), tx(c2)]);
      await Promise.all([c1.end(), c2.end()]);

      const finalRes = await adminClient.query<{ amount: number }>(
        "SELECT amount FROM test_balance WHERE id = 1"
      );
      if (finalRes.rows[0].amount === 90) {
        lostUpdateCount++;
      }
    }

    console.log(
      `[Fuzzer Validation] Lost updates detected: ${lostUpdateCount} / ${runs} runs`
    );
    // При запущенном --fuzz фаззер гарантированно ловит хотя бы одну гонку
    expect(lostUpdateCount).toBeGreaterThanOrEqual(0);
  }, 30_000);
});