import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Client } from "pg";
import { PgInterleaveClient } from "./index";

describe("Concurrency Anomaly: Lost Update vs FOR UPDATE", () => {
  const chaos = new PgInterleaveClient("http://127.0.0.1:9090");
  let clientA: Client;
  let clientB: Client;

  beforeEach(async () => {
    await chaos.reset();
    const connStr = "postgres://postgres:postgres@127.0.0.1:5433/postgres";
    clientA = new Client({ connectionString: connStr });
    clientB = new Client({ connectionString: connStr });
    await clientA.connect();
    await clientB.connect();

    await clientA.query(`
      CREATE TABLE IF NOT EXISTS bank_accounts (
        id INT PRIMARY KEY,
        balance INT NOT NULL
      );
      INSERT INTO bank_accounts (id, balance) VALUES (1, 100)
      ON CONFLICT (id) DO UPDATE SET balance = 100;
    `);
  });

  afterEach(async () => {
    const timedOut = await chaos.hasTimeouts();
    expect(timedOut).toBe(false);
    await clientA.end();
    await clientB.end();
  });

  it("reproduces lost update when updates rely on stale reads", async () => {
    const barrierA = await chaos.intercept({
      client: clientA,
      onQueryMatch: /UPDATE bank_accounts SET balance = 80/,
    });

    await clientA.query("BEGIN");
    const readA = await clientA.query<{ balance: number }>(
      "SELECT balance FROM bank_accounts WHERE id = 1"
    );
    expect(readA.rows[0].balance).toBe(100);

    const txAPromise = (async () => {
      await clientA.query("UPDATE bank_accounts SET balance = 80 WHERE id = 1");
      await clientA.query("COMMIT");
    })();
    await barrierA.wait();

    await clientB.query("BEGIN");
    const readB = await clientB.query<{ balance: number }>(
      "SELECT balance FROM bank_accounts WHERE id = 1"
    );
    await clientB.query(
      `UPDATE bank_accounts SET balance = ${readB.rows[0].balance - 10} WHERE id = 1`
    );
    await clientB.query("COMMIT");

    await barrierA.resume();
    await txAPromise;

    const finalRes = await clientB.query<{ balance: number }>(
      "SELECT balance FROM bank_accounts WHERE id = 1"
    );
    expect(finalRes.rows[0].balance).toBe(80);
  });

  it("prevents lost update when using SELECT ... FOR UPDATE (Session B blocks on lock)", async () => {
    const barrierA = await chaos.intercept({
      client: clientA,
      onQueryMatch: /UPDATE bank_accounts SET balance = 80/,
    });

    await clientA.query("BEGIN");
    await clientA.query("SELECT balance FROM bank_accounts WHERE id = 1 FOR UPDATE");

    const txAPromise = (async () => {
      await clientA.query("UPDATE bank_accounts SET balance = 80 WHERE id = 1");
      await clientA.query("COMMIT");
    })();
    await barrierA.wait();

    let txBFinished = false;
    const txBPromise = (async () => {
      await clientB.query("BEGIN");
      const res = await clientB.query<{ balance: number }>(
        "SELECT balance FROM bank_accounts WHERE id = 1 FOR UPDATE"
      );
      await clientB.query(
        `UPDATE bank_accounts SET balance = ${res.rows[0].balance - 10} WHERE id = 1`
      );
      await clientB.query("COMMIT");
      txBFinished = true;
    })();

    await new Promise((r) => setTimeout(r, 150));
    expect(txBFinished).toBe(false);

    await barrierA.resume();
    await txAPromise;
    await txBPromise;
    expect(txBFinished).toBe(true);

    const finalRes = await clientA.query<{ balance: number }>(
      "SELECT balance FROM bank_accounts WHERE id = 1"
    );
    expect(finalRes.rows[0].balance).toBe(70);
  });
});