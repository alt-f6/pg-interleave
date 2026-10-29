import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Client } from "pg";
import { PgInterleaveClient } from "./index.js";

describe("Deterministic Transaction Race Conditions", () => {
  const chaos = new PgInterleaveClient("http://127.0.0.1:9090");
  let clientA: Client;
  let clientB: Client;

  beforeEach(async () => {
    await chaos.reset();

    clientA = new Client({ connectionString: "postgres://postgres:postgres@127.0.0.1:5433/postgres" });
    clientB = new Client({ connectionString: "postgres://postgres:postgres@127.0.0.1:5433/postgres" });

    await clientA.connect();
    await clientB.connect();

    await clientA.query(`
      CREATE TABLE IF NOT EXISTS accounts (
        id INT PRIMARY KEY,
        balance INT NOT NULL
      );
      INSERT INTO accounts (id, balance) VALUES (1, 100)
      ON CONFLICT (id) DO UPDATE SET balance = 100;
    `);
  });

  afterEach(async () => {
    await clientA.end();
    await clientB.end();
  });

  it("deterministically pauses tx1 at balance update to allow tx2 race", async () => {
    const barrier = await chaos.intercept({
      client: clientA,
      onQueryMatch: /UPDATE accounts SET balance = balance - 50/,
    });

    let txAFinished = false;
    const txAPromise = (async () => {
      await clientA.query("BEGIN");
      await clientA.query("UPDATE accounts SET balance = balance - 50 WHERE id = 1");
      await clientA.query("COMMIT");
      txAFinished = true;
    })();

    await barrier.wait();
    expect(txAFinished).toBe(false);

    const snapshotB = await clientB.query<{ balance: number }>("SELECT balance FROM accounts WHERE id = 1");
    expect(snapshotB.rows[0].balance).toBe(100);

    await barrier.resume();
    await txAPromise;
    expect(txAFinished).toBe(true);

    const finalRes = await clientB.query<{ balance: number }>("SELECT balance FROM accounts WHERE id = 1");
    expect(finalRes.rows[0].balance).toBe(50);
  });
});