import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Client } from "pg";
import { PgInterleaveClient } from "./index";

describe("AST Query Selectors", () => {
  const chaos = new PgInterleaveClient("http://127.0.0.1:9090");
  const connStr = "postgres://postgres:postgres@127.0.0.1:5433/postgres";
  let c1: Client;
  let c2: Client;

  beforeEach(async () => {
    await chaos.reset();
    c1 = new Client({ connectionString: connStr });
    c2 = new Client({ connectionString: connStr });
    await Promise.all([c1.connect(), c2.connect()]);

    await c1.query(`
      DROP TABLE IF EXISTS accounts CASCADE;
      CREATE TABLE accounts (id int PRIMARY KEY, balance int NOT NULL);
      INSERT INTO accounts VALUES (1, 100);
    `);
  });

  afterEach(async () => {
    const closes: Promise<void>[] = [];
    if (c1) closes.push(c1.end());
    if (c2) closes.push(c2.end());
    await Promise.all(closes);
  });

  it("intercepts query regardless of SQL formatting, quotes, or schema qualification", async () => {
    const barrier = await chaos.intercept({
      client: c1,
      ast: { operation: "UPDATE", table: "accounts" },
    });

    const updatePromise = c1.query(`
      UPDATE "public"."accounts"
      SET balance = balance - 20
      WHERE id = 1
    `);

    await barrier.wait();

    const midCheck = await c2.query("SELECT balance FROM accounts WHERE id = 1");
    expect(midCheck.rows[0].balance).toBe(100);

    await barrier.resume();
    await updatePromise;

    const finalCheck = await c2.query("SELECT balance FROM accounts WHERE id = 1");
    expect(finalCheck.rows[0].balance).toBe(80);
  });
});