import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Client } from "pg";
import { PgInterleaveClient } from "./index";

describe("Prepared Statement Parameter Matching (Bind)", () => {
  const chaos = new PgInterleaveClient("http://127.0.0.1:9090");
  const connStr = "postgres://postgres:postgres@127.0.0.1:5433/postgres";
  let c1: Client;

  beforeEach(async () => {
    await chaos.reset();
    c1 = new Client({ connectionString: connStr });
    await c1.connect();

    await c1.query(`
      DROP TABLE IF EXISTS params_inventory CASCADE;
      CREATE TABLE params_inventory (
        id INT PRIMARY KEY,
        item_name TEXT NOT NULL,
        stock INT NOT NULL
      );
      INSERT INTO params_inventory (id, item_name, stock) VALUES 
        (1, 'laptop', 10),
        (2, 'keyboard', 20);
    `);
  });

  afterEach(async () => {
    const timedOut = await chaos.hasTimeouts();
    expect(timedOut).toBe(false);
    await c1.end();
  });

  it("intercepts query only when parameterized Bind values match", async () => {
    const barrier = await chaos.intercept({
      client: c1,
      onQueryMatch: /UPDATE params_inventory SET stock = stock - \$1 WHERE id = \$2/,
      withParams: [1, 2],
    });

    await c1.query("UPDATE params_inventory SET stock = stock - $1 WHERE id = $2", [1, 1]);
    expect(await chaos.barriers()).toHaveLength(0);

    let updateDone = false;
    const pendingUpdate = c1
      .query("UPDATE params_inventory SET stock = stock - $1 WHERE id = $2", [1, 2])
      .then(() => {
        updateDone = true;
      });

    await barrier.wait();
    expect(updateDone).toBe(false);

    await barrier.resume();
    await pendingUpdate;
    expect(updateDone).toBe(true);

    const res = await c1.query<{ id: number; stock: number }>(
      "SELECT id, stock FROM params_inventory ORDER BY id ASC"
    );
    expect(res.rows[0].stock).toBe(9);
    expect(res.rows[1].stock).toBe(19);
  });

  it("supports wildcard parameter matching", async () => {
    const barrier = await chaos.intercept({
      client: c1,
      onQueryMatch: /UPDATE params_inventory SET stock = stock - \$1 WHERE id = \$2/,
      withParams: ["*", 1],
    });

    let updateDone = false;
    const pendingUpdate = c1
      .query("UPDATE params_inventory SET stock = stock - $1 WHERE id = $2", [5, 1])
      .then(() => {
        updateDone = true;
      });

    await barrier.wait();
    expect(updateDone).toBe(false);

    await barrier.resume();
    await pendingUpdate;
    expect(updateDone).toBe(true);
  });
});