import { Client } from "pg";
import { PgInterleaveClient } from "./index";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function runDemo() {
  const chaos = new PgInterleaveClient("http://127.0.0.1:9090");
  const connStr = "postgres://postgres:postgres@127.0.0.1:5433/postgres";

  console.log("\x1b[36m[pg-interleave]\x1b[0m Resetting proxy control plane...");
  await chaos.reset();

  const observer = new Client({ connectionString: connStr });
  const workerA = new Client({ connectionString: connStr });
  await observer.connect();
  await workerA.connect();

  console.log("\x1b[36m[setup]\x1b[0m Initializing accounts (id=1, balance=100)...");
  await observer.query(`
    DROP TABLE IF EXISTS accounts CASCADE;
    CREATE TABLE accounts (id int PRIMARY KEY, balance int NOT NULL);
    INSERT INTO accounts VALUES (1, 100);
  `)

  console.log("\x1b[33m[barrier]\x1b[0m Arming barrier on: \x1b[1mUPDATE accounts\x1b[0m");
  const barrier = await chaos.intercept({
    client: workerA,
    onQueryMatch: /UPDATE accounts/,
  });

  console.log("\x1b[32m[workerA]\x1b[0m Dispatching: UPDATE accounts SET balance = balance - 50 WHERE id = 1");
  const pendingUpdate = workerA.query(
    "UPDATE accounts SET balance = balance - 50 WHERE id = 1"
  );

  console.log("\x1b[33m[barrier]\x1b[0m Waiting for query to freeze in proxy pipeline...");
  await barrier.wait();
  console.log("\x1b[31m[proxy]\x1b[0m \x1b[1mPAUSED!\x1b[0m PostgreSQL has NOT seen the query yet.");

  await sleep(1500);

  const beforeResume = await observer.query("SELECT balance FROM accounts WHERE id = 1");
  console.log(`\x1b[35m[observer]\x1b[0m Reading balance during freeze: \x1b[1m$${beforeResume.rows[0].balance}\x1b[0m`);

  await sleep(1500);

  console.log("\x1b[33m[barrier]\x1b[0m Releasing barrier: barrier.resume()...");
  await barrier.resume();
  await pendingUpdate;
  console.log("\x1b[32m[workerA]\x1b[0m UPDATE finished successfully.");

  const afterResume = await observer.query("SELECT balance FROM accounts WHERE id = 1");
  console.log(`\x1b[35m[observer]\x1b[0m Final balance: \x1b[1m$${afterResume.rows[0].balance}\x1b[0m`);

  await observer.end();
  await workerA.end();
}

runDemo().catch(console.error);