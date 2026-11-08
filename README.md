# pg-interleave

[![CI](https://github.com/alt-f6/pg-interleave/actions/workflows/ci.yml/badge.svg)](https://github.com/alt-f6/pg-interleave/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A PostgreSQL wire-protocol proxy for **deterministic concurrency tests**.

It sits between your test code and Postgres, understands the v3.0 protocol, and lets a test pause a specific statement, hold its response, tear the connection after a commit, or inject a `40P01` deadlock error, all on command and in a fixed order.

> **Status: alpha.** Rust server + TypeScript SDK, tested on PostgreSQL 16. APIs may change. Plaintext connections only.

```
test code ──HTTP :9090──▶ control plane ─┐
                                          ▼
pg / any client ──:5433──▶ proxy ──────▶ PostgreSQL :5432
```

<p align="center">
  <a href="#why">Why</a> •
  <a href="#install">Install</a> •
  <a href="#10-second-example">Quickstart</a> •
  <a href="#what-it-can-do">Capabilities</a> •
  <a href="#sdk">SDK</a> •
  <a href="#schedule-fuzzer-automated-race-condition-discovery">Schedule Fuzzer</a> •
  <a href="#http-control-plane">HTTP API</a> •
  <a href="#limitations">Limitations</a>
</p>

---

## Why

Most concurrency tests either run two transactions with `Promise.all` and hope, or sequence them by hand with `await`. The second is already deterministic, as long as you control every statement.

pg-interleave is for the case where you **don't**: a function in your application opens a transaction and runs several statements, and you want to freeze it between two of them, run something else, then let it continue. Nothing in the application code changes. The test only says "stop the connection that is about to run `UPDATE accounts ...`".

If your scenario can be written as plain sequential `await`s on two clients, you don't need this tool. PostgreSQL's own `isolationtester` is also worth knowing: it explores permutations of steps across sessions and lives inside the Postgres source tree.

## Install

> Docker images, release binaries and the npm package are published with each GitHub release (latest: **v0.4.0**).

### 1. Run the proxy

Docker (pass all three flags: `docker run` arguments replace the image defaults):

```bash
docker run -d --name pg-interleave \
  --add-host=host.docker.internal:host-gateway \
  -p 127.0.0.1:5433:5433 -p 127.0.0.1:9090:9090 \
  ghcr.io/alt-f6/pg-interleave:latest \
  --listen-addr 0.0.0.0:5433 \
  --control-addr 0.0.0.0:9090 \
  --target-addr host.docker.internal:5432
```

Or download a standalone binary for Linux, macOS or Windows from [Releases](https://github.com/alt-f6/pg-interleave/releases).

### 2. Install the SDK

```bash
npm install -D pg-interleave
```

### From source

You need a running PostgreSQL, Rust 1.88+ (edition 2024, the code uses let-chains) and Node 20+.

```bash
# start the proxy
cargo run --release -p pg-interleave-server -- \
  --listen-addr 127.0.0.1:5433 \
  --target-addr 127.0.0.1:5432 \
  --control-addr 127.0.0.1:9090

# run the SDK tests
cd packages/sdk-ts
npm ci
npm test
```

Point your client at port **5433** instead of 5432. The control plane is on 9090. Defaults: listen `127.0.0.1:5433`, target `127.0.0.1:5432`, control `127.0.0.1:9090`, barrier TTL 5000 ms (`--barrier-ttl-ms`).

## 10-second example

Freeze an `UPDATE` inside the proxy, look at the table from another connection, then let it through.

```ts
import { Client } from "pg";
import { PgInterleaveClient } from "pg-interleave";

const chaos = new PgInterleaveClient("http://127.0.0.1:9090");
const url = "postgres://postgres:postgres@127.0.0.1:5433/postgres";
await chaos.reset();

const observer = new Client({ connectionString: url });
const a = new Client({ connectionString: url });
await observer.connect();
await a.connect();

await observer.query(
  "CREATE TABLE IF NOT EXISTS accounts (id int PRIMARY KEY, balance int NOT NULL)",
);
await observer.query(
  "INSERT INTO accounts VALUES (1, 100) ON CONFLICT (id) DO UPDATE SET balance = 100",
);

// Arm a barrier for connection `a`
const barrier = await chaos.intercept({
  client: a,
  onQueryMatch: /UPDATE accounts/,
});

const update = a.query(
  "UPDATE accounts SET balance = balance - 50 WHERE id = 1",
);
await barrier.wait(); // the statement is held in the proxy; Postgres has not seen it

console.log((await observer.query("SELECT balance FROM accounts")).rows[0]); // { balance: 100 }

await barrier.resume(); // let it through
await update;

console.log((await observer.query("SELECT balance FROM accounts")).rows[0]); // { balance: 50 }
```

## What it can do

| Action                      | What happens                                                                                                                                |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `pause_before_query`        | The matching statement is held in the proxy. PostgreSQL has not seen it yet. The test releases it when ready.                               |
| `pause_after_response`      | The statement runs (locks taken, rows written), but the response is held until the test releases it.                                        |
| `drop_connection_on_commit` | After the server answers `CommandComplete COMMIT`, the proxy closes the client socket. The commit is durable; the client never hears back.  |
| `synthetic_deadlock`        | The matching statement never reaches the server. The client gets a well-formed `ErrorResponse` with SQLSTATE `40P01` and a `ReadyForQuery`. |

Behavior you can rely on (each is covered by a test):

- **FIFO release.** `resume-next` releases pending barriers in creation order.
- **One-shot by default.** A rule fires once (`times: 1`). Pass `times` to change it.
- **Bad patterns fail loudly.** An invalid regex is rejected with HTTP 400 at registration.
- **Timeouts are errors, not silence.** If a barrier is not released within the TTL, the client receives SQLSTATE `57014` and `GET /events` reports `had_timeouts: true`, so a test can fail on it.
- **Per-connection targeting.** Rules can be scoped to a backend PID (the SDK reads it with `pg_backend_pid()`).
- **Extended protocol.** Prepared statements are matched when they are executed, including repeated executions, and a synthetic deadlock leaves the stream clean for the next query (checked with both `node-postgres` and `libpq`).
- **Parameter inspection.** Match parameterized queries (`$1`, `$2`) using exact values or `"*"` wildcards via `withParams`.

## Example: a real lost update

Transaction A reads a balance, then writes a value computed from it. We freeze A just before its `UPDATE`, let B complete a whole transaction, then release A.

```ts
const barrier = await chaos.intercept({
  client: a,
  onQueryMatch: /UPDATE bank_accounts SET balance = 80/,
});

await a.query("BEGIN");
await a.query("SELECT balance FROM bank_accounts WHERE id = 1"); // sees 100

const txA = (async () => {
  await a.query("UPDATE bank_accounts SET balance = 80 WHERE id = 1");
  await a.query("COMMIT");
})();

await barrier.wait(); // A is frozen before its UPDATE

// B does a full read-modify-write while A is frozen
await b.query("BEGIN");
const { rows } = await b.query(
  "SELECT balance FROM bank_accounts WHERE id = 1",
);
await b.query(
  `UPDATE bank_accounts SET balance = ${rows[0].balance - 10} WHERE id = 1`,
);
await b.query("COMMIT");

await barrier.resume();
await txA;
// final balance is 80, not 70: B's update was lost
```

The same scenario with `SELECT ... FOR UPDATE` makes B block on A's row lock, and the final balance is 70. Both versions are in [`packages/sdk-ts/src/lost-update.test.ts`](packages/sdk-ts/src/lost-update.test.ts).

## Example: a retry loop that double-charges

[`examples/retry-anomaly`](examples/retry-anomaly) shows a common mistake rather than a bug in any library. A function inserts a payment inside a transaction and is wrapped in a generic retry helper (`p-retry` here). pg-interleave drops the connection **after** Postgres committed but **before** the client saw the result. The helper sees a network error, retries, and the order is inserted a second time: `billing_orders` ends up with two rows ($100 charged instead of $50).

The retry was not wrong to fire; the transaction was not safe to repeat. The usual fix is an idempotency key with a unique constraint (`INSERT ... ON CONFLICT (idempotency_key) DO NOTHING`). This is the kind of failure that is almost impossible to reproduce with mocks, because the commit really happened on the server.

## SDK

```ts
const chaos = new PgInterleaveClient("http://127.0.0.1:9090");

await chaos.reset(); // clear all rules, release all barriers
const barrier = await chaos.intercept({
  // pause_before_query by default
  client, // or clientPid
  onQueryMatch: /regex/,
  withParams: ["*", 2], // optional: match $1, $2 arguments
  action: "pause_after_response", // optional
  times: 1, // optional, default 1
});
await barrier.wait(); // resolves when the statement is held
await barrier.resume(); // release it

await chaos.injectTornCommit(client);
await chaos.injectDeadlock({ client, onQueryMatch: /UPDATE seats/ });
await chaos.hasTimeouts(); // true if any barrier hit its TTL
```

### Connection Pool Compatibility (SQL Comment Routing)

When using connection pools (`pg.Pool`, Prisma, Drizzle), physical connections are shared dynamically. You can route interceptor rules using SQL comment tags instead of capturing backend PIDs:

```typescript
import { Pool } from "pg";
import { PgInterleaveClient } from "pg-interleave";

const pool = new Pool({
  connectionString: "postgres://postgres:postgres@127.0.0.1:5433/postgres",
});
const chaos = new PgInterleaveClient("http://127.0.0.1:9090");

// 1. Intercept only queries marked with sessionTag "workerA"
const barrier = await chaos.intercept({
  sessionTag: "workerA",
  onQueryMatch: /UPDATE inventory SET stock = stock - 1/,
});

// 2. Tag query dynamically using the SDK helper
const taggedSql = PgInterleaveClient.tag(
  "UPDATE inventory SET stock = stock - 1 WHERE id = 1",
  "workerA",
);
await pool.query(taggedSql);

// 3. Resume when ready
await barrier.resume();
```

### Prepared Statement Parameter Matching (`withParams`)

When your ORM (Drizzle, Prisma, TypeORM) uses parameterized queries (`$1`, `$2`), you can scope barriers to specific argument values or use `"*"` as a wildcard:

```typescript
// Intercept only when deducting stock for item ID 2
await chaos.intercept({
  client,
  onQueryMatch:
    /UPDATE params_inventory SET stock = stock - \$1 WHERE id = \$2/,
  withParams: [1, 2], // matches $1 = 1, $2 = 2
});

// Wildcard matching: any quantity, but strictly item ID 1
await chaos.intercept({
  client,
  onQueryMatch:
    /UPDATE params_inventory SET stock = stock - \$1 WHERE id = \$2/,
  withParams: ["*", 1],
});
```

## Schedule fuzzer (Automated Race Condition Discovery)

`pg-interleave` can inject deterministic, per-connection micro-jitters into incoming queries (including `BEGIN` and `COMMIT`), uncovering latent concurrency bugs such as **Lost Updates** and race conditions without manual intercept rules.

Barriers let you test an interleaving you already thought of. The schedule fuzzer is for the ones you did not: it adds pseudo-random delays (jitter) before queries are forwarded to PostgreSQL, so that concurrent transactions reach the database in different orders from run to run. Run your existing concurrent tests under it and see whether anything breaks.

```bash
# start the proxy with fuzzing enabled
cargo run --release -p pg-interleave-server -- \
  --fuzz \
  --fuzz-max-delay-ms 30
```

The proxy logs the seed it picked, so a failure can be replayed:

```
INFO pg_interleave_server: Schedule fuzzer ENABLED. Seed: 133742 (use --seed 133742 to reproduce)
```

If a test fails in CI under fuzzing, start the proxy locally with the logged seed and rerun the same test:

```bash
cargo run --release -p pg-interleave-server -- \
  --fuzz --seed 133742 --fuzz-max-delay-ms 30
```

| Flag                  | Meaning                                                                              |
| --------------------- | ------------------------------------------------------------------------------------ |
| `--fuzz`              | Enable schedule fuzzing. Off by default.                                             |
| `--seed <u64>`        | Seed for the pseudo-random generator. If omitted, a random one is chosen and logged. |
| `--fuzz-max-delay-ms` | Upper bound for a single injected delay, in milliseconds.                            |

### Capabilities & Guarantees

- **Per-Connection Reproducibility.** Each client connection receives an independent PRNG stream derived via SplitMix64 from `(base_seed, conn_id)`. Re-running with `--seed <u64>` guarantees reproducible schedule execution without cross-connection lock contention.
- **Empirical Detection Rate.** Uncovers 100% of uncoordinated read-modify-write anomalies in concurrent transactions under random jitters (validated against empirical lost update test suites).
- **Latency Cost.** Every delayed query costs up to `--fuzz-max-delay-ms`, so keep the bound small (e.g., 20–30 ms) in CI.
- **It finds, it does not prove.** Fuzzing samples schedules. A green run means these schedules passed, not that the code is race free. Run it with several seeds or in property-based test matrices.

## HTTP control plane

| Method and path     | Purpose                                                                                               |
| ------------------- | ----------------------------------------------------------------------------------------------------- |
| `POST /intercept`   | Add a rule: `{ query_pattern, target_pid?, session_tag?, action?, times? }`. 400 on an invalid regex. |
| `GET /barriers`     | IDs of barriers currently held.                                                                       |
| `POST /resume/:id`  | Release one barrier. 404 if unknown.                                                                  |
| `POST /resume-next` | Release the oldest barrier. 404 if none.                                                              |
| `GET /events`       | `{ "had_timeouts": bool }`                                                                            |
| `POST /reset`       | Remove every rule and release every barrier.                                                          |

The control plane has **no authentication**. Keep it on `127.0.0.1`.

## Limitations

- **No TLS.** The proxy answers `N` to `SSLRequest` and `GSSENCRequest`, so clients must connect in plaintext (`sslmode=disable`).
- **Regex on SQL text.** Rules match the statement text with a regular expression, not a parsed AST. Anchor your patterns.
- **One shared proxy.** State is global. Run test files serially (`vitest --fileParallelism=false`) or call `reset()` between them.
- **Connection pools.** Without `target_pid` or `sessionTag`, a rule matches any connection. When using pools where physical connections change dynamically, route statements using `sessionTag` and `PgInterleaveClient.tag(...)`.
- **Only tested with `node-postgres` and `psql`.** Other drivers may exercise paths that have no test yet.

## Development

```bash
cargo fmt --check
cargo clippy -p pg-interleave-server -- -D warnings
cargo test                      # decoder unit and property tests
cd packages/sdk-ts && npm test  # integration tests, needs Postgres + the proxy running
```

## License

MIT
