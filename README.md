# pg-interleave

[![CI](https://github.com/alt-f6/pg-interleave/actions/workflows/ci.yml/badge.svg)](https://github.com/alt-f6/pg-interleave/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![npm version](https://img.shields.io/npm/v/pg-interleave.svg)](https://www.npmjs.com/package/pg-interleave)

<p align="center">
  <img src="./demo.gif" alt="pg-interleave demo" width="850">
</p>

**Deterministic concurrency tests for PostgreSQL.** A wire-protocol proxy that sits between your code and Postgres and lets a test freeze a specific statement, hold its response, tear a connection right after a commit, or inject a `40P01` deadlock, on command and in a fixed order. No changes to the application code.

> **Status: alpha.** Rust server plus TypeScript SDK. CI runs against PostgreSQL 14, 15 and 16. APIs may change. Plaintext connections only (see [Limitations](#limitations--protocol-edge-cases)).

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
  <a href="#trace-record--replay">Record & Replay</a> •
  <a href="#real-world-case-study-pg-boss">pg-boss case study</a> •
  <a href="#http-control-plane">HTTP API</a> •
  <a href="#comparison-with-alternatives">Alternatives</a> •
  <a href="#limitations--protocol-edge-cases">Limitations</a>
</p>

---

## Results at a glance

| Scenario                                                                        | Plain test run             | With the schedule fuzzer     |
| ------------------------------------------------------------------------------- | -------------------------- | ---------------------------- |
| Uncoordinated read-modify-write, two concurrent transactions, 100 runs per cell | **0–5 / 100** lost updates | **85–98 / 100** lost updates |

The race is real in both runs. Without the fuzzer, queries finish in well under a millisecond, so the transactions almost never overlap. The fuzzer moves them into overlapping orders. Method and commands to reproduce are in [Empirical detection rate](#capabilities--guarantees).

## Why

Most concurrency tests either run two transactions with `Promise.all` and hope, or sequence them by hand with `await`. The second is already deterministic, as long as you control every statement.

pg-interleave is for the case where you **don't**: a function in your application opens a transaction and runs several statements, and you want to freeze it between two of them, run something else, then let it continue. The test only says "stop the connection that is about to run `UPDATE accounts ...`".

If your scenario can be written as plain sequential `await`s on two clients, you don't need this tool. PostgreSQL's own `isolationtester` is also worth knowing: it explores permutations of steps across sessions and lives inside the Postgres source tree.

### Why not just `Promise.all` or `sleep(50)`?

- **Uncoordinated tests pass by accident.** Racing transactions rarely overlap on a local connection. The bug is there, but the plain test almost never sees it (see the benchmark in the Schedule fuzzer section).
- **`sleep()` is slow and brittle.** Arbitrary delays lengthen CI and still fail under CPU throttling or a busy runner.
- **Barriers pin the order.** pg-interleave pauses statements at the PostgreSQL wire-protocol boundary, so you choose which statement goes first, every time.

## Install

> Docker images, release binaries and the npm package are published with each GitHub release (latest: **v0.7.1**).

### 1. Run the proxy

**Docker.** Pass all three flags: `docker run` arguments replace the image defaults.

```bash
docker run -d --name pg-interleave \
  --add-host=host.docker.internal:host-gateway \
  -p 127.0.0.1:5433:5433 -p 127.0.0.1:9090:9090 \
  ghcr.io/alt-f6/pg-interleave:latest \
  --listen-addr 0.0.0.0:5433 \
  --control-addr 0.0.0.0:9090 \
  --target-addr host.docker.internal:5432
```

**Standalone binary.** Download the archive for your platform from [Releases](https://github.com/alt-f6/pg-interleave/releases):

| Platform            | Asset                                |
| ------------------- | ------------------------------------ |
| Linux x86_64        | `pg-interleave-linux-x86_64.tar.gz`  |
| macOS Apple Silicon | `pg-interleave-macos-aarch64.tar.gz` |
| Windows x86_64      | `pg-interleave-windows-x86_64.zip`   |

The archive contains a single executable named `pg-interleave` (`pg-interleave.exe` on Windows). It is the same program as the `pg-interleave-server` crate.

```bash
tar xzf pg-interleave-linux-x86_64.tar.gz
./pg-interleave --listen-addr 127.0.0.1:5433 --target-addr 127.0.0.1:5432
```

### 2. Install the SDK

```bash
npm install -D pg-interleave
```

Requires Node 20+.

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
npm ci        # also builds dist/ via the prepare script
npm test
```

Point your client at port **5433** instead of 5432. The control plane is on 9090. Defaults: listen `127.0.0.1:5433`, target `127.0.0.1:5432`, control `127.0.0.1:9090`, barrier TTL 5000 ms (`--barrier-ttl-ms`). The target address can be a hostname.

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

| Action                      | What happens                                                                                                                                                                                                   |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pause_before_query`        | The matching statement is held in the proxy. PostgreSQL has not seen it yet. The test releases it when ready.                                                                                                  |
| `pause_after_response`      | The statement runs (locks taken, rows written), but the response is held until the test releases it.                                                                                                           |
| `drop_connection_on_commit` | After PostgreSQL completes the commit (explicit `COMMIT` or a single autocommit statement), the proxy closes the client TCP socket. The commit is durable on disk, but the client never receives confirmation. |
| `synthetic_deadlock`        | The matching statement never reaches the server. The client gets a well-formed `ErrorResponse` with SQLSTATE `40P01` and a `ReadyForQuery`.                                                                    |

Behavior you can rely on (each is covered by a test):

- **FIFO release.** `resume-next` releases pending barriers in creation order.
- **One-shot by default.** A rule fires once (`times: 1`). Pass `times` to change it.
- **Bad patterns fail loudly.** An invalid regex is rejected with HTTP 400 at registration.
- **Timeouts are errors, not silence.** If a barrier is not released within the TTL, the client receives SQLSTATE `57014` and `GET /events` reports `had_timeouts: true`, so a test can fail on it.
- **No silent misses.** `await rule.assertFired(n)` checks that the rule matched exactly `n` times and throws otherwise. If an ORM or library changes its SQL formatting and your pattern stops matching, the test fails instead of passing for the wrong reason.
- **Per-connection targeting.** Rules can be scoped to a backend PID (the SDK reads it with `pg_backend_pid()`) or to a SQL comment tag.
- **Extended protocol.** Prepared statements are matched when executed, including repeated executions, and synthetic deadlocks cleanly recover the protocol stream with standard `ErrorResponse` frames.
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

The retry was not wrong to fire; the transaction was not safe to repeat. The usual fix is an idempotency key with a unique constraint (`INSERT ... ON CONFLICT (idempotency_key) DO NOTHING`). This kind of failure is almost impossible to reproduce with mocks, because the commit really happened on the server.

## Real-world case study: pg-boss

We ran pg-interleave against [`pg-boss`](https://github.com/timgit/pg-boss) v12.38.1, a PostgreSQL job queue, without modifying the library. The suite is in [`examples/pg-boss-exploration`](examples/pg-boss-exploration) (8 tests). What it checks and what it found:

| Scenario                  | Injected fault                                                                                  | Observed                                                                                          |
| ------------------------- | ----------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Deadlock while polling    | `synthetic_deadlock` (`40P01`) on the fetch `UPDATE pgboss.job_common ... SET state = 'active'` | Rule fired once (`assertFired`), the worker kept polling and processed the job                    |
| Torn commit on completion | `drop_connection_on_commit` on the `completed` update                                           | Job ended in `completed`, the handler ran exactly once                                            |
| Held completion           | `pause_before_query` on the `completed` update                                                  | Job stays `active` in the database while the statement is held, becomes `completed` after release |
| Schedule fuzzing          | 50 jobs under `--fuzz --fuzz-max-delay-ms 30`                                                   | Every job executed exactly once                                                                   |
| Expire vs. complete       | Handler sleeps past the visibility timeout                                                      | Job is picked up again (`retry_count: 1`), two executions in total                                |
| Singleton send            | 10 concurrent `send()` calls with one `singletonKey`                                            | One row in the table                                                                              |

None of these produced a bug in pg-boss. They are listed because "the fault really happened and the library coped" is the result the tool is meant to give you.

**What it found in pg-interleave itself.** The first version of the deadlock test passed without the fault ever being injected: pg-boss writes its fetch statement over several lines, and a single-line regex never matched. Nothing failed, because a rule that does not fire is silent by default. That is why `assertFired` exists and why a JavaScript `RegExp` is now compiled with dotAll and keeps its `i` flag (see [Query matching semantics](#query-matching-semantics)).

## SDK

```ts
const chaos = new PgInterleaveClient("http://127.0.0.1:9090");

await chaos.reset(); // clear all rules, release all barriers
const barrier = await chaos.intercept({
  client, // scopes the rule to this client's backend PID
  onQueryMatch: /UPDATE accounts SET balance/,
  withParams: ["*", 100], // optional: match $1, $2 arguments
  action: "pause_before_query", // or "pause_after_response"
  times: 1, // optional, default 1
});

await barrier.wait(); // resolves when the statement is frozen in the proxy
await barrier.resume(); // releases the statement to PostgreSQL
await barrier.assertFired(1); // throws unless the rule fired exactly once

// Network fault injection
const tear = await chaos.intercept({
  client,
  onQueryMatch: /COMMIT/i,
  action: "drop_connection_on_commit",
});
await tear.assertFired(1);

// Synthetic deadlock
const deadlock = await chaos.injectDeadlock({
  onQueryMatch: /UPDATE accounts/,
  times: 1,
});
await deadlock.assertFired(1);

await chaos.hasTimeouts(); // true if any barrier exceeded the TTL
```

### Query matching semantics

- **Multiline and dotAll.** SQL from ORMs and drivers is often multi-line. A JavaScript `RegExp` is always compiled with the Rust `s` flag, so `.` matches `\n`. The `i` and `m` flags are preserved. This differs from plain JavaScript, where `.` does not cross newlines.
- **Plain strings.** A `string` pattern gets `(?s)` prepended. If it already starts with Rust inline flags such as `(?is)`, it is used as-is.
- **Check that it fired.** Patterns match against the exact SQL text the driver sends. When in doubt, record a run with `--record` and read the statements, then call `assertFired()` in the test.

### Connection pool compatibility (SQL comment routing)

With connection pools (`pg.Pool`, Prisma, Drizzle), physical connections are shared dynamically. Route rules with SQL comment tags instead of backend PIDs:

```ts
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

// 2. Tag the query with the SDK helper
const taggedSql = PgInterleaveClient.tag(
  "UPDATE inventory SET stock = stock - 1 WHERE id = 1",
  "workerA",
);
await pool.query(taggedSql);

// 3. Resume when ready
await barrier.resume();
```

### Prepared statement parameter matching (`withParams`)

When an ORM uses parameterized queries (`$1`, `$2`), scope barriers to specific argument values, or use `"*"` as a wildcard:

```ts
// Intercept only when deducting stock for item ID 2
await chaos.intercept({
  client,
  onQueryMatch:
    /UPDATE params_inventory SET stock = stock - \$1 WHERE id = \$2/,
  withParams: [1, 2], // matches $1 = 1, $2 = 2
});

// Wildcard: any quantity, but strictly item ID 1
await chaos.intercept({
  client,
  onQueryMatch:
    /UPDATE params_inventory SET stock = stock - \$1 WHERE id = \$2/,
  withParams: ["*", 1],
});
```

### Structural AST selectors

Match queries structurally (parsed with `sqlparser-rs`) instead of with brittle regexes:

```ts
// Freezes the matching operation on the matching table, whatever the formatting
const barrier = await chaos.intercept({
  client,
  ast: {
    operation: "UPDATE",
    table: "accounts",
  },
});

// Matches regardless of quotes, newlines, or schema prefixes:
await client.query(`
  UPDATE "public"."accounts"
  SET balance = balance - 20
  WHERE id = 1
`);

await barrier.wait();
await barrier.resume();
```

Supported operations: `SELECT`, `INSERT`, `UPDATE`, `DELETE`. A simple-query batch with several statements matches if any statement in it matches. Data-modifying CTEs are not detected, see [Limitations](#limitations--protocol-edge-cases).

## Schedule fuzzer (Automated Race Condition Discovery)

Barriers let you test an interleaving you already thought of. The schedule fuzzer is for the ones you did not: it adds pseudo-random delays before queries (including `BEGIN` and `COMMIT`) are forwarded to PostgreSQL, so concurrent transactions reach the database in different orders from run to run. Run your existing concurrent tests under it and see whether anything breaks, for example a **lost update**.

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
| `--fuzz-max-delay-ms` | Upper bound for a single injected delay, in milliseconds. Default 50.                |
| `--record <path>`     | Record the execution schedule to a JSON trace file.                                  |
| `--replay <path>`     | Replay a previously recorded JSON trace.                                             |

`GET /events` reports `fuzzer_enabled`, so a test or benchmark can confirm which mode it is running in.

### Capabilities & Guarantees

- **Per-connection reproducibility.** Each client connection gets an independent PRNG stream derived with SplitMix64 from `(base_seed, conn_id)`. With the same `--seed`, every connection receives the same sequence of delays, without cross-connection coupling.
- **Empirical detection rate.** Scenario: an uncoordinated read-modify-write on one row, two transactions, no sleeps in the code, the second transaction starting 5 ms or 15 ms after the first, 100 runs per cell ([`examples/lost_update_benchmark.ts`](examples/lost_update_benchmark.ts)).
  - Without the fuzzer (`TCP_NODELAY` enabled on both sockets): **0–5 / 100** lost updates at 5 ms and **0–2 / 100** at 15 ms (baseline scheduling noise varies by machine and OS scheduler).
  - With the fuzzer (`--fuzz --seed 42 --fuzz-max-delay-ms 30`): **85–98 / 100** (e.g. 91–98 / 100 on Windows 11, 85–90 / 100 on Linux).
  - The same seed reproduces the same delay sequence per connection. Detection rates across repeated runs are close but not identical, because client-side timing still varies (see [Limitations](#limitations--protocol-edge-cases)).

  To reproduce:

  ```bash
  cd examples && npm install

  # 1. Baseline run (without fuzzer)
  cargo run --release -p pg-interleave-server -- --listen-addr 127.0.0.1:5433
  npx tsx lost_update_benchmark.ts
  # prints "Schedule Fuzzer: DISABLED", then roughly 0-5/100 (machine-dependent)

  # 2. Fuzzer run with a fixed seed
  cargo run --release -p pg-interleave-server -- --fuzz --seed 42 --fuzz-max-delay-ms 30
  npx tsx lost_update_benchmark.ts
  # prints "Schedule Fuzzer: ENABLED", then roughly 85-98/100
  ```

  The script reads `DATABASE_URL` (default: the local proxy on port 5433) and cleans up its table.

- **Latency cost.** Every delayed query costs up to `--fuzz-max-delay-ms`. Keep the bound small (20–30 ms) in CI.
- **It finds, it does not prove.** Fuzzing samples schedules. A green run means these schedules passed, not that the code is race free. Run it with several seeds.

## Trace record & replay

Record the schedule of a run, then replay it in a regression test.

### 1. Record

Start the proxy with `--record`:

```bash
cargo run --release -p pg-interleave-server -- --record trace.json
```

During the test, write the in-memory trace to disk through the SDK (or `POST /flush-trace`):

```ts
await chaos.flushTrace();
```

The trace is also the quickest way to see the exact SQL a library sends, which helps when writing a pattern.

### 2. Replay

```bash
cargo run --release -p pg-interleave-server -- --replay trace.json
```

The proxy holds outgoing statements until their turn in the recorded schedule, so the captured statement order across connections is reproduced even if the clients start in a different order. This is exercised end to end in [`packages/sdk-ts/src/replay.test.ts`](packages/sdk-ts/src/replay.test.ts): the test sends the second connection's statements first and asserts that the recorded order still wins.

## HTTP control plane

| Method and path     | Purpose                                                                                                                                         |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `POST /intercept`   | Add a rule: `{ query_pattern?, ast?: { operation?, table? }, target_pid?, session_tag?, with_params?, action?, times? }`. 400 on invalid regex. |
| `GET /rules`        | List registered rules with their `fired_count` and `times_remaining`.                                                                           |
| `GET /rules/:id`    | Execution metrics and status for one rule. 404 if not found.                                                                                    |
| `GET /barriers`     | Active barriers held in the proxy: `[ { "id": "barrier_1", "rule_id": "rule_1" } ]`.                                                            |
| `POST /resume/:id`  | Release one barrier by `barrier_id` or `rule_id`. 404 if unknown.                                                                               |
| `POST /resume-next` | Release the oldest active barrier in FIFO order. 404 if none.                                                                                   |
| `POST /flush-trace` | Flush the in-memory trace to the file given in `--record`.                                                                                      |
| `GET /events`       | Proxy status: `{ "had_timeouts": bool, "fuzzer_enabled": bool }`.                                                                               |
| `POST /reset`       | Remove every rule, release every barrier, clear the in-memory trace buffer.                                                                     |

The control plane has **no authentication**. Keep it on `127.0.0.1`.

## Comparison with alternatives

| Feature                          | pg-interleave                      | Toxiproxy                    | PostgreSQL `isolationtester` | In-memory mocks        |
| :------------------------------- | :--------------------------------- | :--------------------------- | :--------------------------- | :--------------------- |
| **Layer**                        | L7 (PostgreSQL wire v3.0)          | L4 (TCP stream)              | PostgreSQL internals (C)     | In-process             |
| **Statement-level interception** | Yes (regex, AST, `$1` params)      | No                           | Yes (spec scripts)           | No (mocked driver)     |
| **Requires code changes**        | No (point the DB URL at port 5433) | No                           | Yes (custom `.spec` files)   | Yes (heavy mock setup) |
| **Real ACID and locks**          | Yes (real PostgreSQL engine)       | Yes                          | Yes                          | No                     |
| **Torn commit injection**        | Yes (drop socket after commit)     | No (cannot inspect `COMMIT`) | No                           | No                     |
| **Synthetic 40P01 deadlocks**    | Yes (wire `ErrorResponse`)         | No                           | No                           | Partial (manual throw) |
| **Schedule fuzzing and replay**  | Yes (SplitMix64 plus trace)        | No                           | No                           | No                     |

Toxiproxy is the right tool for latency, bandwidth and connection-level faults on any TCP service. pg-interleave only speaks PostgreSQL, which is what lets it target a single statement.

## Limitations & Protocol Edge Cases

pg-interleave is an L7 stateful proxy over the PostgreSQL Frontend/Backend Protocol v3.0. Keep these constraints in mind:

- **TLS / SSL.** The proxy inspects unencrypted wire-protocol frames. It answers `'N'` to every `SSLRequest` and `GSSENCRequest` during startup, so clients must allow plaintext (`sslmode=disable` or `sslmode=prefer`). `sslmode=require` and `sslmode=verify-full` abort the handshake.
- **Extended protocol with `Flush` (`'H'`).** The proxy buffers messages and evaluates barriers when it receives `Sync` (`'S'`) or a simple `Query` (`'Q'`). Drivers that pipeline and rely on explicit `Flush` frames to read intermediate results before sending `Sync` can deadlock the socket.
- **Data-modifying CTEs.** The AST selector inspects the root statement. For `WITH moved AS (UPDATE inventory ...) SELECT * FROM moved` the statement is classified as `SELECT`. Use a regex (`onQueryMatch`) for these.
- **Connection poolers in transaction mode (PgBouncer, RDS Proxy, Supavisor).** Server connections and backend PIDs change between transactions, so target rules with `sessionTag` comments rather than `client`. Do not hold a barrier longer than the pooler's `query_wait_timeout`.
- **Global in-memory state.** Rules, barrier queues and connection mappings are shared across the proxy process. Run test files serially (`vitest --fileParallelism=false`), call `await chaos.reset()` between tests, or use distinct `sessionTag` values.
- **Out-of-band cancellation.** A statement held by `pause_before_query` has not reached PostgreSQL. A `CancelRequest` (80877102) from another connection therefore has no effect on the held barrier until it is resumed or exceeds `--barrier-ttl-ms`.
- **Fuzzer determinism boundaries.** `--seed <u64>` fixes the sequence of delays the proxy injects. Client-side scheduling (Node.js event loop latency, OS preemption) still varies by milliseconds, so benchmark figures are an empirical distribution, not a lockstep replay.

## Development

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test                              # decoder unit and property tests

# integration tests need Postgres and the proxy running on 5433/9090
cd packages/sdk-ts && npm ci && npm test

# pg-boss suite
cd examples/pg-boss-exploration && npm install && npm test
```

CI runs the test suite on a matrix of Node 18, 20, 22 and PostgreSQL 14, 15, 16. See [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request, and [SECURITY.md](SECURITY.md) to report a vulnerability.

## License

MIT
