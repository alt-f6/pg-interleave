# pg-interleave

A PostgreSQL wire-protocol proxy for **deterministic concurrency tests**.

It sits between your test code and Postgres, understands the v3.0 protocol, and lets a test pause a specific statement, hold its response, tear the connection after a commit, or inject a `40P01` deadlock error, all on command and in a fixed order.

> **Status: alpha.** Small, tested on PostgreSQL 16, Rust server + TypeScript SDK. APIs may change. Plaintext connections only.

```
test code ──HTTP :9090──▶ control plane ─┐
                                          ▼
pg / any client ──:5433──▶ proxy ──────▶ PostgreSQL :5432
```

## Why

Most concurrency tests either run two transactions with `Promise.all` and hope, or sequence them by hand with `await`. The second is already deterministic, as long as you control every statement.

pg-interleave is for the case where you **don't**: a function in your application opens a transaction and runs several statements, and you want to freeze it between two of them, run something else, then let it continue. Nothing in the application code changes. The test only says "stop the connection that is about to run `UPDATE accounts ...`".

If your scenario can be written as plain sequential `await`s on two clients, you don't need this tool. PostgreSQL's own `isolationtester` is also worth knowing: it explores permutations of steps across sessions and lives inside the Postgres source tree.

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
- **Timeouts are errors, not silence.** If a barrier is not released within the TTL (default 5000 ms, `--barrier-ttl-ms`), the client receives SQLSTATE `57014` and `GET /events` reports `had_timeouts: true`, so a test can fail on it.
- **Per-connection targeting.** Rules can be scoped to a backend PID (the SDK reads it with `pg_backend_pid()`).

## Quickstart (from source)

You need a running PostgreSQL, Rust 1.88+ (edition 2024, the code uses let-chains) and Node 20+.

```bash
# 1. start the proxy
cargo run --release -p pg-interleave-server -- \
  --listen-addr 127.0.0.1:5433 \
  --target-addr 127.0.0.1:5432 \
  --control-addr 127.0.0.1:9090

# 2. run the SDK tests
cd packages/sdk-ts
npm ci
npm test
```

Point your client at port **5433** instead of 5432. The control plane is on 9090.

Defaults: listen `127.0.0.1:5433`, target `127.0.0.1:5432`, control `127.0.0.1:9090`, barrier TTL 5000 ms. `--target-addr` takes an IP and port, not a hostname.

## Example: a real lost update

Transaction A reads a balance, then writes a value computed from it. We freeze A just before its `UPDATE`, let B complete a whole transaction, then release A.

```ts
import { Client } from "pg";
import { PgInterleaveClient } from "pg-interleave";

const chaos = new PgInterleaveClient("http://127.0.0.1:9090");
const url = "postgres://postgres:postgres@127.0.0.1:5433/postgres";

await chaos.reset();
const a = new Client({ connectionString: url });
const b = new Client({ connectionString: url });
await a.connect();
await b.connect();

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

The same scenario with `SELECT ... FOR UPDATE` makes B block on A's row lock, and the final balance is 70. Both versions are in `packages/sdk-ts/src/lost-update.test.ts`.

## SDK

```ts
const chaos = new PgInterleaveClient("http://127.0.0.1:9090");

await chaos.reset(); // clear all rules, release all barriers
const barrier = await chaos.intercept({
  // pause_before_query by default
  client, // or clientPid
  onQueryMatch: /regex/,
  action: "pause_after_response", // optional
  times: 1, // optional, default 1
});
await barrier.wait(); // resolves when the statement is held
await barrier.resume(); // release it

await chaos.injectTornCommit(client);
await chaos.injectDeadlock({ client, onQueryMatch: /UPDATE seats/ });
await chaos.hasTimeouts(); // true if any barrier hit its TTL
```

## HTTP control plane

| Method and path     | Purpose                                                                                 |
| ------------------- | --------------------------------------------------------------------------------------- |
| `POST /intercept`   | Add a rule: `{ query_pattern, target_pid?, action?, times? }`. 400 on an invalid regex. |
| `GET /barriers`     | IDs of barriers currently held.                                                         |
| `POST /resume/:id`  | Release one barrier. 404 if unknown.                                                    |
| `POST /resume-next` | Release the oldest barrier. 404 if none.                                                |
| `GET /events`       | `{ "had_timeouts": bool }`                                                              |
| `POST /reset`       | Remove every rule and release every barrier.                                            |

The control plane has **no authentication**. Keep it on `127.0.0.1`.

## Limitations

- **No TLS.** The proxy answers `N` to `SSLRequest` and `GSSENCRequest`, so clients must connect in plaintext (`sslmode=disable`).
- **Regex on SQL text.** Rules match the statement text with a regular expression, not a parsed AST. Anchor your patterns.
- **One shared proxy.** State is global. Run test files serially (`vitest --fileParallelism=false`) or call `reset()` between them.
- **Only tested with `node-postgres` and `psql`.** Other drivers may exercise paths that have no test yet.
- **Docker.** The included `docker-compose.yml` passes `--target-addr postgres:5432`, which the CLI cannot parse (it needs an IP). Use the from-source quickstart until that is fixed.

## Development

```bash
cargo fmt --check
cargo clippy -p pg-interleave-server -- -D warnings
cargo test                      # decoder unit and property tests
cd packages/sdk-ts && npm test  # integration tests, needs Postgres + the proxy running
```

## License

MIT
