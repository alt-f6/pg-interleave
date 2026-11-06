# Transaction Retry Pitfalls: p-retry + PostgreSQL

A real-world reproduction demonstrating why blindly retrying database transactions on network failures causes **silent data corruption / double-spending**.

## The Problem

Libraries like `p-retry` or `async-retry` automatically retry failed asynchronous functions using exponential backoff.

When an error occurs during an active transaction (`BEGIN ... COMMIT`), developers typically catch errors like `Connection terminated unexpectedly` or `ECONNRESET` and retry the whole function.

However, if the network drops **after PostgreSQL writes the commit to WAL** but **before the client receives `CommandComplete`** (a Torn Commit), the first transaction has already succeeded on disk. The subsequent retry executes the transaction a second time, resulting in duplicate payments, double orders, or corrupted account balances.

## Reproduction

Run with `pg-interleave` running on `:5433` and `:9090`:

```bash
cd examples/retry-anomaly
npm install
npm test
Results
reproduces silent double-charge:

pg-interleave injects drop_connection_on_commit.

p-retry catches socket error and triggers Attempt 2.

SELECT * FROM billing_orders returns 2 rows ($100 total charged) instead of 1 ($50).

Resolution:

Transactions retried on transient connection errors must use idempotency keys (e.g. INSERT ... ON CONFLICT (idempotency_key) DO NOTHING) to protect against torn commits.
```
