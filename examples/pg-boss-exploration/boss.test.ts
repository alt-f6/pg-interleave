import { describe, it, expect, beforeEach, afterEach } from "vitest";
// @ts-ignore
import * as pgbossModule from "pg-boss";
import { Client } from "pg";
import { PgInterleaveClient } from "pg-interleave";

const PgBoss: any =
  pgbossModule.PgBoss ||
  (pgbossModule as any).default?.PgBoss ||
  (pgbossModule as any).default ||
  pgbossModule;

const PG_PROXY_URL = "postgres://postgres:postgres@127.0.0.1:5433/postgres";
const PG_DIRECT_URL = "postgres://postgres:postgres@127.0.0.1:5432/postgres";
const CONTROL_URL = "http://127.0.0.1:9090";

describe("pg-boss Concurrency & Fault Tolerance via pg-interleave", () => {
  let chaos: PgInterleaveClient;
  let adminClient: Client;

  beforeEach(async () => {
    chaos = new PgInterleaveClient(CONTROL_URL);
    await chaos.reset();

    adminClient = new Client({ connectionString: PG_DIRECT_URL });
    await adminClient.connect();
    await adminClient.query("DROP SCHEMA IF EXISTS pgboss CASCADE;");
  });

  afterEach(async () => {
    if (adminClient) {
      await adminClient.end().catch(() => {});
    }
    await chaos.reset();
  });

  it("handles high concurrency without duplicate task dispatches", async () => {
    const boss = new PgBoss({
      connectionString: PG_PROXY_URL,
      schema: "pgboss",
      max: 10,
    });

    try {
      await boss.start();

      const queue = "order-processing";
      await boss.createQueue(queue);

      const jobExecutions = new Map<string, number>();

      await boss.work(
        queue,
        { batchSize: 20, pollingIntervalSeconds: 0.5 },
        async (jobs: any) => {
          const list = Array.isArray(jobs) ? jobs : [jobs];
          for (const job of list) {
            if (!job?.id) continue;
            const count = jobExecutions.get(job.id) || 0;
            jobExecutions.set(job.id, count + 1);
          }
        }
      );

      for (let i = 0; i < 20; i++) {
        await boss.send(queue, { item: `item_${i}` });
      }

      const start = Date.now();
      while (jobExecutions.size < 20 && Date.now() - start < 12_000) {
        await new Promise((r) => setTimeout(r, 100));
      }

      expect(jobExecutions.size).toBe(20);

      for (const [id, count] of jobExecutions.entries()) {
        expect(count, `Job ${id} executed multiple times`).toBe(1);
      }
    } finally {
      await boss.stop({ graceful: false });
    }
  }, 20_000);

  it("recovers safely from torn commit during job completion", async () => {
    const boss = new PgBoss({
      connectionString: PG_PROXY_URL,
      schema: "pgboss",
      max: 2,
    });

    boss.on("error", () => {});

    try {
      await boss.start();
      const queueName = "torn-commit-queue";
      await boss.createQueue(queueName);

      const completeRule = await chaos.intercept({
        onQueryMatch: /UPDATE.*job.*SET.*state\s*=\s*'completed'/i,
        action: "drop_connection_on_commit",
        times: 1,
      });

      const jobId = await boss.send(queueName, { task: 1 });
      let handlerExecuted = 0;

      await boss.work(queueName, async () => {
        handlerExecuted++;
      });

      await new Promise((r) => setTimeout(r, 2000));

      await completeRule.assertFired(1);

      const res = await adminClient.query(
        "SELECT state, retry_count FROM pgboss.job WHERE id = $1",
        [jobId]
      );
      expect(res.rows[0].state).toBe("completed");
      expect(handlerExecuted).toBe(1);
    } finally {
      await boss.stop({ graceful: false });
    }
  }, 15_000);

  it("holds completion query at wire level before database commits", async () => {
    const boss = new PgBoss({
      connectionString: PG_PROXY_URL,
      schema: "pgboss",
      max: 2,
    });

    try {
      await boss.start();
      const queueName = "wire-hold-queue";
      await boss.createQueue(queueName);

      const barrier = await chaos.intercept({
        onQueryMatch: /UPDATE.*job.*SET.*state\s*=\s*'completed'/i,
        action: "pause_before_query",
        times: 1,
      });

      const jobId = await boss.send(queueName, { value: "hold" });
      let processed = false;

      await boss.work(queueName, async () => {
        processed = true;
      });

      await barrier.wait(5000);

      const midRes = await adminClient.query(
        "SELECT state FROM pgboss.job WHERE id = $1",
        [jobId]
      );
      expect(midRes.rows[0].state).toBe("active");

      await barrier.resume();
      await barrier.assertFired(1);

      await new Promise((r) => setTimeout(r, 800));

      const finalRes = await adminClient.query(
        "SELECT state FROM pgboss.job WHERE id = $1",
        [jobId]
      );
      expect(finalRes.rows[0].state).toBe("completed");
      expect(processed).toBe(true);
    } finally {
      await boss.stop({ graceful: false });
    }
  }, 20_000);

  it("stress: handles 50 concurrent jobs under active schedule fuzzing without race conditions", async () => {
    const boss = new PgBoss({
      connectionString: PG_PROXY_URL,
      schema: "pgboss",
      max: 10,
    });

    try {
      await boss.start();

      const queue = "fuzz-stress-queue";
      await boss.createQueue(queue);

      const executions = new Map<string, number>();

      await boss.work(
        queue,
        { batchSize: 5, pollingIntervalSeconds: 0.5 },
        async (jobs: any) => {
          const list = Array.isArray(jobs) ? jobs : [jobs];
          for (const job of list) {
            const current = executions.get(job.id) || 0;
            executions.set(job.id, current + 1);
            await new Promise((r) => setTimeout(r, Math.random() * 20));
          }
        }
      );

      for (let i = 0; i < 50; i++) {
        await boss.send(queue, { index: i });
      }

      const start = Date.now();
      while (executions.size < 50 && Date.now() - start < 25_000) {
        await new Promise((r) => setTimeout(r, 200));
      }

      expect(executions.size).toBe(50);
      for (const [id, count] of executions.entries()) {
        expect(count, `Job ${id} had duplicate execution!`).toBe(1);
      }
    } finally {
      await boss.stop({ graceful: false });
    }
  }, 35_000);

  it("verifies task remains in 'created' state while worker acquisition is held by proxy", async () => {
    const boss = new PgBoss({
      connectionString: PG_PROXY_URL,
      schema: "pgboss",
      expireInSeconds: 5,
    });

    try {
      await boss.start();

      const queue = "zombie-recovery-queue";
      await boss.createQueue(queue);

      const barrier = await chaos.intercept({
        onQueryMatch: /UPDATE.*pgboss\.job.*SET.*state\s*=\s*'active'/i,
        action: "pause_before_query",
        times: 1,
      });

      await boss.work(
        queue,
        { batchSize: 1, pollingIntervalSeconds: 0.5 },
        async () => {}
      );

      const jobId = await boss.send(queue, { payload: "critical_data" });

      await barrier.wait(5000);

      const midRes = await adminClient.query(
        "SELECT state, retry_count FROM pgboss.job WHERE id = $1",
        [jobId]
      );
      expect(midRes.rows[0]?.state).toBe("created");

      await barrier.resume();

      await new Promise((r) => setTimeout(r, 1500));

      const finalRes = await adminClient.query(
        "SELECT state, retry_count FROM pgboss.job WHERE id = $1",
        [jobId]
      );
      expect(finalRes.rows[0]?.state).toBe("completed");

      await barrier.assertFired(1);
    } finally {
      await boss.stop({ graceful: false });
    }
  }, 20_000);

  it("error resilience: worker survives synthetic 40P01 deadlock during polling and resumes processing", async () => {
    const boss = new PgBoss({
      connectionString: PG_PROXY_URL,
      schema: "pgboss",
      max: 2,
    });

    const emittedErrors: Error[] = [];
    boss.on("error", (err: any) => emittedErrors.push(err));

    try {
      await boss.start();
      const queueName = "resilience-queue";
      await boss.createQueue(queueName);

      const deadlockRule = await chaos.intercept({
        onQueryMatch: /WITH\s+next\s+AS.*job/i,
        action: "synthetic_deadlock",
        times: 1,
      });

      let taskDone = false;
      await boss.work(queueName, async () => {
        taskDone = true;
      });

      await new Promise((r) => setTimeout(r, 800));

      await deadlockRule.assertFired(1);

      await boss.send(queueName, { test: "survive" });

      const start = Date.now();
      while (!taskDone && Date.now() - start < 5000) {
        await new Promise((r) => setTimeout(r, 100));
      }

      expect(taskDone).toBe(true);
    } finally {
      await boss.stop({ graceful: false });
    }
  }, 20_000);

  it("singleton integrity: ensures exactly one job is queued under concurrent send race", async () => {
    const boss = new PgBoss({
      connectionString: PG_PROXY_URL,
      schema: "pgboss",
    });

    try {
      await boss.start();

      const queue = "singleton-payout-queue";
      await boss.createQueue(queue);

      const singletonKey = "user_withdraw_tx_42";

      const sendPromises = Array.from({ length: 10 }).map((_, i) =>
        boss.send(
          queue,
          { attempt: i, amount: 100 },
          { singletonKey, singletonSeconds: 60 }
        )
      );

      const results = await Promise.all(sendPromises);
      const acceptedJobIds = results.filter((id) => id !== null);
      expect(acceptedJobIds.length).toBeGreaterThanOrEqual(1);

      const dbRes = await adminClient.query(
        "SELECT count(*)::int as count FROM pgboss.job WHERE singleton_key = $1",
        [singletonKey]
      );

      expect(dbRes.rows[0].count, "Singleton violation: multiple jobs created in database!").toBe(1);
    } finally {
      await boss.stop({ graceful: false });
    }
  }, 15_000);

  it("expire vs complete boundary: handles delayed completion after visibility timeout expiration", async () => {
    const boss = new PgBoss({
      connectionString: PG_PROXY_URL,
      schema: "pgboss",
      maintenanceIntervalSeconds: 1,
    });

    try {
      await boss.start();

      const queue = "expire-boundary-queue";
      await boss.createQueue(queue, {
        expireInSeconds: 2,
        retryLimit: 1,
        retryDelay: 1,
      });

      let executionsCount = 0;

      await boss.work(
        queue,
        { batchSize: 1, pollingIntervalSeconds: 0.5 },
        async () => {
          executionsCount++;
          if (executionsCount === 1) {
            await new Promise((r) => setTimeout(r, 3500));
          }
        }
      );

      const jobId = await boss.send(queue, { op: "process_ledger" });
      expect(jobId).toBeDefined();

      await new Promise((r) => setTimeout(r, 6000));

      expect(executionsCount).toBeGreaterThanOrEqual(2);
    } finally {
      await boss.stop({ graceful: false });
    }
  }, 20_000);
});