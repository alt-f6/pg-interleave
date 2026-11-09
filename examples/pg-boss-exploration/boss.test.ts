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

const PROXY_URL = "postgres://postgres:postgres@127.0.0.1:5433/postgres";
const DIRECT_PG_URL = "postgres://postgres:postgres@127.0.0.1:5432/postgres";
const chaos = new PgInterleaveClient("http://127.0.0.1:9090");

describe("pg-boss Concurrency & Fault Tolerance via pg-interleave", () => {
  let adminClient: Client;

  beforeEach(async () => {
    await chaos.reset();

    adminClient = new Client({ connectionString: DIRECT_PG_URL });
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
      connectionString: PROXY_URL,
      schema: "pgboss",
      max: 10,
    });

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

    await boss.stop({ graceful: true });

    expect(jobExecutions.size).toBe(20);

    for (const [id, count] of jobExecutions.entries()) {
      expect(count, `Job ${id} executed multiple times`).toBe(1);
    }
  }, 20_000);

  it("observes job state when connection drops on complete", async () => {
    const boss = new PgBoss({
      connectionString: PROXY_URL,
      schema: "pgboss",
      expireInSeconds: 3,
    });

    await boss.start();

    const queue = "payment-settlement";
    await boss.createQueue(queue);

    await fetch("http://127.0.0.1:9090/intercept", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        action: "drop_connection_on_commit",
        query_pattern: ".*",
        times: 1,
      }),
    });

    let processedJobId: string | null = null;

    await boss.work(
      queue,
      { batchSize: 1, pollingIntervalSeconds: 0.5 },
      async (jobs: any) => {
        const list = Array.isArray(jobs) ? jobs : [jobs];
        for (const job of list) {
          processedJobId = job.id;
        }
      }
    );

    const id = await boss.send(queue, { amount: 500 });
    expect(id).toBeDefined();

    await new Promise((r) => setTimeout(r, 2000));

    const res = await adminClient.query(
      "SELECT id, state, retry_count FROM pgboss.job WHERE id = $1",
      [id]
    );

    console.log("[pg-boss exploration] Job record in DB:", res.rows[0]);
    expect(res.rows.length).toBe(1);

    await boss.stop({ graceful: false });
  }, 15_000);

  it("stress: handles 50 concurrent jobs under active schedule fuzzing without race conditions", async () => {
    const boss = new PgBoss({
      connectionString: PROXY_URL,
      schema: "pgboss",
      max: 10,
    });

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

    await boss.stop({ graceful: true });

    expect(executions.size).toBe(50);
    for (const [id, count] of executions.entries()) {
      expect(count, `Job ${id} had duplicate execution!`).toBe(1);
    }
  }, 35_000);

  it("verifies task remains in 'created' state while worker acquisition is held by proxy", async () => {
    const boss = new PgBoss({
      connectionString: PROXY_URL,
      schema: "pgboss",
      expireInSeconds: 5,
    });

    await boss.start();

    const queue = "zombie-recovery-queue";
    await boss.createQueue(queue);

    // Перехватываем первый UPDATE воркера (попытку перевести задачу в active)
    const barrier = await chaos.intercept({
      onQueryMatch: /UPDATE.*job/i,
      action: "pause_before_query",
      times: 1,
    });

    await boss.work(
      queue,
      { batchSize: 1, pollingIntervalSeconds: 0.5 },
      async () => {
        // Воркер выполняет логику
      }
    );

    const jobId = await boss.send(queue, { payload: "critical_data" });

    // Ждём, пока прокси перехватит запрос захвата задачи воркером
    await barrier.wait();

    // Проверяем: пока запрос висит в барьере прокси, в БД задача строго 'created' (воркер её ещё не получил)
    const midRes = await adminClient.query(
      "SELECT state, retry_count FROM pgboss.job WHERE id = $1",
      [jobId]
    );
    console.log("[pg-boss exploration] State while acquisition is frozen in proxy:", midRes.rows[0]?.state);
    expect(midRes.rows[0]?.state).toBe("created");

    // Отпускаем барьер — разрешаем UPDATE дойти до PostgreSQL
    await barrier.resume();

    // Ждём завершения задачи
    await new Promise((r) => setTimeout(r, 1500));

    const finalRes = await adminClient.query(
      "SELECT state, retry_count FROM pgboss.job WHERE id = $1",
      [jobId]
    );
    console.log("[pg-boss exploration] Final state after resume:", finalRes.rows[0]?.state);
    expect(finalRes.rows[0]?.state).toBe("completed");

    await boss.stop({ graceful: true });
  }, 20_000);
});