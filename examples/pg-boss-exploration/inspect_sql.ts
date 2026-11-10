// @ts-ignore
import * as pgbossModule from "pg-boss";

const PgBoss: any =
  pgbossModule.PgBoss ||
  (pgbossModule as any).default?.PgBoss ||
  (pgbossModule as any).default ||
  pgbossModule;

const PROXY_URL = "postgres://postgres:postgres@127.0.0.1:5433/postgres";

async function main() {
  const boss = new PgBoss({
    connectionString: PROXY_URL,
    schema: "pgboss",
  });

  await boss.start();

  const queue = "inspect-sql-queue";
  await boss.createQueue(queue);

  let finished = false;

  await boss.work(
    queue,
    { batchSize: 1, pollingIntervalSeconds: 0.5 },
    async (jobs: any) => {
      console.log("[inspect] Worker received job payload:", jobs[0]?.data);
      finished = true;
    }
  );

  await boss.send(queue, { probe: "trace_payload" });

  const start = Date.now();
  while (!finished && Date.now() - start < 5000) {
    await new Promise((r) => setTimeout(r, 100));
  }

  await boss.stop({ graceful: true });
  console.log("[inspect] Finished run, stopping boss.");
}

main().catch(console.error);