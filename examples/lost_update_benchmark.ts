import { Client } from "pg";

const connStr = process.env.DATABASE_URL || "postgres://postgres:postgres@127.0.0.1:5433/postgres";
const controlUrl = process.env.CONTROL_URL || "http://127.0.0.1:9090";

async function checkProxyMode(): Promise<boolean | null> {
  try {
    const res = await fetch(`${controlUrl}/events`);
    if (res.ok) {
      const data = (await res.json()) as { fuzzer_enabled?: boolean };
      return !!data.fuzzer_enabled;
    }
  } catch {}
  return null;
}

async function runBenchmark(offsetMs: number, runs = 100): Promise<number> {
  const admin = new Client({ connectionString: connStr });
  await admin.connect();
  let lostUpdates = 0;

  for (let i = 0; i < runs; i++) {
    await admin.query(`
      DROP TABLE IF EXISTS lost_benchmark CASCADE;
      CREATE TABLE lost_benchmark (id INT PRIMARY KEY, balance INT NOT NULL);
      INSERT INTO lost_benchmark (id, balance) VALUES (1, 100);
    `);

    const c1 = new Client({ connectionString: connStr });
    const c2 = new Client({ connectionString: connStr });
    await Promise.all([c1.connect(), c2.connect()]);

    const tx1 = async () => {
      await c1.query("BEGIN");
      const res = await c1.query<{ balance: number }>("SELECT balance FROM lost_benchmark WHERE id = 1");
      const cur = res.rows[0].balance;
      await c1.query(`UPDATE lost_benchmark SET balance = ${cur - 10} WHERE id = 1`);
      await c1.query("COMMIT");
    };

    const tx2 = async () => {
      if (offsetMs > 0) {
        await new Promise((r) => setTimeout(r, offsetMs));
      }
      await c2.query("BEGIN");
      const res = await c2.query<{ balance: number }>("SELECT balance FROM lost_benchmark WHERE id = 1");
      const cur = res.rows[0].balance;
      await c2.query(`UPDATE lost_benchmark SET balance = ${cur - 10} WHERE id = 1`);
      await c2.query("COMMIT");
    };

    await Promise.all([tx1(), tx2()]);
    await Promise.all([c1.end(), c2.end()]);

    const finalRes = await admin.query<{ balance: number }>("SELECT balance FROM lost_benchmark WHERE id = 1");
    if (finalRes.rows[0].balance === 90) {
      lostUpdates++;
    }
  }

  await admin.query("DROP TABLE IF EXISTS lost_benchmark CASCADE;");
  await admin.end();
  return lostUpdates;
}

async function main() {
  const fuzzerActive = await checkProxyMode();
  const modeStr = fuzzerActive === null ? "UNKNOWN (Control API unreachable)" : fuzzerActive ? "ENABLED (--fuzz active)" : "DISABLED (baseline mode)";
  console.log(`[lost_update_benchmark] Target: ${connStr}`);
  console.log(`[lost_update_benchmark] Schedule Fuzzer: ${modeStr}`);
  console.log("Running Lost Update Benchmark (100 iterations per scenario)...\n");

  const offset5 = await runBenchmark(5, 100);
  console.log(`Offset 5ms:  ${offset5}/100 lost updates detected`);

  const offset15 = await runBenchmark(15, 100);
  console.log(`Offset 15ms: ${offset15}/100 lost updates detected`);
}

main().catch(console.error);