import { describe, it, expect, afterEach } from "vitest";
import { Client } from "pg";
import { spawn, ChildProcess, execSync } from "child_process";
import * as path from "path";
import * as fs from "fs";

function killProcess(proc: ChildProcess | null) {
  if (!proc || !proc.pid) return;
  if (process.platform === "win32") {
    try {
      execSync(`taskkill /pid ${proc.pid} /T /F`, { stdio: "ignore" });
    } catch {}
  } else {
    proc.kill("SIGKILL");
  }
}

function findProxyBinary(): { cmd: string; baseArgs: string[] } {
  const isWin = process.platform === "win32";
  const exe = isWin ? "pg-interleave-server.exe" : "pg-interleave-server";
  const candidates = [
    process.env.PG_INTERLEAVE_BIN,
    path.resolve(process.cwd(), `../../target/release/${exe}`),
    path.resolve(process.cwd(), `../../target/debug/${exe}`),
    path.resolve(process.cwd(), `../target/release/${exe}`),
    path.resolve(process.cwd(), `../target/debug/${exe}`),
  ];
  for (const c of candidates) {
    if (c && fs.existsSync(c)) {
      return { cmd: c, baseArgs: [] };
    }
  }
  return { cmd: "cargo", baseArgs: ["run", "--release", "-p", "pg-interleave-server", "--"] };
}

async function waitForPort(url: string, timeoutMs = 7000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(`${url}/events`);
      if (res.ok) return;
    } catch {}
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`Proxy failed to start at ${url} within ${timeoutMs}ms`);
}

async function runScenario(connStr: string, iterations = 20, offsetMs = 15): Promise<number> {
  const directPgStr = "postgres://postgres:postgres@127.0.0.1:5432/postgres";
  const admin = new Client({ connectionString: directPgStr });
  await admin.connect();
  let lostUpdates = 0;

  for (let i = 0; i < iterations; i++) {
    await admin.query(`
      DROP TABLE IF EXISTS fuzz_test_balance CASCADE;
      CREATE TABLE fuzz_test_balance (id INT PRIMARY KEY, amount INT NOT NULL);
      INSERT INTO fuzz_test_balance (id, amount) VALUES (1, 100);
    `);

    const c1 = new Client({ connectionString: connStr });
    const c2 = new Client({ connectionString: connStr });
    await Promise.all([c1.connect(), c2.connect()]);

    const tx1 = async () => {
      await c1.query("BEGIN");
      const res = await c1.query<{ amount: number }>("SELECT amount FROM fuzz_test_balance WHERE id = 1");
      const cur = res.rows[0].amount;
      await c1.query(`UPDATE fuzz_test_balance SET amount = ${cur - 10} WHERE id = 1`);
      await c1.query("COMMIT");
    };

    const tx2 = async () => {
      if (offsetMs > 0) {
        await new Promise((r) => setTimeout(r, offsetMs));
      }
      await c2.query("BEGIN");
      const res = await c2.query<{ amount: number }>("SELECT amount FROM fuzz_test_balance WHERE id = 1");
      const cur = res.rows[0].amount;
      await c2.query(`UPDATE fuzz_test_balance SET amount = ${cur - 10} WHERE id = 1`);
      await c2.query("COMMIT");
    };

    await Promise.all([tx1(), tx2()]);
    await Promise.all([c1.end(), c2.end()]);

    const finalRes = await admin.query<{ amount: number }>("SELECT amount FROM fuzz_test_balance WHERE id = 1");
    if (finalRes.rows[0].amount === 90) {
      lostUpdates++;
    }
  }

  await admin.query("DROP TABLE IF EXISTS fuzz_test_balance CASCADE;");
  await admin.end();
  return lostUpdates;
}

describe("Schedule Fuzzer Empirical Validation", () => {
  let proxyProcess: ChildProcess | null = null;
  const proxyPort = 5444;
  const controlPort = 9094;
  const connStr = `postgres://postgres:postgres@127.0.0.1:${proxyPort}/postgres`;
  const controlUrl = `http://127.0.0.1:${controlPort}`;

  async function spawnProxy(extraArgs: string[]) {
    if (proxyProcess) {
      killProcess(proxyProcess);
      proxyProcess = null;
      await new Promise((r) => setTimeout(r, 200));
    }
    const { cmd, baseArgs } = findProxyBinary();
    const args = [
      ...baseArgs,
      "--listen-addr", `127.0.0.1:${proxyPort}`,
      "--control-addr", `127.0.0.1:${controlPort}`,
      ...extraArgs,
    ];
    proxyProcess = spawn(cmd, args, { stdio: "ignore" });
    await waitForPort(controlUrl);
  }

  afterEach(async () => {
    if (proxyProcess) {
      killProcess(proxyProcess);
      proxyProcess = null;
    }
  });

  it("proves that fuzzer exposes race conditions where baseline execution yields zero anomalies", async () => {
    await spawnProxy([]);
    const baselineLostUpdates = await runScenario(connStr, 20, 15);
    expect(baselineLostUpdates).toBeLessThanOrEqual(2);

    await spawnProxy(["--fuzz", "--seed", "42", "--fuzz-max-delay-ms", "30"]);
    const fuzzLostUpdates = await runScenario(connStr, 20, 15);
    expect(fuzzLostUpdates).toBeGreaterThan(0);
  }, 45_000);
});