import { describe, it, expect, afterEach } from "vitest";
import { Client } from "pg";
import { PgInterleaveClient } from "./index";
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

describe("Trace Recorder & Replay End-to-End", () => {
  let proxyProcess: ChildProcess | null = null;
  const proxyPort = 5445;
  const controlPort = 9095;
  const connStr = `postgres://postgres:postgres@127.0.0.1:${proxyPort}/postgres`;
  const directPgStr = "postgres://postgres:postgres@127.0.0.1:5432/postgres";
  const controlUrl = `http://127.0.0.1:${controlPort}`;
  const tracePath = path.resolve(process.cwd(), "test_replay_trace.json");

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
    if (fs.existsSync(tracePath)) {
      try { fs.unlinkSync(tracePath); } catch {}
    }
  });

  it("records schedule to disk and enforces execution sequence under --replay", async () => {
    const admin = new Client({ connectionString: directPgStr });
    await admin.connect();
    await admin.query(`
      DROP TABLE IF EXISTS replay_bank CASCADE;
      CREATE TABLE replay_bank (id INT PRIMARY KEY, balance INT NOT NULL);
      INSERT INTO replay_bank (id, balance) VALUES (1, 100);
    `);

    await spawnProxy(["--record", tracePath]);
    const chaos = new PgInterleaveClient(controlUrl);

    const c1 = new Client({ connectionString: connStr });
    const c2 = new Client({ connectionString: connStr });
    await c1.connect(); 
    await c2.connect(); 

    await c1.query("BEGIN");
    await c1.query("SELECT balance FROM replay_bank WHERE id = 1");
    await c1.query("UPDATE replay_bank SET balance = balance - 10 WHERE id = 1");
    await c1.query("COMMIT");

    await c2.query("BEGIN");
    await c2.query("SELECT balance FROM replay_bank WHERE id = 1");
    await c2.query("UPDATE replay_bank SET balance = balance - 20 WHERE id = 1");
    await c2.query("COMMIT");

    await chaos.flushTrace();
    await Promise.all([c1.end(), c2.end()]);

    expect(fs.existsSync(tracePath)).toBe(true);
    const trace = JSON.parse(fs.readFileSync(tracePath, "utf-8"));
    expect(trace.steps.length).toBeGreaterThanOrEqual(8);

    await admin.query("UPDATE replay_bank SET balance = 100 WHERE id = 1");

    await spawnProxy(["--replay", tracePath]);

    const r1 = new Client({ connectionString: connStr });
    const r2 = new Client({ connectionString: connStr });
    await r1.connect(); 
    await r2.connect();

    const executionOrder: string[] = [];

    const run2 = (async () => {
      await r2.query("BEGIN");
      await r2.query("SELECT balance FROM replay_bank WHERE id = 1");
      await r2.query("UPDATE replay_bank SET balance = balance - 20 WHERE id = 1");
      await r2.query("COMMIT");
      executionOrder.push("r2");
    })();

    await new Promise((r) => setTimeout(r, 40));

    const run1 = (async () => {
      await r1.query("BEGIN");
      await r1.query("SELECT balance FROM replay_bank WHERE id = 1");
      await r1.query("UPDATE replay_bank SET balance = balance - 10 WHERE id = 1");
      await r1.query("COMMIT");
      executionOrder.push("r1");
    })();

    await Promise.all([run1, run2]);
    await Promise.all([r1.end(), r2.end()]);

    expect(executionOrder).toEqual(["r1", "r2"]);

    const finalRes = await admin.query<{ balance: number }>("SELECT balance FROM replay_bank WHERE id = 1");
    expect(finalRes.rows[0].balance).toBe(70);
    await admin.end();
  }, 30_000);
});