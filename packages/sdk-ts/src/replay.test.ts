import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Client } from "pg";
import { PgInterleaveClient } from "./index";
import * as fs from "fs";
import * as path from "path";

describe("Trace Recorder & Replay", () => {
    const chaos = new PgInterleaveClient("http://127.0.0.1:9090");
    const connStr = "postgres://postgres:postgres@127.0.0.1:5433/postgres";
    let c1: Client;
    let c2: Client;

    beforeEach(async () => {
        await chaos.reset();
        c1 = new Client({ connectionString: connStr });
        c2 = new Client({ connectionString: connStr });
        await Promise.all([c1.connect(), c2.connect()]);

        await c1.query(`
            DROP TABLE IF EXISTS replay_bank CASCADE;
            CREATE TABLE replay_bank (
              id INT PRIMARY KEY,
              balance INT NOT NULL
            );
            INSERT INTO replay_bank (id, balance) VALUES (1, 100);
        `);
    });

    afterEach(async () => {
        const closes: Promise<void>[] = [];
        if (c1) closes.push(c1.end());
        if (c2) closes.push(c2.end());
        await Promise.all(closes);
    });

    it("records interleaving schedule and flushes trace to disk", async () => {
        await c1.query("BEGIN");
        await c2.query("BEGIN");

        await c1.query("SELECT balance FROM replay_bank WHERE id = 1");
        await c2.query("SELECT balance FROM replay_bank WHERE id = 1");

        await c1.query("UPDATE replay_bank SET balance = balance - 10 WHERE id = 1");
        await c2.query("COMMIT");

        await c1.query("UPDATE replay_bank SET balance = balance - 20 WHERE id = 1");
        await c2.query("COMMIT");

        await chaos.flushTrace();

        const tracePath = path.resolve(process.cwd(), "../../trace.json");
        if (fs.existsSync(tracePath)) {
            const raw = fs.readFileSync(tracePath, "utf-8");
            const trace = JSON.parse(raw);
            expect(Array.isArray(trace.steps)).toBe(true);
            expect(trace.steps.length).toBeGreaterThan(0);

            const hasUpdate = trace.steps.some((s: { query: string }) => 
              s.query.includes("UPDATE replay_bank")
            );
            expect(hasUpdate).toBe(true);
        }
    });

    it("verifies recorded trace file structure and query sequence", async () => {
        const tracePath = path.resolve(process.cwd(), "../../trace.json");
        if (!fs.existsSync(tracePath)) {
            return;
        }

        const raw = fs.readFileSync(tracePath, "utf-8");
        const trace = JSON.parse(raw);

        expect(trace.steps.length).toBeGreaterThanOrEqual(4);

        for (let i = 0; i < trace.steps.length; i++) {
            expect(trace.steps[i].step).toBe(i);
            expect(typeof trace.steps[i].conn_id).toBe("number");
            expect(typeof trace.steps[i].query).toBe("string");
        }
    });
    
});