import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Client } from "pg";
import { PgInterleaveClient } from "./index";

describe("Chaos Engige: Network Failures", () => {
    const chaos = new PgInterleaveClient("http://127.0.0.1:9090");
    let client: Client;
    let observer: Client;

    beforeEach(async () => {
        await chaos.reset();

        client = new Client({ connectionString: "postgres://postgres:postgres@127.0.0.1:5433/postgres" });
        observer = new Client({ connectionString: "postgres://postgres:postgres@127.0.0.1:5433/postgres" });

        await client.connect();
        await observer.connect();

        await observer.query(`
            CREATE TABLE IF NOT EXISTS orders (
              id INT PRIMARY KEY,
              status TEXT NOT NULL
            );
            DELETE FROM orders where id = 999;
        `);
    });

    afterEach(async () => {
        try { await client.end(); } catch {}
        await observer.end();
    });

    it("reproduces torn commit: client throws connection error but data is persisted", async () => {
        client.on("error", () => {})
        
        await chaos.injectTornCommit(client);

        let clientFailedWithError: Error | null = null;

        try {
            await client.query("BEGIN");
            await client.query("INSERT INTO orders (id, status) VALUES (999, 'PAID')");
            await client.query("COMMIT");
        } catch (err) {
            clientFailedWithError = err as Error;
        }

        expect(clientFailedWithError).not.toBeNull();

        const res = await observer.query<{ id: number; status: string }>(
            "SELECT * FROM orders WHERE id = 999"
        );
        expect(res.rows.length).toBe(1);
        expect(res.rows[0].status).toBe("PAID")
    });
});