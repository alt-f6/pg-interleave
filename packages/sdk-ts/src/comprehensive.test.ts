import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Client } from "pg";
import { PgInterleaveClient } from "./index";

describe("Protocol & Barrier Guarantees", () => {
  const chaos = new PgInterleaveClient("http://127.0.0.1:9090");
  let client: Client;

  beforeEach(async () => {
    await chaos.reset();
    client = new Client({
      connectionString: "postgres://postgres:postgres@127.0.0.1:5433/postgres",
    });
    await client.connect();
  });

  afterEach(async () => {
    await client.end().catch(() => {});
  });

  it("rejects invalid regex pattern with HTTP 400", async () => {
    await expect(
      chaos.intercept({
        client,
        onQueryMatch: "[invalid(regex+",
      })
    ).rejects.toThrow(/HTTP 400/);
  });

  it("verifies PauseAfterResponse holds data before releasing to client", async () => {
    const barrier = await chaos.intercept({
      client,
      onQueryMatch: /SELECT 777/,
      action: "pause_after_response",
    });

    let clientReceived = false;
    const queryPromise = client.query("SELECT 777 as val").then((res) => {
      clientReceived = true;
      return res;
    });

    await barrier.wait();
    expect(clientReceived).toBe(false);

    await barrier.resume();
    const res = await queryPromise;
    expect(clientReceived).toBe(true);
    expect(res.rows[0].val).toBe(777);
  });

  it("aborts query with SQLSTATE 57014 when barrier TTL expires", async () => {
    await chaos.intercept({
      client,
      onQueryMatch: /SELECT 888/,
    });

    let errorCode = "";
    try {
      await client.query("SELECT 888");
    } catch (err: any) {
      errorCode = err.code;
    }

    expect(errorCode).toBe("57014");
    expect(await chaos.hasTimeouts()).toBe(true);
  }, 10000);

  it("supports prepared statement re-execution across multiple cycles", async () => {
    const q = {
      name: "get_square",
      text: "SELECT ($1::int * $1::int) as sq",
      values: [4],
    };

    const first = await client.query(q);
    expect(first.rows[0].sq).toBe(16);

    q.values = [5];
    const second = await client.query(q);
    expect(second.rows[0].sq).toBe(25);
  });
});