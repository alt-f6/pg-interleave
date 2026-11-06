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
    await client?.end().catch(() => {});
  });

  it("rejects invalid regex pattern with HTTP 400", async () => {
    await expect(
      chaos.intercept({
        client,
        onQueryMatch: "[invalid(regex+",
      })
    ).rejects.toThrow(/HTTP 400/);
  });

  it("verifies PauseAfterResponse with parameterized query holds data until release", async () => {
    const barrier = await chaos.intercept({
      client,
      onQueryMatch: /SELECT \$1::int as val/,
      action: "pause_after_response",
    });

    let clientReceived = false;
    const queryPromise = client.query({
      name: "fetch_param",
      text: "SELECT $1::int as val",
      values: [999],
    }).then((res) => {
      clientReceived = true;
      return res;
    });

    await barrier.wait();
    expect(clientReceived).toBe(false);

    await barrier.resume();
    const res = await queryPromise;
    expect(clientReceived).toBe(true);
    expect(res.rows[0].val).toBe(999);
  });

  it("aborts extended protocol query with SQLSTATE 57014 when barrier TTL expires", async () => {
    await chaos.intercept({
      client,
      onQueryMatch: /SELECT \$1::int as timeout_val/,
    });

    let errorCode = "";
    try {
      await client.query({
        name: "test_timeout_stmt",
        text: "SELECT $1::int as timeout_val",
        values: [888],
      });
    } catch (err: any) {
      errorCode = err.code;
    }

    expect(errorCode).toBe("57014");
    expect(await chaos.hasTimeouts()).toBe(true);
  }, 10000);

  it("pauses prepared statement only on second execution cycle", async () => {
    const queryConfig = {
      name: "re_exec_stmt",
      text: "SELECT ($1::int * 2) as doubled",
      values: [5],
    };

    // Первая итерация выполняется свободно
    const firstRes = await client.query(queryConfig);
    expect(firstRes.rows[0].doubled).toBe(10);

    // Ставим перехватчик перед вторым вызовом
    const barrier = await chaos.intercept({
      client,
      onQueryMatch: /SELECT \(\$1::int \* 2\) as doubled/,
    });

    queryConfig.values = [10];
    let secondFinished = false;
    const secondPromise = client.query(queryConfig).then((r) => {
      secondFinished = true;
      return r;
    });

    await barrier.wait();
    expect(secondFinished).toBe(false);

    await barrier.resume();
    const secondRes = await secondPromise;
    expect(secondFinished).toBe(true);
    expect(secondRes.rows[0].doubled).toBe(20);
  });
});