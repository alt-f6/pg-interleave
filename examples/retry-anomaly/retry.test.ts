import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Client } from "pg";
import pRetry from "p-retry";
import { PgInterleaveClient } from "pg-interleave";

const PROXY_URL = "postgres://postgres:postgres@127.0.0.1:5433/postgres";
const chaos = new PgInterleaveClient("http://127.0.0.1:9090");

describe("Transaction Retry Pitfalls: p-retry + PostgreSQL", () => {
  let observer: Client;

  beforeEach(async () => {
    await chaos.reset();

    observer = new Client({ connectionString: PROXY_URL });
    observer.on("error", () => {});
    await observer.connect();

    await observer.query(`
      CREATE TABLE IF NOT EXISTS billing_orders (
        id SERIAL PRIMARY KEY,
        account_id INT NOT NULL,
        amount INT NOT NULL
      );
      TRUNCATE billing_orders;
    `);
  });

  afterEach(async () => {
    await observer.end().catch(() => {});
  });

  it("reproduces silent double-charge: p-retry duplicates non-idempotent transaction on torn commit", async () => {
    let attempt = 0;

    // Бизнес-логика создания заказа на списание $50
    const createOrderTransaction = async () => {
      attempt++;
      const client = new Client({ connectionString: PROXY_URL });
      // Подавляем неперехваченный emit('error') при внезапном разрыве сокета со стороны прокси
      client.on("error", () => {});
      await client.connect();

      // Инжектируем разрыв соединения строго на первой попытке
      if (attempt === 1) {
        await chaos.injectTornCommit(client, 1);
      }

      try {
        await client.query("BEGIN");
        await client.query("INSERT INTO billing_orders (account_id, amount) VALUES (101, 50)");
        await client.query("COMMIT");
      } finally {
        await client.end().catch(() => {});
      }
    };

    // p-retry ловит сетевой сбой на 1-й попытке и повторяет операцию
    await pRetry(createOrderTransaction, {
      retries: 2,
      minTimeout: 50,
      factor: 1,
    });

    // Убеждаемся, что было 2 попытки
    expect(attempt).toBe(2);

    // В базе оказалось ДВЕ записи вместо одной: списано $100 вместо $50
    const result = await observer.query<{ id: number; amount: number }>(
      "SELECT * FROM billing_orders WHERE account_id = 101"
    );

    expect(result.rowCount).toBe(2);
    expect(result.rows[0].amount).toBe(50);
    expect(result.rows[1].amount).toBe(50);
  });

  it("correctly handles synthetic deadlock retry without data duplication", async () => {
    let attempts = 0;

    await pRetry(
      async () => {
        attempts++;
        const client = new Client({ connectionString: PROXY_URL });
        client.on("error", () => {});
        await client.connect();

        if (attempts === 1) {
          await chaos.injectDeadlock({
            client,
            onQueryMatch: /INSERT INTO billing_orders/,
            times: 1,
          });
        }

        try {
          await client.query("BEGIN");
          await client.query("INSERT INTO billing_orders (account_id, amount) VALUES (202, 100)");
          await client.query("COMMIT");
        } catch (err: any) {
          await client.query("ROLLBACK").catch(() => {});
          throw err;
        } finally {
          await client.end().catch(() => {});
        }
      },
      {
        retries: 3,
        minTimeout: 50,
      }
    );

    expect(attempts).toBe(2);

    const result = await observer.query<{ id: number; amount: number }>(
      "SELECT * FROM billing_orders WHERE account_id = 202"
    );
    expect(result.rowCount).toBe(1);
    expect(result.rows[0].amount).toBe(100);
  });
});