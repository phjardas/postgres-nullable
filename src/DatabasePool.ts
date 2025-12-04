import { Pool } from "pg";
import { pino } from "pino";

const log = pino({ name: "db" });

export type QueryResult<T> = {
  readonly rows: readonly T[];
};

export type Queryable = {
  query<T = unknown>(
    query: string,
    params?: unknown[]
  ): Promise<QueryResult<T>>;
};

export type Connection = Queryable;

export type DatabasePool = Queryable;

export const getDatabasePool: () => DatabasePool = memoize(() => {
  const pool = new Pool();

  pool.on("connect", () => log.trace("connect"));
  pool.on("acquire", () => log.trace("acquire"));
  pool.on("remove", () => log.trace("remove"));
  pool.on("error", (err) => log.error({ err }, "Database error"));

  process.on("SIGTERM", async () => {
    try {
      log.info("SIGTERM received: closing database pool");
      await pool.end();
    } catch (err) {
      log.error({ err }, "Error during database pool shutdown");
    }
  });

  return pool;
});

function memoize<T>(factory: () => T): () => T {
  let value: T | undefined = undefined;
  return () => {
    if (value === undefined) value = factory();
    return value;
  };
}
