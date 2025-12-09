import EventEmitter from "node:events";
import { type Logger } from "pino";
import {
  getDatabasePool,
  type DatabasePool,
  type QueryResult,
} from "./DatabasePool.js";

export type Equals<T extends { [key: string]: unknown }> = {
  readonly eq: { readonly column: keyof T & string; readonly value: unknown };
};

export type In<T extends { [key: string]: unknown }> = {
  readonly in: {
    readonly column: keyof T & string;
    readonly values: readonly unknown[];
  };
};

export type NotIn<T extends { [key: string]: unknown }> = {
  readonly notIn: {
    readonly column: keyof T & string;
    readonly values: readonly unknown[];
  };
};

export type ArrayContains<T extends { [key: string]: unknown }> = {
  readonly arrayContains: {
    readonly column: keyof T & string;
    readonly value: unknown;
  };
};

export type TextSearch<T extends { [key: string]: unknown }> = {
  readonly textSearch: {
    readonly columns: readonly (keyof T & string)[];
    readonly value: string;
  };
};

export type Predicate<T extends { [key: string]: unknown }> =
  | Equals<T>
  | In<T>
  | NotIn<T>
  | ArrayContains<T>
  | TextSearch<T>;

export type ColumnOrder<T extends { [key: string]: unknown }> = {
  readonly column: keyof T & string;
  readonly direction: "asc" | "desc";
};

export type Order<T extends { [key: string]: unknown }> =
  readonly ColumnOrder<T>[];

export type SearchSpec<T extends { [key: string]: unknown }> = {
  where?: readonly Predicate<T>[];
  order?: Order<T>;
  offset?: number;
  limit?: number;
};

type DatabaseClientEvents = {
  saved: [
    {
      readonly [key: string]: unknown;
    },
  ];

  updated: [
    {
      readonly where?: readonly Predicate<any>[];
      readonly update: Partial<any>;
    },
  ];

  deleted: [
    {
      readonly where?: readonly Predicate<any>[];
    },
  ];
};

export type DatabaseClientNull = {
  readonly findById?: {
    readonly [table: string]: { [id: string]: any | undefined };
  };
  readonly findOne?: {
    readonly [table: string]: readonly {
      readonly where: readonly Predicate<any>[];
      readonly result?: unknown;
    }[];
  };
  readonly search?: {
    readonly [table: string]: readonly {
      readonly spec: SearchSpec<any>;
      readonly result: readonly unknown[];
    }[];
  };
  readonly count?: {
    readonly [table: string]: readonly {
      readonly where: readonly Predicate<any>[];
      readonly result: number;
    }[];
  };
  readonly logger?: Logger;
};

export class DatabaseClient extends EventEmitter<DatabaseClientEvents> {
  readonly #pool: DatabasePool;
  readonly #logger?: Logger;

  constructor({ pool, logger }: { pool: DatabasePool; logger?: Logger }) {
    super();
    this.#pool = pool;
    this.#logger = logger;
  }

  static create({
    pool,
    logger,
  }: {
    pool?: DatabasePool;
    logger?: Logger;
  } = {}): DatabaseClient {
    return new DatabaseClient({
      pool: pool ?? getDatabasePool(),
      logger,
    });
  }

  static createNull({
    findById,
    findOne,
    search,
    count,
    logger,
  }: DatabaseClientNull = {}): DatabaseClient {
    return new DatabaseClient({
      pool: new PoolStub({
        queries: [
          ...Object.entries(findById ?? {}).flatMap(([table, records]) =>
            Object.entries(records).map(
              ([id, record]) =>
                ({
                  ...DatabaseClient.findByIdQuery<any>(table, id),
                  result: { rows: record ? [record] : [] },
                }) satisfies StubbedQuery
            )
          ),
          ...Object.entries(findOne ?? {}).flatMap(([table, records]) =>
            records.map(
              ({ where, result }) =>
                ({
                  ...DatabaseClient.findOneQuery<any>(table, where),
                  result: { rows: result ? [result] : [] },
                }) satisfies StubbedQuery
            )
          ),
          ...Object.entries(search ?? {}).flatMap(([table, records]) =>
            records.map(
              ({ spec, result }) =>
                ({
                  ...DatabaseClient.searchQuery<any>(table, spec),
                  result: { rows: result },
                }) satisfies StubbedQuery
            )
          ),
          ...Object.entries(count ?? {}).flatMap(([table, records]) =>
            records.map(
              ({ where, result }) =>
                ({
                  ...DatabaseClient.countQuery<any>(table, where),
                  result: { rows: [{ cnt: result }] },
                }) satisfies StubbedQuery
            )
          ),
        ],
      }),
      logger,
    });
  }

  private static findByIdQuery<
    T extends { id: string; [key: string]: unknown },
  >(
    table: string,
    id: string
  ): {
    readonly query: string;
    readonly params: unknown[];
  } {
    return this.findOneQuery<T>(table, [{ eq: { column: "id", value: id } }]);
  }

  async findById<T extends { id: string; [key: string]: unknown }>(
    table: string,
    id: string
  ): Promise<T | undefined> {
    return this.findOne<T>(table, [{ eq: { column: "id", value: id } }]);
  }

  private static findOneQuery<T extends { [key: string]: unknown }>(
    table: string,
    where?: readonly Predicate<T>[]
  ): {
    readonly query: string;
    readonly params: unknown[];
  } {
    const parameters = new Parameters();
    const predicates: string[] = (where ?? []).map((predicate) =>
      applyPredicate(predicate, parameters)
    );

    let query = `SELECT * FROM ${table}`;

    if (predicates.length) {
      query += ` WHERE ${predicates.join(" AND ")}`;
    }

    query += ` LIMIT 1`;

    return { query, params: parameters.parameters };
  }

  async findOne<T extends { [key: string]: unknown }>(
    table: string,
    where?: readonly Predicate<T>[]
  ): Promise<T | undefined> {
    const { query, params } = DatabaseClient.findOneQuery<T>(table, where);
    const { rows } = await this.#query<T>(query, params);
    return rows[0];
  }

  private static searchQuery<T extends { [key: string]: unknown }>(
    table: string,
    spec: SearchSpec<T> = {}
  ): {
    readonly query: string;
    readonly params: unknown[];
  } {
    const parameters = new Parameters();
    const predicates = (spec.where ?? []).map((predicate) =>
      applyPredicate(predicate, parameters)
    );

    let query = `SELECT * FROM ${table}`;

    if (predicates.length) {
      query += ` WHERE ${predicates.join(" AND ")}`;
    }

    if (spec.order && spec.order.length) {
      const orders = spec.order.map(
        (o) => `${o.column} ${o.direction.toUpperCase()}`
      );
      query += ` ORDER BY ${orders.join(", ")}`;
    }

    if (spec.offset !== undefined) {
      query += ` OFFSET ${parameters.param(spec.offset)}`;
    }

    if (spec.limit !== undefined) {
      query += ` LIMIT ${parameters.param(spec.limit)}`;
    }

    return { query, params: parameters.parameters };
  }

  async search<T extends { [key: string]: unknown }>(
    table: string,
    spec: SearchSpec<T> = {}
  ): Promise<readonly T[]> {
    const { query, params } = DatabaseClient.searchQuery<T>(table, spec);
    const { rows } = await this.#query<T>(query, params);
    return rows;
  }

  private static countQuery<T extends { [key: string]: unknown }>(
    table: string,
    where: readonly Predicate<T>[] = []
  ): {
    readonly query: string;
    readonly params: unknown[];
  } {
    const parameters = new Parameters();
    const predicates = where.map((predicate) =>
      applyPredicate(predicate, parameters)
    );

    let query = `SELECT COUNT(*) AS cnt FROM ${table}`;
    if (predicates.length) query += ` WHERE ${predicates.join(" AND ")}`;

    return { query, params: parameters.parameters };
  }

  async count<T extends { [key: string]: unknown }>(
    table: string,
    where: readonly Predicate<T>[] = []
  ): Promise<number> {
    const { query, params } = DatabaseClient.countQuery<T>(table, where);
    const { rows } = await this.#query<{ readonly cnt: number }>(query, params);
    return rows[0]?.cnt ?? 0;
  }

  async save<T extends { [key: string]: unknown }>(
    table: string,
    record: T
  ): Promise<void> {
    const parameters = new Parameters();
    const values = Object.values(record).map((value) =>
      parameters.param(value)
    );

    const columns = Object.keys(record);
    const query = `INSERT INTO ${table} (${columns.join(
      ", "
    )}) VALUES (${values.join(", ")})
      ON CONFLICT (id) DO UPDATE SET ${columns
        .map((col) => `${col} = EXCLUDED.${col}`)
        .join(", ")}`;

    await this.#query(query, parameters.parameters);

    this.emit("saved", record);
  }

  async update<T extends { [key: string]: unknown }>(
    table: string,
    {
      where,
      update,
    }: { readonly where?: readonly Predicate<T>[]; update: Partial<T> }
  ): Promise<void> {
    const parameters = new Parameters();
    const predicates = (where ?? []).map((predicate) =>
      applyPredicate(predicate, parameters)
    );

    let query = `UPDATE ${table} SET ${Object.entries(update)
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([column, value]) => `${column} = ${parameters.param(value)}`)
      .join(", ")}`;

    if (predicates.length) query += ` WHERE ${predicates.join(" AND ")}`;

    await this.#query(query, parameters.parameters);

    this.emit("updated", { where, update });
  }

  async delete<T extends { [key: string]: unknown }>(
    table: string,
    { where }: { readonly where?: readonly Predicate<T>[] }
  ): Promise<void> {
    const parameters = new Parameters();
    const predicates = (where ?? []).map((predicate) =>
      applyPredicate(predicate, parameters)
    );

    let query = `DELETE FROM ${table}`;
    if (predicates.length) query += ` WHERE ${predicates.join(" AND ")}`;

    await this.#query(query, parameters.parameters);

    this.emit("deleted", { where });
  }

  async #query<T>(query: string, params?: unknown[]): Promise<QueryResult<T>> {
    this.#logger?.debug({ query, params }, "SQL query");
    return this.#pool.query(query, params);
  }
}

function applyPredicate<T extends { [key: string]: unknown }>(
  predicate: Predicate<T>,
  parameters: Parameters
): string {
  if ("eq" in predicate) {
    return predicate.eq.value !== null
      ? `${predicate.eq.column} = ${parameters.param(predicate.eq.value)}`
      : `${predicate.eq.column} IS NULL`;
  }

  if ("in" in predicate) {
    const params = predicate.in.values.map((v) => parameters.param(v));
    return `${predicate.in.column} IN (${params.join(", ")})`;
  }

  if ("notIn" in predicate) {
    const params = predicate.notIn.values.map((v) => parameters.param(v));
    return `${predicate.notIn.column} NOT IN (${params.join(", ")})`;
  }

  if ("arrayContains" in predicate) {
    return `${parameters.param(predicate.arrayContains.value)} = ANY(${predicate.arrayContains.column})`;
  }

  if ("textSearch" in predicate) {
    const param = parameters.param(`%${predicate.textSearch.value}%`);
    const conditions = predicate.textSearch.columns.map(
      (column) => `${column} ILIKE ${param}`
    );
    return conditions.length > 1
      ? `(${conditions.join(" OR ")})`
      : conditions[0]!;
  }

  throw new Error(`Unsupported predicate: ${JSON.stringify(predicate)}`);
}

type StubbedQuery<T = unknown> = {
  readonly query: string;
  readonly params?: readonly unknown[];
  readonly result: QueryResult<T>;
};

class PoolStub implements DatabasePool {
  readonly #queries: readonly StubbedQuery[];

  constructor({ queries }: { queries?: readonly StubbedQuery[] } = {}) {
    this.#queries = (queries ?? []).map((q) => ({
      query: normalizeQuery(q.query),
      params: q.params,
      result: q.result,
    }));
  }

  async query<T = unknown>(
    query: string,
    params?: unknown[]
  ): Promise<QueryResult<T>> {
    const normalizedQuery = normalizeQuery(query);

    const stub = this.#queries.find(
      (q) =>
        q.query === normalizedQuery &&
        JSON.stringify(q.params) === JSON.stringify(params)
    );

    if (stub) return stub.result as QueryResult<T>;

    console.warn("No stubbed query found for:", {
      query: normalizedQuery,
      params,
    });

    return { rows: [] };
  }
}

function normalizeQuery(query: string): string {
  return query.replace(/\s+/g, " ").trim();
}

class Parameters {
  readonly parameters: any[] = [];

  param(value: any): string {
    this.parameters.push(value);
    return `$${this.parameters.length}`;
  }
}
