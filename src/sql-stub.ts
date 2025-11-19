import { type Sql } from "postgres";

export type QueryResolver = (query: string) => Promise<unknown>;

export function createSqlStub(resolver: QueryResolver): Sql {
  const sql = (first: any, ...rest: readonly any[]) => {
    if (
      Array.isArray(first) &&
      "raw" in first &&
      Array.isArray(first.raw) &&
      Array.isArray(rest)
    ) {
      // This is a tagged template literal call
      return new PendingQuery(first, rest, resolver);
    }

    // This is a helper call
    return JSON.stringify(first);
  };

  sql.begin = async () => {
    // FIXME
    throw new Error("Not implemented yet");
  };

  sql.end = async () => {
    // ignored
  };

  return sql as unknown as Sql;
}

class PendingQuery<T> implements PromiseLike<T> {
  readonly #template: readonly string[];
  readonly #parameters: readonly unknown[];
  readonly #resolver: QueryResolver;

  constructor(
    template: readonly string[],
    parameters: readonly unknown[],
    resolver: QueryResolver
  ) {
    this.#template = template;
    this.#parameters = parameters;
    this.#resolver = resolver;
  }

  async then<TResult1 = T, TResult2 = never>(
    onfulfilled: (value: T) => TResult1 | PromiseLike<TResult1>
  ): Promise<TResult1 | TResult2> {
    const query = this.toString();
    return await onfulfilled(this.#resolver(query) as T);
  }

  toString(): string {
    const parts: string[] = [];

    for (let i = 0; i < this.#template.length; i++) {
      parts.push(this.#template[i]!);
      if (i < this.#parameters.length) parts.push(String(this.#parameters[i]));
    }

    return parts.join("");
  }
}
