import { DatabaseClient } from "./DatabaseClient.js";
import type { User } from "./UserService.js";

export type Page<T> = {
  readonly items: readonly T[];
  readonly totalCount: number;
};

export type UserRepositoryNull = {
  readonly findById?: { readonly [id: string]: User };
  readonly searchByName?: { readonly [name: string]: Page<User> };
};

export class UserRepository {
  readonly #table: string;
  readonly #sql: DatabaseClient;

  constructor({
    table,
    sql,
  }: {
    readonly table: string;
    readonly sql: DatabaseClient;
  }) {
    this.#table = table;
    this.#sql = sql;
  }

  static create() {
    return new UserRepository({
      table: "users",
      sql: DatabaseClient.create(),
    });
  }

  static createNull({
    findById,
    searchByName,
  }: UserRepositoryNull = {}): UserRepository {
    const table = "users";

    return new UserRepository({
      table,
      sql: DatabaseClient.createNull({
        findById: { [table]: findById ?? {} },
        search: {
          [table]: Object.entries(searchByName ?? {}).map(([name, result]) => ({
            spec: {
              where: [{ textSearch: { columns: ["name"], value: name } }],
              order: [{ column: "id", direction: "asc" }],
            },
            result: result.items,
          })),
        },
        count: {
          [table]: Object.entries(searchByName ?? {}).map(([name, result]) => ({
            where: [{ textSearch: { columns: ["name"], value: name } }],
            result: result.totalCount,
          })),
        },
      }),
    });
  }

  async findById(id: string): Promise<User | undefined> {
    return this.#sql.findById<User>(this.#table, id);
  }

  async searchByName(name: string): Promise<Page<User>> {
    const [items, totalCount] = await Promise.all([
      this.#sql.search<User>(this.#table, {
        where: [{ textSearch: { columns: ["name"], value: name } }],
        order: [{ column: "id", direction: "asc" }],
      }),
      this.#sql.count<User>(this.#table),
    ]);

    return { items, totalCount };
  }

  async save(user: User): Promise<void> {
    await this.#sql.save(this.#table, user);
  }

  async deleteById(id: string): Promise<void> {
    await this.#sql.delete(this.#table, {
      where: [{ eq: { column: "id", value: id } }],
    });
  }
}
