import postgres, { type Sql } from "postgres";
import { createSqlStub } from "./sql-stub.js";

export type User = {
  id: string;
  name: string;
};

export class UserRepository {
  readonly #sql: Sql;

  constructor({ sql = postgres() }: { sql?: Sql } = {}) {
    this.#sql = sql;
  }

  static createNull(): UserRepository {
    return new UserRepository({
      sql: createSqlStub(async (query) => {
        // FIXME: implement
        console.log("resolving query:", query);
        return [];
      }),
    });
  }

  async findById(id: string): Promise<User | undefined> {
    const rows = await this.#sql<
      readonly User[]
    >`select id, name from ${this.#sql("users")} where id = ${id} limit 1`;
    return rows[0];
  }

  async findByName(name: string): Promise<readonly User[]> {
    return this.#sql<
      readonly User[]
    >`select id, name from ${this.#sql("users")} where name ilike ${`%${name}%`} order by id`;
  }

  async save(user: User): Promise<void> {
    await this.#sql<
      readonly User[]
    >`insert into ${this.#sql("users")} (id, name) values (${user.id}, ${user.name}) on conflict (id) do update set name = ${user.name}`;
  }

  async deleteById(id: string): Promise<void> {
    await this.#sql<
      readonly User[]
    >`delete from ${this.#sql("users")} where id = ${id}`;
  }
}
