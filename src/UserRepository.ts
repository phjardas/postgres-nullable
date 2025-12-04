import { DatabaseClient } from "./DatabaseClient.js";
import type { User } from "./UserService.js";

export class UserRepository {
  readonly #sql: DatabaseClient;

  constructor({
    sql = DatabaseClient.create(),
  }: { sql?: DatabaseClient } = {}) {
    this.#sql = sql;
  }

  static createNull({
    users = [],
  }: { users?: readonly User[] } = {}): UserRepository {
    return new UserRepository({
      sql: DatabaseClient.createNull({
        queries: users.flatMap((user) => [
          {
            query: "SELECT * FROM users WHERE id = $1 LIMIT 1",
            parameters: [user.id],
            result: { rows: [user] },
          },
        ]),
      }),
    });
  }

  async findById(id: string): Promise<User | undefined> {
    return this.#sql.findById<User>("users", id);
  }

  async searchByName(name: string): Promise<readonly User[]> {
    // FIXME implement
    throw new Error("Not implemented");
    // return this.#sql<
    //   readonly User[]
    // >`select id, name from ${this.#sql("users")} where name ilike ${`%${name}%`} order by id`;
  }

  async save(user: User): Promise<void> {
    // FIXME implement
    throw new Error("Not implemented");
    // await this.#sql<
    //   readonly User[]
    // >`insert into ${this.#sql("users")} (id, name) values (${user.id}, ${user.name}) on conflict (id) do update set name = ${user.name}`;
  }

  async deleteById(id: string): Promise<void> {
    // FIXME implement
    throw new Error("Not implemented");
    // await this.#sql<
    //   readonly User[]
    // >`delete from ${this.#sql("users")} where id = ${id}`;
  }
}
