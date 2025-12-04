import { Pool } from "pg";
import { GenericContainer, type StartedTestContainer } from "testcontainers";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { DatabaseClient, type SearchSpec } from "./DatabaseClient.js";

describe("DatabaseClient", () => {
  describe("actual", () => {
    let postgres: StartedTestContainer;
    let pool: Pool;

    type UserRecord = {
      id: string;
      name: string | null;
      email: string | null;
      roles: readonly string[] | null;
    };

    beforeAll(async () => {
      postgres = await new GenericContainer("postgres:17.4-alpine")
        .withEnvironment({
          POSTGRES_DB: "smartrepair",
          POSTGRES_USER: "smartrepair",
          POSTGRES_PASSWORD: "password",
        })
        .withExposedPorts(5432)
        .start();

      pool = new Pool({
        host: postgres.getHost(),
        port: postgres.getMappedPort(5432),
        database: "smartrepair",
        user: "smartrepair",
        password: "password",
      });

      await pool.query(
        "CREATE TABLE users (id VARCHAR(100) NOT NULL PRIMARY KEY, name TEXT, email TEXT, roles VARCHAR(100)[]);"
      );
    });

    afterAll(async () => {
      await pool?.end();
      await postgres?.stop();
    });

    beforeEach(async () => {
      await pool.query("TRUNCATE TABLE users;");
    });

    it("should return undefined for non-existent record", async () => {
      const client = await DatabaseClient.create({ pool });
      const user = await client.findById<UserRecord>("users", "unknown");
      expect(user).toBeUndefined;
    });

    it("should return data for existent record", async () => {
      await pool.query("INSERT INTO users (id, name) VALUES ($1, $2)", [
        "1",
        "Alice",
      ]);

      const client = await DatabaseClient.create({ pool });
      const user = await client.findById<UserRecord>("users", "1");
      expect(user).toEqual({
        id: "1",
        name: "Alice",
        email: null,
        roles: null,
      });
    });

    it("should insert a new record", async () => {
      const client = await DatabaseClient.create({ pool });

      await client.save("users", {
        id: "1",
        name: "Alice",
        email: "email",
        roles: ["admin", "user"],
      });

      const user = await client.findById<UserRecord>("users", "1");

      expect(user).toEqual({
        id: "1",
        name: "Alice",
        email: "email",
        roles: ["admin", "user"],
      });
    });

    it("should update an existing record", async () => {
      await pool.query(
        "INSERT INTO users (id, name, email, roles) VALUES ($1, $2, $3, $4)",
        ["1", "Alice", "email", ["admin", "user"]]
      );

      const client = await DatabaseClient.create({ pool });

      await client.save<UserRecord>("users", {
        id: "1",
        name: "Alice #2",
        email: "email #2",
        roles: ["new"],
      });

      const user = await client.findById<UserRecord>("users", "1");

      expect(user).toEqual({
        id: "1",
        name: "Alice #2",
        email: "email #2",
        roles: ["new"],
      });
    });

    describe("search", () => {
      beforeEach(async () => {
        await pool.query(
          "INSERT INTO users (id, name, email, roles) VALUES ($1, $2, $3, $4)",
          ["1", "Jane Doe", "jane.doe@example.com", ["admin", "user"]]
        );
        await pool.query(
          "INSERT INTO users (id, name, email, roles) VALUES ($1, $2, $3, $4)",
          ["2", "John Doe", "john.doe@example.com", ["user"]]
        );
      });

      const search = async (spec: SearchSpec<UserRecord>) => {
        const client = await DatabaseClient.create({ pool });
        const results = await client.search<UserRecord>("users", spec);
        return results.map((r) => r.id);
      };

      it("where: eq", () =>
        expect(
          search({
            where: [{ eq: { column: "name", value: "Jane Doe" } }],
            order: [{ column: "id", direction: "asc" }],
          })
        ).resolves.toEqual(["1"]));

      it("where: in", () =>
        expect(
          search({
            where: [{ in: { column: "id", values: ["2"] } }],
            order: [{ column: "id", direction: "asc" }],
          })
        ).resolves.toEqual(["2"]));

      it("where: notIn", () =>
        expect(
          search({
            where: [{ notIn: { column: "id", values: ["2"] } }],
            order: [{ column: "id", direction: "asc" }],
          })
        ).resolves.toEqual(["1"]));

      it("where: arrayContains", () =>
        expect(
          search({
            where: [{ arrayContains: { column: "roles", value: "admin" } }],
            order: [{ column: "id", direction: "asc" }],
          })
        ).resolves.toEqual(["1"]));

      it("where: textSearch", () =>
        expect(
          search({
            where: [
              { textSearch: { columns: ["name", "email"], value: "doe" } },
            ],
            order: [{ column: "id", direction: "asc" }],
          })
        ).resolves.toEqual(["1", "2"]));
    });
  });

  describe("null", () => {
    describe("findById", () => {
      it("should return undefined for an unstubbed query", async () => {
        const client = DatabaseClient.createNull();
        const result = await client.findById("table", "id");
        expect(result).toBeUndefined();
      });

      it("should return the expected result for a stubbed query", async () => {
        const client = DatabaseClient.createNull({
          queries: [
            {
              query: "SELECT * FROM table WHERE id = $1 LIMIT 1",
              params: ["id"],
              result: { rows: [{ id: "id", value: "test" }] },
            },
          ],
        });
        const result = await client.findById("table", "id");
        expect(result).toEqual({ id: "id", value: "test" });
      });
    });

    describe("search", () => {
      it("should return an empty array for an unstubbed query", async () => {
        const client = DatabaseClient.createNull();
        const result = await client.search("table");
        expect(result).toEqual([]);
      });

      it("should return the expected result for a stubbed query", async () => {
        const client = DatabaseClient.createNull({
          queries: [
            {
              query:
                "SELECT * FROM table WHERE id = $1 ORDER BY id ASC OFFSET $2 LIMIT $3",
              params: ["id", 0, 1],
              result: { rows: [{ id: "id", value: "test" }] },
            },
          ],
        });

        const result = await client.search("table", {
          where: [{ eq: { column: "id", value: "id" } }],
          order: [{ column: "id", direction: "asc" }],
          offset: 0,
          limit: 1,
        });

        expect(result).toEqual([{ id: "id", value: "test" }]);
      });

      it("should support 'in' predicate", async () => {
        const client = DatabaseClient.createNull({
          queries: [
            {
              query: "SELECT * FROM table WHERE status IN ($1, $2, $3)",
              params: ["active", "pending", "completed"],
              result: { rows: [{ id: "1", status: "active" }] },
            },
          ],
        });

        const result = await client.search("table", {
          where: [
            {
              in: {
                column: "status",
                values: ["active", "pending", "completed"],
              },
            },
          ],
        });

        expect(result).toEqual([{ id: "1", status: "active" }]);
      });

      it("should support 'notIn' predicate", async () => {
        const client = DatabaseClient.createNull({
          queries: [
            {
              query: "SELECT * FROM table WHERE status NOT IN ($1, $2)",
              params: ["deleted", "archived"],
              result: { rows: [{ id: "1", status: "active" }] },
            },
          ],
        });

        const result = await client.search("table", {
          where: [
            { notIn: { column: "status", values: ["deleted", "archived"] } },
          ],
        });

        expect(result).toEqual([{ id: "1", status: "active" }]);
      });

      it("should support 'arrayContains' predicate", async () => {
        const client = DatabaseClient.createNull({
          queries: [
            {
              query: "SELECT * FROM table WHERE $1 = ANY(tags)",
              params: ["important"],
              result: { rows: [{ id: "1", tags: ["important", "urgent"] }] },
            },
          ],
        });

        const result = await client.search("table", {
          where: [{ arrayContains: { column: "tags", value: "important" } }],
        });

        expect(result).toEqual([{ id: "1", tags: ["important", "urgent"] }]);
      });

      it("should support multiple predicates combined", async () => {
        const client = DatabaseClient.createNull({
          queries: [
            {
              query:
                "SELECT * FROM table WHERE status = $1 AND category IN ($2, $3) AND $4 = ANY(tags)",
              params: ["active", "news", "blog", "featured"],
              result: {
                rows: [
                  {
                    id: "1",
                    status: "active",
                    category: "news",
                    tags: ["featured"],
                  },
                ],
              },
            },
          ],
        });

        const result = await client.search("table", {
          where: [
            { eq: { column: "status", value: "active" } },
            { in: { column: "category", values: ["news", "blog"] } },
            { arrayContains: { column: "tags", value: "featured" } },
          ],
        });

        expect(result).toEqual([
          { id: "1", status: "active", category: "news", tags: ["featured"] },
        ]);
      });

      it("should support 'textSearch' predicate across multiple columns", async () => {
        const client = DatabaseClient.createNull({
          queries: [
            {
              query:
                "SELECT * FROM table WHERE (name ILIKE $1 OR description ILIKE $1)",
              params: ["%smith%"],
              result: {
                rows: [
                  { id: "1", name: "John Smith", description: "Developer" },
                ],
              },
            },
          ],
        });

        const result = await client.search("table", {
          where: [
            {
              textSearch: {
                columns: ["name", "description"],
                value: "smith",
              },
            },
          ],
        });

        expect(result).toEqual([
          { id: "1", name: "John Smith", description: "Developer" },
        ]);
      });

      it("should support 'textSearch' predicate with single column", async () => {
        const client = DatabaseClient.createNull({
          queries: [
            {
              query: "SELECT * FROM table WHERE email ILIKE $1",
              params: ["%@example.com%"],
              result: { rows: [{ id: "1", email: "user@example.com" }] },
            },
          ],
        });

        const result = await client.search("table", {
          where: [
            {
              textSearch: {
                columns: ["email"],
                value: "@example.com",
              },
            },
          ],
        });

        expect(result).toEqual([{ id: "1", email: "user@example.com" }]);
      });

      it("should combine 'textSearch' with other predicates", async () => {
        const client = DatabaseClient.createNull({
          queries: [
            {
              query:
                "SELECT * FROM table WHERE status = $1 AND (name ILIKE $2 OR email ILIKE $2)",
              params: ["active", "%john%"],
              result: {
                rows: [
                  {
                    id: "1",
                    name: "John Doe",
                    email: "john@test.com",
                    status: "active",
                  },
                ],
              },
            },
          ],
        });

        const result = await client.search("table", {
          where: [
            { eq: { column: "status", value: "active" } },
            {
              textSearch: {
                columns: ["name", "email"],
                value: "john",
              },
            },
          ],
        });

        expect(result).toEqual([
          {
            id: "1",
            name: "John Doe",
            email: "john@test.com",
            status: "active",
          },
        ]);
      });
    });
  });
});
