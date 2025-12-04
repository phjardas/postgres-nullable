import { describe, expect, it } from "vitest";
import { DatabaseClient } from "./DatabaseClient.js";
import { UserRepository } from "./UserRepository.js";
import type { User } from "./UserService.js";

describe("UserRepository", () => {
  it("findById", async () => {
    const user: User = { id: "1", name: "John Doe" };

    const sql = DatabaseClient.createNull({
      queries: [
        {
          query: "SELECT * FROM users WHERE id = $1 LIMIT 1",
          params: [user.id],
          result: { rows: [user] },
        },
      ],
    });

    const repo = new UserRepository({ sql });

    await expect(repo.findById("1")).resolves.toEqual(user);
  });
});
