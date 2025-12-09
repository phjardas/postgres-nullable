import { describe, expect, it } from "vitest";
import { DatabaseClient } from "./DatabaseClient.js";
import { UserRepository } from "./UserRepository.js";
import type { User } from "./UserService.js";

describe("UserRepository", () => {
  it("findById", async () => {
    const user: User = { id: "1", name: "John Doe" };

    const repo = new UserRepository({
      table: "users",
      sql: DatabaseClient.createNull({
        findById: { users: { [user.id]: user } },
      }),
    });

    await expect(repo.findById("1")).resolves.toEqual(user);
  });
});
