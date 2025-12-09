import { describe, expect, it } from "vitest";
import { UserRepository } from "./UserRepository.js";
import { UserService } from "./UserService.js";

describe("UserService", () => {
  it("getUserById", async () => {
    const repo = UserRepository.createNull({
      findById: { "1": { id: "1", name: "John Doe" } },
    });

    const service = new UserService({ repo });

    await expect(service.getUserById("1")).resolves.toEqual({
      id: "1",
      name: "John Doe",
    });
  });
});
