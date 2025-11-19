import { describe, expect, it } from "vitest";
import { UserRepository } from "./user-repo.js";

describe("UserRepository", () => {
  it("should work", async () => {
    const repo = UserRepository.createNull();
    await repo.save({ id: "1", name: "John Doe" });
    await repo.save({ id: "2", name: "Jane Doe" });

    await expect(repo.findById("1")).resolves.toEqual({
      id: "1",
      name: "John Doe",
    });

    await expect(repo.findByName("doe")).resolves.toEqual([
      { id: "1", name: "John Doe" },
      { id: "2", name: "Jane Doe" },
    ]);

    await repo.deleteById("1");

    await expect(repo.findById("1")).resolves.toBeUndefined();
  });
});
