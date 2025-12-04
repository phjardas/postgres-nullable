import { UserRepository } from "./UserRepository.js";

export type User = {
  id: string;
  name: string;
};

export class UserService {
  readonly #repo: UserRepository;

  constructor({ repo }: { repo: UserRepository }) {
    this.#repo = repo;
  }

  async getUserById(id: string): Promise<User | undefined> {
    return this.#repo.findById(id);
  }
}
