import type { DedupeStore } from "./types.js";

/**
 * In-memory LRU {@link DedupeStore}. Keys are lost on restart, so it only dedupes redeliveries
 * within one process; back it with Redis or a database for cross-process dedupe.
 */
export class MemoryDedupeStore implements DedupeStore {
  readonly #keys = new Map<string, true>();
  readonly #maxSize: number;

  /** @param opts.maxSize Maximum keys kept before the least recently used is evicted. Default 100,000. */
  constructor(opts: { maxSize?: number } = {}) {
    const maxSize = opts.maxSize ?? 100_000;
    if (!Number.isInteger(maxSize) || maxSize < 1) {
      throw new TypeError("maxSize must be a positive integer");
    }
    this.#maxSize = maxSize;
  }

  /** Number of keys currently held. */
  get size(): number {
    return this.#keys.size;
  }

  async has(key: string): Promise<boolean> {
    if (!this.#keys.has(key)) return false;
    this.#keys.delete(key);
    this.#keys.set(key, true);
    return true;
  }

  async hasMany(keys: string[]): Promise<boolean[]> {
    return Promise.all(keys.map((key) => this.has(key)));
  }

  async addMany(keys: string[]): Promise<void> {
    for (const key of keys) await this.add(key);
  }

  async add(key: string): Promise<void> {
    this.#keys.delete(key);
    this.#keys.set(key, true);
    if (this.#keys.size > this.#maxSize) {
      const oldest = this.#keys.keys().next();
      if (!oldest.done) this.#keys.delete(oldest.value);
    }
  }
}
