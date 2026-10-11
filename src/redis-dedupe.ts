import type { DedupeStore } from "./types.js";

/**
 * The two Redis operations {@link RedisDedupeStore} needs. Adapt any client (ioredis, node-redis,
 * Upstash, Valkey) in a few lines; see the README for ready-made adapters. k2kit itself has no
 * Redis dependency.
 */
export interface RedisDedupeClient {
  /** `MGET`: the value of each key, `null` (or `undefined`) when missing. */
  mget(keys: string[]): Promise<(string | null | undefined)[]>;
  /** Sets each key to `"1"` with an expiry in seconds, ideally in one pipeline or `MULTI`. */
  setWithTtl(keys: string[], ttlSeconds: number): Promise<void>;
}

/** Options for {@link RedisDedupeStore}. */
export interface RedisDedupeStoreOptions {
  client: RedisDedupeClient;
  /**
   * Separates consumers that must not dedupe each other, usually the subscription name.
   * Two subscriptions both process every record, so they need different namespaces.
   */
  namespace: string;
  /** How long a key is remembered. Default 7 days (K2's default retention). */
  ttlSeconds?: number;
  /** Key prefix. Default `"k2kit:dedupe:"`. */
  prefix?: string;
}

/**
 * {@link DedupeStore} backed by Redis, shared by every process consuming a subscription.
 * A batch costs one `MGET` and one pipelined write.
 */
export class RedisDedupeStore implements DedupeStore {
  readonly #client: RedisDedupeClient;
  readonly #keyPrefix: string;
  readonly #ttlSeconds: number;

  constructor(opts: RedisDedupeStoreOptions) {
    if (!opts?.client) throw new TypeError("client is required");
    if (typeof opts.namespace !== "string" || opts.namespace.length === 0) {
      throw new TypeError("namespace is required, e.g. the subscription name");
    }
    const ttl = opts.ttlSeconds ?? 7 * 24 * 3600;
    if (!Number.isInteger(ttl) || ttl < 1)
      throw new TypeError("ttlSeconds must be a positive integer");
    this.#client = opts.client;
    this.#keyPrefix = `${opts.prefix ?? "k2kit:dedupe:"}${opts.namespace}:`;
    this.#ttlSeconds = ttl;
  }

  async has(key: string): Promise<boolean> {
    const [seen] = await this.hasMany([key]);
    return seen === true;
  }

  async add(key: string): Promise<void> {
    await this.addMany([key]);
  }

  async hasMany(keys: string[]): Promise<boolean[]> {
    if (keys.length === 0) return [];
    const values = await this.#client.mget(keys.map((k) => this.#keyPrefix + k));
    return keys.map((_, i) => values[i] !== null && values[i] !== undefined);
  }

  async addMany(keys: string[]): Promise<void> {
    if (keys.length === 0) return;
    await this.#client.setWithTtl(
      keys.map((k) => this.#keyPrefix + k),
      this.#ttlSeconds,
    );
  }
}
