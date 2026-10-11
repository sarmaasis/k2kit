// Adapters from popular Redis clients to RedisDedupeStore. k2kit has no Redis dependency:
// copy the adapter for the client you already use. Type-checked against each client in CI.
import type { Redis as UpstashRedis } from "@upstash/redis";
import type { Redis as IORedis } from "ioredis";
import { type RedisDedupeClient, RedisDedupeStore } from "k2kit";
import type { RedisClientType } from "redis";

/** ioredis */
export function fromIORedis(redis: IORedis): RedisDedupeClient {
  return {
    mget: (keys) => redis.mget(keys),
    async setWithTtl(keys, ttlSeconds) {
      const pipeline = redis.pipeline();
      for (const key of keys) pipeline.set(key, "1", "EX", ttlSeconds);
      await pipeline.exec();
    },
  };
}

/** node-redis (`redis` package) */
export function fromNodeRedis(client: RedisClientType): RedisDedupeClient {
  return {
    mget: (keys) => client.mGet(keys),
    async setWithTtl(keys, ttlSeconds) {
      const multi = client.multi();
      for (const key of keys)
        multi.set(key, "1", { expiration: { type: "EX", value: ttlSeconds } });
      await multi.exec();
    },
  };
}

/** Upstash Redis (HTTP) */
export function fromUpstash(redis: UpstashRedis): RedisDedupeClient {
  return {
    mget: (keys) => redis.mget<(string | null)[]>(...keys),
    async setWithTtl(keys, ttlSeconds) {
      const pipeline = redis.pipeline();
      for (const key of keys) pipeline.set(key, "1", { ex: ttlSeconds });
      await pipeline.exec();
    },
  };
}

// Usage: one namespace per subscription, shared by every process consuming it.
export function ordersDedupe(redis: IORedis): RedisDedupeStore {
  return new RedisDedupeStore({ client: fromIORedis(redis), namespace: "orders-processor" });
}
