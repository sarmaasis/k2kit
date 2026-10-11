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
      // ioredis resolves a pipeline even when commands fail, returning [error, result] pairs.
      // Throw, so k2kit reports the failure instead of dedupe silently not recording keys.
      const results = await pipeline.exec();
      const failed = results?.find(([err]) => err);
      if (failed) throw failed[0];
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

// Usage: one namespace per subscription, shared by every process consuming it. Use the
// subscription id (from `client.ensureSubscription(name, startAt)`), not its name: a subscription
// deleted and recreated under the same name gets a new id, so old keys can't make it skip records.
export function ordersDedupe(redis: IORedis, subscriptionId: string): RedisDedupeStore {
  return new RedisDedupeStore({ client: fromIORedis(redis), namespace: subscriptionId });
}
