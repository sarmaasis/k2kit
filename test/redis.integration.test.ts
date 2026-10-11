// Runs RedisDedupeStore against a real Redis through each documented adapter.
// Skipped unless REDIS_URL is set (Upstash cases also need UPSTASH_REDIS_REST_URL/TOKEN), e.g.:
//   docker run -d -p 127.0.0.1:6390:6379 redis:8-alpine  →  REDIS_URL=redis://127.0.0.1:6390
import { Redis as UpstashRedis } from "@upstash/redis";
import { Redis as IORedis } from "ioredis";
import { createClient, type RedisClientType } from "redis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { fromIORedis, fromNodeRedis, fromUpstash } from "../examples/redis-dedupe.js";
import { Consumer } from "../src/consumer.js";
import { RedisDedupeStore } from "../src/redis-dedupe.js";
import type { Logger } from "../src/types.js";
import { FakeK2 } from "./fake-k2.js";

const redisUrl = process.env.REDIS_URL;
const upstashUrl = process.env.UPSTASH_REDIS_REST_URL;
const upstashToken = process.env.UPSTASH_REDIS_REST_TOKEN;
const quiet: Logger = { debug() {}, info() {}, warn() {}, error: console.error };

describe.skipIf(!redisUrl)("RedisDedupeStore against real Redis", () => {
  let ioredis: IORedis;
  let nodeRedis: RedisClientType;
  const run = `it-${Date.now().toString(36)}`;

  beforeAll(async () => {
    ioredis = new IORedis(redisUrl as string, { lazyConnect: true });
    await ioredis.connect();
    nodeRedis = createClient({ url: redisUrl as string }) as RedisClientType;
    await nodeRedis.connect();
  });

  afterAll(async () => {
    const keys = await ioredis.keys(`k2kit:dedupe:${run}*`);
    if (keys.length > 0) await ioredis.del(keys);
    ioredis.disconnect();
    await nodeRedis.quit();
  });

  const adapters = [
    ["ioredis", () => fromIORedis(ioredis)],
    ["node-redis", () => fromNodeRedis(nodeRedis)],
  ] as const;

  it.each(adapters)("%s: stores a batch with a TTL and reads it back", async (name, adapter) => {
    const store = new RedisDedupeStore({
      client: adapter(),
      namespace: `${run}-${name}`,
      ttlSeconds: 120,
    });
    expect(await store.hasMany(["a", "b", "c"])).toEqual([false, false, false]);
    await store.addMany(["a", "b"]);
    expect(await store.hasMany(["a", "b", "c"])).toEqual([true, true, false]);
    expect(await store.has("b")).toBe(true);
    const ttl = await ioredis.ttl(`k2kit:dedupe:${run}-${name}:a`);
    expect(ttl).toBeGreaterThan(100);
    expect(ttl).toBeLessThanOrEqual(120);
  });

  it("keeps namespaces apart across adapters sharing one Redis", async () => {
    const a = new RedisDedupeStore({ client: fromIORedis(ioredis), namespace: `${run}-ns-a` });
    const b = new RedisDedupeStore({ client: fromNodeRedis(nodeRedis), namespace: `${run}-ns-b` });
    await a.add("same-key");
    expect(await b.has("same-key")).toBe(false);
    const a2 = new RedisDedupeStore({ client: fromNodeRedis(nodeRedis), namespace: `${run}-ns-a` });
    expect(await a2.has("same-key")).toBe(true);
  });

  it.skipIf(!upstashUrl || !upstashToken)("Upstash (REST): stores a batch with a TTL", async () => {
    const upstash = new UpstashRedis({ url: upstashUrl as string, token: upstashToken as string });
    const store = new RedisDedupeStore({
      client: fromUpstash(upstash),
      namespace: `${run}-upstash`,
      ttlSeconds: 120,
    });
    expect(await store.hasMany(["x", "y"])).toEqual([false, false]);
    await store.addMany(["x", "y"]);
    expect(await store.hasMany(["x", "y", "z"])).toEqual([true, true, false]);
    expect(await ioredis.ttl(`k2kit:dedupe:${run}-upstash:x`)).toBeGreaterThan(100);
  });

  it("two pods sharing Redis: pod B skips a record pod A processed but failed to ack", async () => {
    const fake = new FakeK2();
    fake.append({ order: 1001 });
    const subId = fake.addSubscription("orders");
    const shared = () =>
      new RedisDedupeStore({ client: fromIORedis(ioredis), namespace: `${run}-pods` });
    const podOptions = (prefix: string) => ({
      streamId: "0123456789abcdef0123456789abcdef",
      token: fake.token,
      subscription: { id: subId },
      endpoint: "https://fake.k2.test",
      fetch: fake.fetch,
      workerIdPrefix: prefix,
      logger: quiet,
      onError: () => {},
      pollBackoff: { minMs: 10, maxMs: 20 },
      retryBackoff: { minMs: 10, maxMs: 20 },
    });

    // Pod A handles the record, then goes down: its ack fails and it requests nothing more,
    // so the lease stays open until it expires.
    fake.fail("ack", 10213);
    let handledByA = 0;
    let batchId = "";
    const podA = new Consumer({ ...podOptions("pod-a"), dedupe: shared() });
    await podA.run((batch) => {
      handledByA++;
      batchId = batch.id;
      void podA.stop();
    });

    // The lease expires and K2 redelivers the record to pod B, a different process.
    fake.dropLease(batchId);
    let handledByB = 0;
    const podB = new Consumer({ ...podOptions("pod-b"), dedupe: shared() });
    const runB = podB.run(() => {
      handledByB++;
    });
    while (podB.stats().acks < 1) await new Promise((r) => setTimeout(r, 10));
    await podB.stop();
    await runB;

    expect(handledByA).toBe(1);
    expect(handledByB).toBe(0); // skipped thanks to the shared Redis dedupe
    expect(fake.deliveries.get(0)).toBe(2);
    expect(fake.backlog(subId)).toBe(0);
  });
});
