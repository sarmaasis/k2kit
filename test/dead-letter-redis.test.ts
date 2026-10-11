import { describe, expect, it } from "vitest";
import { K2Client } from "../src/client.js";
import { DEAD_LETTER_HEADERS, deadLetterToStream } from "../src/dead-letter.js";
import { decodeRecord, ID_HEADER, toBase64, utf8Encode } from "../src/record.js";
import { type RedisDedupeClient, RedisDedupeStore } from "../src/redis-dedupe.js";
import { FakeK2 } from "./fake-k2.js";

const STREAM = "0123456789abcdef0123456789abcdef";

describe("deadLetterToStream", () => {
  function setup() {
    const dlq = new FakeK2();
    const client = new K2Client({
      streamId: STREAM,
      token: dlq.token,
      endpoint: "https://dlq.k2.test",
      fetch: dlq.fetch,
    });
    return { dlq, send: deadLetterToStream(client) };
  }
  const ctx = { workerId: "w-0", batchId: "b1", signal: new AbortController().signal };

  it("keeps content, headers and k2kit-id, and adds the dlq headers", async () => {
    const { dlq, send } = setup();
    const record = decodeRecord({
      timestamp_ms: 1234,
      content: toBase64(utf8Encode('{"order":1}')),
      headers: { [ID_HEADER]: "id-1", "event-type": "order.created" },
    });
    await send([{ record, error: new Error("cannot parse"), attempt: 5 }], ctx);
    expect(dlq.records).toHaveLength(1);
    const stored = dlq.records[0];
    expect(atob(stored?.content ?? "")).toBe('{"order":1}');
    expect(stored?.headers).toEqual({
      [ID_HEADER]: "id-1",
      "event-type": "order.created",
      [DEAD_LETTER_HEADERS.error]: "Error: cannot parse",
      [DEAD_LETTER_HEADERS.attempts]: "5",
      [DEAD_LETTER_HEADERS.sourceTimestamp]: "1234",
    });
  });

  it("stays within K2's 32-header limit and truncates long errors", async () => {
    const { dlq, send } = setup();
    const headers: { [k: string]: string } = { [ID_HEADER]: "id-2" };
    for (let i = 0; i < 31; i++) headers[`h${i}`] = String(i);
    const record = decodeRecord({ timestamp_ms: 1, content: toBase64(utf8Encode("x")), headers });
    await send([{ record, error: new Error("e".repeat(5_000)), attempt: 9 }], ctx);
    const stored = dlq.records[0]?.headers ?? {};
    expect(Object.keys(stored)).toHaveLength(32);
    expect(stored[ID_HEADER]).toBe("id-2");
    expect(stored[DEAD_LETTER_HEADERS.error]?.length).toBe(1_024);
  });
});

describe("RedisDedupeStore", () => {
  function fakeRedis() {
    const data = new Map<string, { value: string; ttl: number }>();
    const calls: string[] = [];
    const client: RedisDedupeClient = {
      async mget(keys) {
        calls.push(`mget ${keys.length}`);
        return keys.map((k) => data.get(k)?.value ?? null);
      },
      async setWithTtl(keys, ttl) {
        calls.push(`set ${keys.length}`);
        for (const k of keys) data.set(k, { value: "1", ttl });
      },
    };
    return { client, data, calls };
  }

  it("checks and stores a whole batch in one call each, under a namespace", async () => {
    const { client, data, calls } = fakeRedis();
    const store = new RedisDedupeStore({ client, namespace: "orders" });
    expect(await store.hasMany(["a", "b"])).toEqual([false, false]);
    await store.addMany(["a", "b"]);
    expect(await store.hasMany(["a", "b", "c"])).toEqual([true, true, false]);
    expect(await store.has("a")).toBe(true);
    expect(calls).toEqual(["mget 2", "set 2", "mget 3", "mget 1"]);
    expect([...data.keys()]).toEqual(["k2kit:dedupe:orders:a", "k2kit:dedupe:orders:b"]);
    expect(data.get("k2kit:dedupe:orders:a")?.ttl).toBe(7 * 24 * 3600);
  });

  it("keeps namespaces apart and skips empty batches", async () => {
    const { client, calls } = fakeRedis();
    const a = new RedisDedupeStore({ client, namespace: "sub-a", ttlSeconds: 60 });
    const b = new RedisDedupeStore({ client, namespace: "sub-b" });
    await a.add("k");
    expect(await b.has("k")).toBe(false);
    calls.length = 0;
    expect(await a.hasMany([])).toEqual([]);
    await a.addMany([]);
    expect(calls).toEqual([]);
  });

  it("validates options", () => {
    const { client } = fakeRedis();
    expect(() => new RedisDedupeStore({ client, namespace: "" })).toThrow(TypeError);
    expect(() => new RedisDedupeStore({ client, namespace: "x", ttlSeconds: 0 })).toThrow(
      TypeError,
    );
  });
});
