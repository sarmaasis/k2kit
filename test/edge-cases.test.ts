// Regression tests for edge cases found in review; numbers match the list in NOTES.md.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MAX_TIMER_MS, sleep } from "../src/backoff.js";
import { K2Client } from "../src/client.js";
import { Consumer } from "../src/consumer.js";
import { deadLetterToStream } from "../src/dead-letter.js";
import { MemoryDedupeStore } from "../src/dedupe.js";
import type { ConsumerOptions } from "../src/options.js";
import { decodeRecord, fromBase64, ID_HEADER, toBase64, utf8Encode } from "../src/record.js";
import { type RedisDedupeClient, RedisDedupeStore } from "../src/redis-dedupe.js";
import type { Logger } from "../src/types.js";
import { FakeK2 } from "./fake-k2.js";

const STREAM = "0123456789abcdef0123456789abcdef";
const OTHER_STREAM = "fedcba9876543210fedcba9876543210";

function setup(fake = new FakeK2()) {
  const logs: string[] = [];
  const logger: Logger = {
    debug: () => {},
    info: () => {},
    warn: (m) => void logs.push(m),
    error: (m) => void logs.push(m),
  };
  const subId = fake.addSubscription("s");
  const consumer = (opts: Partial<ConsumerOptions> = {}) =>
    new Consumer({
      streamId: STREAM,
      token: fake.token,
      subscription: { id: subId },
      endpoint: "https://fake.k2.test",
      fetch: fake.fetch,
      logger,
      onError: () => {},
      pollBackoff: { minMs: 10, maxMs: 100 },
      retryBackoff: { minMs: 10, maxMs: 100 },
      workerIdPrefix: "w",
      ...opts,
    });
  const client = new K2Client({
    streamId: STREAM,
    token: fake.token,
    endpoint: "https://fake.k2.test",
    fetch: fake.fetch,
  });
  return { fake, logs, subId, consumer, client };
}

async function until(cond: () => boolean, maxMs = 30 * 60_000, stepMs = 50): Promise<void> {
  for (let t = 0; t <= maxMs; t += stepMs) {
    if (cond()) return;
    await vi.advanceTimersByTimeAsync(stepMs);
  }
  throw new Error(`condition not met within ${maxMs} fake ms`);
}

function settle(p: Promise<unknown>) {
  const state: { done: boolean; error?: unknown } = { done: false };
  p.then(
    () => {
      state.done = true;
    },
    (error) => {
      state.done = true;
      state.error = error;
    },
  );
  return state;
}

const count = (fake: FakeK2, endpoint: string) =>
  fake.requests.filter((r) => r.endpoint === endpoint).length;

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
});
afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("1. blank k2kit-id", () => {
  it("falls back to the content hash instead of sharing one key", () => {
    const rec = (n: number, id: string) =>
      decodeRecord({
        timestamp_ms: n,
        content: toBase64(utf8Encode(`order ${n}`)),
        headers: { [ID_HEADER]: id },
      });
    expect(rec(1, "").key()).not.toBe(rec(2, "").key());
    expect(rec(1, "   ").key()).toMatch(/^[0-9a-f]{16}$/);
    expect(rec(1, "abc").key()).toBe("abc");
  });

  it("no longer drops records end to end with dedupe on", async () => {
    const { client, consumer } = setup();
    await client.produce([
      { content: "order 1", headers: { [ID_HEADER]: "" } },
      { content: "order 2", headers: { [ID_HEADER]: "" } },
    ]);
    const seen: string[] = [];
    const c = consumer({ dedupe: new MemoryDedupeStore(), maxRecords: 1 });
    settle(c.run((b) => void seen.push(...b.records.map((r) => r.text()))));
    await until(() => seen.length === 2);
    await c.stop();
    expect(seen).toEqual(["order 1", "order 2"]);
  });
});

describe("2. decoded bytes own their memory", () => {
  it("never exposes a shared pool through .buffer", () => {
    for (const text of ["hi", "x".repeat(100), "y".repeat(5_000)]) {
      const bytes = fromBase64(toBase64(utf8Encode(text)));
      expect(bytes.buffer.byteLength).toBe(bytes.byteLength);
      expect(bytes.byteOffset).toBe(0);
    }
  });
});

describe("3. default worker ids", () => {
  it("differ between two consumers in one process, so a record isn't handled twice at once", async () => {
    const { fake, consumer } = setup();
    fake.append("x");
    let handled = 0;
    const workers = new Set<string>();
    const handler = async (_b: unknown, ctx: { workerId: string }) => {
      workers.add(ctx.workerId);
      handled++;
      await sleep(50);
    };
    const a = consumer({ workerIdPrefix: undefined });
    const b = consumer({ workerIdPrefix: undefined });
    settle(a.run(handler));
    settle(b.run(handler));
    await until(() => count(fake, "ack") === 1);
    await vi.advanceTimersByTimeAsync(1_000);
    await a.stop();
    await b.stop();
    expect(handled).toBe(1);
    const ids = fake.requests
      .filter((r) => r.endpoint === "consume")
      .map((r) => (r.body as { worker_id: string }).worker_id);
    expect(new Set(ids.map((id) => id.replace(/-0$/, ""))).size).toBe(2);
  });
});

describe("4. dead-lettering into the consumed stream", () => {
  it("is rejected at construction; another stream is fine", () => {
    const { consumer } = setup();
    const same = new K2Client({ streamId: STREAM, token: "t" });
    const other = new K2Client({ streamId: OTHER_STREAM, token: "t" });
    expect(() => consumer({ deadLetter: { send: deadLetterToStream(same) } })).toThrow(
      /separate dead-letter stream/,
    );
    expect(() => consumer({ deadLetter: { send: deadLetterToStream(other) } })).not.toThrow();
  });
});

describe("5. stop() waiting on handlers that never finish", () => {
  it("warns when a handler ignores ctx.signal after the timeout", async () => {
    const { fake, consumer, logs } = setup();
    fake.append("x");
    let started = false;
    const c = consumer();
    settle(
      c.run(async () => {
        started = true;
        await new Promise(() => {}); // ignores ctx.signal forever
      }),
    );
    await until(() => started);
    settle(c.stop({ timeoutMs: 1_000 }));
    await vi.advanceTimersByTimeAsync(1_000 + 30_000 + 100);
    expect(logs.some((l) => l.includes("stop() is still waiting for 1 handler"))).toBe(true);
  });

  it("points at awaiting stop() inside a handler", async () => {
    const { fake, consumer, logs } = setup();
    fake.append("x");
    const c = consumer();
    settle(c.run(async () => c.stop({ timeoutMs: 500 })));
    await vi.advanceTimersByTimeAsync(500 + 30_000 + 1_000);
    expect(logs.some((l) => l.includes("awaits consumer.stop() itself"))).toBe(true);
  });
});

describe("6. failed lease extensions", () => {
  it("are retried within seconds, so two failures in a row don't lose the lease", async () => {
    const { fake, consumer } = setup();
    fake.append("slow");
    fake.fail("extend", 10211, 2);
    let finished = false;
    const c = consumer();
    settle(
      c.run(async (_b, ctx) => {
        await sleep(6.5 * 60_000);
        finished = !ctx.signal.aborted;
      }),
    );
    await until(() => count(fake, "ack") === 1, 20 * 60_000, 1_000);
    await c.stop();
    expect(finished).toBe(true);
    expect(fake.deliveries.get(0)).toBe(1); // never redelivered
    expect(c.stats().lateAcks).toBe(0);
  });
});

describe("7. RedisDedupeStore on Redis Cluster", () => {
  // Redis Cluster's key-slot function: CRC16/XMODEM of the hash tag (or whole key) mod 16384.
  function crc16(bytes: Uint8Array): number {
    let crc = 0;
    for (const b of bytes) {
      crc ^= b << 8;
      for (let i = 0; i < 8; i++)
        crc = crc & 0x8000 ? ((crc << 1) ^ 0x1021) & 0xffff : (crc << 1) & 0xffff;
    }
    return crc;
  }
  function slot(key: string): number {
    const open = key.indexOf("{");
    const close = open === -1 ? -1 : key.indexOf("}", open + 1);
    const tag = open !== -1 && close > open + 1 ? key.slice(open + 1, close) : key;
    return crc16(utf8Encode(tag)) % 16384;
  }

  it("puts every key of a namespace in one slot, so MGET works in cluster mode", async () => {
    const keys: string[] = [];
    const client: RedisDedupeClient = {
      mget: async (k) => {
        keys.push(...k);
        return k.map(() => null);
      },
      setWithTtl: async () => {},
    };
    const store = new RedisDedupeStore({ client, namespace: "sub_123" });
    await store.hasMany(Array.from({ length: 50 }, (_, i) => `record-${i}`));
    expect(new Set(keys.map(slot)).size).toBe(1);
    expect(crc16(utf8Encode("123456789"))).toBe(0x31c3); // CRC16/XMODEM check value
  });
});

describe("8. oversized timer values", () => {
  it("are rejected in backoff options", () => {
    const { consumer } = setup();
    expect(() => consumer({ pollBackoff: { maxMs: MAX_TIMER_MS + 1 } })).toThrow(TypeError);
    expect(() => consumer({ retryBackoff: { maxMs: 3e9 } })).toThrow(TypeError);
    expect(() => consumer({ pollBackoff: { maxMs: MAX_TIMER_MS } })).not.toThrow();
  });

  it("make sleep() wait the maximum instead of firing immediately", async () => {
    let done = false;
    void sleep(3e9).then(() => {
      done = true;
    });
    await vi.advanceTimersByTimeAsync(60_000);
    expect(done).toBe(false);
  });

  it("don't make stop() abort handlers immediately", async () => {
    const { fake, consumer } = setup();
    fake.append("x");
    let signal: AbortSignal | undefined;
    const c = consumer();
    settle(
      c.run(async (_b, ctx) => {
        signal = ctx.signal;
        await sleep(1_000);
      }),
    );
    await until(() => signal !== undefined);
    const stopped = settle(c.stop({ timeoutMs: Number.POSITIVE_INFINITY }));
    await vi.advanceTimersByTimeAsync(10);
    expect(signal?.aborted).toBe(false);
    await until(() => stopped.done);
  });
});

describe("9. records that already have 32 headers", () => {
  it("are produced without the automatic k2kit-id instead of being rejected", async () => {
    const { client, fake } = setup();
    const full: { [k: string]: string } = {};
    for (let i = 0; i < 32; i++) full[`h${i}`] = "v";
    const almost: { [k: string]: string } = {};
    for (let i = 0; i < 31; i++) almost[`h${i}`] = "v";
    await client.produce([
      { content: "full", headers: full },
      { content: "room", headers: almost },
    ]);
    expect(Object.keys(fake.records[0]?.headers ?? {})).toHaveLength(32);
    expect(fake.records[0]?.headers?.[ID_HEADER]).toBeUndefined();
    expect(fake.records[1]?.headers?.[ID_HEADER]).toMatch(/^[0-9a-f-]{36}$/);
  });
});

describe("11. duplicates inside one batch", () => {
  it("reach the handler once when dedupe is on", async () => {
    const { client, consumer } = setup();
    await client.produce(
      [
        { content: "a", headers: { [ID_HEADER]: "same" } },
        { content: "a (producer retry)", headers: { [ID_HEADER]: "same" } },
        { content: "b" },
      ],
      { idHeader: true },
    );
    const seen: string[] = [];
    const c = consumer({ dedupe: new MemoryDedupeStore() });
    settle(c.run((b) => void seen.push(...b.records.map((r) => r.text()))));
    await until(() => seen.length >= 2);
    await vi.advanceTimersByTimeAsync(500);
    await c.stop();
    expect(seen).toEqual(["a", "b"]);
  });
});

describe("12. clock skew", () => {
  it("doesn't cut ack retries short when K2's clock is behind ours", async () => {
    const { fake, consumer } = setup();
    fake.append("x");
    // K2's clock an hour behind: leased_until_ms looks long expired to the local clock.
    const skewed: typeof fetch = async (input, init) => {
      const res = await fake.fetch(input, init);
      if (!String(input).endsWith("/consume")) return res;
      const body = await res.json();
      if (body.result?.leased_until_ms) body.result.leased_until_ms -= 3_600_000;
      return new Response(JSON.stringify(body), { status: res.status });
    };
    fake.fail("ack", 10211, 2);
    const c = consumer({ fetch: skewed });
    settle(c.run(() => {}));
    await until(() => c.stats().acks === 1);
    await c.stop();
    expect(count(fake, "ack")).toBe(3);
  });
});

describe("13. acks after the lease expired", () => {
  it("are counted and warned about instead of looking like clean acks", async () => {
    const { fake, consumer, logs } = setup();
    fake.append("x");
    fake.fail("extend", 10211, 100); // the heartbeat can never extend
    const c = consumer();
    // K2 redelivers after the late ack, so a second slow handler starts: let stop() abort it.
    settle(c.run((_b, ctx) => sleep(6 * 60_000, ctx.signal)));
    await until(() => c.stats().lateAcks === 1, 20 * 60_000, 1_000);
    const stopped = settle(c.stop({ timeoutMs: 1_000 }));
    await until(() => stopped.done);
    expect(logs.some((l) => l.includes("lease probably expired before ack"))).toBe(true);
  });
});

describe("15. one dedupe store for two subscriptions", () => {
  it("is refused, since it would silently skip records", async () => {
    const fake = new FakeK2();
    const a = setup(fake);
    const otherSub = fake.addSubscription("other");
    const store = new MemoryDedupeStore();
    const first = a.consumer({ dedupe: store });
    settle(first.run(() => {}));
    await vi.advanceTimersByTimeAsync(100);
    const second = a.consumer({ dedupe: store, subscription: { id: otherSub } });
    const run = settle(second.run(() => {}));
    await until(() => run.done);
    expect(String(run.error)).toMatch(/already used for subscription/);
    await first.stop();
    // The same store for another consumer on the same subscription is fine.
    const third = a.consumer({ dedupe: store });
    const ok = settle(third.run(() => {}));
    await vi.advanceTimersByTimeAsync(100);
    await third.stop();
    expect(ok.error).toBeUndefined();
  });
});

describe("16. produceBatched and network errors", () => {
  it("doesn't retry them, since the batch may already be stored", async () => {
    const { client, fake } = setup();
    fake.failNetwork("produce");
    await expect(client.produceBatched([{ content: "x" }])).rejects.toThrow("fetch failed");
    expect(count(fake, "produce")).toBe(1);
  });
});
