// Regression tests: request timeouts, dead-letter header limits, client validation, logger
// failures, heartbeat timing, consumer lifecycle and malformed ids.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { sleep } from "../src/backoff.js";
import { K2Client, K2TimeoutError } from "../src/client.js";
import { Consumer } from "../src/consumer.js";
import { DEAD_LETTER_HEADERS, deadLetterToStream } from "../src/dead-letter.js";
import type { ConsumerOptions } from "../src/options.js";
import { decodeRecord, ID_HEADER, toBase64, utf8Encode } from "../src/record.js";
import type { Logger } from "../src/types.js";
import { FakeK2 } from "./fake-k2.js";

const STREAM = "0123456789abcdef0123456789abcdef";
const DLQ_STREAM = "fedcba9876543210fedcba9876543210";

function setup(fake = new FakeK2()) {
  const logs: string[] = [];
  const logger: Logger = {
    debug: () => {},
    info: () => {},
    warn: (m) => void logs.push(m),
    error: (m) => void logs.push(m),
  };
  const subId = fake.addSubscription("s");
  const base = {
    streamId: STREAM,
    token: fake.token,
    endpoint: "https://fake.k2.test",
    fetch: fake.fetch,
  };
  const consumer = (opts: Partial<ConsumerOptions> = {}) =>
    new Consumer({
      ...base,
      subscription: { id: subId },
      logger,
      onError: () => {},
      pollBackoff: { minMs: 10, maxMs: 100 },
      retryBackoff: { minMs: 10, maxMs: 100 },
      workerIdPrefix: "w",
      ...opts,
    });
  return { fake, logs, subId, consumer, client: new K2Client(base) };
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

describe("1. requests that never get a response", () => {
  it("time out and are retried, so the record is still processed", async () => {
    const { fake, consumer } = setup();
    fake.append("x");
    fake.hang("consume");
    let handled = 0;
    const c = consumer({ requestTimeoutMs: 1_000 });
    settle(c.run(() => void handled++));
    await until(() => handled === 1, 10_000);
    await c.stop();
    expect(count(fake, "consume")).toBeGreaterThanOrEqual(2);
  });

  it("can't keep stop() waiting longer than the request timeout", async () => {
    const { fake, consumer } = setup();
    fake.hang("consume", 100);
    const c = consumer({ requestTimeoutMs: 2_000 });
    settle(c.run(() => {}));
    await vi.advanceTimersByTimeAsync(10);
    const stopped = settle(c.stop());
    await until(() => stopped.done, 2_100);
  });

  it("time out in the heartbeat too, which retries and keeps the lease", async () => {
    const { fake, consumer } = setup();
    fake.append("slow");
    fake.hang("extend", 2);
    let finished = false;
    const c = consumer();
    settle(
      c.run(async (_b, ctx) => {
        await sleep(6.5 * 60_000, ctx.signal);
        finished = !ctx.signal.aborted;
      }),
    );
    await until(() => count(fake, "ack") === 1, 20 * 60_000, 1_000);
    await c.stop();
    expect(finished).toBe(true);
    expect(fake.deliveries.get(0)).toBe(1);
  });

  it("throw a K2TimeoutError from the client, and produce doesn't retry it", async () => {
    const { fake } = setup();
    fake.hang("produce");
    const client = new K2Client({
      streamId: STREAM,
      token: fake.token,
      endpoint: "https://fake.k2.test",
      fetch: fake.fetch,
      timeoutMs: 500,
    });
    const result = settle(client.produceBatched([{ content: "x" }]));
    await until(() => result.done, 1_000);
    expect(result.error).toBeInstanceOf(K2TimeoutError);
    expect(count(fake, "produce")).toBe(1);
  });
});

describe("2. dead letters for records whose headers are near K2's 64 KiB limit", () => {
  it("are trimmed to fit, with the number of dropped headers recorded", async () => {
    const { fake, client, consumer } = setup();
    const headers: { [k: string]: string } = {};
    for (let i = 0; i < 9; i++) headers[`h${i}`] = "x".repeat(7_270); // 65,448 B: fits, but not with DLQ headers
    await client.produce([{ content: "poison", headers }], { idHeader: false });
    const dlqFake = new FakeK2({ token: fake.token });
    const dlq = new K2Client({
      streamId: DLQ_STREAM,
      token: fake.token,
      endpoint: "https://dlq.k2.test",
      fetch: dlqFake.fetch,
    });
    const c = consumer({ deadLetter: { afterAttempts: 1, send: deadLetterToStream(dlq) } });
    settle(
      c.run(() => {
        throw new Error("poison");
      }),
    );
    await until(() => c.stats().deadLettered === 1, 10_000);
    await c.stop();
    const stored = dlqFake.records[0]?.headers ?? {};
    expect(Number(stored[DEAD_LETTER_HEADERS.droppedHeaders])).toBeGreaterThanOrEqual(1);
    expect(stored[DEAD_LETTER_HEADERS.error]).toBe("Error: poison");
    expect(stored[ID_HEADER]).toMatch(/^[0-9a-f-]{36}$/);
    expect(count(fake, "ack")).toBe(1);
  });

  it("produce skips the automatic id when the headers are already at the byte limit", async () => {
    const { client, fake } = setup();
    const headers: { [k: string]: string } = {};
    for (let i = 0; i < 8; i++) headers[`h${i}`] = "y".repeat(8_190);
    await client.produce([{ content: "x", headers }]); // 65,536 bytes: no room for k2kit-id
    expect(fake.records[0]?.headers?.[ID_HEADER]).toBeUndefined();
  });
});

describe("3 and 15. bad tokens and endpoints", () => {
  it("fail fast with a TypeError instead of being retried forever", () => {
    const opts = { streamId: STREAM };
    expect(() => new K2Client({ ...opts, token: "abc\n" })).toThrow(/trailing newline/);
    expect(() => new K2Client({ ...opts, token: "a b" })).toThrow(TypeError);
    expect(() => new K2Client({ ...opts, token: "t", endpoint: "k2.example.com" })).toThrow(
      /not a valid URL/,
    );
    expect(() => new K2Client({ ...opts, token: "t", endpoint: "http://example.com" })).toThrow(
      /https/,
    );
    expect(
      () => new K2Client({ ...opts, token: "t", endpoint: "http://localhost:8787" }),
    ).not.toThrow();
    expect(
      () => new K2Client({ ...opts, token: "t", endpoint: "http://127.0.0.1:1" }),
    ).not.toThrow();
    const { consumer } = setup();
    expect(() => consumer({ token: "tok\r\n" })).toThrow(TypeError);
  });

  it("never echo the token in the error", () => {
    try {
      new K2Client({ streamId: STREAM, token: "SECRET-TOKEN\n" });
    } catch (err) {
      expect(String(err)).not.toContain("SECRET");
    }
  });
});

describe("6. zero backoff", () => {
  it("is rejected, and every wait yields to the event loop", async () => {
    const { consumer, fake } = setup();
    expect(() => consumer({ pollBackoff: { minMs: 0, maxMs: 0 } })).toThrow(/at least 10 ms/);
    expect(() => consumer({ retryBackoff: { maxMs: 5 } })).toThrow(TypeError);
    vi.spyOn(Math, "random").mockReturnValue(0); // full jitter always picks 0
    const c = consumer({ pollBackoff: { minMs: 0, maxMs: 10 } });
    settle(c.run(() => {}));
    await vi.advanceTimersByTimeAsync(100);
    await c.stop();
    // At most one poll per millisecond, not an unbounded busy loop.
    expect(count(fake, "consume")).toBeLessThanOrEqual(110);
  });
});

describe("7. a logger that throws", () => {
  it("doesn't stop the consumer; it's reported once", async () => {
    const { fake, consumer } = setup();
    fake.append("a", "b");
    const stderr = vi.spyOn(console, "error").mockImplementation(() => {});
    const broken: Logger = {
      debug: () => {
        throw new Error("log transport closed");
      },
      info: () => {
        throw new Error("log transport closed");
      },
      warn: () => {},
      error: () => {},
    };
    let handled = 0;
    const c = consumer({ logger: broken, maxRecords: 1 });
    const run = settle(c.run(() => void handled++));
    await until(() => handled === 2);
    await c.stop();
    expect(run.error).toBeUndefined();
    expect(stderr).toHaveBeenCalledTimes(1);
  });
});

describe("8. heartbeatMs", () => {
  it("must be 1 s to 4 min", () => {
    const { consumer } = setup();
    expect(() => consumer({ heartbeatMs: 1 })).toThrow(TypeError);
    expect(() => consumer({ heartbeatMs: 999 })).toThrow(TypeError);
    expect(() => consumer({ heartbeatMs: 240_001 })).toThrow(TypeError);
    expect(() => consumer({ heartbeatMs: 1_000 })).not.toThrow();
    expect(() => consumer({ heartbeatMs: 240_000 })).not.toThrow();
  });
});

describe("10. a blocked event loop", () => {
  it("is detected by the heartbeat and warned about", async () => {
    const { fake, consumer, logs } = setup();
    fake.append("x");
    let started = false;
    const c = consumer();
    settle(
      c.run(async (_b, ctx) => {
        started = true;
        await sleep(10 * 60_000, ctx.signal);
      }),
    );
    await until(() => started);
    vi.setSystemTime(Date.now() + 90_000); // time passes while timers can't run (blocked loop)
    await vi.advanceTimersByTimeAsync(120_000);
    expect(logs.some((l) => /heartbeat ran \d+ s late/.test(l))).toBe(true);
    const stopped = settle(c.stop({ timeoutMs: 100 }));
    await until(() => stopped.done);
  });
});

describe("13. very long k2kit-id values", () => {
  it("are hashed into a short, stable dedupe key", () => {
    const rec = (id: string) =>
      decodeRecord({
        timestamp_ms: 0,
        content: toBase64(utf8Encode("x")),
        headers: { [ID_HEADER]: id },
      });
    const long = "z".repeat(8_000);
    expect(rec(long).key()).toMatch(/^id-fnv:[0-9a-f]{16}$/);
    expect(rec(long).key()).toBe(rec(long).key());
    expect(rec(`${long}!`).key()).not.toBe(rec(long).key());
    const max = "q".repeat(128);
    expect(rec(max).key()).toBe(max);
  });
});

describe("consumer lifecycle and bad ids", () => {
  it("run() after an early stop() (e.g. SIGTERM during startup) resolves without starting", async () => {
    const { fake, consumer } = setup();
    const c = consumer();
    await c.stop();
    await c.run(() => {});
    expect(fake.requests).toHaveLength(0);
    await expect(c.run(() => {})).rejects.toThrow("only be called once"); // only the first is a no-op
  });

  it("a malformed subscription id gets a clear error instead of K2's vague 10204", async () => {
    const { consumer } = setup();
    const c = consumer({ subscription: { id: "orders-processor" } });
    const run = settle(c.run(() => {}));
    await until(() => run.done);
    expect(String(run.error)).toMatch(/"orders-processor" is not a valid K2 subscription id/);
  });
});
