import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { sleep } from "../src/backoff.js";
import { Consumer } from "../src/consumer.js";
import type { DeadLetter, DeadLetterOptions } from "../src/dead-letter.js";
import { MemoryDedupeStore } from "../src/dedupe.js";
import { K2Error } from "../src/errors.js";
import type { ConsumerOptions } from "../src/options.js";
import type { Batch, HandlerContext, Logger } from "../src/types.js";
import { FakeK2, LEASE_MS } from "./fake-k2.js";

const STREAM = "0123456789abcdef0123456789abcdef";

interface Harness {
  fake: FakeK2;
  logs: string[];
  logger: Logger;
  consumer(opts?: Partial<ConsumerOptions>): Consumer;
}

function harness(fake = new FakeK2()): Harness {
  const logs: string[] = [];
  const logger: Logger = {
    debug: (m) => logs.push(`debug ${m}`),
    info: (m) => logs.push(`info ${m}`),
    warn: (m) => logs.push(`warn ${m}`),
    error: (m) => logs.push(`error ${m}`),
  };
  return {
    fake,
    logs,
    logger,
    consumer: (opts = {}) =>
      new Consumer({
        streamId: STREAM,
        token: fake.token,
        subscription: { name: "test-sub", startAt: "earliest" },
        workerIdPrefix: "w",
        endpoint: "https://fake.k2.test",
        fetch: fake.fetch,
        logger,
        pollBackoff: { minMs: 10, maxMs: 100 },
        retryBackoff: { minMs: 10, maxMs: 100 },
        ...opts,
      }),
  };
}

/** Advances fake time in small steps until `cond` holds. */
async function until(cond: () => boolean, maxMs = 60 * 60_000, stepMs = 50): Promise<void> {
  for (let t = 0; t <= maxMs; t += stepMs) {
    if (cond()) return;
    await vi.advanceTimersByTimeAsync(stepMs);
  }
  throw new Error(`condition not met within ${maxMs} fake ms`);
}

/** Collects a promise's outcome without leaving it unhandled. */
function settle<T>(p: Promise<T>) {
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

function subId(fake: FakeK2): string {
  const req = fake.requests.find((r) => r.endpoint === "consume");
  return req?.path.split("/")[2] ?? "";
}

function count(fake: FakeK2, endpoint: string): number {
  return fake.requests.filter((r) => r.endpoint === endpoint).length;
}

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("Consumer", () => {
  it("acks when the handler succeeds", async () => {
    const h = harness();
    h.fake.append({ n: 1 }, { n: 2 }, { n: 3 });
    const seen: number[] = [];
    const acks: string[] = [];
    const c = h.consumer().on("ack", (e) => acks.push(e.batchId));
    const run = settle(
      c.run((batch) => {
        for (const r of batch.records) seen.push(r.json<{ n: number }>().n);
      }),
    );
    await until(() => seen.length === 3 && acks.length === 1);
    await c.stop();
    expect(run).toEqual({ done: true });
    expect(seen).toEqual([1, 2, 3]);
    expect(h.fake.backlog(subId(h.fake))).toBe(0);
    expect(count(h.fake, "nack")).toBe(0);
  });

  it("nacks when the handler throws and the same records come back in a new batch", async () => {
    const h = harness();
    h.fake.append("a", "b");
    const batches: { id: string; texts: string[] }[] = [];
    const nacks: string[] = [];
    const errors: unknown[] = [];
    const onError = vi.fn();
    const c = h
      .consumer({ onError })
      .on("nack", (e) => nacks.push(e.batchId))
      .on("error", (err) => errors.push(err));
    const run = settle(
      c.run((batch) => {
        batches.push({ id: batch.id, texts: batch.records.map((r) => r.text()) });
        if (batches.length === 1) throw new Error("first attempt fails");
      }),
    );
    await until(() => batches.length === 2 && count(h.fake, "ack") === 1);
    await c.stop();
    expect(run.done).toBe(true);
    expect(batches[0]?.texts).toEqual(["a", "b"]);
    expect(batches[1]?.texts).toEqual(["a", "b"]);
    expect(batches[1]?.id).not.toBe(batches[0]?.id);
    expect(nacks).toEqual([batches[0]?.id]);
    expect(errors).toHaveLength(1);
    expect(onError).toHaveBeenCalledWith(
      expect.objectContaining({ message: "first attempt fails" }),
      expect.objectContaining({ phase: "handler", batchId: batches[0]?.id, records: 2 }),
    );
  });

  it("backs off on empty polls and resets after a non-empty batch", async () => {
    vi.spyOn(Math, "random").mockReturnValue(0.999);
    const h = harness();
    const times: number[] = [];
    const fetch: typeof globalThis.fetch = (input, init) => {
      if (String(input).endsWith("/consume")) times.push(Date.now());
      return h.fake.fetch(input, init);
    };
    let idle = 0;
    const c = h
      .consumer({ fetch, pollBackoff: { minMs: 100, maxMs: 1_000_000 } })
      .on("idle", () => idle++);
    const handled: number[] = [];
    const run = settle(c.run(() => void handled.push(Date.now())));

    await until(() => times.length >= 5);
    const gaps = times.slice(1, 5).map((t, i) => t - (times[i] as number));
    // Full jitter at random≈1: ~100, ~200, ~400, ~800 (+ ≤ 50 ms of test step granularity).
    for (let i = 1; i < gaps.length; i++) {
      expect(gaps[i]).toBeGreaterThan((gaps[i - 1] as number) * 1.5);
    }
    expect(idle).toBeGreaterThanOrEqual(4);

    h.fake.append("x");
    await until(() => handled.length === 1);
    const handledAt = handled[0] as number;
    await until(() => times.filter((t) => t >= handledAt).length >= 4);
    // After the batch: an immediate poll (empty), then gaps restart at ~100, ~200.
    const after = times.filter((t) => t >= handledAt);
    const gapsAfter = after
      .slice(1)
      .map((t, i) => t - (after[i] as number))
      .filter((g) => g > 0);
    expect(gapsAfter[0]).toBeLessThan(160);
    expect(gapsAfter[1]).toBeGreaterThan(160);
    await c.stop();
    expect(run.done).toBe(true);
  });

  it.each([10211, 10214, 10216, 10217])("retries retryable consume error %i", async (code) => {
    const h = harness();
    h.fake.append("r");
    h.fake.fail("consume", code, 3);
    let handled = 0;
    const c = h.consumer();
    const run = settle(c.run(() => void handled++));
    await until(() => handled === 1);
    await c.stop();
    expect(run).toEqual({ done: true });
    expect(count(h.fake, "consume")).toBeGreaterThanOrEqual(4);
  });

  it("treats network errors as retryable", async () => {
    const h = harness();
    h.fake.append("r");
    h.fake.failNetwork("consume", 2);
    let handled = 0;
    const c = h.consumer();
    const run = settle(c.run(() => void handled++));
    await until(() => handled === 1);
    await c.stop();
    expect(run).toEqual({ done: true });
  });

  it("rejects run() on a non-retryable error and stops all slots", async () => {
    const h = harness();
    h.fake.fail("consume", 10210, 1);
    const c = h.consumer({ concurrency: 3 });
    const run = settle(c.run(() => {}));
    await until(() => run.done);
    expect(run.error).toBeInstanceOf(K2Error);
    expect((run.error as K2Error).code).toBe(10210);
    const consumes = count(h.fake, "consume");
    await vi.advanceTimersByTimeAsync(60_000);
    expect(count(h.fake, "consume")).toBe(consumes);
  });

  it("rejects run() after retryBackoff.maxAttempts consecutive retryable failures", async () => {
    const h = harness();
    h.fake.fail("consume", 10211, 10);
    const c = h.consumer({ retryBackoff: { minMs: 10, maxMs: 10, maxAttempts: 3 } });
    const run = settle(c.run(() => {}));
    await until(() => run.done);
    expect((run.error as K2Error).code).toBe(10211);
    expect(count(h.fake, "consume")).toBe(4);
  });

  it("fails fast with a clear message when the subscription exists with a different start_at", async () => {
    const h = harness();
    h.fake.addSubscription("test-sub", "latest");
    const run = settle(h.consumer().run(() => {}));
    await until(() => run.done);
    expect((run.error as K2Error).code).toBe(10201);
    expect((run.error as K2Error).message).toContain("different start_at");
  });

  it("uses an existing subscription by id", async () => {
    const h = harness();
    const id = h.fake.addSubscription("existing", "earliest");
    h.fake.append("x");
    let handled = 0;
    const c = h.consumer({ subscription: { id } });
    settle(c.run(() => void handled++));
    await until(() => handled === 1);
    await c.stop();
    expect(count(h.fake, "createSubscription")).toBe(0);
  });

  it("keeps the lease of a long handler (6+ minutes) through heartbeats and acks once", async () => {
    const h = harness();
    h.fake.append("slow");
    let handled = 0;
    const c = h.consumer({ concurrency: 2 });
    const run = settle(
      c.run(async (_batch, ctx) => {
        handled++;
        await sleep(6.5 * 60_000);
        expect(ctx.signal.aborted).toBe(false);
      }),
    );
    await until(() => count(h.fake, "ack") === 1, 20 * 60_000, 1000);
    await vi.advanceTimersByTimeAsync(10 * 60_000);
    await c.stop();
    expect(run).toEqual({ done: true });
    expect(handled).toBe(1);
    expect(h.fake.deliveries.get(0)).toBe(1);
    expect(count(h.fake, "extend")).toBe(3);
    expect(count(h.fake, "ack")).toBe(1);
  });

  it("aborts ctx.signal on 10218, skips ack and nack, and keeps consuming", async () => {
    const h = harness();
    h.fake.append("lost");
    const contexts: { batch: Batch; ctx: HandlerContext; abortedAt?: number }[] = [];
    const c = h.consumer();
    settle(
      c.run(async (batch, ctx) => {
        const entry: (typeof contexts)[number] = { batch, ctx };
        contexts.push(entry);
        if (contexts.length === 1) {
          h.fake.dropLease(batch.id); // the next heartbeat extend gets a real 10218
          await new Promise<void>((resolve) =>
            ctx.signal.addEventListener("abort", () => resolve(), { once: true }),
          );
          entry.abortedAt = Date.now();
        }
      }),
    );
    await until(() => contexts.length === 2, 20 * 60_000, 1000);
    const [first, second] = contexts as [(typeof contexts)[number], (typeof contexts)[number]];
    expect(first.ctx.signal.aborted).toBe(true);
    expect((first.ctx.signal.reason as Error).name).toBe("LeaseLostError");
    expect(second.batch.id).not.toBe(first.batch.id);
    expect(second.ctx.attempt).toBe(2);
    const touchedFirst = h.fake.requests.filter(
      (r) => (r.endpoint === "ack" || r.endpoint === "nack") && r.path.includes(first.batch.id),
    );
    expect(touchedFirst).toEqual([]);
    expect(h.logs.some((l) => l.includes("lease lost; K2 will redeliver"))).toBe(true);
    await c.stop();
  });

  it("concurrency 4 uses 4 distinct worker_ids and never handles a record twice at once", async () => {
    const h = harness();
    for (let i = 0; i < 40; i++) h.fake.append({ i });
    const active = new Set<string>();
    let overlap = false;
    let handled = 0;
    const c = h.consumer({ concurrency: 4, maxRecords: 3 });
    settle(
      c.run(async (batch) => {
        const keys = batch.records.map((r) => r.key());
        for (const k of keys) {
          if (active.has(k)) overlap = true;
          active.add(k);
        }
        await sleep(200 + (handled % 3) * 100);
        for (const k of keys) active.delete(k);
        handled += keys.length;
      }),
    );
    await until(() => handled >= 40);
    await c.stop();
    const workers = new Set(
      h.fake.requests
        .filter((r) => r.endpoint === "consume")
        .map((r) => (r.body as { worker_id: string }).worker_id),
    );
    expect([...workers].sort()).toEqual(["w-0", "w-1", "w-2", "w-3"]);
    expect(overlap).toBe(false);
    expect(h.fake.backlog(subId(h.fake))).toBe(0);
  });

  it("dedupe skips a redelivered record after a failed ack", async () => {
    const h = harness();
    h.fake.append("once");
    h.fake.fail("ack", 10213); // non-retryable: the ack is given up, lease expires, K2 redelivers
    let handled = 0;
    const c = h.consumer({ dedupe: new MemoryDedupeStore() });
    settle(c.run(() => void handled++));
    // The failed ack leaves the lease held, so K2 hands the same records back to this worker.
    await until(() => count(h.fake, "ack") === 2, LEASE_MS * 3, 1000);
    await c.stop();
    expect(count(h.fake, "consume")).toBeGreaterThanOrEqual(2);
    expect(handled).toBe(1);
    expect(h.fake.backlog(subId(h.fake))).toBe(0);
  });

  it("stop() waits for in-flight handlers and acks them", async () => {
    const h = harness();
    h.fake.append("a");
    let finished = false;
    const c = h.consumer();
    const run = settle(
      c.run(async () => {
        await sleep(5_000);
        finished = true;
      }),
    );
    await until(() => count(h.fake, "consume") === 1);
    await vi.advanceTimersByTimeAsync(100);
    const stop = settle(c.stop());
    await vi.advanceTimersByTimeAsync(4_000);
    expect(stop.done).toBe(false);
    await until(() => stop.done);
    expect(finished).toBe(true);
    expect(run).toEqual({ done: true });
    expect(count(h.fake, "ack")).toBe(1);
    expect(count(h.fake, "consume")).toBe(1);
  });

  it("stop({ timeoutMs }) aborts ctx.signal and nacks unfinished batches", async () => {
    const h = harness();
    h.fake.append("a");
    let signal: AbortSignal | undefined;
    const c = h.consumer();
    const run = settle(
      c.run(async (_batch, ctx) => {
        signal = ctx.signal;
        await sleep(60_000, ctx.signal);
      }),
    );
    await until(() => signal !== undefined);
    const stop = settle(c.stop({ timeoutMs: 1_000 }));
    await until(() => stop.done, 5_000);
    expect(signal?.aborted).toBe(true);
    expect(run).toEqual({ done: true });
    expect(count(h.fake, "nack")).toBe(1);
    expect(count(h.fake, "ack")).toBe(0);
    expect(h.fake.backlog(subId(h.fake))).toBe(1);
  });

  it("stop() before run() resolves immediately", async () => {
    await harness().consumer().stop();
  });

  it("warns about possible poison batches from attempt 5", async () => {
    const h = harness();
    h.fake.append("poison");
    const attempts: number[] = [];
    const c = h.consumer({ onError: () => {} });
    settle(
      c.run((_b, ctx) => {
        attempts.push(ctx.attempt);
        throw new Error("always fails");
      }),
    );
    await until(() => attempts.length >= 6);
    await c.stop();
    expect(attempts.slice(0, 6)).toEqual([1, 2, 3, 4, 5, 6]);
    expect(h.logs.filter((l) => l.includes("possible poison batch")).length).toBeGreaterThanOrEqual(
      2,
    );
  });

  it("never logs the token", async () => {
    const token = "tok_SUPER_SECRET_value_123";
    const h = harness(new FakeK2({ token }));
    h.fake.append("a", "b");
    h.fake.fail("consume", 10211, 2);
    h.fake.fail("extend", 10214);
    h.fake.fail("ack", 10211);
    let calls = 0;
    const c = h.consumer({ heartbeatMs: 1_000 });
    settle(
      c.run(async () => {
        calls++;
        await sleep(2_500);
        if (calls === 1) throw new Error("handler failure");
      }),
    );
    await until(() => count(h.fake, "ack") >= 2);
    await c.stop();
    expect(h.logs.length).toBeGreaterThan(5);
    for (const line of h.logs) expect(line).not.toContain(token);
  });

  it("logs one line per event with worker_id, batch_id and record count", async () => {
    const h = harness();
    h.fake.append("a", "b");
    const c = h.consumer();
    settle(c.run(() => {}));
    await until(() => count(h.fake, "ack") === 1);
    await c.stop();
    const ack = h.logs.find((l) => l.startsWith("debug k2kit: ack "));
    expect(ack).toMatch(/worker_id=w-0 batch_id=\S+ records=2$/);
  });

  it("validates options", () => {
    const h = harness();
    expect(() => h.consumer({ streamId: "ABC" })).toThrow(TypeError);
    expect(() => h.consumer({ streamId: STREAM.toUpperCase() })).toThrow(TypeError);
    expect(() => h.consumer({ concurrency: 0 })).toThrow(TypeError);
    expect(() => h.consumer({ concurrency: 129 })).toThrow(TypeError);
    expect(() => h.consumer({ maxRecords: 10_001 })).toThrow(TypeError);
    expect(() => h.consumer({ maxRecords: 1.5 })).toThrow(TypeError);
    expect(() => h.consumer({ heartbeatMs: LEASE_MS })).toThrow(TypeError);
    expect(() => h.consumer({ token: "" })).toThrow(TypeError);
    expect(() => h.consumer({ subscription: { name: "bad name!", startAt: "earliest" } })).toThrow(
      TypeError,
    );
    expect(() => h.consumer({ concurrency: 128, maxRecords: 10_000 })).not.toThrow();
  });

  it("run() can only be called once", async () => {
    const h = harness();
    const c = h.consumer();
    settle(c.run(() => {}));
    await expect(c.run(() => {})).rejects.toThrow("only be called once");
    await c.stop();
  });

  it("reports stats for health checks", async () => {
    const h = harness();
    h.fake.append("a", "b", "c");
    let calls = 0;
    const c = h.consumer({ maxRecords: 2, onError: () => {} });
    expect(c.stats()).toMatchObject({ state: "idle", startedAt: null, batches: 0 });
    settle(
      c.run(() => {
        calls++;
        if (calls === 1) throw new Error("once");
      }),
    );
    await until(() => count(h.fake, "ack") === 2);
    await until(() => c.stats().idlePolls > 0);
    const running = c.stats();
    expect(running).toMatchObject({
      state: "running",
      inFlight: 0,
      batches: 3,
      records: 5,
      acks: 2,
      nacks: 1,
      errors: 1,
    });
    expect(running.startedAt).toBeTypeOf("number");
    expect(running.lastAckAt).toBeGreaterThanOrEqual(running.lastErrorAt ?? 0);
    await c.stop();
    expect(c.stats().state).toBe("stopped");
  });

  it("stops when the run() signal aborts", async () => {
    const h = harness();
    h.fake.append("a");
    const ac = new AbortController();
    let handled = 0;
    const c = h.consumer();
    const run = settle(c.run(() => void handled++, { signal: ac.signal }));
    await until(() => handled === 1 && count(h.fake, "ack") === 1);
    ac.abort();
    await until(() => run.done);
    expect(run).toEqual({ done: true });

    const already = h.consumer();
    const run2 = settle(already.run(() => {}, { signal: AbortSignal.abort() }));
    await until(() => run2.done);
    expect(run2).toEqual({ done: true });
  });

  describe("dead-lettering", () => {
    it("isolates a poison record after afterAttempts, dead-letters it and acks the rest", async () => {
      const h = harness();
      h.fake.append("ok-1", "bad", "ok-2");
      const letters: DeadLetter[] = [];
      const events: number[] = [];
      const handled: string[] = [];
      const attempts: number[] = [];
      const c = h
        .consumer({
          onError: () => {},
          deadLetter: { afterAttempts: 3, send: async (l) => void letters.push(...l) },
        })
        .on("deadLetter", (e) => events.push(e.deadLettered));
      settle(
        c.run((batch, ctx) => {
          attempts.push(ctx.attempt);
          for (const r of batch.records) {
            if (r.text() === "bad") throw new Error("cannot parse");
            handled.push(r.text());
          }
        }),
      );
      await until(() => count(h.fake, "ack") === 1);
      await c.stop();
      expect(attempts.slice(0, 3)).toEqual([1, 2, 3]);
      expect(count(h.fake, "nack")).toBe(2);
      expect(letters.map((l) => [l.record.text(), (l.error as Error).message, l.attempt])).toEqual([
        ["bad", "cannot parse", 3],
      ]);
      expect(events).toEqual([1]);
      expect(handled.filter((t) => t === "ok-2")).toHaveLength(1);
      expect(c.stats()).toMatchObject({ deadLettered: 1, acks: 1, nacks: 2 });
      expect(h.fake.backlog(subId(h.fake))).toBe(0);
    });

    it("nacks and drops nothing when sending dead letters fails", async () => {
      const h = harness();
      h.fake.append("bad");
      let sends = 0;
      const errors: string[] = [];
      const c = h
        .consumer({
          onError: (_err, ctx) => void errors.push(ctx.phase),
          deadLetter: {
            afterAttempts: 1,
            send: async () => {
              sends++;
              if (sends === 1) throw new Error("DLQ unavailable");
            },
          },
        })
        .on("error", () => {});
      settle(
        c.run(() => {
          throw new Error("always");
        }),
      );
      await until(() => count(h.fake, "ack") === 1);
      await c.stop();
      expect(sends).toBe(2);
      expect(errors).toContain("deadLetter");
      expect(count(h.fake, "nack")).toBe(1);
      expect(h.fake.backlog(subId(h.fake))).toBe(0);
    });

    it("acks without dead letters when every record succeeds on its own", async () => {
      const h = harness();
      h.fake.append("a", "b");
      const send = vi.fn(async () => {});
      const c = h.consumer({ onError: () => {}, deadLetter: { afterAttempts: 1, send } });
      settle(
        c.run((batch) => {
          if (batch.records.length > 1) throw new Error("batch-level failure");
        }),
      );
      await until(() => count(h.fake, "ack") === 1);
      await c.stop();
      expect(send).not.toHaveBeenCalled();
      expect(count(h.fake, "nack")).toBe(0);
    });

    it("validates the deadLetter option", () => {
      const h = harness();
      expect(() => h.consumer({ deadLetter: {} as DeadLetterOptions })).toThrow(TypeError);
      expect(() => h.consumer({ deadLetter: { afterAttempts: 0, send: async () => {} } })).toThrow(
        TypeError,
      );
    });
  });

  describe("idle polling", () => {
    async function idleConsumes(idlePolling: "shared" | "every-slot"): Promise<number> {
      const h = harness();
      const c = h.consumer({
        concurrency: 8,
        idlePolling,
        pollBackoff: { minMs: 100, maxMs: 1_000 },
      });
      settle(c.run(() => {}));
      await until(() => count(h.fake, "consume") >= 8);
      const before = count(h.fake, "consume");
      await vi.advanceTimersByTimeAsync(60_000);
      const polls = count(h.fake, "consume") - before;
      await c.stop();
      return polls;
    }

    it("keeps one slot polling while the stream is quiet", async () => {
      vi.spyOn(Math, "random").mockReturnValue(0.999);
      const shared = await idleConsumes("shared");
      const everySlot = await idleConsumes("every-slot");
      // ~1 s backoff cap → ~60 polls per slot per minute.
      expect(shared).toBeLessThan(70);
      expect(everySlot).toBeGreaterThan(shared * 6);
    });

    it("wakes every slot when records arrive", async () => {
      const h = harness();
      const c = h.consumer({ concurrency: 4, maxRecords: 1 });
      const workers = new Set<string>();
      let handled = 0;
      settle(
        c.run(async (_b, ctx) => {
          workers.add(ctx.workerId);
          await sleep(1_000);
          handled++;
        }),
      );
      await until(() => count(h.fake, "consume") >= 4);
      await vi.advanceTimersByTimeAsync(30_000);
      for (let i = 0; i < 8; i++) h.fake.append({ i });
      await until(() => handled === 8);
      await c.stop();
      expect(workers.size).toBe(4);
    });
  });

  it("warns when all 128 leases are taken", async () => {
    const h = harness();
    h.fake.append("x");
    h.fake.fail("consume", 10216, 3);
    let handled = 0;
    const c = h.consumer({ onError: () => {} });
    settle(c.run(() => void handled++));
    await until(() => handled === 1);
    await c.stop();
    expect(c.stats().leaseLimitHits).toBe(3);
    expect(h.logs.filter((l) => l.includes("all 128 leases"))).toHaveLength(1);
  });

  it("uses hasMany/addMany when the dedupe store has them", async () => {
    const h = harness();
    h.fake.append("a", "b");
    const store = {
      has: vi.fn(async () => false),
      add: vi.fn(async () => {}),
      hasMany: vi.fn(async (keys: string[]) => keys.map(() => false)),
      addMany: vi.fn(async () => {}),
    };
    const c = h.consumer({ dedupe: store });
    settle(c.run(() => {}));
    await until(() => count(h.fake, "ack") === 1);
    await c.stop();
    expect(store.hasMany).toHaveBeenCalledTimes(1);
    expect(store.addMany).toHaveBeenCalledWith([expect.any(String), expect.any(String)]);
    expect(store.has).not.toHaveBeenCalled();
    expect(store.add).not.toHaveBeenCalled();
  });
});
