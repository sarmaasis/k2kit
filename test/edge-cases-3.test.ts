// Regression tests for the fourth round: user-supplied code that never answers, and K2's real
// record size limit (content + headers + overhead, measured on real K2).
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  K2Client,
  MAX_RECORD_BYTES,
  MAX_RECORDS_PER_REQUEST,
  planProduceRequests,
} from "../src/client.js";
import { Consumer } from "../src/consumer.js";
import {
  DEAD_LETTER_HEADERS,
  type DeadLetter,
  type DeadLetterContext,
  deadLetterTargetStream,
  deadLetterToStream,
} from "../src/dead-letter.js";
import { MemoryDedupeStore } from "../src/dedupe.js";
import type { ConsumerOptions } from "../src/options.js";
import { ID_HEADER } from "../src/record.js";
import type { DedupeStore } from "../src/types.js";
import { FakeK2 } from "./fake-k2.js";

const STREAM = "0123456789abcdef0123456789abcdef";
const DLQ_STREAM = "fedcba9876543210fedcba9876543210";

function setup(fake = new FakeK2()) {
  const phases: string[] = [];
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
      logger: { debug() {}, info() {}, warn() {}, error() {} },
      onError: (_e, ctx) => void phases.push(ctx.phase),
      pollBackoff: { minMs: 10, maxMs: 100 },
      retryBackoff: { minMs: 10, maxMs: 100 },
      workerIdPrefix: "w",
      requestTimeoutMs: 1_000,
      ...opts,
    });
  return { fake, phases, consumer, client: new K2Client(base) };
}

async function until(cond: () => boolean, maxMs = 60_000, stepMs = 50): Promise<void> {
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

const never = <T>() => new Promise<T>(() => {});

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
});
afterEach(() => {
  vi.useRealTimers();
});

describe("user-supplied code that never answers", () => {
  it("a dedupe check that hangs times out, and the batch is processed without dedupe", async () => {
    const { fake, consumer, phases } = setup();
    fake.appendWithId("x");
    const store: DedupeStore = { has: never, add: async () => {}, hasMany: never };
    let handled = 0;
    const c = consumer({ dedupe: store });
    settle(c.run(() => void handled++));
    await until(() => c.stats().acks === 1, 5_000);
    expect(handled).toBe(1);
    expect(phases).toContain("dedupe");
    const stopped = settle(c.stop({ timeoutMs: 200 }));
    await until(() => stopped.done, 1_000);
  });

  it("a dedupe write that hangs times out, and the batch is still acked", async () => {
    const { fake, consumer, phases } = setup();
    fake.appendWithId("x");
    const store: DedupeStore = {
      has: async () => false,
      add: never,
      hasMany: async (keys) => keys.map(() => false),
      addMany: never,
    };
    const c = consumer({ dedupe: store });
    settle(c.run(() => {}));
    await until(() => c.stats().acks === 1, 5_000);
    expect(phases).toContain("dedupe");
    const stopped = settle(c.stop({ timeoutMs: 200 }));
    await until(() => stopped.done, 1_000);
  });

  it("a dead-letter send that hangs (ignoring its signal) times out and the batch is nacked", async () => {
    const { fake, consumer, phases } = setup();
    fake.append("poison");
    const send = vi.fn((_letters: DeadLetter[], _ctx: DeadLetterContext) => never<void>());
    const c = consumer({ deadLetter: { afterAttempts: 1, send } });
    settle(
      c.run(() => {
        throw new Error("poison");
      }),
    );
    await until(() => c.stats().nacks === 1, 10_000);
    expect(phases).toContain("deadLetter");
    expect(c.stats().deadLettered).toBe(0); // nothing dropped
    const signal = send.mock.calls[0]?.[1]?.signal;
    expect(signal?.aborted).toBe(true); // the sender was told to stop
  });

  it("stop() isn't held up by a hanging dead-letter send", async () => {
    const { fake, consumer } = setup();
    fake.append("poison");
    let sending = false;
    const c = consumer({
      requestTimeoutMs: 60_000,
      deadLetter: {
        afterAttempts: 1,
        send: () => {
          sending = true;
          return never();
        },
      },
    });
    settle(
      c.run(() => {
        throw new Error("poison");
      }),
    );
    await until(() => sending, 5_000);
    const stopped = settle(c.stop({ timeoutMs: 500 }));
    await until(() => stopped.done, 2_000);
  });
});

describe("K2's real record size limit (content + headers + overhead)", () => {
  it("produceBatched rejects a record K2 would reject, before sending anything", async () => {
    const { client, fake } = setup();
    const result = settle(
      client.produceBatched([
        { content: "ok" },
        { content: new Uint8Array(999_980) }, // under 1,000,000 alone, over with K2's overhead
      ]),
    );
    await until(() => result.done);
    expect(result.error).toBeInstanceOf(RangeError);
    expect(String(result.error)).toContain("with its headers");
    expect(fake.requests.filter((r) => r.endpoint === "produce")).toHaveLength(0);
  });

  it("skips the automatic k2kit-id when it would push a record over the limit", async () => {
    const { client, fake } = setup();
    await client.produceBatched([{ content: new Uint8Array(999_950) }, { content: "small" }]);
    expect(fake.records[0]?.headers?.[ID_HEADER]).toBeUndefined();
    expect(fake.records[1]?.headers?.[ID_HEADER]).toMatch(/^[0-9a-f-]{36}$/);
  });

  it("dead-letters a record that's right at the size limit, keeping its content", async () => {
    const { fake, client, consumer } = setup();
    const headers = { "event-type": "order.created", trace: "t".repeat(500) };
    // The largest content that fits with these headers under k2kit's estimate.
    const content = new Uint8Array(MAX_RECORD_BYTES - 32 - 2 * 4 - 10 - 13 - 5 - 500 - 1).fill(
      0x61,
    );
    await client.produce([{ content, headers }], { idHeader: false });
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
        throw new Error(`poison ${"!".repeat(900)}`);
      }),
    );
    await until(() => c.stats().deadLettered === 1, 10_000);
    await c.stop();
    const stored = dlqFake.records[0];
    expect(stored).toBeDefined();
    expect(atob(stored?.content ?? "").length).toBe(content.length); // content kept whole
    expect(stored?.headers?.[DEAD_LETTER_HEADERS.droppedHeaders]).toBeDefined();
    expect(fake.requests.filter((r) => r.endpoint === "ack")).toHaveLength(1);
  });
});

describe("synchronous dedupe stores and dead-letter senders (allowed, e.g. from JavaScript)", () => {
  it("a synchronous dedupe store works: no errors, and redelivered records are skipped", async () => {
    const { fake, consumer, phases } = setup();
    fake.appendWithId("x");
    fake.fail("ack", 10213); // ack fails, so the same records come back
    const seen = new Set<string>();
    const store: DedupeStore = {
      has: (k) => seen.has(k),
      add: (k) => void seen.add(k),
      hasMany: (keys) => keys.map((k) => seen.has(k)),
      addMany: (keys) => {
        for (const k of keys) seen.add(k);
      },
    };
    let handled = 0;
    const c = consumer({ dedupe: store });
    settle(c.run(() => void handled++));
    await until(() => c.stats().acks === 1, 10_000);
    await c.stop();
    expect(handled).toBe(1);
    expect(phases.filter((p) => p === "dedupe")).toEqual([]);
  });

  it("a synchronous dead-letter send works, and a synchronous throw nacks instead of crashing", async () => {
    const { fake, consumer } = setup();
    fake.append("poison");
    const letters: string[] = [];
    const c = consumer({
      deadLetter: {
        afterAttempts: 1,
        send: (l) => void letters.push(...l.map((x) => x.record.text())),
      },
    });
    settle(
      c.run(() => {
        throw new Error("poison");
      }),
    );
    await until(() => c.stats().acks === 1, 10_000);
    await c.stop();
    expect(letters).toEqual(["poison"]);

    const t = setup();
    t.fake.append("poison");
    let calls = 0;
    const c2 = t.consumer({
      deadLetter: {
        afterAttempts: 1,
        send: () => {
          calls++;
          if (calls === 1) throw new Error("DLQ down");
        },
      },
    });
    const run = settle(
      c2.run(() => {
        throw new Error("poison");
      }),
    );
    await until(() => c2.stats().acks === 1, 10_000);
    await c2.stop();
    expect(run.error).toBeUndefined();
    expect(t.phases).toContain("deadLetter");
    expect(c2.stats().nacks).toBe(1);
  });
});

describe("subscription names K2 wouldn't allow", () => {
  it("findSubscription returns null and ensureSubscription throws clearly, without asking K2", async () => {
    const { client, fake } = setup();
    const before = fake.requests.length;
    expect(await client.findSubscription("bad name")).toBeNull();
    expect(await client.findSubscription("a".repeat(129))).toBeNull();
    await expect(client.ensureSubscription("bad name", "latest")).rejects.toThrow(TypeError);
    expect(fake.requests.length).toBe(before);
    expect(await client.findSubscription("valid-but-missing")).toBeNull(); // asks K2
    expect(fake.requests.length).toBe(before + 1);
  });
});

describe("request timeouts don't depend on fetch honouring the abort signal", () => {
  const client = (fetchImpl: typeof fetch) =>
    new K2Client({ streamId: STREAM, token: "t", fetch: fetchImpl, timeoutMs: 500 });

  it("a custom fetch that ignores the signal still times out", async () => {
    const ignoresSignal = (() => new Promise<Response>(() => {})) as typeof fetch;
    const result = settle(client(ignoresSignal).listSubscriptions());
    await until(() => result.done, 1_000);
    expect(String(result.error)).toContain("timed out after 500 ms");
  });

  it("a response whose body never finishes still times out", async () => {
    const endlessBody = (async () =>
      new Response(new ReadableStream({ start() {} }), { status: 200 })) as typeof fetch;
    const result = settle(client(endlessBody).listSubscriptions());
    await until(() => result.done, 1_000);
    expect(String(result.error)).toContain("timed out after 500 ms");
  });
});

describe("the dedupe option's shape is checked at construction", () => {
  it("rejects objects that would fail on every batch, accepts working ones", () => {
    const { consumer } = setup();
    expect(() => consumer({ dedupe: new Map() as unknown as DedupeStore })).toThrow(
      /has\(\) and add\(\)/,
    );
    expect(() =>
      consumer({ dedupe: { hasMany: async () => [] } as unknown as DedupeStore }),
    ).toThrow(TypeError);
    expect(() =>
      consumer({
        dedupe: { has: () => false, add: () => {}, hasMany: "yes" } as unknown as DedupeStore,
      }),
    ).toThrow(/hasMany must be a function/);
    // A plain Set has has() and add(), so it works at runtime (though it grows without limit).
    expect(() => consumer({ dedupe: new Set<string>() as unknown as DedupeStore })).not.toThrow();
    class Store {
      has() {
        return false;
      }
      add() {}
    }
    expect(() => consumer({ dedupe: new Store() })).not.toThrow(); // methods on the prototype
    expect(() => consumer({ dedupe: null as unknown as DedupeStore })).not.toThrow(); // means none
  });
});

describe("per the K2 docs: every record in a produce batch shares one timestamp", () => {
  it("identical records without ids are all processed, never skipped as duplicates", async () => {
    const { client, consumer } = setup();
    // Two legitimate, identical events in one batch: same content, same server timestamp.
    await client.produce([{ content: "click" }, { content: "click" }, { content: "click" }], {
      idHeader: false,
    });
    const hasMany = vi.fn(async (keys: string[]) => keys.map(() => false));
    const store: DedupeStore = { has: async () => false, add: async () => {}, hasMany };
    let handled = 0;
    const c = consumer({ dedupe: store });
    settle(
      c.run((b) => {
        handled += b.records.length;
      }),
    );
    await until(() => c.stats().acks === 1, 5_000);
    await c.stop();
    expect(handled).toBe(3);
    expect(hasMany).not.toHaveBeenCalled(); // records without ids never touch the store
  });

  it("…including when they arrive in different batches", async () => {
    const { client, consumer } = setup();
    await client.produce([{ content: "click" }, { content: "click" }], { idHeader: false });
    let handled = 0;
    const c = consumer({ dedupe: new MemoryDedupeStore(), maxRecords: 1 });
    settle(
      c.run((b) => {
        handled += b.records.length;
      }),
    );
    await until(() => c.stats().acks === 2, 5_000);
    await c.stop();
    expect(handled).toBe(2);
  });

  it("identical records in a failing batch count one failed delivery each time, not two", async () => {
    const { client, consumer } = setup();
    // They share a key, so counting per record would double ctx.attempt and dead-letter early.
    await client.produce([{ content: "click" }, { content: "click" }], { idHeader: false });
    const attempts: number[] = [];
    const c = consumer();
    settle(
      c.run((_b, ctx) => {
        attempts.push(ctx.attempt);
        throw new Error("fails");
      }),
    );
    await until(() => attempts.length >= 3, 30_000);
    await c.stop();
    expect(attempts.slice(0, 3)).toEqual([1, 2, 3]);
  });
});

describe("request planning leaves room for K2's per-request metadata", () => {
  it("puts at most 50,000 records and 4,950,000 bytes in a request", () => {
    const tiny = Array.from({ length: 120_000 }, () => ({ content: "QQ==" }));
    const requests = planProduceRequests(tiny);
    expect(requests.map((r) => r.length)).toEqual([
      MAX_RECORDS_PER_REQUEST,
      MAX_RECORDS_PER_REQUEST,
      20_000,
    ]);
    const big = Array.from({ length: 20 }, () => ({ content: "A".repeat(400_000) }));
    for (const r of planProduceRequests(big)) {
      expect(
        new TextEncoder().encode(JSON.stringify({ records: r })).byteLength,
      ).toBeLessThanOrEqual(4_950_000);
    }
  });
});

describe("header limits from the K2 docs are checked before sending", () => {
  const cases: [string, { [k: string]: string }, RegExp][] = [
    ["a value over 8 KiB", { big: "v".repeat(8_193) }, /value exceeds 8192/],
    ["a name over 256 bytes", { ["n".repeat(257)]: "v" }, /name .* exceeds 256/],
    [
      "more than 32 headers",
      Object.fromEntries(Array.from({ length: 33 }, (_, i) => [`h${i}`, "v"])),
      /33 headers/,
    ],
    [
      "over 64 KiB in total",
      Object.fromEntries(Array.from({ length: 9 }, (_, i) => [`h${i}`, "x".repeat(7_300)])),
      /headers total/,
    ],
    ["multi-byte characters counted in UTF-8", { big: "é".repeat(4_097) }, /value exceeds 8192/],
  ];
  it.each(cases)("rejects %s without sending the batch", async (_label, headers, message) => {
    const { client, fake } = setup();
    const result = settle(client.produceBatched([{ content: "ok" }, { content: "x", headers }]));
    await until(() => result.done);
    expect(result.error).toBeInstanceOf(RangeError);
    expect(String(result.error)).toMatch(message);
    expect(String(result.error)).toContain("record 1");
    expect(fake.requests.filter((r) => r.endpoint === "produce")).toHaveLength(0);
  });

  it("accepts headers exactly at the limits", async () => {
    const { client, fake } = setup();
    await client.produceBatched([
      { content: "x", headers: { ["n".repeat(256)]: "v".repeat(8_192) } },
    ]);
    expect(fake.records).toHaveLength(1);
  });
});

describe("produce input from plain JavaScript is checked, never silently changed", () => {
  const sent = (fake: FakeK2) =>
    fake.records.map((r) => Buffer.from(r.content, "base64").toString());

  it("content with no JSON form (undefined, a function) throws instead of producing an empty record", async () => {
    const { fake, client } = setup();
    for (const content of [undefined, () => 1]) {
      const records = [{ content: "ok" }, { content }] as never[];
      await expect(client.produceBatched(records)).rejects.toThrow(/record 1: content must be/);
      await expect(client.produce(records)).rejects.toThrow(TypeError);
    }
    expect(fake.records).toHaveLength(0);
  });

  it("ArrayBuffer and typed-array views are sent as their bytes, not JSON-stringified", async () => {
    const { fake, client } = setup();
    const bytes = new TextEncoder().encode("xxhelloxx");
    await client.produce(
      [
        { content: bytes.buffer },
        { content: new DataView(bytes.buffer, 2, 5) },
        { content: new Uint16Array(bytes.buffer.slice(2, 6)) },
      ],
      { idHeader: false },
    );
    expect(sent(fake)).toEqual(["xxhelloxx", "hello", "hell"]);
  });

  it("header values must be strings; undefined values are dropped as JSON would", async () => {
    const { fake, client } = setup();
    const bad = [{ a: 1 }, { a: null }, { a: { b: "c" } }, "a=b", ["a"], new Map([["a", "b"]])];
    for (const headers of [...bad, new Headers({ a: "b" })]) {
      await expect(client.produceBatched([{ content: "x", headers } as never])).rejects.toThrow(
        TypeError,
      );
    }
    expect(fake.records).toHaveLength(0);
    // 40 undefined values don't count toward K2's 32-header limit and aren't sent.
    const headers: { [name: string]: string | undefined } = { kept: "v" };
    for (let i = 0; i < 40; i++) headers[`gone${i}`] = undefined;
    await client.produceBatched([
      { content: "x", headers } as never,
      { content: "y", headers: null } as never,
    ]);
    expect(fake.records.map((r) => Object.keys(r.headers ?? {}).sort())).toEqual([
      ["k2kit-id", "kept"],
      ["k2kit-id"],
    ]);
  });

  it("a header named __proto__ is kept as a header, not turned into a prototype", async () => {
    const { fake, client } = setup();
    const headers = JSON.parse('{"__proto__":"v","a":"b"}');
    await client.produce([{ content: "x", headers }], { idHeader: false });
    expect(Object.entries(fake.records[0]?.headers ?? {})).toEqual([
      ["__proto__", "v"],
      ["a", "b"],
    ]);
  });
});

describe("dead-letter headers with unusual names and text", () => {
  async function deadLetterOne(headers: { [name: string]: string }, message: string) {
    const { fake, client, consumer } = setup();
    await client.produce([{ content: "x", headers }]);
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
        throw new Error(message);
      }),
    );
    await until(() => c.stats().deadLettered === 1, 10_000);
    await c.stop();
    return dlqFake.records[0]?.headers ?? {};
  }

  it("keeps headers named like Object properties (constructor, toString, __proto__)", async () => {
    const original = JSON.parse('{"constructor":"c","toString":"t","__proto__":"p","a":"b"}');
    const stored = await deadLetterOne(original, "poison");
    for (const [name, value] of Object.entries(original)) {
      expect(Object.hasOwn(stored, name) && stored[name]).toBe(value);
    }
    expect(stored[DEAD_LETTER_HEADERS.droppedHeaders]).toBeUndefined();
  });

  it("doesn't cut a character in half when shortening the error message", async () => {
    // Error: prefix + message; place an emoji (two UTF-16 units) across the 1,024-unit cut.
    const prefix = "Error: ";
    const stored = await deadLetterOne({}, `${"x".repeat(1_023 - prefix.length)}😀 tail`);
    const error = stored[DEAD_LETTER_HEADERS.error] ?? "";
    expect(error).toMatch(/^Error: x+$/); // ends before the emoji, not on half of it
  });
});

describe("produce with nothing or holes to send", () => {
  it("produce([]) sends no request (K2 rejects a batch with no records)", async () => {
    const { fake, client } = setup();
    await client.produce([]);
    expect(await client.produceBatched([])).toEqual({ records: 0, requests: 0 });
    expect(fake.requests).toHaveLength(0);
  });

  it("a single record object (not an array) throws instead of producing nothing", async () => {
    const { fake, client } = setup();
    const one = { content: "x" } as never;
    await expect(client.produceBatched(one)).rejects.toThrow(/records must be an array/);
    await expect(client.produce(one)).rejects.toThrow(/records must be an array/);
    expect(fake.requests).toHaveLength(0);
  });

  it("a sparse array throws for the hole instead of sending null", async () => {
    const { fake, client } = setup();
    // biome-ignore lint/suspicious/noSparseArray: the hole is the point
    const records = [{ content: "a" }, , { content: "c" }] as never[];
    await expect(client.produceBatched(records)).rejects.toThrow(/record 1 must be an object/);
    expect(fake.requests).toHaveLength(0);
  });
});

describe("options given explicitly as undefined mean the default", () => {
  it("pollBackoff/retryBackoff with undefined fields don't make an idle consumer spin", async () => {
    const { fake, consumer } = setup();
    const c = consumer({
      pollBackoff: { minMs: undefined, maxMs: undefined },
      retryBackoff: { minMs: undefined, maxMs: undefined, maxAttempts: undefined },
    } as Partial<ConsumerOptions>);
    settle(c.run(() => {}));
    await vi.advanceTimersByTimeAsync(10_000);
    await c.stop();
    // Defaults (250 ms .. 10 s, full jitter) allow a handful of polls in 10 s, not thousands.
    expect(fake.requests.filter((r) => r.endpoint === "consume").length).toBeLessThan(100);
  });
});

describe("produceBatched retry options", () => {
  it("undefined backoff fields keep their defaults, so retries still wait", async () => {
    const { fake, client } = setup();
    fake.fail("produce", 10211); // retryable: K2 temporarily unavailable
    const random = vi.spyOn(Math, "random").mockReturnValue(0.5);
    try {
      const state = settle(
        client.produceBatched([{ content: "x" }], {
          retryBackoff: { minMs: 1_000, maxMs: undefined } as never,
        }),
      );
      // Full jitter: 0.5 × min(default maxMs 10,000, minMs 1,000) = 500 ms before the retry.
      await vi.advanceTimersByTimeAsync(400);
      expect(state.done).toBe(false);
      await vi.advanceTimersByTimeAsync(200);
      expect(state).toEqual({ done: true });
      expect(fake.records).toHaveLength(1);
    } finally {
      random.mockRestore();
    }
  });

  it("NaN or negative maxAttempts and backoff values are rejected before sending", async () => {
    const { fake, client } = setup();
    for (const opts of [
      { maxAttempts: Number.NaN },
      { maxAttempts: -1 },
      { retryBackoff: { minMs: Number.NaN, maxMs: 100 } },
      { retryBackoff: { minMs: 0, maxMs: -5 } },
    ]) {
      await expect(client.produceBatched([{ content: "x" }], opts)).rejects.toThrow(TypeError);
    }
    expect(fake.requests).toHaveLength(0);
    await client.produceBatched([{ content: "x" }], { maxAttempts: 0 }); // allowed, as in 0.1.0
    expect(fake.records).toHaveLength(1);
  });
});

describe("K2Client stream id", () => {
  it("rejects a stream id that would send the token to another host", async () => {
    const fetchSpy = vi.fn(async () => new Response("{}"));
    for (const streamId of ["attacker.example#", "attacker.example/x?", "a@b.example?", "", 5]) {
      expect(() => new K2Client({ streamId, token: "t", fetch: fetchSpy } as never)).toThrow(
        /streamId must be 32 hex characters/,
      );
    }
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("accepts an uppercase id (it worked in 0.1.0) and normalizes it to lowercase", () => {
    const client = new K2Client({ streamId: STREAM.toUpperCase(), token: "t" });
    expect(client.streamId).toBe(STREAM);
    expect(client.endpoint).toBe(`https://${STREAM}.k2.cloudflarestorage.com`);
    // So the dead-letter "same stream" check can't be bypassed by case.
    expect(deadLetterTargetStream(deadLetterToStream(client))).toBe(STREAM);
  });
});

describe("an ack K2 keeps refusing", () => {
  it("backs off instead of reprocessing the still-leased batch in a tight loop", async () => {
    const { fake, consumer, phases } = setup();
    fake.appendWithId({ n: 1 });
    fake.fail("ack", 10213, 10_000); // non-retryable internal error, every time
    let handled = 0;
    const c = consumer({ retryBackoff: { minMs: 500, maxMs: 30_000 } });
    settle(
      c.run(() => {
        handled++;
      }),
    );
    await vi.advanceTimersByTimeAsync(10_000);
    // Without the backoff this was ~100,000 handler calls (and K2 requests) in 10 s.
    expect(handled).toBeGreaterThan(1);
    expect(handled).toBeLessThan(40);
    expect(phases).toContain("ack");
    void c.stop();
    await vi.advanceTimersByTimeAsync(60_000);
  });
});
