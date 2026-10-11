import { afterEach, describe, expect, it, vi } from "vitest";
import { Backoff, fullJitter, sleep } from "../src/backoff.js";
import { MemoryDedupeStore } from "../src/dedupe.js";
import { describeError, isRetryableError, K2Error } from "../src/errors.js";
import {
  contentToBytes,
  decodeRecord,
  fnv1a64,
  fromBase64,
  fromBase64Portable,
  ID_HEADER,
  toBase64,
  toBase64Portable,
  utf8DecodeStrict,
  utf8Encode,
} from "../src/record.js";

describe("base64", () => {
  it("round-trips binary, UTF-8 and JSON content", () => {
    const binary = new Uint8Array(256).map((_, i) => i);
    expect(fromBase64(toBase64(binary))).toEqual(binary);

    const text = "héllo 🌍 — K2";
    expect(utf8DecodeStrict(fromBase64(toBase64(contentToBytes(text))))).toBe(text);

    const obj = { n: 1, nested: { ok: true }, s: "ünïcode" };
    expect(JSON.parse(utf8DecodeStrict(fromBase64(toBase64(contentToBytes(obj)))) ?? "")).toEqual(
      obj,
    );
  });

  it("produces standard, padded base64", () => {
    expect(toBase64(new Uint8Array([0xfb, 0xff]))).toBe("+/8=");
    expect(toBase64(new Uint8Array([1]))).toBe("AQ==");
    expect(toBase64(new Uint8Array())).toBe("");
  });

  it("handles large payloads without stack overflow", () => {
    const big = new Uint8Array(3_000_000).map((_, i) => i % 251);
    expect(fromBase64(toBase64(big))).toEqual(big);
  });

  it("detects invalid UTF-8", () => {
    expect(utf8DecodeStrict(new Uint8Array([0xff, 0xfe]))).toBeNull();
  });
});

describe("fnv1a64", () => {
  it("matches published test vectors", () => {
    expect(fnv1a64(new Uint8Array())).toBe("cbf29ce484222325");
    expect(fnv1a64(utf8Encode("a"))).toBe("af63dc4c8601ec8c");
    expect(fnv1a64(utf8Encode("foobar"))).toBe("85944171f73967e8");
  });

  it("hashes parts as one stream", () => {
    expect(fnv1a64(utf8Encode("foo"), utf8Encode("bar"))).toBe(fnv1a64(utf8Encode("foobar")));
  });
});

describe("decodeRecord", () => {
  it("decodes content and defaults headers", () => {
    const r = decodeRecord({ timestamp_ms: 5, content: toBase64(utf8Encode('{"a":1}')) });
    expect(r.headers).toEqual({});
    expect(r.text()).toBe('{"a":1}');
    expect(r.json<{ a: number }>().a).toBe(1);
  });

  it("uses the k2kit-id header as key, else a hash of timestamp + content", () => {
    const content = toBase64(utf8Encode("x"));
    const withId = decodeRecord({ timestamp_ms: 1, content, headers: { [ID_HEADER]: "abc" } });
    expect(withId.key()).toBe("abc");

    const a = decodeRecord({ timestamp_ms: 1, content });
    const b = decodeRecord({ timestamp_ms: 1, content });
    const c = decodeRecord({ timestamp_ms: 2, content });
    expect(a.key()).toMatch(/^[0-9a-f]{16}$/);
    expect(a.key()).toBe(b.key());
    expect(a.key()).not.toBe(c.key());
  });
});

describe("backoff", () => {
  it("uses full jitter capped at maxMs", () => {
    const range = { minMs: 100, maxMs: 1000 };
    expect(fullJitter(0, range, () => 0.999)).toBe(99);
    expect(fullJitter(3, range, () => 0.5)).toBe(400);
    expect(fullJitter(20, range, () => 0.999)).toBe(999);
    expect(fullJitter(5, range, () => 0)).toBe(0);
  });

  it("tracks and resets attempts", () => {
    const b = new Backoff({ minMs: 10, maxMs: 1000 }, () => 1);
    expect([b.next(), b.next(), b.next()]).toEqual([10, 20, 40]);
    expect(b.attempt).toBe(3);
    b.reset();
    expect(b.next()).toBe(10);
  });

  describe("sleep", () => {
    afterEach(() => {
      vi.useRealTimers();
    });

    it("resolves after the delay", async () => {
      vi.useFakeTimers();
      let done = false;
      const p = sleep(1000).then(() => {
        done = true;
      });
      await vi.advanceTimersByTimeAsync(999);
      expect(done).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      await p;
      expect(done).toBe(true);
    });

    it("wakes early on abort", async () => {
      vi.useFakeTimers();
      const ac = new AbortController();
      const p = sleep(60_000, ac.signal);
      ac.abort();
      await p;
      expect(vi.getTimerCount()).toBe(0);
    });

    it("returns immediately when already aborted", async () => {
      await sleep(60_000, AbortSignal.abort());
    });
  });
});

describe("MemoryDedupeStore", () => {
  it("evicts the least recently used key", async () => {
    const store = new MemoryDedupeStore({ maxSize: 2 });
    await store.add("a");
    await store.add("b");
    expect(await store.has("a")).toBe(true); // a is now most recent
    await store.add("c"); // evicts b
    expect(await store.has("b")).toBe(false);
    expect(await store.has("a")).toBe(true);
    expect(await store.has("c")).toBe(true);
    expect(store.size).toBe(2);
  });

  it("rejects a bad maxSize", () => {
    expect(() => new MemoryDedupeStore({ maxSize: 0 })).toThrow(TypeError);
  });
});

describe("errors", () => {
  it("treats non-K2 errors as retryable", () => {
    expect(isRetryableError(new TypeError("fetch failed"))).toBe(true);
    const err = new K2Error({ code: 10209, status: 401, message: "bad token", retryable: false });
    expect(isRetryableError(err)).toBe(false);
    expect(describeError(err)).toBe("K2 10209: bad token");
  });
});

describe("fast paths match the portable implementations", () => {
  // Deterministic pseudo-random bytes so failures reproduce.
  function bytes(n: number, seed: number): Uint8Array {
    const out = new Uint8Array(n);
    let x = seed;
    for (let i = 0; i < n; i++) {
      x = (Math.imul(x, 1103515245) + 12345) >>> 0;
      out[i] = x >>> 24;
    }
    return out;
  }

  function fnvBigInt(...parts: Uint8Array[]): string {
    let hash = 0xcbf29ce484222325n;
    for (const part of parts) {
      for (const byte of part) {
        hash ^= BigInt(byte);
        hash = (hash * 0x100000001b3n) & 0xffffffffffffffffn;
      }
    }
    return hash.toString(16).padStart(16, "0");
  }

  it("base64 encodes and decodes identically for every length 0–600 and large inputs", () => {
    const sizes = [...Array.from({ length: 601 }, (_, i) => i), 24_575, 24_576, 24_577, 1_000_003];
    for (const n of sizes) {
      const input = bytes(n, n + 1);
      const encoded = toBase64(input);
      expect(encoded, `encode ${n}`).toBe(toBase64Portable(input));
      expect(fromBase64(encoded), `decode ${n}`).toEqual(fromBase64Portable(encoded));
      expect(fromBase64(encoded)).toEqual(input);
    }
  });

  it("encodes subarray views without reading outside them", () => {
    const whole = bytes(100, 7);
    const view = whole.subarray(10, 20);
    expect(toBase64(view)).toBe(toBase64Portable(view));
    expect(fromBase64(toBase64(view))).toEqual(view.slice());
  });

  it("the 16-bit-limb FNV-1a matches a BigInt reference", () => {
    for (let n = 0; n < 300; n++) {
      const input = bytes(n, n * 31 + 7);
      expect(fnv1a64(input), `length ${n}`).toBe(fnvBigInt(input));
    }
    const a = bytes(5_000, 1);
    const b = bytes(777, 2);
    expect(fnv1a64(a, b)).toBe(fnvBigInt(a, b));
  });
});
