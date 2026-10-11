import { readFileSync } from "node:fs";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { CliIo } from "../src/cli/context.js";
import { formatRecord, parseTime, UsageError } from "../src/cli/format.js";
import { main } from "../src/cli/main.js";
import { parseHeaders, splitInput } from "../src/cli/produce.js";
import { MAX_PRODUCE_REQUEST_BYTES as MAX_REQUEST_BYTES } from "../src/client.js";
import { decodeRecord, toBase64, utf8Encode } from "../src/record.js";
import { VERSION } from "../src/version.js";
import { FakeK2 } from "./fake-k2.js";

const STREAM = "0123456789abcdef0123456789abcdef";
const ENDPOINT = ["--endpoint", "https://fake.k2.test"];

interface TestCli {
  fake: FakeK2;
  notes: (string | undefined)[];
  io: CliIo;
  out: string[];
  err: string[];
  /** Simulates Ctrl+C. */
  interrupt(): void;
  stdout(): string;
  stderr(): string;
  run(...argv: string[]): { code?: number; done: boolean };
}

/** Feeds bytes as stdin in 64 KiB chunks, like a pipe would. */
async function* chunksOf(bytes: Uint8Array): AsyncGenerator<Uint8Array> {
  for (let i = 0; i < bytes.length; i += 65_536) yield bytes.subarray(i, i + 65_536);
}

function cli(
  opts: {
    stdin?: Uint8Array | string;
    stdinStream?: AsyncIterable<Uint8Array>;
    stdinIsTTY?: boolean;
    /** Replaces the stdout writer, e.g. to simulate a slow reader. */
    stdout?: (text: string) => void | Promise<void>;
    confirm?: boolean | null;
    fake?: FakeK2;
    /** Delays K2's response to subscription creation, to test interrupts during setup. */
    createDelayMs?: number;
    /** Wraps the fake's fetch, e.g. to lose responses after K2 has applied a request. */
    wrapFetch?: (inner: typeof fetch) => typeof fetch;
  } = {},
) {
  const fake = opts.fake ?? new FakeK2();
  const out: string[] = [];
  const err: string[] = [];
  const handlers = new Set<(s: string) => void>();
  const notes: (string | undefined)[] = [];
  const stdin = typeof opts.stdin === "string" ? utf8Encode(opts.stdin) : opts.stdin;
  const io: CliIo = {
    env: { K2_STREAM_ID: STREAM, CLOUDFLARE_API_TOKEN: fake.token },
    stdout: (t) => {
      out.push(t);
      return opts.stdout?.(t);
    },
    stdinIsTTY: opts.stdinIsTTY ?? false,
    stderr: (t) => void err.push(t),
    stdin: () => opts.stdinStream ?? chunksOf(stdin ?? new Uint8Array()),
    confirm: async () => opts.confirm ?? null,
    onSignal: (h) => {
      handlers.add(h);
      return () => void handlers.delete(h);
    },
    now: () => Date.now(),
    fetch: (opts.wrapFetch ?? ((f) => f))(async (input, init) => {
      if (
        opts.createDelayMs &&
        init?.method === "POST" &&
        String(input).endsWith("/subscriptions")
      ) {
        await new Promise((r) => setTimeout(r, opts.createDelayMs));
      }
      return fake.fetch(input, init);
    }),
    setCleanupNote: (note) => void notes.push(note),
  };
  const t: TestCli = {
    fake,
    io,
    notes,
    out,
    err,
    interrupt: () => {
      for (const h of handlers) h("SIGINT");
    },
    stdout: () => out.join(""),
    stderr: () => err.join(""),
    run: (...argv) => {
      const state: { code?: number; done: boolean } = { done: false };
      void main([...argv, ...ENDPOINT], io).then((code) => {
        state.code = code;
        state.done = true;
      });
      return state;
    },
  };
  return t;
}

async function until(cond: () => boolean, maxMs = 120_000, stepMs = 50): Promise<void> {
  for (let t = 0; t <= maxMs; t += stepMs) {
    if (cond()) return;
    await vi.advanceTimersByTimeAsync(stepMs);
  }
  throw new Error(`condition not met within ${maxMs} fake ms`);
}

const tempSubs = (fake: FakeK2, kind: string) =>
  fake.subscriptionNames().filter((n) => n.startsWith(`k2kit-${kind}-`));

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
});

afterEach(() => {
  vi.useRealTimers();
});

describe("cli tail", () => {
  it("prints new records and deletes its subscription on SIGINT", async () => {
    const t = cli();
    t.fake.append("before tail");
    const run = t.run("tail", "--headers");
    await until(() => tempSubs(t.fake, "tail").length === 1);
    expect(tempSubs(t.fake, "tail")[0]).toMatch(/^k2kit-tail-[a-z0-9]{6}$/);
    t.fake.append({ n: 1 }, { n: 2 });
    await until(() => t.out.length === 2);
    t.interrupt();
    await until(() => run.done);
    expect(run.code).toBe(0);
    expect(t.stdout()).not.toContain("before tail");
    expect(t.out[0]).toMatch(/^\d{4}-\d\d-\d\dT[\d:.]+Z {2}- {2}\{"n":1\}\n$/);
    expect(tempSubs(t.fake, "tail")).toEqual([]);
  });

  it("filters with --grep and --header", async () => {
    const t = cli();
    const run = t.run("tail", "--grep", "order", "--header", "env=prod", "--json", "--limit", "1");
    await until(() => tempSubs(t.fake, "tail").length === 1);
    await t.fake.fetch("https://fake.k2.test/produce", {
      method: "POST",
      headers: { authorization: `Bearer ${t.fake.token}`, "content-type": "application/json" },
      body: JSON.stringify({
        records: [
          { content: btoa("order 1"), headers: { env: "dev" } },
          { content: btoa("refund 2"), headers: { env: "prod" } },
          { content: btoa("order 3"), headers: { env: "prod" } },
        ],
      }),
    });
    await until(() => run.done);
    expect(run.code).toBe(0);
    expect(t.out.map((l) => JSON.parse(l).content)).toEqual(["order 3"]);
  });

  it("deletes its subscription when consuming fails", async () => {
    const t = cli();
    const run = t.run("tail");
    await until(() => tempSubs(t.fake, "tail").length === 1);
    t.fake.fail("consume", 10210);
    await until(() => run.done);
    expect(run.code).toBe(1);
    expect(t.stderr()).toContain("K2 10210:");
    expect(tempSubs(t.fake, "tail")).toEqual([]);
  });

  it("stops at --limit and cleans up", async () => {
    const t = cli();
    const run = t.run("tail", "--limit", "2", "--json");
    await until(() => tempSubs(t.fake, "tail").length === 1);
    t.fake.append("a", "b", "c");
    await until(() => run.done);
    expect(run.code).toBe(0);
    expect(t.out.map((l) => JSON.parse(l).content)).toEqual(["a", "b"]);
    expect(tempSubs(t.fake, "tail")).toEqual([]);
  });

  it("prints the subscription name when deletion fails", async () => {
    const t = cli();
    const run = t.run("tail", "--limit", "1");
    await until(() => tempSubs(t.fake, "tail").length === 1);
    const name = tempSubs(t.fake, "tail")[0] as string;
    t.fake.fail("deleteSubscription", 10213);
    t.fake.append("x");
    await until(() => run.done);
    expect(t.stderr()).toContain(`failed to delete subscription ${name}`);
    expect(t.stderr()).toContain(`k2kit subs rm ${name}`);
  });

  it("refuses at 100 subscriptions and warns from 90", async () => {
    const full = cli();
    for (let i = 0; i < 100; i++) full.fake.addSubscription(`s${i}`);
    const run = full.run("tail");
    await until(() => run.done);
    expect(run.code).toBe(1);
    expect(full.stderr()).toContain("k2kit subs rm --prefix k2kit-");

    const busy = cli();
    for (let i = 0; i < 90; i++) busy.fake.addSubscription(`s${i}`);
    const run2 = busy.run("tail", "--limit", "1");
    await until(() => tempSubs(busy.fake, "tail").length === 1);
    expect(busy.stderr()).toContain("warning: stream has 90/100 subscriptions");
    busy.fake.append("x");
    await until(() => run2.done);
    expect(run2.code).toBe(0);
  });
});

describe("cli replay", () => {
  it("filters by time and text, stops after two empty batches, and cleans up", async () => {
    const t = cli();
    const start = Date.now();
    t.fake.append("old match");
    await vi.advanceTimersByTimeAsync(3 * 3_600_000);
    t.fake.append("new match", "new other", "binary?");
    const run = t.run("replay", "--since", "2h", "--grep", "match");
    await until(() => run.done);
    expect(run.code).toBe(0);
    expect(t.out).toHaveLength(1);
    expect(t.out[0]).toContain("new match");
    expect(t.stderr()).not.toContain("billed");
    expect(tempSubs(t.fake, "replay")).toEqual([]);
    expect(Date.now() - start).toBeGreaterThan(0);
  });

  it("warns without --since and honours --until and --limit", async () => {
    const t = cli();
    t.fake.append("a", "b");
    await vi.advanceTimersByTimeAsync(60 * 60_000);
    t.fake.append("c");
    const run = t.run("replay", "--until", "30m", "--json");
    await until(() => run.done);
    expect(t.stderr()).toContain("billed once K2 billing starts");
    expect(t.out.map((l) => JSON.parse(l).content)).toEqual(["a", "b"]);

    const t2 = cli();
    t2.fake.append("1", "2", "3");
    const run2 = t2.run("replay", "--limit", "2");
    await until(() => run2.done);
    expect(t2.out).toHaveLength(2);
    expect(tempSubs(t2.fake, "replay")).toEqual([]);
  });
});

describe("cli subs", () => {
  it("lists subscriptions as a table or NDJSON", async () => {
    const t = cli();
    t.fake.addSubscription("alpha");
    t.fake.addSubscription("beta", "latest");
    const run = t.run("subs", "ls");
    await until(() => run.done);
    const lines = t.stdout().trim().split("\n");
    expect(lines[0]).toMatch(/^NAME\s+ID\s+START_AT\s+CREATED_AT$/);
    expect(lines[2]).toMatch(/^beta\s+[0-9a-f]{32}\s+latest\s+\d{4}-/);

    const t2 = cli({ fake: t.fake });
    const run2 = t2.run("subs", "ls", "--json");
    await until(() => run2.done);
    expect(t2.out.map((l) => JSON.parse(l).name)).toEqual(["alpha", "beta"]);
  });

  it("removes one subscription by name or id", async () => {
    const t = cli();
    t.fake.addSubscription("by-name");
    const id = t.fake.addSubscription("by-id");
    const a = t.run("subs", "rm", "by-name");
    await until(() => a.done);
    const b = t.run("subs", "rm", id);
    await until(() => b.done);
    expect([a.code, b.code]).toEqual([0, 0]);
    expect(t.fake.subscriptionNames()).toEqual([]);
  });

  it("subs rm --prefix refuses without --yes", async () => {
    const t = cli({ confirm: null });
    t.fake.addSubscription("k2kit-tail-aaaaaa");
    t.fake.addSubscription("k2kit-tail-bbbbbb");
    t.fake.addSubscription("orders");
    const run = t.run("subs", "rm", "--prefix", "k2kit-tail-");
    await until(() => run.done);
    expect(run.code).toBe(1);
    expect(t.stderr()).toContain("refusing to delete without --yes");
    expect(t.fake.subscriptionNames()).toHaveLength(3);

    const declined = cli({ confirm: false, fake: t.fake });
    const run2 = declined.run("subs", "rm", "--prefix", "k2kit-tail-");
    await until(() => run2.done);
    expect(t.fake.subscriptionNames()).toHaveLength(3);

    const yes = cli({ fake: t.fake });
    const run3 = yes.run("subs", "rm", "--prefix", "k2kit-tail-", "--yes");
    await until(() => run3.done);
    expect(run3.code).toBe(0);
    expect(t.fake.subscriptionNames()).toEqual(["orders"]);
  });
});

describe("cli produce", () => {
  it("splits a 12 MB input into 3+ requests under 5 MB", async () => {
    const line = `${JSON.stringify({ pad: "x".repeat(990) })}\n`;
    const input = line.repeat(Math.ceil(12_000_000 / line.length));
    const t = cli({ stdin: input });
    const run = t.run("produce", "--header", "event-type=test");
    await until(() => run.done);
    expect(run.code).toBe(0);
    const requests = t.fake.requests.filter((r) => r.endpoint === "produce");
    expect(requests.length).toBeGreaterThanOrEqual(3);
    for (const r of requests) {
      expect(utf8Encode(JSON.stringify(r.body)).byteLength).toBeLessThan(MAX_REQUEST_BYTES);
    }
    const lines = input.split("\n").filter(Boolean).length;
    expect(t.fake.records).toHaveLength(lines);
    expect(t.fake.records[0]?.headers?.["event-type"]).toBe("test");
    expect(t.stderr()).toContain(`produced ${lines} records in ${requests.length} requests`);
  });

  it("sends all of stdin as one record with --raw, and gzips with --gzip", async () => {
    const t = cli({ stdin: "line 1\nline 2\n" });
    const run = t.run("produce", "--raw", "--gzip");
    await until(() => run.done);
    expect(t.fake.records).toHaveLength(1);
    expect(t.fake.requests.at(-1)?.headers["content-encoding"]).toBe("gzip");
  });

  it("retries retryable errors and does not retry 10212", async () => {
    const t = cli({ stdin: "a\nb\n" });
    t.fake.fail("produce", 10211, 2);
    const run = t.run("produce");
    await until(() => run.done);
    expect(run.code).toBe(0);
    expect(t.fake.records).toHaveLength(2);

    const t2 = cli({ stdin: "a\n" });
    t2.fake.fail("produce", 10212);
    const run2 = t2.run("produce");
    await until(() => run2.done);
    expect(run2.code).toBe(1);
    expect(t2.stderr()).toContain("K2 10212:");
    expect(t2.fake.requests.filter((r) => r.endpoint === "produce")).toHaveLength(1);
  });

  it("splits NDJSON lines and parses headers", () => {
    const parts = splitInput(utf8Encode('{"a":1}\r\n\n  \n{"b":2}'), false);
    expect(parts.map((p) => new TextDecoder().decode(p))).toEqual(['{"a":1}', '{"b":2}']);
    expect(parseHeaders(["k=v", "x=a=b"])).toEqual({ k: "v", x: "a=b" });
    expect(() => parseHeaders(["novalue"])).toThrow(UsageError);
  });
});

describe("cli basics", () => {
  it("parses durations 30m, 2h, 7d and ISO dates", () => {
    const now = Date.UTC(2026, 9, 11, 12, 0, 0);
    expect(parseTime("30m", now, "--since")).toBe(now - 30 * 60_000);
    expect(parseTime("2h", now, "--since")).toBe(now - 2 * 3_600_000);
    expect(parseTime("7d", now, "--since")).toBe(now - 7 * 86_400_000);
    expect(parseTime("2026-10-01T00:00:00Z", now, "--since")).toBe(Date.UTC(2026, 9, 1));
    expect(() => parseTime("yesterday", now, "--since")).toThrow(UsageError);
  });

  it("formats binary content as base64", () => {
    const record = decodeRecord({
      timestamp_ms: 0,
      content: toBase64(new Uint8Array([0xff, 0xfe])),
    });
    expect(formatRecord(record, {})).toBe("1970-01-01T00:00:00.000Z  base64://4=");
    expect(JSON.parse(formatRecord(record, { json: true }))).toEqual({
      timestamp_ms: 0,
      headers: {},
      content: "//4=",
      encoding: "base64",
    });
  });

  it("exits 2 on usage errors", async () => {
    for (const argv of [
      ["nope"],
      ["tail", "--raw"],
      ["subs", "ls", "extra"],
      ["tail", "--limit", "0"],
    ]) {
      const t = cli();
      const run = t.run(...argv);
      await until(() => run.done);
      expect(run.code, argv.join(" ")).toBe(2);
    }
    const t = cli();
    t.io.env = {};
    const run = t.run("subs", "ls");
    await until(() => run.done);
    expect(run.code).toBe(2);
    expect(t.stderr()).toContain("missing --stream");
  });

  it("prints the version, matching package.json", async () => {
    const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
    expect(VERSION).toBe(pkg.version);
    const t = cli();
    const run = t.run("--version");
    await until(() => run.done);
    expect(t.stdout()).toBe(`${VERSION}\n`);
  });

  it("never prints the token", async () => {
    const t = cli();
    const secret = "tok_NEVER_PRINT_me";
    const run = t.run("subs", "ls", "--token", secret);
    await until(() => run.done);
    expect(run.code).toBe(1);
    expect(t.stderr()).toContain("K2 10209:");
    expect(t.stdout() + t.stderr()).not.toContain(secret);
  });
});

describe("cli edge cases", () => {
  it("10. produce streams: lines are sent while stdin is still open", async () => {
    let release = () => {};
    const gate = new Promise<void>((r) => {
      release = r;
    });
    async function* slowStdin() {
      yield utf8Encode('{"n":1}\n');
      await gate; // like `tail -f`: more input may never come
      yield utf8Encode('{"n":2}\n');
    }
    const t = cli({ stdinStream: slowStdin() });
    const run = t.run("produce");
    await until(() => t.fake.records.length === 1, 5_000);
    expect(run.done).toBe(false); // sent after 1 s idle, before stdin ended
    release();
    await until(() => run.done);
    expect(run.code).toBe(0);
    expect(t.fake.records).toHaveLength(2);
    expect(t.stderr()).toContain("produced 2 records in 2 requests");
  });

  it("10. produce sends pending lines on Ctrl+C", async () => {
    async function* endless() {
      yield utf8Encode("a\nb\n");
      await new Promise(() => {});
    }
    const t = cli({ stdinStream: endless() });
    const run = t.run("produce");
    await vi.advanceTimersByTimeAsync(100); // before the idle flush
    t.interrupt();
    await until(() => run.done);
    expect(run.code).toBe(0);
    expect(t.fake.records).toHaveLength(2);
    expect(t.stderr()).toContain("(interrupted)");
  });

  it("10. produce rejects oversized input without reading it all", async () => {
    let chunksRead = 0;
    async function* huge() {
      for (let i = 0; i < 1_000; i++) {
        chunksRead++;
        yield new Uint8Array(65_536).fill(0x61); // no newline: one ever-growing record
      }
    }
    const t = cli({ stdinStream: huge() });
    const run = t.run("produce");
    await until(() => run.done);
    expect(run.code).toBe(2);
    expect(t.stderr()).toContain("over 1 MB");
    expect(chunksRead).toBeLessThan(30);
    expect(t.fake.requests.filter((r) => r.endpoint === "produce")).toHaveLength(0);

    const raw = cli({ stdin: new Uint8Array(1_100_000) });
    const run2 = raw.run("produce", "--raw");
    await until(() => run2.done);
    expect(run2.code).toBe(2);
    expect(raw.stderr()).toContain("--raw input exceeds 1 MB");
  });

  it("14. replay --until skips out-of-order records instead of stopping", async () => {
    const t = cli();
    const now = Date.now();
    const at = (minutesAgo: number, text: string) =>
      t.fake.records.push({ timestamp_ms: now - minutesAgo * 60_000, content: btoa(text) });
    at(50, "in window 1");
    at(5, "after --until");
    at(40, "in window 2 (out of order)");
    const run = t.run("replay", "--since", "1h", "--until", "10m");
    await until(() => run.done);
    expect(t.out.map((l) => l.trim().split("  ").at(-1))).toEqual([
      "in window 1",
      "in window 2 (out of order)",
    ]);
  });

  it("14. replay rejects --since later than --until", async () => {
    const t = cli();
    const run = t.run("replay", "--since", "10m", "--until", "1h");
    await until(() => run.done);
    expect(run.code).toBe(2);
    expect(t.stderr()).toContain("--since must be earlier than --until");
  });
});

describe("cli edge cases, round two", () => {
  it("3. a token with a trailing newline is a usage error, not endless retries", async () => {
    const t = cli();
    t.io.env.CLOUDFLARE_API_TOKEN = `${t.fake.token}\n`;
    const run = t.run("subs", "ls");
    await until(() => run.done);
    expect(run.code).toBe(2);
    expect(t.stderr()).toContain("trailing newline");
    expect(t.stderr()).not.toContain(t.fake.token);
  });

  it("4. tail escapes terminal control characters in content and headers", async () => {
    const t = cli();
    const run = t.run("tail", "--headers", "--limit", "1");
    await until(() => tempSubs(t.fake, "tail").length === 1);
    await t.fake.fetch("https://fake.k2.test/produce", {
      method: "POST",
      headers: { authorization: `Bearer ${t.fake.token}`, "content-type": "application/json" },
      body: JSON.stringify({
        records: [
          {
            content: btoa("hi\u001b]0;PWNED\u0007\u001b[2K\rfake$ \nline2"),
            headers: { "x\u001b[31m": "v\u001b[0m" },
          },
        ],
      }),
    });
    await until(() => run.done);
    const line = t.stdout();
    // biome-ignore lint/suspicious/noControlCharactersInRegex: asserting no control characters remain
    expect(line).not.toMatch(/[\u0000-\u0008\u000b-\u001f\u007f]/);
    expect(line.trimEnd().split("\n")).toHaveLength(1); // one line per record
    expect(line).toContain("\\u001b]0;PWNED\\u0007");
    expect(line).toContain("\\n");
  });

  it("4. --json output also escapes C1 and bidi-override characters", () => {
    const record = decodeRecord({
      timestamp_ms: 0,
      content: toBase64(utf8Encode("a\u009b31mb\u202ec")),
    });
    const json = formatRecord(record, { json: true });
    expect(json).not.toMatch(/[\u0080-\u009f\u202a-\u202e]/);
    expect(JSON.parse(json).content).toBe("a\u009b31mb\u202ec"); // still valid, same data
  });

  it("5. produce exits when a send fails, even while stdin is idle", async () => {
    async function* idleAfterOneLine() {
      yield utf8Encode("line 1\n");
      await new Promise(() => {}); // like tail -f with no new output
    }
    const t = cli({ stdinStream: idleAfterOneLine() });
    t.fake.fail("produce", 10209, 5);
    const run = t.run("produce");
    await until(() => run.done, 5_000);
    expect(run.code).toBe(1);
    expect(t.stderr()).toContain("K2 10209");
    expect(t.stderr()).toContain("produced 0 records before the error");
  });

  it("9. subs rm --older-than only deletes stale subscriptions, and warns about recent ones", async () => {
    const t = cli();
    t.fake.addSubscription("k2kit-tail-old111");
    await vi.advanceTimersByTimeAsync(2 * 3_600_000);
    t.fake.addSubscription("k2kit-tail-new222");
    const run = t.run("subs", "rm", "--prefix", "k2kit-", "--older-than", "1h", "--yes");
    await until(() => run.done);
    expect(run.code).toBe(0);
    expect(t.fake.subscriptionNames()).toEqual(["k2kit-tail-new222"]);

    const t2 = cli({ fake: t.fake, confirm: false });
    const run2 = t2.run("subs", "rm", "--prefix", "k2kit-");
    await until(() => run2.done);
    expect(t2.stderr()).toContain("may belong to running tail/replay sessions");
    expect(t2.stderr()).toContain("--older-than 1h");
  });

  it("9. the capacity hint suggests --older-than", async () => {
    const t = cli();
    for (let i = 0; i < 100; i++) t.fake.addSubscription(`s${i}`);
    const run = t.run("tail");
    await until(() => run.done);
    expect(t.stderr()).toContain("subs rm --prefix k2kit- --older-than 1h");
  });

  it("11. an oversized line reports how many earlier records were already sent", async () => {
    const big = "x".repeat(1_100_000);
    const t = cli({ stdin: `a\nb\n${big}\n` });
    const run = t.run("produce");
    await until(() => run.done);
    expect(run.code).toBe(2);
    expect(t.fake.records).toHaveLength(2);
    expect(t.stderr()).toContain("produced 2 records before the error");
  });

  it("12. a header named __proto__ is kept, not silently dropped", async () => {
    expect(Object.keys(parseHeaders(["__proto__=x", "constructor=y"]))).toEqual([
      "__proto__",
      "constructor",
    ]);
    const t = cli({ stdin: "a\n" });
    const run = t.run("produce", "--header", "__proto__=x");
    await until(() => run.done);
    expect(Object.keys(t.fake.records[0]?.headers ?? {})).toContain("__proto__");
  });

  it("14. ISO date-times need a timezone; a bare date is UTC midnight", () => {
    const now = Date.UTC(2026, 9, 11);
    expect(() => parseTime("2026-10-11T10:00", now, "--since")).toThrow(/needs a timezone/);
    expect(parseTime("2026-10-11T10:00Z", now, "--since")).toBe(Date.UTC(2026, 9, 11, 10));
    expect(parseTime("2026-10-11T10:00+05:30", now, "--since")).toBe(Date.UTC(2026, 9, 11, 4, 30));
    expect(parseTime("2026-10-11", now, "--since")).toBe(Date.UTC(2026, 9, 11));
  });

  it("times that Date.parse would guess at (as local time, or a year like 2001) are rejected", () => {
    const now = Date.UTC(2026, 9, 11);
    // A bare number: Date.parse("10") is a date in 2001, which would replay the whole stream.
    expect(() => parseTime("10", now, "--since")).toThrow(/needs a unit, e\.g\. 10m/);
    // A space instead of T, without a timezone, is local time too.
    expect(() => parseTime("2026-10-11 10:00", now, "--since")).toThrow(/needs a timezone/);
    for (const value of ["Oct 11 2026", "2026/10/11 10:00Z", "2026-13-01", "2026-10-11T10:00+05"]) {
      expect(() => parseTime(value, now, "--since")).toThrow(UsageError);
    }
    // ISO variants with a timezone are read exactly, on every engine.
    const tenUtc = Date.UTC(2026, 9, 11, 10);
    expect(parseTime("2026-10-11 10:00Z", now, "--since")).toBe(tenUtc);
    expect(parseTime("2026-10-11t10:00z", now, "--since")).toBe(tenUtc);
    expect(parseTime(" 2026-10-11T15:30+0530 ", now, "--since")).toBe(tenUtc);
    expect(parseTime("2026-10-11T10:00:00.123456Z", now, "--since")).toBe(tenUtc + 123);
    expect(parseTime("2026-10-11T10:00:00.5Z", now, "--since")).toBe(tenUtc + 500);
    expect(parseTime("2026-10", now, "--since")).toBe(Date.UTC(2026, 9, 1));
    expect(() => parseTime("2026", now, "--since")).toThrow(/needs a unit.*YYYY-MM-DD/);
  });

  it("16. produce explains itself when stdin is a terminal", async () => {
    const t = cli({ stdin: "a\n", stdinIsTTY: true });
    const run = t.run("produce");
    await until(() => run.done);
    expect(t.stderr()).toContain("press Ctrl+D to finish");
  });

  it("17. an interrupted --raw produce says so", async () => {
    async function* never() {
      yield* [];
      await new Promise(() => {});
    }
    const t = cli({ stdinStream: never() });
    const run = t.run("produce", "--raw");
    await vi.advanceTimersByTimeAsync(10);
    t.interrupt();
    await until(() => run.done);
    expect(t.stderr()).toContain("interrupted; nothing produced");
  });

  it("18. tail waits for a slow stdout instead of buffering without limit", async () => {
    let release = () => {};
    const drained = new Promise<void>((r) => {
      release = r;
    });
    const t = cli({ stdout: () => drained });
    const run = t.run("tail", "--limit", "2");
    await until(() => tempSubs(t.fake, "tail").length === 1);
    t.fake.append("a", "b");
    await until(() => t.out.length === 1);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(t.out).toHaveLength(1); // blocked on the first write
    expect(t.fake.requests.filter((r) => r.endpoint === "ack")).toHaveLength(0);
    release();
    await until(() => run.done);
    expect(t.out).toHaveLength(2);
  });
});

describe("cli edge cases, round three", () => {
  it.each(["tail", "replay"])(
    "%s: Ctrl+C while the temporary subscription is being created doesn't leak it",
    async (command) => {
      const t = cli({ createDelayMs: 2_000 });
      const run = t.run(command);
      await vi.advanceTimersByTimeAsync(500); // creation request in flight
      t.interrupt();
      await until(() => run.done);
      expect(run.code).toBe(0);
      expect(t.fake.subscriptionNames()).toEqual([]);
      expect(t.fake.requests.filter((r) => r.endpoint === "consume")).toHaveLength(0);
    },
  );

  it("names the temporary subscription for a forced exit, and clears it once deleted", async () => {
    const t = cli();
    const run = t.run("tail", "--limit", "1");
    await until(() => tempSubs(t.fake, "tail").length === 1);
    const name = tempSubs(t.fake, "tail")[0] as string;
    expect(t.notes.at(-1)).toContain(`k2kit subs rm ${name}`);
    t.fake.append("x");
    await until(() => run.done);
    expect(t.notes.at(-1)).toBeUndefined();
  });

  it("subs rm with an unknown name says so instead of K2's vague 10204", async () => {
    const t = cli();
    const run = t.run("subs", "rm", "no-such-subscription");
    await until(() => run.done);
    expect(run.code).toBe(1);
    expect(t.stderr()).toContain('no subscription named "no-such-subscription"');
    expect(t.fake.requests.filter((r) => r.endpoint === "deleteSubscription")).toHaveLength(0);
  });

  it("subs rm accepts an id pasted in uppercase", async () => {
    const t = cli();
    const id = t.fake.addSubscription("orders");
    const run = t.run("subs", "rm", id.toUpperCase());
    await until(() => run.done);
    expect(run.code).toBe(0);
    expect(t.fake.subscriptionNames()).toEqual([]);
  });
});

describe("cli: creating the temporary subscription when the response is lost", () => {
  const isCreate = (input: RequestInfo | URL, init?: RequestInit) =>
    init?.method === "POST" && String(input).endsWith("/subscriptions");
  const isLookup = (input: RequestInfo | URL) => String(input).includes("/subscriptions?name=");

  /** K2 applies the create, but the client never hears back (dropped connection). */
  const loseCreateResponses = (times: number) => (inner: typeof fetch) => {
    let lost = 0;
    return (async (input, init) => {
      const res = await inner(input, init);
      if (isCreate(input, init) && lost < times) {
        lost++;
        throw new TypeError("fetch failed");
      }
      return res;
    }) as typeof fetch;
  };

  it("retries (creation is idempotent) and carries on when a response is lost once", async () => {
    const t = cli({ wrapFetch: loseCreateResponses(1) });
    const run = t.run("tail", "--limit", "1");
    await until(() => tempSubs(t.fake, "tail").length === 1);
    t.fake.append("x");
    await until(() => run.done);
    expect(run.code).toBe(0);
    expect(t.out).toHaveLength(1);
    expect(t.fake.subscriptionNames()).toEqual([]);
  });

  it("finds and deletes the subscription when every create response is lost", async () => {
    const t = cli({ wrapFetch: loseCreateResponses(99) });
    const run = t.run("tail");
    await until(() => run.done);
    expect(run.code).toBe(1);
    expect(t.fake.subscriptionNames()).toEqual([]);
    expect(t.stderr()).not.toContain("may exist");
  });

  it("names the subscription when it can't even check whether it exists", async () => {
    const t = cli({
      wrapFetch: (inner) => {
        const lossy = loseCreateResponses(99)(inner);
        return (async (input, init) => {
          if (isLookup(input)) throw new TypeError("fetch failed");
          return lossy(input, init);
        }) as typeof fetch;
      },
    });
    const run = t.run("tail");
    await until(() => run.done);
    expect(run.code).toBe(1);
    const name = tempSubs(t.fake, "tail")[0] as string;
    expect(name).toBeDefined();
    expect(t.stderr()).toContain(`k2kit subs rm ${name}`);
  });

  it("doesn't look anything up when K2 answered with an error (nothing was created)", async () => {
    const t = cli();
    t.fake.fail("createSubscription", 10210);
    const run = t.run("tail");
    await until(() => run.done);
    expect(run.code).toBe(1);
    expect(t.stderr()).toContain("K2 10210");
    expect(t.stderr()).not.toContain("may exist");
    expect(t.fake.requests.filter((r) => r.endpoint === "listSubscriptions")).toHaveLength(1); // capacity check only
  });
});

describe("cli: subs rm with a name K2 wouldn't allow", () => {
  it("says no such subscription instead of K2's vague 10204", async () => {
    const t = cli();
    const run = t.run("subs", "rm", "bad name");
    await until(() => run.done);
    expect(run.code).toBe(1);
    expect(t.stderr()).toContain('no subscription named "bad name"');
    expect(t.stderr()).not.toContain("10204");
  });
});

describe("cli: --header values are checked against K2's limits", () => {
  it("rejects a value over 8 KiB as a usage error, before sending", async () => {
    const t = cli({ stdin: "a\n" });
    const run = t.run("produce", "--header", `big=${"v".repeat(8_193)}`);
    await until(() => run.done);
    expect(run.code).toBe(2);
    expect(t.stderr()).toContain("value exceeds 8192");
    expect(t.fake.requests.filter((r) => r.endpoint === "produce")).toHaveLength(0);
  });
});
