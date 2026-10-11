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
  io: CliIo;
  out: string[];
  err: string[];
  /** Simulates Ctrl+C. */
  interrupt(): void;
  stdout(): string;
  stderr(): string;
  run(...argv: string[]): { code?: number; done: boolean };
}

function cli(opts: { stdin?: Uint8Array | string; confirm?: boolean | null; fake?: FakeK2 } = {}) {
  const fake = opts.fake ?? new FakeK2();
  const out: string[] = [];
  const err: string[] = [];
  const handlers = new Set<(s: string) => void>();
  const stdin = typeof opts.stdin === "string" ? utf8Encode(opts.stdin) : opts.stdin;
  const io: CliIo = {
    env: { K2_STREAM_ID: STREAM, CLOUDFLARE_API_TOKEN: fake.token },
    stdout: (t) => void out.push(t),
    stderr: (t) => void err.push(t),
    readStdin: async () => stdin ?? new Uint8Array(),
    confirm: async () => opts.confirm ?? null,
    onSignal: (h) => {
      handlers.add(h);
      return () => void handlers.delete(h);
    },
    now: () => Date.now(),
    fetch: fake.fetch,
  };
  const t: TestCli = {
    fake,
    io,
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
    expect(lines[2]).toMatch(/^beta\s+sub_\d+\s+latest\s+\d{4}-/);

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
