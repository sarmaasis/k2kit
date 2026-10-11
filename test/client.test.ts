import { describe, expect, it } from "vitest";
import { K2Client, MAX_PRODUCE_REQUEST_BYTES } from "../src/client.js";
import { K2Error } from "../src/errors.js";
import { ID_HEADER } from "../src/record.js";
import { FakeK2 } from "./fake-k2.js";

const STREAM = "0123456789abcdef0123456789abcdef";

function setup() {
  const fake = new FakeK2();
  const client = new K2Client({
    streamId: STREAM,
    token: fake.token,
    endpoint: "https://fake.k2.test",
    fetch: fake.fetch,
  });
  return { fake, client };
}

async function caught(p: Promise<unknown>): Promise<K2Error> {
  try {
    await p;
  } catch (err) {
    if (err instanceof K2Error) return err;
    throw err;
  }
  throw new Error("expected a K2Error");
}

describe("K2Client", () => {
  it("defaults the endpoint to the stream host", () => {
    const client = new K2Client({ streamId: STREAM, token: "t" });
    expect(client.endpoint).toBe(`https://${STREAM}.k2.cloudflarestorage.com`);
  });

  it("does not expose the token as an enumerable property", () => {
    const client = new K2Client({ streamId: STREAM, token: "super-secret" });
    expect(JSON.stringify(client)).not.toContain("super-secret");
    expect(Object.values(client)).not.toContain("super-secret");
  });

  it("round-trips binary, UTF-8 and JSON content through produce and consume", async () => {
    const { client } = setup();
    const binary = new Uint8Array([0, 1, 2, 250, 251, 252, 253, 254, 255]);
    await client.produce([
      { content: binary },
      { content: "héllo 🌍" },
      { content: { order: 42, tags: ["a", "ü"] } },
    ]);
    const subId = await client.ensureSubscription("rt", "earliest");
    const batch = await client.consume(subId, "w1", 10);
    expect(batch?.records).toHaveLength(3);
    const [a, b, c] = batch?.records ?? [];
    expect(a?.bytes).toEqual(binary);
    expect(b?.text()).toBe("héllo 🌍");
    expect(c?.json()).toEqual({ order: 42, tags: ["a", "ü"] });
  });

  it("produces only standard padded base64 that K2 accepts", async () => {
    const { client, fake } = setup();
    // Sizes 0..5 exercise every padding case; 0xfb/0xff would become -/_ in URL-safe base64.
    const inputs = [0, 1, 2, 3, 4, 5].map((n) => new Uint8Array(n).fill(0xfb));
    inputs.push(new Uint8Array([0xff, 0xfe, 0xfb]));
    await client.produce(inputs.map((content) => ({ content })));
    expect(fake.records).toHaveLength(inputs.length);
  });

  it("adds a k2kit-id header by default, with opt-out and no overwrite", async () => {
    const { client, fake } = setup();
    await client.produce([{ content: "a" }, { content: "b", headers: { [ID_HEADER]: "mine" } }]);
    await client.produce([{ content: "c" }], { idHeader: false });
    expect(fake.records[0]?.headers?.[ID_HEADER]).toMatch(/^[0-9a-f-]{36}$/);
    expect(fake.records[1]?.headers?.[ID_HEADER]).toBe("mine");
    expect(fake.records[2]?.headers).toBeUndefined();
  });

  it("gzips produce bodies on request", async () => {
    const { client, fake } = setup();
    await client.produce([{ content: "x".repeat(10_000) }], { gzip: true });
    expect(fake.requests.at(-1)?.headers["content-encoding"]).toBe("gzip");
    expect(fake.records).toHaveLength(1);
  });

  it("returns null for an empty consume", async () => {
    const { client } = setup();
    const subId = await client.ensureSubscription("empty", "latest");
    expect(await client.consume(subId, "w1", 10)).toBeNull();
  });

  it("creates subscriptions idempotently and maps 10201 to a clear error", async () => {
    const { client } = setup();
    const a = await client.ensureSubscription("orders", "earliest");
    const b = await client.ensureSubscription("ORDERS", "earliest");
    expect(b).toBe(a);
    const err = await caught(client.ensureSubscription("orders", "latest"));
    expect(err.code).toBe(10201);
    expect(err.message).toContain("different start_at");
  });

  it("lists, finds, gets and deletes subscriptions", async () => {
    const { client } = setup();
    const id1 = await client.ensureSubscription("one", "earliest");
    await client.ensureSubscription("two", "latest");
    expect((await client.listSubscriptions()).map((s) => s.name)).toEqual(["one", "two"]);
    expect((await client.findSubscription("TWO"))?.start_at.type).toBe("latest");
    expect(await client.findSubscription("nope")).toBeNull();
    expect((await client.getSubscription(id1)).name).toBe("one");
    await client.deleteSubscription(id1);
    expect((await caught(client.getSubscription(id1))).code).toBe(10215);
  });

  it("supports ack, nack and extend", async () => {
    const { client, fake } = setup();
    fake.append({ n: 1 });
    const subId = await client.ensureSubscription("s", "earliest");
    const batch = await client.consume(subId, "w1", 10);
    if (!batch) throw new Error("expected a batch");
    expect(await client.extend(subId, batch.id, "w1")).toBeGreaterThanOrEqual(batch.leasedUntilMs);
    expect((await caught(client.extend(subId, batch.id, "w2"))).code).toBe(10218);
    await client.nack(subId, batch.id, "w1");
    const again = await client.consume(subId, "w2", 10);
    expect(again?.id).not.toBe(batch.id);
    if (!again) throw new Error("expected redelivery");
    await client.ack(subId, again.id, "w2");
    await client.ack(subId, again.id, "w2"); // idempotent
    await client.ack(subId, "unknown-batch", "w2");
    expect(await client.consume(subId, "w1", 10)).toBeNull();
  });

  const CONSUME_CODES: [code: number, status: number, retryable: boolean][] = [
    [10200, 404, false],
    [10201, 422, false],
    [10204, 400, false],
    [10205, 415, false],
    [10208, 401, false],
    [10209, 401, false],
    [10210, 403, false],
    [10211, 503, true],
    [10213, 500, false],
    [10214, 503, true],
    [10215, 404, false],
    [10216, 429, true],
    [10217, 409, true],
    [10218, 409, false],
    [10219, 422, false],
  ];

  it.each(CONSUME_CODES)(
    "maps consume error %i to K2Error(status %i, retryable %s)",
    async (code, status, retryable) => {
      const { client, fake } = setup();
      const subId = fake.addSubscription("s");
      fake.fail("consume", code, 1, "boom");
      const err = await caught(client.consume(subId, "w1", 10));
      expect(err).toMatchObject({ code, status, retryable, message: "boom" });
    },
  );

  it("uses the produce response's retryable field; 10212 is not retryable", async () => {
    const { client, fake } = setup();
    fake.fail("produce", 10211);
    expect(await caught(client.produce([{ content: "a" }]))).toMatchObject({
      code: 10211,
      status: 503,
      retryable: true,
    });
    fake.fail("produce", 10212);
    expect(await caught(client.produce([{ content: "a" }]))).toMatchObject({
      code: 10212,
      status: 503,
      retryable: false,
    });
  });

  it("surfaces real auth and validation errors from the fake", async () => {
    const { fake } = setup();
    const bad = new K2Client({
      streamId: STREAM,
      token: "wrong",
      endpoint: "https://fake.k2.test",
      fetch: fake.fetch,
    });
    expect((await caught(bad.listSubscriptions())).code).toBe(10209);
    const none = new K2Client({
      streamId: STREAM,
      endpoint: "https://fake.k2.test",
      fetch: fake.fetch,
    });
    expect((await caught(none.listSubscriptions())).code).toBe(10208);
  });

  it("maps a non-JSON gateway error to a retryable code-0 K2Error", async () => {
    const client = new K2Client({
      streamId: STREAM,
      token: "t",
      fetch: async () => new Response("<html>bad gateway</html>", { status: 502 }),
    });
    expect(await caught(client.listSubscriptions())).toMatchObject({
      code: 0,
      status: 502,
      retryable: true,
    });
  });

  it("never puts the token in error messages", async () => {
    const { fake } = setup();
    const client = new K2Client({
      streamId: STREAM,
      token: "tok-SECRET-123",
      endpoint: "https://fake.k2.test",
      fetch: fake.fetch,
    });
    const err = await caught(client.listSubscriptions());
    expect(`${err.message} ${err.stack}`).not.toContain("tok-SECRET-123");
  });

  describe("produceBatched", () => {
    it("splits into requests under 5 MB and stores every record once", async () => {
      const { client, fake } = setup();
      const records = Array.from({ length: 30 }, (_, i) => ({
        content: `${i}:${"x".repeat(400_000)}`,
      }));
      const progress: number[] = [];
      const result = await client.produceBatched(records, {
        onProgress: (p) => progress.push(p.records),
      });
      const requests = fake.requests.filter((r) => r.endpoint === "produce");
      expect(result).toEqual({ records: 30, requests: requests.length });
      expect(requests.length).toBeGreaterThanOrEqual(3);
      for (const r of requests) {
        expect(new TextEncoder().encode(JSON.stringify(r.body)).byteLength).toBeLessThan(
          MAX_PRODUCE_REQUEST_BYTES,
        );
      }
      expect(progress.at(-1)).toBe(30);
      expect(fake.records).toHaveLength(30);
    });

    it("retries retryable errors with the same k2kit-id headers", async () => {
      const { client, fake } = setup();
      fake.fail("produce", 10211, 2);
      await client.produceBatched([{ content: "a" }], { retryBackoff: { minMs: 1, maxMs: 1 } });
      const bodies = fake.requests
        .filter((r) => r.endpoint === "produce")
        .map((r) => (r.body as { records: { headers: { [k: string]: string } }[] }).records[0]);
      expect(bodies).toHaveLength(3);
      expect(new Set(bodies.map((b) => b?.headers[ID_HEADER])).size).toBe(1);
      expect(fake.records).toHaveLength(1);
    });

    it("does not retry 10212 or past maxAttempts", async () => {
      const { client, fake } = setup();
      fake.fail("produce", 10212);
      expect((await caught(client.produceBatched([{ content: "a" }]))).code).toBe(10212);
      fake.fail("produce", 10211, 5);
      const err = await caught(
        client.produceBatched([{ content: "a" }], {
          maxAttempts: 2,
          retryBackoff: { minMs: 1, maxMs: 1 },
        }),
      );
      expect(err.code).toBe(10211);
      expect(fake.requests.filter((r) => r.endpoint === "produce")).toHaveLength(3);
    });

    it("rejects records over 1 MB before sending anything", async () => {
      const { client, fake } = setup();
      await expect(
        client.produceBatched([{ content: "ok" }, { content: new Uint8Array(1_000_001) }]),
      ).rejects.toThrow(RangeError);
      expect(fake.requests).toHaveLength(0);
    });
  });
});
