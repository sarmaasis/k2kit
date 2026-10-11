import { describe, expect, it } from "vitest";
import { K2Client } from "../src/client.js";
import { Consumer } from "../src/consumer.js";
import { DEAD_LETTER_HEADERS, deadLetterToStream } from "../src/dead-letter.js";
import type { Logger } from "../src/types.js";

const streamId = process.env.K2_STREAM_ID;
const token = process.env.CLOUDFLARE_API_TOKEN;
const enabled = Boolean(streamId && token);

const quiet: Logger = { debug() {}, info() {}, warn: console.warn, error: console.error };

function uniqueName(kind: string): string {
  return `k2kit-it-${kind}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
}

/** Creates a `latest` subscription, runs `fn`, and always deletes the subscription. */
async function withSubscription(
  kind: string,
  fn: (client: K2Client, subId: string) => Promise<void>,
): Promise<void> {
  const client = new K2Client({ streamId: streamId as string, token: token as string });
  const subId = await client.ensureSubscription(uniqueName(kind), "latest");
  try {
    await fn(client, subId);
  } finally {
    await client.deleteSubscription(subId);
  }
}

function waitFor(cond: () => boolean, timeoutMs: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const timer = setInterval(() => {
      if (cond()) {
        clearInterval(timer);
        resolve();
      } else if (Date.now() - started > timeoutMs) {
        clearInterval(timer);
        reject(new Error(`timed out after ${timeoutMs} ms`));
      }
    }, 100);
  });
}

describe.skipIf(!enabled)("integration (real K2)", () => {
  it("consumes 50 produced records with concurrency 2", { timeout: 180_000 }, async () => {
    await withSubscription("basic", async (client, subId) => {
      const runId = crypto.randomUUID();
      await client.produce(Array.from({ length: 50 }, (_, n) => ({ content: { runId, n } })));

      const seen = new Set<number>();
      const workers = new Set<string>();
      const consumer = new Consumer({
        streamId: streamId as string,
        token: token as string,
        subscription: { id: subId },
        concurrency: 2,
        maxRecords: 10,
        workerIdPrefix: uniqueName("w"),
        logger: quiet,
      });
      const run = consumer.run((batch, ctx) => {
        workers.add(ctx.workerId);
        for (const r of batch.records) {
          const event = r.json<{ runId?: string; n?: number }>();
          if (event.runId === runId && typeof event.n === "number") seen.add(event.n);
        }
      });
      try {
        await waitFor(() => seen.size === 50, 120_000);
      } finally {
        await consumer.stop();
        await run;
      }
      expect(seen.size).toBe(50); // duplicates are allowed; every record must arrive
    });
  });

  it("redelivers a batch after the handler throws once", { timeout: 180_000 }, async () => {
    await withSubscription("nack", async (client, subId) => {
      const runId = crypto.randomUUID();
      await client.produce([{ content: { runId, n: 0 } }]);

      const deliveries: string[] = [];
      const consumer = new Consumer({
        streamId: streamId as string,
        token: token as string,
        subscription: { id: subId },
        workerIdPrefix: uniqueName("w"),
        logger: quiet,
        onError: () => {},
        retryBackoff: { minMs: 100, maxMs: 1_000 },
      });
      const run = consumer.run((batch) => {
        const mine = batch.records.some((r) => r.json<{ runId?: string }>().runId === runId);
        if (!mine) return;
        deliveries.push(batch.id);
        if (deliveries.length === 1) throw new Error("fail first delivery");
      });
      try {
        await waitFor(() => deliveries.length >= 2, 120_000);
      } finally {
        await consumer.stop();
        await run;
      }
      expect(deliveries[1]).not.toBe(deliveries[0]);
    });
  });

  it("accepts the k2kit-id header on produce and returns it on consume", {
    timeout: 60_000,
  }, async () => {
    await withSubscription("hdr", async (client, subId) => {
      await client.produce([{ content: "hdr", headers: { "event-type": "it" } }]);
      let batch = null;
      for (let i = 0; i < 20 && !batch; i++) {
        batch = await client.consume(subId, uniqueName("w"), 10);
        if (!batch) await new Promise((r) => setTimeout(r, 500));
      }
      expect(batch).not.toBeNull();
      const record = batch?.records[0];
      expect(record?.headers["k2kit-id"]).toMatch(/^[0-9a-f-]{36}$/);
      expect(record?.headers["event-type"]).toBe("it");
    });
  });

  it("dead-letters a poison record to a stream and acks the rest", {
    timeout: 180_000,
  }, async () => {
    await withSubscription("dlq", async (client, subId) => {
      // Observer subscription, created before producing, to read the dead letter back.
      const observer = await client.ensureSubscription(uniqueName("dlq-obs"), "latest");
      try {
        const runId = crypto.randomUUID();
        await client.produce([0, 1, 2].map((n) => ({ content: { runId, n } })));

        const handled: number[] = [];
        let deadLettered = 0;
        const consumer = new Consumer({
          streamId: streamId as string,
          token: token as string,
          subscription: { id: subId },
          workerIdPrefix: uniqueName("w"),
          logger: quiet,
          onError: () => {},
          retryBackoff: { minMs: 100, maxMs: 500 },
          deadLetter: { afterAttempts: 2, send: deadLetterToStream(client) },
        }).on("deadLetter", (e) => {
          deadLettered += e.deadLettered;
        });
        const run = consumer.run((batch) => {
          for (const r of batch.records) {
            if (r.headers[DEAD_LETTER_HEADERS.error] !== undefined) continue; // our own DLQ output
            const e = r.json<{ runId?: string; n?: number }>();
            if (e.runId !== runId) continue;
            if (e.n === 1) throw new Error("poison record");
            handled.push(e.n as number);
          }
        });
        try {
          await waitFor(() => deadLettered === 1 && consumer.stats().acks >= 1, 120_000);
        } finally {
          await consumer.stop();
          await run;
        }
        expect(new Set(handled)).toEqual(new Set([0, 2]));

        let letter = null;
        for (let i = 0; i < 40 && !letter; i++) {
          const batch = await client.consume(observer, uniqueName("obs"), 100);
          letter =
            batch?.records.find((r) => r.headers[DEAD_LETTER_HEADERS.error] !== undefined) ?? null;
          if (batch) await client.ack(observer, batch.id, "obs");
          if (!letter) await new Promise((r) => setTimeout(r, 500));
        }
        expect(letter?.json()).toEqual({ runId, n: 1 });
        expect(letter?.headers[DEAD_LETTER_HEADERS.error]).toBe("Error: poison record");
        expect(letter?.headers[DEAD_LETTER_HEADERS.attempts]).toBe("2");
      } finally {
        await client.deleteSubscription(observer);
      }
    });
  });
});
