// Run with: K2_STREAM_ID=... CLOUDFLARE_API_TOKEN=... node --experimental-strip-types examples/basic-consumer.ts
import { Consumer, MemoryDedupeStore } from "k2kit";

interface OrderEvent {
  order_id: number;
  status: string;
}

async function handleOrder(event: OrderEvent, opts: { signal: AbortSignal }): Promise<void> {
  if (opts.signal.aborted) throw opts.signal.reason;
  console.log(`order ${event.order_id} is ${event.status}`);
}

const streamId = process.env.K2_STREAM_ID;
const token = process.env.CLOUDFLARE_API_TOKEN;
if (!streamId || !token) throw new Error("set K2_STREAM_ID and CLOUDFLARE_API_TOKEN");

const consumer = new Consumer({
  streamId,
  token,
  subscription: { name: "orders-processor", startAt: "earliest" },
  concurrency: 4,
  maxRecords: 100,
  dedupe: new MemoryDedupeStore(),
  // In Kubernetes, the pod name is stable across restarts (so leases are recovered) and unique
  // per live process. Elsewhere, keep the default (host-pid-random): a fixed fallback would let
  // two local copies share worker ids and act on each other's leases.
  ...(process.env.HOSTNAME ? { workerIdPrefix: process.env.HOSTNAME } : {}),
});

consumer.on("ack", (e) => console.error(`acked ${e.records} records (${e.workerId})`));
process.on("SIGINT", () => void consumer.stop());
process.on("SIGTERM", () => void consumer.stop());

await consumer.run(async (batch, ctx) => {
  for (const record of batch.records) {
    await handleOrder(record.json<OrderEvent>(), { signal: ctx.signal });
  }
});
