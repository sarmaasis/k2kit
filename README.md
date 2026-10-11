# k2kit

A consumer runtime and CLI for [Cloudflare K2](https://developers.cloudflare.com/k2/) streams.

K2 consumers speak raw HTTP: lease a batch, process it, then ack, nack or extend the lease. `k2kit` replaces the hand-written `while (true)` loop with `consumer.run(handler)`, which adds backoff, lease heartbeats, retry handling, concurrency, optional dedupe and graceful shutdown. It also ships a CLI to tail, replay and inspect streams.

- Zero runtime dependencies. Uses global `fetch`; tested on Node 20+, Bun and Deno 2 (Deno needs `--allow-net`, `--allow-env`, and `--allow-sys=hostname` for the default worker id).
- TypeScript types included; ESM and CommonJS builds.
- K2 is in public beta and needs the Workers Paid plan.

> Not affiliated with or endorsed by Cloudflare.

## Install

```sh
pnpm add k2kit        # or: npm install k2kit / bun add k2kit
```

## Quickstart

You need a stream id and an API token with the **K2 Consume** permission (plus **K2 Produce** to produce when the stream's HTTP input has authentication on).

```js
// consumer.mjs
import { Consumer } from "k2kit";

const consumer = new Consumer({
  streamId: process.env.K2_STREAM_ID,
  token: process.env.CLOUDFLARE_API_TOKEN,
  subscription: { name: "quickstart", startAt: "earliest" },
  concurrency: 4,
  maxRecords: 100,
});

process.on("SIGINT", () => consumer.stop());
process.on("SIGTERM", () => consumer.stop());

await consumer.run(async (batch, ctx) => {
  for (const record of batch.records) {
    console.log(ctx.workerId, new Date(record.timestampMs).toISOString(), record.text());
  }
  // return normally → ack; throw → nack (K2 redelivers the records)
});
```

```sh
export K2_STREAM_ID=<32-char stream id>
export CLOUDFLARE_API_TOKEN=<token>
node consumer.mjs
```

In another terminal, send something to it:

```sh
seq 1 5 | jq -c '{n: .}' | npx k2kit produce
```

A TypeScript version lives in [`examples/basic-consumer.ts`](examples/basic-consumer.ts).

## How the consumer works

Each of the `concurrency` slots runs its own loop with its own `worker_id`:

1. Request a batch. If it's empty, back off (`pollBackoff`). While the stream is quiet, only one slot keeps polling; the others wait and wake as soon as it sees records.
2. Skip records already seen, if a `dedupe` store is set.
3. Call your handler. While it runs, extend the lease every `heartbeatMs` (2 minutes by default; leases last 5).
4. Handler returned → ack (retried until the lease expires). Handler threw → nack, back off, continue. With `deadLetter` set, a batch that keeps failing has its bad records diverted (see below).

Retryable K2 errors (`10211`, `10214`, `10216`, `10217`) and network errors are retried with `retryBackoff`. Any other K2 error is fatal: all slots stop and `run()` rejects. If `10216` keeps coming back, more workers are polling than K2's 128 leases per subscription allow (processes × `concurrency`); the consumer logs a warning every 5 minutes and counts it in `stats().leaseLimitHits`. If the lease is lost mid-handler (`10218`), `ctx.signal` aborts and the batch is left for K2 to redeliver.

### Options

| Option | Default | Notes |
| --- | --- | --- |
| `streamId` | required | 32-char lowercase hex |
| `token` | required | needs K2 Consume; never logged |
| `subscription` | required | `{ name, startAt: "earliest" \| "latest" }` (created if missing) or `{ id }` |
| `concurrency` | `1` | 1–128, one `worker_id` and lease per slot |
| `workerIdPrefix` | `${hostname}-${pid}` | worker ids are `${prefix}-${slot}` — see crash recovery |
| `maxRecords` | `100` | 1–10,000 per batch |
| `pollBackoff` | `{ minMs: 250, maxMs: 10_000 }` | after empty polls |
| `idlePolling` | `"shared"` | `"shared"`: one slot polls a quiet stream and wakes the others; `"every-slot"`: each slot polls on its own |
| `retryBackoff` | `{ minMs: 500, maxMs: 30_000, maxAttempts: Infinity }` | after retryable errors and failed batches |
| `heartbeatMs` | `120_000` | lease extend interval while a handler runs |
| `dedupe` | none | a `DedupeStore`: `new MemoryDedupeStore()` or `new RedisDedupeStore(…)` |
| `deadLetter` | none | `{ afterAttempts: 5, send }` — see dead-lettering |
| `onError` | logs one line | non-fatal errors: `(err, { phase, workerId, batchId?, records? })` |
| `logger` | stderr, `info` level | `{ debug, info, warn, error }` |

### Handler context

`handler(batch, ctx)` receives `batch.records`, where each record has `timestampMs`, `headers`, `bytes`, `text()`, `json()` and `key()`. `ctx` holds:

- `workerId`: the lease holder.
- `signal`: aborted when the lease is lost or `stop()` times out. Pass it to your own I/O.
- `attempt`: a best-effort, in-memory delivery count starting at 1: one more than the most failed (or lease-lost) deliveries of any record in the batch. Without `deadLetter`, the consumer logs a possible poison batch from attempt 5.

### Events

```js
consumer
  .on("batch", (e) => metrics.inc("batches", e.records))
  .on("ack", (e) => {})
  .on("nack", (e) => {})
  .on("deadLetter", (e) => metrics.inc("dead_letters", e.deadLettered))
  .on("error", (err, ctx) => {})
  .on("idle", ({ workerId }) => {});
```

### Dead-lettering

K2 can only ack or nack a whole batch, so one bad record would otherwise fail its batch forever. With `deadLetter` set, once a failing batch reaches `afterAttempts` (default 5), the consumer retries it **one record at a time**. Records that still fail go to `send`, and the batch is acked. If `send` throws, the batch is nacked and nothing is dropped.

```js
import { Consumer, K2Client, deadLetterToStream } from "k2kit";

const dlq = new K2Client({ streamId: process.env.K2_DLQ_STREAM_ID, token: process.env.CLOUDFLARE_API_TOKEN });

const consumer = new Consumer({
  // …
  deadLetter: { afterAttempts: 5, send: deadLetterToStream(dlq) },
});
```

`deadLetterToStream` writes each failed record to another K2 stream with its content and headers (including `k2kit-id`), plus `k2kit-dlq-error`, `k2kit-dlq-attempts` and `k2kit-dlq-source-timestamp-ms`. The token needs K2 Produce on that stream. Keep secrets and personal data out of your error messages, since they're stored in `k2kit-dlq-error`. You can also pass your own `send(letters, ctx)` to write to a database or alerting tool instead.

Records that succeed in the one-at-a-time pass may have been processed in the earlier failed batch attempts too, which is normal under at-least-once delivery.

### Health and stats

`consumer.stats()` returns a cheap snapshot for health checks and metrics: `state` (`idle`, `running`, `stopping`, `stopped`), `inFlight`, counts of `batches`, `records`, `acks`, `nacks`, `deadLettered`, `errors`, `idlePolls` and `leaseLimitHits`, and `lastBatchAt`, `lastAckAt` and `lastErrorAt` timestamps.

```js
// e.g. fail a liveness probe if nothing was acked for 10 minutes while there is traffic
const { state, lastAckAt, lastBatchAt } = consumer.stats();
```

### Shutdown

`await consumer.stop({ timeoutMs = 30_000 })` stops requesting batches and waits for in-flight handlers, which are then acked or nacked. After the timeout it aborts `ctx.signal`, waits for handlers to settle, and nacks the unfinished batches. The library never installs signal handlers; wire up `SIGINT`/`SIGTERM` yourself as in the quickstart, or pass an `AbortSignal`: `consumer.run(handler, { signal })` stops when it aborts.

## Delivery guarantees and limits

K2 delivers **at least once** per subscription, with no deduplication and no ordering across workers. Plan for duplicates.

- **Dedupe in one process.** `dedupe: new MemoryDedupeStore()` skips records already processed in this process (an LRU of 100,000 keys by default).
- **Dedupe across processes.** `RedisDedupeStore` shares seen keys between every process consuming a subscription, with one `MGET` and one pipelined write per batch. k2kit has no Redis dependency: pass a two-method adapter for the client you already use. [`examples/redis-dedupe.ts`](examples/redis-dedupe.ts) has adapters for ioredis, node-redis and Upstash.

  ```js
  import { RedisDedupeStore } from "k2kit";
  import { Redis } from "ioredis";

  const redis = new Redis(process.env.REDIS_URL);
  const dedupe = new RedisDedupeStore({
    namespace: "orders-processor", // one per subscription
    ttlSeconds: 7 * 24 * 3600, // default: K2's default retention
    client: {
      mget: (keys) => redis.mget(keys),
      async setWithTtl(keys, ttl) {
        const p = redis.pipeline();
        for (const key of keys) p.set(key, "1", "EX", ttl);
        await p.exec();
      },
    },
  });
  ```

  Use a different `namespace` per subscription: two subscriptions each process every record, so they must not dedupe each other. Your own stores can implement `hasMany`/`addMany` to get the same one-round-trip-per-batch behaviour.
- **Record keys.** K2 records have no id. `K2Client.produce()` adds a random `k2kit-id` header to each record (opt out with `{ idHeader: false }`), and `record.key()` uses it, so dedupe also catches producer retries. Without the header, the key is a FNV-1a 64-bit hash of `timestamp_ms` and the content. That catches K2 redeliveries but not producer retries, which get a new timestamp. Two identical records received in the same millisecond also share a key.
- **K2 limits.** `max_records` 1–10,000; 10 MB per consume response; 128 active leases per subscription; 100 subscriptions per stream; 5 MB per produce request; ~1 MB per record; 32 headers per record.

## Crash recovery

K2 hands a batch back to a `worker_id` that still holds its lease. A restarted process therefore recovers its in-flight batches **only if `workerIdPrefix` is stable across restarts**. The default includes the pid: it's safe but not recoverable, so in-flight leases simply expire within 5 minutes and get redelivered.

In production, set `workerIdPrefix` to a stable, unique value such as the pod name (`process.env.HOSTNAME` in Kubernetes StatefulSets). **Never let two live processes share a prefix**: they would act on each other's leases.

## CLI

```sh
export K2_STREAM_ID=<stream id> CLOUDFLARE_API_TOKEN=<token>   # or --stream / --token
```

| Command | What it does |
| --- | --- |
| `k2kit tail [--grep text] [--header k=v …] [--json] [--headers] [--limit N]` | Prints new records as they arrive, through a temporary `k2kit-tail-*` subscription that is deleted on exit |
| `k2kit replay [--since 2h\|ISO] [--until …] [--grep text] [--header k=v …] [--limit N] [--json]` | Reads from the earliest retained record, filters on the client, and stops after two empty polls |
| `k2kit subs ls [--json]` | Lists subscriptions |
| `k2kit subs rm <id\|name>` | Deletes one subscription |
| `k2kit subs rm --prefix <p> [--yes]` | Deletes every subscription whose name starts with `<p>`, e.g. leaked `k2kit-` ones |
| `k2kit produce [--header k=v …] [--raw] [--gzip]` | Sends stdin: one record per line (NDJSON), or all of it as one record with `--raw` |

`--grep` matches the record's text and every `--header k=v` must match exactly, e.g. `k2kit tail --header event-type=order.created --grep 1001`. Records go to stdout and logs go to stderr, so `k2kit tail --json | jq` works. Exit codes: `0` ok, `1` runtime or K2 error (printed as `K2 <code>: <message>`), `2` usage error. `replay` without `--since` reads all retained data, which will be billed once K2 billing starts.

## Low-level client

`K2Client` is a 1:1 typed wrapper over the data-plane API. It's useful when you want your own loop:

```js
import { K2Client } from "k2kit";

const k2 = new K2Client({ streamId, token });
await k2.produce([{ content: { order: 1 }, headers: { "event-type": "order.created" } }]);
const subId = await k2.ensureSubscription("audit", "earliest");
const batch = await k2.consume(subId, "worker-1", 100); // null when empty
if (batch) await k2.ack(subId, batch.id, "worker-1");
```

Every non-success response throws a `K2Error` with `code`, `status`, `message` and `retryable`.

To produce more than one request's worth, use `produceBatched`. It splits records into requests under K2's 5 MB limit, retries only errors K2 marks retryable, and keeps each record's `k2kit-id` across retries so consumers can dedupe:

```js
const { records, requests } = await k2.produceBatched(events.map((e) => ({ content: e })), {
  gzip: true,
  onProgress: (p) => console.error(`${p.requests}/${p.totalRequests} requests`),
});
```

Requests aren't atomic as a group: if one fails, earlier ones stay stored.

## Contributing and security

Contributions are welcome: see [CONTRIBUTING.md](CONTRIBUTING.md) for setup, tests and code standards, and the [Code of Conduct](CODE_OF_CONDUCT.md). Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

## License

MIT
