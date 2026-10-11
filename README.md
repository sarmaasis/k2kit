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
| `workerIdPrefix` | `${hostname}-${pid}-${random}` | worker ids are `${prefix}-${slot}`; the default is unique per consumer instance — see crash recovery |
| `maxRecords` | `100` | 1–10,000 per batch |
| `pollBackoff` | `{ minMs: 250, maxMs: 10_000 }` | after empty polls; `maxMs` at least 10 |
| `idlePolling` | `"shared"` | `"shared"`: one slot polls a quiet stream and wakes the others; `"every-slot"`: each slot polls on its own |
| `retryBackoff` | `{ minMs: 500, maxMs: 30_000, maxAttempts: Infinity }` | after retryable errors and failed batches; `maxMs` at least 10 |
| `heartbeatMs` | `120_000` | lease extend interval while a handler runs, 1,000–240,000 |
| `requestTimeoutMs` | `30_000` | per-request timeout; a request that never answers is retried instead of hanging the consumer. Dedupe store calls get the same limit (a store that doesn't answer in time is treated as failed, and the batch is processed without dedupe). |
| `dedupe` | none | a `DedupeStore`: `new MemoryDedupeStore()` or `new RedisDedupeStore(…)` |
| `deadLetter` | none | `{ afterAttempts: 5, send }` — see dead-lettering |
| `onError` | logs one line | non-fatal errors: `(err, { phase, workerId, batchId?, records? })` |
| `logger` | stderr, `info` level | `{ debug, info, warn, error }`; if it throws, the error is reported once and ignored |

### Handler context

`handler(batch, ctx)` receives `batch.records`, where each record has `timestampMs`, `headers`, `bytes`, `id` (its `k2kit-id`, if any), `text()`, `json()` and `key()`. `ctx` holds:

- `workerId`: the lease holder.
- `signal`: aborted when the lease is lost or `stop()` times out. Pass it to your own I/O.

Keep handlers asynchronous. The lease heartbeat runs on the same event loop, so a handler that blocks it with CPU-bound work for minutes can't extend its lease, and K2 redelivers the batch. The consumer warns when the heartbeat runs late. Move heavy computation to a worker thread.
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

`deadLetterToStream` writes each failed record to **another** K2 stream (the consumer refuses the stream it's consuming, since dead letters would be consumed and fail again in a loop) with its content and headers (including `k2kit-id`), plus `k2kit-dlq-error`, `k2kit-dlq-attempts` and `k2kit-dlq-source-timestamp-ms`. K2 limits each record to 32 headers, 64 KiB of headers, and about 1 MB including content. When a record is too close to those limits for everything to fit, the content is always kept, the dead-letter headers come first (the error message is shortened if needed), and any headers that don't fit are counted in `k2kit-dlq-dropped-headers`. So any record K2 accepted can be dead-lettered. If `send` doesn't finish within 4× `requestTimeoutMs`, or the lease is lost, the attempt is abandoned and the batch nacked. If that send still lands later, the redelivered records may be dead-lettered twice, with the same `k2kit-id`. The token needs K2 Produce on that stream. Keep secrets and personal data out of your error messages, since they're stored in `k2kit-dlq-error`. You can also pass your own `send(letters, ctx)` to write to a database or alerting tool instead.

Records that succeed in the one-at-a-time pass may have been processed in the earlier failed batch attempts too, which is normal under at-least-once delivery.

### Health and stats

`consumer.stats()` returns a cheap snapshot for health checks and metrics: `state` (`idle`, `running`, `stopping`, `stopped`), `inFlight`, counts of `batches`, `records`, `acks`, `nacks`, `deadLettered`, `errors`, `idlePolls`, `leaseLimitHits` and `lateAcks` (acks sent after the lease had probably expired), and `lastBatchAt`, `lastAckAt` and `lastErrorAt` timestamps.

```js
// e.g. fail a liveness probe if nothing was acked for 10 minutes while there is traffic
const { state, lastAckAt, lastBatchAt } = consumer.stats();
```

### Shutdown

If `stop()` runs before `run()` (say, a SIGTERM arrives during startup), the next `run()` resolves straight away without starting. `await consumer.stop({ timeoutMs = 30_000 })` stops requesting batches and waits for in-flight handlers, which are then acked or nacked. After the timeout it aborts `ctx.signal`, waits for handlers to settle, and nacks the unfinished batches. The library never installs signal handlers; wire up `SIGINT`/`SIGTERM` yourself as in the quickstart, or pass an `AbortSignal`: `consumer.run(handler, { signal })` stops when it aborts.

Inside a handler, call `void consumer.stop()`, **not** `await consumer.stop()`: `stop()` waits for that very handler, so awaiting it never finishes. A handler that ignores `ctx.signal` also keeps `stop()` waiting; the consumer logs a warning every 30 seconds while that happens.

## Delivery guarantees and limits

K2 delivers **at least once** per subscription, with no deduplication and no ordering across workers. Plan for duplicates.

Consuming a record doesn't delete it, and retention applies whether or not a record was read: if a subscription falls further behind than the stream's retention period (for example while consumers are down), K2 deletes the records it hasn't read yet. Set retention to cover the longest consumer downtime you need to tolerate.

**Dedupe only applies to records with a `k2kit-id` header.** K2 gives every record in a produce batch the same timestamp, so two identical records without an id can't be told apart from a redelivery; skipping them could lose data, so they're always processed. k2kit's producers add the id automatically; other producers should set it (see below).

- **Dedupe in one process.** `dedupe: new MemoryDedupeStore()` skips records already processed in this process (an LRU of 100,000 keys by default).
- **Dedupe across processes.** `RedisDedupeStore` shares seen keys between every process consuming a subscription, with one `MGET` and one pipelined write per batch. k2kit has no Redis dependency: pass a two-method adapter for the client you already use. [`examples/redis-dedupe.ts`](examples/redis-dedupe.ts) has adapters for ioredis, node-redis and Upstash.

  ```js
  import { K2Client, RedisDedupeStore } from "k2kit";
  import { Redis } from "ioredis";

  const k2 = new K2Client({ streamId: process.env.K2_STREAM_ID, token: process.env.CLOUDFLARE_API_TOKEN });
  const subscriptionId = await k2.ensureSubscription("orders-processor", "earliest");
  const redis = new Redis(process.env.REDIS_URL);
  const dedupe = new RedisDedupeStore({
    namespace: subscriptionId, // one per subscription; see below
    ttlSeconds: 7 * 24 * 3600, // default; set it to at least your stream's retention
    client: {
      mget: (keys) => redis.mget(keys),
      async setWithTtl(keys, ttl) {
        const p = redis.pipeline();
        for (const key of keys) p.set(key, "1", "EX", ttl);
        // ioredis doesn't throw when pipelined commands fail; surface errors so they're reported.
        const failed = (await p.exec())?.find(([err]) => err);
        if (failed) throw failed[0];
      },
    },
  });
  ```

  Use the **subscription id** as the `namespace`. Two subscriptions each process every record, so they must not dedupe each other, and a subscription deleted and recreated under the same name to reprocess a stream gets a new id, so old keys can't make it skip records. (Get the id from `client.ensureSubscription(name, startAt)`.) Keys look like `k2kit:dedupe:{namespace}:<key>`; the braces are a Redis Cluster hash tag, so it works in cluster mode. Set `ttlSeconds` to at least the stream's retention (K2 allows up to 30 days; the default of 7 days matches K2's default): a record whose ack failed is redelivered whenever consumers next poll, which after a long outage can be later than a shorter TTL remembers. Your own stores can implement `hasMany`/`addMany` to get the same one-round-trip-per-batch behaviour. Store methods (and a dead-letter `send`) may be synchronous or return promises.
- **One store per subscription.** Passing the same store object to consumers of two different subscriptions is refused at startup, since it would silently skip records. Sharing one between consumers of the same subscription is fine.
- **Record keys.** K2 records have no offset or id of their own. `K2Client.produce()`, `produceBatched()` and `k2kit produce` add a random `k2kit-id` header to each record (opt out with `{ idHeader: false }`); `record.id` exposes it and dedupe uses it, so it also catches producer retries. `record.key()` falls back to a hash of the timestamp and content for records without an id, but dedupe doesn't use that fallback, since identical records in one batch share it.
- **Producing without k2kit** (for example from a Worker using the K2 binding)? Add a `k2kit-id` header with a random UUID (for example `crypto.randomUUID()`) to get the same producer-retry dedupe. Generate it once per record and reuse it when you retry. It must be unique per record: a blank id is ignored (the record isn't deduped), but a constant one would make every record look like a duplicate.
- **Records with 32 headers** (K2's maximum) (or so close to the size limits that the id wouldn't fit) are produced without the automatic `k2kit-id`, so they aren't deduped.
- **K2 limits** ([docs](https://developers.cloudflare.com/k2/platform/limits/)). `max_records` 1–10,000; 10 MB per consume response; 128 active leases per subscription; 100 subscriptions per stream; 5 MB per produce request (plus internal metadata); per record, about 1 MB of content and headers combined, at most 32 headers, names ≤256 bytes, values ≤8 KiB, and ≤64 KiB of headers in total (UTF-8). `produceBatched` and `k2kit produce` check every record against these before sending anything, using a slightly conservative size estimate, so one bad record can't fail a whole request. Requests are planned with a 1% byte margin and at most 50,000 records, because K2's per-request metadata grows with the record count.

### Dedupe guarantees

Dedupe **reduces** duplicate processing; it doesn't make it exactly-once. **Your handler should still be idempotent.** For each batch, the consumer:

1. Checks the dedupe store and skips records it has already seen. If every record was seen, the batch is acked without calling your handler.
2. Calls your handler with the remaining records.
3. **Only if the handler returns successfully**, writes the records' keys to the store, **then** acks the batch.

What that means when things go wrong:

| What happens | Result |
| --- | --- |
| The process crashes or the handler throws mid-batch | No keys are written. K2 redelivers and the records are processed again. **Nothing is dropped.** |
| The process crashes after the handler, before the key write | The records are processed again on redelivery. |
| The keys are written but the ack fails | The redelivered records are skipped and the batch is acked. |
| The lease is lost while the handler runs | `ctx.signal` aborts and nothing is written or acked. If your handler doesn't stop, it can overlap with the redelivery on another worker. |
| The dedupe store is down | The error goes to `onError` and the batch is processed without dedupe, rather than dropped or blocked. |
| The same `k2kit-id` appears twice in one batch | It reaches the handler once (with dedupe on). Records without an id are never skipped. |
| A lease extend fails | It's retried after 5, 10, 20… seconds, not at the next heartbeat, so two failures in a row don't lose the lease. If the lease still expires before the ack, the ack is sent anyway, a warning is logged and `stats().lateAcks` counts it. |

In short, k2kit errs towards processing a record twice, never towards silently skipping one.

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
| `k2kit subs rm <id\|name>` | Deletes one subscription (ids are 32 hex characters; any case) |
| `k2kit subs rm --prefix <p> [--older-than 1h] [--yes]` | Deletes every subscription whose name starts with `<p>`, e.g. leaked `k2kit-` ones. Add `--older-than` to skip recent ones, which may belong to `tail`/`replay` sessions still running elsewhere. |
| `k2kit produce [--header k=v …] [--raw] [--gzip]` | Streams stdin: one record per line (NDJSON), sent every ~4 MB or after 1 s without new input, so `tail -f app.log \| k2kit produce` works. `--raw` sends all of stdin as one record (max 1 MB). Ctrl+C sends what's pending, then exits. |

`--grep` matches the record's text and every `--header k=v` must match exactly, e.g. `k2kit tail --header event-type=order.created --grep 1001`. `tail` and `replay` always delete their temporary subscription, including when you press Ctrl+C while it's still being created. Pressing Ctrl+C a second time forces an immediate exit and prints the name of any subscription left behind, with the command to delete it. Records go to stdout and logs go to stderr, so `k2kit tail --json | jq` works. Control characters in record content and headers are escaped (`\n`, `\u001b`), so a record can't break the one-line-per-record format or send escape codes to your terminal. Times are durations (`30m`, `2h`, `7d`) or ISO dates; a date-time needs a timezone (`2026-10-11T10:00Z` or `+05:30`), a bare date means midnight UTC, and a number without a unit is rejected rather than guessed at. Exit codes: `0` ok, `1` runtime or K2 error (printed as `K2 <code>: <message>`), `2` usage error. `replay` without `--since` reads all retained data, which will be billed once K2 billing starts.

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

`content` can be a `Uint8Array` (or any `ArrayBuffer` or typed array, sent as its bytes), a string (sent as UTF-8), or any other JSON value (sent as JSON). Header values must be strings. Invalid input throws a `TypeError` before anything is sent. To check a record's size yourself, compare `estimateRecordBytes(contentBytes, headers)` with `MAX_RECORD_BYTES`; it's the same conservative estimate `produceBatched` uses.

Every non-success response throws a `K2Error` with `code`, `status`, `message` and `retryable`. A request that doesn't complete within `timeoutMs` (default 30 seconds) throws a `K2TimeoutError`. The constructor rejects a token with whitespace or control characters, and an endpoint that isn't `https://` (`http://` is allowed only for localhost).

To produce more than one request's worth, use `produceBatched`. It splits records into requests under K2's 5 MB limit, retries only errors K2 marks retryable, and keeps each record's `k2kit-id` across retries so consumers can dedupe:

```js
const { records, requests } = await k2.produceBatched(events.map((e) => ({ content: e })), {
  gzip: true,
  onProgress: (p) => console.error(`${p.requests}/${p.totalRequests} requests`),
});
```

Requests aren't atomic as a group: if one fails, earlier ones stay stored. Network errors and errors K2 marks non-retryable (like `10212`) aren't retried, because the batch may already be stored and K2 doesn't deduplicate; they're thrown so you can decide, knowing a retry may store duplicates.

## Contributing and security

Contributions are welcome: see [CONTRIBUTING.md](CONTRIBUTING.md) for setup, tests and code standards, and the [Code of Conduct](CODE_OF_CONDUCT.md). Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

## License

MIT
