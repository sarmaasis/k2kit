# Notes

Maintainer notes: how K2 actually behaves (from the docs and measured on real K2), the design decisions behind k2kit, how it's tested, and its known limits. User-facing changes are in [CHANGELOG.md](CHANGELOG.md).

## How K2 behaves

### From the docs

Checked against the full K2 documentation ([llms-full.txt](https://developers.cloudflare.com/k2/llms-full.txt)) on 2026-10-12.

- **Leases** last 5 minutes. One lease per `worker_id`; asking again while holding it returns the same batch. At most 128 active leases per subscription (`10216`).
- **Ack and nack** of an unknown or already-acked batch succeed; a late ack has no effect. Nack redelivers with a new `batch_id`. Extend needs the worker that holds the lease, otherwise `10218`: stop and request a new batch.
- **Retries:** only `10211`, `10214`, `10216` and `10217` on the consume side; "Do not retry other errors without changing the request". On produce, trust the response's `retryable`. `10212` means the outcome is unknown: the batch may or may not be stored.
- **Error shapes:** consume-side errors are `{ success: false, errors: [{ code, message }] }`; produce errors are `{ success: false, error: { code, message, retryable } }`.
- **Subscriptions:** names are 1–128 of `[A-Za-z0-9_-]`, case-insensitive. Create is idempotent; a name with different settings gives `10201`. At most 100 per stream (`10219`).
- **Timestamps are written by the server per produce batch**, so every record in a batch has the same timestamp. K2 doesn't deduplicate; the docs recommend a unique id in each record.
- **Produce:** standard padded base64 content; only `content` and `headers` fields; headers map strings to strings; a batch needs at least one record and is atomic.
- **Limits:** 5,000,000 bytes per request (before and after gzip) "plus a small amount of internal metadata"; about 1 MB per record, counting content and headers; at most 32 headers, names ≤256 bytes, values ≤8 KiB, ≤64 KiB of headers in total (UTF-8); `max_records` 1–10,000; 10 MB per consume response; `worker_id` 1–256 characters.
- **Retention** is 1 hour to 30 days (default 7 days) and applies whether or not records were read, so a subscription that falls too far behind loses unread records.
- **Billing:** produced, consumed and stored data ($0.04/GB, $0.04/GB, $0.02/GB-month), not enabled during the beta, with 30 days' notice before it starts.

### Measured on real K2 (2026-10-11 and 2026-10-12)

- **Record size:** K2 counts content + header names and values + about 14 bytes, plus 2–4 bytes per header, and the figure varies by a few bytes. k2kit estimates content + header bytes + 32 + 4 per header, which is above every measurement, so a record that passes the estimate is never rejected for size.
- **Request metadata grows with the record count:** 50,000 small records in a ~5,000,000-byte request were accepted; 150,000 records in a ~4,990,000-byte request failed with `10212`. k2kit plans requests at ≤4,950,000 bytes and ≤50,000 records.
- **Ids** (stream, subscription, batch) are 32 lowercase hex characters. A malformed subscription id gets `10204 Request body is invalid` from GET/DELETE and from ack/nack/extend, but `10215` from consume. An invalid name in `GET /subscriptions?name=` gets `10204`.
- `DELETE /subscriptions/<id>` returns `{ id }` (the body isn't documented).
- Header names keep their case and may contain spaces, Unicode, or be empty.

## Design decisions

### Consumer

- **Back off after a failed batch or a failed ack.** After a handler throws (batch nacked) or an ack K2 keeps refusing, the slot waits with `retryBackoff` before requesting again. A failed ack matters because the worker still holds the lease, so K2 would return the same batch at once. Backoff fields set to `undefined` keep their defaults.
- **Timeouts everywhere.** Every K2 request (including the body read) is raced against `requestTimeoutMs`, so it fires even if a custom `fetch` ignores the abort signal. Dedupe store calls are limited to `requestTimeoutMs` and dead-letter sends to 4× that.
- **Leases are timed on the local clock** (request time + 5 minutes), so clock skew between this machine and K2 can't stretch or shrink ack retries. A failed extend is retried after 5, 10, 20… s, capped at the heartbeat interval. An ack sent after the lease probably expired is still sent, but warned about and counted in `stats().lateAcks`.
- **`stop()` timeout:** batches still running when it fires are nacked, even if the handler returns successfully after the abort. A lost or timed-out lease never marks records as processed in the dedupe store, because a handler that returned early on `ctx.signal` may not have finished.
- **Fatal vs retryable:** retryable K2 errors and network errors are retried; any other K2 error stops all slots and `run()` rejects. Responses without a K2 error body (e.g. an HTML 502) are retried only for 429 and 5xx.
- **Shared idle polling:** after an empty poll, one slot keeps polling with `pollBackoff` and the others wait at an in-process gate until it sees records. It's per process, not across pods.
- **`10216` warning:** logged at most every 5 minutes and counted in `stats().leaseLimitHits`, since it means processes × `concurrency` exceeds 128 leases.
- **Default `workerIdPrefix`** is host-pid-random. The random part keeps two consumers apart even in one process or in containers where every pid is 1. It isn't stable across restarts, so in-flight leases expire instead of being recovered; a stable, unique prefix (e.g. the pod name) recovers them.

### Dedupe

- **Only records with a `k2kit-id` take part.** Because a produce batch shares one timestamp, a content hash can't tell a redelivery from two identical events. Records without an id are always processed. `record.key()` still falls back to a hash of timestamp + content, but dedupe doesn't use it.
- Keys are written after the handler succeeds and before the ack. A store failure is reported and the batch is processed without dedupe (at-least-once still holds).
- Blank ids count as missing; ids over 128 characters are hashed into the key; repeats of an id within one batch reach the handler once; one store object can't serve two subscriptions.
- `RedisDedupeStore` keys are `k2kit:dedupe:{namespace}:<key>` (a Redis Cluster hash tag, so `MGET` works in cluster mode). Use the subscription id as the namespace, and a TTL of at least the stream's retention.

### Producing

- `produceBatched` and CLI `produce` set each record's `k2kit-id` once, so a retried request reuses the same ids. They retry only errors K2 marks `retryable` (5 attempts by default); network errors and `10212` are thrown, since the batch may already be stored. Requests aren't atomic as a group.
- Every record is checked against K2's limits before anything is sent, so one bad record can't fail a request full of good ones. The automatic `k2kit-id` is skipped when a record has no room for it.
- Input from plain JavaScript is validated rather than silently changed: binary data is sent as its bytes, values with no JSON form throw, header values must be strings (`undefined` ones are left out).
- `K2Client` validates `streamId`, because the default endpoint is built from it; uppercase ids are lowercased.

### Dead letters

- At `afterAttempts`, a failing batch is retried one record at a time and the records that still fail go to `send`. Records that succeed then may already have been processed in earlier failed attempts (normal under at-least-once).
- `deadLetterToStream` always fits K2's limits: content kept, dead-letter headers first (the error shortened if needed), original headers while they fit, the rest counted in `k2kit-dlq-dropped-headers`. It refuses the stream being consumed.
- Failure counts behind `ctx.attempt` are in memory (an LRU of 100,000 keys), counted once per key per delivery.

### CLI

- `tail` and `replay` delete their temporary subscription on normal exit, errors, `SIGINT`/`SIGTERM`, and a closed stdout (`EPIPE`). Interrupt handling is registered before the subscription is created; a create whose outcome is unknown is retried, then looked up and deleted. A second Ctrl+C forces an exit and names any leftover subscription.
- Like `tail -f`, a closed pipe is only noticed on the next write, so `k2kit tail | head -3` runs until another record arrives. Use `--limit`.
- `replay` reads with a single worker. Records past `--until` are skipped rather than ending the replay (timestamps needn't be ordered across producers); it stops once a whole batch is past `--until`, or after two empty polls.
- `produce` streams stdin in ~4 MB or 1-second-idle batches with backpressure, so `tail -f | k2kit produce` works with bounded memory. It stops at the first failed send and always reports how many records were sent.
- Times accept only durations and ISO dates; date-times need a timezone. ISO date-times are rebuilt in the format ECMAScript requires every engine to parse, so Node, Bun and Deno agree.

## Testing

- Most tests run against `test/fake-k2.ts`, an in-memory fake of the K2 HTTP API with a fake clock and fault injection. It mirrors real K2's id formats, malformed-id responses, size and header limits, and error shapes; 49 raw-HTTP checks were run against both the fake and real K2 to keep them aligned.
- Every bug fix has a regression test, verified by removing the fix and watching the test fail.
- CLI signal handling is tested with real processes against the fake served over local HTTP.
- `RedisDedupeStore` runs against Redis 8 in Docker through all three documented adapters (ioredis, node-redis, and Upstash's REST API via the `serverless-redis-http` emulator), including a Redis Cluster node. Hosted Upstash hasn't been tested.
- The built package is loaded on Node 20, Bun and Deno in CI (`scripts/smoke-dist.mjs`).
- Live K2 tests (`test/integration.test.ts`) never run on pushes, PRs or releases; only locally with credentials or from a manual CI run with **live** checked.

## Toolchain

- **Latest versions:** pnpm 12, TypeScript 7, Vitest 5, Biome 2.5, tsup 8.5, @types/node 26.
- **TypeScript 7** is the native compiler and ships no JS compiler API, so tsup's `dts` step can't run. tsup bundles the JavaScript; `tsc -p tsconfig.build.json` emits the declarations; `scripts/dts-cjs.mjs` mirrors them to `.d.cts` for `require` users.
- **Vitest 5 needs Node 22.12+,** so CI tests on Node 22 and 24. The package declares `engines: >=20` with no runtime dependencies; a separate CI job loads the built package on Node 20. Library code only uses web-standard globals (`fetch`, `AbortController`, `TextEncoder`/`TextDecoder`, `atob`/`btoa`, `CompressionStream`, `crypto.randomUUID`).
- **`node:os`** is loaded with a dynamic import through a variable, only for the default `workerIdPrefix`; esbuild otherwise rewrites it to bare `os`, which Deno rejects.
- **Dependabot** may not parse pnpm 12's lockfile yet. If its npm updates fail, switch to Renovate.

## Releases

Push a `v<version>` tag after bumping `package.json` and `src/version.ts` and naming the CHANGELOG section `<version> — <date>`. `release.yml` publishes through npm trusted publishing (OIDC) in the `npm` GitHub environment, which needs a maintainer's approval, with a provenance attestation and no npm token. The GitHub release notes are that version's CHANGELOG section. 0.1.0 was published with a granular token and has no provenance; later versions do.

## Known limits

- `ctx.attempt` and dead-letter thresholds use in-memory failure counts. They reset on restart and aren't shared between processes, so with several pods a poison record may need up to `afterAttempts` failures on each pod before it's dead-lettered.
- Records without a `k2kit-id` aren't deduplicated, including records produced with 32 headers or too close to the size limit for the id to fit.
- If K2 ever returned a malformed record (e.g. no content), the consumer would retry that batch with backoff and report each failure, rather than treat it as a poison record.
- With `maxRecords` near 10,000, one batch's `MGET` is about 870 KB, which may exceed some hosted Redis plans' request size (e.g. Upstash's).
- `k2kit produce` keeps a leading UTF-8 byte-order mark in the first record's bytes. `record.text()` and `record.json()` strip it; only code reading `record.bytes` sees it.

## Ecosystem

Checked on 2026-10-11:

- **Cloudflare's `cf` CLI** (1.0.0-beta.14) covers stream management and `subscriptions list` with lag, but has no tail, consume, produce, replay or subscription delete. The K2 team has said more metrics, including consumer lag, are coming; k2kit should read those rather than compute its own.
- **Cloudflare's TypeScript SDK** (7.3.0) has no K2 resources.
- **Other libraries:** no JavaScript/TypeScript K2 consumer library found on npm or GitHub. A .NET library, [soenneker.cloudflare.k2](https://github.com/soenneker/soenneker.cloudflare.k2), includes a consumer with polling, lease renewal, retries and concurrency. PyPI, Go and Rust weren't searched.
