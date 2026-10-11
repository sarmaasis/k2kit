# Changelog

## 0.2.0 — 2026-10-12

A reliability release. Every fix below has a regression test, and limits and behaviour were checked against the official K2 docs and real K2.

### Upgrade notes

- **Dedupe only skips records that have a `k2kit-id`.** K2 gives every record in a produce batch the same timestamp, so the content-hash fallback treated identical records as redeliveries and silently dropped them. Records without an id are now always processed. Records produced with k2kit get an id automatically; other producers should set the header (see the README).
- **`RedisDedupeStore` keys moved** to `k2kit:dedupe:{namespace}:…` (a Redis Cluster hash tag). Keys written by 0.1.x aren't read after upgrading.
- **Invalid input fails fast with `TypeError`** instead of being sent and rejected by K2 (or silently changed): non-string header values, headers given as a `Map`/`Headers`, content with no JSON form (`undefined`, a function), records that aren't an array, a `streamId` that isn't 32 hex characters, and subscription names K2 doesn't allow.
- **Stricter options:** backoff `maxMs` must be at least 10 ms, `heartbeatMs` 1–240 s, delays at most ~24.8 days; tokens with whitespace and non-HTTPS endpoints (other than localhost) are rejected; a `dedupe` object without `has()`/`add()` is rejected.
- **CLI times** (`--since`, `--until`, `--older-than`) accept only durations (`30m`, `2h`, `7d`) and ISO dates. A bare number, a non-ISO date, or a date-time without a timezone is rejected.
- **TypeScript:** `DedupeStore` methods and `DeadLetterSend` may now return plain values instead of promises. Code that calls them through the interface may need to handle a non-promise result.

### Security

- `K2Client` validates `streamId`. The default endpoint is built from it, so an id like `attacker.example#` sent the API token to another host. Uppercase ids are still accepted and lowercased.
- Endpoints must be HTTPS (HTTP only for localhost), since the token is sent with every request.
- `tail` and `replay` escape control characters and bidirectional-override characters in record content and headers, so a record can't inject terminal escape codes.
- On Node, `record.bytes` no longer exposes Node's shared buffer pool through `.buffer`.

### Fixed: runaway requests

- An idle consumer polled K2 in a tight loop (thousands of requests a second) when a backoff option was set to `undefined`, e.g. `{ maxMs: process.env.X ? Number(process.env.X) : undefined }`. Fields set to `undefined` now keep their defaults.
- When K2 kept refusing an ack with a non-retryable error, the consumer requested again at once, got the same still-leased batch, and reprocessed it in a tight loop. It now backs off.
- `produceBatched` merges `retryBackoff` over its defaults (a partial range retried with no delay) and rejects a `NaN` or negative `maxAttempts` (which meant unlimited attempts).
- Very large backoffs are rejected and timers are clamped, instead of firing immediately in a busy loop.

### Fixed: consumer

- Requests time out (default 30 s, `requestTimeoutMs`) and are retried, so a stalled connection can't hang a worker, the heartbeat or `stop()`, even with a custom `fetch` that ignores the abort signal.
- Dedupe store calls and dead-letter sends are time-limited, so a store or sender that never answers can't hang a worker or `stop()`.
- A failed lease extend is retried within seconds instead of at the next heartbeat. Lease timing uses the local clock, so clock skew can't shorten ack retries. Acks sent after the lease probably expired are warned about and counted.
- The default `workerIdPrefix` has a random suffix, so two consumers can't share leases (same process, or containers where every pid is 1).
- `run()` after an early `stop()` (e.g. a SIGTERM during startup) resolves instead of throwing.
- `stop()` logs a warning every 30 s while handlers keep it waiting past its timeout.
- A throwing logger can't stop the consumer, and a blocked event loop (late heartbeat) is warned about.
- Identical records in a failing batch counted two failed deliveries each time, so `ctx.attempt` grew twice as fast and could dead-letter them early.

### Fixed: dedupe

- A blank `k2kit-id` no longer gives every record the same key. Repeats of an id within one batch reach the handler once.
- One dedupe store can't be shared between two subscriptions, which would silently skip records.
- Ids longer than 128 characters are hashed, so store keys stay small.

### Fixed: producing

- Records are checked before sending against K2's real limits: about 1 MB including headers and overhead, 32 headers, names up to 256 bytes, values up to 8 KiB, and 64 KiB of headers. One invalid record can no longer fail a whole request.
- Requests are planned with a 1% byte margin and at most 50,000 records, since K2's per-request metadata grows with the record count (larger requests failed with `10212`).
- The automatic `k2kit-id` is skipped for records with no room for it, instead of being rejected.
- `ArrayBuffer`, `DataView` and typed-array content is sent as its bytes instead of being JSON-stringified to `"{}"`. Header values set to `undefined` are left out.
- `produce([])` sends nothing (K2 rejects an empty batch), like `produceBatched([])`. A hole in a sparse array is reported instead of being sent as `null`.

### Fixed: dead letters

- Dead letters always fit K2's limits, even for records near 1 MB. The content is kept, the error message is shortened, and headers that don't fit are counted in `k2kit-dlq-dropped-headers`.
- `deadLetterToStream()` on the stream being consumed is refused, since dead letters would loop.
- Original headers named like object properties (`constructor`, `toString`, `__proto__`) are kept, and shortening the error message never cuts a character in half.

### Fixed: CLI

- `tail` and `replay` no longer leak their temporary subscription when Ctrl+C arrives while it's being created, or when the create request fails after K2 applied it. A forced exit (second Ctrl+C) names any leftover subscription and how to delete it.
- `--since 10` (unit forgotten) was read as a date in 2001, so `replay` read the whole stream. Non-ISO dates were silently read as local time. Both are now rejected with a hint, and ISO times parse the same on Node, Bun and Deno.
- `produce` streams stdin (works with `tail -f`, bounded memory), sends pending lines on Ctrl+C, stops as soon as a send fails, reports how many records were sent, explains itself when stdin is a terminal, and checks `--header` limits.
- `replay --until` no longer stops early on out-of-order timestamps, and `--since` must be earlier than `--until`.
- `subs rm` says when a name doesn't exist and accepts ids in any case. `tail` respects stdout backpressure, and `--header __proto__=…` works.

### Added

- `record.id`: the record's `k2kit-id`, or `undefined`.
- `estimateRecordBytes(contentBytes, headers)`, to check a record against `MAX_RECORD_BYTES` the way `produceBatched` does.
- `K2TimeoutError`, the `requestTimeoutMs` consumer option and the `timeoutMs` client option.
- `stats().lateAcks`.
- `k2kit subs rm --prefix <p> --older-than <duration>`, to clean up leaked temporary subscriptions without touching running sessions.

### Docs and CI

- README: a "Dedupe guarantees" section (when keys are written, each failure mode, why handlers should still be idempotent), how to set `k2kit-id` when producing without k2kit, K2's limits, that retention deletes unread records, and setting the Redis TTL to at least the stream's retention.
- Examples use the subscription id as the Redis dedupe namespace and keep a unique worker id prefix.
- CI and release never pass K2 credentials to the tests, so pushes, PRs and releases send no requests to a real stream. Live tests run only from a manual CI run with **live** checked.

## 0.1.1 — 2026-10-11

- Published from GitHub Actions through npm trusted publishing, with a provenance attestation. No code changes.

## 0.1.0 — 2026-10-11

First release.

- `K2Client`: typed wrapper over the K2 data-plane API (subscriptions, consume, ack, nack, extend, produce), with `K2Error` for every failure.
- `K2Client.produceBatched()`: splits records into requests under 5 MB, with safe retries.
- `Consumer`: `run(handler, { signal })` with concurrency, jittered backoff, lease heartbeats, ack/nack, retryable-error handling, optional dedupe, poison-batch warnings, events, `stats()` and graceful `stop()`.
- Dead-lettering: `deadLetter: { afterAttempts, send }` retries a failing batch one record at a time and diverts the records that still fail; `deadLetterToStream()` sends them to another K2 stream.
- `MemoryDedupeStore` (in-process LRU) and `RedisDedupeStore` (shared across processes, no Redis dependency). `DedupeStore` gains optional `hasMany`/`addMany` for one round trip per batch.
- Shared idle polling: on a quiet stream one slot polls and wakes the others (`idlePolling: "shared"`, the default), plus a warning when the 128-lease limit is hit.
- CLI: `k2kit tail`, `replay` (both with `--grep` and `--header` filters), `subs ls`, `subs rm`, `produce`.
- Tested on Node 20+, Bun and Deno 2.
