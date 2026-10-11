# Changelog

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
