# Notes

Doc discrepancies, decisions and known limits for k2kit v0.1.0.

## K2 docs vs the build spec

Checked on 2026-10-11 against the live [Consume](https://developers.cloudflare.com/k2/features/consume/), [Produce](https://developers.cloudflare.com/k2/features/produce/), [Limits](https://developers.cloudflare.com/k2/platform/limits/) and [Get started](https://developers.cloudflare.com/k2/get-started/) pages.

**The endpoint table, lease rules, error codes and limits match the spec. No discrepancies were found in the docs.** Small clarifications:

- The 5 MB produce limit is 5,000,000 bytes and the record limit is 1,000,000 bytes. The CLI plans requests against those exact numbers.
- The `DELETE /subscriptions/<id>` response body is not documented. The client accepts any 2xx and ignores the body.
- Consume errors are `{ result: null, success: false, errors: [{ code, message }], messages: [] }`. Produce errors are `{ success: false, error: { code, message, retryable } }`. Both are handled.
- Produce `headers` must be a JSON object of string to string, not an array.

## Live behaviour

**None found.** Verified on 2026-10-11 against a real stream:

- `test/integration.test.ts` passed all 4 cases: 50 JSON records consumed with `concurrency: 2`, a nacked batch redelivered with a new `batch_id`, the `k2kit-id` header accepted on produce and returned on consume, and a poison record dead-lettered (via `deadLetterToStream`) with its siblings handled and the batch acked.
- The spec's manual smoke test passed with the built CLI: `k2kit tail --headers` printed all 20 records from `seq 1 20 | jq -c '{n: .}' | k2kit produce --header event-type=test`, Ctrl+C exited 0, and `k2kit subs ls` showed no `k2kit-tail-*` subscription.
- The README quickstart, copied verbatim into a fresh project that installs the packed tarball, consumed the stream (including newly produced records) and stopped cleanly on SIGINT.

Before that, the CLI was also checked against the in-memory fake served over local HTTP: `replay --grep`, the piped `tail | head` cleanup, and exit codes 1 and 2.

Also verified on 2026-10-11: the built package passes `publint --strict` and `@arethetypeswrong/cli` (node10, node16 CJS/ESM and bundler resolution), and a live produce → consume round trip passes on Bun 1.3.3 and Deno 2.9.6 (Deno with `--allow-net --allow-env --allow-read --allow-sys=hostname`).

## Toolchain decisions

- **Latest versions everywhere** (pnpm 12, TypeScript 7, Vitest 5, Biome 2.5, tsup 8.5, @types/node 26).
- **TypeScript 7** is the native compiler and ships no JS compiler API, so tsup's `dts` step can't run. tsup bundles the JavaScript; `tsc -p tsconfig.build.json` emits the declarations; `scripts/dts-cjs.mjs` mirrors them to `.d.cts` for `require` consumers.
- **Vitest 5 needs Node 22.12+.** The spec asked for CI on Node 20 and 22. The package still declares `engines: >=20` and has no runtime dependencies. CI runs lint, typecheck, build and tests on Node 22 and 24, then a separate job loads the built `dist` on Node 20 (`scripts/smoke-dist.mjs`).
- **@types/node 26** is newer than the Node 20 floor. Library code only uses web-standard globals (`fetch`, `AbortController`, `TextEncoder`/`TextDecoder`, `atob`/`btoa`, `CompressionStream`, `crypto.randomUUID`). The Node 20 job guards against accidental use of newer APIs.
- **`node:os`** is loaded with a dynamic import through a variable, only to build the default `workerIdPrefix`. esbuild otherwise rewrites `node:os` to bare `os`, which Deno rejects.

## Behaviour decisions not spelled out in the spec

- **Back off after a failed batch.** After a handler throws and the batch is nacked, the slot sleeps with `retryBackoff` before the next request; the backoff resets on the next successful batch. Without this, a poison batch makes the loop spin against K2 with no delay.
- **`K2Client.consume` returns a decoded `Batch`** (or `null` when empty) instead of the raw wire shape. `ensureSubscription` returns the id string.
- **Responses with no K2 error body** (for example an HTML 502 from the edge) become `K2Error { code: 0 }`, retryable for 429 and 5xx.
- **Network errors on startup** (resolving the subscription) are retried like consume errors. Non-retryable K2 errors are fatal.
- **`stop()` timeout:** batches still running when the timeout fires are nacked, even if the handler returns successfully after the abort. Ack retries also stop at that point.
- **Dedupe store failures** are reported to `onError`, and the batch is processed without dedupe (at-least-once still holds).
- **Same batch after a failed ack.** When an ack fails and the worker still holds the lease, K2 returns the same batch to that `worker_id` on the next request. The dedupe store then skips its records and the batch is acked.
- **`produceBatched` and CLI `produce`** set each record's `k2kit-id` once, so a retried request reuses the same ids. They retry only errors K2 marks `retryable` (at most 5 attempts by default); `10212` is never retried. The requests are not atomic as a group. The CLI's summary line goes to stderr.
- **CLI `replay`** reads with a single worker, so records arrive in order and it stops at the first record past `--until`. `--grep` matches the UTF-8 text of the content.
- **CLI cleanup.** `tail` and `replay` delete their temporary subscription on normal exit, errors, `SIGINT`/`SIGTERM`, and a closed stdout (`EPIPE`, e.g. `| head`). Like `tail -f`, a closed pipe is only noticed on the next write, so `k2kit tail | head -3` keeps running until another record arrives or Ctrl+C. Use `--limit N` instead. A second Ctrl+C forces an exit without cleanup. The capacity check runs before every temporary subscription is created.

- **Dead-lettering re-runs the handler per record** for a failing batch at `afterAttempts`, so records that succeed then may already have been processed in earlier failed attempts (normal under at-least-once). If the lease is lost or `stop()` times out during that pass, the batch is not acked. A lost lease counts as a failed delivery for `ctx.attempt`; a `stop()` timeout does not.
- **Shared idle polling.** After an empty poll, the first slot to see it keeps polling with `pollBackoff`; the others wait at an in-process gate until that slot gets records, then all resume. It's per process, not across pods.
- **10216 warning.** When consume returns `10216` (all 128 leases taken), the consumer counts it and logs a warning at most every 5 minutes, since it means processes × `concurrency` exceeds what K2 allows.

- **Dependabot and pnpm 12.** Dependabot may not parse pnpm 12's lockfile yet. If its npm updates fail, switch to Renovate, which tracks pnpm releases closely; the GitHub Actions updates are unaffected.

- **Releases.** 0.1.0 was published from CI with a granular npm token; npm staged it for approval, and it shipped without a provenance attestation. From the next release on, `release.yml` uses npm trusted publishing (OIDC) through the `npm` GitHub environment, with `--provenance`, and no npm token exists. The package requires 2FA and disallows bypass tokens for any other publish.

## Known limits

- The FNV-1a fallback key (no `k2kit-id` header) collides for identical content received in the same millisecond, and doesn't catch producer retries.
- `ctx.attempt` and dead-letter thresholds use in-memory, per-record failure counts (an LRU of 100,000 keys). They reset on restart and aren't shared between processes, so with several pods a poison record may need up to `afterAttempts` failures on each pod before it's dead-lettered.
- `RedisDedupeStore` was verified on 2026-10-11 against Redis 8.10.2 in Docker through all three documented adapters (ioredis, node-redis, and Upstash's REST API via the `serverless-redis-http` emulator), including TTLs, namespace isolation, and two consumers ("pods") sharing one Redis, where the second skipped a record the first had processed but failed to ack. CI runs these against Redis service containers; locally, `docker compose up -d` (see `compose.yaml`). Real Upstash (hosted) was not tested.
- With the default `workerIdPrefix` (includes the pid), in-flight leases are not recovered after a restart; they expire within 5 minutes.

## Open questions from the spec

| Question | Status |
| --- | --- |
| Does the `cf` CLI already have a K2 tail/consume command? | No (checked `cf` 1.0.0-beta.14 on 2026-10-11). `cf k2 streams` covers stream create/get/list/update/delete and `subscriptions list` (with committed-position lag). It has no tail, consume, produce, replay or subscription delete, so nothing is dropped from scope. `k2kit subs ls` overlaps `cf k2 streams subscriptions list` but uses the data-plane token; cf adds lag. |
| Is the npm name `k2kit` free? | Yes on 2026-10-11 (`npm view k2kit` → 404). |
| Is there an existing community K2 consumer library? | None found in a web search on 2026-10-11 (coverage partial). |
| Does the test account have Workers Paid? | Yes: K2 works on the test account. |
| Does produce accept the `k2kit-id` header? | Yes: confirmed by the integration test on 2026-10-11. |
| Is FNV-1a over `timestamp_ms` + content collision-safe enough? | Documented as a known limit (above and in the README). |
