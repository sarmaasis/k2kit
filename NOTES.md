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

- `test/integration.test.ts` passed all 4 cases: 50 JSON records consumed with `concurrency: 2`, a nacked batch redelivered with a new `batch_id`, the `k2kit-id` header accepted on produce and returned on consume, and a poison record dead-lettered with its siblings handled and the batch acked. (Since the edge-case fixes, dead letters can't target the consumed stream through `deadLetterToStream`; the live test uses a custom sender on the test stream, or `deadLetterToStream` on a second stream when `K2_DLQ_STREAM_ID` is set, and checks the same-stream guard.)
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

## Edge cases reviewed (2026-10-11)

Found in a review of the code; each fix has a regression test in `test/edge-cases.test.ts` or `test/cli.test.ts` (numbered to match).

| # | Edge case | Resolution |
| --- | --- | --- |
| 1 | A blank `k2kit-id` gave every record the same dedupe key, so dedupe silently skipped records | Blank ids count as missing (content hash). Docs: ids must be unique per record. |
| 2 | On Node, small decoded records were views into Node's shared 8 KB pool, so `record.bytes.buffer` exposed unrelated memory | Such results are copied; `.buffer` holds only the record. |
| 3 | Two consumers with the default prefix (same process, or containers where pid is 1) shared worker ids and leases, processing records twice at once | Default prefix adds a random per-instance suffix. |
| 4 | `deadLetterToStream()` on the consumed stream looped dead letters forever | The consumer refuses it at construction. |
| 5 | `await consumer.stop()` inside a handler (or a handler ignoring `ctx.signal`) hung forever, silently | Documented; a warning every 30 s while stop() waits past its timeout. |
| 6 | Two failed extends in a row (at 2 and 4 min) lost the 5-minute lease | Failed extends retry after 5, 10, 20… s. |
| 7 | `MGET` across Redis Cluster slots fails, so Redis dedupe was effectively off in cluster mode | Keys use a `{namespace}` hash tag. Breaks reading 0.1.x keys (one-time). |
| 8 | Delays over 2^31−1 ms make timers fire immediately, turning backoff into a busy loop | Backoff options are validated; `sleep()` and the stop timer clamp. |
| 9 | Records with 32 headers were rejected because of the automatic `k2kit-id` | The id is skipped when the record has no room. |
| 10 | `k2kit produce` read all stdin first: `tail -f \| k2kit produce` never sent, large input used unbounded memory | Streams with ~4 MB / 1 s-idle batching and backpressure; Ctrl+C flushes. |
| 11 | Duplicates within one batch both reached the handler | Filtered within the batch when dedupe is on. |
| 12 | Ack retry windows compared the local clock with K2's lease time, so clock skew distorted them | Lease timing uses a local-clock deadline (request time + 5 min). |
| 13 | An ack after the lease silently expired was reported as a clean ack | Warned about and counted in `stats().lateAcks`. |
| 14 | `replay --until` stopped at the first record past the cut-off, missing out-of-order records | Skips them; stops only when a whole batch is past `--until`. `--since` must precede `--until`. |
| 15 | Sharing one dedupe store between subscriptions, or reusing a Redis namespace after recreating a subscription, silently skipped records | Sharing a store object across subscriptions is refused; docs recommend the subscription id as namespace. |
| 16 | `produceBatched` not retrying network errors was undocumented | Documented in TSDoc and README (deliberate: the outcome is unknown). |
| 17 | Found while verifying #7: ioredis resolves pipelines even when commands fail, so the documented ioredis adapter silently dropped failed dedupe writes | The adapter (README and `examples/`) now throws on any failed command; node-redis and Upstash already throw. Verified against Redis 8 and a Redis Cluster node in Docker. |

## Edge cases reviewed, round two (2026-10-11)

A second review focused on the HTTP layer, CLI I/O and the new streaming produce. Regression tests are in `test/edge-cases-2.test.ts` and `test/cli.test.ts`.

| # | Edge case | Resolution |
| --- | --- | --- |
| 1 | No request timeout: a connection that never answered hung the worker, `stop()`, and the heartbeat (silently losing the lease) | Per-request timeout (default 30 s, `requestTimeoutMs`), covering the response body; timeouts are retried. `K2TimeoutError` from the client. |
| 2 | A poison record with headers near K2's 64 KiB total couldn't be dead-lettered (verified on real K2: 10204), so it looped forever | Dead-letter headers keep `k2kit-id` and the dlq headers, then as many originals as fit; dropped ones are counted in `k2kit-dlq-dropped-headers`. The fake K2 now enforces K2's header byte limits. `produce` also skips the automatic id when the header bytes are full. |
| 3 | A token with a trailing newline, or an endpoint without a scheme, made `fetch` throw on every request, retried forever | Validated when the client is created (`TypeError`, never echoing the token); a usage error (exit 2) in the CLI. |
| 4 | `tail`/`replay` printed record content and headers raw: escape codes could retitle the terminal or fake a prompt; newlines broke one-line output | Control characters, C1 codes and bidi overrides are escaped in text output; `--json` also escapes C1 and bidi. |
| 5 | `produce` didn't notice a failed send while stdin was idle (`tail -f`), so it sat silently forever | A failed send stops reading immediately and exits 1. |
| 6 | `pollBackoff: { maxMs: 0 }` made the poll loop never yield, freezing the process | Backoff ceilings must be at least 10 ms, and every consumer wait lasts at least 1 ms. |
| 7 | A throwing logger made `run()` reject | Logger calls are wrapped; the first failure is reported to stderr once. |
| 8 | `heartbeatMs: 1` sent ~745 extends per second | `heartbeatMs` must be 1,000–240,000 (the cap leaves time to retry a failed extend). |
| 9 | The capacity hint `subs rm --prefix k2kit-` also deleted other people's running tail/replay sessions | `--older-than`; the hint uses it; a warning lists recent matches. |
| 10 | A CPU-bound handler blocking the event loop silently starved the heartbeat | The heartbeat warns when it runs over 30 s late; documented. |
| 11 | When `produce` stopped on an oversized line, earlier lines had been sent but the count wasn't shown | The produced count is reported on every error. |
| 12 | `--header __proto__=x` was silently dropped | Headers are parsed into a prototype-less object. |
| 13 | A very long `k2kit-id` (up to 8 KiB) was used verbatim as a Redis key | Ids over 128 characters are hashed (`id-fnv:<hash>`). |
| 14 | ISO date-times without a timezone were read as local time, silently | Rejected with a hint; a bare date means midnight UTC. |
| 15 | `--endpoint http://…` sent the token unencrypted | Only `https://`, or `http://` to localhost. |
| 16 | `produce` with nothing piped in waited silently for terminal input | Prints how to finish (Ctrl+D). |
| 17 | An interrupted `produce --raw` said "no input" | Says "interrupted; nothing produced". |
| 18 | `tail` ignored stdout backpressure, so a slow reader made memory grow | Writes wait for stdout to drain before handling more records. |

## Review round three (2026-10-11)

Compared the fake K2 with real K2 (49 raw-HTTP checks, run against both), drove the built CLI with real signals, probed the consumer's lifecycle, and checked the README examples and TSDoc against the code.

**Fake vs real K2:** 45 of 49 checks matched (lease rules, validation, error codes, response shapes). The differences were all about ids. Real K2 ids are 32 lowercase hex characters; a malformed id gets `10204 Request body is invalid` for GET/DELETE subscription and for ack/nack/extend, but `10215` for consume (verified on real K2). The fake used `sub_1`-style ids and accepted anything.

| # | Edge case | Resolution |
| --- | --- | --- |
| 1 | The fake K2 used non-K2 id formats and didn't reject malformed ids, so tests couldn't catch id bugs; its DELETE returned `result: null` instead of `{ id }` | The fake generates 32-hex ids and mirrors real K2's malformed-id responses; a client test pins them. |
| 2 | `k2kit subs rm <name>` with an unknown name fell through to deleting it as an id, and K2 answered "Request body is invalid"; an id pasted in uppercase failed the same way | Unknown names get "no subscription named …"; ids are accepted in any case. |
| 3 | `Consumer({ subscription: { id } })` with a malformed id failed with K2's vague 10204 | A clear error naming the id and the expected format. |
| 4 | Ctrl+C while `tail`/`replay` were creating their temporary subscription exited immediately and leaked it (reproduced with real signals) | Interrupt handling is registered before creation, and creation finishes so the subscription can be deleted (`withTemporarySubscription`). |
| 5 | The interrupt handler was released before deletion finished, so a second Ctrl+C killed the process mid-delete with no message | The handler is held until deletion completes; a forced exit prints "forced exit" and names any leftover subscription with the delete command. |
| 6 | `stop()` before `run()` (a SIGTERM during startup, with the README's own pattern) made `run()` throw "can only be called once" | The first `run()` after an early `stop()` resolves without starting. |
| 7 | `K2Client` TSDoc said network failures surface as raw `fetch` errors, but timeouts throw `K2TimeoutError` | Corrected. |
| 8 | Found in a self-review of #4: if creating the temporary subscription failed *after* K2 applied it (timeout, dropped connection), the CLI exited and the subscription leaked silently | Creation (idempotent) is retried up to 3 times; if the outcome is still unknown, the random name is looked up and deleted, or printed if even the lookup fails. A K2 error code means nothing was created, so no lookup. |

Everything else checked out: concurrent `stop()` calls, `stop()` during subscription resolution, `stop()` after a fatal error, `stop()` from an event listener, a second `run()`, SIGTERM, `replay`/`produce` + Ctrl+C, and `subs ls | head`. All README code blocks parse.

## Review round four (2026-10-12)

Looked at what earlier rounds never exercised: user-supplied code k2kit calls (dedupe stores, dead-letter senders), K2's size limits at the exact boundary, and K2's handling of header names. Each finding was reproduced first, and the size limit was bisected on real K2.

**K2's record size limit, measured on real K2:** a record's content, header names and values, and some encoding overhead must fit within 1,000,000 bytes. The overhead was 14 bytes with no headers (11 in an earlier run), 16–17 with one header, and 47 with ten. It doesn't follow a clean formula and varies by a few bytes, so k2kit uses a conservative estimate, content + header bytes + 32 + 4 per header, which is above every measurement. Request bodies: 4,999,999 bytes accepted; anything over 5,000,000 rejected. Header names keep their case and may contain spaces, Unicode, or be empty; over 256 bytes is rejected.

| # | Edge case | Resolution |
| --- | --- | --- |
| 1 | A dedupe store whose check or write never answered (e.g. Redis down with no client timeout) hung the worker and `stop()` forever | Store calls are limited to `requestTimeoutMs`; a timeout is a dedupe failure (check: process without dedupe; write: still ack). |
| 2 | A dead-letter sender that never answered, even ignoring its abort signal, hung the worker and `stop()` | The send is limited to 4× `requestTimeoutMs` and abandoned when the lease is lost or `stop()` times out; the batch is nacked and the sender's signal aborted. |
| 3 | k2kit checked records against 1,000,000 content bytes, ignoring headers and overhead, so records K2 rejects (10207, non-retryable) were sent and failed their whole request, taking the other records with them | `produceBatched` and CLI `produce` check every record against the estimate (headers included) before sending anything; the automatic `k2kit-id` is skipped when it would push a record over. |
| 4 | Dead-lettering a valid record near 1 MB added headers that pushed it over the limit, so it was rejected and the poison record looped forever | Dead-letter headers fit within all three limits: content always kept, dead-letter headers first (error message shortened if needed), the rest dropped and counted. Verified on real K2 with a near-limit record. |

## Regression check after round four (2026-10-12)

Reviewed every changed line from the previous rounds for bugs the fixes themselves introduced.

| # | Issue | Resolution |
| --- | --- | --- |
| 1 | **Regression from round four:** the timeout wrapper called `.catch()` on whatever a dedupe store or dead-letter sender returned. Synchronous implementations (valid in JavaScript) broke: a sync dedupe store failed every batch (dedupe silently off), and a sync `send` failed every time, so a poison batch was nacked in a loop (37 times in 0.4 s in the probe). | The wrapper accepts plain values (`Promise.resolve`); the `DedupeStore` and `DeadLetterSend` types now allow synchronous implementations. A synchronous throw is handled like a rejection. |
| 3 | Found in a second regression pass: the round-two request timeout only *asked* `fetch` to abort. A custom `fetch` (the public `fetch` option: a wrapper or polyfill) that ignores the signal made the timeout do nothing, so a stalled request could hang again. | The request and the body read are raced against the timeout, so it fires regardless of `fetch`; abandoned promises' late results are ignored. |
| 4 | A malformed `dedupe` option (e.g. a `Map`, which has `has` but no `add`) was accepted and then failed on every batch, silently disabling dedupe | Checked at construction: `has` and `add` must be functions, and `hasMany`/`addMany` if present. A `Set` works and is accepted; `null` means no dedupe. |
| 2 | `subs rm` with a name K2 doesn't allow (space, over 128 characters) still showed K2's vague 10204, because real K2 rejects such a name in the lookup itself (verified). The fake returned an empty list instead, which hid it. | `findSubscription` returns `null` for such names without asking K2; `ensureSubscription` throws a clear `TypeError`. The fake now rejects invalid name filters like real K2. |

## Official docs audit (2026-10-12)

Read the complete K2 documentation ([llms-full.txt](https://developers.cloudflare.com/k2/llms-full.txt): Get started, Concepts, Configuration, Consume, Produce, Limits, Pricing, Delivery guarantees) and compared every statement with k2kit. Lease rules, retry rules, error codes, ack/nack/extend semantics, subscription rules and produce validation all matched. Mismatches and fixes:

| Docs say | Problem in k2kit | Fix |
| --- | --- | --- |
| "Timestamps are written by the server when the batch is received" | Every record in a batch shares a timestamp, so the fallback dedupe key (hash of timestamp + content) made identical records in one batch look like duplicates: with dedupe on, legitimate events were **silently dropped** (and the round-one "within a batch" fix dropped them actively). | Dedupe only applies to records with a producer-set `k2kit-id` (`record.id`), as the Delivery guarantees page recommends ("Include a unique ID in each record"). Records without an id are always processed. |
| The 5 MB request limit includes "a small amount of internal metadata" | The planner filled requests to exactly 5,000,000 bytes. Measured on real K2: 50,000 small records in a ~5,000,000-byte request were accepted; 150,000 in a ~4,990,000-byte request failed with `10212` (outcome unknown, not retryable). | Requests are planned with a 1% byte margin and at most 50,000 records. |
| Per record: ≤32 headers, name ≤256 B, value ≤8 KiB, total ≤64 KiB (UTF-8) | Only the overall record size was checked before sending; a bad header failed its whole request, taking sibling records with it. | `produceBatched` and CLI `--header` check every limit first. |
| Unread records are deleted when retention ends | Not mentioned. | README explains it. |

Follow-up found while checking the dedupe change: identical records share a `key()`, and failure counting incremented that key once per record, so two identical records in a failing batch made `ctx.attempt` go 1, 3, 5 (dead-lettering early). It now counts once per key per delivery.

Billing (Pricing page): K2 bills produced, consumed and retained data, but billing is not enabled during the beta (30 days' notice before it starts). Tests and probes against real K2 now need explicit approval.

## Final edge-case pass (2026-10-12)

| Case | Was | Now |
| --- | --- | --- |
| `content: undefined` or a function (plain JS) | `JSON.stringify` returns `undefined`, so an empty record was produced | `TypeError` naming the record, nothing sent |
| `ArrayBuffer` / `DataView` / `Int16Array` content (TypeScript allows them as `object`) | JSON-stringified to `"{}"` or `{"0":1,…}`, data lost | Sent as their bytes, like a fetch body |
| Header value that isn't a string; headers given as a `Map`, `Headers`, array or string | Sent as is (K2 requires string→string and would reject the request) or silently empty | `TypeError` before sending; `undefined` values are left out as JSON would, and don't count toward the 32-header limit |
| Original header named `constructor`, `toString` or `__proto__` on a dead letter | `name in headers` saw inherited properties: dropped without being counted | Null-prototype object, own-key checks |
| Error message cut at 1,024 characters mid-emoji | Lone surrogate in `k2kit-dlq-error` | Cut before the pair |

| `produce([])` | Sent a request K2 always rejects ("A batch must contain at least one record") | No request, like `produceBatched([])` |
| Sparse records array | `map` skipped the hole, so `null` was sent and the whole request rejected | Reported as `record i must be an object` |
| One record object passed instead of an array | Threw `records.map is not a function` in 0.1.0; briefly (during this pass, after switching to `Array.from` for holes) produced nothing and reported success | `records must be an array`, checked before `Array.from` |
| `pollBackoff: { maxMs: undefined }` (explicit undefined, common when built from env vars) | Spread over the defaults, so `maxMs` was `undefined`, every delay `NaN`, clamped to 0: an idle consumer polled K2 in a tight loop (the test worker ran out of memory) | `definedFields()` drops undefined fields before merging |
| `produceBatched({ retryBackoff: { minMs: 100 } })`, `maxAttempts: NaN` | Partial range had no `maxMs` (NaN delays: retries with no wait); NaN attempts never hit the limit | Merged over defaults; NaN/negative rejected (`0` still allowed) |
| Ack refused with a non-retryable error (e.g. a persistent `10213`) | Reported, then the slot consumed again at once; the worker still held the lease, so K2 returned the same batch: ~10,000 handler calls (and K2 requests) a second until the lease expired | `#ack` returns whether it landed; a failed ack backs off like a failed batch (without counting a handler failure) |
| `new K2Client({ streamId: "attacker.example#" })` | Default endpoint `https://attacker.example#.k2…` has host `attacker.example`: the token went there (consumer and CLI validated; the client didn't) | 32 hex required; uppercase lowercased (worked in 0.1.0, keeps the dead-letter same-stream check exact) |
| Examples | Redis example namespaced by subscription name; basic consumer fell back to a fixed `workerIdPrefix` | Subscription id; default prefix unless `HOSTNAME` is set |
| `estimateRecordBytes` | Referenced by `MAX_RECORD_BYTES` docs but not exported | Exported |
| Upstash adapter write errors | Not covered by a test (ioredis and node-redis were) | Covered; it throws |
| `ttlSeconds` on `RedisDedupeStore` | Docs implied 7 days always suffices | README: use at least the stream's retention (up to 30 days), since a redelivery after a long outage can come later |
| `--since 10` (unit forgotten) | `Date.parse("10")` is 2001-10-01 local time: replay read the whole retained stream | "needs a unit, e.g. 10m"; a lone year is rejected for the same reason |
| `--since "2026-10-11 10:00"`, `"Oct 11 2026"`, `"2026/10/11 10:00"` | Read as local time with no warning (the timezone check only caught the `T` form) | Only durations and ISO forms; a space-separated date-time needs a timezone too; date-times are rebuilt in ECMAScript's required format so every engine agrees |

Each has a regression test that fails without the fix.

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
| Is there an existing community K2 consumer library? | Rechecked 2026-10-11 (npm search, GitHub repository search, Cloudflare's official TypeScript SDK 7.3.0). **JavaScript/TypeScript: none found**, and the official SDK has no K2 resources. **.NET: [soenneker.cloudflare.k2](https://github.com/soenneker/soenneker.cloudflare.k2)** (created 2026-10-01) includes a consumer with polling, lease renewal, retry backoff and concurrency. PyPI, Go and Rust registries were not searched. |
| Does the test account have Workers Paid? | Yes: K2 works on the test account. |
| Does produce accept the `k2kit-id` header? | Yes: confirmed by the integration test on 2026-10-11. |
| Is FNV-1a over `timestamp_ms` + content collision-safe enough? | Documented as a known limit (above and in the README). |
