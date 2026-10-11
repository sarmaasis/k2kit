import type { BackoffRange } from "./backoff.js";
import type { DeadLetterOptions } from "./dead-letter.js";
import { describeError } from "./errors.js";
import type { DedupeStore, Logger, StartAt } from "./types.js";

/** Backoff for retryable errors, with an optional cap on consecutive attempts. */
export interface RetryBackoff extends BackoffRange {
  /** Consecutive retryable consume failures tolerated before `run()` rejects. Default `Infinity`. */
  maxAttempts: number;
}

/** Which step a non-fatal error came from. */
export type ErrorPhase =
  | "consume"
  | "handler"
  | "ack"
  | "nack"
  | "extend"
  | "dedupe"
  | "deadLetter";

/** Context passed to `onError` and `error` listeners. */
export interface ErrorContext {
  phase: ErrorPhase;
  workerId: string;
  batchId?: string;
  records?: number;
}

/** Options for {@link Consumer}. */
export interface ConsumerOptions {
  /** K2 stream id (32 lowercase hex chars). */
  streamId: string;
  /** Cloudflare API token with the K2 Consume permission. Never logged. */
  token: string;
  /**
   * The subscription to consume. `{ name, startAt }` creates it if needed (idempotent);
   * `{ id }` uses an existing one.
   */
  subscription: { name: string; startAt: StartAt } | { id: string };
  /** Parallel loops, each with its own `worker_id` and lease. 1–128. Default 1. */
  concurrency?: number;
  /**
   * Prefix for worker ids (`${prefix}-${slot}`). Default `${hostname}-${pid}`. Set a stable,
   * unique value (such as the pod name) to recover leases after a restart; never share it
   * between two live processes.
   */
  workerIdPrefix?: string;
  /** Maximum records per batch. 1–10,000. Default 100. */
  maxRecords?: number;
  /** Jittered exponential backoff after empty polls. Default `{ minMs: 250, maxMs: 10_000 }`. */
  pollBackoff?: Partial<BackoffRange>;
  /**
   * When the stream is quiet: `"shared"` (default) keeps one slot polling and wakes the others
   * when it sees records; `"every-slot"` has every slot poll on its own backoff.
   */
  idlePolling?: "shared" | "every-slot";
  /** Backoff after retryable errors. Default `{ minMs: 500, maxMs: 30_000, maxAttempts: Infinity }`. */
  retryBackoff?: Partial<RetryBackoff>;
  /** Lease extend interval while a handler runs. Must be under 5 minutes. Default 120,000. */
  heartbeatMs?: number;
  /** Skips records whose `key()` was already processed. K2 delivers at least once. */
  dedupe?: DedupeStore;
  /**
   * Diverts records that keep failing: from attempt `afterAttempts` (default 5), a failing batch
   * is retried one record at a time, records that still fail go to `send`, and the batch is acked.
   * Without it, failing records are retried forever.
   */
  deadLetter?: DeadLetterOptions;
  /** Called for non-fatal errors. Default logs one line through `logger`. */
  onError?: (err: unknown, ctx: ErrorContext) => void;
  /** Logger. Default writes `info` and above to stderr. */
  logger?: Logger;
  /** Override the data-plane base URL, e.g. for tests. */
  endpoint?: string;
  /** Custom `fetch`, e.g. for tests. */
  fetch?: typeof fetch;
}

/** {@link ConsumerOptions} with defaults applied. */
export interface ResolvedOptions {
  concurrency: number;
  workerIdPrefix: string | undefined;
  maxRecords: number;
  pollBackoff: BackoffRange;
  sharedIdlePolling: boolean;
  retryBackoff: RetryBackoff;
  heartbeatMs: number;
  dedupe: DedupeStore | undefined;
  deadLetter: Required<DeadLetterOptions> | undefined;
  logger: Logger;
  onError: (err: unknown, ctx: ErrorContext) => void;
}

/** Default logger: `info` and above to stderr, so stdout stays free for data. */
export const consoleLogger: Logger = {
  debug: () => {},
  info: (m) => console.error(m),
  warn: (m) => console.warn(m),
  error: (m) => console.error(m),
};

export const LEASE_MS = 5 * 60_000;
export const MAX_WORKER_ID = 256;
const STREAM_ID = /^[0-9a-f]{32}$/;
const SUB_NAME = /^[A-Za-z0-9_-]{1,128}$/;

/** One log line's identifying fields. */
export function fields(f: { workerId: string; batchId?: string; records?: number }): string {
  let line = `worker_id=${f.workerId}`;
  if (f.batchId !== undefined) line += ` batch_id=${f.batchId}`;
  if (f.records !== undefined) line += ` records=${f.records}`;
  return line;
}

/** Validates options (throwing `TypeError`) and applies defaults. */
export function resolveOptions(opts: ConsumerOptions): ResolvedOptions {
  validate(opts);
  const logger = opts.logger ?? consoleLogger;
  const deadLetter = opts.deadLetter;
  return {
    concurrency: opts.concurrency ?? 1,
    workerIdPrefix: opts.workerIdPrefix,
    maxRecords: opts.maxRecords ?? 100,
    pollBackoff: { minMs: 250, maxMs: 10_000, ...opts.pollBackoff },
    sharedIdlePolling: (opts.idlePolling ?? "shared") === "shared",
    retryBackoff: { minMs: 500, maxMs: 30_000, maxAttempts: Infinity, ...opts.retryBackoff },
    heartbeatMs: opts.heartbeatMs ?? 120_000,
    dedupe: opts.dedupe,
    deadLetter: deadLetter && {
      afterAttempts: deadLetter.afterAttempts ?? 5,
      send: deadLetter.send,
    },
    logger,
    onError:
      opts.onError ??
      ((err, ctx) => {
        const line = `k2kit: ${ctx.phase} error ${fields(ctx)}: ${describeError(err)}`;
        if (ctx.phase === "handler") logger.error(line);
        else logger.warn(line);
      }),
  };
}

/** `${hostname}-${pid}`, falling back when `node:os` or `process` is unavailable. */
export async function defaultWorkerIdPrefix(): Promise<string> {
  let host = "k2kit";
  try {
    // Via a variable so bundlers keep the `node:` prefix (Deno needs it) and don't inline it.
    const specifier = "node:os";
    const os: { hostname(): string } = await import(specifier);
    host = os.hostname() || host;
  } catch {
    // Runtime without node:os (or Deno without --allow-sys); keep the fallback host.
  }
  const pid = globalThis.process?.pid ?? globalThis.crypto.randomUUID().slice(0, 8);
  return `${host}-${pid}`.replace(/\s+/g, "-").slice(0, MAX_WORKER_ID - 4);
}

function validate(opts: ConsumerOptions): void {
  if (typeof opts !== "object" || opts === null) throw new TypeError("options are required");
  if (typeof opts.streamId !== "string" || !STREAM_ID.test(opts.streamId)) {
    throw new TypeError("streamId must be 32 lowercase hex characters");
  }
  if (typeof opts.token !== "string" || opts.token.length === 0) {
    throw new TypeError("token is required");
  }
  const sub = opts.subscription as { id?: unknown; name?: unknown; startAt?: unknown } | undefined;
  if (typeof sub !== "object" || sub === null) {
    throw new TypeError("subscription must be { name, startAt } or { id }");
  }
  if ("id" in sub) {
    if (typeof sub.id !== "string" || sub.id.length === 0) {
      throw new TypeError("subscription.id must be a non-empty string");
    }
  } else {
    if (typeof sub.name !== "string" || !SUB_NAME.test(sub.name)) {
      throw new TypeError("subscription.name must be 1–128 letters, digits, _ or -");
    }
    if (sub.startAt !== "earliest" && sub.startAt !== "latest") {
      throw new TypeError('subscription.startAt must be "earliest" or "latest"');
    }
  }
  intInRange("concurrency", opts.concurrency, 1, 128);
  intInRange("maxRecords", opts.maxRecords, 1, 10_000);
  intInRange("heartbeatMs", opts.heartbeatMs, 1, LEASE_MS - 1);
  if (opts.workerIdPrefix !== undefined) {
    if (
      typeof opts.workerIdPrefix !== "string" ||
      opts.workerIdPrefix.length === 0 ||
      opts.workerIdPrefix.length > MAX_WORKER_ID - 4
    ) {
      throw new TypeError(`workerIdPrefix must be 1–${MAX_WORKER_ID - 4} characters`);
    }
  }
  if (opts.idlePolling !== undefined && !["shared", "every-slot"].includes(opts.idlePolling)) {
    throw new TypeError('idlePolling must be "shared" or "every-slot"');
  }
  for (const [name, range] of [
    ["pollBackoff", opts.pollBackoff],
    ["retryBackoff", opts.retryBackoff],
  ] as const) {
    if (range === undefined) continue;
    const { minMs, maxMs } = range;
    if (minMs !== undefined && !(minMs >= 0)) throw new TypeError(`${name}.minMs must be >= 0`);
    if (maxMs !== undefined && !(maxMs >= (minMs ?? 0))) {
      throw new TypeError(`${name}.maxMs must be >= minMs`);
    }
  }
  const maxAttempts = opts.retryBackoff?.maxAttempts;
  if (maxAttempts !== undefined && !(maxAttempts >= 0)) {
    throw new TypeError("retryBackoff.maxAttempts must be >= 0");
  }
  if (opts.deadLetter !== undefined) {
    if (typeof opts.deadLetter?.send !== "function") {
      throw new TypeError("deadLetter.send must be a function");
    }
    intInRange("deadLetter.afterAttempts", opts.deadLetter.afterAttempts, 1, 1_000);
  }
  if (opts.onError !== undefined && typeof opts.onError !== "function") {
    throw new TypeError("onError must be a function");
  }
}

function intInRange(name: string, value: unknown, min: number, max: number): void {
  if (value === undefined) return;
  if (typeof value !== "number" || !Number.isInteger(value) || value < min || value > max) {
    throw new TypeError(`${name} must be an integer from ${min} to ${max}`);
  }
}
