import { type BackoffRange, definedFields, MAX_TIMER_MS } from "./backoff.js";
import { type DeadLetterOptions, deadLetterTargetStream } from "./dead-letter.js";
import { describeError } from "./errors.js";
import { LEASE_MS } from "./heartbeat.js";
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
   * Prefix for worker ids (`${prefix}-${slot}`). Default `${hostname}-${pid}-${random}`, unique
   * per consumer instance. Set a stable, unique value (such as the pod name) to recover leases
   * after a restart; never share it between two live consumers.
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
  /**
   * Lease extend interval while a handler runs, 1,000–240,000 ms (leases last 5 minutes; the cap
   * leaves room to retry a failed extend). Default 120,000.
   */
  heartbeatMs?: number;
  /** Per-request timeout for K2 calls, in ms. Timed-out requests are retried. Default 30,000. */
  requestTimeoutMs?: number;
  /**
   * Skips records whose `k2kit-id` was already processed. K2 delivers at least once. Records
   * without an id are always processed.
   */
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
  requestTimeoutMs: number;
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

export const MAX_WORKER_ID = 256;
/** Smallest backoff ceiling allowed, so an idle or failing consumer can't spin. */
export const MIN_BACKOFF_CEILING_MS = 10;
const MAX_HEARTBEAT_MS = 240_000;

/**
 * Wraps a logger so a throwing logger (say, a closed log transport) can't break the consumer.
 * Logging failures are reported once to stderr and otherwise ignored.
 */
function safeLogger(logger: Logger): Logger {
  let reported = false;
  const wrap =
    (fn: (m: string) => void) =>
    (message: string): void => {
      try {
        fn.call(logger, message);
      } catch (err) {
        if (reported) return;
        reported = true;
        try {
          console.error(
            `k2kit: logger threw, ignoring logger errors from now on: ${describeError(err)}`,
          );
        } catch {
          // Nothing left to report to.
        }
      }
    };
  return {
    debug: wrap(logger.debug),
    info: wrap(logger.info),
    warn: wrap(logger.warn),
    error: wrap(logger.error),
  };
}
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
  const logger = opts.logger ? safeLogger(opts.logger) : consoleLogger;
  const deadLetter = opts.deadLetter;
  return {
    concurrency: opts.concurrency ?? 1,
    workerIdPrefix: opts.workerIdPrefix,
    maxRecords: opts.maxRecords ?? 100,
    pollBackoff: { minMs: 250, maxMs: 10_000, ...definedFields(opts.pollBackoff) },
    sharedIdlePolling: (opts.idlePolling ?? "shared") === "shared",
    retryBackoff: {
      minMs: 500,
      maxMs: 30_000,
      maxAttempts: Infinity,
      ...definedFields(opts.retryBackoff),
    },
    heartbeatMs: opts.heartbeatMs ?? 120_000,
    requestTimeoutMs: opts.requestTimeoutMs ?? 30_000,
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

/**
 * `${hostname}-${pid}-${random}`. The random part keeps two consumers apart even when they
 * share a hostname and pid (two instances in one process, or containers where every pid is 1).
 */
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
  const pid = globalThis.process?.pid ?? "nopid";
  const random = globalThis.crypto.randomUUID().slice(0, 8);
  // Trim the host, not the random part, so the prefix stays unique within worker_id's 256 chars.
  const tail = `-${pid}-${random}`;
  return `${host.replace(/\s+/g, "-").slice(0, MAX_WORKER_ID - 4 - tail.length)}${tail}`;
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
  // Below 1 s the heartbeat floods K2; above 4 min a failed extend can't be retried in time.
  intInRange("heartbeatMs", opts.heartbeatMs, 1_000, Math.min(MAX_HEARTBEAT_MS, LEASE_MS - 1));
  intInRange("requestTimeoutMs", opts.requestTimeoutMs, 1, MAX_TIMER_MS);
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
    if (minMs !== undefined && !(minMs >= 0 && minMs <= MAX_TIMER_MS)) {
      throw new TypeError(`${name}.minMs must be from 0 to ${MAX_TIMER_MS}`);
    }
    if (maxMs !== undefined && !(maxMs >= (minMs ?? 0) && maxMs <= MAX_TIMER_MS)) {
      throw new TypeError(`${name}.maxMs must be >= minMs and <= ${MAX_TIMER_MS} (~24.8 days)`);
    }
    if (maxMs !== undefined && maxMs < MIN_BACKOFF_CEILING_MS) {
      throw new TypeError(
        `${name}.maxMs must be at least ${MIN_BACKOFF_CEILING_MS} ms; lower values make the consumer spin`,
      );
    }
  }
  const maxAttempts = opts.retryBackoff?.maxAttempts;
  if (maxAttempts !== undefined && !(maxAttempts >= 0)) {
    throw new TypeError("retryBackoff.maxAttempts must be >= 0");
  }
  const dedupe = opts.dedupe as { [method: string]: unknown } | null | undefined;
  if (dedupe !== undefined && dedupe !== null) {
    // Caught here rather than failing (and silently disabling dedupe) on every batch.
    if (typeof dedupe.has !== "function" || typeof dedupe.add !== "function") {
      throw new TypeError("dedupe must have has() and add() methods (a DedupeStore)");
    }
    for (const method of ["hasMany", "addMany"]) {
      if (dedupe[method] !== undefined && typeof dedupe[method] !== "function") {
        throw new TypeError(`dedupe.${method} must be a function when present`);
      }
    }
  }
  if (opts.deadLetter !== undefined) {
    if (typeof opts.deadLetter?.send !== "function") {
      throw new TypeError("deadLetter.send must be a function");
    }
    if (deadLetterTargetStream(opts.deadLetter.send) === opts.streamId) {
      throw new TypeError(
        "deadLetter: deadLetterToStream() targets the stream being consumed, so dead letters " +
          "would be consumed and fail again in a loop. Use a separate dead-letter stream.",
      );
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
