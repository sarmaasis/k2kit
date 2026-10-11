import { Backoff, clampDelay, sleep } from "./backoff.js";
import { K2Client, K2TimeoutError } from "./client.js";
import type { DeadLetter } from "./dead-letter.js";
import { describeError, isK2Error, isRetryableError, K2Error } from "./errors.js";
import { LEASE_MS, type Lease, namedError, startHeartbeat } from "./heartbeat.js";
import { IdleGate } from "./idle-gate.js";
import {
  type ConsumerOptions,
  defaultWorkerIdPrefix,
  type ErrorContext,
  type ErrorPhase,
  fields,
  type ResolvedOptions,
  resolveOptions,
} from "./options.js";
import type { Batch, DedupeStore, Handler, HandlerContext, K2Record } from "./types.js";

/** Payload of `batch`, `ack` and `nack` events. */
export interface BatchEvent {
  workerId: string;
  batchId: string;
  records: number;
}

/** Event names and listener arguments for {@link Consumer.on}. */
export interface ConsumerEvents {
  /** A non-empty batch was leased. */
  batch: [event: BatchEvent];
  /** A batch was acked. */
  ack: [event: BatchEvent];
  /** A batch was nacked (handler threw, or `stop()` timed out). */
  nack: [event: BatchEvent];
  /** Records from a batch were sent to the dead-letter handler; the batch is then acked. */
  deadLetter: [event: BatchEvent & { deadLettered: number }];
  /** A non-fatal error. */
  error: [err: unknown, ctx: ErrorContext];
  /** A poll returned no records. */
  idle: [event: { workerId: string }];
}

/** A snapshot of consumer activity, for health checks and metrics. */
export interface ConsumerStats {
  /** Lifecycle state. `stopping` means `stop()` was called and in-flight batches are finishing. */
  state: "idle" | "running" | "stopping" | "stopped";
  /** When `run()` was called, epoch ms. */
  startedAt: number | null;
  /** Batches currently being handled. */
  inFlight: number;
  /** Non-empty batches leased. */
  batches: number;
  /** Records in those batches (before dedupe). */
  records: number;
  /** Batches acked. */
  acks: number;
  /** Batches nacked. */
  nacks: number;
  /** Records sent to the dead-letter handler. */
  deadLettered: number;
  /** Non-fatal errors reported. */
  errors: number;
  /** Polls that returned no records. */
  idlePolls: number;
  /** Consume calls refused because all 128 leases on the subscription were taken (`10216`). */
  leaseLimitHits: number;
  /** Acks sent after the lease had probably expired; K2 may have redelivered those records. */
  lateAcks: number;
  /** Epoch ms of the last leased batch, ack and error. */
  lastBatchAt: number | null;
  lastAckAt: number | null;
  lastErrorAt: number | null;
}

/** Options for {@link Consumer.run}. */
export interface RunOptions {
  /** Aborting it calls `stop()` with the default timeout. */
  signal?: AbortSignal;
}

const POISON_ATTEMPTS = 5;
const MAX_TRACKED_FAILURES = 100_000;
const LEASE_LIMIT_CODE = 10216;
const INVALID_REQUEST_CODE = 10204;
const LEASE_LIMIT_WARN_EVERY_MS = 5 * 60_000;
/** After stop()'s timeout, how often to warn while handlers still haven't settled. */
const STUCK_WARN_EVERY_MS = 30_000;

/**
 * Sleeps at least 1 ms, so a loop always yields to the event loop (a zero-length sleep resolves
 * synchronously and could starve timers and I/O).
 */
function pause(ms: number, signal: AbortSignal): Promise<void> {
  return sleep(Math.max(1, ms), signal);
}

/**
 * Resolves or rejects like `value`, but rejects after `ms` (or when `signal` aborts) if it
 * hasn't settled. For calls into user-supplied code (dedupe stores, dead-letter senders) that
 * might never answer. Accepts plain values too, since such code may be synchronous. A late
 * rejection from the abandoned promise is ignored.
 */
function bounded<T>(
  value: T | PromiseLike<T>,
  ms: number,
  what: string,
  signal?: AbortSignal,
): Promise<T> {
  const promise = Promise.resolve(value);
  promise.catch(() => {});
  return new Promise<T>((resolve, reject) => {
    const fail = (reason: unknown) => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      reject(reason);
    };
    const onAbort = () => fail(signal?.reason);
    const timer = setTimeout(
      () => fail(new K2TimeoutError(`k2kit: ${what} didn't finish within ${ms} ms`)),
      clampDelay(ms),
    );
    if (signal?.aborted) return onAbort();
    signal?.addEventListener("abort", onAbort, { once: true });
    promise.then(
      (value) => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (err) => fail(err),
    );
  });
}

/** How much longer than one request a dead-letter send may take (it can span several requests). */
const DEAD_LETTER_TIMEOUT_FACTOR = 4;

/** Which subscription each dedupe store object serves, so one store can't span two. */
const storeOwners = new WeakMap<DedupeStore, string>();

type Listeners = { [K in keyof ConsumerEvents]: Set<(...args: ConsumerEvents[K]) => void> };
type Outcome = "acked" | "failed" | "ackFailed" | "abandoned";

/**
 * Runs handlers over K2 batches: leases a batch, keeps the lease alive while the handler runs,
 * acks on success and nacks on failure, with backoff, concurrency and graceful shutdown.
 */
export class Consumer {
  readonly #client: K2Client;
  readonly #subscription: ConsumerOptions["subscription"];
  readonly #opts: ResolvedOptions;

  readonly #listeners: Listeners = {
    batch: new Set(),
    ack: new Set(),
    nack: new Set(),
    deadLetter: new Set(),
    error: new Set(),
    idle: new Set(),
  };
  readonly #inFlight = new Set<Lease>();
  /** Failed deliveries per record key (bounded LRU), for `ctx.attempt` and dead-lettering. */
  readonly #failures = new Map<string, number>();
  readonly #stopController = new AbortController();
  readonly #idleGate: IdleGate;
  #state: "idle" | "running" | "stopped" = "idle";
  /** stop() ran before run(), e.g. a SIGTERM during startup: run() then does nothing. */
  #stoppedBeforeRun = false;
  #done: Promise<void> = Promise.resolve();
  #fatal: { error: unknown } | undefined;
  #stopTimer: ReturnType<typeof setTimeout> | undefined;
  #stuckTimer: ReturnType<typeof setTimeout> | undefined;
  #lastLeaseLimitWarnAt = -Infinity;
  readonly #stats = {
    startedAt: null as number | null,
    batches: 0,
    records: 0,
    acks: 0,
    nacks: 0,
    deadLettered: 0,
    errors: 0,
    idlePolls: 0,
    leaseLimitHits: 0,
    lateAcks: 0,
    lastBatchAt: null as number | null,
    lastAckAt: null as number | null,
    lastErrorAt: null as number | null,
  };

  constructor(opts: ConsumerOptions) {
    this.#opts = resolveOptions(opts);
    this.#client = new K2Client({
      streamId: opts.streamId,
      token: opts.token,
      ...(opts.endpoint ? { endpoint: opts.endpoint } : {}),
      ...(opts.fetch ? { fetch: opts.fetch } : {}),
      timeoutMs: this.#opts.requestTimeoutMs,
    });
    this.#subscription = opts.subscription;
    this.#idleGate = new IdleGate(this.#opts.sharedIdlePolling);
  }

  /**
   * Starts `concurrency` loops that call `handler` for each batch. Resolves once `stop()` has
   * completed; rejects on a fatal error (a non-retryable K2 error), after stopping all loops.
   * Pass `opts.signal` to stop when it aborts.
   */
  async run(handler: Handler, opts: RunOptions = {}): Promise<void> {
    if (typeof handler !== "function") throw new TypeError("handler must be a function");
    if (this.#stoppedBeforeRun) {
      this.#stoppedBeforeRun = false;
      this.#opts.logger.info("k2kit: stop() was called before run(); not starting");
      return;
    }
    if (this.#state !== "idle") throw new Error("Consumer.run() can only be called once");
    this.#state = "running";
    this.#stats.startedAt = Date.now();
    let release = () => {};
    this.#done = new Promise((resolve) => {
      release = resolve;
    });
    const { signal } = opts;
    const onAbort = () => void this.stop();
    if (signal?.aborted) this.#beginStop(30_000);
    else signal?.addEventListener("abort", onAbort, { once: true });

    try {
      const subId = await this.#resolveSubscription();
      if (subId !== null) this.#claimDedupeStore(subId);
      if (subId !== null && !this.#stopping) {
        const prefix = this.#opts.workerIdPrefix ?? (await defaultWorkerIdPrefix());
        this.#opts.logger.info(
          `k2kit: consumer started subscription=${subId} concurrency=${this.#opts.concurrency} worker_id_prefix=${prefix}`,
        );
        await Promise.all(
          Array.from({ length: this.#opts.concurrency }, (_, i) =>
            this.#slot(`${prefix}-${i}`, subId, handler).catch((err) => this.#setFatal(err)),
          ),
        );
      }
    } catch (err) {
      this.#setFatal(err);
    } finally {
      signal?.removeEventListener("abort", onAbort);
      clearTimeout(this.#stopTimer);
      clearTimeout(this.#stuckTimer);
      this.#state = "stopped";
      this.#opts.logger.info("k2kit: consumer stopped");
      release();
    }
    if (this.#fatal) throw this.#fatal.error;
  }

  /**
   * Stops requesting batches and waits for in-flight handlers, then acks or nacks them.
   * After `timeoutMs` (default 30,000), aborts `ctx.signal`, waits for handlers to settle,
   * and nacks unfinished batches. Never installs process signal handlers.
   *
   * Don't `await` this from inside a handler: `stop()` waits for that handler, so it would
   * never resolve. Call `void consumer.stop()` there instead. Handlers that ignore `ctx.signal`
   * also keep `stop()` waiting; a warning is logged every 30 s while that happens.
   */
  async stop(opts: { timeoutMs?: number } = {}): Promise<void> {
    if (this.#state === "idle") {
      this.#state = "stopped";
      this.#stoppedBeforeRun = true;
      return;
    }
    this.#beginStop(opts.timeoutMs ?? 30_000);
    await this.#done;
  }

  /** Returns a snapshot of counters and timestamps. Cheap; safe to call from a health check. */
  stats(): ConsumerStats {
    const state =
      this.#state === "running" && this.#stopping
        ? "stopping"
        : (this.#state as ConsumerStats["state"]);
    return { state, inFlight: this.#inFlight.size, ...this.#stats };
  }

  /** Subscribes to an event. Listener errors are caught and logged. */
  on<K extends keyof ConsumerEvents>(
    event: K,
    listener: (...args: ConsumerEvents[K]) => void,
  ): this {
    this.#listeners[event].add(listener);
    return this;
  }

  /** Removes a listener added with {@link Consumer.on}. */
  off<K extends keyof ConsumerEvents>(
    event: K,
    listener: (...args: ConsumerEvents[K]) => void,
  ): this {
    this.#listeners[event].delete(listener);
    return this;
  }

  get #stopping(): boolean {
    return this.#stopController.signal.aborted;
  }

  #beginStop(timeoutMs: number): void {
    if (!this.#stopping) {
      this.#opts.logger.info(`k2kit: stopping in_flight=${this.#inFlight.size}`);
      this.#stopController.abort();
    }
    if (this.#stopTimer === undefined && this.#state === "running") {
      this.#stopTimer = setTimeout(
        () => {
          for (const lease of this.#inFlight) {
            lease.timedOut = true;
            lease.controller.abort(namedError("StopTimeoutError", "k2kit: stop() timed out"));
          }
          this.#warnWhileStuck();
        },
        clampDelay(Number.isFinite(timeoutMs) ? timeoutMs : 30_000),
      );
    }
  }

  /** Warns periodically while handlers keep stop() from finishing after its timeout. */
  #warnWhileStuck(): void {
    this.#stuckTimer = setTimeout(() => {
      if (this.#inFlight.size === 0 || this.#state !== "running") return;
      this.#opts.logger.warn(
        `k2kit: stop() is still waiting for ${this.#inFlight.size} handler(s) after its timeout. ` +
          "A handler that ignores ctx.signal, or one that awaits consumer.stop() itself, never finishes; " +
          "inside a handler, call `void consumer.stop()` instead.",
      );
      this.#warnWhileStuck();
    }, STUCK_WARN_EVERY_MS);
  }

  /** Refuses to share one dedupe store object between two subscriptions. */
  #claimDedupeStore(subId: string): void {
    const store = this.#opts.dedupe;
    if (!store) return;
    const owner = storeOwners.get(store);
    if (owner !== undefined && owner !== subId) {
      throw new Error(
        `k2kit: this dedupe store is already used for subscription ${owner}. Each subscription ` +
          "processes every record, so sharing a store would silently skip records. " +
          "Use one store (or one RedisDedupeStore namespace) per subscription.",
      );
    }
    storeOwners.set(store, subId);
  }

  #setFatal(err: unknown): void {
    if (this.#fatal) return;
    this.#fatal = { error: err };
    this.#opts.logger.error(`k2kit: fatal: ${describeError(err)}`);
    this.#beginStop(30_000);
  }

  async #resolveSubscription(): Promise<string | null> {
    const sub = this.#subscription;
    return this.#retrying("consume", "startup", async () => {
      if (!("id" in sub)) return this.#client.ensureSubscription(sub.name, sub.startAt);
      try {
        return (await this.#client.getSubscription(sub.id)).id;
      } catch (err) {
        // K2 answers a malformed id with a vague "Request body is invalid" (10204).
        if (isK2Error(err) && err.code === INVALID_REQUEST_CODE) {
          throw new K2Error({
            code: err.code,
            status: err.status,
            retryable: false,
            message: `subscription id "${sub.id}" is not a valid K2 subscription id (32 lowercase hex characters)`,
          });
        }
        throw err;
      }
    });
  }

  /** Runs `fn`, retrying retryable errors with retry backoff. Returns `null` if stopped. */
  async #retrying<T>(phase: ErrorPhase, workerId: string, fn: () => Promise<T>): Promise<T | null> {
    const retry = new Backoff(this.#opts.retryBackoff);
    while (!this.#stopping) {
      try {
        return await fn();
      } catch (err) {
        if (!isRetryableError(err) || retry.attempt >= this.#opts.retryBackoff.maxAttempts) {
          throw err;
        }
        if (isK2Error(err) && err.code === LEASE_LIMIT_CODE) this.#noteLeaseLimit();
        this.#reportError(err, { phase, workerId });
        await pause(retry.next(), this.#stopController.signal);
      }
    }
    return null;
  }

  #noteLeaseLimit(): void {
    this.#stats.leaseLimitHits++;
    const now = Date.now();
    if (now - this.#lastLeaseLimitWarnAt < LEASE_LIMIT_WARN_EVERY_MS) return;
    this.#lastLeaseLimitWarnAt = now;
    this.#opts.logger.warn(
      "k2kit: all 128 leases on this subscription are taken (K2 10216); workers across all " +
        "processes (processes × concurrency) exceed what K2 allows, so some wait. Lower concurrency.",
    );
  }

  async #slot(workerId: string, subId: string, handler: Handler): Promise<void> {
    const poll = new Backoff(this.#opts.pollBackoff);
    const failures = new Backoff(this.#opts.retryBackoff);
    const gate = this.#idleGate;
    try {
      while (!this.#stopping) {
        if (gate.shouldWait(workerId)) {
          await gate.wait(this.#stopController.signal);
          continue;
        }
        let sentAt = 0;
        const batch = await this.#retrying("consume", workerId, () => {
          sentAt = Date.now();
          return this.#client.consume(subId, workerId, this.#opts.maxRecords);
        });
        if (this.#stopping && batch === null) return;
        if (batch === null) {
          this.#emit("idle", { workerId });
          // The polling slot backs off; the others wait at the gate until it sees records.
          if (gate.markEmpty(workerId)) await pause(poll.next(), this.#stopController.signal);
          continue;
        }
        gate.markBusy();
        poll.reset();
        const outcome = await this.#process(workerId, subId, batch, handler, sentAt);
        if (outcome === "failed" || outcome === "ackFailed") {
          // Back off after a failed batch so a poison batch does not spin against K2. Likewise
          // after a failed ack: this worker still holds the lease, so K2 would hand the same
          // batch straight back and it would be reprocessed in a tight loop.
          await pause(failures.next(), this.#stopController.signal);
        } else {
          failures.reset();
        }
      }
    } finally {
      gate.release(workerId);
    }
  }

  /** Handles one batch end to end: dedupe, handler, heartbeat, dead-letter, ack or nack. */
  async #process(
    workerId: string,
    subId: string,
    batch: Batch,
    handler: Handler,
    sentAt: number,
  ): Promise<Outcome> {
    const info: BatchEvent = { workerId, batchId: batch.id, records: batch.records.length };
    this.#opts.logger.debug(`k2kit: batch ${fields(info)}`);
    this.#emit("batch", info);

    const attempt = this.#attemptOf(batch.records);
    const deadLetter = this.#opts.deadLetter;
    if (!deadLetter && attempt >= POISON_ATTEMPTS) {
      this.#opts.logger.warn(
        `k2kit: possible poison batch ${fields(info)} attempt=${attempt}; set the deadLetter option to divert failing records`,
      );
    }

    const lease: Lease = {
      subId,
      batchId: batch.id,
      workerId,
      controller: new AbortController(),
      leasedUntilMs: batch.leasedUntilMs,
      // K2 starts the lease when it receives the request, so this is a safe lower bound.
      localDeadline: sentAt + LEASE_MS,
      lost: false,
      timedOut: false,
    };
    this.#inFlight.add(lease);
    try {
      const records = await this.#filterDuplicates(batch.records, info);
      if (records.length === 0) {
        this.#opts.logger.debug(`k2kit: all records already processed, acking ${fields(info)}`);
        return (await this.#ack(batch.records, info, lease)) ? "acked" : "ackFailed";
      }

      const ctx: HandlerContext = { workerId, signal: lease.controller.signal, attempt };
      const stopHeartbeat = startHeartbeat(this.#client, lease, this.#opts.heartbeatMs, {
        onExtended: () => this.#opts.logger.debug(`k2kit: lease extended ${fields(info)}`),
        onError: (err) => this.#reportError(err, { phase: "extend", ...info }),
        onLate: (lateMs) =>
          this.#opts.logger.warn(
            `k2kit: lease heartbeat ran ${Math.round(lateMs / 1000)} s late ${fields(info)}; the event loop was blocked ` +
              "(CPU-heavy handler?), so the lease may expire. Move CPU-bound work to a worker thread.",
          ),
      });
      let failure: { error: unknown } | undefined;
      let isolated = false;
      try {
        try {
          await handler({ id: batch.id, leasedUntilMs: batch.leasedUntilMs, records }, ctx);
        } catch (error) {
          failure = { error };
        }
        if (failure && deadLetter && attempt >= deadLetter.afterAttempts) {
          isolated = await this.#isolate(batch, records, ctx, lease, handler, info);
        }
      } finally {
        stopHeartbeat();
      }

      if (lease.lost) {
        // A delivery that never completed counts toward ctx.attempt (a stop() timeout doesn't).
        this.#countFailure(batch.records);
        this.#opts.logger.warn(`k2kit: lease lost; K2 will redeliver ${fields(info)}`);
        return "abandoned";
      }
      if (lease.timedOut) {
        this.#opts.logger.warn(`k2kit: stop() timed out; nacking ${fields(info)}`);
        await this.#nack(subId, info);
        return "abandoned";
      }
      if (failure && !isolated) {
        await this.#nack(subId, info);
        this.#countFailure(batch.records);
        this.#reportError(failure.error, { phase: "handler", ...info });
        return "failed";
      }
      if (Date.now() >= lease.localDeadline) {
        // The heartbeat couldn't keep the lease alive (extends kept failing), so K2 may already
        // have redelivered these records. Ack anyway (harmless if so) and record the keys so
        // dedupe can skip the redelivery, but say so instead of reporting a clean ack.
        this.#stats.lateAcks++;
        this.#opts.logger.warn(
          `k2kit: lease probably expired before ack; K2 may redeliver ${fields(info)} (enable dedupe or keep handlers idempotent)`,
        );
      }
      await this.#remember(batch.records, info);
      return (await this.#ack(batch.records, info, lease)) ? "acked" : "ackFailed";
    } finally {
      this.#inFlight.delete(lease);
    }
  }

  /**
   * Retries a failing batch one record at a time and sends the records that still fail to the
   * dead-letter handler. Returns `true` when the batch can be acked.
   */
  async #isolate(
    batch: Batch,
    records: K2Record[],
    ctx: HandlerContext,
    lease: Lease,
    handler: Handler,
    info: BatchEvent,
  ): Promise<boolean> {
    const deadLetter = this.#opts.deadLetter;
    if (!deadLetter) return false;
    const letters: DeadLetter[] = [];
    for (const record of records) {
      if (lease.controller.signal.aborted) return false;
      try {
        await handler({ id: batch.id, leasedUntilMs: lease.leasedUntilMs, records: [record] }, ctx);
      } catch (error) {
        letters.push({ record, error, attempt: ctx.attempt });
      }
    }
    if (lease.controller.signal.aborted) return false;
    if (letters.length === 0) return true;
    // Bounded: a sender that never answers (even ignoring the signal) must not hold the worker.
    const sendCtl = new AbortController();
    const onLeaseAbort = () => sendCtl.abort(lease.controller.signal.reason);
    lease.controller.signal.addEventListener("abort", onLeaseAbort, { once: true });
    try {
      await bounded(
        deadLetter.send(letters, {
          workerId: info.workerId,
          batchId: info.batchId,
          signal: sendCtl.signal,
        }),
        this.#opts.requestTimeoutMs * DEAD_LETTER_TIMEOUT_FACTOR,
        "dead-letter send",
        sendCtl.signal,
      ).catch((err) => {
        sendCtl.abort(err); // tell a still-running sender to stop
        throw err;
      });
    } catch (err) {
      // Nothing is dropped: the batch is nacked and the records come back. If the send actually
      // landed after the timeout, the redelivery may be dead-lettered again (same k2kit-id).
      this.#reportError(err, { phase: "deadLetter", ...info });
      return false;
    } finally {
      lease.controller.signal.removeEventListener("abort", onLeaseAbort);
    }
    this.#stats.deadLettered += letters.length;
    this.#opts.logger.warn(
      `k2kit: dead-lettered ${letters.length} of ${records.length} records ${fields(info)} attempt=${ctx.attempt}`,
    );
    this.#emit("deadLetter", { ...info, deadLettered: letters.length });
    return true;
  }

  /**
   * Drops records already processed. Only records with a producer-set id take part: K2 gives
   * every record in a produce batch the same timestamp, so identical records without an id
   * can't be told apart from a redelivery, and skipping them could lose data.
   */
  async #filterDuplicates(records: K2Record[], info: BatchEvent): Promise<K2Record[]> {
    const dedupe = this.#opts.dedupe;
    const withId = records.filter((r) => r.id !== undefined);
    if (!dedupe || withId.length === 0) return records;
    try {
      const keys = withId.map((r) => r.key());
      // Bounded: a store that never answers (e.g. Redis down with no client timeout) must not
      // hold the worker; on timeout the batch is processed without dedupe, like any store error.
      const seen = await bounded(
        dedupe.hasMany ? dedupe.hasMany(keys) : Promise.all(keys.map((k) => dedupe.has(k))),
        this.#opts.requestTimeoutMs,
        "dedupe store check",
      );
      // Skip ids already processed, and repeats of the same id within this batch (e.g. a
      // producer retry that landed next to the original).
      const skip = new Set<K2Record>();
      const inBatch = new Set<string>();
      withId.forEach((record, i) => {
        const key = keys[i] as string;
        if (seen[i] || inBatch.has(key)) skip.add(record);
        else inBatch.add(key);
      });
      return records.filter((r) => !skip.has(r));
    } catch (err) {
      // Dedupe is an optimisation over at-least-once delivery; process everything if it fails.
      this.#reportError(err, { phase: "dedupe", ...info });
      return records;
    }
  }

  async #remember(records: K2Record[], info: BatchEvent): Promise<void> {
    const dedupe = this.#opts.dedupe;
    const keys = records.filter((r) => r.id !== undefined).map((r) => r.key());
    if (!dedupe || keys.length === 0) return;
    try {
      await bounded(
        dedupe.addMany
          ? dedupe.addMany(keys)
          : Promise.all(keys.map((k) => dedupe.add(k))).then(() => {}),
        this.#opts.requestTimeoutMs,
        "dedupe store write",
      );
    } catch (err) {
      this.#reportError(err, { phase: "dedupe", ...info });
    }
  }

  /** Acks, retrying retryable errors until the lease expires. Returns whether the ack landed. */
  async #ack(records: K2Record[], info: BatchEvent, lease: Lease): Promise<boolean> {
    const retry = new Backoff(this.#opts.retryBackoff);
    for (;;) {
      try {
        await this.#client.ack(lease.subId, info.batchId, info.workerId);
        this.#opts.logger.debug(`k2kit: ack ${fields(info)}`);
        for (const r of records) this.#failures.delete(r.key());
        this.#emit("ack", info);
        return true;
      } catch (err) {
        const delay = retry.next();
        // Local clock only: comparing Date.now() with K2's leasedUntilMs would be skewed.
        const expired = Date.now() + delay >= lease.localDeadline;
        if (!isRetryableError(err) || expired || lease.controller.signal.aborted) {
          // Safe: K2 redelivers after the lease expires, and dedupe (if set) skips the records.
          this.#reportError(err, { phase: "ack", ...info });
          return false;
        }
        this.#opts.logger.debug(`k2kit: ack retry ${fields(info)}: ${describeError(err)}`);
        await pause(delay, lease.controller.signal);
      }
    }
  }

  async #nack(subId: string, info: BatchEvent): Promise<void> {
    try {
      await this.#client.nack(subId, info.batchId, info.workerId);
      this.#opts.logger.debug(`k2kit: nack ${fields(info)}`);
    } catch (err) {
      // Lease expiry redelivers anyway.
      this.#opts.logger.warn(`k2kit: nack failed ${fields(info)}: ${describeError(err)}`);
    }
    this.#emit("nack", info);
  }

  /** 1 + the most failed deliveries seen for any record in the batch. */
  #attemptOf(records: K2Record[]): number {
    let most = 0;
    for (const r of records) most = Math.max(most, this.#failures.get(r.key()) ?? 0);
    return most + 1;
  }

  #countFailure(records: K2Record[]): void {
    // Once per key: identical records in one batch share a key, and this is one delivery.
    for (const key of new Set(records.map((r) => r.key()))) {
      const failures = (this.#failures.get(key) ?? 0) + 1;
      this.#failures.delete(key);
      this.#failures.set(key, failures);
    }
    while (this.#failures.size > MAX_TRACKED_FAILURES) {
      const oldest = this.#failures.keys().next();
      if (oldest.done) break;
      this.#failures.delete(oldest.value);
    }
  }

  #reportError(err: unknown, ctx: ErrorContext): void {
    this.#emit("error", err, ctx);
    try {
      this.#opts.onError(err, ctx);
    } catch (hookErr) {
      this.#opts.logger.error(`k2kit: onError threw: ${describeError(hookErr)}`);
    }
  }

  #emit<K extends keyof ConsumerEvents>(event: K, ...args: ConsumerEvents[K]): void {
    const now = Date.now();
    const stats = this.#stats;
    if (event === "batch") {
      stats.batches++;
      stats.records += (args[0] as BatchEvent).records;
      stats.lastBatchAt = now;
    } else if (event === "ack") {
      stats.acks++;
      stats.lastAckAt = now;
    } else if (event === "nack") {
      stats.nacks++;
    } else if (event === "error") {
      stats.errors++;
      stats.lastErrorAt = now;
    } else if (event === "idle") {
      stats.idlePolls++;
    }
    for (const listener of this.#listeners[event]) {
      try {
        listener(...args);
      } catch (err) {
        this.#opts.logger.error(`k2kit: ${event} listener threw: ${describeError(err)}`);
      }
    }
  }
}
