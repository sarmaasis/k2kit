import { Backoff, sleep } from "./backoff.js";
import { K2Client } from "./client.js";
import type { DeadLetter } from "./dead-letter.js";
import { describeError, isK2Error, isRetryableError } from "./errors.js";
import { type Lease, namedError, startHeartbeat } from "./heartbeat.js";
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
import type { Batch, Handler, HandlerContext, K2Record } from "./types.js";

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
const LEASE_LIMIT_WARN_EVERY_MS = 5 * 60_000;

type Listeners = { [K in keyof ConsumerEvents]: Set<(...args: ConsumerEvents[K]) => void> };
type Outcome = "acked" | "failed" | "abandoned";

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
  #done: Promise<void> = Promise.resolve();
  #fatal: { error: unknown } | undefined;
  #stopTimer: ReturnType<typeof setTimeout> | undefined;
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
   */
  async stop(opts: { timeoutMs?: number } = {}): Promise<void> {
    if (this.#state === "idle") {
      this.#state = "stopped";
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
      this.#stopTimer = setTimeout(() => {
        for (const lease of this.#inFlight) {
          lease.timedOut = true;
          lease.controller.abort(namedError("StopTimeoutError", "k2kit: stop() timed out"));
        }
      }, timeoutMs);
    }
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
      if ("id" in sub) return (await this.#client.getSubscription(sub.id)).id;
      return this.#client.ensureSubscription(sub.name, sub.startAt);
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
        await sleep(retry.next(), this.#stopController.signal);
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
        const batch = await this.#retrying("consume", workerId, () =>
          this.#client.consume(subId, workerId, this.#opts.maxRecords),
        );
        if (this.#stopping && batch === null) return;
        if (batch === null) {
          this.#emit("idle", { workerId });
          // The polling slot backs off; the others wait at the gate until it sees records.
          if (gate.markEmpty(workerId)) await sleep(poll.next(), this.#stopController.signal);
          continue;
        }
        gate.markBusy();
        poll.reset();
        if ((await this.#process(workerId, subId, batch, handler)) === "failed") {
          // Back off after a failed batch so a poison batch does not spin against K2.
          await sleep(failures.next(), this.#stopController.signal);
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
      lost: false,
      timedOut: false,
    };
    this.#inFlight.add(lease);
    try {
      const records = await this.#filterDuplicates(batch.records, info);
      if (records.length === 0) {
        this.#opts.logger.debug(`k2kit: all records already processed, acking ${fields(info)}`);
        await this.#ack(batch.records, info, lease);
        return "acked";
      }

      const ctx: HandlerContext = { workerId, signal: lease.controller.signal, attempt };
      const stopHeartbeat = startHeartbeat(this.#client, lease, this.#opts.heartbeatMs, {
        onExtended: () => this.#opts.logger.debug(`k2kit: lease extended ${fields(info)}`),
        onError: (err) => this.#reportError(err, { phase: "extend", ...info }),
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
      await this.#remember(batch.records, info);
      await this.#ack(batch.records, info, lease);
      return "acked";
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
    try {
      await deadLetter.send(letters, {
        workerId: info.workerId,
        batchId: info.batchId,
        signal: lease.controller.signal,
      });
    } catch (err) {
      // Nothing is dropped: the batch is nacked and the records come back.
      this.#reportError(err, { phase: "deadLetter", ...info });
      return false;
    }
    this.#stats.deadLettered += letters.length;
    this.#opts.logger.warn(
      `k2kit: dead-lettered ${letters.length} of ${records.length} records ${fields(info)} attempt=${ctx.attempt}`,
    );
    this.#emit("deadLetter", { ...info, deadLettered: letters.length });
    return true;
  }

  async #filterDuplicates(records: K2Record[], info: BatchEvent): Promise<K2Record[]> {
    const dedupe = this.#opts.dedupe;
    if (!dedupe) return records;
    try {
      const keys = records.map((r) => r.key());
      const seen = dedupe.hasMany
        ? await dedupe.hasMany(keys)
        : await Promise.all(keys.map((k) => dedupe.has(k)));
      return records.filter((_, i) => !seen[i]);
    } catch (err) {
      // Dedupe is an optimisation over at-least-once delivery; process everything if it fails.
      this.#reportError(err, { phase: "dedupe", ...info });
      return records;
    }
  }

  async #remember(records: K2Record[], info: BatchEvent): Promise<void> {
    const dedupe = this.#opts.dedupe;
    if (!dedupe) return;
    try {
      const keys = records.map((r) => r.key());
      if (dedupe.addMany) await dedupe.addMany(keys);
      else await Promise.all(keys.map((k) => dedupe.add(k)));
    } catch (err) {
      this.#reportError(err, { phase: "dedupe", ...info });
    }
  }

  /** Acks, retrying retryable errors until the lease expires. */
  async #ack(records: K2Record[], info: BatchEvent, lease: Lease): Promise<void> {
    const retry = new Backoff(this.#opts.retryBackoff);
    for (;;) {
      try {
        await this.#client.ack(lease.subId, info.batchId, info.workerId);
        this.#opts.logger.debug(`k2kit: ack ${fields(info)}`);
        for (const r of records) this.#failures.delete(r.key());
        this.#emit("ack", info);
        return;
      } catch (err) {
        const delay = retry.next();
        const expired = Date.now() + delay >= lease.leasedUntilMs;
        if (!isRetryableError(err) || expired || lease.controller.signal.aborted) {
          // Safe: K2 redelivers after the lease expires, and dedupe (if set) skips the records.
          this.#reportError(err, { phase: "ack", ...info });
          return;
        }
        this.#opts.logger.debug(`k2kit: ack retry ${fields(info)}: ${describeError(err)}`);
        await sleep(delay, lease.controller.signal);
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
    for (const r of records) {
      const key = r.key();
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
