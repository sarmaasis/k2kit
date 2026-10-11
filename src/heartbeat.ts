import type { K2Client } from "./client.js";
import { isK2Error, LEASE_LOST_CODE } from "./errors.js";

/** How long a K2 lease lasts after a consume or extend request. */
export const LEASE_MS = 5 * 60_000;
/** First retry delay after a failed extend; doubles each time, capped at the heartbeat interval. */
const EXTEND_RETRY_MS = 5_000;
/** A tick this much later than scheduled means the event loop was blocked. */
const LATE_WARN_MS = 30_000;

/** Mutable lease state shared between a batch's handler run and its heartbeat. */
export interface Lease {
  subId: string;
  batchId: string;
  workerId: string;
  /** Aborted (with a `LeaseLostError` or `StopTimeoutError`) to cancel the handler. */
  controller: AbortController;
  /** Lease expiry reported by K2 (server clock). */
  leasedUntilMs: number;
  /**
   * Conservative lease expiry on the local clock: the time the consume or last successful
   * extend request was sent, plus 5 minutes. Used for local decisions so clock skew between
   * this machine and K2 can't stretch or shrink them.
   */
  localDeadline: number;
  /** Set when K2 says the lease is gone (`10218`); the batch must not be acked or nacked. */
  lost: boolean;
  /** Set when `stop()` timed out while the batch was in flight. */
  timedOut: boolean;
}

export function namedError(name: string, message: string): Error {
  const err = new Error(message);
  err.name = name;
  return err;
}

/**
 * Extends `lease` every `intervalMs` until the returned function is called. A `10218` marks the
 * lease lost and aborts its controller. Any other failure goes to `onError` and is retried
 * after 5 s, 10 s, 20 s… (capped at `intervalMs`), so two failures in a row can't outlast the lease.
 */
export function startHeartbeat(
  client: K2Client,
  lease: Lease,
  intervalMs: number,
  hooks: { onExtended(): void; onError(err: unknown): void; onLate?(lateMs: number): void },
): () => void {
  let stopped = false;
  let failures = 0;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let dueAt = Date.now() + intervalMs;
  const schedule = (ms: number) => {
    dueAt = Date.now() + ms;
    timer = setTimeout(tick, ms);
  };
  const tick = async () => {
    if (stopped) return;
    const sentAt = Date.now();
    if (sentAt - dueAt > LATE_WARN_MS) hooks.onLate?.(sentAt - dueAt);
    let next = intervalMs;
    try {
      lease.leasedUntilMs = await client.extend(lease.subId, lease.batchId, lease.workerId);
      lease.localDeadline = sentAt + LEASE_MS;
      failures = 0;
      hooks.onExtended();
    } catch (err) {
      if (stopped) return;
      if (isK2Error(err) && err.code === LEASE_LOST_CODE) {
        stopped = true;
        lease.lost = true;
        lease.controller.abort(namedError("LeaseLostError", "k2kit: lease lost"));
        return;
      }
      hooks.onError(err);
      next = Math.min(intervalMs, EXTEND_RETRY_MS * 2 ** failures);
      failures++;
    }
    if (!stopped) schedule(next);
  };
  schedule(intervalMs);
  return () => {
    stopped = true;
    clearTimeout(timer);
  };
}
