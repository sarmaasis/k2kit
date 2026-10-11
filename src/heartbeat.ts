import type { K2Client } from "./client.js";
import { isK2Error, LEASE_LOST_CODE } from "./errors.js";

/** Mutable lease state shared between a batch's handler run and its heartbeat. */
export interface Lease {
  subId: string;
  batchId: string;
  workerId: string;
  /** Aborted (with a `LeaseLostError` or `StopTimeoutError`) to cancel the handler. */
  controller: AbortController;
  leasedUntilMs: number;
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
 * lease lost and aborts its controller; other errors go to `onError` and the next tick retries.
 */
export function startHeartbeat(
  client: K2Client,
  lease: Lease,
  intervalMs: number,
  hooks: { onExtended(): void; onError(err: unknown): void },
): () => void {
  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const tick = async () => {
    if (stopped) return;
    try {
      lease.leasedUntilMs = await client.extend(lease.subId, lease.batchId, lease.workerId);
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
    }
    if (!stopped) timer = setTimeout(tick, intervalMs);
  };
  timer = setTimeout(tick, intervalMs);
  return () => {
    stopped = true;
    clearTimeout(timer);
  };
}
