/** Bounds for an exponential backoff. */
export interface BackoffRange {
  /** Base delay for the first attempt, in milliseconds. */
  minMs: number;
  /** Upper bound for any single delay, in milliseconds. */
  maxMs: number;
}

/** Full-jitter delay: `random(0, min(maxMs, minMs * 2^attempt))`. */
export function fullJitter(
  attempt: number,
  range: BackoffRange,
  random: () => number = Math.random,
): number {
  const ceiling = Math.min(range.maxMs, range.minMs * 2 ** attempt);
  return Math.floor(random() * ceiling);
}

/**
 * The fields of `obj` that aren't `undefined`, for merging over defaults. A plain spread would
 * copy `{ maxMs: undefined }` over the default, making every delay NaN, which is clamped to 0:
 * an idle consumer would then poll K2 in a tight loop.
 */
export function definedFields<T extends object>(obj: T | undefined): Partial<T> {
  if (typeof obj !== "object" || obj === null) return {};
  return Object.fromEntries(
    Object.entries(obj).filter(([, value]) => value !== undefined),
  ) as Partial<T>;
}

/** Per-loop attempt tracker that produces full-jitter delays. */
export class Backoff {
  #attempt = 0;

  constructor(
    private readonly range: BackoffRange,
    private readonly random: () => number = Math.random,
  ) {}

  /** Number of delays handed out since the last reset. */
  get attempt(): number {
    return this.#attempt;
  }

  /** Returns the next delay and increments the attempt count. */
  next(): number {
    const delay = fullJitter(this.#attempt, this.range, this.random);
    this.#attempt++;
    return delay;
  }

  /** Resets the attempt count after a success. */
  reset(): void {
    this.#attempt = 0;
  }
}

/**
 * Longest delay a JavaScript timer supports (~24.8 days). Larger values make `setTimeout` fire
 * immediately, which would turn a backoff into a busy loop, so delays are clamped to it.
 */
export const MAX_TIMER_MS = 2_147_483_647;

/** Clamps a delay to what a timer can represent (and to 0 for negative or NaN input). */
export function clampDelay(ms: number): number {
  return Number.isFinite(ms) ? Math.min(Math.max(ms, 0), MAX_TIMER_MS) : ms > 0 ? MAX_TIMER_MS : 0;
}

/** Sleeps for `ms` (clamped to {@link MAX_TIMER_MS}), resolving early, never rejecting, when `signal` aborts. */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  ms = clampDelay(ms);
  return new Promise((resolve) => {
    if (signal?.aborted || ms <= 0) {
      resolve();
      return;
    }
    const done = () => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal?.addEventListener("abort", done, { once: true });
  });
}
