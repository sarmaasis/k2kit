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

/** Sleeps for `ms`, resolving early (never rejecting) when `signal` aborts. */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
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
