/**
 * Coordinates polling across a consumer's slots when the stream is quiet: after an empty poll,
 * one slot keeps polling (with backoff) and the others wait until it sees records.
 */
export class IdleGate {
  readonly #enabled: boolean;
  #poller: string | null = null;
  readonly #waiters = new Set<() => void>();

  constructor(enabled: boolean) {
    this.#enabled = enabled;
  }

  /** Whether `workerId` should wait instead of polling. */
  shouldWait(workerId: string): boolean {
    return this.#enabled && this.#poller !== null && this.#poller !== workerId;
  }

  /** Records an empty poll. Returns whether `workerId` is the slot that keeps polling. */
  markEmpty(workerId: string): boolean {
    if (!this.#enabled) return true;
    this.#poller ??= workerId;
    return this.#poller === workerId;
  }

  /** Records a non-empty poll: every slot resumes polling. */
  markBusy(): void {
    this.#poller = null;
    for (const wake of this.#waiters) wake();
    this.#waiters.clear();
  }

  /** Hands polling to the other slots when `workerId` exits. */
  release(workerId: string): void {
    if (this.#poller === workerId) this.markBusy();
  }

  /** Resolves when polling resumes or `signal` aborts. */
  wait(signal: AbortSignal): Promise<void> {
    return new Promise((resolve) => {
      if (signal.aborted) {
        resolve();
        return;
      }
      const wake = () => {
        signal.removeEventListener("abort", wake);
        this.#waiters.delete(wake);
        resolve();
      };
      this.#waiters.add(wake);
      signal.addEventListener("abort", wake, { once: true });
    });
  }
}
