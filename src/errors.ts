/** Consume-side error codes that K2 documents as safe to retry with backoff. */
export const RETRYABLE_CONSUME_CODES: ReadonlySet<number> = new Set([10211, 10214, 10216, 10217]);

/** K2 code returned by `extend` when this worker no longer holds the lease. */
export const LEASE_LOST_CODE = 10218;

/** K2 code returned when a subscription name exists with different settings. */
export const SUBSCRIPTION_CONFLICT_CODE = 10201;

/**
 * Error thrown for every non-success K2 response.
 *
 * `code` is the K2 error code (`0` when the response carried none, e.g. an HTML gateway error),
 * `status` is the HTTP status, and `retryable` says whether the same request may be retried.
 */
export class K2Error extends Error {
  /** K2 error code, e.g. `10216`. `0` when the response had no K2 error body. */
  readonly code: number;
  /** HTTP status code of the response. */
  readonly status: number;
  /** Whether retrying the same request with backoff is safe and may succeed. */
  readonly retryable: boolean;

  constructor(init: { code: number; status: number; message: string; retryable: boolean }) {
    super(init.message);
    this.name = "K2Error";
    this.code = init.code;
    this.status = init.status;
    this.retryable = init.retryable;
  }
}

/** Type guard for {@link K2Error}. */
export function isK2Error(err: unknown): err is K2Error {
  return err instanceof K2Error;
}

/**
 * Whether an error from a consume-side call may be retried: retryable K2 errors, and any
 * non-K2 error (network failures, aborted fetches, unparseable responses).
 */
export function isRetryableError(err: unknown): boolean {
  return isK2Error(err) ? err.retryable : true;
}

/** Short, token-free description of any thrown value for log lines. */
export function describeError(err: unknown): string {
  if (isK2Error(err)) return `K2 ${err.code}: ${err.message}`;
  if (err instanceof Error) return `${err.name}: ${err.message}`;
  return String(err);
}
