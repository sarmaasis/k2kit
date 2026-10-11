import type { K2Client } from "./client.js";
import { describeError } from "./errors.js";
import { ID_HEADER } from "./record.js";
import type { K2Record, ProduceRecord } from "./types.js";

/** A record that kept failing after its batch was retried one record at a time. */
export interface DeadLetter {
  record: K2Record;
  /** The error the handler threw for this record alone. */
  error: unknown;
  /** Delivery attempt at which it was dead-lettered. */
  attempt: number;
}

/** Context passed to a {@link DeadLetterSend} function. */
export interface DeadLetterContext {
  workerId: string;
  batchId: string;
  /** Aborted when the lease is lost or `stop()` times out. */
  signal: AbortSignal;
}

/**
 * Stores dead letters somewhere durable. If it throws, the batch is nacked and nothing is
 * dropped; it will be retried on the next delivery.
 */
export type DeadLetterSend = (letters: DeadLetter[], ctx: DeadLetterContext) => Promise<void>;

/** The consumer's `deadLetter` option. */
export interface DeadLetterOptions {
  /**
   * Delivery attempt at which a failing batch is retried one record at a time and the records
   * that still fail are sent to `send`. Default 5.
   */
  afterAttempts?: number;
  /** Where dead letters go, e.g. {@link deadLetterToStream}. */
  send: DeadLetterSend;
}

/** Headers {@link deadLetterToStream} adds to each dead letter. */
export const DEAD_LETTER_HEADERS = {
  error: "k2kit-dlq-error",
  attempts: "k2kit-dlq-attempts",
  sourceTimestamp: "k2kit-dlq-source-timestamp-ms",
} as const;

const MAX_HEADERS = 32;
const MAX_ERROR_CHARS = 1_024;

/**
 * Sends dead letters to another K2 stream (the client's stream), keeping each record's content
 * and headers, including `k2kit-id`, and adding `k2kit-dlq-error`, `k2kit-dlq-attempts` and
 * `k2kit-dlq-source-timestamp-ms`. The client's token needs K2 Produce on that stream.
 * The error header holds the handler's error message: keep secrets and personal data out of it.
 */
export function deadLetterToStream(client: K2Client): DeadLetterSend {
  return async (letters, ctx) => {
    const records: ProduceRecord[] = letters.map((letter) => ({
      content: letter.record.bytes,
      headers: deadLetterHeaders(letter),
    }));
    await client.produceBatched(records, { signal: ctx.signal });
  };
}

function deadLetterHeaders(letter: DeadLetter): { [name: string]: string } {
  const added = {
    [DEAD_LETTER_HEADERS.error]: describeError(letter.error).slice(0, MAX_ERROR_CHARS),
    [DEAD_LETTER_HEADERS.attempts]: String(letter.attempt),
    [DEAD_LETTER_HEADERS.sourceTimestamp]: String(letter.record.timestampMs),
  };
  // K2 allows 32 headers: reserve one for k2kit-id (kept, or added by produceBatched), then keep
  // as many original headers as fit.
  const original = Object.entries(letter.record.headers).filter(
    ([name]) => !(name in added) && name !== ID_HEADER,
  );
  const id = letter.record.headers[ID_HEADER];
  const room = MAX_HEADERS - Object.keys(added).length - 1;
  const headers: { [name: string]: string } = Object.fromEntries(original.slice(0, room));
  if (id !== undefined) headers[ID_HEADER] = id;
  return { ...headers, ...added };
}
