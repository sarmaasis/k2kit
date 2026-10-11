import {
  estimateRecordBytes,
  headerBytes,
  type K2Client,
  MAX_HEADER_BYTES_PER_RECORD,
  MAX_RECORD_BYTES,
} from "./client.js";
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
export type DeadLetterSend = (
  letters: DeadLetter[],
  ctx: DeadLetterContext,
) => void | Promise<void>;

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
  /** How many headers didn't fit within K2's limits, dead-letter or original (only when some didn't). */
  droppedHeaders: "k2kit-dlq-dropped-headers",
} as const;

const MAX_HEADERS = 32;
const MAX_ERROR_CHARS = 1_024;
const TARGET_STREAM = Symbol.for("k2kit.deadLetterTargetStream");

/** The stream a {@link deadLetterToStream} sender writes to, if `send` is one. */
export function deadLetterTargetStream(send: DeadLetterSend): string | undefined {
  return (send as DeadLetterSend & { [TARGET_STREAM]?: string })[TARGET_STREAM];
}

/**
 * Sends dead letters to another K2 stream (the client's stream), keeping each record's content
 * and headers, including `k2kit-id`, and adding `k2kit-dlq-error`, `k2kit-dlq-attempts` and
 * `k2kit-dlq-source-timestamp-ms`. The client's token needs K2 Produce on that stream. It must be
 * a different stream from the one being consumed, or dead letters would be consumed and fail
 * again in a loop; the consumer rejects that configuration.
 * The error header holds the handler's error message: keep secrets and personal data out of it.
 */
export function deadLetterToStream(client: K2Client): DeadLetterSend {
  const send: DeadLetterSend = async (letters, ctx) => {
    const records: ProduceRecord[] = letters.map((letter) => ({
      content: letter.record.bytes,
      headers: deadLetterHeaders(letter),
    }));
    await client.produceBatched(records, { signal: ctx.signal });
  };
  // Lets the consumer refuse a dead-letter stream that is the stream it consumes.
  Object.defineProperty(send, TARGET_STREAM, { value: client.streamId });
  return send;
}

function deadLetterHeaders(letter: DeadLetter): { [name: string]: string } {
  // K2 limits a record to 32 headers, 64 KiB of header names + values, and ~1 MB in total. The
  // original record fit, so its content always fits; headers are added in priority order while
  // they fit, with the error message truncated if only part of it does. A dead letter therefore
  // never fails for size (which would make the poison record loop forever).
  const contentBytes = letter.record.bytes.byteLength;
  const reserved = { [DEAD_LETTER_HEADERS.droppedHeaders]: "32" }; // room for the dropped count
  // Null prototype and own-key checks: K2 allows any header name, including "__proto__" or
  // "constructor", which a plain object would mistake for (or turn into) inherited properties.
  const headers: { [name: string]: string } = Object.create(null);
  const fits = (candidate: { [name: string]: string }) => {
    const all = { ...headers, ...candidate, ...reserved };
    return (
      Object.keys(all).length <= MAX_HEADERS &&
      headerBytes(all) <= MAX_HEADER_BYTES_PER_RECORD &&
      estimateRecordBytes(contentBytes, all) <= MAX_RECORD_BYTES
    );
  };
  let dropped = 0;
  const add = (name: string, value: string, truncatable = false) => {
    if (fits({ [name]: value })) {
      headers[name] = value;
      return;
    }
    if (truncatable) {
      // Longest prefix that fits (binary search on characters).
      let lo = 0;
      let hi = value.length;
      while (lo < hi) {
        const mid = Math.ceil((lo + hi) / 2);
        if (fits({ [name]: sliceText(value, mid) })) lo = mid;
        else hi = mid - 1;
      }
      if (sliceText(value, lo).length > 0) {
        headers[name] = sliceText(value, lo);
        return;
      }
    }
    dropped++;
  };

  add(DEAD_LETTER_HEADERS.error, sliceText(describeError(letter.error), MAX_ERROR_CHARS), true);
  add(DEAD_LETTER_HEADERS.attempts, String(letter.attempt));
  add(ID_HEADER, letter.record.headers[ID_HEADER] ?? globalThis.crypto.randomUUID());
  add(DEAD_LETTER_HEADERS.sourceTimestamp, String(letter.record.timestampMs));
  for (const [name, value] of Object.entries(letter.record.headers)) {
    if (!Object.hasOwn(headers, name) && name !== ID_HEADER) add(name, value);
  }
  if (dropped > 0) headers[DEAD_LETTER_HEADERS.droppedHeaders] = String(dropped);
  return headers;
}

/** The first `length` UTF-16 units of `text`, minus a trailing half of a surrogate pair. */
function sliceText(text: string, length: number): string {
  const end =
    length > 0 && length < text.length && /[\ud800-\udbff]/.test(text[length - 1] ?? "")
      ? length - 1
      : length;
  return text.slice(0, end);
}
