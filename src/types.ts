/** Where a new subscription starts reading: the oldest retained record, or after the newest. */
export type StartAt = "earliest" | "latest";

/** A subscription as returned by the K2 subscriptions endpoints. */
export interface Subscription {
  id: string;
  name: string;
  start_at: { type: StartAt };
  created_at: string;
  modified_at: string;
}

/** A record exactly as returned by the K2 consume endpoint. */
export interface RawRecord {
  /** When K2 received the record, Unix epoch milliseconds. */
  timestamp_ms: number;
  /** Standard base64 content. */
  content: string;
  /** Omitted by K2 when the record has no headers. */
  headers?: { [name: string]: string };
}

/** The consume endpoint's `result`. All fields are `null`/empty when there are no new records. */
export interface RawBatch {
  batch_id: string | null;
  leased_until_ms: number | null;
  records: RawRecord[];
}

/** A decoded K2 record. */
export interface K2Record {
  /** When K2 received the record, Unix epoch milliseconds. */
  readonly timestampMs: number;
  /** Record headers; `{}` when K2 omitted them. */
  readonly headers: Readonly<{ [name: string]: string }>;
  /** Decoded content bytes. */
  readonly bytes: Uint8Array;
  /** Content decoded as UTF-8 (invalid sequences become U+FFFD). */
  text(): string;
  /** `JSON.parse(text())`. */
  json<T = unknown>(): T;
  /**
   * The producer-set `k2kit-id` header, or `undefined` when absent or blank. Only records with an
   * id are deduplicated: K2 gives every record in a produce batch the same timestamp, so records
   * without an id can't be told apart from a redelivery by their content.
   */
  readonly id: string | undefined;
  /**
   * Stable key: the `k2kit-id` header when present (hashed if longer than 128 characters),
   * otherwise a FNV-1a 64-bit hash of `timestamp_ms` and the content bytes. Identical records
   * produced in the same batch share a hash key.
   */
  key(): string;
}

/** A leased batch of records. */
export interface Batch {
  /** K2 batch id, used for ack, nack and extend. */
  id: string;
  /** Lease expiry, Unix epoch milliseconds. */
  leasedUntilMs: number;
  records: K2Record[];
}

/** Context passed to a {@link Handler} alongside each batch. */
export interface HandlerContext {
  /** The `worker_id` holding this batch's lease. */
  workerId: string;
  /** Aborted when the lease is lost or `stop()` times out. Pass it to downstream I/O. */
  signal: AbortSignal;
  /**
   * Best-effort, in-memory delivery count, starting at 1: one more than the most failed (or
   * lease-lost) deliveries seen for any record in the batch. Resets on process restart.
   */
  attempt: number;
}

/** Processes one batch. Return normally to ack; throw to nack. */
export type Handler = (batch: Batch, ctx: HandlerContext) => unknown | Promise<unknown>;

/**
 * Storage for record keys already processed, used to skip redelivered records.
 * Implement `hasMany`/`addMany` for remote stores so a batch costs one round trip, not one per record.
 */
export interface DedupeStore {
  /** Whether `key` was already processed. May be synchronous. */
  has(key: string): boolean | Promise<boolean>;
  /** Marks `key` as processed. May be synchronous. */
  add(key: string): void | Promise<void>;
  /** Batch form of `has`: one boolean per key, in order. Used instead of `has` when present. */
  hasMany?(keys: string[]): boolean[] | Promise<boolean[]>;
  /** Batch form of `add`. Used instead of `add` when present. */
  addMany?(keys: string[]): void | Promise<void>;
}

/** A record to produce. */
export interface ProduceRecord {
  /**
   * Bytes as is, strings as UTF-8, other binary data (`ArrayBuffer`, `DataView`, typed arrays) as
   * its bytes, any other value JSON-stringified. Values with no JSON form throw `TypeError`.
   */
  content: Uint8Array | string | object;
  /** Up to 32 string headers. Headers whose value is `undefined` are left out. */
  headers?: { [name: string]: string };
}

/** Options for {@link K2Client.produce}. */
export interface ProduceOptions {
  /** Gzip the request body. Default `false`. */
  gzip?: boolean;
  /** Add a random `k2kit-id` header to each record that lacks one, for consumer dedupe. Default `true`. */
  idHeader?: boolean;
}

/** Minimal logger. Each method receives one preformatted line. */
export interface Logger {
  debug(message: string): void;
  info(message: string): void;
  warn(message: string): void;
  error(message: string): void;
}
