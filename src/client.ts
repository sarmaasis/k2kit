import {
  Backoff,
  type BackoffRange,
  clampDelay,
  definedFields,
  MAX_TIMER_MS,
  sleep,
} from "./backoff.js";
import {
  isK2Error,
  K2Error,
  RETRYABLE_CONSUME_CODES,
  SUBSCRIPTION_CONFLICT_CODE,
} from "./errors.js";
import { contentToBytes, decodeRecord, ID_HEADER, toBase64, utf8Encode } from "./record.js";
import type {
  Batch,
  ProduceOptions,
  ProduceRecord,
  RawBatch,
  StartAt,
  Subscription,
} from "./types.js";

/** Options for {@link K2Client}. */
export interface K2ClientOptions {
  /** K2 stream id (32-char hex). */
  streamId: string;
  /** Cloudflare API token. Needs K2 Consume to consume, K2 Produce to produce (when auth is on). */
  token?: string;
  /** Override the data-plane base URL. Defaults to `https://<streamId>.k2.cloudflarestorage.com`. */
  endpoint?: string;
  /** Custom `fetch`, e.g. for tests. Defaults to the global `fetch`. */
  fetch?: typeof fetch;
  /**
   * Per-request timeout, including reading the response, in ms. A request that times out throws
   * a `TimeoutError`, which consumers treat as retryable. Default 30,000.
   */
  timeoutMs?: number;
}

/** Thrown when a K2 request doesn't complete within the client's `timeoutMs`. */
export class K2TimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TimeoutError";
  }
}

// K2 stream ids are 32 hex characters. Checked because the default endpoint is built from the id:
// one like "attacker.example#" would send the token to another host.
const STREAM_ID = /^[0-9a-f]{32}$/i;

/** K2's rule for subscription names. */
const SUBSCRIPTION_NAME = /^[A-Za-z0-9_-]{1,128}$/;

// Cloudflare API tokens are visible ASCII. Anything else (often a trailing newline from a file)
// makes fetch throw on every request, which would otherwise be retried forever as a network error.
const TOKEN_CHARS = /^[\x21-\x7e]+$/;
const LOOPBACK = new Set(["localhost", "127.0.0.1", "[::1]"]);

/** Validates client options, throwing `TypeError` with a message that never contains the token. */
function validateClientOptions(opts: K2ClientOptions, endpoint: string): void {
  if (opts.token !== undefined && !TOKEN_CHARS.test(opts.token)) {
    throw new TypeError(
      "token contains whitespace or control characters (a trailing newline from a file or env var?)",
    );
  }
  let url: URL;
  try {
    url = new URL(endpoint);
  } catch {
    throw new TypeError(`endpoint is not a valid URL: ${endpoint}`);
  }
  // The token is sent with every request, so plain HTTP is only allowed to this machine (tests).
  if (url.protocol !== "https:" && !(url.protocol === "http:" && LOOPBACK.has(url.hostname))) {
    throw new TypeError(`endpoint must use https:// (http:// only for localhost): ${endpoint}`);
  }
  const t = opts.timeoutMs;
  if (t !== undefined && !(Number.isInteger(t) && t >= 1 && t <= MAX_TIMER_MS)) {
    throw new TypeError(`timeoutMs must be an integer from 1 to ${MAX_TIMER_MS}`);
  }
}

/** Options for {@link K2Client.produceBatched}. */
export interface ProduceBatchedOptions extends ProduceOptions {
  /** Attempts per request for errors K2 marks retryable. Default 5. */
  maxAttempts?: number;
  /** Backoff between attempts. Default `{ minMs: 500, maxMs: 10_000 }`. */
  retryBackoff?: BackoffRange;
  /** Stops before the next request or retry when aborted. */
  signal?: AbortSignal;
  /** Called after each request lands, with running totals. */
  onProgress?: (progress: ProduceProgress) => void;
}

/** Running totals from {@link K2Client.produceBatched}. */
export interface ProduceProgress {
  /** Records stored so far. */
  records: number;
  /** Requests that succeeded so far. */
  requests: number;
  /** Total requests planned. */
  totalRequests: number;
}

/** K2's produce request limit: 5 MB = 5,000,000 bytes, before and after gzip. */
export const MAX_PRODUCE_REQUEST_BYTES = 5_000_000;
// The docs say the limit also covers "a small amount of internal metadata", which grows with the
// record count: on real K2, 50,000 small records in a ~5,000,000-byte request were accepted but
// 150,000 in a ~4,990,000-byte request failed with 10212 (outcome unknown, not retryable). So
// requests are planned with a 1% byte margin and at most 50,000 records.
const PLANNED_REQUEST_BYTES = 4_950_000;
/** Most records k2kit puts in one produce request (the largest count verified on real K2). */
export const MAX_RECORDS_PER_REQUEST = 50_000;
/** K2's limits on one header's name and value, in UTF-8 bytes. */
export const MAX_HEADER_NAME_BYTES = 256;
export const MAX_HEADER_VALUE_BYTES = 8_192;
/**
 * K2's record size limit: content plus headers plus some encoding overhead, ~1 MB in total.
 * Compare {@link estimateRecordBytes} against it.
 */
export const MAX_RECORD_BYTES = 1_000_000;
// Measured on real K2 (2026-10-11): it counts content + header names/values + 14 bytes, plus up to
// 4 bytes per header, and the exact figure varies by a few bytes. These margins sit above every
// measurement, so a record that passes the estimate is never rejected for size.
const RECORD_OVERHEAD_BYTES = 32;
const PER_HEADER_OVERHEAD_BYTES = 4;

/** Conservative size of a record as K2 counts it toward {@link MAX_RECORD_BYTES}. */
export function estimateRecordBytes(
  contentBytes: number,
  headers?: { [name: string]: string },
): number {
  if (!headers) return contentBytes + RECORD_OVERHEAD_BYTES;
  const count = Object.keys(headers).length;
  return (
    contentBytes + RECORD_OVERHEAD_BYTES + headerBytes(headers) + count * PER_HEADER_OVERHEAD_BYTES
  );
}

/** Decoded byte length of standard, padded base64. */
function base64Bytes(b64: string): number {
  const padding = b64.endsWith("==") ? 2 : b64.endsWith("=") ? 1 : 0;
  return (b64.length / 4) * 3 - padding;
}
/** K2's limit on headers per record. */
export const MAX_HEADERS_PER_RECORD = 32;
/** K2's limit on the combined UTF-8 bytes of header names and values per record (64 KiB). */
export const MAX_HEADER_BYTES_PER_RECORD = 65_536;

/**
 * Checks headers against K2's documented limits (32 headers, name ≤256 B, value ≤8 KiB, ≤64 KiB in
 * total, all UTF-8). Returns a description of the first problem, or `undefined` if they're valid.
 */
export function checkHeaders(headers: { [name: string]: string }): string | undefined {
  const entries = Object.entries(headers);
  if (entries.length > MAX_HEADERS_PER_RECORD) {
    return `${entries.length} headers; K2 allows ${MAX_HEADERS_PER_RECORD}`;
  }
  for (const [name, value] of entries) {
    if (utf8Encode(name).byteLength > MAX_HEADER_NAME_BYTES) {
      return `header name "${name.slice(0, 40)}…" exceeds ${MAX_HEADER_NAME_BYTES} bytes`;
    }
    if (utf8Encode(value).byteLength > MAX_HEADER_VALUE_BYTES) {
      return `header "${name}" value exceeds ${MAX_HEADER_VALUE_BYTES} bytes`;
    }
  }
  const total = headerBytes(headers);
  if (total > MAX_HEADER_BYTES_PER_RECORD) {
    return `headers total ${total} bytes; K2 allows ${MAX_HEADER_BYTES_PER_RECORD}`;
  }
  return undefined;
}

/** UTF-8 bytes of all header names and values, as K2 counts them. */
export function headerBytes(headers: { [name: string]: string }): number {
  let total = 0;
  for (const [name, value] of Object.entries(headers)) {
    total += utf8Encode(name).byteLength + utf8Encode(value).byteLength;
  }
  return total;
}

/** Wire shape of a produced record. */
export interface WireRecord {
  content: string;
  headers?: { [name: string]: string };
}

type Api = "consume" | "produce";

/**
 * Thin, typed wrapper over the K2 stream data-plane HTTP API. Every non-success response
 * throws a {@link K2Error}; a request that exceeds `timeoutMs` throws a {@link K2TimeoutError};
 * other network failures propagate as the underlying `fetch` error.
 */
export class K2Client {
  /** The stream this client talks to. */
  readonly streamId: string;
  /** Base URL all requests go to. */
  readonly endpoint: string;
  readonly #token: string | undefined;
  readonly #fetch: typeof fetch;
  readonly #timeoutMs: number;

  constructor(opts: K2ClientOptions) {
    if (typeof opts?.streamId !== "string" || !STREAM_ID.test(opts.streamId)) {
      throw new TypeError("streamId must be 32 hex characters (the K2 stream id)");
    }
    // Lowercase, as K2 shows it, so comparisons (e.g. the dead-letter same-stream check) hold.
    const streamId = opts.streamId.toLowerCase();
    this.endpoint = (opts.endpoint ?? `https://${streamId}.k2.cloudflarestorage.com`).replace(
      /\/+$/,
      "",
    );
    validateClientOptions(opts, this.endpoint);
    this.#timeoutMs = opts.timeoutMs ?? 30_000;
    this.streamId = streamId;
    this.#token = opts.token;
    this.#fetch = opts.fetch ?? ((input, init) => globalThis.fetch(input, init));
  }

  /**
   * Creates the subscription, or returns the existing id when one with the same name and
   * `startAt` exists (K2 creates idempotently). Throws `K2Error` 10201 when the name exists
   * with a different `startAt`.
   */
  async ensureSubscription(name: string, startAt: StartAt): Promise<string> {
    // K2 answers an invalid name with a vague "Request body is invalid"; say what's wrong instead.
    if (!SUBSCRIPTION_NAME.test(name)) {
      throw new TypeError(`subscription name must be 1–128 letters, digits, _ or -: "${name}"`);
    }
    try {
      const result = await this.#call<{ id: string }>("POST", "/subscriptions", {
        name,
        start_at: { type: startAt },
      });
      return result.id;
    } catch (err) {
      if (isK2Error(err) && err.code === SUBSCRIPTION_CONFLICT_CODE) {
        throw new K2Error({
          code: err.code,
          status: err.status,
          retryable: false,
          message: `subscription "${name}" exists with a different start_at`,
        });
      }
      throw err;
    }
  }

  /** Lists all subscriptions on the stream, oldest first. */
  listSubscriptions(): Promise<Subscription[]> {
    return this.#call<Subscription[]>("GET", "/subscriptions");
  }

  /**
   * Finds a subscription by name (case-insensitive). Returns `null` when none exists, including
   * for a name K2 wouldn't allow (which K2 itself rejects as an invalid request).
   */
  async findSubscription(name: string): Promise<Subscription | null> {
    if (!SUBSCRIPTION_NAME.test(name)) return null;
    const found = await this.#call<Subscription[]>(
      "GET",
      `/subscriptions?name=${encodeURIComponent(name)}`,
    );
    return found[0] ?? null;
  }

  /** Gets one subscription. Throws `K2Error` 10215 when it does not exist. */
  getSubscription(id: string): Promise<Subscription> {
    return this.#call<Subscription>("GET", `/subscriptions/${encodeURIComponent(id)}`);
  }

  /** Deletes one subscription. */
  async deleteSubscription(id: string): Promise<void> {
    await this.#call("DELETE", `/subscriptions/${encodeURIComponent(id)}`);
  }

  /**
   * Requests a batch and leases it to `workerId` for 5 minutes. Returns `null` when there are
   * no new records. If `workerId` already holds a lease, K2 returns that same batch.
   */
  async consume(subId: string, workerId: string, maxRecords: number): Promise<Batch | null> {
    const raw = await this.#call<RawBatch>("POST", `${this.#subPath(subId)}/consume`, {
      worker_id: workerId,
      max_records: maxRecords,
    });
    if (raw.batch_id === null || raw.leased_until_ms === null || raw.records.length === 0) {
      return null;
    }
    return {
      id: raw.batch_id,
      leasedUntilMs: raw.leased_until_ms,
      records: raw.records.map(decodeRecord),
    };
  }

  /** Acks a batch: marks its records processed and releases the lease. Safe to repeat. */
  async ack(subId: string, batchId: string, workerId: string): Promise<void> {
    await this.#call("POST", this.#batchPath(subId, batchId, "ack"), { worker_id: workerId });
  }

  /** Nacks a batch: releases the lease now so the records are redelivered. */
  async nack(subId: string, batchId: string, workerId: string): Promise<void> {
    await this.#call("POST", this.#batchPath(subId, batchId, "nack"), { worker_id: workerId });
  }

  /**
   * Extends the lease to 5 minutes from now and returns the new `leasedUntilMs`.
   * Throws `K2Error` 10218 when `workerId` no longer holds the lease.
   */
  async extend(subId: string, batchId: string, workerId: string): Promise<number> {
    const result = await this.#call<{ leased_until_ms: number }>(
      "POST",
      this.#batchPath(subId, batchId, "extend"),
      { worker_id: workerId },
    );
    return result.leased_until_ms;
  }

  /**
   * Appends records atomically. Adds a random `k2kit-id` header to each record unless
   * `idHeader: false`. Throws `K2Error` whose `retryable` comes from K2's response.
   */
  async produce(records: ProduceRecord[], opts: ProduceOptions = {}): Promise<void> {
    // K2 rejects a batch with no records; there's nothing to send.
    if (Array.isArray(records) && records.length === 0) return;
    await this.#produceWire(encodeProduceRecords(records, opts), opts.gzip ?? false);
  }

  /**
   * Produces any number of records, split into requests under K2's 5 MB limit. Each request is
   * retried only when K2 marks the error retryable (`10212` is never retried, since its outcome
   * is unknown). `k2kit-id` headers are assigned once, so a retried request carries the same ids.
   * Requests are not atomic as a group: on failure, earlier requests stay stored; use
   * `onProgress` to track how far it got. Throws `RangeError` before sending anything if a
   * record exceeds 1 MB.
   *
   * Network errors (the request may or may not have reached K2) and K2 errors marked
   * non-retryable, such as `10212`, are not retried, because the batch may already be stored
   * and K2 doesn't deduplicate. They're thrown to the caller, who can retry knowing that
   * duplicates are possible (consumers using dedupe skip them via `k2kit-id`).
   */
  async produceBatched(
    records: ProduceRecord[],
    opts: ProduceBatchedOptions = {},
  ): Promise<{ records: number; requests: number }> {
    // Checked up front: NaN (e.g. from Number(process.env.X)) would mean unlimited attempts, and
    // a missing or NaN backoff field would mean retrying with no delay.
    const maxAttempts = opts.maxAttempts ?? 5;
    if (!(maxAttempts >= 0)) throw new TypeError("maxAttempts must be a number >= 0");
    const range = { minMs: 500, maxMs: 10_000, ...definedFields(opts.retryBackoff) };
    if (!(range.minMs >= 0) || !(range.maxMs >= 0)) {
      throw new TypeError("retryBackoff.minMs and maxMs must be numbers >= 0");
    }
    const wire = encodeProduceRecords(records, opts);
    // Checked before sending anything: one invalid record would fail its whole request.
    wire.forEach((w, i) => {
      const headerProblem = w.headers && checkHeaders(w.headers);
      if (headerProblem) throw new RangeError(`record ${i}: ${headerProblem}`);
      const size = estimateRecordBytes(base64Bytes(w.content), w.headers);
      if (size > MAX_RECORD_BYTES) {
        throw new RangeError(
          `record ${i} is about ${size} bytes with its headers; K2 records must be under 1 MB`,
        );
      }
    });
    const requests = planProduceRequests(wire);
    let done = 0;
    for (const [i, wire] of requests.entries()) {
      const retry = new Backoff(range);
      for (;;) {
        opts.signal?.throwIfAborted();
        try {
          await this.#produceWire(wire, opts.gzip ?? false);
          break;
        } catch (err) {
          if (!isK2Error(err) || !err.retryable || retry.attempt >= maxAttempts - 1) throw err;
          await sleep(retry.next(), opts.signal);
        }
      }
      done += wire.length;
      opts.onProgress?.({ records: done, requests: i + 1, totalRequests: requests.length });
    }
    return { records: done, requests: requests.length };
  }

  async #produceWire(records: WireRecord[], gzip: boolean): Promise<void> {
    await this.#send("produce", "POST", "/produce", JSON.stringify({ records }), gzip);
  }

  #subPath(subId: string): string {
    return `/subscriptions/${encodeURIComponent(subId)}`;
  }

  #batchPath(subId: string, batchId: string, action: string): string {
    return `${this.#subPath(subId)}/batches/${encodeURIComponent(batchId)}/${action}`;
  }

  async #call<T = unknown>(method: string, path: string, body?: unknown): Promise<T> {
    const json = body === undefined ? undefined : JSON.stringify(body);
    const parsed = await this.#send("consume", method, path, json, false);
    return (parsed as { result: T }).result;
  }

  async #send(
    api: Api,
    method: string,
    path: string,
    body: string | undefined,
    gzip: boolean,
  ): Promise<unknown> {
    const headers: { [name: string]: string } = {};
    if (this.#token) headers.authorization = `Bearer ${this.#token}`;
    let payload: string | Uint8Array | undefined = body;
    if (body !== undefined) {
      headers["content-type"] = "application/json";
      if (gzip) {
        headers["content-encoding"] = "gzip";
        payload = await gzipBytes(body);
      }
    }
    // The timeout covers the response body too: a stalled connection must not hang a worker.
    const controller = new AbortController();
    const timeout = new K2TimeoutError(
      `k2kit: ${method} ${path.split("?")[0]} timed out after ${this.#timeoutMs} ms`,
    );
    const timer = setTimeout(() => controller.abort(timeout), clampDelay(this.#timeoutMs));
    // Raced against the timeout as well as passed as a signal: a custom fetch may ignore the
    // signal, and the timeout must still fire. Abandoned promises' late results are ignored.
    const timedOut = new Promise<never>((_, reject) => {
      controller.signal.addEventListener("abort", () => reject(timeout), { once: true });
    });
    timedOut.catch(() => {});
    let text: string;
    let response: Response;
    try {
      const responseP = this.#fetch(`${this.endpoint}${path}`, {
        method,
        headers,
        signal: controller.signal,
        ...(payload === undefined ? {} : { body: payload as BodyInit }),
      });
      responseP.catch(() => {});
      response = await Promise.race([responseP, timedOut]);
      const textP = response.text();
      textP.catch(() => {});
      text = await Promise.race([textP, timedOut]);
    } catch (err) {
      throw controller.signal.aborted ? timeout : err;
    } finally {
      clearTimeout(timer);
    }
    let parsed: unknown;
    try {
      parsed = text ? JSON.parse(text) : {};
    } catch {
      parsed = undefined;
    }
    const success = isRecord(parsed) && parsed.success !== false;
    if (response.ok && success) return parsed;
    throw toK2Error(api, response.status, parsed);
  }
}

/** Encodes produce input into K2's wire shape. Exposed for request-size planning. */
export function encodeProduceRecords(
  records: ProduceRecord[],
  opts: ProduceOptions = {},
): WireRecord[] {
  const addId = opts.idHeader ?? true;
  // Checked first: Array.from would turn a single record object into [] and produce nothing.
  if (!Array.isArray(records)) {
    throw new TypeError("records must be an array of { content, headers? } objects");
  }
  // Array.from visits holes in sparse arrays (as undefined), so they're reported, not sent as null.
  return Array.from(records, (r, i) => {
    if (typeof r !== "object" || r === null) {
      throw new TypeError(`record ${i} must be an object with content and optional headers`);
    }
    let headers = stringHeaders(r.headers, i);
    let bytes: Uint8Array;
    try {
      bytes = contentToBytes(r.content);
    } catch (err) {
      throw new TypeError(`record ${i}: ${err instanceof Error ? err.message : String(err)}`, {
        cause: err,
      });
    }
    if (addId && headers?.[ID_HEADER] === undefined) {
      // A record without room under K2's limits (32 headers, 64 KiB of headers, ~1 MB in total)
      // gets no id (so consumers won't dedupe it), rather than being rejected.
      const withId = { ...headers, [ID_HEADER]: globalThis.crypto.randomUUID() };
      const room =
        Object.keys(withId).length <= MAX_HEADERS_PER_RECORD &&
        headerBytes(withId) <= MAX_HEADER_BYTES_PER_RECORD &&
        estimateRecordBytes(bytes.byteLength, withId) <= MAX_RECORD_BYTES;
      if (room) headers = withId;
    }
    const wire: WireRecord = { content: toBase64(bytes) };
    if (headers && Object.keys(headers).length > 0) wire.headers = headers;
    return wire;
  });
}

/**
 * Copies a record's headers, checking K2's rule that they map strings to strings. `undefined`
 * values are dropped, as JSON would drop them (so they don't count toward the 32-header limit).
 */
function stringHeaders(headers: unknown, index: number): { [name: string]: string } | undefined {
  if (headers === undefined || headers === null) return undefined;
  // A Map or fetch Headers has no own entries, so its headers would silently disappear.
  if (
    typeof headers !== "object" ||
    Array.isArray(headers) ||
    headers instanceof Map ||
    (typeof Headers !== "undefined" && headers instanceof Headers)
  ) {
    throw new TypeError(`record ${index}: headers must be a plain object of string values`);
  }
  // Null prototype, so a header named "__proto__" stays a header.
  const copy: { [name: string]: string } = Object.create(null);
  for (const [name, value] of Object.entries(headers)) {
    if (value === undefined) continue;
    if (typeof value !== "string") {
      const kind = value === null ? "null" : typeof value;
      throw new TypeError(`record ${index}: header "${name}" must be a string, not ${kind}`);
    }
    copy[name] = value;
  }
  return copy;
}

const ENVELOPE_BYTES = utf8Encode('{"records":[]}').byteLength;

/** Groups wire records into request batches whose JSON body stays under 5 MB. */
export function planProduceRequests(
  records: WireRecord[],
  maxBytes = PLANNED_REQUEST_BYTES,
  maxRecords = MAX_RECORDS_PER_REQUEST,
): WireRecord[][] {
  const batches: WireRecord[][] = [];
  let current: WireRecord[] = [];
  let size = ENVELOPE_BYTES;
  for (const record of records) {
    const bytes = utf8Encode(JSON.stringify(record)).byteLength + 1; // +1 for the comma
    if (current.length > 0 && (size + bytes > maxBytes || current.length >= maxRecords)) {
      batches.push(current);
      current = [];
      size = ENVELOPE_BYTES;
    }
    current.push(record);
    size += bytes;
  }
  if (current.length > 0) batches.push(current);
  return batches;
}

function toK2Error(api: Api, status: number, body: unknown): K2Error {
  if (api === "produce" && isRecord(body) && isRecord(body.error)) {
    const { code, message, retryable } = body.error;
    return new K2Error({
      code: typeof code === "number" ? code : 0,
      status,
      message: typeof message === "string" ? message : `HTTP ${status}`,
      retryable: retryable === true,
    });
  }
  if (isRecord(body) && Array.isArray(body.errors) && isRecord(body.errors[0])) {
    const { code, message } = body.errors[0];
    const numeric = typeof code === "number" ? code : 0;
    return new K2Error({
      code: numeric,
      status,
      message: typeof message === "string" ? message : `HTTP ${status}`,
      retryable: RETRYABLE_CONSUME_CODES.has(numeric),
    });
  }
  // No K2 error body (gateway error page, empty body): retry only transient HTTP statuses.
  return new K2Error({
    code: 0,
    status,
    message: `HTTP ${status} with no K2 error body`,
    retryable: status === 429 || status >= 500,
  });
}

function isRecord(v: unknown): v is { [key: string]: unknown } {
  return typeof v === "object" && v !== null;
}

async function gzipBytes(text: string): Promise<Uint8Array> {
  const stream = new Blob([text]).stream().pipeThrough(new CompressionStream("gzip"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}
