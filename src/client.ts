import { Backoff, type BackoffRange, sleep } from "./backoff.js";
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
/** K2's record size limit (~1 MB). */
export const MAX_RECORD_BYTES = 1_000_000;

/** Wire shape of a produced record. */
export interface WireRecord {
  content: string;
  headers?: { [name: string]: string };
}

type Api = "consume" | "produce";

/**
 * Thin, typed wrapper over the K2 stream data-plane HTTP API. Every non-success response
 * throws a {@link K2Error}; network failures propagate as the underlying `fetch` error.
 */
export class K2Client {
  /** Base URL all requests go to. */
  readonly endpoint: string;
  readonly #token: string | undefined;
  readonly #fetch: typeof fetch;

  constructor(opts: K2ClientOptions) {
    this.endpoint = (opts.endpoint ?? `https://${opts.streamId}.k2.cloudflarestorage.com`).replace(
      /\/+$/,
      "",
    );
    this.#token = opts.token;
    this.#fetch = opts.fetch ?? ((input, init) => globalThis.fetch(input, init));
  }

  /**
   * Creates the subscription, or returns the existing id when one with the same name and
   * `startAt` exists (K2 creates idempotently). Throws `K2Error` 10201 when the name exists
   * with a different `startAt`.
   */
  async ensureSubscription(name: string, startAt: StartAt): Promise<string> {
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

  /** Finds a subscription by name (case-insensitive). Returns `null` when none exists. */
  async findSubscription(name: string): Promise<Subscription | null> {
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
    await this.#produceWire(encodeProduceRecords(records, opts), opts.gzip ?? false);
  }

  /**
   * Produces any number of records, split into requests under K2's 5 MB limit. Each request is
   * retried only when K2 marks the error retryable (`10212` is never retried, since its outcome
   * is unknown). `k2kit-id` headers are assigned once, so a retried request carries the same ids.
   * Requests are not atomic as a group: on failure, earlier requests stay stored; use
   * `onProgress` to track how far it got. Throws `RangeError` before sending anything if a
   * record exceeds 1 MB.
   */
  async produceBatched(
    records: ProduceRecord[],
    opts: ProduceBatchedOptions = {},
  ): Promise<{ records: number; requests: number }> {
    records.forEach((r, i) => {
      const size = contentToBytes(r.content).byteLength;
      if (size > MAX_RECORD_BYTES) {
        throw new RangeError(`record ${i} is ${size} bytes; K2 records must be under 1 MB`);
      }
    });
    const requests = planProduceRequests(encodeProduceRecords(records, opts));
    const maxAttempts = opts.maxAttempts ?? 5;
    let done = 0;
    for (const [i, wire] of requests.entries()) {
      const retry = new Backoff(opts.retryBackoff ?? { minMs: 500, maxMs: 10_000 });
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
    const response = await this.#fetch(`${this.endpoint}${path}`, {
      method,
      headers,
      ...(payload === undefined ? {} : { body: payload as BodyInit }),
    });
    const text = await response.text();
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
  return records.map((r) => {
    let headers = r.headers;
    if (addId && headers?.[ID_HEADER] === undefined) {
      headers = { ...headers, [ID_HEADER]: globalThis.crypto.randomUUID() };
    }
    const wire: WireRecord = { content: toBase64(contentToBytes(r.content)) };
    if (headers && Object.keys(headers).length > 0) wire.headers = headers;
    return wire;
  });
}

const ENVELOPE_BYTES = utf8Encode('{"records":[]}').byteLength;

/** Groups wire records into request batches whose JSON body stays under 5 MB. */
export function planProduceRequests(
  records: WireRecord[],
  maxBytes = MAX_PRODUCE_REQUEST_BYTES,
): WireRecord[][] {
  const batches: WireRecord[][] = [];
  let current: WireRecord[] = [];
  let size = ENVELOPE_BYTES;
  for (const record of records) {
    const bytes = utf8Encode(JSON.stringify(record)).byteLength + 1; // +1 for the comma
    if (current.length > 0 && size + bytes > maxBytes) {
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
