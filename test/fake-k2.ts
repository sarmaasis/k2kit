/**
 * In-memory fake of the K2 data-plane HTTP API, injected through the `fetch` option.
 *
 * Implements the endpoint table, subscription rules and lease rules from the build spec
 * (which mirror the K2 docs): per-subscription position, leases keyed by `worker_id`,
 * 5-minute leases on the injected clock, the 128-lease and 100-subscription caps,
 * redelivery on nack and expiry, and idempotent ack. Faults can be injected per endpoint.
 */

export const LEASE_MS = 5 * 60_000;
const MAX_LEASES = 128;
const MAX_SUBSCRIPTIONS = 100;
const MAX_PRODUCE_BYTES = 5_000_000;
const MAX_RECORD_BYTES = 1_000_000;
const MAX_CONSUME_BYTES = 10_000_000;

export type Endpoint =
  | "createSubscription"
  | "listSubscriptions"
  | "getSubscription"
  | "deleteSubscription"
  | "consume"
  | "ack"
  | "nack"
  | "extend"
  | "produce";

interface StoredRecord {
  timestamp_ms: number;
  content: string;
  headers?: { [name: string]: string };
}

interface Lease {
  batchId: string;
  workerId: string;
  indices: number[];
  leasedUntilMs: number;
}

interface Sub {
  id: string;
  name: string;
  startAt: "earliest" | "latest";
  createdAt: string;
  cursor: number;
  /** Record indices released by nack or expiry, redelivered before new records. */
  redeliver: number[];
  leases: Map<string, Lease>;
}

type Fault = { kind: "code"; code: number; message?: string } | { kind: "network" };

const STATUS: { [code: number]: number } = {
  10200: 404,
  10201: 422,
  10204: 400,
  10205: 415,
  10206: 413,
  10207: 413,
  10208: 401,
  10209: 401,
  10210: 403,
  10211: 503,
  10212: 503,
  10213: 500,
  10214: 503,
  10215: 404,
  10216: 429,
  10217: 409,
  10218: 409,
  10219: 422,
};

const PRODUCE_RETRYABLE = new Set([10211]);
const STD_BASE64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
const SUB_NAME = /^[A-Za-z0-9_-]{1,128}$/;

class HttpError {
  constructor(
    readonly code: number,
    readonly message: string,
  ) {}
}

export interface FakeK2Options {
  token?: string;
  /** Clock in epoch ms. Defaults to `Date.now`, so Vitest fake timers drive it. */
  now?: () => number;
  /** When true, produce does not require auth (HTTP input auth disabled). */
  publicProduce?: boolean;
}

export interface RequestLogEntry {
  endpoint: Endpoint;
  method: string;
  path: string;
  body: unknown;
  headers: { [name: string]: string };
}

export class FakeK2 {
  readonly token: string;
  readonly records: StoredRecord[] = [];
  readonly requests: RequestLogEntry[] = [];
  /** How many times each record index was handed out in a batch. */
  readonly deliveries = new Map<number, number>();
  /** Record indices acked, in ack order (per subscription id). */
  readonly acked = new Map<string, number[]>();

  readonly #now: () => number;
  readonly #publicProduce: boolean;
  readonly #subs = new Map<string, Sub>();
  readonly #faults = new Map<Endpoint, Fault[]>();
  #seq = 0;

  constructor(opts: FakeK2Options = {}) {
    this.token = opts.token ?? "test-token";
    this.#now = opts.now ?? Date.now;
    this.#publicProduce = opts.publicProduce ?? false;
  }

  /** `fetch`-compatible entry point. */
  readonly fetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const request = new Request(input, init);
    const url = new URL(request.url);
    const route = this.#route(request.method, url.pathname);
    if (!route) return json(404, { success: false, errors: [{ code: 0, message: "no route" }] });

    const { endpoint, params } = route;
    const headers: { [name: string]: string } = {};
    request.headers.forEach((v, k) => {
      headers[k] = v;
    });

    let raw: Uint8Array = new Uint8Array(await request.arrayBuffer());
    if (headers["content-encoding"] === "gzip") {
      if (raw.byteLength > MAX_PRODUCE_BYTES) return this.#fail(endpoint, 10206, "too large");
      raw = await gunzip(raw);
    }
    const text = new TextDecoder().decode(raw);
    let body: unknown;
    try {
      body = text ? JSON.parse(text) : undefined;
    } catch {
      body = Symbol.for("invalid-json");
    }
    this.requests.push({ endpoint, method: request.method, path: url.pathname, body, headers });

    const fault = this.#faults.get(endpoint)?.shift();
    if (fault?.kind === "network") throw new TypeError("fetch failed");
    if (fault?.kind === "code")
      return this.#fail(endpoint, fault.code, fault.message ?? "injected");

    try {
      const isProduce = endpoint === "produce";
      if (!(isProduce && this.#publicProduce)) this.#checkAuth(headers.authorization);
      if (request.method === "POST" && headers["content-type"] !== "application/json") {
        throw new HttpError(10205, "Content-Type must be application/json");
      }
      if (body === Symbol.for("invalid-json")) throw new HttpError(10204, "malformed JSON");
      if (isProduce) {
        if (raw.byteLength > MAX_PRODUCE_BYTES) throw new HttpError(10206, "request exceeds 5 MB");
        this.#produce(body);
        return json(200, { success: true });
      }
      return json(200, envelope(this.#handle(endpoint, params, url, body)));
    } catch (err) {
      if (err instanceof HttpError) return this.#fail(endpoint, err.code, err.message);
      throw err;
    }
  };

  /** Makes the next `times` calls to `endpoint` fail with K2 `code`. */
  fail(endpoint: Endpoint, code: number, times = 1, message?: string): void {
    const queue = this.#faults.get(endpoint) ?? [];
    for (let i = 0; i < times; i++)
      queue.push({ kind: "code", code, ...(message ? { message } : {}) });
    this.#faults.set(endpoint, queue);
  }

  /** Makes the next `times` calls to `endpoint` throw a network error. */
  failNetwork(endpoint: Endpoint, times = 1): void {
    const queue = this.#faults.get(endpoint) ?? [];
    for (let i = 0; i < times; i++) queue.push({ kind: "network" });
    this.#faults.set(endpoint, queue);
  }

  /** Appends records directly, bypassing HTTP. Content is any JSON value unless `raw`. */
  append(...contents: unknown[]): void {
    for (const c of contents) {
      const bytes = new TextEncoder().encode(typeof c === "string" ? c : JSON.stringify(c));
      this.records.push({
        timestamp_ms: this.#now(),
        content: btoa(String.fromCharCode(...bytes)),
      });
    }
  }

  /** Creates a subscription directly, bypassing HTTP. */
  addSubscription(name: string, startAt: "earliest" | "latest" = "earliest"): string {
    return (this.#createSub({ name, start_at: { type: startAt } }) as { id: string }).id;
  }

  subscriptionNames(): string[] {
    return [...this.#subs.values()].map((s) => s.name);
  }

  /** Active (unexpired) leases on a subscription. */
  activeLeases(subId: string): Lease[] {
    const sub = this.#subs.get(subId);
    if (!sub) return [];
    this.#expire(sub);
    return [...sub.leases.values()];
  }

  /** Revokes a lease as if it had expired; its records go back for redelivery. */
  dropLease(batchId: string): void {
    for (const sub of this.#subs.values()) {
      for (const [workerId, lease] of sub.leases) {
        if (lease.batchId === batchId) {
          sub.leases.delete(workerId);
          sub.redeliver.push(...lease.indices);
          sub.redeliver.sort((a, b) => a - b);
        }
      }
    }
  }

  /** Number of records not yet acked on a subscription (pending + leased + unread). */
  backlog(subId: string): number {
    const sub = this.#subs.get(subId);
    if (!sub) return 0;
    this.#expire(sub);
    const leased = [...sub.leases.values()].reduce((n, l) => n + l.indices.length, 0);
    return this.records.length - sub.cursor + sub.redeliver.length + leased;
  }

  // --- routing -------------------------------------------------------------------------

  #route(method: string, path: string): { endpoint: Endpoint; params: string[] } | null {
    const parts = path.split("/").filter(Boolean);
    if (method === "POST" && path === "/produce") return { endpoint: "produce", params: [] };
    if (parts[0] !== "subscriptions") return null;
    if (parts.length === 1) {
      if (method === "POST") return { endpoint: "createSubscription", params: [] };
      if (method === "GET") return { endpoint: "listSubscriptions", params: [] };
    }
    const id = parts[1] ?? "";
    if (parts.length === 2) {
      if (method === "GET") return { endpoint: "getSubscription", params: [id] };
      if (method === "DELETE") return { endpoint: "deleteSubscription", params: [id] };
    }
    if (parts.length === 3 && parts[2] === "consume" && method === "POST") {
      return { endpoint: "consume", params: [id] };
    }
    if (parts.length === 5 && parts[2] === "batches" && method === "POST") {
      const action = parts[4];
      if (action === "ack" || action === "nack" || action === "extend") {
        return { endpoint: action, params: [id, parts[3] ?? ""] };
      }
    }
    return null;
  }

  #handle(endpoint: Endpoint, params: string[], url: URL, body: unknown): unknown {
    switch (endpoint) {
      case "createSubscription":
        return this.#createSub(body);
      case "listSubscriptions": {
        const name = url.searchParams.get("name");
        const subs = [...this.#subs.values()].filter(
          (s) => name === null || s.name.toLowerCase() === name.toLowerCase(),
        );
        return subs.map(view);
      }
      case "getSubscription":
        return view(this.#sub(params[0]));
      case "deleteSubscription":
        this.#sub(params[0]);
        this.#subs.delete(params[0] ?? "");
        return null;
      case "consume":
        return this.#consume(this.#sub(params[0]), body);
      case "ack":
        return this.#ack(this.#sub(params[0]), params[1] ?? "", body);
      case "nack":
        return this.#nack(this.#sub(params[0]), params[1] ?? "", body);
      case "extend":
        return this.#extend(this.#sub(params[0]), params[1] ?? "", body);
      default:
        throw new HttpError(10204, "unsupported");
    }
  }

  // --- subscriptions ---------------------------------------------------------------------

  #createSub(body: unknown): { id: string } {
    if (!isObject(body) || typeof body.name !== "string" || !SUB_NAME.test(body.name)) {
      throw new HttpError(10204, "invalid subscription name");
    }
    const startAt = isObject(body.start_at) ? body.start_at.type : undefined;
    if (startAt !== "earliest" && startAt !== "latest") {
      throw new HttpError(10204, "invalid start_at");
    }
    const existing = [...this.#subs.values()].find(
      (s) => s.name.toLowerCase() === (body.name as string).toLowerCase(),
    );
    if (existing) {
      if (existing.startAt === startAt) return { id: existing.id };
      throw new HttpError(10201, "subscription exists with different settings");
    }
    if (this.#subs.size >= MAX_SUBSCRIPTIONS) {
      throw new HttpError(10219, "stream already has 100 subscriptions");
    }
    const id = `sub_${++this.#seq}`;
    this.#subs.set(id, {
      id,
      name: body.name,
      startAt,
      createdAt: new Date(this.#now()).toISOString(),
      cursor: startAt === "earliest" ? 0 : this.records.length,
      redeliver: [],
      leases: new Map(),
    });
    return { id };
  }

  #sub(id: string | undefined): Sub {
    const sub = this.#subs.get(id ?? "");
    if (!sub) throw new HttpError(10215, "subscription does not exist");
    return sub;
  }

  // --- leases ----------------------------------------------------------------------------

  #expire(sub: Sub): void {
    const now = this.#now();
    for (const [workerId, lease] of sub.leases) {
      if (now >= lease.leasedUntilMs) {
        sub.leases.delete(workerId);
        sub.redeliver.unshift(...lease.indices);
      }
    }
    sub.redeliver.sort((a, b) => a - b);
  }

  #consume(sub: Sub, body: unknown): unknown {
    if (!isObject(body)) throw new HttpError(10204, "invalid body");
    const { worker_id: workerId, max_records: max } = body;
    if (typeof workerId !== "string" || workerId.length < 1 || workerId.length > 256) {
      throw new HttpError(10204, "invalid worker_id");
    }
    if (typeof max !== "number" || !Number.isInteger(max) || max < 1 || max > 10_000) {
      throw new HttpError(10204, "invalid max_records");
    }
    this.#expire(sub);
    const now = this.#now();

    const held = sub.leases.get(workerId);
    if (held) {
      held.leasedUntilMs = now + LEASE_MS;
      return this.#batchView(held);
    }
    if (sub.leases.size >= MAX_LEASES) throw new HttpError(10216, "all 128 lease slots in use");

    const indices: number[] = [];
    let bytes = 0;
    const take = (index: number): boolean => {
      const size = this.records[index]?.content.length ?? 0;
      if (indices.length >= max || (indices.length > 0 && bytes + size > MAX_CONSUME_BYTES)) {
        return false;
      }
      indices.push(index);
      bytes += size;
      return true;
    };
    while (sub.redeliver.length > 0 && take(sub.redeliver[0] as number)) sub.redeliver.shift();
    while (sub.cursor < this.records.length && take(sub.cursor)) sub.cursor++;

    if (indices.length === 0) return { batch_id: null, leased_until_ms: null, records: [] };

    const lease: Lease = {
      batchId: `batch_${++this.#seq}`,
      workerId,
      indices,
      leasedUntilMs: now + LEASE_MS,
    };
    sub.leases.set(workerId, lease);
    for (const i of indices) this.deliveries.set(i, (this.deliveries.get(i) ?? 0) + 1);
    return this.#batchView(lease);
  }

  #batchView(lease: Lease): unknown {
    return {
      batch_id: lease.batchId,
      leased_until_ms: lease.leasedUntilMs,
      records: lease.indices.map((i) => this.records[i]),
    };
  }

  #findLease(sub: Sub, batchId: string): Lease | undefined {
    this.#expire(sub);
    return [...sub.leases.values()].find((l) => l.batchId === batchId);
  }

  #ack(sub: Sub, batchId: string, body: unknown): unknown {
    requireWorker(body);
    const lease = this.#findLease(sub, batchId);
    if (lease) {
      sub.leases.delete(lease.workerId);
      const acked = this.acked.get(sub.id) ?? [];
      acked.push(...lease.indices);
      this.acked.set(sub.id, acked);
    }
    return {};
  }

  #nack(sub: Sub, batchId: string, body: unknown): unknown {
    requireWorker(body);
    const lease = this.#findLease(sub, batchId);
    if (lease) {
      sub.leases.delete(lease.workerId);
      sub.redeliver.push(...lease.indices);
      sub.redeliver.sort((a, b) => a - b);
    }
    return {};
  }

  #extend(sub: Sub, batchId: string, body: unknown): unknown {
    const workerId = requireWorker(body);
    const lease = this.#findLease(sub, batchId);
    if (!lease || lease.workerId !== workerId) {
      throw new HttpError(10218, "lease no longer held by this worker");
    }
    lease.leasedUntilMs = Math.max(lease.leasedUntilMs, this.#now() + LEASE_MS);
    return { leased_until_ms: lease.leasedUntilMs };
  }

  // --- produce ---------------------------------------------------------------------------

  #produce(body: unknown): void {
    if (!isObject(body) || Object.keys(body).some((k) => k !== "records")) {
      throw new HttpError(10204, "body must only contain records");
    }
    const records = body.records;
    if (!Array.isArray(records) || records.length === 0) {
      throw new HttpError(10204, "records must be a non-empty array");
    }
    const accepted: StoredRecord[] = [];
    for (const r of records) {
      if (!isObject(r) || Object.keys(r).some((k) => k !== "content" && k !== "headers")) {
        throw new HttpError(10204, "invalid record fields");
      }
      if (typeof r.content !== "string" || !STD_BASE64.test(r.content)) {
        throw new HttpError(10204, "content must be standard base64");
      }
      if (Math.floor((r.content.length * 3) / 4) > MAX_RECORD_BYTES) {
        throw new HttpError(10207, "record exceeds 1 MB");
      }
      if (r.headers !== undefined) {
        if (!isObject(r.headers) || Object.values(r.headers).some((v) => typeof v !== "string")) {
          throw new HttpError(10204, "headers must map string to string");
        }
        if (Object.keys(r.headers).length > 32) throw new HttpError(10204, "too many headers");
      }
      accepted.push({
        timestamp_ms: this.#now(),
        content: r.content,
        ...(r.headers && Object.keys(r.headers).length > 0
          ? { headers: r.headers as { [name: string]: string } }
          : {}),
      });
    }
    this.records.push(...accepted);
  }

  // --- errors ----------------------------------------------------------------------------

  #checkAuth(header: string | undefined): void {
    if (!header) throw new HttpError(10208, "missing Authorization header");
    if (header !== `Bearer ${this.token}`) throw new HttpError(10209, "invalid token");
  }

  #fail(endpoint: Endpoint, code: number, message: string): Response {
    const status = STATUS[code] ?? 500;
    if (endpoint === "produce") {
      return json(status, {
        success: false,
        error: { code, message, retryable: PRODUCE_RETRYABLE.has(code) },
      });
    }
    return json(status, {
      result: null,
      success: false,
      errors: [{ code, message }],
      messages: [],
    });
  }
}

function view(s: Sub) {
  return {
    id: s.id,
    name: s.name,
    start_at: { type: s.startAt },
    created_at: s.createdAt,
    modified_at: s.createdAt,
  };
}

function requireWorker(body: unknown): string {
  if (!isObject(body) || typeof body.worker_id !== "string" || body.worker_id.length === 0) {
    throw new HttpError(10204, "invalid worker_id");
  }
  return body.worker_id;
}

function isObject(v: unknown): v is { [key: string]: unknown } {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function envelope(result: unknown) {
  return { result, success: true, errors: [], messages: [] };
}

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

async function gunzip(bytes: Uint8Array): Promise<Uint8Array> {
  const stream = new Blob([bytes as BlobPart])
    .stream()
    .pipeThrough(new DecompressionStream("gzip"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}
