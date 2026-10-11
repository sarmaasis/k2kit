import type { K2Record, RawRecord } from "./types.js";

/** Header that {@link K2Client.produce} adds so consumers can dedupe producer retries. */
export const ID_HEADER = "k2kit-id";
/** Ids longer than this are hashed into the dedupe key, so keys (e.g. in Redis) stay small. */
const MAX_RAW_ID_LENGTH = 128;

const encoder = new TextEncoder();
const strictDecoder = new TextDecoder("utf-8", { fatal: true });
const lenientDecoder = new TextDecoder("utf-8");

// Fast paths, picked once: the native Uint8Array base64 API (Bun, Deno, newer Node), then Node's
// Buffer, then portable atob/btoa. All produce identical standard, padded base64.
type NativeBase64 = {
  fromBase64?: (text: string) => Uint8Array;
  prototype: { toBase64?: (this: Uint8Array) => string };
};
type BufferLike = {
  from(
    data: ArrayBufferLike,
    byteOffset: number,
    length: number,
  ): { toString(enc: "base64"): string };
  from(data: string, enc: "base64"): Uint8Array;
};
const native = Uint8Array as unknown as NativeBase64;
const NodeBuffer = (globalThis as { Buffer?: BufferLike }).Buffer;

/** Encodes bytes as standard base64 with padding (the only form K2 accepts). */
export const toBase64: (bytes: Uint8Array) => string = native.prototype.toBase64
  ? (bytes) => (native.prototype.toBase64 as (this: Uint8Array) => string).call(bytes)
  : NodeBuffer
    ? (bytes) =>
        NodeBuffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString("base64")
    : toBase64Portable;

/** Decodes standard base64 into bytes. Input comes from K2, which only sends valid base64. */
export const fromBase64: (base64: string) => Uint8Array = native.fromBase64
  ? (text) => (native.fromBase64 as (text: string) => Uint8Array)(text)
  : NodeBuffer
    ? (text) => {
        const buf = NodeBuffer.from(text, "base64");
        // Small results live in Node's shared 8 KB pool. A view would expose unrelated memory
        // through `.buffer`, so copy those; large results own their memory and are returned as is.
        return buf.byteOffset === 0 && buf.buffer.byteLength === buf.byteLength
          ? new Uint8Array(buf.buffer, 0, buf.byteLength)
          : new Uint8Array(buf);
      }
    : fromBase64Portable;

// A multiple of 3, so each chunk encodes to whole base64 groups with no padding in between.
const ENCODE_CHUNK = 3 * 0x2000;

/** Portable `toBase64` (btoa), used when no faster API exists. Exported for tests. */
export function toBase64Portable(bytes: Uint8Array): string {
  const parts: string[] = [];
  for (let i = 0; i < bytes.length; i += ENCODE_CHUNK) {
    parts.push(btoa(String.fromCharCode(...bytes.subarray(i, i + ENCODE_CHUNK))));
  }
  return parts.join("");
}

/** Portable `fromBase64` (atob), used when no faster API exists. Exported for tests. */
export function fromBase64Portable(base64: string): Uint8Array {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

/** UTF-8 encodes a string. */
export function utf8Encode(text: string): Uint8Array {
  return encoder.encode(text);
}

/** Decodes UTF-8, returning `null` when the bytes are not valid UTF-8. */
export function utf8DecodeStrict(bytes: Uint8Array): string | null {
  try {
    return strictDecoder.decode(bytes);
  } catch {
    return null;
  }
}

/**
 * Turns produce input into bytes: `Uint8Array` as is, strings as UTF-8, other binary data
 * (`ArrayBuffer`, `DataView`, typed arrays) as its bytes, anything else as JSON. Throws
 * `TypeError` for values with no JSON form (`undefined`, functions), which would otherwise
 * silently become empty records.
 */
export function contentToBytes(content: Uint8Array | string | object): Uint8Array {
  if (content instanceof Uint8Array) return content;
  if (typeof content === "string") return utf8Encode(content);
  // Like a fetch body: JSON.stringify would turn these into "{}" or an object of indices.
  if (ArrayBuffer.isView(content)) {
    return new Uint8Array(content.buffer, content.byteOffset, content.byteLength);
  }
  if (
    content instanceof ArrayBuffer ||
    (typeof SharedArrayBuffer !== "undefined" && content instanceof SharedArrayBuffer)
  ) {
    return new Uint8Array(content);
  }
  const json = JSON.stringify(content) as string | undefined;
  if (json === undefined) {
    throw new TypeError(`content must be bytes, a string or a JSON value, not ${typeof content}`);
  }
  return utf8Encode(json);
}

/**
 * FNV-1a 64-bit hash over one or more byte arrays, as 16 lowercase hex chars.
 * Uses four 16-bit limbs and integer math instead of BigInt (about 10x faster). The prime is
 * 2^40 + 0x1b3, so each step is limb·0x1b3 plus the lower limbs shifted by 8, with carries.
 */
export function fnv1a64(...parts: Uint8Array[]): string {
  let h0 = 0x2325;
  let h1 = 0x8422;
  let h2 = 0x9ce4;
  let h3 = 0xcbf2;
  for (const part of parts) {
    for (let i = 0; i < part.length; i++) {
      h0 ^= part[i] as number;
      const t0 = h0 * 0x1b3;
      let t1 = h1 * 0x1b3;
      let t2 = h2 * 0x1b3 + (h0 << 8);
      const t3 = h3 * 0x1b3 + (h1 << 8);
      t1 += t0 >>> 16;
      h0 = t0 & 0xffff;
      t2 += t1 >>> 16;
      h1 = t1 & 0xffff;
      h3 = (t3 + (t2 >>> 16)) & 0xffff;
      h2 = t2 & 0xffff;
    }
  }
  return [h3, h2, h1, h0].map((limb) => limb.toString(16).padStart(4, "0")).join("");
}

class Record implements K2Record {
  readonly timestampMs: number;
  readonly headers: Readonly<{ [name: string]: string }>;
  readonly bytes: Uint8Array;
  readonly id: string | undefined;
  #key: string | undefined;

  constructor(raw: RawRecord) {
    this.timestampMs = raw.timestamp_ms;
    this.headers = raw.headers ?? {};
    this.bytes = fromBase64(raw.content);
    // A blank id would make every such record share one key, so it counts as missing.
    const id = this.headers[ID_HEADER];
    this.id = id === undefined || id.trim() === "" ? undefined : id;
  }

  text(): string {
    return lenientDecoder.decode(this.bytes);
  }

  json<T = unknown>(): T {
    return JSON.parse(this.text()) as T;
  }

  key(): string {
    if (this.#key === undefined) {
      const id = this.id;
      if (id === undefined) {
        this.#key = fnv1a64(utf8Encode(`${this.timestampMs}:`), this.bytes);
      } else {
        this.#key = id.length > MAX_RAW_ID_LENGTH ? `id-fnv:${fnv1a64(utf8Encode(id))}` : id;
      }
    }
    return this.#key;
  }
}

/** Decodes a record as returned by the K2 consume endpoint. */
export function decodeRecord(raw: RawRecord): K2Record {
  return new Record(raw);
}
