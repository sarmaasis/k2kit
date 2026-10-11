import { toBase64, utf8DecodeStrict } from "../record.js";
import type { K2Record, Subscription } from "../types.js";

/** Thrown for bad arguments; the CLI exits with code 2. */
export class UsageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UsageError";
  }
}

/** Client-side record filter shared by `tail` and `replay`. */
export interface RecordFilter {
  /** Substring the UTF-8 content must contain. */
  grep?: string | undefined;
  /** Headers that must all be present with exactly these values. */
  headers?: { [name: string]: string };
}

/** Whether a record passes `filter`. */
export function matchesFilter(record: K2Record, filter: RecordFilter): boolean {
  for (const [name, value] of Object.entries(filter.headers ?? {})) {
    if (record.headers[name] !== value) return false;
  }
  return filter.grep === undefined || record.text().includes(filter.grep);
}

/** One output line for a record (no trailing newline). */
export function formatRecord(
  record: K2Record,
  opts: { json?: boolean; headers?: boolean },
): string {
  const text = utf8DecodeStrict(record.bytes);
  if (opts.json) {
    return JSON.stringify({
      timestamp_ms: record.timestampMs,
      headers: record.headers,
      content: text ?? toBase64(record.bytes),
      encoding: text === null ? "base64" : "utf8",
    });
  }
  const parts = [new Date(record.timestampMs).toISOString()];
  if (opts.headers) {
    const pairs = Object.entries(record.headers).map(([k, v]) => `${k}=${v}`);
    parts.push(pairs.length > 0 ? pairs.join(" ") : "-");
  }
  parts.push(text ?? `base64:${toBase64(record.bytes)}`);
  return parts.join("  ");
}

const DURATION = /^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/;
const UNIT_MS: { [unit: string]: number } = {
  ms: 1,
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
};

/** Parses `30m`, `2h`, `7d` (relative to `now`) or an ISO date into epoch ms. */
export function parseTime(value: string, now: number, flag: string): number {
  const match = DURATION.exec(value.trim());
  if (match) return now - Number(match[1]) * (UNIT_MS[match[2] as string] as number);
  const ms = Date.parse(value);
  if (Number.isNaN(ms)) {
    throw new UsageError(`${flag} must be a duration like 30m, 2h, 7d or an ISO date`);
  }
  return ms;
}

/** Parses a positive integer flag value. */
export function parsePositiveInt(value: string | undefined, flag: string): number | undefined {
  if (value === undefined) return undefined;
  const n = Number(value);
  if (!Number.isInteger(n) || n < 1) throw new UsageError(`${flag} must be a positive integer`);
  return n;
}

/** Renders subscriptions as an aligned text table. */
export function formatSubscriptionTable(subs: Subscription[]): string {
  const rows = [
    ["NAME", "ID", "START_AT", "CREATED_AT"],
    ...subs.map((s) => [s.name, s.id, s.start_at.type, s.created_at]),
  ];
  const widths =
    rows[0]?.map((_, col) => Math.max(...rows.map((r) => (r[col] ?? "").length))) ?? [];
  return rows
    .map((r) =>
      r
        .map((cell, col) => (col === r.length - 1 ? cell : cell.padEnd(widths[col] ?? 0)))
        .join("  "),
    )
    .join("\n");
}

/** Six random lowercase alphanumerics for temporary subscription names. */
export function randomSuffix(): string {
  const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
  const bytes = globalThis.crypto.getRandomValues(new Uint8Array(6));
  return Array.from(bytes, (b) => alphabet[b % alphabet.length]).join("");
}
