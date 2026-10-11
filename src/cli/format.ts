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

// Characters a terminal may act on: C0 controls (except tab), DEL, C1 controls, and the Unicode
// bidirectional overrides/isolates used to visually disguise text ("Trojan Source").
// biome-ignore lint/suspicious/noControlCharactersInRegex: matching control characters is the point
const UNSAFE = /[\u0000-\u0008\u000a-\u001f\u007f-\u009f\u200e\u200f\u202a-\u202e\u2066-\u2069]/g;
const NAMED: { [ch: string]: string } = { "\n": "\\n", "\r": "\\r" };

/** Makes untrusted text safe to print to a terminal on one line: control characters become visible escapes. */
export function escapeForTerminal(text: string): string {
  return text.replace(
    UNSAFE,
    (ch) => NAMED[ch] ?? `\\u${ch.charCodeAt(0).toString(16).padStart(4, "0")}`,
  );
}

/** One output line for a record (no trailing newline). Record data is escaped for the terminal. */
export function formatRecord(
  record: K2Record,
  opts: { json?: boolean; headers?: boolean },
): string {
  const text = utf8DecodeStrict(record.bytes);
  if (opts.json) {
    // JSON.stringify escapes C0 controls; also escape C1 and bidi characters (still valid JSON).
    return JSON.stringify({
      timestamp_ms: record.timestampMs,
      headers: record.headers,
      content: text ?? toBase64(record.bytes),
      encoding: text === null ? "base64" : "utf8",
    }).replace(UNSAFE, (ch) => `\\u${ch.charCodeAt(0).toString(16).padStart(4, "0")}`);
  }
  const parts = [new Date(record.timestampMs).toISOString()];
  if (opts.headers) {
    const pairs = Object.entries(record.headers).map(
      ([k, v]) => `${escapeForTerminal(k)}=${escapeForTerminal(v)}`,
    );
    parts.push(pairs.length > 0 ? pairs.join(" ") : "-");
  }
  parts.push(text === null ? `base64:${toBase64(record.bytes)}` : escapeForTerminal(text));
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

const BARE_NUMBER = /^\d+(?:\.\d+)?$/;
// Only ISO forms are accepted: Date.parse guesses at anything else, usually as local time (and
// reads "10" as a date in 2001). Date-times are rebuilt in the one format ECMAScript requires
// every engine to parse, so Node, Bun and Deno agree.
const ISO_DATE = /^\d{4}-\d{2}(?:-\d{2})?$/;
const ISO_DATETIME =
  /^(\d{4}-\d{2}-\d{2})[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?(Z|[+-]\d{2}:?\d{2})?$/i;

/**
 * Parses `30m`, `2h`, `7d` (relative to `now`) or an ISO date into epoch ms. Date-times need a
 * timezone (`Z` or `+05:30`); a date alone (`2026-10-11`) means midnight UTC.
 */
export function parseTime(value: string, now: number, flag: string): number {
  const text = value.trim();
  const duration = DURATION.exec(text);
  if (duration) return now - Number(duration[1]) * (UNIT_MS[duration[2] as string] as number);
  if (BARE_NUMBER.test(text)) {
    // A unit was probably left off ("10" meaning 10m); a lone year is too easily the same mistake.
    throw new UsageError(
      `${flag} needs a unit, e.g. ${text}m (minutes), ${text}h or ${text}d; for a date use YYYY-MM-DD`,
    );
  }
  let ms = Number.NaN;
  const dt = ISO_DATETIME.exec(text);
  if (dt) {
    const [, date, hours, minutes, seconds = "00", fraction = "", zone] = dt;
    if (zone === undefined) {
      throw new UsageError(`${flag} needs a timezone, e.g. ${text}Z (UTC) or ${text}+05:30`);
    }
    const millis = fraction.slice(0, 3).padEnd(3, "0");
    const offset = zone.toUpperCase() === "Z" ? "Z" : `${zone.slice(0, 3)}:${zone.slice(-2)}`;
    ms = Date.parse(`${date}T${hours}:${minutes}:${seconds}.${millis}${offset}`);
  } else if (ISO_DATE.test(text)) {
    ms = Date.parse(text);
  }
  if (Number.isNaN(ms)) {
    throw new UsageError(
      `${flag} must be a duration like 30m, 2h, 7d or an ISO date like 2026-10-11 or 2026-10-11T10:00Z`,
    );
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
