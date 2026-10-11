import { checkHeaders, estimateRecordBytes, MAX_RECORD_BYTES } from "../client.js";
import { isK2Error } from "../errors.js";
import type { CliContext } from "./context.js";
import { UsageError } from "./format.js";

export interface ProduceOptions {
  headers: string[];
  raw: boolean;
  gzip: boolean;
}

/** Parses repeated `--header k=v` flags. */
export function parseHeaders(pairs: string[]): { [name: string]: string } {
  // No prototype, so a header named `__proto__` or `constructor` is stored like any other.
  const headers: { [name: string]: string } = Object.create(null);
  for (const pair of pairs) {
    const eq = pair.indexOf("=");
    if (eq < 1) throw new UsageError(`--header must be k=v, got "${pair}"`);
    headers[pair.slice(0, eq)] = pair.slice(eq + 1);
  }
  if (Object.keys(headers).length > 31) {
    throw new UsageError("at most 31 --header flags (K2 allows 32 headers, one is k2kit-id)");
  }
  const problem = checkHeaders(headers);
  if (problem) throw new UsageError(`--header: ${problem}`);
  return headers;
}

/** Incrementally splits a byte stream into non-blank lines (LF or CRLF). */
export class LineSplitter {
  #carry: Uint8Array = new Uint8Array();

  /** Bytes of the current, unfinished line. */
  get pendingBytes(): number {
    return this.#carry.byteLength;
  }

  /** Adds a chunk and returns the complete lines it finished. */
  push(chunk: Uint8Array): Uint8Array[] {
    const data = this.#carry.byteLength > 0 ? concat(this.#carry, chunk) : chunk;
    const lines: Uint8Array[] = [];
    let start = 0;
    for (let i = 0; i < data.length; i++) {
      if (data[i] === 0x0a) {
        pushLine(lines, data.subarray(start, i));
        start = i + 1;
      }
    }
    this.#carry = data.slice(start);
    return lines;
  }

  /** Returns the final line, if the input didn't end with a newline. */
  end(): Uint8Array[] {
    const lines: Uint8Array[] = [];
    pushLine(lines, this.#carry);
    this.#carry = new Uint8Array();
    return lines;
  }
}

function pushLine(lines: Uint8Array[], line: Uint8Array): void {
  const end = line.length > 0 && line[line.length - 1] === 0x0d ? line.length - 1 : line.length;
  const trimmed = line.subarray(0, end);
  if (trimmed.some((b) => b !== 0x20 && b !== 0x09)) lines.push(trimmed);
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.byteLength + b.byteLength);
  out.set(a, 0);
  out.set(b, a.byteLength);
  return out;
}

/** Splits a whole input into records: one per non-blank line, or one record with `raw`. */
export function splitInput(input: Uint8Array, raw: boolean): Uint8Array[] {
  if (raw) return input.byteLength > 0 ? [input] : [];
  const splitter = new LineSplitter();
  return [...splitter.push(input), ...splitter.end()];
}

/** Input buffered before a send is forced (requests are still split under 5 MB). */
export const FLUSH_BYTES = 4_000_000;
/** Pending lines are sent after this long without new input, so `tail -f | k2kit produce` works. */
export const FLUSH_IDLE_MS = 1_000;

/**
 * `k2kit produce`: streams stdin to the stream. Lines are sent in batches as they arrive (every
 * ~4 MB, or after 1 s without new input), so it works on endless input like `tail -f` and never
 * holds more than a few MB in memory. `--raw` sends all of stdin as one record (max 1 MB).
 */
export async function produce(ctx: CliContext, opts: ProduceOptions): Promise<void> {
  const { io, client } = ctx;
  const headers = parseHeaders(opts.headers);
  if (io.stdinIsTTY) {
    io.stderr(
      opts.raw
        ? "reading one record from the terminal; press Ctrl+D to send it\n"
        : "reading records from the terminal, one per line; press Ctrl+D to finish\n",
    );
  }

  let produced = 0;
  let requests = 0;
  let lines = 0;
  let pending: Uint8Array[] = [];
  let pendingBytes = 0;
  let idleTimer: ReturnType<typeof setTimeout> | undefined;
  let sending: Promise<void> = Promise.resolve();
  let failure: { error: unknown } | undefined;

  // Resolves when reading should stop early: Ctrl+C, or a send failed (even while stdin is idle,
  // as with `tail -f`, where the next line might never come).
  let interrupted = false;
  let stopReading = () => {};
  const stopped = new Promise<null>((resolve) => {
    stopReading = () => resolve(null);
  });

  // Sends are chained so they stay in order; a failure stops all later sends.
  const flush = (): Promise<void> => {
    clearTimeout(idleTimer);
    idleTimer = undefined;
    if (pending.length === 0) return sending;
    const batch = pending;
    pending = [];
    pendingBytes = 0;
    sending = sending.then(async () => {
      if (failure) return;
      const base = produced;
      try {
        const result = await client.produceBatched(
          batch.map((content) => ({ content, headers })),
          {
            gzip: opts.gzip,
            onProgress: (p) => {
              produced = base + p.records;
            },
          },
        );
        requests += result.requests;
      } catch (error) {
        failure = { error };
        stopReading();
      }
    });
    return sending;
  };

  const add = async (line: Uint8Array): Promise<void> => {
    lines++;
    // Counted like K2 does, with the --header values (the automatic id is skipped if it won't fit).
    const size = estimateRecordBytes(line.byteLength, headers);
    if (size > MAX_RECORD_BYTES) {
      throw new UsageError(
        `record ${lines} is about ${size} bytes with its headers; K2 records must be under 1 MB`,
      );
    }
    pending.push(line);
    pendingBytes += line.byteLength;
    if (pendingBytes >= FLUSH_BYTES) {
      await flush(); // backpressure: don't read further ahead than one request's worth
    } else {
      idleTimer ??= setTimeout(() => void flush(), FLUSH_IDLE_MS);
    }
  };

  const unregister = io.onSignal(() => {
    interrupted = true;
    stopReading();
  });

  const input = io.stdin()[Symbol.asyncIterator]();
  const nextChunk = () => Promise.race([input.next(), stopped]);
  let thrown: { error: unknown } | undefined;
  try {
    if (opts.raw) {
      const parts: Uint8Array[] = [];
      let size = 0;
      for (;;) {
        const next = await nextChunk();
        if (next === null || next.done) break;
        size += next.value.byteLength;
        if (size > MAX_RECORD_BYTES) {
          throw new UsageError("--raw input exceeds 1 MB; K2 records must be under 1 MB");
        }
        parts.push(next.value);
      }
      if (size > 0 && !interrupted) await add(parts.reduce(concat, new Uint8Array()));
    } else {
      const splitter = new LineSplitter();
      for (;;) {
        const next = await nextChunk();
        if (next === null || next.done) break;
        for (const line of splitter.push(next.value)) await add(line);
        if (failure) break;
        if (splitter.pendingBytes > MAX_RECORD_BYTES) {
          throw new UsageError(
            `record ${lines + 1} is over 1 MB with no newline yet; K2 records must be under 1 MB`,
          );
        }
      }
      if (!interrupted && !failure) for (const line of splitter.end()) await add(line);
    }
  } catch (error) {
    thrown = { error };
  } finally {
    unregister();
    // Send what was read so far, even when stopping early on an error or Ctrl+C.
    await flush();
    // Ask stdin to close, but don't wait: a stream blocked waiting for more input (like
    // `tail -f`) only settles that pending read later, which would hang a Ctrl+C exit.
    void input.return?.()?.catch(() => {});
  }

  const error = failure?.error ?? thrown?.error;
  if (error !== undefined) {
    // Say how far it got: earlier lines may already be stored even though the command failed.
    io.stderr(`produced ${produced} records before the error\n`);
    if (isK2Error(error) && error.code === 10212) {
      io.stderr("the failed request may or may not have been stored\n");
    }
    throw error;
  }
  if (lines === 0) {
    io.stderr(
      interrupted ? "interrupted; nothing produced\n" : "no input on stdin; nothing produced\n",
    );
    return;
  }
  const noun = requests === 1 ? "request" : "requests";
  const suffix = interrupted ? " (interrupted)" : "";
  io.stderr(`produced ${produced} records in ${requests} ${noun}${suffix}\n`);
}
