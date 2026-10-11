import { MAX_RECORD_BYTES } from "../client.js";
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
  const headers: { [name: string]: string } = {};
  for (const pair of pairs) {
    const eq = pair.indexOf("=");
    if (eq < 1) throw new UsageError(`--header must be k=v, got "${pair}"`);
    headers[pair.slice(0, eq)] = pair.slice(eq + 1);
  }
  if (Object.keys(headers).length > 31) {
    throw new UsageError("at most 31 --header flags (K2 allows 32 headers, one is k2kit-id)");
  }
  return headers;
}

/** Splits stdin into records: one per non-empty line, or one record with `raw`. */
export function splitInput(input: Uint8Array, raw: boolean): Uint8Array[] {
  if (raw) return input.byteLength > 0 ? [input] : [];
  const out: Uint8Array[] = [];
  let start = 0;
  for (let i = 0; i <= input.length; i++) {
    if (i === input.length || input[i] === 0x0a) {
      let end = i;
      if (end > start && input[end - 1] === 0x0d) end--;
      const line = input.subarray(start, end);
      if (line.some((b) => b !== 0x20 && b !== 0x09)) out.push(line);
      start = i + 1;
    }
  }
  return out;
}

/** `k2kit produce`: reads stdin and appends it to the stream in requests under 5 MB. */
export async function produce(ctx: CliContext, opts: ProduceOptions): Promise<void> {
  const { io, client } = ctx;
  const headers = parseHeaders(opts.headers);
  const chunks = splitInput(await io.readStdin(), opts.raw);
  if (chunks.length === 0) {
    io.stderr("no input on stdin; nothing produced\n");
    return;
  }
  chunks.forEach((content, i) => {
    if (content.byteLength > MAX_RECORD_BYTES) {
      throw new UsageError(
        `record ${i + 1} is ${content.byteLength} bytes; K2 records must be under 1 MB`,
      );
    }
  });

  let produced = 0;
  try {
    const result = await client.produceBatched(
      chunks.map((content) => ({ content, headers })),
      {
        gzip: opts.gzip,
        onProgress: (p) => {
          produced = p.records;
        },
      },
    );
    const noun = result.requests === 1 ? "request" : "requests";
    io.stderr(`produced ${result.records} records in ${result.requests} ${noun}\n`);
  } catch (err) {
    if (produced > 0) io.stderr(`produced ${produced} records before the error\n`);
    if (isK2Error(err) && err.code === 10212) {
      io.stderr("the failed request may or may not have been stored\n");
    }
    throw err;
  }
}
