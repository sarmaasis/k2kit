import { parseArgs } from "node:util";
import { isK2Error } from "../errors.js";
import { VERSION } from "../version.js";
import { type CliIo, createContext } from "./context.js";
import { parsePositiveInt, parseTime, UsageError } from "./format.js";
import { parseHeaders, produce } from "./produce.js";
import { replay } from "./replay.js";
import { subsList, subsRemove } from "./subs.js";
import { tail } from "./tail.js";

export const HELP = `k2kit ${VERSION} — consume, tail and inspect Cloudflare K2 streams

Usage:
  k2kit tail [--grep <text>] [--header k=v ...] [--json] [--headers] [--limit N]
  k2kit replay [--since <dur|ISO>] [--until <dur|ISO>] [--grep <text>] [--header k=v ...]
               [--limit N] [--json] [--headers]
  k2kit subs ls [--json]
  k2kit subs rm <id|name>
  k2kit subs rm --prefix <p> [--older-than <dur>] [--yes]
  k2kit produce [--header k=v ...] [--raw] [--gzip]

Global flags:
  --stream <id>    Stream id (or K2_STREAM_ID)
  --token <token>  API token (or CLOUDFLARE_API_TOKEN)
  -h, --help       Show this help
  --version        Show the version

For tail and replay, --grep matches the record's text and every --header k=v must match.
Durations: 30m, 2h, 7d (relative to now); otherwise an ISO date. Date-times need a
timezone (2026-10-11T10:00Z or +05:30); a date alone means midnight UTC.
Records go to stdout; logs and warnings go to stderr.
Exit codes: 0 ok, 1 runtime or K2 error, 2 usage error.
`;

const OPTIONS = {
  stream: { type: "string" },
  token: { type: "string" },
  endpoint: { type: "string" },
  help: { type: "boolean", short: "h" },
  version: { type: "boolean" },
  json: { type: "boolean" },
  headers: { type: "boolean" },
  limit: { type: "string" },
  since: { type: "string" },
  until: { type: "string" },
  grep: { type: "string" },
  prefix: { type: "string" },
  "older-than": { type: "string" },
  yes: { type: "boolean", short: "y" },
  header: { type: "string", multiple: true },
  raw: { type: "boolean" },
  gzip: { type: "boolean" },
} as const;

type OptionName = keyof typeof OPTIONS;
const GLOBAL: OptionName[] = ["stream", "token", "endpoint", "help", "version"];
const ALLOWED: { [command: string]: OptionName[] } = {
  tail: ["grep", "header", "json", "headers", "limit"],
  replay: ["since", "until", "grep", "header", "limit", "json", "headers"],
  "subs ls": ["json"],
  "subs rm": ["prefix", "older-than", "yes"],
  produce: ["header", "raw", "gzip"],
};

/** Runs the CLI and returns the process exit code. */
export async function main(argv: string[], io: CliIo): Promise<number> {
  let token: string | undefined;
  try {
    const { values, positionals } = parseArgs({
      args: argv,
      options: OPTIONS,
      allowPositionals: true,
      strict: true,
    });
    token = values.token ?? io.env.CLOUDFLARE_API_TOKEN;
    if (values.version) {
      io.stdout(`${VERSION}\n`);
      return 0;
    }
    if (values.help || positionals.length === 0) {
      io.stdout(HELP);
      return positionals.length === 0 && !values.help ? 2 : 0;
    }

    const [first, second, ...rest] = positionals;
    const command = first === "subs" ? `subs ${second ?? ""}`.trim() : (first as string);
    const extra = first === "subs" ? rest : [second, ...rest].filter((p) => p !== undefined);
    const allowed = ALLOWED[command];
    if (!allowed) throw new UsageError(`unknown command "${command}"`);
    for (const name of Object.keys(values) as OptionName[]) {
      if (!GLOBAL.includes(name) && !allowed.includes(name)) {
        throw new UsageError(`--${name} is not valid for "${command}"`);
      }
    }
    const maxPositionals = command === "subs rm" ? 1 : 0;
    if (extra.length > maxPositionals) {
      throw new UsageError(`unexpected argument "${extra[maxPositionals]}"`);
    }

    const limit = parsePositiveInt(values.limit, "--limit");
    const now = io.now();
    const since = values.since === undefined ? undefined : parseTime(values.since, now, "--since");
    const until = values.until === undefined ? undefined : parseTime(values.until, now, "--until");
    if (since !== undefined && until !== undefined && since >= until) {
      throw new UsageError("--since must be earlier than --until");
    }
    const ctx = createContext(io, values);
    const json = values.json ?? false;
    const headers = values.headers ?? false;
    const filter = { grep: values.grep, headers: parseHeaders(values.header ?? []) };

    switch (command) {
      case "tail":
        await tail(ctx, { json, headers, limit, filter });
        break;
      case "replay":
        await replay(ctx, { since, until, filter, limit, json, headers });
        break;
      case "subs ls":
        await subsList(ctx, { json });
        break;
      case "subs rm":
        await subsRemove(ctx, {
          target: extra[0],
          prefix: values.prefix,
          olderThan:
            values["older-than"] === undefined
              ? undefined
              : parseTime(values["older-than"], now, "--older-than"),
          yes: values.yes ?? false,
        });
        break;
      case "produce":
        await produce(ctx, {
          headers: values.header ?? [],
          raw: values.raw ?? false,
          gzip: values.gzip ?? false,
        });
        break;
    }
    return 0;
  } catch (err) {
    const redact = (text: string) => (token ? text.split(token).join("***") : text);
    if (err instanceof UsageError || isParseArgsError(err)) {
      io.stderr(`error: ${redact((err as Error).message)}\nrun "k2kit --help" for usage\n`);
      return 2;
    }
    if (isK2Error(err)) {
      io.stderr(`K2 ${err.code}: ${redact(err.message)}\n`);
      return 1;
    }
    io.stderr(`error: ${redact(err instanceof Error ? err.message : String(err))}\n`);
    return 1;
  }
}

function isParseArgsError(err: unknown): boolean {
  const code = (err as { code?: unknown } | null)?.code;
  return typeof code === "string" && code.startsWith("ERR_PARSE_ARGS");
}
