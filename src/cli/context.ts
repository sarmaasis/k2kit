import { K2Client } from "../client.js";
import type { Logger } from "../types.js";
import { UsageError } from "./format.js";

/** Everything the CLI touches in the outside world, injectable for tests. */
export interface CliIo {
  env: { [name: string]: string | undefined };
  /** Writes to stdout (records only). */
  stdout(text: string): void;
  /** Writes to stderr (logs, warnings, prompts). */
  stderr(text: string): void;
  /** Reads all of stdin. */
  readStdin(): Promise<Uint8Array>;
  /** Asks a yes/no question; `null` when there is no interactive terminal. */
  confirm(question: string): Promise<boolean | null>;
  /** Registers a SIGINT/SIGTERM handler; returns an unregister function. */
  onSignal(handler: (signal: string) => void): () => void;
  now(): number;
  fetch?: typeof fetch;
}

/** Parsed global flags plus a ready client. */
export interface CliContext {
  io: CliIo;
  streamId: string;
  token: string;
  endpoint: string | undefined;
  client: K2Client;
}

const STREAM_ID = /^[0-9a-f]{32}$/;

export function createContext(
  io: CliIo,
  flags: { stream?: string | undefined; token?: string | undefined; endpoint?: string | undefined },
): CliContext {
  const streamId = flags.stream ?? io.env.K2_STREAM_ID;
  const token = flags.token ?? io.env.CLOUDFLARE_API_TOKEN;
  if (!streamId) throw new UsageError("missing --stream (or K2_STREAM_ID)");
  if (!STREAM_ID.test(streamId))
    throw new UsageError("stream id must be 32 lowercase hex characters");
  if (!token) throw new UsageError("missing --token (or CLOUDFLARE_API_TOKEN)");
  const endpoint = flags.endpoint;
  const client = new K2Client({
    streamId,
    token,
    ...(endpoint ? { endpoint } : {}),
    ...(io.fetch ? { fetch: io.fetch } : {}),
  });
  return { io, streamId, token, endpoint, client };
}

/** Logger that sends warnings and errors to stderr and drops chatter. */
export function stderrLogger(io: CliIo): Logger {
  return {
    debug: () => {},
    info: () => {},
    warn: (m) => io.stderr(`${m}\n`),
    error: (m) => io.stderr(`${m}\n`),
  };
}

const TEMP_PREFIX = "k2kit-";

/**
 * Lists subscriptions before creating a temporary one: fails at the 100 cap and warns from 90.
 */
export async function checkSubscriptionCapacity(ctx: CliContext): Promise<void> {
  const subs = await ctx.client.listSubscriptions();
  const hint = `run \`k2kit subs rm --prefix ${TEMP_PREFIX}\` to clean up leaked temporary subscriptions`;
  if (subs.length >= 100) {
    throw new Error(`stream already has ${subs.length}/100 subscriptions; ${hint}`);
  }
  if (subs.length >= 90) {
    ctx.io.stderr(`warning: stream has ${subs.length}/100 subscriptions; ${hint}\n`);
  }
}

/** Deletes a temporary subscription, printing its name if that fails. Never throws. */
export async function deleteTemporary(ctx: CliContext, id: string, name: string): Promise<void> {
  try {
    await ctx.client.deleteSubscription(id);
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err);
    ctx.io.stderr(
      `warning: failed to delete subscription ${name} (${id}): ${reason}\n` +
        `  delete it with: k2kit subs rm ${name}\n`,
    );
  }
}
