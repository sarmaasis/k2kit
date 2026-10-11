import { Backoff, sleep } from "../backoff.js";
import { K2Client } from "../client.js";
import { isK2Error, isRetryableError } from "../errors.js";
import type { Logger, StartAt } from "../types.js";
import { randomSuffix, UsageError } from "./format.js";

/** Everything the CLI touches in the outside world, injectable for tests. */
export interface CliIo {
  env: { [name: string]: string | undefined };
  /**
   * Writes to stdout (records only). May return a promise that resolves once the output has
   * drained; callers await it, so a slow reader slows the CLI down instead of filling memory.
   */
  stdout(text: string): void | Promise<void>;
  /** Writes to stderr (logs, warnings, prompts). */
  stderr(text: string): void;
  /** Stdin as a stream of chunks. */
  stdin(): AsyncIterable<Uint8Array>;
  /** Whether stdin is an interactive terminal (nothing piped in). */
  stdinIsTTY?: boolean;
  /** Asks a yes/no question; `null` when there is no interactive terminal. */
  confirm(question: string): Promise<boolean | null>;
  /**
   * Sets (or clears, with `undefined`) a message printed if the process is force-exited, e.g.
   * which temporary subscription may be left behind.
   */
  setCleanupNote?(note: string | undefined): void;
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
  let client: K2Client;
  try {
    client = new K2Client({
      streamId,
      token,
      ...(endpoint ? { endpoint } : {}),
      ...(io.fetch ? { fetch: io.fetch } : {}),
    });
  } catch (err) {
    // Bad token or endpoint (e.g. a trailing newline): a usage error, not a K2 failure.
    throw new UsageError(err instanceof Error ? err.message : String(err));
  }
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
  const hint = `run \`k2kit subs rm --prefix ${TEMP_PREFIX} --older-than 1h\` to clean up leaked temporary subscriptions`;
  if (subs.length >= 100) {
    throw new Error(`stream already has ${subs.length}/100 subscriptions; ${hint}`);
  }
  if (subs.length >= 90) {
    ctx.io.stderr(`warning: stream has ${subs.length}/100 subscriptions; ${hint}\n`);
  }
}

/**
 * Runs `body` with a temporary `k2kit-<kind>-*` subscription and always deletes it afterwards.
 * Interrupt handling is registered *before* the subscription is created and released only after
 * it's deleted, so a Ctrl+C at any point can't leak it; `stop` aborts on that interrupt.
 */
export async function withTemporarySubscription(
  ctx: CliContext,
  kind: "tail" | "replay",
  startAt: StartAt,
  body: (sub: { id: string; name: string }, stop: AbortSignal) => Promise<void>,
): Promise<void> {
  const { io } = ctx;
  const stop = new AbortController();
  const unregister = io.onSignal(() => stop.abort());
  let created: { id: string; name: string } | undefined;
  try {
    await checkSubscriptionCapacity(ctx);
    if (stop.signal.aborted) return;
    const name = `${TEMP_PREFIX}${kind}-${randomSuffix()}`;
    // Set before the create request, so even a forced exit mid-request names what to clean up.
    io.setCleanupNote?.(
      `temporary subscription ${name} may not have been deleted; delete it with: k2kit subs rm ${name}`,
    );
    // If Ctrl+C arrives while this request is in flight, finish creating it so it can be deleted.
    const id = await createTemporary(ctx, name, startAt);
    created = { id, name };
    if (stop.signal.aborted) return;
    await body(created, stop.signal);
  } finally {
    if (created) await deleteTemporary(ctx, created.id, created.name);
    io.setCleanupNote?.(undefined);
    unregister();
  }
}

const CREATE_ATTEMPTS = 3;

/**
 * Creates a temporary subscription. Creation is idempotent (same name and settings return the
 * same id), so transient failures are retried. A request can fail without an answer after K2
 * has already created the subscription (a timeout or dropped connection), so in that case the
 * random name is looked up and, if it exists, deleted; if even that lookup fails, the name is
 * printed. When K2 answers with an error code, it created nothing.
 */
async function createTemporary(ctx: CliContext, name: string, startAt: StartAt): Promise<string> {
  const retry = new Backoff({ minMs: 250, maxMs: 2_000 });
  for (let attempt = 1; ; attempt++) {
    try {
      return await ctx.client.ensureSubscription(name, startAt);
    } catch (err) {
      if (isRetryableError(err) && attempt < CREATE_ATTEMPTS) {
        await sleep(retry.next());
        continue;
      }
      // A K2 error code means K2 answered and didn't create it. Only without an answer
      // (network error, timeout, gateway error) might it exist.
      if (isK2Error(err) && err.code !== 0) throw err;
      let leftover: { id: string } | null | undefined;
      try {
        leftover = await ctx.client.findSubscription(name);
      } catch {
        leftover = undefined; // can't tell whether it was created
      }
      if (leftover) {
        await deleteTemporary(ctx, leftover.id, name);
      } else if (leftover === undefined) {
        ctx.io.stderr(
          `warning: creating temporary subscription ${name} failed and it may exist; ` +
            `check with \`k2kit subs ls\` and delete it with: k2kit subs rm ${name}\n`,
        );
      }
      throw err;
    }
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
