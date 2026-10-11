import type { CliContext } from "./context.js";
import { formatSubscriptionTable, UsageError } from "./format.js";

const RECENT_MS = 60 * 60_000;
const SUBSCRIPTION_ID = /^[0-9a-f]{32}$/i;

/** `k2kit subs ls`: table, or NDJSON with `--json`. */
export async function subsList(ctx: CliContext, opts: { json: boolean }): Promise<void> {
  const subs = await ctx.client.listSubscriptions();
  if (opts.json) {
    for (const s of subs) await ctx.io.stdout(`${JSON.stringify(s)}\n`);
    return;
  }
  if (subs.length === 0) {
    ctx.io.stderr("no subscriptions\n");
    return;
  }
  await ctx.io.stdout(`${formatSubscriptionTable(subs)}\n`);
}

/** `k2kit subs rm <id|name>` or `k2kit subs rm --prefix <p> [--yes]`. */
export async function subsRemove(
  ctx: CliContext,
  opts: {
    target: string | undefined;
    prefix: string | undefined;
    /** Only delete subscriptions created before this time (epoch ms). */
    olderThan: number | undefined;
    yes: boolean;
  },
): Promise<void> {
  const { io, client } = ctx;
  if (opts.target !== undefined && opts.prefix !== undefined) {
    throw new UsageError("give either <id|name> or --prefix, not both");
  }
  if (opts.target !== undefined && opts.olderThan !== undefined) {
    throw new UsageError("--older-than only works with --prefix");
  }
  if (opts.target !== undefined) {
    const byName = await client.findSubscription(opts.target);
    let id = byName?.id;
    if (id === undefined) {
      // Not a name: treat it as an id. K2 ids are 32 lowercase hex characters (it rejects other
      // forms with a vague "Request body is invalid"), so normalize case and say plainly when
      // it's neither a name nor an id.
      if (!SUBSCRIPTION_ID.test(opts.target)) {
        throw new Error(`no subscription named "${opts.target}" (and it isn't a subscription id)`);
      }
      id = opts.target.toLowerCase();
    }
    await client.deleteSubscription(id);
    io.stderr(`deleted ${byName ? `${byName.name} (${id})` : id}\n`);
    return;
  }
  if (opts.prefix === undefined || opts.prefix === "") {
    throw new UsageError("subs rm needs <id|name> or --prefix <p>");
  }

  const prefix = opts.prefix.toLowerCase();
  const olderThan = opts.olderThan;
  const matches = (await client.listSubscriptions()).filter(
    (s) =>
      s.name.toLowerCase().startsWith(prefix) &&
      (olderThan === undefined || Date.parse(s.created_at) < olderThan),
  );
  const age = opts.olderThan === undefined ? "" : " and are older than --older-than";
  if (matches.length === 0) {
    io.stderr(`no subscriptions start with "${opts.prefix}"${age}\n`);
    return;
  }
  // Recent k2kit-* subscriptions may belong to tail/replay sessions still running elsewhere.
  const recent = matches.filter((s) => io.now() - Date.parse(s.created_at) < RECENT_MS).length;
  if (opts.olderThan === undefined && recent > 0) {
    io.stderr(
      `warning: ${recent} of these were created in the last hour and may belong to running ` +
        "tail/replay sessions; add --older-than 1h to skip them\n",
    );
  }
  if (!opts.yes) {
    io.stderr(`${matches.length} subscription(s) match "${opts.prefix}"${age}:\n`);
    for (const s of matches) io.stderr(`  ${s.name}  ${s.id}  created ${s.created_at}\n`);
    const ok = await io.confirm(`delete ${matches.length} subscription(s)? [y/N] `);
    if (ok === null) throw new Error("refusing to delete without --yes (no interactive terminal)");
    if (!ok) {
      io.stderr("aborted; nothing deleted\n");
      return;
    }
  }
  let failed = 0;
  for (const s of matches) {
    try {
      await client.deleteSubscription(s.id);
      io.stderr(`deleted ${s.name} (${s.id})\n`);
    } catch (err) {
      failed++;
      io.stderr(
        `failed to delete ${s.name} (${s.id}): ${err instanceof Error ? err.message : err}\n`,
      );
    }
  }
  if (failed > 0) throw new Error(`${failed} of ${matches.length} deletions failed`);
}
