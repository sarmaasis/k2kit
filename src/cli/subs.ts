import type { CliContext } from "./context.js";
import { formatSubscriptionTable, UsageError } from "./format.js";

/** `k2kit subs ls`: table, or NDJSON with `--json`. */
export async function subsList(ctx: CliContext, opts: { json: boolean }): Promise<void> {
  const subs = await ctx.client.listSubscriptions();
  if (opts.json) {
    for (const s of subs) ctx.io.stdout(`${JSON.stringify(s)}\n`);
    return;
  }
  if (subs.length === 0) {
    ctx.io.stderr("no subscriptions\n");
    return;
  }
  ctx.io.stdout(`${formatSubscriptionTable(subs)}\n`);
}

/** `k2kit subs rm <id|name>` or `k2kit subs rm --prefix <p> [--yes]`. */
export async function subsRemove(
  ctx: CliContext,
  opts: { target: string | undefined; prefix: string | undefined; yes: boolean },
): Promise<void> {
  const { io, client } = ctx;
  if (opts.target !== undefined && opts.prefix !== undefined) {
    throw new UsageError("give either <id|name> or --prefix, not both");
  }
  if (opts.target !== undefined) {
    const byName = await client.findSubscription(opts.target);
    const id = byName?.id ?? opts.target;
    await client.deleteSubscription(id);
    io.stderr(`deleted ${byName ? `${byName.name} (${id})` : id}\n`);
    return;
  }
  if (opts.prefix === undefined || opts.prefix === "") {
    throw new UsageError("subs rm needs <id|name> or --prefix <p>");
  }

  const prefix = opts.prefix.toLowerCase();
  const matches = (await client.listSubscriptions()).filter((s) =>
    s.name.toLowerCase().startsWith(prefix),
  );
  if (matches.length === 0) {
    io.stderr(`no subscriptions start with "${opts.prefix}"\n`);
    return;
  }
  if (!opts.yes) {
    io.stderr(`${matches.length} subscription(s) match "${opts.prefix}":\n`);
    for (const s of matches) io.stderr(`  ${s.name}  ${s.id}\n`);
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
