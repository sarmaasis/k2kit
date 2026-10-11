import { Consumer } from "../consumer.js";
import { type CliContext, stderrLogger, withTemporarySubscription } from "./context.js";
import { formatRecord, matchesFilter, type RecordFilter } from "./format.js";

export interface TailOptions {
  json: boolean;
  headers: boolean;
  limit: number | undefined;
  filter: RecordFilter;
}

/**
 * Creates a temporary `latest` subscription, prints records as they arrive, acks each batch,
 * and always tries to delete the subscription on exit.
 */
export async function tail(ctx: CliContext, opts: TailOptions): Promise<void> {
  const { io } = ctx;
  await withTemporarySubscription(ctx, "tail", "latest", async ({ id, name }, stop) => {
    io.stderr(`tailing via temporary subscription ${name}; press Ctrl+C to stop\n`);
    const consumer = new Consumer({
      streamId: ctx.streamId,
      token: ctx.token,
      subscription: { id },
      workerIdPrefix: name,
      pollBackoff: { minMs: 250, maxMs: 2_000 },
      logger: stderrLogger(io),
      ...(ctx.endpoint ? { endpoint: ctx.endpoint } : {}),
      ...(io.fetch ? { fetch: io.fetch } : {}),
    });
    stop.addEventListener("abort", () => void consumer.stop({ timeoutMs: 5_000 }), { once: true });

    let printed = 0;
    await consumer.run(async (batch) => {
      for (const record of batch.records) {
        if (opts.limit !== undefined && printed >= opts.limit) break;
        if (!matchesFilter(record, opts.filter)) continue;
        await io.stdout(`${formatRecord(record, opts)}\n`);
        printed++;
      }
      if (opts.limit !== undefined && printed >= opts.limit) void consumer.stop();
    });
  });
}
