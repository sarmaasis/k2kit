import { Consumer } from "../consumer.js";
import {
  type CliContext,
  checkSubscriptionCapacity,
  deleteTemporary,
  stderrLogger,
} from "./context.js";
import { formatRecord, matchesFilter, type RecordFilter, randomSuffix } from "./format.js";

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
  await checkSubscriptionCapacity(ctx);
  const name = `k2kit-tail-${randomSuffix()}`;
  const id = await ctx.client.ensureSubscription(name, "latest");
  io.stderr(`tailing via temporary subscription ${name}; press Ctrl+C to stop\n`);

  let unregister = () => {};
  try {
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
    unregister = io.onSignal(() => {
      void consumer.stop({ timeoutMs: 5_000 });
    });

    let printed = 0;
    await consumer.run((batch) => {
      for (const record of batch.records) {
        if (opts.limit !== undefined && printed >= opts.limit) break;
        if (!matchesFilter(record, opts.filter)) continue;
        io.stdout(`${formatRecord(record, opts)}\n`);
        printed++;
      }
      if (opts.limit !== undefined && printed >= opts.limit) void consumer.stop();
    });
  } finally {
    unregister();
    await deleteTemporary(ctx, id, name);
  }
}
