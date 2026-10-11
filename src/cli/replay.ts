import { Backoff, sleep } from "../backoff.js";
import { isRetryableError } from "../errors.js";
import { type CliContext, checkSubscriptionCapacity, deleteTemporary } from "./context.js";
import { formatRecord, matchesFilter, type RecordFilter, randomSuffix } from "./format.js";

export interface ReplayOptions {
  since: number | undefined;
  until: number | undefined;
  filter: RecordFilter;
  limit: number | undefined;
  json: boolean;
  headers: boolean;
}

const MAX_RECORDS = 1_000;
const EMPTY_POLL_MS = 1_000;

/**
 * Reads the stream from `earliest` through a temporary subscription, filtering by time and text
 * on the client. Stops after two consecutive empty batches or at `limit`, then deletes the
 * subscription.
 */
export async function replay(ctx: CliContext, opts: ReplayOptions): Promise<void> {
  const { io, client } = ctx;
  if (opts.since === undefined) {
    io.stderr(
      "warning: no --since given; replaying all retained data (consumed data will be billed once K2 billing starts)\n",
    );
  }
  await checkSubscriptionCapacity(ctx);
  const name = `k2kit-replay-${randomSuffix()}`;
  const id = await client.ensureSubscription(name, "earliest");
  io.stderr(`replaying via temporary subscription ${name}\n`);

  const stop = new AbortController();
  const unregister = io.onSignal(() => stop.abort());
  const retry = new Backoff({ minMs: 500, maxMs: 10_000 });
  let printed = 0;
  let empties = 0;
  try {
    while (!stop.signal.aborted && empties < 2) {
      let batch: Awaited<ReturnType<typeof client.consume>>;
      try {
        batch = await client.consume(id, name, MAX_RECORDS);
        retry.reset();
      } catch (err) {
        if (!isRetryableError(err) || retry.attempt >= 8) throw err;
        await sleep(retry.next(), stop.signal);
        continue;
      }
      if (batch === null) {
        empties++;
        if (empties < 2) await sleep(EMPTY_POLL_MS, stop.signal);
        continue;
      }
      empties = 0;

      let pastUntil = false;
      for (const record of batch.records) {
        if (opts.limit !== undefined && printed >= opts.limit) break;
        if (opts.since !== undefined && record.timestampMs < opts.since) continue;
        if (opts.until !== undefined && record.timestampMs >= opts.until) {
          pastUntil = true; // a single worker on one subscription reads in order
          break;
        }
        if (!matchesFilter(record, opts.filter)) continue;
        io.stdout(`${formatRecord(record, opts)}\n`);
        printed++;
      }
      await client.ack(id, batch.id, name);
      if (pastUntil || (opts.limit !== undefined && printed >= opts.limit)) break;
    }
  } finally {
    unregister();
    await deleteTemporary(ctx, id, name);
  }
  io.stderr(`replayed ${printed} records\n`);
}
