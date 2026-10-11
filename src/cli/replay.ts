import { Backoff, sleep } from "../backoff.js";
import { isRetryableError } from "../errors.js";
import { type CliContext, withTemporarySubscription } from "./context.js";
import { formatRecord, matchesFilter, type RecordFilter } from "./format.js";

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
  let printed = 0;
  await withTemporarySubscription(ctx, "replay", "earliest", async ({ id, name }, stop) => {
    io.stderr(`replaying via temporary subscription ${name}\n`);
    const retry = new Backoff({ minMs: 500, maxMs: 10_000 });
    let empties = 0;
    while (!stop.aborted && empties < 2) {
      let batch: Awaited<ReturnType<typeof client.consume>>;
      try {
        batch = await client.consume(id, name, MAX_RECORDS);
        retry.reset();
      } catch (err) {
        if (!isRetryableError(err) || retry.attempt >= 8) throw err;
        await sleep(retry.next(), stop);
        continue;
      }
      if (batch === null) {
        empties++;
        if (empties < 2) await sleep(EMPTY_POLL_MS, stop);
        continue;
      }
      empties = 0;

      // K2 timestamps are receive times and needn't be strictly ordered across producers, so
      // records past --until are skipped rather than ending the replay. It stops early only once
      // a whole batch is past --until, which means the reader has moved beyond the window.
      let allPastUntil = opts.until !== undefined;
      for (const record of batch.records) {
        if (opts.limit !== undefined && printed >= opts.limit) break;
        if (opts.until !== undefined && record.timestampMs >= opts.until) continue;
        allPastUntil = false;
        if (opts.since !== undefined && record.timestampMs < opts.since) continue;
        if (!matchesFilter(record, opts.filter)) continue;
        await io.stdout(`${formatRecord(record, opts)}\n`);
        printed++;
      }
      await client.ack(id, batch.id, name);
      if (allPastUntil || (opts.limit !== undefined && printed >= opts.limit)) break;
    }
  });
  io.stderr(`replayed ${printed} records\n`);
}
