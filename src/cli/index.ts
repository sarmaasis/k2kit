import { createInterface } from "node:readline/promises";
import type { CliIo } from "./context.js";
import { main } from "./main.js";

const stopHandlers = new Set<(signal: string) => void>();
let stopRequests = 0;
let stdoutClosed = false;
let cleanupNote: string | undefined;

function requestStop(signal: string): void {
  stopRequests++;
  const forced = stopHandlers.size === 0 || (stopRequests > 1 && signal !== "EPIPE");
  if (forced) {
    if (stopHandlers.size > 0) process.stderr.write("forced exit\n");
    if (cleanupNote) process.stderr.write(`warning: ${cleanupNote}\n`);
    process.exit(signal === "EPIPE" ? 0 : 130);
  }
  if (signal !== "EPIPE") {
    process.stderr.write(`\n${signal} received; cleaning up (press again to force)\n`);
  }
  for (const handler of stopHandlers) handler(signal);
}

process.on("SIGINT", () => requestStop("SIGINT"));
process.on("SIGTERM", () => requestStop("SIGTERM"));
// Stdout closed early (e.g. `k2kit tail | head`): stop and clean up like Ctrl+C.
process.stdout.on("error", (err: NodeJS.ErrnoException) => {
  if (err.code !== "EPIPE") throw err;
  stdoutClosed = true;
  requestStop("EPIPE");
});

const io: CliIo = {
  env: process.env,
  stdout: (text) => {
    if (stdoutClosed || process.stdout.write(text)) return;
    // The pipe is full (a slow reader): wait until it drains, so output can't pile up in memory.
    return new Promise<void>((resolve) => {
      const done = () => {
        process.stdout.off("drain", done);
        process.stdout.off("close", done);
        process.stdout.off("error", done);
        resolve();
      };
      process.stdout.on("drain", done);
      process.stdout.on("close", done);
      process.stdout.on("error", done);
    });
  },
  stderr: (text) => {
    process.stderr.write(text);
  },
  stdin: () => process.stdin as AsyncIterable<Uint8Array>,
  stdinIsTTY: process.stdin.isTTY === true,
  async confirm(question) {
    if (!process.stdin.isTTY || !process.stderr.isTTY) return null;
    const rl = createInterface({ input: process.stdin, output: process.stderr });
    try {
      return /^y(es)?$/i.test((await rl.question(question)).trim());
    } finally {
      rl.close();
    }
  },
  setCleanupNote(note) {
    cleanupNote = note;
  },
  onSignal(handler) {
    stopHandlers.add(handler);
    return () => {
      stopHandlers.delete(handler);
    };
  },
  now: () => Date.now(),
};

process.exitCode = await main(process.argv.slice(2), io);
