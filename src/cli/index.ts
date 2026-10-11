import { createInterface } from "node:readline/promises";
import type { CliIo } from "./context.js";
import { main } from "./main.js";

const stopHandlers = new Set<(signal: string) => void>();
let stopRequests = 0;
let stdoutClosed = false;

function requestStop(signal: string): void {
  stopRequests++;
  if (stopHandlers.size === 0) process.exit(signal === "EPIPE" ? 0 : 130);
  if (stopRequests > 1 && signal !== "EPIPE") {
    process.stderr.write("forced exit\n");
    process.exit(130);
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
    if (!stdoutClosed) process.stdout.write(text);
  },
  stderr: (text) => {
    process.stderr.write(text);
  },
  async readStdin() {
    const chunks: Uint8Array[] = [];
    for await (const chunk of process.stdin) chunks.push(chunk as Uint8Array);
    return new Uint8Array(Buffer.concat(chunks));
  },
  async confirm(question) {
    if (!process.stdin.isTTY || !process.stderr.isTTY) return null;
    const rl = createInterface({ input: process.stdin, output: process.stderr });
    try {
      return /^y(es)?$/i.test((await rl.question(question)).trim());
    } finally {
      rl.close();
    }
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
