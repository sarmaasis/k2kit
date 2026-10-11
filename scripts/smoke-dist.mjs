// Loads the built package under whatever Node runs this script, with no dev dependencies installed.
import assert from "node:assert/strict";
import { createRequire } from "node:module";

const esm = await import("../dist/index.js");
const cjs = createRequire(import.meta.url)("../dist/index.cjs");

for (const mod of [esm, cjs]) {
  assert.equal(typeof mod.K2Client, "function");
  assert.equal(typeof mod.Consumer, "function");
  assert.equal(typeof mod.MemoryDedupeStore, "function");
}

const client = new esm.K2Client({ streamId: "0".repeat(32), token: "x" });
assert.equal(typeof client.consume, "function");
const runtime = globalThis.Deno
  ? `Deno ${globalThis.Deno.version.deno}`
  : process.versions.bun
    ? `Bun ${process.versions.bun}`
    : `Node ${process.versions.node}`;
console.log(`dist loads on ${runtime}`);
