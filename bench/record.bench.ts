// Hot paths of the consume and produce loops. Run with `pnpm bench`.
import { test } from "vitest";
import {
  decodeRecord,
  fnv1a64,
  fromBase64,
  fromBase64Portable,
  toBase64,
  toBase64Portable,
} from "../src/record.js";

// Local aliases: Vitest's module runner adds a getter call to every imported-binding access.
const decode = decodeRecord;

const oneMb = new Uint8Array(1_000_000).map((_, i) => i % 251);
const fiveMb = new Uint8Array(5_000_000).map((_, i) => i % 251);
const tenMbBase64 = toBase64(new Uint8Array(10_000_000).map((_, i) => i % 251));
const smallRecords = Array.from({ length: 100 }, (_, i) => ({
  timestamp_ms: i,
  content: toBase64(new TextEncoder().encode(JSON.stringify({ order: i, items: ["a", "b"] }))),
}));

test("decode a full 10 MB consume batch", async ({ bench }) => {
  await bench.compare(
    bench("fromBase64 (fast path)", () => void fromBase64(tenMbBase64)),
    bench("fromBase64Portable", () => void fromBase64Portable(tenMbBase64)),
  );
});

test("encode a 5 MB produce request", async ({ bench }) => {
  await bench.compare(
    bench("toBase64 (fast path)", () => void toBase64(fiveMb)),
    bench("toBase64Portable", () => void toBase64Portable(fiveMb)),
  );
});

test("dedupe keys", async ({ bench }) => {
  await bench.compare(
    bench("fnv1a64 over 1 MB (key() without k2kit-id)", () => void fnv1a64(oneMb)),
    bench("decode + key() for 100 small records", () => {
      for (const raw of smallRecords) decode(raw).key();
    }),
  );
});
