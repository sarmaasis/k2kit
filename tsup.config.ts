import { defineConfig } from "tsup";

// Declarations are emitted by `tsc -p tsconfig.build.json` (TypeScript 7 has no JS API for tsup's dts step).
export default defineConfig([
  {
    entry: { index: "src/index.ts" },
    format: ["esm", "cjs"],
    target: "node20",
    platform: "neutral",
    external: [/^node:/],
    sourcemap: true,
  },
  {
    entry: { "cli/index": "src/cli/index.ts" },
    format: ["esm"],
    target: "node20",
    platform: "node",
    sourcemap: true,
    banner: { js: "#!/usr/bin/env node" },
  },
]);
