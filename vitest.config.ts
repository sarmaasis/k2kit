import { existsSync, readFileSync } from "node:fs";
import { defineConfig } from "vitest/config";

// Load K2 credentials for the integration test from .dev.vars (git-ignored) when present.
if (existsSync(".dev.vars")) {
  for (const line of readFileSync(".dev.vars", "utf8").split("\n")) {
    const match = /^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$/.exec(line);
    if (match?.[1] && process.env[match[1]] === undefined) process.env[match[1]] = match[2];
  }
}

export default defineConfig({
  // examples/ import the published name; point it at the source.
  resolve: { alias: { k2kit: new URL("./src/index.ts", import.meta.url).pathname } },
  test: {
    include: ["test/**/*.test.ts"],
    testTimeout: 20_000,
    coverage: {
      provider: "v8",
      include: ["src/**"],
      // The bin entry only wires up process I/O; the CLI smoke tests cover it end to end.
      exclude: ["src/cli/index.ts"],
      reporter: ["text-summary", "text"],
      // Minimums from unit tests alone (live K2/Redis tests are skipped without credentials).
      thresholds: { statements: 90, branches: 83, functions: 89, lines: 91 },
    },
  },
});
