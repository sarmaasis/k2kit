// Mirrors dist/**/*.d.ts to .d.cts so `require` consumers get CommonJS-flavoured types.
import { readdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

async function walk(dir) {
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name !== "cli") await walk(path);
    } else if (entry.name.endsWith(".d.ts")) {
      const source = await readFile(path, "utf8");
      const cts = source.replace(/(from\s+["']\.{1,2}\/[^"']+)\.js(["'])/g, "$1.cjs$2");
      await writeFile(path.replace(/\.d\.ts$/, ".d.cts"), cts);
    }
  }
}

await walk("dist");
