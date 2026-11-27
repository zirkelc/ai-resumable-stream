import { configDefaults, coverageConfigDefaults, defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    /**
     * Each Redis-backed test file boots its own in-memory Redis server and flushes the
     * database between tests. Running the files sequentially keeps two servers from
     * racing for the same port and keeps one file's flush out of another's stream.
     */
    fileParallelism: false,
    // These tests need the Bun runtime and run with `bun test`
    exclude: [...configDefaults.exclude, "**/*.bun.test.ts"],
    coverage: {
      provider: "v8",
      include: ["src/**/*.ts"],
      exclude: [...coverageConfigDefaults.exclude, "**/*.bun.test.ts"],
    },
  },
});
