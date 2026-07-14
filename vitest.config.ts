import { coverageConfigDefaults, defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    /**
     * Each Redis-backed test file boots its own in-memory Redis server and flushes the
     * database between tests. Running the files sequentially keeps two servers from
     * racing for the same port and keeps one file's flush out of another's stream.
     */
    fileParallelism: false,
    coverage: {
      provider: "v8",
      include: ["src/**/*.ts"],
      exclude: [...coverageConfigDefaults.exclude],
    },
  },
});
