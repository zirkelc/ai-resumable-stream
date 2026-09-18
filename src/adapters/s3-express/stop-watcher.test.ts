import { describe, expect, test, vi } from "vitest";
import { createStopWatchers } from "./stop-watcher.js";

const POLL_INTERVAL_MS = 5;

/**
 * A stop request store, so a test can say which generations have been stopped without a
 * bucket, a database, or anything else the watchers would otherwise need.
 */
function createStops() {
  const stopped = new Set<string>();
  const asked: Array<string> = [];

  return {
    asked,
    stop: (streamId: string, generationId: string) => stopped.add(`${streamId}:${generationId}`),
    isStopRequested: async (streamId: string, generationId: string) => {
      asked.push(`${streamId}:${generationId}`);
      return stopped.has(`${streamId}:${generationId}`);
    },
  };
}

describe(`stop watchers`, () => {
  test(`should report a stop once the generation it guards is stopped`, async () => {
    // Arrange
    const stops = createStops();
    const watchers = createStopWatchers({ ...stops, pollIntervalMs: POLL_INTERVAL_MS });
    const onStop = vi.fn();

    // Act
    watchers.watch(`chat`, `gen-1`, onStop);
    stops.stop(`chat`, `gen-1`);

    // Assert
    await vi.waitFor(() => expect(onStop).toHaveBeenCalled());
  });

  test(`should report a stop requested before it started watching`, async () => {
    // Arrange
    const stops = createStops();
    const watchers = createStopWatchers({ ...stops, pollIntervalMs: POLL_INTERVAL_MS });
    const onStop = vi.fn();
    stops.stop(`chat`, `gen-1`);

    // Act
    watchers.watch(`chat`, `gen-1`, onStop);

    // Assert
    await vi.waitFor(() => expect(onStop).toHaveBeenCalled());
  });

  test(`should not report the stop of another generation of the same stream id`, async () => {
    // Arrange
    const stops = createStops();
    const watchers = createStopWatchers({ ...stops, pollIntervalMs: POLL_INTERVAL_MS });
    const onFirst = vi.fn();
    const onSecond = vi.fn();

    /** An id reused while its previous producer is still shutting down. */
    watchers.watch(`chat`, `gen-1`, onFirst);
    watchers.watch(`chat`, `gen-2`, onSecond);

    // Act
    stops.stop(`chat`, `gen-2`);

    // Assert
    await vi.waitFor(() => expect(onSecond).toHaveBeenCalled());
    await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS * 4));
    expect(onFirst).not.toHaveBeenCalled();
  });

  test(`should stop asking once it is removed`, async () => {
    // Arrange
    const stops = createStops();
    const watchers = createStopWatchers({ ...stops, pollIntervalMs: POLL_INTERVAL_MS });
    const onStop = vi.fn();
    const unwatch = watchers.watch(`chat`, `gen-1`, onStop);
    await vi.waitFor(() => expect(stops.asked.length).toBeGreaterThan(0));

    // Act
    unwatch();
    const askedAfterUnwatch = stops.asked.length;
    stops.stop(`chat`, `gen-1`);
    await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS * 4));

    // Assert
    expect(stops.asked.length).toBe(askedAfterUnwatch);
    expect(onStop).not.toHaveBeenCalled();
  });

  test(`should keep asking when the store fails`, async () => {
    // Arrange
    let calls = 0;
    const onStop = vi.fn();
    const watchers = createStopWatchers({
      pollIntervalMs: POLL_INTERVAL_MS,
      isStopRequested: async () => {
        calls += 1;
        if (calls === 1) throw new Error(`unreachable`);
        return true;
      },
    });

    // Act
    watchers.watch(`chat`, `gen-1`, onStop);

    // Assert
    await vi.waitFor(() => expect(onStop).toHaveBeenCalled());
  });
});
