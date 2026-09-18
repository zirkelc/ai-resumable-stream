import { delay } from "./delay.js";

export type StopWatchers = {
  /**
   * Asks, on a timer, whether a stop has been requested for this generation, and calls
   * `onStop` the first time it has. Returns a function that stops asking.
   */
  watch(streamId: string, generationId: string, onStop: () => void): () => void;
};

export type CreateStopWatchersOptions = {
  /**
   * Whether a stop has been asked for on this generation. Called on a timer, so it should
   * be cheap; anything it throws is treated as "not yet" and asked again.
   */
  isStopRequested: (streamId: string, generationId: string) => Promise<boolean>;
  /**
   * How often to ask.
   */
  pollIntervalMs: number;
};

/**
 * Watches for stop requests on behalf of producers, when the store has no way to push one.
 *
 * A watcher is told the generation it guards, and stop requests are kept per generation,
 * so it may start asking before the generation's log exists and still see a stop that was
 * requested earlier. Two generations of one stream id are watched independently.
 *
 * Nothing here knows where a stop request is kept. It asks a question and waits.
 */
export function createStopWatchers(options: CreateStopWatchersOptions): StopWatchers {
  const { isStopRequested, pollIntervalMs } = options;

  return {
    watch(streamId, generationId, onStop) {
      const abortController = new AbortController();
      const { signal } = abortController;

      void (async () => {
        while (!signal.aborted) {
          try {
            if (await isStopRequested(streamId, generationId)) {
              if (!signal.aborted) onStop();
              return;
            }
          } catch {
            /** Asked again after the next wait */
          }

          await delay(pollIntervalMs, signal);
        }
      })();

      return () => abortController.abort();
    },
  };
}
