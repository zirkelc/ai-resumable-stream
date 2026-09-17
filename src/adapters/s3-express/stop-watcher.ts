import { delay } from "./delay.js";

export type StopWatchers = {
  /**
   * Registers a watcher for a stream whose generation does not exist yet. It does nothing
   * until `begin` names one. Returns a function that removes it.
   */
  watch(streamId: string, onStop: () => void): () => void;
  /**
   * Hands the generation to whichever watcher of this stream id has waited longest, and
   * starts it asking.
   */
  begin(streamId: string, generationId: string): void;
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

type Watcher = {
  begin: (generationId: string | undefined) => void;
  cancel: () => void;
};

/**
 * Watches for stop requests on behalf of producers, when the store has no way to push one.
 *
 * Two things make this more than a poll loop. A watcher is asked for before the generation
 * it guards exists, because `core` registers the listener first and the stream second. And
 * a stream id may be reused while its previous producer is still shutting down, so at that
 * moment two watchers of one id are alive at once, each belonging to a different
 * generation. Pairing them first in first out is exact, because the two calls are always
 * made in pairs and in order.
 *
 * Nothing here knows where a stop request is kept. It asks a question and waits.
 */
export function createStopWatchers(options: CreateStopWatchersOptions): StopWatchers {
  const { isStopRequested, pollIntervalMs } = options;

  const waiting = new Map<string, Array<Watcher>>();

  function createWatcher(streamId: string, onStop: () => void): Watcher {
    const abortController = new AbortController();
    const { signal } = abortController;

    let begin: (generationId: string | undefined) => void = () => {};
    const started = new Promise<string | undefined>((resolve) => {
      begin = resolve;
    });

    void (async () => {
      const generationId = await started;
      /** Cancelled before it was ever paired with a generation. */
      if (generationId === undefined) return;

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

    return {
      begin,
      cancel: () => {
        abortController.abort();
        begin(undefined);
      },
    };
  }

  return {
    watch(streamId, onStop) {
      const watcher = createWatcher(streamId, onStop);
      const queue = waiting.get(streamId) ?? [];
      queue.push(watcher);
      waiting.set(streamId, queue);

      return () => {
        watcher.cancel();

        const queued = waiting.get(streamId);
        if (!queued) return;

        const index = queued.indexOf(watcher);
        if (index !== -1) queued.splice(index, 1);
        if (queued.length === 0) waiting.delete(streamId);
      };
    },

    begin(streamId, generationId) {
      const queue = waiting.get(streamId);
      const watcher = queue?.shift();
      if (queue?.length === 0) waiting.delete(streamId);
      watcher?.begin(generationId);
    },
  };
}
