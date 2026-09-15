export type SerialQueue = {
  /**
   * Runs `work` once everything queued before it has settled. Resolves, or rejects, with
   * that piece of work alone.
   */
  run(work: () => Promise<void>): Promise<void>;
};

/**
 * Runs work one piece at a time, in the order it was queued.
 *
 * Appends to a log have to be strictly ordered, because each one carries the offset the
 * last one ended at. Chunks, beats and the end record are written from different timers,
 * so without this they would race for that offset.
 */
export function createSerialQueue(): SerialQueue {
  let tail: Promise<void> = Promise.resolve();

  return {
    run(work) {
      /**
       * The next piece of work runs whether or not the one before it succeeded. A write
       * that was refused leaves the store exactly as it was, so what follows is still
       * valid at the same offset; stopping the queue on the first failure would strand
       * every write after it.
       */
      const next = tail.then(work, work);
      tail = next.then(
        () => {},
        () => {},
      );
      return next;
    },
  };
}
