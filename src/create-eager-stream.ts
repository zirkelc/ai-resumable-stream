import { EventEmitter, on } from "node:events";

/**
 * Wraps a ReadableStream and eagerly drains it into a buffer.
 * Returns a new ReadableStream that reads from the buffer.
 *
 * This decouples the producer (source) from the consumer, ensuring the source
 * stream is always fully consumed regardless of consumer speed. This is critical
 * when using `tee()` - without eager draining, a slow consumer on one branch
 * creates backpressure that blocks the other branch.
 *
 * Implementation uses both a buffer and events:
 * - Buffer: Stores chunks for replay (handles chunks that arrived before consumer started)
 * - Events: Notifies consumer of new chunks arriving while actively waiting
 *
 * @example
 * const [clientStream, redisStream] = source.tee();
 * // Without eager stream: slow client blocks redis writes
 * // With eager stream: redis proceeds at full speed
 * const eager = createEagerStream(clientStream, () => cleanup());
 *
 * @param source - The source stream to buffer
 * @param onComplete - Optional callback fired when stream ends (normal completion or cancel)
 */
export function createEagerStream<T>(
  source: ReadableStream<T>,
  onComplete?: () => void,
): ReadableStream<T> {
  const emitter = new EventEmitter();
  const buffer: Array<T> = [];
  let done = false;
  let error: unknown = null;
  let readIndex = 0;

  /**
   * Background task: Eagerly drain source stream into buffer.
   * Runs independently of consumer - no backpressure.
   * Emits events to notify waiting consumers of new data.
   */
  (async () => {
    try {
      const reader = source.getReader();
      while (true) {
        const result = await reader.read();
        if (result.done) {
          done = true;
          emitter.emit("done");
          break;
        }
        buffer.push(result.value);
        emitter.emit("data", result.value);
      }
    } catch (err) {
      error = err;
      emitter.emit("error", err);
    }
  })();

  /**
   * Consumer-facing stream that reads from buffer.
   * First yields any buffered chunks, then awaits new events.
   */
  return new ReadableStream<T>({
    async pull(controller) {
      /** First: yield any chunks that were buffered before we started listening */
      while (readIndex < buffer.length) {
        controller.enqueue(buffer[readIndex++]);
      }

      /** If source already finished or errored, handle immediately */
      if (done) {
        controller.close();
        onComplete?.();
        return;
      }
      if (error) {
        controller.error(error);
        return;
      }

      /**
       * Wait for live events from background drainer.
       * - `close: ["done"]` ends iteration when "done" event is emitted
       * - `on()` automatically throws if an "error" event is emitted
       * The cast `as T` is necessary because Node's `on()` types event args as `any[]`.
       */
      try {
        for await (const [data] of on(emitter, "data", { close: ["done"] })) {
          controller.enqueue(data as T);
          readIndex++;
        }
        controller.close();
        onComplete?.();
      } catch (err) {
        controller.error(err);
      }
    },
    cancel() {
      onComplete?.();
    },
  });
}
