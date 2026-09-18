/**
 * Storage and signalling backend for resumable streams.
 *
 * Chunks are transported as opaque strings and their order must be preserved. Any
 * framing needed to make a chunk survive the transport is the adapter's own concern.
 */
export type StreamAdapter = {
  /**
   * Discards any state left over from a previous stream with the same id, registers
   * the id, and begins consuming `chunks` in the background.
   *
   * Resolves once the stream is registered and resumable, not once it is complete.
   * A stream id may be reused, so implementations must not replay stale chunks.
   *
   * A generation id names one generation and is never reused: what an implementation does
   * with a reused one is its own business, and no implementation has to make it work.
   */
  createStream(options: {
    streamId: string;
    /**
     * Identifies this generation of the stream id, so it can be resumed and stopped on its
     * own. The stream id is repointed at it.
     */
    generationId: string;
    chunks: ReadableStream<string>;
    /**
     * Keeps the host process alive until the promise settles.
     * Relevant for serverless runtimes where work after the response is suspended.
     */
    waitUntil?: (promise: Promise<unknown>) => void;
  }): Promise<void>;
  /**
   * Returns the chunks of an in-flight stream: those already produced, followed by
   * those still to come, ending when the stream ends.
   *
   * With `generationId`, follows that generation even when a newer one of the stream id is
   * current. Without it, follows the generation the stream id currently points at.
   *
   * Resolves `null` when the stream is unknown, already finished, or expired.
   */
  resumeStream(options: {
    streamId: string;
    generationId?: string;
  }): Promise<ReadableStream<string> | null>;
  /**
   * Signals the process that owns a generation of the stream to stop producing.
   *
   * With `generationId`, only that generation is stopped, including one that has not
   * started yet if the store can keep the request until it does. Without it, the generation
   * the stream id currently points at is stopped, and there is nothing to stop when it
   * points at none. Safe to call for an unknown or finished stream.
   */
  requestStop(options: { streamId: string; generationId?: string }): Promise<void>;
  /**
   * Registers a producer-side listener for stop requests addressed to one generation. Returns
   * a function that removes the listener, and only that listener.
   *
   * A stop requested for the generation before the listener was registered should still be
   * reported, where the store can keep it.
   */
  onStopRequested(options: {
    streamId: string;
    generationId: string;
    onStop: () => void;
  }): Promise<() => void>;
};

/**
 * Translates between chunks and the strings an adapter stores.
 *
 * `decode` returns `undefined` to drop a chunk that cannot be represented,
 * so a single corrupt chunk never fails an entire resume.
 */
export type StreamCodec<CHUNK> = {
  encode(chunk: CHUNK): string;
  decode(data: string): CHUNK | undefined | Promise<CHUNK | undefined>;
};
