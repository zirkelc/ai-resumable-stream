/**
 * Extra information a storage adapter may use when registering a stream.
 */
export type AdapterContext = {
  /**
   * Keeps the host process alive until the promise settles.
   * Relevant for serverless runtimes where work after the response is suspended.
   */
  waitUntil?: (promise: Promise<unknown>) => void;
};

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
   */
  createStream(
    streamId: string,
    chunks: ReadableStream<string>,
    context: AdapterContext,
  ): Promise<void>;
  /**
   * Returns the chunks of an in-flight stream: those already produced, followed by
   * those still to come, ending when the stream ends.
   *
   * Resolves `null` when the stream is unknown, already finished, or expired.
   */
  resumeStream(streamId: string): Promise<ReadableStream<string> | null>;
  /**
   * Signals whichever process owns the stream to stop producing.
   * Safe to call for an unknown or finished stream.
   */
  requestStop(streamId: string): Promise<void>;
  /**
   * Registers a producer-side listener for stop requests. Returns a function that
   * removes the listener.
   */
  onStopRequested(streamId: string, onStop: () => void): Promise<() => void>;
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
