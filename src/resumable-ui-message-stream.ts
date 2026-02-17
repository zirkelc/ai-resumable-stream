import type { UIMessageChunk } from "ai";
import { type AsyncIterableStream, createAsyncIterableStream } from "ai-stream-utils";
import type { createClient } from "redis";
import { createResumableStreamContext } from "resumable-stream";
import { convertSSEToUIMessageStream } from "./convert-sse-stream-to-ui-message-stream.js";
import { convertUIMessageToSSEStream } from "./convert-ui-message-stream-to-sse-stream.js";

const KEY_PREFIX = `ai-resumable-stream`;

type Redis = ReturnType<typeof createClient>;

type CreateResumableUIMessageStream = {
  /**
   * A unique identifier for the stream.
   */
  streamId: string;
  /**
   * A Redis client from the `redis` package.
   * Checks if the client is already connected before attempting to connect.
   */
  subscriber: Redis;
  /**
   * A Redis client from the `redis` package.
   * Checks if the client is already connected before attempting to connect.
   */
  publisher: Redis;
  /**
   * An optional AbortController that, when provided, allows the stream to be stopped by aborting the controller.
   * If not provided, the stream will not be stoppable.
   */
  abortController?: AbortController;
  /**
   * A function that takes a promise and ensures that the current program stays alive
   * until the promise is resolved.
   *
   * Omit if you are deploying to a server environment, where you don't have to worry about
   * the function getting suspended.
   */
  waitUntil?: (promise: Promise<unknown>) => void;
};

/**
 * Creates a resumable context for starting, resuming and stopping UI message streams.
 */
export async function createResumableUIMessageStream(options: CreateResumableUIMessageStream) {
  const { streamId, abortController, publisher, subscriber, waitUntil = null } = options;

  const stopChannel = `${KEY_PREFIX}:stop:${streamId}`;

  const context = createResumableStreamContext({
    waitUntil,
    publisher,
    subscriber,
    keyPrefix: KEY_PREFIX,
  });

  await Promise.all([
    publisher.isOpen ? Promise.resolve() : publisher.connect(),
    subscriber.isOpen ? Promise.resolve() : subscriber.connect(),
  ]);

  /**
   * Unsubscribe from stop channel
   */
  async function unsubscribe() {
    if (!abortController) return;
    await subscriber.unsubscribe(stopChannel);
  }

  /**
   * Set up stop subscription if abortController provided
   */
  if (abortController) {
    await subscriber.subscribe(stopChannel, () => {
      abortController.abort();
    });

    /**
     * Cleanup when abort signal fires
     */
    abortController.signal.addEventListener(
      `abort`,
      () => {
        unsubscribe();
      },
      { once: true },
    );
  }

  /**
   * Start a new stream by creating a new resumable stream in Redis and returning a client stream for the UI.
   */
  async function startStream(
    stream: ReadableStream<UIMessageChunk>,
  ): Promise<AsyncIterableStream<UIMessageChunk>> {
    /**
     * Tee the stream into two streams: one for the client and one for the resumable stream in Redis.
     */
    const [clientStream, resumableStream] = stream.tee();

    /**
     * Convert stream of UI message chunks to SSE-formatted stream for storage in Redis.
     */
    const sseStream = convertUIMessageToSSEStream(resumableStream);

    /**
     * Create a new resumable stream in Redis with the stream ID.
     */
    await context.createNewResumableStream(streamId, () => sseStream);

    /**
     * Wrap client stream to auto-unsubscribe on completion
     */
    const wrappedStream = clientStream.pipeThrough(
      new TransformStream<UIMessageChunk, UIMessageChunk>({
        transform(chunk, controller) {
          controller.enqueue(chunk);
        },
        flush() {
          unsubscribe();
        },
      }),
    );

    return createAsyncIterableStream(wrappedStream);
  }

  /**
   * Resume an existing stream by fetching the resumable stream from Redis using the stream ID.
   */
  async function resumeStream(): Promise<AsyncIterableStream<UIMessageChunk> | null> {
    /**
     * Resume the existing stream from Redis using the stream ID.
     */
    const resumableStream = await context.resumeExistingStream(streamId);
    if (!resumableStream) return null;

    /**
     * Convert the SSE-formatted stream from Redis back into a stream of UI message chunks for the client.
     */
    const uiStream = convertSSEToUIMessageStream(resumableStream);

    return createAsyncIterableStream(uiStream);
  }

  /**
   * Publish a stop message to the stop channel, which will trigger the abortController to abort the stream.
   */
  async function stopStream(): Promise<void> {
    await publisher.publish(stopChannel, `stop`);
  }

  return { startStream, resumeStream, stopStream };
}
