import { JsonToSseTransformStream, type UIMessageChunk } from "ai";
import { type AsyncIterableStream, createAsyncIterableStream } from "ai-stream-utils";
import type { createClient } from "redis";
import { createResumableStreamContext } from "resumable-stream";
import { convertSSEToUIMessageStream } from "./convert-sse-stream-to-ui-message-stream.js";

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
 *
 * Leverages resumable-stream's internal eager drain pattern which drains the source
 * eagerly and enqueues directly to the output stream.
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
   *
   * Uses a single drain loop that:
   * 1. Reads from source stream
   * 2. Sends UIMessageChunk directly to client stream (no conversion)
   * 3. Sends SSE to Redis stream → resumable-stream → Redis
   * 4. Propagates errors to both streams
   */
  async function startStream(
    stream: ReadableStream<UIMessageChunk>,
  ): Promise<AsyncIterableStream<UIMessageChunk>> {
    /**
     * Track client disconnect to avoid unbounded memory growth
     */
    let clientCancelled = false;

    /**
     * Client stream for sending UI message chunks directly to the client without conversion.
     */
    let clientController: ReadableStreamDefaultController<UIMessageChunk>;
    const clientStream = new ReadableStream<UIMessageChunk>({
      start(controller) {
        clientController = controller;
      },
      cancel() {
        clientCancelled = true;
      },
    });

    /**
     * Redis stream with SSE conversion for resumable-stream persistence in Redis.
     * JsonToSseTransformStream converts UIMessageChunk → SSE string and adds [DONE] on flush.
     */
    let redisController: ReadableStreamDefaultController<UIMessageChunk>;
    const redisStream = new ReadableStream<UIMessageChunk>({
      start(controller) {
        redisController = controller;
      },
    }).pipeThrough(new JsonToSseTransformStream());

    /**
     * Get reader synchronously to fail fast if stream is locked
     */
    const reader = stream.getReader();

    /**
     * Register Redis stream with resumable-stream for persistence.
     * Release reader lock if registration fails to avoid locking the source stream.
     */
    try {
      await context.createNewResumableStream(streamId, () => redisStream);
    } catch (error) {
      reader.releaseLock();
      throw error;
    }

    /**
     * Single drain loop.
     * Continues draining to Redis even if client disconnects
     */
    (async () => {
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            redisController!.close();
            if (!clientCancelled) {
              clientController!.close();
            }
            break;
          }
          /**
           * Always enqueue to Redis for persistence
           */
          redisController!.enqueue(value);

          /**
           * Only enqueue to client if still connected to avoid unbounded memory growth.
           *
           */
          if (!clientCancelled) {
            // TODO: Consider treating sustained backpressure as implicit disconnect
            // to handle clients that stay connected but stop reading:
            // const desiredSize = clientController!.desiredSize;
            // if (desiredSize !== null && desiredSize <= 0) {
            //   clientCancelled = true;
            //   try { clientController!.close(); } catch {}
            // }
            clientController!.enqueue(value);
          }
        }
      } catch (error) {
        redisController!.error(error);
        if (!clientCancelled) {
          clientController!.error(error);
        }
      } finally {
        reader.releaseLock();
        try {
          await unsubscribe();
        } catch {
          /** Ignore errors from unsubscribe during cleanup */
        }
      }
    })();

    return createAsyncIterableStream(clientStream);
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
