import { type AsyncIterableStream, createAsyncIterableStream } from "ai-stream-utils";
import type { StreamAdapter, StreamCodec } from "./adapter.js";

export type StartStreamOptions = {
  /**
   * A stable identifier for the stream, used to resume and to stop it.
   * Defaults to a generated id.
   *
   * Passing an id the caller already owns (a chat id, a message id) removes the need
   * to track a separate pointer. Reusing an id discards the previous stream's chunks.
   */
  streamId?: string;
  /**
   * The controller aborted when the stream is stopped. Created if not supplied, so a
   * stream is always stoppable.
   *
   * Supply one when the producer needs the signal, for example to pass it to
   * `streamText({ abortSignal })`, so the provider request is aborted directly instead
   * of relying on stream cancellation propagating upstream.
   */
  abortController?: AbortController;
  /**
   * Called once the source has ended and the adapter has been told the stream is
   * complete. Runs on every exit path, including errors and stops. Errors are ignored.
   */
  onFinish?: () => void | Promise<void>;
};

export type StartStreamResult<CHUNK> = {
  /**
   * The id the stream was registered under, whether supplied or generated.
   */
  streamId: string;
  /**
   * The chunks, for the client that started the stream. Cancelling it does not stop
   * persistence, so a disconnected client can still resume.
   */
  stream: AsyncIterableStream<CHUNK>;
};

export type CreateResumableStreamOptions<CHUNK> = {
  adapter: StreamAdapter;
  codec: StreamCodec<CHUNK>;
  /**
   * Keeps the host process alive until persistence finishes. Omit on long-lived servers.
   */
  waitUntil?: (promise: Promise<unknown>) => void;
  /**
   * Generates a stream id when `startStream` is not given one.
   */
  generateId?: () => string;
};

/**
 * Ignores teardown failures so they never mask the original outcome.
 */
async function ignoreErrors(fn: () => unknown): Promise<void> {
  try {
    await fn();
  } catch {
    /** Nothing to do */
  }
}

/**
 * Creates a resumable stream context bound to a storage adapter and a chunk codec.
 *
 * The returned stream is drained once and fanned out to two consumers: the client
 * that started it, and the adapter. The two are independent, so a client that
 * disconnects mid-stream neither stops nor corrupts persistence.
 */
export function createResumableStream<CHUNK>(options: CreateResumableStreamOptions<CHUNK>) {
  const { adapter, codec, waitUntil, generateId = () => crypto.randomUUID() } = options;

  async function startStream(
    source: ReadableStream<CHUNK>,
    startOptions: StartStreamOptions = {},
  ): Promise<StartStreamResult<CHUNK>> {
    const {
      streamId = generateId(),
      abortController = new AbortController(),
      onFinish,
    } = startOptions;

    const unsubscribe = await adapter.onStopRequested(streamId, () => abortController.abort());

    /**
     * Chunks for the client, exactly as produced. Cancelling stops the client fan-out
     * but leaves the drain loop running so the adapter still receives every chunk.
     */
    let clientCancelled = false;
    let clientController!: ReadableStreamDefaultController<CHUNK>;
    const clientStream = new ReadableStream<CHUNK>({
      start(controller) {
        clientController = controller;
      },
      cancel() {
        clientCancelled = true;
      },
    });

    /**
     * Serialized chunks for the adapter.
     */
    let adapterController!: ReadableStreamDefaultController<string>;
    const adapterStream = new ReadableStream<string>({
      start(controller) {
        adapterController = controller;
      },
    });

    /**
     * Acquired before registration so a locked source fails before any state is written.
     */
    const reader = source.getReader();

    try {
      await adapter.createStream(streamId, adapterStream, { waitUntil });
    } catch (error) {
      reader.releaseLock();
      await ignoreErrors(unsubscribe);
      throw error;
    }

    /**
     * Cancelling the source resolves any pending read as done, which ends the drain
     * loop, and propagates upstream so the producer stops doing work.
     */
    abortController.signal.addEventListener(
      `abort`,
      () => {
        reader.cancel().catch(() => {
          /** The source is already gone */
        });
      },
      { once: true },
    );

    const drained = (async () => {
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            if (!clientCancelled) clientController.close();
            break;
          }

          adapterController.enqueue(codec.encode(value));
          if (!clientCancelled) clientController.enqueue(value);
        }
      } catch (error) {
        adapterController.error(error);
        if (!clientCancelled) clientController.error(error);
      } finally {
        reader.releaseLock();
        await ignoreErrors(unsubscribe);
        await ignoreErrors(() => adapterController.close());
        await ignoreErrors(() => onFinish?.());
      }
    })();

    waitUntil?.(drained);

    return { streamId, stream: createAsyncIterableStream(clientStream) };
  }

  /**
   * Returns the chunks of an in-flight stream, starting from the first one it produced,
   * or `null` when there is nothing to resume.
   */
  async function resumeStream(streamId: string): Promise<AsyncIterableStream<CHUNK> | null> {
    const encoded = await adapter.resumeStream(streamId);
    if (!encoded) return null;

    const chunks = encoded.pipeThrough(
      new TransformStream<string, CHUNK>({
        async transform(data, controller) {
          const chunk = await codec.decode(data);
          if (chunk !== undefined) controller.enqueue(chunk);
        },
      }),
    );

    return createAsyncIterableStream(chunks);
  }

  /**
   * Asks the process producing the stream to stop. Resolves once the request is
   * recorded, which may be before the producer has observed it.
   */
  async function stopStream(streamId: string): Promise<void> {
    await adapter.requestStop(streamId);
  }

  return { startStream, resumeStream, stopStream };
}
