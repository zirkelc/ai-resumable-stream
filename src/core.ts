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
   * Identifies this generation of the stream id, so it can be resumed and stopped on its
   * own, even after a newer generation of the same stream id has started. Defaults to a
   * generated id, returned as `generationId`.
   *
   * Names exactly one generation, so an id is never used for a second generation of the
   * same stream id: a resume or a stop kept for it reaches the generation that holds it.
   */
  generationId?: string;
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
   * How long a stop waits for the source to end on its own before it cancels the source.
   * Defaults to `1_000` when an `abortController` is supplied, and to `0` otherwise, since
   * a controller created here has no other listener that could end the source.
   *
   * A producer that observes the signal ends its stream itself, and the chunks it emits
   * while doing so still reach the client and the adapter. For `streamText` that is the
   * `abort` chunk, which is what makes `isAborted` true in the UI message stream's
   * `onEnd`/`onFinish`: cancelling first ends that stream before the chunk arrives. A
   * source that ignores the signal, or hangs, is cancelled once the time runs out, so it
   * may produce chunks for that long after the stop. `0` cancels the source immediately.
   *
   * Must be longer than the work a producer does before it ends its stream, for `streamText`
   * the `onAbort` callbacks it awaits before it emits the `abort` chunk. Values longer than a
   * timer can hold, such as `Infinity`, never cancel the source.
   */
  stopTimeoutMs?: number;
  /**
   * Called when listening for stop requests fails. The stream is unaffected, but it
   * cannot be stopped. Errors thrown by the callback are ignored.
   */
  onStopSubscriptionError?: (error: unknown) => void;
  /**
   * Called once the source has ended and the adapter has been told the stream is
   * complete. Runs on every exit path, including errors and stops. Errors are ignored.
   */
  onFinish?: () => void | Promise<void>;
};

export type ResumeStreamOptions = {
  /**
   * The id the stream was started under.
   */
  streamId: string;
  /**
   * The generation to resume. Defaults to the one the stream id currently points at.
   */
  generationId?: string;
};

export type StopStreamOptions = {
  /**
   * The id of the stream to stop.
   */
  streamId: string;
  /**
   * The generation to stop. Defaults to the one the stream id currently points at.
   */
  generationId?: string;
};

export type StartStreamResult<CHUNK> = {
  /**
   * The id the stream was registered under, whether supplied or generated.
   */
  streamId: string;
  /**
   * The id of this generation, whether supplied or generated. Pass it to `resumeStream` or
   * `stopStream` to address this generation and no other.
   */
  generationId: string;
  /**
   * The chunks, for the client that started it. Cancelling it does not stop
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
   * Generates a stream id or a generation id when `startStream` is not given one.
   * Defaults to `crypto.randomUUID`.
   *
   * Must return an id that has not been used before: two generations of a stream id can
   * never share a generation id.
   */
  generateId?: () => string;
};

/**
 * The longest delay a timer accepts. A longer one fires almost immediately instead.
 */
const MAX_TIMEOUT_MS = 2_147_483_647;

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
      generationId = generateId(),
      abortController: suppliedController,
      stopTimeoutMs = suppliedController ? 1_000 : 0,
      onFinish,
      onStopSubscriptionError,
    } = startOptions;
    const abortController = suppliedController ?? new AbortController();

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

    /**
     * Cancelling the source resolves any pending read as done, which ends the drain
     * loop, and propagates upstream so the producer stops doing work. Deferred by
     * `stopTimeoutMs`, so a producer that observes the signal can end its stream first and
     * the chunks that report the stop are still drained. Registered before anything can
     * request a stop, so an early stop is not missed.
     */
    let cancelTimer: ReturnType<typeof setTimeout> | undefined;
    const cancelSource = () => {
      reader.cancel().catch(() => {
        /** The source is already gone */
      });
    };
    const onAbort = () => {
      if (stopTimeoutMs <= 0) {
        cancelSource();
        return;
      }
      if (stopTimeoutMs > MAX_TIMEOUT_MS) return;
      cancelTimer = setTimeout(cancelSource, stopTimeoutMs);
    };
    abortController.signal.addEventListener(`abort`, onAbort, { once: true });

    /**
     * Set once the source has ended, after which a stop has nothing left to abort and a
     * subscription has nothing left to guard.
     */
    let finished = false;
    let unsubscribe: (() => unknown) | undefined;

    /**
     * Stops listening, now or as soon as the listener is registered.
     */
    async function finish() {
      finished = true;
      clearTimeout(cancelTimer);
      abortController.signal.removeEventListener(`abort`, onAbort);
      const remove = unsubscribe;
      unsubscribe = undefined;
      if (remove) await ignoreErrors(remove);
    }

    try {
      await adapter.createStream({ streamId, generationId, chunks: adapterStream, waitUntil });
    } catch (error) {
      reader.releaseLock();
      await finish();
      throw error;
    }

    /**
     * Listening for stops is best effort and never awaited, so a slow or failing store
     * neither delays the client nor fails the stream. A stop requested in the meantime is
     * not lost where the adapter keeps it until the listener asks. Started only once the
     * generation is registered, so a start that is refused never touches the stop listeners
     * of the generation it collided with.
     */
    void (async () => {
      try {
        const remove = await adapter.onStopRequested({
          streamId,
          generationId,
          onStop: () => {
            if (!finished) abortController.abort();
          },
        });

        /** The source ended first, so the listener must not outlive it. */
        if (finished) await ignoreErrors(remove);
        else unsubscribe = remove;
      } catch (error) {
        await ignoreErrors(() => onStopSubscriptionError?.(error));
      }
    })();

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
        await finish();
        await ignoreErrors(() => adapterController.close());
        await ignoreErrors(() => onFinish?.());
      }
    })();

    waitUntil?.(drained);

    return { streamId, generationId, stream: createAsyncIterableStream(clientStream) };
  }

  /**
   * Returns the chunks of an in-flight stream, starting from the first one it produced,
   * or `null` when there is nothing to resume: the generation named by `generationId`, or
   * the current one.
   */
  async function resumeStream(
    options: ResumeStreamOptions,
  ): Promise<AsyncIterableStream<CHUNK> | null> {
    const { streamId, generationId } = options;
    const encoded = await adapter.resumeStream(
      generationId === undefined ? { streamId } : { streamId, generationId },
    );
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
   * Asks the process producing a generation of the stream to stop: the one named by
   * `generationId`, or the current one. Resolves once the request is recorded, which may
   * be before the producer has observed it.
   */
  async function stopStream(options: StopStreamOptions): Promise<void> {
    const { streamId, generationId } = options;
    await adapter.requestStop(
      generationId === undefined ? { streamId } : { streamId, generationId },
    );
  }

  return { startStream, resumeStream, stopStream };
}
