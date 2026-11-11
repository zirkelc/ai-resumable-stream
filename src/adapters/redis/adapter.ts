import { AckTimeoutError, createRuntime } from "./runtime/runtime.js";
import type { StreamAdapter } from "../../adapter.js";
import { chunksToSSE, sseToChunks } from "./sse.js";

/**
 * A Redis client, described by the commands this adapter actually calls rather than by
 * the client type of a specific `redis` release. The generic parameters of
 * `RedisClientType` are not mutually assignable across redis v5 and v6, so a nominal
 * type would pin consumers to one of them.
 *
 * `unsubscribe` takes the listener because a listener removes itself alone, never every
 * listener of its channel.
 */
type Redis = {
  isOpen: boolean;
  connect(): Promise<unknown>;
  publish(channel: string, message: string): Promise<unknown>;
  subscribe(channel: string, listener: (message: string) => void): Promise<unknown>;
  unsubscribe(channel: string, listener?: (message: string) => void): Promise<unknown>;
  get(key: string): Promise<string | null>;
  set(
    key: string,
    value: string,
    options?: { expiration?: { type: `EX` | `PX`; value: number } },
  ): Promise<unknown>;
  del(key: string): Promise<unknown>;
  eval(
    script: string,
    options?: { keys?: Array<string>; arguments?: Array<string> },
  ): Promise<unknown>;
};

export type CreateRedisAdapterOptions = {
  /**
   * A client from the `redis` package, used to publish chunks and stop requests.
   * Connected on first use and never disconnected, so it stays reusable across streams.
   */
  publisher: Redis;
  /**
   * A second client, required because a subscribed connection cannot issue commands.
   */
  subscriber: Redis;
  /**
   * Namespace for every key and channel this adapter touches.
   */
  keyPrefix?: string;
  /**
   * How long a resume waits for the producer to answer before it gives up and returns
   * `null`. A producer that died never answers, so every resume of its stream waits this
   * long. A producer that is alive but busy can need more than the default. Defaults to
   * 1000 ms.
   */
  ackTimeoutMs?: number;
  /**
   * When the Redis keys of a stream expire, in milliseconds: the pointer of the stream id
   * and the mark of a finished stream. Must be longer than your longest stream, because a
   * resume without a generation id finds nothing after the pointer expired, even while the
   * producer still runs. The stream of a producer that died is gone sooner, because its
   * producer no longer renews it. Defaults to 24 hours.
   */
  expireAfterMs?: number;
  /**
   * The same as `expireAfterMs`, in seconds. Ignored when `expireAfterMs` is set.
   *
   * @deprecated Use `expireAfterMs`.
   */
  ttlSeconds?: number;
  /**
   * How long the adapter waits for Redis when it connects, registers a stream, reads the
   * pointer, or sends and listens for a stop. After this time, the call rejects with a
   * `CommandTimeoutError`. Use it when Redis can become unreachable: the client does not
   * get an answer from a dead peer, and the timeout and the abort signal of the client no
   * longer apply once a command is written. The command can still be pending in the client.
   * Defaults to no limit.
   */
  commandTimeoutMs?: number;
  /**
   * How often a producer renews the mark that its stream is running. The mark expires
   * after three intervals without a renewal, so the stream of a producer that died is
   * gone after three intervals. Each renewal is one Redis command per running stream.
   * Defaults to 30 s, so a dead producer is detected after 90 s.
   */
  heartbeatMs?: number;
};

/**
 * Raised when Redis does not answer a call of the adapter within `commandTimeoutMs`.
 *
 * Its name is `TimeoutError`, the name of the error that `AbortSignal.timeout()` gives, so
 * a check of `error.name === "TimeoutError"` finds both. A plain `Error` and not a
 * `DOMException`, because a `DOMException` takes its name only from its constructor, and a
 * subclass of it is unusual.
 */
export class CommandTimeoutError extends Error {
  constructor(timeoutMs: number) {
    super(`Redis did not answer within ${timeoutMs} ms`);
    this.name = `TimeoutError`;
  }
}

const DEFAULT_KEY_PREFIX = `ai-resumable-stream`;

/**
 * The fixed lifetime of the original `resumable-stream`, long enough for any chat turn.
 */
const DEFAULT_EXPIRE_AFTER_MS = 24 * 60 * 60 * 1_000;

/**
 * Deletes a key only while it still holds the expected value. One script, so no other
 * client can write the key between the comparison and the deletion.
 */
const DELETE_IF_EQUAL_SCRIPT = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) end return 0`;

/**
 * How long a stop request is kept for a generation that has not observed it yet. A
 * generation drops its own stop key when it ends, so this only bounds a stop that was
 * requested for a generation which never ran, or which died before it could clean up.
 */
const STOP_TTL_SECONDS = 60 * 60;

/**
 * Stores streams in Redis, with a runtime derived from `resumable-stream`.
 *
 * Chunks live in the memory of the producing process and reach late subscribers over
 * Redis pub/sub, so a stream is only resumable while its producer is alive. Use a
 * durable adapter when a stream must outlive the process that started it.
 */
export function createRedisAdapter(options: CreateRedisAdapterOptions): StreamAdapter {
  const {
    publisher,
    subscriber,
    keyPrefix = DEFAULT_KEY_PREFIX,
    ackTimeoutMs,
    ttlSeconds,
    commandTimeoutMs,
    heartbeatMs,
  } = options;
  if (heartbeatMs !== undefined && !(heartbeatMs > 0)) {
    throw new RangeError(`heartbeatMs must be greater than 0, got ${heartbeatMs}`);
  }
  const expireAfterMs =
    options.expireAfterMs ??
    (ttlSeconds === undefined ? DEFAULT_EXPIRE_AFTER_MS : ttlSeconds * 1_000);

  /**
   * Rejects with a `CommandTimeoutError` when the call takes longer than
   * `commandTimeoutMs`. A late failure of the call is handled, so it is not an unhandled
   * rejection.
   */
  function limit<VALUE>(call: Promise<VALUE>): Promise<VALUE> {
    if (commandTimeoutMs === undefined) return call;
    const timeoutMs = commandTimeoutMs;
    return new Promise<VALUE>((resolve, reject) => {
      const timer = setTimeout(() => reject(new CommandTimeoutError(timeoutMs)), timeoutMs);
      call.then(
        (value) => {
          clearTimeout(timer);
          resolve(value);
        },
        (error: unknown) => {
          clearTimeout(timer);
          reject(error);
        },
      );
    });
  }

  /**
   * The name of one generation of a stream id. The generation id is encoded, so it holds
   * no colon and the name splits one way only: stream `a` with generation `b:c` never
   * shares a name with stream `a:b` with generation `c`. A generated id is left unchanged.
   */
  const generationName = (streamId: string, generationId: string) =>
    `${streamId}:${encodeURIComponent(generationId)}`;

  /**
   * One channel per generation, so a stop reaches that generation alone, and removing the
   * listener of one never removes the listener of another on the shared subscriber.
   */
  const stopChannel = (streamId: string, generationId: string) =>
    `${keyPrefix}:stop:${generationName(streamId, generationId)}`;

  /**
   * A stop kept for a generation, because pub/sub has no retention and a stop published
   * before its producer subscribed would otherwise be lost. Keyed by generation, so it
   * can never reach a different generation of the same stream id.
   */
  const stopKey = (streamId: string, generationId: string) =>
    `${keyPrefix}:stop:${generationName(streamId, generationId)}`;

  /**
   * Holds the generation a caller's stream id currently points at.
   *
   * A generation is one run of a stream id, and each gets a fresh id because a
   * producer's teardown is asynchronous: it marks its stream done and drops its
   * subscriptions well after the source has ended. Were a reused id to address the same
   * underlying stream, a late teardown would tear down the generation that replaced it.
   */
  const pointerKey = (streamId: string) => `${keyPrefix}:generation:${streamId}`;

  async function connect() {
    await limit(
      Promise.all([
        publisher.isOpen ? Promise.resolve() : publisher.connect(),
        subscriber.isOpen ? Promise.resolve() : subscriber.connect(),
      ]),
    );
  }

  function createContext(waitUntil?: (promise: Promise<unknown>) => void) {
    return createRuntime({
      waitUntil,
      publisher,
      subscriber,
      keyPrefix,
      ackTimeoutMs,
      expireAfterMs,
      heartbeatIntervalMs: heartbeatMs,
    });
  }

  async function readPointer(streamId: string): Promise<string | null> {
    return limit(publisher.get(pointerKey(streamId)));
  }

  /**
   * Drops the stop kept for a generation once it is over, so nothing outlives the
   * generation it was meant for.
   */
  async function clearStop(streamId: string, generationId: string) {
    await limit(publisher.del(stopKey(streamId, generationId)));
  }

  /**
   * Retires the generation, but only while it is still the current one, so a slow
   * teardown never retires its successor. The comparison and the deletion are atomic: a
   * successor may repoint the id at any moment, including between the two.
   */
  async function clearPointer(streamId: string, generationId: string) {
    await limit(
      publisher.eval(DELETE_IF_EQUAL_SCRIPT, {
        keys: [pointerKey(streamId)],
        arguments: [generationId],
      }),
    );
  }

  return {
    async createStream({ streamId, generationId: runId, chunks, waitUntil }) {
      await connect();

      /**
       * The pointer holds the full name, which is also the id the runtime knows
       * the generation by, so two stream ids with the same generation id never share
       * state.
       */
      const generationId = generationName(streamId, runId);

      /**
       * The pointer lives as long as the sentinel of the runtime. The adapter deletes it
       * when the source ends, so the expiry only removes the pointer of a producer that
       * died before it could clean up. A pointer that lives longer than its sentinel only
       * leads to a stream that the runtime does not know.
       */
      await limit(
        publisher.set(pointerKey(streamId), generationId, {
          expiration: { type: `PX`, value: expireAfterMs },
        }),
      );

      /**
       * The pointer is dropped as soon as the source ends, so a resume that arrives
       * during teardown is told there is nothing to resume rather than racing it. The
       * stop goes with it: a generation that is over has nothing left to stop.
       */
      const abandon = new AbortController();
      const sse = chunks.pipeThrough(chunksToSSE(), { signal: abandon.signal }).pipeThrough(
        new TransformStream<string, string>({
          async flush() {
            await Promise.all([
              clearPointer(streamId, generationId),
              clearStop(streamId, runId),
            ]).catch(() => {
              /** Both expire on their own */
            });
          },
        }),
      );

      /**
       * A registration that times out is abandoned. The runtime can still finish its setup
       * later, so the pipe is aborted: the runtime then sees a failed source and tears the
       * stream down, and the source is cancelled.
       */
      try {
        await limit(createContext(waitUntil).createNewResumableStream(generationId, () => sse));
      } catch (error) {
        abandon.abort(error);
        await clearPointer(streamId, generationId).catch(() => {
          /** It expires on its own */
        });
        throw error;
      }
    },

    async resumeStream({ streamId, generationId: runId, signal }) {
      await connect();

      /**
       * A named generation is followed without the pointer, so it can be resumed after a
       * newer generation took the stream id. Whether its producer is still alive is then only
       * known from the runtime itself.
       */
      const generationId =
        runId === undefined ? await readPointer(streamId) : generationName(streamId, runId);
      if (!generationId) return null;

      /**
       * A producer that died without marking its generation done never answers, and the
       * runtime rejects after waiting for it. There is nothing to resume.
       */
      let sse: ReadableStream<string> | null | undefined;
      try {
        sse = await createContext().resumeExistingStream(generationId);
      } catch (error) {
        if (error instanceof AckTimeoutError) return null;
        throw error;
      }
      if (!sse) return null;

      const reader = sse.pipeThrough(sseToChunks()).getReader();

      /**
       * The first chunk is read here rather than by the caller because a resume that
       * races the producer's teardown only fails once it is read. Reporting that as
       * "nothing to resume" makes it indistinguishable from a stream that already
       * finished, which is what it is. The cost is that resuming a producer which has
       * not yet emitted anything waits for its first chunk.
       */
      let first: Awaited<ReturnType<typeof reader.read>>;
      /**
       * A producer that never sends its first chunk would hold this read, and with it the
       * subscription, forever. The abort cancels the read, which removes the subscription.
       */
      const onAbort = () => void reader.cancel(signal?.reason).catch(() => undefined);
      signal?.addEventListener(`abort`, onAbort, { once: true });
      try {
        if (signal?.aborted) onAbort();
        first = await reader.read();
      } catch (error) {
        reader.releaseLock();
        // TODO(#26): also map the error for a producer that died to null
        if (error instanceof AckTimeoutError) return null;
        throw error;
      } finally {
        signal?.removeEventListener(`abort`, onAbort);
      }

      if (first.done) {
        reader.releaseLock();
        return null;
      }

      return new ReadableStream<string>({
        start(controller) {
          controller.enqueue(first.value!);
        },
        async pull(controller) {
          const { done, value } = await reader.read();
          if (done) {
            controller.close();
            return;
          }
          controller.enqueue(value);
        },
        cancel(reason) {
          return reader.cancel(reason);
        },
      });
    },

    async requestStop({ streamId, generationId }) {
      await connect();

      /**
       * Without a generation id, the stop is for the generation the pointer names at this
       * moment. With the pointer naming none, there is nothing to keep the request under.
       */
      let runId = generationId;
      if (runId === undefined) {
        const current = await readPointer(streamId);
        if (!current?.startsWith(`${streamId}:`)) return;
        runId = decodeURIComponent(current.slice(streamId.length + 1));
      }

      /**
       * Kept before it is published. A producer subscribes before it reads, so it either
       * receives the message or finds the key.
       */
      await limit(
        publisher.set(stopKey(streamId, runId), `1`, {
          expiration: { type: `EX`, value: STOP_TTL_SECONDS },
        }),
      );
      await limit(publisher.publish(stopChannel(streamId, runId), `stop`));
    },

    async onStopRequested({ streamId, generationId, onStop }) {
      await connect();

      const channel = stopChannel(streamId, generationId);
      const listener = () => onStop();
      await limit(subscriber.subscribe(channel, listener));

      const unsubscribe = async () => {
        await limit(subscriber.unsubscribe(channel, listener));
      };

      /**
       * Read only once subscribed. Reading first, or both at once, leaves a window in
       * which a stop is neither kept yet nor delivered.
       */
      try {
        if ((await limit(publisher.get(stopKey(streamId, generationId)))) !== null) onStop();
      } catch (error) {
        await unsubscribe().catch(() => {
          /** The subscription is gone with the connection */
        });
        throw error;
      }

      return unsubscribe;
    },
  };
}
