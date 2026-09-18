import { createResumableStreamContext, type Publisher, type Subscriber } from "resumable-stream";
import type { StreamAdapter } from "../../adapter.js";
import { chunksToSSE, sseToChunks } from "./sse.js";

/**
 * A Redis client, described by the commands this adapter actually calls rather than by
 * the client type of a specific `redis` release. The generic parameters of
 * `RedisClientType` are not mutually assignable across redis v5 and v6, so a nominal
 * type would pin consumers to one of them.
 *
 * `get` and `set` are redeclared because the pointer is a string with an expiry, which is
 * narrower than what `resumable-stream` describes for its own use. `unsubscribe` is
 * redeclared because a listener removes itself alone, never every listener of its channel.
 */
type Redis = Omit<Publisher, `get` | `set`> &
  Omit<Subscriber, `unsubscribe`> & {
    isOpen: boolean;
    get(key: string): Promise<string | null>;
    set(
      key: string,
      value: string,
      options?: { expiration?: { type: `EX`; value: number } },
    ): Promise<unknown>;
    unsubscribe(channel: string, listener?: (message: string) => void): Promise<unknown>;
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
};

const DEFAULT_KEY_PREFIX = `ai-resumable-stream`;

/**
 * Matches the lifetime `resumable-stream` gives its own keys. The pointer is deleted as
 * soon as the source ends, so this only bounds the leak from a producer that died before
 * it could clean up. There is nothing to gain from expiring sooner than the stream state
 * it points at.
 */
const GENERATION_TTL_SECONDS = 24 * 60 * 60;

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
 * Raised by `resumable-stream` when a producer stops answering resume requests.
 */
const ACK_TIMEOUT_MESSAGE = `Timeout waiting for ack`;

function isAckTimeout(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return message.includes(ACK_TIMEOUT_MESSAGE);
}

/**
 * Stores streams in Redis via `resumable-stream`.
 *
 * Chunks live in the memory of the producing process and reach late subscribers over
 * Redis pub/sub, so a stream is only resumable while its producer is alive. Use a
 * durable adapter when a stream must outlive the process that started it.
 */
export function createRedisAdapter(options: CreateRedisAdapterOptions): StreamAdapter {
  const { publisher, subscriber, keyPrefix = DEFAULT_KEY_PREFIX } = options;

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
    await Promise.all([
      publisher.isOpen ? Promise.resolve() : publisher.connect(),
      subscriber.isOpen ? Promise.resolve() : subscriber.connect(),
    ]);
  }

  function createContext(waitUntil?: (promise: Promise<unknown>) => void) {
    return createResumableStreamContext({
      waitUntil: waitUntil ?? null,
      publisher,
      subscriber,
      keyPrefix,
    });
  }

  async function readPointer(streamId: string): Promise<string | null> {
    return publisher.get(pointerKey(streamId));
  }

  /**
   * Drops the stop kept for a generation once it is over, so nothing outlives the
   * generation it was meant for.
   */
  async function clearStop(streamId: string, generationId: string) {
    await publisher.del(stopKey(streamId, generationId));
  }

  /**
   * Retires the generation, but only while it is still the current one, so a slow
   * teardown never retires its successor. The comparison and the deletion are atomic: a
   * successor may repoint the id at any moment, including between the two.
   */
  async function clearPointer(streamId: string, generationId: string) {
    await publisher.eval(DELETE_IF_EQUAL_SCRIPT, {
      keys: [pointerKey(streamId)],
      arguments: [generationId],
    });
  }

  return {
    async createStream({ streamId, generationId: runId, chunks, waitUntil }) {
      await connect();

      /**
       * The pointer holds the full name, which is also the id `resumable-stream` knows
       * the generation by, so two stream ids with the same generation id never share
       * state.
       */
      const generationId = generationName(streamId, runId);

      await publisher.set(pointerKey(streamId), generationId, {
        expiration: { type: `EX`, value: GENERATION_TTL_SECONDS },
      });

      /**
       * The pointer is dropped as soon as the source ends, so a resume that arrives
       * during teardown is told there is nothing to resume rather than racing it. The
       * stop goes with it: a generation that is over has nothing left to stop.
       */
      const sse = chunks.pipeThrough(chunksToSSE()).pipeThrough(
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

      await createContext(waitUntil).createNewResumableStream(generationId, () => sse);
    },

    async resumeStream({ streamId, generationId: runId }) {
      await connect();

      /**
       * A named generation is followed without the pointer, so it can be resumed after a
       * newer generation took the stream id. Whether its producer is still alive is then only
       * known from `resumable-stream` itself.
       */
      const generationId =
        runId === undefined ? await readPointer(streamId) : generationName(streamId, runId);
      if (!generationId) return null;

      const sse = await createContext().resumeExistingStream(generationId);
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
      try {
        first = await reader.read();
      } catch (error) {
        reader.releaseLock();
        if (isAckTimeout(error)) return null;
        throw error;
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
      await publisher.set(stopKey(streamId, runId), `1`, {
        expiration: { type: `EX`, value: STOP_TTL_SECONDS },
      });
      await publisher.publish(stopChannel(streamId, runId), `stop`);
    },

    async onStopRequested({ streamId, generationId, onStop }) {
      await connect();

      const channel = stopChannel(streamId, generationId);
      const listener = () => onStop();
      await subscriber.subscribe(channel, listener);

      const unsubscribe = async () => {
        await subscriber.unsubscribe(channel, listener);
      };

      /**
       * Read only once subscribed. Reading first, or both at once, leaves a window in
       * which a stop is neither kept yet nor delivered.
       */
      try {
        if ((await publisher.get(stopKey(streamId, generationId))) !== null) onStop();
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
