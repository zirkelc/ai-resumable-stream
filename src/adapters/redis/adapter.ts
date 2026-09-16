import { createResumableStreamContext, type Publisher, type Subscriber } from "resumable-stream";
import type { StreamAdapter } from "../../adapter.js";
import { chunksToSSE, sseToChunks } from "./sse.js";

/**
 * A Redis client, described by the commands this adapter actually calls rather than by
 * the client type of a specific `redis` release. The generic parameters of
 * `RedisClientType` are not mutually assignable across redis v5 and v6, so a nominal
 * type would pin consumers to one of them.
 *
 * `get` and `set` are redeclared because the pointer is a string with an expiry, which
 * is narrower than what `resumable-stream` describes for its own use.
 */
type Redis = Omit<Publisher, `get` | `set`> &
  Subscriber & {
    isOpen: boolean;
    get(key: string): Promise<string | null>;
    set(
      key: string,
      value: string,
      options?: { expiration?: { type: `EX`; value: number } },
    ): Promise<unknown>;
    del(key: string): Promise<unknown>;
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

  const stopChannel = (streamId: string) => `${keyPrefix}:stop:${streamId}`;

  /**
   * Points a caller's stream id at its current generation.
   *
   * A generation is one run of a stream id, and each gets a fresh id because a
   * producer's teardown is asynchronous: it marks its stream done and drops its
   * subscriptions well after the source has ended. Were a reused id to address the same
   * underlying stream, a late teardown would tear down the generation that replaced it.
   */
  const generationKey = (streamId: string) => `${keyPrefix}:generation:${streamId}`;

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

  async function readGenerationId(streamId: string): Promise<string | null> {
    return publisher.get(generationKey(streamId));
  }

  /**
   * Retires the generation, but only while it is still the current one, so a slow
   * teardown never retires its successor.
   */
  async function clearGenerationId(streamId: string, generationId: string) {
    if ((await readGenerationId(streamId)) === generationId) {
      await publisher.del(generationKey(streamId));
    }
  }

  return {
    async createStream({ streamId, chunks, waitUntil }) {
      await connect();

      const generationId = `${streamId}:${crypto.randomUUID()}`;

      await publisher.set(generationKey(streamId), generationId, {
        expiration: { type: `EX`, value: GENERATION_TTL_SECONDS },
      });

      /**
       * The pointer is dropped as soon as the source ends, so a resume that arrives
       * during teardown is told there is nothing to resume rather than racing it.
       */
      const sse = chunks.pipeThrough(chunksToSSE()).pipeThrough(
        new TransformStream<string, string>({
          async flush() {
            await clearGenerationId(streamId, generationId).catch(() => {
              /** Expires on its own */
            });
          },
        }),
      );

      await createContext(waitUntil).createNewResumableStream(generationId, () => sse);
    },

    async resumeStream({ streamId }) {
      await connect();

      const generationId = await readGenerationId(streamId);
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

    async requestStop({ streamId }) {
      await connect();
      await publisher.publish(stopChannel(streamId), `stop`);
    },

    async onStopRequested({ streamId, onStop }) {
      await connect();

      const channel = stopChannel(streamId);
      await subscriber.subscribe(channel, () => onStop());

      return async () => {
        await subscriber.unsubscribe(channel);
      };
    },
  };
}
