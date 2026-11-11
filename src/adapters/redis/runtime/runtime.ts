/*! @license
 * This file is derived from resumable-stream 2.2.13 (src/runtime.ts),
 * https://github.com/vercel/resumable-stream
 *
 * Copyright (c) 2025 Vercel, Inc
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies
 * or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 * CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
 * THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

/**
 * The producer and the consumer side of a resumable stream over Redis pub/sub.
 *
 * The producer keeps every chunk in memory. A consumer asks for the chunks on the request
 * channel of the stream, and the producer answers on a chunk channel of that consumer:
 * first everything so far, then each new chunk, then a DONE message.
 *
 * The keys, channels and messages are the same as in upstream, so processes that run this
 * code and processes that run upstream can resume each other's streams. Each change to
 * upstream is marked with "Differs from upstream" and the issue it fixes. See
 * docs/redis-runtime.md for the list.
 *
 * Only the parts that the Redis adapter uses are here. Left out from upstream: the
 * combined create-or-resume call with its INCR listener count, the existence check, the
 * default clients from environment variables, the ioredis wrappers and the debug logs.
 */

/**
 * The commands the runtime sends on the publisher connection.
 */
export type Publisher = {
  publish(channel: string, message: string): Promise<unknown>;
  set(
    key: string,
    value: string,
    options?: { expiration?: { type: `EX` | `PX`; value: number } },
  ): Promise<unknown>;
  get(key: string): Promise<string | null>;
};

/**
 * The commands the runtime sends on the subscriber connection.
 */
export type Subscriber = {
  subscribe(channel: string, listener: (message: string) => void): Promise<unknown>;
  unsubscribe(channel: string, listener?: (message: string) => void): Promise<unknown>;
};

export type CreateRuntimeOptions = {
  publisher: Publisher;
  subscriber: Subscriber;
  /**
   * Prefix of every key and channel. The runtime appends `:rs`, as upstream does.
   */
  keyPrefix: string;
  /**
   * Receives a promise that settles when the producer has finished its teardown.
   */
  waitUntil?: ((promise: Promise<unknown>) => void) | null;
  /**
   * How long a consumer waits for the first answer of the producer. Defaults to 1000 ms,
   * the fixed wait of upstream.
   *
   * Differs from upstream: upstream waits a fixed 1000 ms
   * (https://github.com/vercel/resumable-stream/issues/42).
   */
  ackTimeoutMs?: number;
  /**
   * How long the sentinel of a finished stream lives, in milliseconds. The sentinel of a
   * running stream lives as long as the heartbeat renews it.
   *
   * Differs from upstream: upstream keeps the sentinel a fixed 24 hours
   * (https://github.com/vercel/resumable-stream/issues/46,
   * https://github.com/vercel/resumable-stream/pull/18).
   */
  expireAfterMs: number;
  /**
   * How often a consumer checks the sentinel, in case the DONE message is lost.
   */
  doneWatchdogIntervalMs?: number;
  /**
   * How often a producer renews the sentinel of its running stream. The sentinel expires
   * after three intervals without a renewal, so the stream of a producer that died is gone
   * soon, and not only after 24 hours.
   *
   * Differs from upstream: upstream sets the running sentinel once, for 24 hours.
   */
  heartbeatIntervalMs?: number;
};

export type Runtime = {
  /**
   * Starts to read the source and makes it resumable under `streamId`.
   */
  createNewResumableStream(
    streamId: string,
    makeStream: () => ReadableStream<string>,
  ): Promise<ReadableStream<string>>;
  /**
   * Resumes the stream `streamId`. Gives `undefined` if no stream has this id, and `null`
   * if the stream is done.
   */
  resumeExistingStream(streamId: string): Promise<ReadableStream<string> | null | undefined>;
};

/**
 * Raised when the producer does not answer a resume in time, for example because it died
 * before it could mark its stream done.
 *
 * Differs from upstream: upstream raises a plain `Error`, which a caller can only identify
 * by its message.
 */
export class AckTimeoutError extends Error {
  constructor() {
    /** The upstream message, kept for callers that still match on it */
    super(`Timeout waiting for ack`);
    this.name = `AckTimeoutError`;
  }
}

type ResumeStreamMessage = {
  listenerId: string;
  skipCharacters?: number;
};

const DONE_MESSAGE = `\n\n\nDONE_SENTINEL_hasdfasudfyge374%$%^$EDSATRTYFtydryrte\n`;

const DONE_VALUE = `DONE`;

const DEFAULT_ACK_TIMEOUT_MS = 1_000;

const DEFAULT_DONE_WATCHDOG_INTERVAL_MS = 10_000;

/**
 * Long enough to keep the commands few, one SET per running stream every 30 s, and short
 * enough that the stream of a producer that died is gone in about 1.5 minutes.
 */
const DEFAULT_HEARTBEAT_INTERVAL_MS = 30_000;

/**
 * The running sentinel lives this many heartbeat intervals, so two renewals in a row can
 * fail, for example during a reconnect, before it expires.
 */
const HEARTBEATS_PER_SENTINEL_TTL = 3;

function noop() {}

export function createRuntime(options: CreateRuntimeOptions): Runtime {
  const ctx: Context = {
    keyPrefix: `${options.keyPrefix}:rs`,
    publisher: options.publisher,
    subscriber: options.subscriber,
    waitUntil: options.waitUntil ?? noop,
    ackTimeoutMs: options.ackTimeoutMs ?? DEFAULT_ACK_TIMEOUT_MS,
    expireAfterMs: options.expireAfterMs,
    doneWatchdogIntervalMs: options.doneWatchdogIntervalMs ?? DEFAULT_DONE_WATCHDOG_INTERVAL_MS,
    heartbeatIntervalMs: options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS,
  };

  return {
    createNewResumableStream: (streamId, makeStream) =>
      createNewResumableStream(ctx, streamId, makeStream),
    resumeExistingStream: (streamId) => resumeExistingStream(ctx, streamId),
  };
}

type Context = {
  keyPrefix: string;
  publisher: Publisher;
  subscriber: Subscriber;
  waitUntil: (promise: Promise<unknown>) => void;
  ackTimeoutMs: number;
  expireAfterMs: number;
  doneWatchdogIntervalMs: number;
  heartbeatIntervalMs: number;
};

async function resumeExistingStream(
  ctx: Context,
  streamId: string,
): Promise<ReadableStream<string> | null | undefined> {
  const state = await ctx.publisher.get(`${ctx.keyPrefix}:sentinel:${streamId}`);
  if (!state) return undefined;
  if (state === DONE_VALUE) return null;
  return resumeStream(ctx, streamId);
}

async function createNewResumableStream(
  ctx: Context,
  streamId: string,
  makeStream: () => ReadableStream<string>,
): Promise<ReadableStream<string>> {
  const requestChannel = `${ctx.keyPrefix}:request:${streamId}`;
  const sentinelKey = `${ctx.keyPrefix}:sentinel:${streamId}`;
  const chunkChannel = (listenerId: string) => `${ctx.keyPrefix}:chunk:${listenerId}`;
  const runningExpiration = {
    expiration: {
      type: `PX` as const,
      value: ctx.heartbeatIntervalMs * HEARTBEATS_PER_SENTINEL_TTL,
    },
  };
  const doneExpiration = { expiration: { type: `PX` as const, value: ctx.expireAfterMs } };

  const chunks: Array<string> = [];
  const listenerIds: Array<string> = [];
  let isDone = false;

  let resolveDone!: () => void;
  ctx.waitUntil(
    new Promise<void>((resolve) => {
      resolveDone = resolve;
    }),
  );

  /**
   * Publishes without waiting for the reply, and contains a failure.
   *
   * Differs from upstream in two ways:
   * - Upstream awaits the publishes of each chunk before it reads the next one, so while a
   *   consumer is attached, the producer (and its own client) reads one chunk per Redis
   *   round trip (https://github.com/vercel/resumable-stream/issues/52). The client sends
   *   commands of one connection in the order they are called, so the consumer still
   *   gets the chunks in order.
   * - Upstream does not handle a failed publish, so a dropped connection is an unhandled
   *   rejection that ends a Node process
   *   (https://github.com/vercel/resumable-stream/issues/53). A consumer that misses a
   *   chunk this way gets incomplete data, but a dropped connection also ends its own
   *   subscription.
   *
   * Publishes still in flight are kept, so the teardown can wait for them.
   */
  const inFlight = new Set<Promise<void>>();
  const publish = (channel: string, message: string) => {
    const promise = ctx.publisher.publish(channel, message).then(noop, noop);
    inFlight.add(promise);
    void promise.then(() => inFlight.delete(promise));
  };

  /**
   * Answers a consumer with every chunk so far, and with DONE if the source has ended.
   *
   * Differs from upstream: upstream passes an async listener, whose promise the Redis
   * client ignores. A failed publish was an unhandled rejection, and a message that is no
   * JSON threw inside the client. No upstream issue covers this.
   */
  const onRequest = (message: string) => {
    let request: ResumeStreamMessage;
    try {
      request = JSON.parse(message) as ResumeStreamMessage;
    } catch {
      return;
    }
    if (typeof request?.listenerId !== `string`) return;

    listenerIds.push(request.listenerId);
    const channel = chunkChannel(request.listenerId);
    publish(channel, chunks.join(``).slice(request.skipCharacters ?? 0));
    if (isDone) publish(channel, DONE_MESSAGE);
  };

  /**
   * Subscribed before the sentinel is set, so a consumer that sees the sentinel always
   * finds the producer listening. From upstream 2.2.13
   * (https://github.com/vercel/resumable-stream/pull/41).
   *
   * Differs from upstream: if this setup fails, the promise given to `waitUntil` still
   * settles, and the subscription is removed again.
   */
  try {
    await ctx.subscriber.subscribe(requestChannel, onRequest);
    try {
      await ctx.publisher.set(sentinelKey, `1`, runningExpiration);
    } catch (error) {
      await ctx.subscriber.unsubscribe(requestChannel, onRequest).catch(noop);
      throw error;
    }
  } catch (error) {
    resolveDone();
    throw error;
  }

  /**
   * Renews the running sentinel while the producer is alive. A producer that died stops
   * the renewals, so its sentinel expires: a new resume then finds no stream at once, and
   * the watchdog of an attached consumer closes its stream. A failed renewal is retried at
   * the next interval. The renewal is a plain SET, so it also restores a sentinel that
   * expired while Redis was unreachable. The teardown stops the renewals before it sends
   * DONE, and one connection keeps the order of its commands, so a renewal never
   * overwrites DONE.
   *
   * Differs from upstream: upstream sets the running sentinel once, for 24 hours, so a
   * consumer waits for the ack timeout on every resume of a stream whose producer died.
   */
  const heartbeat = setInterval(() => {
    void ctx.publisher.set(sentinelKey, `1`, runningExpiration).catch(noop);
  }, ctx.heartbeatIntervalMs);
  /** A long-lived process can still exit while a stream runs */
  heartbeat.unref?.();

  /**
   * Marks the stream done, tells the consumers and stops to answer requests.
   *
   * Differs from upstream:
   * - Runs only once, and it cannot reject. Each command can fail alone, for example when
   *   Redis drops the connections, and the promise given to `waitUntil` still settles.
   *   Upstream awaits all commands together with no handler, so one failure is an
   *   unhandled rejection that ends a Node process, and the teardown never finishes
   *   (https://github.com/vercel/resumable-stream/issues/53,
   *   https://github.com/vercel/resumable-stream/pull/54).
   * - Removes only its own listener of the request channel. Upstream removes every
   *   listener of the channel on the shared subscriber.
   * - Waits for the chunk publishes still in flight, so a runtime that freezes the process
   *   after `waitUntil` does not lose them.
   *
   * If the DONE write fails, the sentinel stays "1" until it expires, which is three
   * heartbeat intervals after the teardown stopped the renewals. A consumer then gets
   * no answer and fails with the ack timeout.
   */
  let finishing: Promise<void> | undefined;
  const finish = () => {
    finishing ??= (async () => {
      isDone = true;
      clearInterval(heartbeat);
      for (const listenerId of listenerIds) publish(chunkChannel(listenerId), DONE_MESSAGE);
      await Promise.allSettled([
        ctx.publisher.set(sentinelKey, DONE_VALUE, doneExpiration),
        ctx.subscriber.unsubscribe(requestChannel, onRequest),
        ...inFlight,
      ]);
      resolveDone();
    })();
    return finishing;
  };

  return new ReadableStream<string>({
    start(controller) {
      /**
       * Reads the whole source, also when nobody reads this stream any more, so the
       * consumers still get every chunk.
       *
       * Differs from upstream: the loop has a rejection handler. A source that errors
       * errors this stream and still runs the teardown. Upstream gives an unhandled
       * rejection, which ends a Node process, and the stream stays resumable until the
       * sentinel expires (https://github.com/vercel/resumable-stream/issues/53).
       */
      const read = async () => {
        try {
          const reader = makeStream().getReader();
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            chunks.push(value);
            try {
              controller.enqueue(value);
            } catch {
              /** Nobody reads this stream any more, but the consumers still do */
            }
            for (const listenerId of listenerIds) publish(chunkChannel(listenerId), value);
          }
          try {
            controller.close();
          } catch {
            /** Already closed */
          }
        } catch (error) {
          try {
            controller.error(error);
          } catch {
            /** Already closed */
          }
        } finally {
          await finish();
        }
      };
      void read();
    },
  });
}

/**
 * Asks the producer of `streamId` for its chunks and gives them as a stream. Settles with
 * the stream on the first answer, with `null` if the producer has finished, and rejects
 * with an `AckTimeoutError` if no answer comes in time.
 */
function resumeStream(ctx: Context, streamId: string): Promise<ReadableStream<string> | null> {
  const listenerId = crypto.randomUUID();
  const chunkChannel = `${ctx.keyPrefix}:chunk:${listenerId}`;
  const sentinelKey = `${ctx.keyPrefix}:sentinel:${streamId}`;

  return new Promise<ReadableStream<string> | null>((resolve, reject) => {
    /**
     * Differs from upstream: the promise settles once, also when the timeout and an answer
     * race. Upstream can settle twice, and can also not settle at all
     * (https://github.com/vercel/resumable-stream/issues/50,
     * https://github.com/vercel/resumable-stream/pull/43).
     */
    let settled = false;
    const settle = (fn: () => void) => {
      if (settled) return;
      settled = true;
      fn();
    };

    let ackTimeout: ReturnType<typeof setTimeout> | undefined;
    let watchdogTimeout: ReturnType<typeof setTimeout> | undefined;
    let timedOut = false;
    let cleanupPromise: Promise<void> | undefined;
    let onMessage: ((message: string) => void) | undefined;

    /**
     * Differs from upstream: cannot reject. Upstream awaits it in the message listener and
     * in the timer, where a failed unsubscribe, for example on a dropped connection, is an
     * unhandled rejection. The subscription is gone with the connection in that case.
     */
    const cleanup = () => {
      cleanupPromise ??= (async () => {
        clearTimeout(ackTimeout);
        clearTimeout(watchdogTimeout);
        await ctx.subscriber.unsubscribe(chunkChannel, onMessage).catch(noop);
      })();
      return cleanupPromise;
    };

    const readableStream = new ReadableStream<string>({
      async start(controller) {
        const closeStream = () => {
          try {
            controller.close();
          } catch {
            /** The client may have cancelled the stream already */
          }
        };

        /**
         * The DONE message travels over pub/sub and can be lost, for example on a
         * reconnect. The sentinel is checked from time to time, and the stream closes
         * after two checks in a row see it done or gone, so that messages still in transit
         * get one interval to arrive. From upstream 2.2.13
         * (https://github.com/vercel/resumable-stream/pull/47).
         */
        let doneObservations = 0;
        let watchdogStarted = false;
        const scheduleDoneWatchdog = () => {
          if (cleanupPromise) return;
          watchdogTimeout = setTimeout(() => void checkDone(), ctx.doneWatchdogIntervalMs);
        };
        const startDoneWatchdog = () => {
          if (watchdogStarted) return;
          watchdogStarted = true;
          scheduleDoneWatchdog();
        };
        const checkDone = async () => {
          try {
            const state = await ctx.publisher.get(sentinelKey);
            if (state !== DONE_VALUE && state !== null) {
              doneObservations = 0;
              return;
            }
            doneObservations += 1;
            if (doneObservations < 2) return;
            // TODO(#26): error the stream when the sentinel is gone, close it only on DONE
            closeStream();
            await cleanup();
          } catch {
            /** A failed read is retried at the next check */
          } finally {
            scheduleDoneWatchdog();
          }
        };

        /**
         * Differs from upstream:
         * - The wait is configurable (https://github.com/vercel/resumable-stream/issues/42).
         * - The timer always settles the promise: with `null` if the producer has
         *   finished, else with an `AckTimeoutError`. Upstream also checks that the wall
         *   clock moved on by more than 1000 ms. When the timer fires at exactly 1000 ms,
         *   or when the clock goes back, the promise never settles. Upstream also awaits
         *   Redis in the timer with no handler, so a failure is an unhandled rejection
         *   (https://github.com/vercel/resumable-stream/issues/50,
         *   https://github.com/vercel/resumable-stream/pull/43).
         * - An answer that arrives after the timer fired is ignored, so it cannot open a
         *   stream that the timer is about to fail.
         */
        const onAckTimeout = async () => {
          timedOut = true;
          await cleanup();
          const state = await ctx.publisher.get(sentinelKey).catch(() => undefined);
          if (state === DONE_VALUE) {
            closeStream();
            settle(() => resolve(null));
            return;
          }
          const error = new AckTimeoutError();
          try {
            controller.error(error);
          } catch {
            /** Already closed */
          }
          settle(() => reject(error));
        };

        /**
         * Differs from upstream: a plain function. Upstream passes an async listener whose
         * promise the Redis client ignores, so a failed cleanup in it was an unhandled
         * rejection.
         */
        onMessage = (message: string) => {
          if (timedOut) return;
          /** The producer always answers, with an empty string if it has no chunk yet */
          clearTimeout(ackTimeout);
          settle(() => resolve(readableStream));
          if (message === DONE_MESSAGE) {
            closeStream();
            void cleanup();
            return;
          }
          startDoneWatchdog();
          try {
            controller.enqueue(message);
          } catch {
            /** The client cancelled the stream */
            void cleanup();
          }
        };

        try {
          ackTimeout = setTimeout(() => void onAckTimeout(), ctx.ackTimeoutMs);
          await ctx.subscriber.subscribe(chunkChannel, onMessage);
          const request: ResumeStreamMessage = { listenerId };
          await ctx.publisher.publish(
            `${ctx.keyPrefix}:request:${streamId}`,
            JSON.stringify(request),
          );
        } catch (error) {
          await cleanup();
          settle(() => reject(error));
        }
      },
      async cancel() {
        await cleanup();
      },
    });
  });
}
