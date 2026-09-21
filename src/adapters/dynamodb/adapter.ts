import type { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import type { StreamAdapter } from "../../adapter.js";
import { delay } from "../../internal/delay.js";
import { assertHeartbeatWindow, unref } from "../../internal/heartbeat.js";
import { createSerialQueue } from "../../internal/serial.js";
import { createStopWatchers } from "../../internal/stop-watcher.js";
import { createDynamoOperations, type DynamoOperations, type StoredItem } from "./client.js";
import { type Assembler, createAssembler, FORMAT_VERSION, splitIntoItems } from "./log.js";

export type DynamoDBAdapterOptions = {
  /**
   * Namespace every partition key this adapter writes begins with, so the table can hold
   * other things as well.
   */
  prefix?: string;
  /**
   * How long chunks may sit in memory before being written. Defaults to 250ms.
   *
   * This is the cost dial. Every flush is one item written, so doubling it roughly halves
   * what a stream costs, and adds that much to how far a resuming reader lags.
   */
  flushIntervalMs?: number;
  /**
   * Forces a write once this many chunks are buffered, whatever the interval.
   */
  batchSize?: number;
  /**
   * How often a resuming reader looks for new items. Defaults to 500ms.
   */
  resumePollIntervalMs?: number;
  /**
   * How often a producer checks for a stop request. Defaults to 1s.
   */
  stopPollIntervalMs?: number;
  /**
   * How often a producer that has nothing to write records that it is alive.
   * Defaults to 5s.
   */
  heartbeatMs?: number;
  /**
   * How long a generation may go unwritten before its producer is presumed dead.
   * Defaults to 30s.
   *
   * Keep it a healthy multiple of `heartbeatMs`: a missed beat means a busy event loop far
   * more often than a dead process, and declaring death early truncates a live stream.
   */
  deadAfterMs?: number;
  /**
   * How long an item lives before the table's time to live removes it. Defaults to 24h.
   *
   * It bounds how long a finished stream can still be replayed, so keep it comfortably
   * above the longest a client may take to come back.
   */
  ttlSeconds?: number;
};

export type CreateDynamoDBAdapterOptions = DynamoDBAdapterOptions & {
  /**
   * A client from `@aws-sdk/client-dynamodb`, built and configured by the caller.
   */
  client: DynamoDBClient;
  /**
   * The table to store streams in. It needs a string partition key, a string sort key,
   * and time to live enabled on `ttlAttributeName`.
   */
  tableName: string;
  /**
   * The attribute holding the table's partition key. Defaults to `pk`.
   */
  partitionKeyName?: string;
  /**
   * The attribute holding the table's sort key. Defaults to `sk`.
   */
  sortKeyName?: string;
  /**
   * The attribute the table's time to live is configured on. Defaults to `expiresAt`.
   */
  ttlAttributeName?: string;
};

/**
 * Settings that are not a caller's business.
 *
 * `maxItemBytes` follows from the DynamoDB ceiling of 400 KB per item rather than from
 * anything an application knows, and raising it past that ceiling would break streams
 * rather than tune them. The tests lower it to cut a chunk across items without producing
 * a payload of several hundred kilobytes.
 */
type InternalOptions = DynamoDBAdapterOptions & {
  maxItemBytes?: number;
};

const DEFAULT_PREFIX = `ai-resumable-stream`;
const DEFAULT_FLUSH_INTERVAL_MS = 250;
const DEFAULT_BATCH_SIZE = 50;
const DEFAULT_RESUME_POLL_INTERVAL_MS = 500;
const DEFAULT_STOP_POLL_INTERVAL_MS = 1_000;
const DEFAULT_HEARTBEAT_MS = 5_000;
const DEFAULT_DEAD_AFTER_MS = 30_000;
const DEFAULT_TTL_SECONDS = 24 * 60 * 60;

/**
 * Leaves roughly 50 KB of the 400 KB item for the keys, the attribute names and the
 * expiry, which no chunk should have to account for.
 */
const DEFAULT_MAX_ITEM_BYTES = 350_000;

const DEFAULT_PARTITION_KEY_NAME = `pk`;
const DEFAULT_SORT_KEY_NAME = `sk`;
const DEFAULT_TTL_ATTRIBUTE_NAME = `expiresAt`;

/** The sort key of the item a stream id's generation is read from. */
const POINTER_SORT_KEY = `POINTER`;

/** The sort key of the item that asks a generation to stop. */
const STOP_SORT_KEY = `STOP`;

const LOG_SORT_KEY_PREFIX = `LOG#`;

/**
 * Wide enough for more items than a stream can produce before its time to live removes
 * it, and fixed, because DynamoDB sorts keys as strings: an unpadded 10 would come before
 * a 9.
 */
const SEQUENCE_DIGITS = 12;

/**
 * The end of a log's range. `~` sorts after every digit, so the range holds every log
 * item and nothing else: `STOP` sits above it, and the pointer lives in its own partition.
 */
const LOG_RANGE_END = `${LOG_SORT_KEY_PREFIX}~`;

function logSortKey(sequence: number): string {
  return `${LOG_SORT_KEY_PREFIX}${String(sequence).padStart(SEQUENCE_DIGITS, `0`)}`;
}

function sequenceOf(sortKey: string): number {
  return Number(sortKey.slice(LOG_SORT_KEY_PREFIX.length));
}

/**
 * What a reader made of a page of items.
 */
const Progress = {
  /** Nothing the reader could use, so the generation has not moved on. */
  NONE: `none`,
  /** The generation is alive but had nothing to say. */
  BEATS: `beats`,
  /** Chunks were handed to the consumer. */
  CHUNKS: `chunks`,
  /** The generation is over. */
  END: `end`,
} as const;

type Progress = (typeof Progress)[keyof typeof Progress];

/**
 * A reader's place in one generation: the item it expects next, and the payload it is
 * still carrying from a chunk that was cut across items.
 */
type Follower = {
  expected: number;
  ended: boolean;
  lastWrittenAt: number | undefined;
  assembler: Assembler;
};

/**
 * Stores streams as a run of items in one DynamoDB table, and follows them by polling.
 *
 * Chunks, liveness and completion are all items of one partition, so a reader following a
 * stream costs one query per poll however many chunks arrive in between. Nothing is ever
 * scanned: every key is derived from the stream id and a counter.
 *
 * A stream stops being resumable when its producer stops, which is `deadAfterMs` at the
 * outside. How long the items then stay in the table is `ttlSeconds`, and the table's own
 * time to live is what removes them, so nothing here is deleted.
 */
export function createDynamoDBAdapter(options: CreateDynamoDBAdapterOptions): StreamAdapter {
  const {
    client,
    tableName,
    partitionKeyName = DEFAULT_PARTITION_KEY_NAME,
    sortKeyName = DEFAULT_SORT_KEY_NAME,
    ttlAttributeName = DEFAULT_TTL_ATTRIBUTE_NAME,
    ...rest
  } = options;

  const operations = createDynamoOperations({
    client,
    tableName,
    partitionKeyName,
    sortKeyName,
    ttlAttributeName,
    ttlSeconds: rest.ttlSeconds ?? DEFAULT_TTL_SECONDS,
  });

  return createStreamAdapter(operations, rest);
}

/**
 * The adapter, bound to storage operations rather than to a client, so the same logic can
 * be driven against something other than a table.
 */
export function createStreamAdapter(
  dynamo: DynamoOperations,
  options: InternalOptions = {},
): StreamAdapter {
  const {
    prefix = DEFAULT_PREFIX,
    flushIntervalMs = DEFAULT_FLUSH_INTERVAL_MS,
    batchSize = DEFAULT_BATCH_SIZE,
    resumePollIntervalMs = DEFAULT_RESUME_POLL_INTERVAL_MS,
    stopPollIntervalMs = DEFAULT_STOP_POLL_INTERVAL_MS,
    heartbeatMs = DEFAULT_HEARTBEAT_MS,
    deadAfterMs = DEFAULT_DEAD_AFTER_MS,
    maxItemBytes = DEFAULT_MAX_ITEM_BYTES,
  } = options;

  assertHeartbeatWindow(heartbeatMs, deadAfterMs);

  /**
   * Ids are encoded so that neither of them can bring a `#` into the key and make one
   * stream's partition look like another's.
   */
  const pointerPartition = (streamId: string) => `${prefix}#${encodeURIComponent(streamId)}`;

  function generationPartition(streamId: string, generationId: string): string {
    if (generationId === ``) throw new Error(`generationId must not be empty`);
    return `${pointerPartition(streamId)}#${encodeURIComponent(generationId)}`;
  }

  async function readPointer(streamId: string): Promise<string | undefined> {
    /**
     * Read consistently: a resume that is served the pointer as it was a moment ago
     * follows a generation the client has already replaced.
     */
    const item = await dynamo.get(pointerPartition(streamId), POINTER_SORT_KEY, {
      consistent: true,
    });
    return item?.generationId;
  }

  /**
   * Stop requests are keyed by generation, so one can never reach another generation of
   * the same stream id, and nothing has to be cleaned up when a stream id is reused. A stop
   * may be written before its generation starts, and is found once it does.
   */
  const stopWatchers = createStopWatchers({
    pollIntervalMs: stopPollIntervalMs,
    isStopRequested: async (streamId, generationId) =>
      (await dynamo.get(generationPartition(streamId, generationId), STOP_SORT_KEY)) !== undefined,
  });

  /**
   * Takes as many items as continue the follower's run, in order, and appends the chunks
   * they complete to `chunks`. Returns how many items were taken.
   *
   * An item out of sequence stops the run rather than being skipped. A query is served
   * from one partition and cannot reorder, but an eventually consistent one can be served
   * a page that is missing an item in the middle, and taking what came after it would drop
   * chunks for good.
   */
  function consume(follower: Follower, items: Array<StoredItem>, chunks: Array<string>): number {
    let taken = 0;

    for (const item of items) {
      if (sequenceOf(item.sortKey) !== follower.expected) break;

      if (item.version !== undefined && item.version !== FORMAT_VERSION) {
        throw new Error(`Stream is format ${item.version}, which this version cannot read`);
      }

      if (item.at !== undefined) follower.lastWrittenAt = item.at;
      if (item.chunks !== undefined) {
        chunks.push(
          ...follower.assembler.take({ chunks: item.chunks, partial: item.partial ?? false }),
        );
      }

      follower.expected += 1;
      taken += 1;

      if (item.end) {
        follower.ended = true;
        break;
      }
    }

    return taken;
  }

  return {
    async createStream({ streamId, generationId, chunks, waitUntil }) {
      const partition = generationPartition(streamId, generationId);

      /**
       * The generation opens with an item of its own, so it exists before it has produced
       * anything: a reader can tell a generation that has not got going yet from one that
       * was never started, and gets a timestamp to judge the producer by either way.
       *
       * Created rather than written: a generation id that already has items belongs to
       * another producer, and overwriting them would corrupt that stream and leave its
       * producer writing into a run it no longer agrees with. `ItemExistsError` says so
       * instead.
       */
      await dynamo.create(partition, logSortKey(0), {
        version: FORMAT_VERSION,
        at: Date.now(),
      });
      /**
       * Written after the generation exists, so the pointer always names something a
       * reader can read, and a reader never has to treat a missing generation as anything
       * but gone.
       */
      await dynamo.put(pointerPartition(streamId), POINTER_SORT_KEY, {
        generationId,
        version: FORMAT_VERSION,
      });

      const writes = createSerialQueue();

      /**
       * The next sequence number, advanced only once a write has landed. A number that was
       * never written would leave a hole no later item can fill, and readers stop at a hole
       * rather than read past it.
       */
      let sequence = 1;

      async function write(body: {
        chunks?: Array<string>;
        partial?: boolean;
        end?: boolean;
      }): Promise<void> {
        await dynamo.put(partition, logSortKey(sequence), { ...body, at: Date.now() });
        sequence += 1;
      }

      let buffer: Array<string> = [];
      let lastFlushAt = Date.now();
      let flushTimer: ReturnType<typeof setTimeout> | undefined;

      function clearFlushTimer() {
        if (flushTimer === undefined) return;
        clearTimeout(flushTimer);
        flushTimer = undefined;
      }

      async function flush(): Promise<void> {
        clearFlushTimer();
        if (buffer.length === 0) return;

        const pending = buffer;
        buffer = [];
        lastFlushAt = Date.now();

        await writes.run(async () => {
          for (const body of splitIntoItems(pending, maxItemBytes)) {
            await write(body);
          }
        });
      }

      /**
       * Guarantees that a chunk arriving just after a flush is still written within the
       * interval, rather than waiting for a chunk that may never come.
       */
      function scheduleFlush(): void {
        if (flushTimer !== undefined) return;

        const due = Math.max(0, lastFlushAt + flushIntervalMs - Date.now());
        flushTimer = setTimeout(() => {
          flushTimer = undefined;
          void flush().catch(() => {
            /** The next write reports it */
          });
        }, due);
      }

      /**
       * Beats for as long as the source is being drained, including while it is idle, so a
       * reader can tell an idle producer from one that has died.
       */
      const heartbeat = setInterval(() => {
        void writes
          .run(async () => {
            if (buffer.length > 0) return;
            await write({});
          })
          .catch(() => {
            /** Retried on the next beat */
          });
      }, heartbeatMs);
      unref(heartbeat);

      const consumed = (async () => {
        const reader = chunks.getReader();

        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer.push(value);
            if (buffer.length >= batchSize || Date.now() - lastFlushAt >= flushIntervalMs) {
              await flush();
            } else {
              scheduleFlush();
            }
          }

          await flush();
        } catch {
          await flush().catch(() => {
            /** The chunks are lost either way */
          });
        } finally {
          clearInterval(heartbeat);
          clearFlushTimer();
          reader.releaseLock();

          /**
           * A stream that failed is reported exactly like one that completed, so the end
           * item says only that the generation is over.
           */
          await writes
            .run(() => write({ end: true }))
            .catch(() => {
              /** Readers fall back to the silence */
            });
        }
      })();

      waitUntil?.(consumed);
    },

    async resumeStream({ streamId, generationId }) {
      /**
       * A named generation is read directly, so it can be resumed after a newer one took
       * the pointer. An id that cannot name a generation names nothing to resume.
       */
      const target = generationId ?? (await readPointer(streamId));
      if (target === undefined) return null;

      let partition: string;
      try {
        partition = generationPartition(streamId, target);
      } catch {
        return null;
      }

      const follower: Follower = {
        expected: 0,
        ended: false,
        lastWrittenAt: undefined,
        assembler: createAssembler(),
      };
      const replay: Array<string> = [];

      /**
       * The backlog is read consistently and page by page, because what it does not find
       * is the answer: an empty first page is how an unknown generation is told apart from
       * one that has only just started.
       */
      while (!follower.ended) {
        const items = await dynamo.query(
          partition,
          { from: logSortKey(follower.expected), to: LOG_RANGE_END },
          { consistent: true },
        );
        if (consume(follower, items, replay) === 0) break;
      }

      if (follower.lastWrittenAt === undefined) return null;

      /**
       * A stream that ended is reported the same way whether it completed or failed, and
       * so is one whose producer died. Unlike a bucket, a table hands out no clock of its
       * own, so this compares the reader's clock with the producer's: hosts whose clocks
       * are far apart will disagree about when a stream went quiet.
       */
      if (follower.ended || Date.now() - follower.lastWrittenAt > deadAfterMs) return null;

      const abortController = new AbortController();
      const { signal } = abortController;
      let lastProgressAt = Date.now();

      return new ReadableStream<string>({
        start(controller) {
          for (const data of replay) controller.enqueue(data);
        },

        async pull(controller) {
          /**
           * Reads once and reports what it found, enqueueing whatever the items completed.
           */
          async function advance(consistent: boolean): Promise<Progress> {
            const items = await dynamo.query(
              partition,
              { from: logSortKey(follower.expected), to: LOG_RANGE_END },
              { consistent },
            );

            const chunks: Array<string> = [];
            if (consume(follower, items, chunks) === 0) return Progress.NONE;

            lastProgressAt = Date.now();
            for (const data of chunks) controller.enqueue(data);

            if (follower.ended) return Progress.END;
            return chunks.length > 0 ? Progress.CHUNKS : Progress.BEATS;
          }

          while (!signal.aborted) {
            const progress = await advance(false);
            if (progress === Progress.END) {
              controller.close();
              return;
            }
            if (progress === Progress.CHUNKS) return;
            if (progress === Progress.BEATS) continue;

            /**
             * Nothing new. A living producer beats often enough that silence this long
             * means it is gone, but an eventually consistent read is allowed to be behind,
             * so the silence is confirmed before a reader is cut off for good.
             */
            if (Date.now() - lastProgressAt >= deadAfterMs) {
              const confirmed = await advance(true);
              if (confirmed === Progress.END || confirmed === Progress.NONE) {
                controller.close();
                return;
              }
              if (confirmed === Progress.CHUNKS) return;
              continue;
            }

            await delay(resumePollIntervalMs, signal);
          }
        },

        cancel() {
          abortController.abort();
        },
      });
    },

    async requestStop({ streamId, generationId }) {
      const target = generationId ?? (await readPointer(streamId));
      if (target === undefined) return;

      await dynamo.put(generationPartition(streamId, target), STOP_SORT_KEY, {
        at: Date.now(),
      });
    },

    async onStopRequested({ streamId, generationId, onStop }) {
      /** Fails here, rather than on every poll, for an id that cannot name a generation. */
      generationPartition(streamId, generationId);
      return stopWatchers.watch(streamId, generationId, onStop);
    },
  };
}
