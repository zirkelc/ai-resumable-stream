import type { S3Client } from "@aws-sdk/client-s3";
import type { StreamAdapter } from "../../adapter.js";
import { createS3Operations, type S3Operations, WriteOffsetMismatchError } from "./client.js";
import { delay } from "./delay.js";
import {
  collectChunks,
  decodeRecords,
  encodeBeat,
  encodeChunk,
  encodeEnd,
  encodeNext,
  encodeVersion,
  joinRecords,
  Outcome,
} from "./log.js";
import { createSerialQueue } from "./serial.js";
import { createStopWatchers } from "./stop-watcher.js";

export type S3ExpressAdapterOptions = {
  /**
   * Namespace for every key this adapter writes. Point a lifecycle expiration rule at it.
   */
  prefix?: string;
  /**
   * How long chunks may sit in memory before being written. Defaults to 250ms.
   *
   * This is the cost dial. Every flush is one billed PUT, so doubling it roughly halves
   * what a stream costs, and adds that much to how far a resuming reader lags.
   */
  flushIntervalMs?: number;
  /**
   * Forces a write once this many chunks are buffered, whatever the interval.
   */
  batchSize?: number;
  /**
   * How often a resuming reader looks for new bytes. Defaults to 500ms.
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
   * How long a log may go unwritten before its producer is presumed dead. Defaults to 30s.
   *
   * Keep it a healthy multiple of `heartbeatMs`: a missed beat means a busy event loop far
   * more often than a dead process, and declaring death early truncates a live stream.
   */
  deadAfterMs?: number;
};

export type CreateS3ExpressAdapterOptions = S3ExpressAdapterOptions & {
  /**
   * A client from `@aws-sdk/client-s3`, built and configured by the caller.
   */
  client: S3Client;
  /**
   * An S3 Express One Zone directory bucket in an Availability Zone. Appends exist
   * nowhere else, and they are what makes a stream one object rather than thousands.
   */
  bucket: string;
};

/**
 * Settings that are not a caller's business.
 *
 * `maxPartsPerSegment` follows from the S3 ceiling of 10,000 parts per object rather than
 * from anything an application knows, and a value above that ceiling would break streams
 * rather than tune them. The tests lower it to fill a segment without writing nine
 * thousand records.
 */
type InternalOptions = S3ExpressAdapterOptions & {
  maxPartsPerSegment?: number;
};

const DEFAULT_PREFIX = `ai-resumable-stream`;
const DEFAULT_FLUSH_INTERVAL_MS = 250;
const DEFAULT_BATCH_SIZE = 50;
const DEFAULT_RESUME_POLL_INTERVAL_MS = 500;
const DEFAULT_STOP_POLL_INTERVAL_MS = 1_000;
const DEFAULT_HEARTBEAT_MS = 5_000;
const DEFAULT_DEAD_AFTER_MS = 30_000;
const DEFAULT_MAX_PARTS_PER_SEGMENT = 9_000;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * The name of the pointer object inside a stream's prefix. A generation id may not take
 * it, or the pointer would sit where that generation's directory has to be.
 */
const POINTER_NAME = `current`;

/**
 * Path segments a generation id cannot become: nothing, a relative path, or the pointer.
 */
const RESERVED_SEGMENTS = new Set([``, `.`, `..`, POINTER_NAME]);

const VERSION_RECORD = encodeVersion();
const STOP_MARKER = encoder.encode(`1`);

/**
 * What a stream id points at. Written once per generation and never changed.
 *
 * It is JSON rather than a bare id so the entry point to a stream can gain a field later
 * without every reader having to be replaced first.
 */
type Pointer = {
  generationId: string;
};

function unref(timer: unknown): void {
  (timer as { unref?: () => void }).unref?.();
}

/**
 * A death threshold that is not comfortably larger than the beat will declare healthy
 * producers dead, which truncates live streams. Refuse the configuration outright rather
 * than let it corrupt streams under load.
 */
function assertHeartbeatWindow(heartbeatMs: number, deadAfterMs: number): void {
  if (deadAfterMs < heartbeatMs * 2) {
    throw new Error(
      `deadAfterMs (${deadAfterMs}) must be at least twice heartbeatMs (${heartbeatMs})`,
    );
  }
}

/**
 * Whether a log has been silent long enough for its producer to be presumed dead.
 *
 * Both timestamps come from the same S3 response, so this is a comparison of S3's clock
 * with itself. A producer and its readers run on different hosts, and a check that mixed
 * their clocks would kill healthy streams on the first few seconds of drift.
 */
function isStale(
  response: { date: number | undefined; lastModified: number | undefined },
  deadAfterMs: number,
): boolean {
  const { date, lastModified } = response;
  if (date === undefined || lastModified === undefined) return false;
  return date - lastModified > deadAfterMs;
}

/**
 * Stores streams as one appendable object each, in an S3 Express One Zone bucket.
 *
 * Chunks, liveness and completion all live in the same log, so a reader following a
 * stream costs one request per poll. Nothing is ever listed: every key is either derived
 * from the stream id or named by the log itself.
 *
 * A stream stops being resumable when its producer stops, which is `deadAfterMs` at the
 * outside. How long the objects then sit in the bucket is a lifecycle expiration rule
 * rather than anything this can enforce, so nothing here is deleted.
 */
export function createS3ExpressAdapter(options: CreateS3ExpressAdapterOptions): StreamAdapter {
  const { client, bucket, ...rest } = options;
  return createStreamAdapter(createS3Operations({ client, bucket }), rest);
}

/**
 * The adapter, bound to storage operations rather than to a client, so the tests can run
 * the whole of it against an in-memory bucket. No local emulator implements appends.
 */
export function createStreamAdapter(
  s3: S3Operations,
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
    maxPartsPerSegment = DEFAULT_MAX_PARTS_PER_SEGMENT,
  } = options;

  assertHeartbeatWindow(heartbeatMs, deadAfterMs);

  /**
   * A stream id becomes one path segment, so an id like `tenant:app/session` cannot nest
   * a directory or collide with the keys of another stream.
   */
  const streamPrefix = (streamId: string) => `${prefix}/${encodeURIComponent(streamId)}`;
  const pointerKey = (streamId: string) => `${streamPrefix(streamId)}/${POINTER_NAME}`;
  const stopKey = (streamId: string, generationId: string) =>
    `${generationPrefix(streamId, generationId)}/stop`;
  const segmentKey = (streamId: string, generationId: string, index: number) =>
    `${generationPrefix(streamId, generationId)}/${index}`;

  /**
   * A generation id becomes one path segment next to the pointer, for the same reason as
   * a stream id. Encoding leaves a generated id unchanged, so the layout is the same
   * whether the caller chose the id or not.
   */
  function generationPrefix(streamId: string, generationId: string): string {
    const segment = encodeURIComponent(generationId);
    if (RESERVED_SEGMENTS.has(segment)) {
      throw new Error(`generationId must not be empty, "." or "..", nor "${POINTER_NAME}"`);
    }
    return `${streamPrefix(streamId)}/${segment}`;
  }

  async function readPointer(streamId: string): Promise<Pointer | undefined> {
    const result = await s3.read(pointerKey(streamId), 0);
    if (!result || result.bytes.length === 0) return undefined;

    try {
      return JSON.parse(decoder.decode(result.bytes)) as Pointer;
    } catch {
      return undefined;
    }
  }

  /**
   * Stop requests are keyed by generation, so one can never reach another generation of
   * the same stream id, and nothing has to be cleaned up when a stream id is reused. A stop
   * may be written before its generation starts, and is found once it does.
   */
  const stopWatchers = createStopWatchers({
    pollIntervalMs: stopPollIntervalMs,
    isStopRequested: async (streamId, generationId) =>
      (await s3.head(stopKey(streamId, generationId))) !== undefined,
  });

  return {
    async createStream({ streamId, generationId, chunks, waitUntil }) {
      const pointer: Pointer = { generationId };

      /**
       * A segment opens with its format version, so it is never empty and a reader gets
       * bytes, and with them S3's clock, from its very first read. The log is created
       * before the pointer, so a pointer always names a log that exists and a reader never
       * has to treat a missing one as anything but gone.
       */
      const segment = {
        index: 0,
        key: segmentKey(streamId, generationId, 0),
        offset: VERSION_RECORD.length,
        parts: 1,
      };
      /**
       * Created rather than written: a generation id that already has a log belongs to
       * another generation, and overwriting it would corrupt that stream and leave its
       * producer appending into an object it no longer agrees with. `ObjectExistsError`
       * says so instead.
       */
      await s3.create(segment.key, VERSION_RECORD);
      await s3.put(pointerKey(streamId), encoder.encode(JSON.stringify(pointer)));

      const writes = createSerialQueue();

      async function appendRecord(body: Uint8Array): Promise<void> {
        try {
          await s3.append(segment.key, segment.offset, body);
        } catch (error) {
          if (!(error instanceof WriteOffsetMismatchError)) throw error;

          /**
           * Either the append landed and its response was lost, or the object is not the
           * length this producer thinks it is. The size tells the two apart, and an
           * append is never replayed blindly: it is not idempotent.
           */
          const head = await s3.head(segment.key);
          if (head?.size === segment.offset + body.length) {
            segment.offset = head.size;
            segment.parts += 1;
            return;
          }
          if (head?.size !== segment.offset) throw error;

          await s3.append(segment.key, segment.offset, body);
        }

        segment.offset += body.length;
        segment.parts += 1;
      }

      /**
       * Continues the log in a new object once this one has used its parts, leaving
       * behind a record naming where a reader should carry on.
       */
      async function rollSegment(): Promise<void> {
        const next = segmentKey(streamId, generationId, segment.index + 1);
        await appendRecord(encodeNext(next));
        await s3.put(next, VERSION_RECORD);

        segment.index += 1;
        segment.key = next;
        segment.offset = VERSION_RECORD.length;
        segment.parts = 1;
      }

      async function write(body: Uint8Array): Promise<void> {
        if (segment.parts >= maxPartsPerSegment) await rollSegment();
        await appendRecord(body);
      }

      let buffer: Array<Uint8Array> = [];
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
        await writes.run(() => write(joinRecords(pending)));
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
       * Beats for as long as the source is being drained, including while it is idle. A
       * beat is a real record because S3 rejects an append with an empty body.
       */
      const heartbeat = setInterval(() => {
        void writes
          .run(async () => {
            if (buffer.length > 0) return;
            await write(encodeBeat());
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

            buffer.push(encodeChunk(value));
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
           * record says only that the generation is over.
           */
          await writes
            .run(() => write(encodeEnd()))
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
      const target = generationId ?? (await readPointer(streamId))?.generationId;
      if (target === undefined) return null;

      let firstKey: string;
      try {
        firstKey = segmentKey(streamId, target, 0);
      } catch {
        return null;
      }

      const cursor = { key: firstKey, offset: 0 };
      const replay: Array<string> = [];
      let ended = false;
      let stale = false;

      /**
       * One ranged read returns the whole backlog. A second one happens only when the log
       * has already continued in another object.
       */
      while (true) {
        const result = await s3.read(cursor.key, cursor.offset);
        if (!result) return null;

        stale = isStale(result, deadAfterMs);
        if (result.bytes.length === 0) break;

        const { records, consumed } = decodeRecords(result.bytes);
        cursor.offset += consumed;

        const collected = collectChunks(records, replay);
        if (collected.outcome === Outcome.END) {
          ended = true;
          break;
        }
        if (collected.outcome === Outcome.NEXT) {
          cursor.key = collected.key;
          cursor.offset = 0;
          continue;
        }
        break;
      }

      /**
       * A stream that ended is reported the same way whether it completed or failed, and
       * so is one whose producer died: there is nothing left to follow.
       */
      if (ended || stale) return null;

      const abortController = new AbortController();
      const { signal } = abortController;
      let lastProgressAt = Date.now();

      return new ReadableStream<string>({
        start(controller) {
          for (const data of replay) controller.enqueue(data);
        },

        async pull(controller) {
          while (!signal.aborted) {
            const result = await s3.read(cursor.key, cursor.offset);
            if (!result) {
              controller.close();
              return;
            }

            if (result.bytes.length > 0) {
              lastProgressAt = Date.now();

              const { records, consumed } = decodeRecords(result.bytes);
              cursor.offset += consumed;

              const chunks: Array<string> = [];
              const collected = collectChunks(records, chunks);
              for (const data of chunks) controller.enqueue(data);

              if (collected.outcome === Outcome.END) {
                controller.close();
                return;
              }
              if (collected.outcome === Outcome.NEXT) {
                cursor.key = collected.key;
                cursor.offset = 0;
              }
              if (chunks.length > 0) return;
              continue;
            }

            /**
             * Nothing new. How long the log has been quiet is only worth asking S3 once
             * the reader's own wait suggests something is wrong, and the answer itself is
             * always S3's, never the reader's.
             */
            if (Date.now() - lastProgressAt >= deadAfterMs) {
              const head = await s3.head(cursor.key);
              if (!head || isStale(head, deadAfterMs)) {
                controller.close();
                return;
              }
              lastProgressAt = Date.now();
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
      const target = generationId ?? (await readPointer(streamId))?.generationId;
      if (target === undefined) return;

      await s3.put(stopKey(streamId, target), STOP_MARKER);
    },

    async onStopRequested({ streamId, generationId, onStop }) {
      /** Fails here, rather than on every poll, for an id that cannot name a generation. */
      stopKey(streamId, generationId);
      return stopWatchers.watch(streamId, generationId, onStop);
    },
  };
}
