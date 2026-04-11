import type { LanguageModelV3StreamPart } from "@ai-sdk/provider";
import type { UIMessageChunk } from "ai";
import { simulateReadableStream, streamText } from "ai";
import { MockLanguageModelV3 } from "ai/test";
import { consumeUIMessageStream } from "ai-stream-utils";
import { convertAsyncIterableToArray } from "ai-stream-utils/utils";
import { createClient } from "redis";
import { RedisMemoryServer } from "redis-memory-server";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test, vi } from "vitest";
import { createResumableUIMessageStream } from "./resumable-ui-message-stream.js";

type Redis = ReturnType<typeof createClient>;

let redisServer: RedisMemoryServer;
let redisUrl: string;
let redisClients: Array<Redis> = [];
let pendingPromises: Array<Promise<unknown>>;

function createRedisClient(): Redis {
  const client = createClient({ url: redisUrl });
  redisClients.push(client);
  return client;
}

function waitUntil(promise: Promise<unknown>): void {
  pendingPromises.push(promise);
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Create a stream from chunks with optional delay
 */
function createStream<CHUNK>(chunks: Array<CHUNK>, delayMs = 0): ReadableStream<CHUNK> {
  return simulateReadableStream({
    chunks,
    initialDelayInMs: delayMs,
    chunkDelayInMs: delayMs,
  });
}

/**
 * Create a mock model with optional delay and abort signal support
 */
function createMockModel(options: {
  chunks: Array<LanguageModelV3StreamPart>;
  delay?: number;
  abortSignal?: AbortSignal;
}) {
  const { chunks, delay = 0, abortSignal } = options;
  let pullCount = 0;

  return new MockLanguageModelV3({
    doStream: async () => ({
      stream: new ReadableStream({
        async pull(controller) {
          if (abortSignal?.aborted) {
            controller.error(new DOMException(`The user aborted a request.`, `AbortError`));
            return;
          }

          await sleep(delay);

          if (abortSignal?.aborted) {
            controller.error(new DOMException(`The user aborted a request.`, `AbortError`));
            return;
          }

          if (pullCount < chunks.length) {
            controller.enqueue(chunks[pullCount]);
            pullCount++;
          } else {
            controller.close();
          }
        },
      }),
    }),
  });
}

beforeAll(async () => {
  redisServer = new RedisMemoryServer();
  const host = await redisServer.getHost();
  const port = await redisServer.getPort();
  redisUrl = `redis://${host}:${port}`;
}, 60_000);

afterAll(async () => {
  await redisServer.stop();
}, 30_000);

beforeEach(() => {
  redisClients = [];
  pendingPromises = [];
});

afterEach(async () => {
  /**
   * Wait for pending promises with timeout to handle cases where
   * Redis disconnects mid-stream or streams error
   */
  await Promise.race([Promise.allSettled(pendingPromises), sleep(200)]);
  await sleep(25);

  await Promise.all(
    redisClients.map((client) => (client.isOpen ? client.quit() : Promise.resolve())),
  );

  /** Flush DB with a temp client to ensure clean state for next test */
  const tempClient = createClient({ url: redisUrl });
  await tempClient.connect();
  await tempClient.flushDb();
  await tempClient.quit();
});

describe(`createResumableUIMessageStream`, () => {
  describe(`Redis connection`, () => {
    test(`should connect clients if disconnected`, async () => {
      // Arrange - create fresh disconnected clients
      const publisher = createRedisClient();
      const subscriber = createRedisClient();

      // Verify not connected
      expect(publisher.isOpen).toBe(false);
      expect(subscriber.isOpen).toBe(false);

      // Act
      await createResumableUIMessageStream({
        streamId: `test-stream`,
        publisher,
        subscriber,
        waitUntil,
      });

      // Assert - now connected
      expect(publisher.isOpen).toBe(true);
      expect(subscriber.isOpen).toBe(true);
    });

    test(`should not disconnect clients after stream completes`, async () => {
      // Arrange
      const [publisher, subscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      // Verify connected
      expect(publisher.isOpen).toBe(true);
      expect(subscriber.isOpen).toBe(true);

      const streamId = `test-stream`;
      const chunks: Array<UIMessageChunk> = [
        { type: `start` },
        { type: `finish`, finishReason: `stop` },
      ];
      const stream = createStream(chunks);

      const context = await createResumableUIMessageStream({
        streamId,
        publisher,
        subscriber,
        waitUntil,
      });

      // Act - complete the stream
      const resultStream = await context.startStream(stream);
      await convertAsyncIterableToArray(resultStream);

      // Assert - clients still connected
      expect(publisher.isOpen).toBe(true);
      expect(subscriber.isOpen).toBe(true);
    });

    test(`should not reconnect already-connected clients`, async () => {
      // Arrange
      const publisher = createRedisClient();
      const subscriber = createRedisClient();
      await Promise.all([publisher.connect(), subscriber.connect()]);

      expect(publisher.isOpen).toBe(true);
      expect(subscriber.isOpen).toBe(true);

      // Spy on connect to verify it's not called
      const publisherConnect = vi.spyOn(publisher, `connect`);
      const subscriberConnect = vi.spyOn(subscriber, `connect`);

      // Act
      await createResumableUIMessageStream({
        streamId: `test-stream`,
        publisher,
        subscriber,
        waitUntil,
      });

      // Assert - connect was never called
      expect(publisherConnect).not.toHaveBeenCalled();
      expect(subscriberConnect).not.toHaveBeenCalled();
    });

    test(`should unsubscribe from stop channel after stream completes`, async () => {
      // Arrange
      const [publisher, subscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      const streamId = `test-stream`;
      const abortController = new AbortController();
      const chunks: Array<UIMessageChunk> = [
        { type: `start` },
        { type: `finish`, finishReason: `stop` },
      ];
      const stream = createStream(chunks);

      const unsubscribeSpy = vi.spyOn(subscriber, `unsubscribe`);

      const context = await createResumableUIMessageStream({
        streamId,
        publisher,
        subscriber,
        abortController,
        waitUntil,
      });

      // Act - complete the stream
      const resultStream = await context.startStream(stream);
      await convertAsyncIterableToArray(resultStream);

      // Assert - unsubscribe was called
      expect(unsubscribeSpy).toHaveBeenCalled();
    });
  });

  describe(`Errors`, () => {
    /**
     * Suppress expected stream pipeline errors and Redis connection
     * lifecycle errors (e.g. disconnects / closed sockets) that are
     * expected when exercising failure scenarios.
     */
    const rejectionHandler = (reason: Error) => {
      const expectedErrors = [`Stream error`, `The client is closed`];
      if (expectedErrors.includes(reason?.message)) {
        return; // Suppress
      }
      throw reason;
    };
    const uncaughtHandler = (error: Error) => {
      if (error?.message?.includes(`Socket closed unexpectedly`)) {
        return; // Suppress
      }
      throw error;
    };

    beforeEach(() => {
      process.on(`unhandledRejection`, rejectionHandler);
      process.on(`uncaughtException`, uncaughtHandler);
    });

    afterEach(async () => {
      // Wait for async rejections to fire before removing handlers
      await sleep(100);
      process.off(`unhandledRejection`, rejectionHandler);
      process.off(`uncaughtException`, uncaughtHandler);
    });

    test(`should fail when Redis connection fails at init`, async () => {
      // Arrange
      const badPublisher = createClient({ url: `redis://invalid:9999` });
      const badSubscriber = createClient({ url: `redis://invalid:9999` });

      try {
        // Act & Assert
        const promise = createResumableUIMessageStream({
          streamId: `test-stream`,
          publisher: badPublisher,
          subscriber: badSubscriber,
          waitUntil,
        });

        await expect(promise).rejects.toThrow();
      } finally {
        await Promise.allSettled([badPublisher.disconnect(), badSubscriber.disconnect()]);
      }
    });

    test(`should fail when source stream is locked`, async () => {
      // Arrange
      const [publisher, subscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      const streamId = `test-stream`;
      const chunks: Array<UIMessageChunk> = [
        { type: `start` },
        { type: `finish`, finishReason: `stop` },
      ];
      const stream = createStream(chunks);
      stream.getReader(); // Lock without releasing

      const context = await createResumableUIMessageStream({
        streamId,
        publisher,
        subscriber,
        waitUntil,
      });

      // Act
      const resultStream = context.startStream(stream);

      // Assert - should error due to locked stream
      await expect(resultStream).rejects.toThrow();
    });

    test(`should fail when reading from erroring source stream`, async () => {
      // Arrange
      const [publisher, subscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      const streamId = `test-stream`;
      const errorStream = new ReadableStream({
        pull(controller) {
          controller.error(new Error(`Stream error`));
        },
      });

      const context = await createResumableUIMessageStream({
        streamId,
        publisher,
        subscriber,
        waitUntil,
      });

      // Act
      const resultStream = await context.startStream(errorStream);
      const reader = resultStream.getReader();

      // Assert - error propagates when reading
      await expect(reader.read()).rejects.toThrow();
    });

    test(`should continue client stream when Redis fails mid-stream`, async () => {
      // Arrange
      const [publisher, subscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      const streamId = `test-stream`;
      const chunks: Array<UIMessageChunk> = [
        { type: `start` },
        { type: `text-delta`, id: `1`, delta: `Hello` },
        { type: `text-delta`, id: `1`, delta: `World` },
        { type: `finish`, finishReason: `stop` },
      ];
      const delayedStream = createStream(chunks, 50);

      const context = await createResumableUIMessageStream({
        streamId,
        publisher,
        subscriber,
        waitUntil,
      });

      // Act
      const resultStream = await context.startStream(delayedStream);
      const reader = resultStream.getReader();

      // Read first chunk
      const first = await reader.read();
      expect(first.value?.type).toBe(`start`);

      // Kill Redis connection mid-stream
      await publisher.quit();

      // Continue reading - should still work
      const remaining: Array<UIMessageChunk> = [];
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        remaining.push(value!);
      }

      // Assert - client received all chunks despite Redis failure
      expect(remaining.length).toBe(3);
      expect(remaining[2]?.type).toBe(`finish`);
    });
  });

  describe(`waitUntil`, () => {
    test(`should call waitUntil with background promise`, async () => {
      // Arrange
      const [publisher, subscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      const streamId = `test-stream`;
      const chunks: Array<UIMessageChunk> = [
        { type: `start` },
        { type: `finish`, finishReason: `stop` },
      ];
      const stream = createStream(chunks);
      const waitUntilSpy = vi.fn();

      const context = await createResumableUIMessageStream({
        streamId,
        publisher,
        subscriber,
        waitUntil: waitUntilSpy,
      });

      // Act
      const resultStream = await context.startStream(stream);
      await convertAsyncIterableToArray(resultStream);

      // Assert - waitUntil was called with a promise
      expect(waitUntilSpy).toHaveBeenCalled();
      expect(waitUntilSpy.mock.calls[0]?.[0]).toBeInstanceOf(Promise);
    });
  });

  describe(`startStream`, () => {
    test(`should start stream and consume all chunks`, async () => {
      // Arrange
      const [publisher, subscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      const streamId = `test-stream`;
      const chunks: Array<UIMessageChunk> = [
        { type: `start` },
        { type: `start-step` },
        { type: `text-start`, id: `1` },
        { type: `text-delta`, id: `1`, delta: `Hello` },
        { type: `text-delta`, id: `1`, delta: ` ` },
        { type: `text-delta`, id: `1`, delta: `World` },
        { type: `text-end`, id: `1` },
        { type: `finish-step` },
        { type: `finish`, finishReason: `stop` },
      ];
      const stream = createStream(chunks);
      const context = await createResumableUIMessageStream({
        streamId,
        publisher,
        subscriber,
        waitUntil,
      });

      // Act
      const resultStream = await context.startStream(stream);
      const result = await convertAsyncIterableToArray(resultStream);

      // Assert
      expect(result.length).toBe(chunks.length);
      expect(result).toEqual(chunks);
    });

    test(`should handle empty stream`, async () => {
      // Arrange
      const [publisher, subscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      const streamId = `test-stream`;
      const chunks: Array<UIMessageChunk> = [];
      const stream = createStream(chunks);
      const context = await createResumableUIMessageStream({
        streamId,
        publisher,
        subscriber,
        waitUntil,
      });

      // Act
      const resultStream = await context.startStream(stream);
      const result = await convertAsyncIterableToArray(resultStream);

      // Assert
      expect(result.length).toBe(0);
    });

    test(`should handle control-only chunks (start/finish)`, async () => {
      // Arrange
      const [publisher, subscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      const streamId = `test-stream`;
      const chunks: Array<UIMessageChunk> = [
        { type: `start` },
        { type: `finish`, finishReason: `stop` },
      ];
      const stream = createStream(chunks);
      const context = await createResumableUIMessageStream({
        streamId,
        publisher,
        subscriber,
        waitUntil,
      });

      // Act
      const resultStream = await context.startStream(stream);
      const result = await convertAsyncIterableToArray(resultStream);

      // Assert
      expect(result.length).toBe(2);
      expect(result[0]).toEqual({ type: `start` });
      expect(result[1]).toEqual({ type: `finish`, finishReason: `stop` });
    });

    describe(`onFlush`, () => {
      test(`should call onFlush when stream finishes normally`, async () => {
        // Arrange
        const [publisher, subscriber] = await Promise.all([
          createRedisClient().connect(),
          createRedisClient().connect(),
        ]);

        const streamId = `test-stream`;
        const chunks: Array<UIMessageChunk> = [
          { type: `start` },
          { type: `finish`, finishReason: `stop` },
        ];
        const stream = createStream(chunks);
        const onFlush = vi.fn();

        const context = await createResumableUIMessageStream({
          streamId,
          publisher,
          subscriber,
          waitUntil,
        });

        // Act
        const resultStream = await context.startStream(stream, { onFlush });
        await convertAsyncIterableToArray(resultStream);
        await sleep(25);

        // Assert
        expect(onFlush).toHaveBeenCalledTimes(1);
      });

      test(`should call onFlush when source stream errors`, async () => {
        // Arrange
        const rejectionHandler = (reason: Error) => {
          if (reason?.message === `Stream error`) return;
          throw reason;
        };
        process.on(`unhandledRejection`, rejectionHandler);

        const [publisher, subscriber] = await Promise.all([
          createRedisClient().connect(),
          createRedisClient().connect(),
        ]);

        const streamId = `test-stream`;
        const errorStream = new ReadableStream({
          pull(controller) {
            controller.error(new Error(`Stream error`));
          },
        });
        let flushResolve: () => void;
        const flushPromise = new Promise<void>((r) => {
          flushResolve = r;
        });
        const onFlush = vi.fn(() => {
          flushResolve();
        });

        const context = await createResumableUIMessageStream({
          streamId,
          publisher,
          subscriber,
          waitUntil,
        });

        // Act
        const resultStream = await context.startStream(errorStream, { onFlush });
        const reader = resultStream.getReader();
        await reader.read().catch(() => {});
        await flushPromise;

        // Assert
        expect(onFlush).toHaveBeenCalledTimes(1);

        await sleep(100);
        process.off(`unhandledRejection`, rejectionHandler);
      });

      test(`should call onFlush when stream is aborted`, async () => {
        // Arrange
        const [publisher, subscriber] = await Promise.all([
          createRedisClient().connect(),
          createRedisClient().connect(),
        ]);

        const streamId = `test-stream`;
        const abortController = new AbortController();
        const chunks: Array<UIMessageChunk> = [
          { type: `start` },
          { type: `text-delta`, id: `1`, delta: `Hello` },
          { type: `text-delta`, id: `1`, delta: `World` },
          { type: `finish`, finishReason: `stop` },
        ];
        const delayedStream = createStream(chunks, 50);
        const onFlush = vi.fn();

        const context = await createResumableUIMessageStream({
          streamId,
          publisher,
          subscriber,
          abortController,
          waitUntil,
        });

        // Act
        const resultStream = await context.startStream(delayedStream, { onFlush });
        const reader = resultStream.getReader();
        await reader.read();

        await context.stopStream();
        await sleep(300);

        // Assert
        expect(onFlush).toHaveBeenCalledTimes(1);

        reader.releaseLock();
      });

      test(`should call onFlush after Redis persistence is complete`, async () => {
        // Arrange
        const [publisher, subscriber, secondPublisher, secondSubscriber] = await Promise.all([
          createRedisClient().connect(),
          createRedisClient().connect(),
          createRedisClient().connect(),
          createRedisClient().connect(),
        ]);

        const streamId = `test-stream`;
        const chunks: Array<UIMessageChunk> = [
          { type: `start` },
          { type: `finish`, finishReason: `stop` },
        ];
        const stream = createStream(chunks);
        let resumeResultInsideCallback:
          | Awaited<ReturnType<typeof resumeContext.resumeStream>>
          | undefined;
        let flushResolve: () => void;
        const flushPromise = new Promise<void>((r) => {
          flushResolve = r;
        });

        const resumeContext = await createResumableUIMessageStream({
          streamId,
          publisher: secondPublisher,
          subscriber: secondSubscriber,
          waitUntil,
        });

        const onFlush = async () => {
          resumeResultInsideCallback = await resumeContext.resumeStream();
          flushResolve();
        };

        const context = await createResumableUIMessageStream({
          streamId,
          publisher,
          subscriber,
          waitUntil,
        });

        // Act
        const resultStream = await context.startStream(stream, { onFlush });
        await convertAsyncIterableToArray(resultStream);
        await flushPromise;

        // Assert - stream is completed, resume returns null
        expect(resumeResultInsideCallback).toBeNull();
      });

      test(`should not throw when onFlush callback throws`, async () => {
        // Arrange
        const [publisher, subscriber] = await Promise.all([
          createRedisClient().connect(),
          createRedisClient().connect(),
        ]);

        const streamId = `test-stream`;
        const chunks: Array<UIMessageChunk> = [
          { type: `start` },
          { type: `finish`, finishReason: `stop` },
        ];
        const stream = createStream(chunks);
        const onFlush = vi.fn(() => {
          throw new Error(`callback error`);
        });

        const context = await createResumableUIMessageStream({
          streamId,
          publisher,
          subscriber,
          waitUntil,
        });

        // Act
        const resultStream = await context.startStream(stream, { onFlush });
        const result = await convertAsyncIterableToArray(resultStream);

        // Assert - stream completed successfully despite callback error
        expect(result.length).toBe(2);
        expect(onFlush).toHaveBeenCalledTimes(1);
      });
    });
  });

  describe(`resumeStream`, () => {
    test(`should return null when no active stream`, async () => {
      // Arrange
      const [publisher, subscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      const streamId = `test-stream`;
      const context = await createResumableUIMessageStream({
        streamId,
        publisher,
        subscriber,
        waitUntil,
      });

      // Act
      const result = await context.resumeStream();

      // Assert
      expect(result).toBeNull();
    });

    test(`should return null for non-existent streamId`, async () => {
      // Arrange
      const [publisher, subscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      const streamId = `non-existent-stream`;
      const context = await createResumableUIMessageStream({
        streamId,
        publisher,
        subscriber,
        waitUntil,
      });

      // Act
      const result = await context.resumeStream();

      // Assert
      expect(result).toBeNull();
    });

    test(`should return null when stream is already completed`, async () => {
      // Arrange
      const [publisher, subscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      const streamId = `test-stream`;
      const chunks: Array<UIMessageChunk> = [
        { type: `start` },
        { type: `start-step` },
        { type: `text-start`, id: `1` },
        { type: `text-delta`, id: `1`, delta: `Hello` },
        { type: `text-delta`, id: `1`, delta: ` ` },
        { type: `text-delta`, id: `1`, delta: `World` },
        { type: `text-end`, id: `1` },
        { type: `finish-step` },
        { type: `finish`, finishReason: `stop` },
      ];
      const stream = createStream(chunks);
      const context = await createResumableUIMessageStream({
        streamId,
        publisher,
        subscriber,
        waitUntil,
      });

      // Start and complete the stream
      const resultStream = await context.startStream(stream);
      await convertAsyncIterableToArray(resultStream);

      // Wait for background operations
      await sleep(25);

      // Act
      const resumedStream = await context.resumeStream();

      // Assert
      expect(resumedStream).toBeNull();
    });
  });

  describe(`stopStream`, () => {
    test(`should stop active stream and fire abort signal`, async () => {
      // Arrange
      const [publisher, subscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      const streamId = `test-stream`;
      const chunks: Array<UIMessageChunk> = [
        { type: `start` },
        { type: `start-step` },
        { type: `text-start`, id: `1` },
        { type: `text-delta`, id: `1`, delta: `Hello` },
        { type: `text-delta`, id: `1`, delta: ` ` },
        { type: `text-delta`, id: `1`, delta: `World` },
        { type: `text-end`, id: `1` },
        { type: `finish-step` },
        { type: `finish`, finishReason: `stop` },
      ];
      const delayedStream = createStream(chunks, 25);
      const abortController = new AbortController();
      let abortFired = false;

      abortController.signal.addEventListener(`abort`, () => {
        abortFired = true;
      });

      const context = await createResumableUIMessageStream({
        streamId,
        publisher,
        subscriber,
        abortController,
        waitUntil,
      });

      // Act
      const resultStream = await context.startStream(delayedStream);
      const reader = resultStream.getReader();
      await reader.read();

      await context.stopStream();

      // Wait for abort to propagate
      await sleep(25);

      // Assert
      expect(abortController.signal.aborted).toBe(true);
      expect(abortFired).toBe(true);

      reader.releaseLock();
    });

    test(`should work without abortController (no-op)`, async () => {
      // Arrange
      const [publisher, subscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      const streamId = `test-stream`;
      const context = await createResumableUIMessageStream({
        streamId,
        publisher,
        subscriber,
        waitUntil,
      });

      const chunks: Array<UIMessageChunk> = [
        { type: `start` },
        { type: `start-step` },
        { type: `text-start`, id: `1` },
        { type: `text-delta`, id: `1`, delta: `Hello` },
        { type: `text-delta`, id: `1`, delta: ` ` },
        { type: `text-delta`, id: `1`, delta: `World` },
        { type: `text-end`, id: `1` },
        { type: `finish-step` },
        { type: `finish`, finishReason: `stop` },
      ];
      const stream = createStream(chunks);
      await context.startStream(stream);

      // Act & Assert - should not throw
      await expect(context.stopStream()).resolves.not.toThrow();
    });
  });

  describe(`keepAlive`, () => {
    test(`should allow resumeStream after source ends when keepAlive is provided`, async () => {
      // Arrange
      const [publisher, subscriber, secondPublisher, secondSubscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      const streamId = `test-stream`;
      const chunks: Array<UIMessageChunk> = [
        { type: `start` },
        { type: `text-delta`, id: `1`, delta: `Hello` },
        { type: `finish`, finishReason: `stop` },
      ];
      const stream = createStream(chunks);
      const { promise, resolve } = Promise.withResolvers<void>();

      const context = await createResumableUIMessageStream({
        streamId,
        publisher,
        subscriber,
        waitUntil,
      });

      // Act - consume the stream fully
      const resultStream = await context.startStream(stream, { keepAlive: promise });
      const result = await convertAsyncIterableToArray(resultStream);
      expect(result.length).toBe(3);

      // Wait for drain loop to reach the promise
      await sleep(25);

      // Resume from a second client — should still work because keepAlive defers teardown
      const resumeContext = await createResumableUIMessageStream({
        streamId,
        publisher: secondPublisher,
        subscriber: secondSubscriber,
        waitUntil,
      });

      const resumedStream = await resumeContext.resumeStream();

      // Assert - stream is still resumable (not null)
      expect(resumedStream).not.toBeNull();

      // Resolve the promise to trigger teardown so the resumed stream completes
      resolve();

      const resumedChunks = await convertAsyncIterableToArray(resumedStream!);
      expect(resumedChunks).toEqual(result);
    });

    test(`should return null from resumeStream after keepAlive promise resolves`, async () => {
      // Arrange
      const [publisher, subscriber, secondPublisher, secondSubscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      const streamId = `test-stream`;
      const chunks: Array<UIMessageChunk> = [
        { type: `start` },
        { type: `finish`, finishReason: `stop` },
      ];
      const stream = createStream(chunks);
      const { promise, resolve } = Promise.withResolvers<void>();

      const context = await createResumableUIMessageStream({
        streamId,
        publisher,
        subscriber,
        waitUntil,
      });

      // Act - consume, then resolve the promise
      const resultStream = await context.startStream(stream, { keepAlive: promise });
      await convertAsyncIterableToArray(resultStream);
      await sleep(25);
      resolve();
      await sleep(25);

      // Resume after teardown
      const resumeContext = await createResumableUIMessageStream({
        streamId,
        publisher: secondPublisher,
        subscriber: secondSubscriber,
        waitUntil,
      });

      const resumedStream = await resumeContext.resumeStream();

      // Assert - stream is done, resume returns null
      expect(resumedStream).toBeNull();
    });

    test(`should close redis stream even when keepAlive promise rejects`, async () => {
      // Arrange
      const [publisher, subscriber, secondPublisher, secondSubscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      const streamId = `test-stream`;
      const chunks: Array<UIMessageChunk> = [
        { type: `start` },
        { type: `finish`, finishReason: `stop` },
      ];
      const stream = createStream(chunks);
      const { promise, reject } = Promise.withResolvers<void>();

      const context = await createResumableUIMessageStream({
        streamId,
        publisher,
        subscriber,
        waitUntil,
      });

      // Act - consume, then reject the promise
      const resultStream = await context.startStream(stream, { keepAlive: promise });
      await convertAsyncIterableToArray(resultStream);
      await sleep(25);
      reject(new Error(`post-stream work failed`));
      await sleep(25);

      // Resume after teardown
      const resumeContext = await createResumableUIMessageStream({
        streamId,
        publisher: secondPublisher,
        subscriber: secondSubscriber,
        waitUntil,
      });

      const resumedStream = await resumeContext.resumeStream();

      // Assert - stream still tore down despite rejection
      expect(resumedStream).toBeNull();
    });

    test(`should not await keepAlive promise when source stream errors`, async () => {
      // Arrange
      const rejectionHandler = (reason: Error) => {
        if (reason?.message === `Stream error`) return;
        throw reason;
      };
      process.on(`unhandledRejection`, rejectionHandler);

      const [publisher, subscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      const streamId = `test-stream`;
      const errorStream = new ReadableStream({
        pull(controller) {
          controller.error(new Error(`Stream error`));
        },
      });
      const { promise } = Promise.withResolvers<void>();

      const context = await createResumableUIMessageStream({
        streamId,
        publisher,
        subscriber,
        waitUntil,
      });

      // Act - start stream that errors
      const resultStream = await context.startStream(errorStream, { keepAlive: promise });
      const reader = resultStream.getReader();
      await reader.read().catch(() => {});

      // Wait for drain loop to finish — should not hang on the unresolved promise
      await sleep(100);

      // Assert - drain loop completed without waiting for promise
      // (if it waited, this test would timeout)

      await sleep(100);
      process.off(`unhandledRejection`, rejectionHandler);
    });

    test(`should call onFlush after teardown when keepAlive is also provided`, async () => {
      // Arrange
      const [publisher, subscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      const streamId = `test-stream`;
      const chunks: Array<UIMessageChunk> = [
        { type: `start` },
        { type: `finish`, finishReason: `stop` },
      ];
      const stream = createStream(chunks);
      const { promise, resolve } = Promise.withResolvers<void>();
      const onFlush = vi.fn();

      const context = await createResumableUIMessageStream({
        streamId,
        publisher,
        subscriber,
        waitUntil,
      });

      // Act
      const resultStream = await context.startStream(stream, { keepAlive: promise, onFlush });
      await convertAsyncIterableToArray(resultStream);
      await sleep(25);

      // Assert - onFlush not called yet (waiting on keepAlive)
      expect(onFlush).toHaveBeenCalledTimes(0);

      resolve();
      await sleep(25);

      // Assert - onFlush called after keepAlive resolved and redis closed
      expect(onFlush).toHaveBeenCalledTimes(1);
    });
  });

  describe(`streamText`, () => {
    const modelChunks: Array<LanguageModelV3StreamPart> = [
      { type: `text-start`, id: `1` },
      { type: `text-delta`, id: `1`, delta: `Hello` },
      { type: `text-delta`, id: `1`, delta: ` ` },
      { type: `text-delta`, id: `1`, delta: `World` },
      { type: `text-end`, id: `1` },
      {
        type: `finish`,
        finishReason: { raw: `stop`, unified: `stop` },
        usage: {
          inputTokens: {
            total: 10,
            noCache: undefined,
            cacheRead: undefined,
            cacheWrite: undefined,
          },
          outputTokens: { total: 5, text: undefined, reasoning: undefined },
        },
      },
    ];

    test(`should resume an active stream from another client`, async () => {
      // Arrange
      const [publisher, subscriber, secondPublisher, secondSubscriber] = await Promise.all([
        createRedisClient().connect(),
        createRedisClient().connect(),
        createRedisClient().connect(),
        createRedisClient().connect(),
      ]);

      const streamId = `test-stream`;
      const abortController = new AbortController();

      // Start streaming in background
      const startPromise = (async () => {
        const context = await createResumableUIMessageStream({
          streamId,
          publisher,
          subscriber,
          abortController,
          waitUntil,
        });

        const model = createMockModel({ chunks: modelChunks, delay: 25 });
        const result = streamText({ model, prompt: `Generate text` });
        const uiStream = result.toUIMessageStream();
        return context.startStream(uiStream);
      })();

      // Wait for stream to start, then resume from new context
      await sleep(25);

      const resumeContext = await createResumableUIMessageStream({
        streamId,
        publisher: secondPublisher,
        subscriber: secondSubscriber,
        waitUntil,
      });

      // Act
      const resumedStream = await resumeContext.resumeStream();

      // Assert
      if (resumedStream === null) {
        throw new Error(`Expected resumedStream to not be null`);
      }

      const resumedChunks = await convertAsyncIterableToArray(resumedStream);
      const originalStream = await startPromise;
      const originalChunks = await convertAsyncIterableToArray(originalStream);

      expect(resumedChunks.length).toBe(originalChunks.length);
      expect(resumedChunks).toEqual(originalChunks);
    });

    test(`should stop an active stream from another client`, async () => {
      // Arrange
      const [firstPublisher, firstSubscriber, secondPublisher, secondSubscriber] =
        await Promise.all([
          createRedisClient().connect(),
          createRedisClient().connect(),
          createRedisClient().connect(),
          createRedisClient().connect(),
        ]);

      const streamId = `test-stream`;
      const abortController = new AbortController();
      let isAborted = false;

      // Start streaming in background (first client)
      const startPromise = (async () => {
        const context = await createResumableUIMessageStream({
          streamId,
          publisher: firstPublisher,
          subscriber: firstSubscriber,
          abortController,
          waitUntil,
        });

        // Create abort-aware model and pass abortSignal to streamText
        const model = createMockModel({
          chunks: modelChunks,
          delay: 25,
          abortSignal: abortController.signal,
        });
        const result = streamText({
          model,
          prompt: `Generate text`,
          abortSignal: abortController.signal,
          onAbort: () => {
            isAborted = true;
          },
        });
        const uiStream = result.toUIMessageStream();
        return context.startStream(uiStream);
      })();

      // Wait for stream to start
      await sleep(25);

      // Stop from second client
      const stopContext = await createResumableUIMessageStream({
        streamId,
        publisher: secondPublisher,
        subscriber: secondSubscriber,
        waitUntil,
      });

      // Act - stop from second client
      await stopContext.stopStream();
      await sleep(25);

      // Assert
      expect(abortController.signal.aborted).toBe(true);
      expect(isAborted).toBe(true);

      const originalStream = await startPromise;
      const originalChunks = await convertAsyncIterableToArray(originalStream);

      // Stream was aborted early, so fewer chunks than full output
      // Full stream: start + start-step + text-start + 3 text-deltas + text-end + finish-step + finish = 9+ chunks
      expect(originalChunks.length).toBeLessThan(9);

      // Last chunk should be abort chunk
      const lastChunk = originalChunks[originalChunks.length - 1];
      expect(lastChunk?.type).toBe(`abort`);
    });

    test(`should produce same final UIMessage from startStream and resumeStream via readUIMessageStream`, async () => {
      // Arrange
      const [firstPublisher, firstSubscriber, secondPublisher, secondSubscriber] =
        await Promise.all([
          createRedisClient().connect(),
          createRedisClient().connect(),
          createRedisClient().connect(),
          createRedisClient().connect(),
        ]);

      const streamId = `test-stream`;
      const abortController = new AbortController();

      // Start streaming from first client
      const startPromise = (async () => {
        const context = await createResumableUIMessageStream({
          streamId,
          publisher: firstPublisher,
          subscriber: firstSubscriber,
          abortController,
          waitUntil,
        });

        const model = createMockModel({ chunks: modelChunks, delay: 25 });
        const result = streamText({ model, prompt: `Generate text` });
        const uiStream = result.toUIMessageStream();
        return context.startStream(uiStream);
      })();

      // Wait for stream to start, then resume from second client
      await sleep(25);

      const resumeContext = await createResumableUIMessageStream({
        streamId,
        publisher: secondPublisher,
        subscriber: secondSubscriber,
        waitUntil,
      });

      // Act
      const resumedStream = await resumeContext.resumeStream();

      if (resumedStream === null) {
        throw new Error(`Expected resumedStream to not be null`);
      }

      // Consume resumed stream via readUIMessageStream and get final UIMessage
      const originalStream = await startPromise;
      const originalMessage = await consumeUIMessageStream(originalStream);
      const resumedMessage = await consumeUIMessageStream(resumedStream);

      // Assert
      expect(resumedMessage).toBeDefined();
      expect(originalMessage).toBeDefined();
      expect(resumedMessage).toEqual(originalMessage);
    });
  });
});
