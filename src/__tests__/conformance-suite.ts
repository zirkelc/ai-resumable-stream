import type { UIMessageChunk } from "ai";
import { UIChunks } from "ai-test-kit/ui";
import { afterAll, afterEach, beforeAll, describe, expect, test, vi } from "vitest";
import type { StreamAdapter } from "../adapter.js";
import { createResumableUIMessageStream } from "../ai-sdk/index.js";

/**
 * An adapter whose store cannot push polls instead, so the intervals are collapsed to
 * keep the suite quick. Shared so every adapter is measured the same way.
 */
export const FAST_POLLING = {
  flushIntervalMs: 0,
  batchSize: 1,
  resumePollIntervalMs: 10,
  stopPollIntervalMs: 10,
};

export type Harness = {
  name: string;
  createAdapter: () => Promise<StreamAdapter>;
  setup?: () => Promise<void>;
  teardown?: () => Promise<void>;
  /** Per-test cleanup, e.g. destroying the clients an adapter opened. */
  afterEach?: () => Promise<void>;
};

/**
 * A source whose chunks are pushed by the test rather than produced on a timer.
 */
export function createControlledSource() {
  let controller!: ReadableStreamDefaultController<UIMessageChunk>;
  const stream = new ReadableStream<UIMessageChunk>({
    start(streamController) {
      controller = streamController;
    },
  });

  return {
    stream,
    push: (chunk: UIMessageChunk) => controller.enqueue(chunk),
    close: () => controller.close(),
  };
}

export async function collect(
  stream: AsyncIterable<UIMessageChunk>,
): Promise<Array<UIMessageChunk>> {
  const chunks: Array<UIMessageChunk> = [];
  for await (const chunk of stream) chunks.push(chunk);
  return chunks;
}

/**
 * The behaviour every adapter must share, regardless of backend. Called once per harness
 * so the published adapters and the example adapters are held to the same contract.
 */
export function defineConformanceTests(harness: Harness) {
  describe(`${harness.name} adapter`, () => {
    beforeAll(async () => {
      await harness.setup?.();
    }, 120_000);

    afterAll(async () => {
      await harness.teardown?.();
    });

    /**
     * The work the adapters defer. A producer keeps writing after the test that started
     * it has returned, so the harness must not take its connection away mid-write.
     */
    let deferred: Array<Promise<unknown>> = [];

    afterEach(async () => {
      /**
       * Bounded, because a few tests deliberately leave a producer running to prove that
       * persistence outlives its client. Those are meant never to settle.
       */
      await Promise.race([
        Promise.allSettled(deferred),
        new Promise((resolve) => setTimeout(resolve, 500)),
      ]);
      deferred = [];

      await harness.afterEach?.();
    });

    async function createContext() {
      return createResumableUIMessageStream({
        adapter: await harness.createAdapter(),
        waitUntil: (promise) => {
          deferred.push(promise);
        },
      });
    }

    test(`should stream every chunk to the client that started it`, async () => {
      // Arrange
      const context = await createContext();
      const chunks = [
        UIChunks.textStart({ id: `1` }),
        UIChunks.textDelta({ id: `1`, delta: `hello` }),
      ];
      const source = createControlledSource();

      // Act
      const { stream } = await context.startStream(source.stream, { streamId: `stream-1` });
      chunks.forEach(source.push);
      source.close();
      const received = await collect(stream);

      // Assert
      expect(received).toEqual(chunks);
    });

    test(`should replay produced chunks and then tail the live ones on resume`, async () => {
      // Arrange
      const context = await createContext();
      const produced = [
        UIChunks.textStart({ id: `1` }),
        UIChunks.textDelta({ id: `1`, delta: `he` }),
        UIChunks.textDelta({ id: `1`, delta: `llo` }),
      ];
      const source = createControlledSource();
      const { stream } = await context.startStream(source.stream, { streamId: `stream-2` });

      // Act
      source.push(produced[0]!);
      source.push(produced[1]!);
      const resumed = await vi.waitFor(
        async () => {
          const candidate = await context.resumeStream({ streamId: `stream-2` });
          expect(candidate).not.toBeNull();
          return candidate!;
        },
        { timeout: 5_000 },
      );

      const collected = collect(resumed);
      source.push(produced[2]!);
      source.close();
      await collect(stream);

      // Assert
      expect(await collected).toEqual(produced);
    });

    test(`should serve several resumers from one producer at the same time`, async () => {
      // Arrange
      const context = await createContext();
      const produced = [
        UIChunks.textStart({ id: `1` }),
        UIChunks.textDelta({ id: `1`, delta: `he` }),
        UIChunks.textDelta({ id: `1`, delta: `llo` }),
      ];
      const source = createControlledSource();
      const { stream } = await context.startStream(source.stream, { streamId: `stream-10` });

      // Act
      source.push(produced[0]!);
      source.push(produced[1]!);

      /** Every reader attaches while the producer is still running. */
      const resumed = await Promise.all(
        Array.from({ length: 3 }, () =>
          vi.waitFor(
            async () => {
              const candidate = await context.resumeStream({ streamId: `stream-10` });
              expect(candidate).not.toBeNull();
              return candidate!;
            },
            { timeout: 5_000 },
          ),
        ),
      );

      const collected = resumed.map(collect);
      source.push(produced[2]!);
      source.close();
      await collect(stream);

      // Assert
      for (const chunks of collected) {
        expect(await chunks).toEqual(produced);
      }
    });

    test(`should return null when resuming a finished stream`, async () => {
      // Arrange
      const context = await createContext();
      const source = createControlledSource();
      const { stream } = await context.startStream(source.stream, { streamId: `stream-3` });
      source.push(UIChunks.textStart({ id: `1` }));
      source.close();
      await collect(stream);

      // Act
      const resumed = await vi.waitFor(
        async () => {
          const candidate = await context.resumeStream({ streamId: `stream-3` });
          expect(candidate).toBeNull();
          return candidate;
        },
        { timeout: 5_000 },
      );

      // Assert
      expect(resumed).toBeNull();
    });

    test(`should return null when resuming an unknown stream`, async () => {
      // Arrange
      const context = await createContext();

      // Act
      const resumed = await context.resumeStream({ streamId: `stream-does-not-exist` });

      // Assert
      expect(resumed).toBeNull();
    });

    test(`should keep persisting after the client disconnects`, async () => {
      // Arrange
      const context = await createContext();
      const produced = [
        UIChunks.textStart({ id: `1` }),
        UIChunks.textDelta({ id: `1`, delta: `he` }),
        UIChunks.textDelta({ id: `1`, delta: `llo` }),
      ];
      const source = createControlledSource();
      const { stream } = await context.startStream(source.stream, { streamId: `stream-4` });

      // Act
      source.push(produced[0]!);
      await stream.cancel();

      source.push(produced[1]!);
      const resumed = await vi.waitFor(
        async () => {
          const candidate = await context.resumeStream({ streamId: `stream-4` });
          expect(candidate).not.toBeNull();
          return candidate!;
        },
        { timeout: 5_000 },
      );

      const collected = collect(resumed);
      source.push(produced[2]!);
      source.close();

      // Assert
      expect(await collected).toEqual(produced);
    });

    test(`should discard the chunks of a previous stream with the same id`, async () => {
      // Arrange
      const context = await createContext();
      const first = createControlledSource();
      const { stream: firstStream } = await context.startStream(first.stream, {
        streamId: `stream-5`,
      });
      first.push(UIChunks.textDelta({ id: `1`, delta: `stale` }));
      first.close();
      await collect(firstStream);

      // Act
      const second = createControlledSource();
      const fresh = UIChunks.textDelta({ id: `2`, delta: `fresh` });
      const { stream: secondStream } = await context.startStream(second.stream, {
        streamId: `stream-5`,
      });
      second.push(fresh);

      const resumed = await vi.waitFor(
        async () => {
          const candidate = await context.resumeStream({ streamId: `stream-5` });
          expect(candidate).not.toBeNull();
          return candidate!;
        },
        { timeout: 5_000 },
      );

      const collected = collect(resumed);
      second.close();
      await collect(secondStream);

      // Assert
      expect(await collected).toEqual([fresh]);
    });

    test(`should not let a finished stream tear down the one that reused its id`, async () => {
      // Arrange
      const context = await createContext();
      const first = createControlledSource();
      const { stream: firstStream } = await context.startStream(first.stream, {
        streamId: `stream-9`,
      });
      first.push(UIChunks.textDelta({ id: `1`, delta: `stale` }));

      // Act
      /**
       * The second stream starts while the first is still tearing down, which is what a
       * client that immediately retries a chat looks like.
       */
      first.close();
      const second = createControlledSource();
      const fresh = UIChunks.textDelta({ id: `2`, delta: `fresh` });
      const { stream: secondStream } = await context.startStream(second.stream, {
        streamId: `stream-9`,
      });
      second.push(fresh);

      const resumed = await vi.waitFor(
        async () => {
          const candidate = await context.resumeStream({ streamId: `stream-9` });
          expect(candidate).not.toBeNull();
          return candidate!;
        },
        { timeout: 5_000 },
      );

      const collected = collect(resumed);
      second.close();
      await Promise.all([collect(firstStream), collect(secondStream)]);

      // Assert
      expect(await collected).toEqual([fresh]);
    });

    describe(`generations`, () => {
      test(`should resume a specific older generation while a newer one is current`, async () => {
        // Arrange
        const context = await createContext();
        const older = createControlledSource();
        const newer = createControlledSource();
        const olderChunks = [
          UIChunks.textDelta({ id: `1`, delta: `old-1` }),
          UIChunks.textDelta({ id: `1`, delta: `old-2` }),
        ];
        const { stream: olderStream } = await context.startStream(older.stream, {
          streamId: `stream-16`,
          generationId: `generation-a`,
        });
        older.push(olderChunks[0]!);
        const { stream: newerStream } = await context.startStream(newer.stream, {
          streamId: `stream-16`,
          generationId: `generation-b`,
        });
        newer.push(UIChunks.textDelta({ id: `2`, delta: `new-1` }));

        // Act
        const resumed = await vi.waitFor(
          async () => {
            const candidate = await context.resumeStream({
              streamId: `stream-16`,
              generationId: `generation-a`,
            });
            expect(candidate).not.toBeNull();
            return candidate!;
          },
          { timeout: 5_000 },
        );
        const collected = collect(resumed);
        older.push(olderChunks[1]!);
        older.close();
        newer.close();
        await Promise.all([collect(olderStream), collect(newerStream)]);

        // Assert
        expect(await collected).toEqual(olderChunks);
      });

      test(`should resume the newest generation by stream id`, async () => {
        // Arrange
        const context = await createContext();
        const older = createControlledSource();
        const newer = createControlledSource();
        const newerChunk = UIChunks.textDelta({ id: `2`, delta: `new-1` });
        const { stream: olderStream } = await context.startStream(older.stream, {
          streamId: `stream-17`,
          generationId: `generation-a`,
        });
        older.push(UIChunks.textDelta({ id: `1`, delta: `old-1` }));
        const { stream: newerStream } = await context.startStream(newer.stream, {
          streamId: `stream-17`,
          generationId: `generation-b`,
        });
        newer.push(newerChunk);

        // Act
        const resumed = await vi.waitFor(
          async () => {
            const candidate = await context.resumeStream({ streamId: `stream-17` });
            expect(candidate).not.toBeNull();
            return candidate!;
          },
          { timeout: 5_000 },
        );
        const collected = collect(resumed);
        newer.close();
        older.close();
        await Promise.all([collect(olderStream), collect(newerStream)]);

        // Assert
        expect(await collected).toEqual([newerChunk]);
      });
    });

    describe(`stopStream`, () => {
      test(`should stop a stream whose source is a plain ReadableStream`, async () => {
        // Arrange
        const context = await createContext();
        const source = createControlledSource();
        const { stream } = await context.startStream(source.stream, { streamId: `stream-6` });
        source.push(UIChunks.textStart({ id: `1` }));

        // Act
        await context.stopStream({ streamId: `stream-6` });
        const received = await collect(stream);

        // Assert
        expect(received.length).toBe(1);
      });

      test(`should abort a caller-supplied controller`, async () => {
        // Arrange
        const context = await createContext();
        const abortController = new AbortController();

        /** How a producer reacts to the signal it handed to `streamText`. */
        const source = new ReadableStream<UIMessageChunk>({
          start(controller) {
            controller.enqueue(UIChunks.textStart({ id: `1` }));
            abortController.signal.addEventListener(`abort`, () => controller.close(), {
              once: true,
            });
          },
        });

        const { stream } = await context.startStream(source, {
          streamId: `stream-7`,
          abortController,
        });

        // Act
        await context.stopStream({ streamId: `stream-7` });
        const received = await collect(stream);

        // Assert
        expect(abortController.signal.aborted).toBe(true);
        expect(received.length).toBe(1);
      });

      test(`should terminate a resumed stream when the producer is stopped`, async () => {
        // Arrange
        const context = await createContext();
        const source = createControlledSource();
        const { stream } = await context.startStream(source.stream, { streamId: `stream-8` });
        source.push(UIChunks.textStart({ id: `1` }));

        const resumed = await vi.waitFor(
          async () => {
            const candidate = await context.resumeStream({ streamId: `stream-8` });
            expect(candidate).not.toBeNull();
            return candidate!;
          },
          { timeout: 5_000 },
        );

        // Act
        const collected = collect(resumed);
        await context.stopStream({ streamId: `stream-8` });
        await collect(stream);

        // Assert
        expect((await collected).length).toBe(1);
      });

      test(`should return the caller-supplied generation id`, async () => {
        // Arrange
        const context = await createContext();
        const source = createControlledSource();

        // Act
        const result = await context.startStream(source.stream, {
          streamId: `stream-11`,
          generationId: `generation-a`,
        });
        source.close();
        await collect(result.stream);

        // Assert
        expect(result.generationId).toBe(`generation-a`);
      });

      test(`should not abort a concurrent generation of the same stream id`, async () => {
        // Arrange
        const context = await createContext();
        const first = createControlledSource();
        const second = createControlledSource();
        const firstController = new AbortController();
        const secondController = new AbortController();
        const { stream: firstStream } = await context.startStream(first.stream, {
          streamId: `stream-12`,
          generationId: `generation-a`,
          abortController: firstController,
        });
        const { stream: secondStream } = await context.startStream(second.stream, {
          streamId: `stream-12`,
          generationId: `generation-b`,
          abortController: secondController,
        });

        // Act
        await context.stopStream({ streamId: `stream-12`, generationId: `generation-a` });
        await vi.waitFor(() => expect(firstController.signal.aborted).toBe(true), {
          timeout: 5_000,
        });

        /** Long enough for a misrouted stop to arrive by push or by poll. */
        await new Promise((resolve) => setTimeout(resolve, 100));
        const secondAborted = secondController.signal.aborted;
        second.close();
        await Promise.all([collect(firstStream), collect(secondStream)]);

        // Assert
        expect(secondAborted).toBe(false);
      });

      test(`should abort a generation that was stopped before it started`, async () => {
        // Arrange
        const context = await createContext();
        const abortController = new AbortController();
        const source = createControlledSource();

        // Act
        await context.stopStream({ streamId: `stream-13`, generationId: `generation-a` });
        const { stream } = await context.startStream(source.stream, {
          streamId: `stream-13`,
          generationId: `generation-a`,
          abortController,
        });
        await vi.waitFor(() => expect(abortController.signal.aborted).toBe(true), {
          timeout: 5_000,
        });
        await collect(stream);

        // Assert
        expect(abortController.signal.aborted).toBe(true);
      });

      test(`should stop the current generation when no generation id is given`, async () => {
        // Arrange
        const context = await createContext();
        const first = createControlledSource();
        const second = createControlledSource();
        const firstController = new AbortController();
        const secondController = new AbortController();
        const { stream: firstStream } = await context.startStream(first.stream, {
          streamId: `stream-14`,
          abortController: firstController,
        });
        const { stream: secondStream } = await context.startStream(second.stream, {
          streamId: `stream-14`,
          abortController: secondController,
        });

        // Act
        await context.stopStream({ streamId: `stream-14` });
        await vi.waitFor(() => expect(secondController.signal.aborted).toBe(true), {
          timeout: 5_000,
        });
        await new Promise((resolve) => setTimeout(resolve, 100));
        const firstAborted = firstController.signal.aborted;
        first.close();
        await Promise.all([collect(firstStream), collect(secondStream)]);

        // Assert
        expect(firstAborted).toBe(false);
      });

      test(`should keep listening for another generation after one generation ends`, async () => {
        // Arrange
        const context = await createContext();
        const first = createControlledSource();
        const second = createControlledSource();
        const secondController = new AbortController();
        const firstFinished = vi.fn();
        const { stream: firstStream } = await context.startStream(first.stream, {
          streamId: `stream-15`,
          generationId: `generation-a`,
          onFinish: firstFinished,
        });
        const { stream: secondStream } = await context.startStream(second.stream, {
          streamId: `stream-15`,
          generationId: `generation-b`,
          abortController: secondController,
        });

        /** Lets the second generation's listener register before the first one tears down. */
        await new Promise((resolve) => setTimeout(resolve, 100));
        first.close();
        await collect(firstStream);
        await vi.waitFor(() => expect(firstFinished).toHaveBeenCalled(), { timeout: 5_000 });

        // Act
        await context.stopStream({ streamId: `stream-15`, generationId: `generation-b` });
        await vi.waitFor(() => expect(secondController.signal.aborted).toBe(true), {
          timeout: 5_000,
        });
        await collect(secondStream);

        // Assert
        expect(secondController.signal.aborted).toBe(true);
      });
    });
  });
}
