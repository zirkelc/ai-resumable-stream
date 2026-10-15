import { createClient } from "redis";
import { RedisMemoryServer } from "redis-memory-server";
import { afterAll, afterEach, beforeAll, describe, expect, test, vi } from "vitest";
import type { StreamCodec } from "../../adapter.js";
import { createResumableStream } from "../../core.js";
import { createRedisAdapter } from "./index.js";

/**
 * The adapter against a real server and the real `resumable-stream`, for the failures
 * that only show in the keys a producer leaves behind.
 */

const codec: StreamCodec<string> = {
  encode: (chunk) => chunk,
  decode: (data) => data,
};

let redisServer: RedisMemoryServer;
let redisUrl: string;
const redisClients: Array<{ isOpen: boolean; quit: () => Promise<unknown> }> = [];
let pendingPromises: Array<Promise<unknown>> = [];

/**
 * Rejections nobody handled while a test ran. `resumable-stream` reads its source without
 * a rejection handler, so a source that fails surfaces here, and in production it ends
 * the process.
 */
const unhandledRejections: Array<unknown> = [];
const onUnhandledRejection = (reason: unknown) => {
  unhandledRejections.push(reason);
};

function waitUntil(promise: Promise<unknown>): void {
  pendingPromises.push(promise);
}

function createClients() {
  const publisher = createClient({ url: redisUrl });
  const subscriber = createClient({ url: redisUrl });
  redisClients.push(publisher, subscriber);
  return { publisher, subscriber };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * A source that emits one value and then fails on the next read.
 */
function createFailingSource(value: string, error: Error) {
  let sent = false;
  return new ReadableStream<string>({
    pull(controller) {
      if (sent) {
        controller.error(error);
        return;
      }
      sent = true;
      controller.enqueue(value);
    },
  });
}

beforeAll(async () => {
  redisServer = await RedisMemoryServer.create();
  redisUrl = `redis://${await redisServer.getHost()}:${await redisServer.getPort()}`;
  process.on(`unhandledRejection`, onUnhandledRejection);
}, 60_000);

afterAll(async () => {
  process.off(`unhandledRejection`, onUnhandledRejection);
  await redisServer.stop();
}, 30_000);

/**
 * Every test in this file fails on a rejection nobody handled, not only the one that
 * names it, and also on one raised after the test body returned.
 */
afterEach(async () => {
  await Promise.race([Promise.allSettled(pendingPromises), sleep(200)]);
  pendingPromises = [];

  await Promise.all(
    redisClients.splice(0).map((client) => (client.isOpen ? client.quit() : undefined)),
  );

  expect(unhandledRejections.splice(0)).toEqual([]);
});

describe(`redis adapter with a failed source`, () => {
  test(`should not leave an unhandled rejection when the source fails`, async () => {
    // Arrange
    const { publisher, subscriber } = createClients();
    const adapter = createRedisAdapter({ publisher, subscriber });
    const context = createResumableStream({ adapter, codec, waitUntil });
    const source = createFailingSource(`a`, new Error(`source failed`));

    // Act
    const { stream } = await context.startStream(source, {
      streamId: `chat-1`,
      generationId: `turn-1`,
    });
    await expect(stream.pipeTo(new WritableStream())).rejects.toThrow();

    /** Settles once the producer has marked the generation done. */
    await Promise.all(pendingPromises);

    // Assert
    expect(unhandledRejections).toEqual([]);
  });

  test(`should mark the generation finished when the source fails`, async () => {
    // Arrange
    const { publisher, subscriber } = createClients();
    const adapter = createRedisAdapter({ publisher, subscriber });
    const context = createResumableStream({ adapter, codec, waitUntil });
    const source = createFailingSource(`a`, new Error(`source failed`));

    // Act
    const { stream } = await context.startStream(source, {
      streamId: `chat-2`,
      generationId: `turn-1`,
    });
    await expect(stream.pipeTo(new WritableStream())).rejects.toThrow();

    // Assert
    await vi.waitFor(async () => {
      expect(await publisher.get(`ai-resumable-stream:rs:sentinel:chat-2:turn-1`)).toBe(`DONE`);
    });
    expect(await publisher.get(`ai-resumable-stream:generation:chat-2`)).toBeNull();
  });

  test(`should return null when resuming a generation whose producer died`, async () => {
    // Arrange
    /**
     * What a producer that died mid-stream leaves behind: the pointer still names the
     * generation, the sentinel still says it is running, and nobody answers on its
     * request channel.
     */
    const { publisher, subscriber } = createClients();
    await publisher.connect();
    await publisher.set(`ai-resumable-stream:generation:chat-3`, `chat-3:turn-1`);
    await publisher.set(`ai-resumable-stream:rs:sentinel:chat-3:turn-1`, `1`);
    const adapter = createRedisAdapter({ publisher, subscriber });

    // Act
    const resumed = await adapter.resumeStream({ streamId: `chat-3` });

    // Assert
    expect(resumed).toBeNull();
  });
});
