import type { UIMessageChunk } from "ai";
import { Streams } from "ai-test-kit";
import { UIChunks } from "ai-test-kit/ui";
import { convertAsyncIterableToArray } from "ai-stream-utils/utils";
import { RedisMemoryServer } from "redis-memory-server";
import { createClient as createClientV5 } from "redis-v5";
import { createClient as createClientV6 } from "redis-v6";
import { afterAll, afterEach, beforeAll, describe, expect, test, vi } from "vitest";
import { createResumableUIMessageStream } from "./resumable-ui-message-stream.js";

/**
 * The peer range allows both redis v5 and v6, whose client types are not mutually
 * assignable (v6 pins RESP3 into the client generics). This file asserts that both
 * versions still satisfy the structural client type: at compile time via the
 * conformance checks below, and at runtime by driving every Redis command the library
 * relies on (`isOpen`, `connect`, `set`/`get`/`incr` through resumable-stream,
 * `subscribe`, `unsubscribe`, and `publish`).
 */
type Options = Parameters<typeof createResumableUIMessageStream>[0];
type RedisClient = Options[`publisher`] & Options[`subscriber`];

/**
 * Fails to compile if the client is missing any command or property the library needs.
 */
type Conforms<CLIENT extends RedisClient> = CLIENT;
type RedisV5Client = Conforms<ReturnType<typeof createClientV5>>;
type RedisV6Client = Conforms<ReturnType<typeof createClientV6>>;

let redisServer: RedisMemoryServer;
let redisUrl: string;
const redisClients: Array<{ isOpen: boolean; quit: () => Promise<unknown> }> = [];
let pendingPromises: Array<Promise<unknown>> = [];

/**
 * Collects the background work the library defers, so a test can drain it before
 * disconnecting the clients the producer is still writing to.
 */
function waitUntil(promise: Promise<unknown>): void {
  pendingPromises.push(promise);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

afterEach(async () => {
  /** Let the producer finish writing before the clients go away */
  await Promise.race([Promise.allSettled(pendingPromises), sleep(200)]);
  await sleep(25);
  pendingPromises = [];

  await Promise.all(
    redisClients.splice(0).map((client) => (client.isOpen ? client.quit() : Promise.resolve())),
  );
});

const chunks: Array<UIMessageChunk> = [
  UIChunks.start(),
  UIChunks.textStart({ id: `1` }),
  UIChunks.textDelta({ id: `1`, delta: `Hello` }),
  UIChunks.textEnd({ id: `1` }),
  UIChunks.finish({ finishReason: `stop` }),
];

/**
 * Starts a stream and resumes it, asserting both carry every chunk. Clients are passed
 * disconnected, so this also covers `isOpen` and the on-demand `connect`. The parameter
 * types are the library's own, so every call site doubles as an assignability assertion.
 */
async function assertResumableStream(publisher: RedisClient, subscriber: RedisClient) {
  // Arrange
  const streamId = `redis-compat-resume`;

  expect(publisher.isOpen).toBe(false);
  expect(subscriber.isOpen).toBe(false);

  const context = await createResumableUIMessageStream({
    streamId,
    publisher,
    subscriber,
    waitUntil,
  });

  // Act - start a stream and resume it while it is still active
  const stream = await context.startStream(
    Streams.simulate(chunks, { initialDelayInMs: 25, chunkDelayInMs: 25 }),
  );
  const resumed = await context.resumeStream();
  const [startedChunks, resumedChunks] = await Promise.all([
    convertAsyncIterableToArray(stream),
    resumed ? convertAsyncIterableToArray(resumed) : Promise.resolve([]),
  ]);

  // Assert - the library connected both clients, and both streams carry every chunk
  expect(publisher.isOpen).toBe(true);
  expect(subscriber.isOpen).toBe(true);
  expect(startedChunks).toEqual(chunks);
  expect(resumedChunks).toEqual(chunks);
}

/**
 * Stops an active stream, asserting the pub/sub commands (`subscribe`, `publish`, and
 * `unsubscribe` on teardown) work on this client version.
 */
async function assertStoppableStream(publisher: RedisClient, subscriber: RedisClient) {
  // Arrange
  const streamId = `redis-compat-stop`;
  const abortController = new AbortController();
  const subscribeSpy = vi.spyOn(subscriber, `subscribe`);
  const unsubscribeSpy = vi.spyOn(subscriber, `unsubscribe`);
  const publishSpy = vi.spyOn(publisher, `publish`);

  const context = await createResumableUIMessageStream({
    streamId,
    publisher,
    subscriber,
    abortController,
    waitUntil,
  });

  // Act - publish a stop message on the stop channel
  const stream = await context.startStream(
    Streams.simulate(chunks, { initialDelayInMs: 50, chunkDelayInMs: 50 }),
  );
  const consumed = convertAsyncIterableToArray(stream);
  await context.stopStream();

  // Assert - the subscriber received the stop message and aborted the stream
  await vi.waitFor(() => expect(abortController.signal.aborted).toBe(true));
  expect(subscribeSpy).toHaveBeenCalled();
  expect(publishSpy).toHaveBeenCalled();
  await vi.waitFor(() => expect(unsubscribeSpy).toHaveBeenCalled());

  await consumed;
}

describe(`redis v5`, () => {
  test(`should start and resume a stream`, async () => {
    const publisher: RedisV5Client = createClientV5({ url: redisUrl });
    const subscriber: RedisV5Client = createClientV5({ url: redisUrl });
    redisClients.push(publisher, subscriber);

    await assertResumableStream(publisher, subscriber);
  });

  test(`should stop a stream`, async () => {
    const publisher: RedisV5Client = createClientV5({ url: redisUrl });
    const subscriber: RedisV5Client = createClientV5({ url: redisUrl });
    redisClients.push(publisher, subscriber);

    await assertStoppableStream(publisher, subscriber);
  });
});

describe(`redis v6`, () => {
  test(`should start and resume a stream`, async () => {
    const publisher: RedisV6Client = createClientV6({ url: redisUrl });
    const subscriber: RedisV6Client = createClientV6({ url: redisUrl });
    redisClients.push(publisher, subscriber);

    await assertResumableStream(publisher, subscriber);
  });

  test(`should stop a stream`, async () => {
    const publisher: RedisV6Client = createClientV6({ url: redisUrl });
    const subscriber: RedisV6Client = createClientV6({ url: redisUrl });
    redisClients.push(publisher, subscriber);

    await assertStoppableStream(publisher, subscriber);
  });
});
