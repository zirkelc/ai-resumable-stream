/**
 * Runs with `bun test`, not with Vitest, because the client built into Bun exists only in
 * the Bun runtime. It drives every Redis command the adapter relies on through a real
 * `Bun.RedisClient`.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { RedisClient } from "bun";
import type { UIMessageChunk } from "ai";
import { Streams } from "ai-test-kit";
import { UIChunks } from "ai-test-kit/ui";
import { convertAsyncIterableToArray } from "ai-stream-utils/utils";
import { RedisMemoryServer } from "redis-memory-server";
import { createResumableUIMessageStream } from "../../ai-sdk/index.js";
import type { BunRedis } from "./client.js";
import { createRedisAdapter } from "./index.js";

/**
 * Fails to compile if the Bun client is missing any command or property the library needs.
 */
type Options = Parameters<typeof createRedisAdapter>[0];
type Conforms<CLIENT extends BunRedis & Options[`publisher`] & Options[`subscriber`]> = CLIENT;
type BunRedisClient = Conforms<RedisClient>;

let redisServer: RedisMemoryServer;
let redisUrl: string;
const redisClients: Array<BunRedisClient> = [];
let pendingPromises: Array<Promise<unknown>> = [];

function waitUntil(promise: Promise<unknown>): void {
  pendingPromises.push(promise);
}

function createClients({ autoReconnect = false }: { autoReconnect?: boolean } = {}) {
  const publisher: BunRedisClient = new RedisClient(redisUrl, { autoReconnect });
  const subscriber: BunRedisClient = new RedisClient(redisUrl, { autoReconnect });
  redisClients.push(publisher, subscriber);
  return { publisher, subscriber };
}

const WAIT_TIMEOUT_MS = 3_000;

async function waitFor(condition: () => boolean | Promise<boolean>) {
  const deadline = Date.now() + WAIT_TIMEOUT_MS;
  while (!(await condition())) {
    if (Date.now() > deadline)
      throw new Error(`The condition was not met within ${WAIT_TIMEOUT_MS} ms`);
    await Bun.sleep(10);
  }
}

/**
 * The server closes every subscribed connection, as a failover or an idle kill does. Waits
 * until the server has all the channels that it had before, because a message published
 * while a reconnected client has not subscribed yet reaches no one, with any client.
 */
async function dropSubscriber({
  publisher,
  subscriber,
}: {
  publisher: BunRedisClient;
  subscriber: BunRedisClient;
}) {
  const channels = async () =>
    ((await publisher.send(`PUBSUB`, [`CHANNELS`])) as Array<string>).toSorted();
  const before = await channels();
  await publisher.send(`CLIENT`, [`KILL`, `TYPE`, `pubsub`]);
  await waitFor(() => !subscriber.connected);
  await waitFor(async () => (await channels()).join() === before.join());
}

beforeAll(async () => {
  redisServer = new RedisMemoryServer();
  redisUrl = `redis://${await redisServer.getHost()}:${await redisServer.getPort()}`;
}, 60_000);

afterAll(async () => {
  await redisServer.stop();
}, 30_000);

afterEach(async () => {
  // Let the producer finish writing before the clients go away
  await Promise.allSettled(pendingPromises);
  pendingPromises = [];
  for (const client of redisClients.splice(0)) client.close();
});

const chunks: Array<UIMessageChunk> = [
  UIChunks.start(),
  UIChunks.textStart({ id: `1` }),
  UIChunks.textDelta({ id: `1`, delta: `Hello` }),
  UIChunks.textEnd({ id: `1` }),
  UIChunks.finish({ finishReason: `stop` }),
];

describe(`redis adapter with Bun.RedisClient`, () => {
  test(`should start and resume a stream`, async () => {
    // Arrange
    const { publisher, subscriber } = createClients();
    const streamId = `bun-compat-resume`;
    const context = createResumableUIMessageStream({
      adapter: createRedisAdapter({ publisher, subscriber }),
      waitUntil,
    });

    // Act
    const { stream } = await context.startStream(
      Streams.simulate(chunks, { initialDelayInMs: 25, chunkDelayInMs: 25 }),
      { streamId },
    );
    const resumed = await context.resumeStream({ streamId });
    const [startedChunks, resumedChunks] = await Promise.all([
      convertAsyncIterableToArray(stream),
      resumed ? convertAsyncIterableToArray(resumed) : Promise.resolve([]),
    ]);

    // Assert
    expect(publisher.connected).toBe(true);
    expect(subscriber.connected).toBe(true);
    expect(startedChunks).toEqual(chunks);
    expect(resumedChunks).toEqual(chunks);
  });

  test(`should retire the pointer once the stream ends`, async () => {
    // Arrange
    const { publisher, subscriber } = createClients();
    const streamId = `bun-compat-pointer`;
    const context = createResumableUIMessageStream({
      adapter: createRedisAdapter({ publisher, subscriber }),
      waitUntil,
    });
    const { stream } = await context.startStream(Streams.simulate(chunks), { streamId });

    // Act
    await convertAsyncIterableToArray(stream);
    await Promise.allSettled(pendingPromises);

    // Assert
    expect(await publisher.get(`ai-resumable-stream:generation:${streamId}`)).toBe(null);
    expect(await context.resumeStream({ streamId })).toBe(null);
  });

  test(`should stop a stream`, async () => {
    // Arrange
    const { publisher, subscriber } = createClients();
    const streamId = `bun-compat-stop`;
    const abortController = new AbortController();
    const context = createResumableUIMessageStream({
      adapter: createRedisAdapter({ publisher, subscriber }),
      waitUntil,
    });
    const { stream } = await context.startStream(
      Streams.simulate(chunks, { initialDelayInMs: 50, chunkDelayInMs: 50 }),
      { streamId, abortController },
    );
    const consumed = convertAsyncIterableToArray(stream);

    // Act
    await context.stopStream({ streamId });
    await consumed;

    // Assert
    expect(abortController.signal.aborted).toBe(true);
  });

  test(`should stop a stream after the subscriber reconnects`, async () => {
    // Arrange
    const clients = createClients({ autoReconnect: true });
    const streamId = `bun-compat-reconnect-stop`;
    const abortController = new AbortController();
    const context = createResumableUIMessageStream({
      adapter: createRedisAdapter(clients),
      waitUntil,
    });
    const { stream } = await context.startStream(
      Streams.simulate(chunks, { initialDelayInMs: 500, chunkDelayInMs: 50 }),
      { streamId, abortController },
    );
    const consumed = convertAsyncIterableToArray(stream);
    await dropSubscriber(clients);

    // Act
    await context.stopStream({ streamId });

    // Assert
    await waitFor(() => abortController.signal.aborted);
    await consumed;
  });

  test(`should resume a stream after the subscriber reconnects`, async () => {
    // Arrange
    const clients = createClients({ autoReconnect: true });
    const streamId = `bun-compat-reconnect-resume`;
    const context = createResumableUIMessageStream({
      adapter: createRedisAdapter(clients),
      waitUntil,
    });
    const { stream } = await context.startStream(
      Streams.simulate(chunks, { initialDelayInMs: 500, chunkDelayInMs: 50 }),
      { streamId },
    );
    const consumed = convertAsyncIterableToArray(stream);
    await dropSubscriber(clients);

    // Act
    const resumed = await context.resumeStream({ streamId });
    const resumedChunks = resumed ? await convertAsyncIterableToArray(resumed) : null;

    // Assert
    expect(resumedChunks).toEqual(chunks);
    await consumed;
  });

  test(`should stop a stream after a reconnect of a client that was connected before`, async () => {
    // Arrange
    const clients = createClients({ autoReconnect: true });
    await Promise.all([clients.publisher.connect(), clients.subscriber.connect()]);
    const streamId = `bun-compat-reconnect-connected`;
    const abortController = new AbortController();
    const context = createResumableUIMessageStream({
      adapter: createRedisAdapter(clients),
      waitUntil,
    });
    const { stream } = await context.startStream(
      Streams.simulate(chunks, { initialDelayInMs: 500, chunkDelayInMs: 50 }),
      { streamId, abortController },
    );
    const consumed = convertAsyncIterableToArray(stream);
    await dropSubscriber(clients);

    // Act
    await context.stopStream({ streamId });

    // Assert
    await waitFor(() => abortController.signal.aborted);
    await consumed;
  });
});
