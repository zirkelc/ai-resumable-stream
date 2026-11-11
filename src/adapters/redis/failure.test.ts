import { createServer, connect as connectSocket, type Socket } from "node:net";
import { createClient } from "redis";
import { RedisMemoryServer } from "redis-memory-server";
import { createClient as createClientV5 } from "redis-v5";
import { createClient as createClientV6 } from "redis-v6";
import { afterAll, afterEach, beforeAll, describe, expect, onTestFinished, test, vi } from "vitest";
import type { StreamCodec } from "../../adapter.js";
import { createResumableStream } from "../../core.js";
import { createRedisAdapter } from "./index.js";

/**
 * The adapter against a real server and the real runtime, for the failures
 * that only show in the keys a producer leaves behind.
 */

const codec: StreamCodec<string> = {
  encode: (chunk) => chunk,
  decode: (data) => data,
};

let redisServer: RedisMemoryServer;
let redisHost: string;
let redisPort: number;
let redisUrl: string;
const redisClients: Array<{ isOpen: boolean; quit: () => Promise<unknown> }> = [];
let pendingPromises: Array<Promise<unknown>> = [];

/**
 * Rejections nobody handled while a test ran. In production, each one ends the process.
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

/**
 * A client of either redis version, with what a test needs to wait for its reconnect.
 */
type DroppableClient = Parameters<typeof createRedisAdapter>[0][`publisher`] & {
  isReady: boolean;
  on(event: `error`, listener: (error: Error) => void): unknown;
  quit(): Promise<unknown>;
};

/**
 * A TCP proxy in front of the server that drops every connection as soon as a client
 * sends a command that matches `dropOn`. That command never reaches the server, so it is
 * still waiting for its reply when the sockets close.
 *
 * The sockets close with a FIN, as when a managed server drops its clients. The client
 * then fails the commands in flight with "Socket closed unexpectedly". A hard reset would
 * give ECONNRESET instead. The proxy drops once, so the clients can reconnect after.
 */
async function createDroppingProxy({ dropOn }: { dropOn: RegExp }) {
  const sockets = new Set<Socket>();
  let armed = true;
  let onDropped!: () => void;
  const dropped = new Promise<void>((resolve) => {
    onDropped = resolve;
  });

  const server = createServer((client) => {
    const upstream = connectSocket(redisPort, redisHost);
    for (const socket of [client, upstream]) {
      sockets.add(socket);
      socket.on(`error`, () => {
        /** The other side is closed with it */
      });
      socket.on(`close`, () => sockets.delete(socket));
    }
    client.on(`close`, () => upstream.destroy());
    upstream.on(`close`, () => client.destroy());

    /** Commands can arrive split across reads, so the match runs on the recent bytes */
    let recent = ``;
    client.on(`data`, (data) => {
      recent = (recent + data.toString(`latin1`)).slice(-1_024);
      if (armed && dropOn.test(recent)) {
        armed = false;
        for (const socket of sockets) socket.end();
        onDropped();
        return;
      }
      upstream.write(data);
    });
    upstream.on(`data`, (data) => client.write(data));
  });

  await new Promise<void>((resolve) => server.listen(0, `127.0.0.1`, resolve));
  const address = server.address();
  if (!address || typeof address === `string`) throw new Error(`proxy has no port`);

  return {
    url: `redis://127.0.0.1:${address.port}`,
    dropped,
    close: () =>
      new Promise<void>((resolve) => {
        for (const socket of sockets) socket.destroy();
        server.close(() => resolve());
      }),
  };
}

beforeAll(async () => {
  redisServer = await RedisMemoryServer.create();
  redisHost = await redisServer.getHost();
  redisPort = await redisServer.getPort();
  redisUrl = `redis://${redisHost}:${redisPort}`;
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

  test(`should give up on a producer that died after ackTimeoutMs`, async () => {
    // Arrange
    const { publisher, subscriber } = createClients();
    await publisher.connect();
    await publisher.set(`ai-resumable-stream:generation:chat-5`, `chat-5:turn-1`);
    await publisher.set(`ai-resumable-stream:rs:sentinel:chat-5:turn-1`, `1`);
    const adapter = createRedisAdapter({ publisher, subscriber, ackTimeoutMs: 50 });
    const start = performance.now();

    // Act
    const resumed = await adapter.resumeStream({ streamId: `chat-5` });

    // Assert
    expect(resumed).toBeNull();
    /** The default wait is 1000 ms */
    expect(performance.now() - start).toBeLessThan(500);
  });
});

describe(`redis adapter ttlSeconds`, () => {
  test(`should forget a stream after ttlSeconds, even while its producer runs`, async () => {
    // Arrange
    const { publisher, subscriber } = createClients();
    const adapter = createRedisAdapter({ publisher, subscriber, ttlSeconds: 1 });
    const context = createResumableStream({ adapter, codec, waitUntil });
    let source!: ReadableStreamDefaultController<string>;
    const { stream } = await context.startStream(
      new ReadableStream<string>({
        start(controller) {
          source = controller;
          controller.enqueue(`a`);
        },
      }),
      { streamId: `chat-6`, generationId: `turn-1` },
    );
    const drained = stream.pipeTo(new WritableStream());
    onTestFinished(async () => {
      source.close();
      await drained;
    });

    // Act
    await sleep(1_200);
    const start = performance.now();
    const resumed = await adapter.resumeStream({ streamId: `chat-6` });

    // Assert
    expect(resumed).toBeNull();
    /** The pointer expired, so the resume does not wait for an answer */
    expect(performance.now() - start).toBeLessThan(500);
    expect(await publisher.get(`ai-resumable-stream:generation:chat-6`)).toBeNull();
    expect(await publisher.get(`ai-resumable-stream:rs:sentinel:chat-6:turn-1`)).toBeNull();
  });
});

describe.each([
  { version: `v5`, createClient: (url: string): DroppableClient => createClientV5({ url }) },
  { version: `v6`, createClient: (url: string): DroppableClient => createClientV6({ url }) },
])(
  `redis adapter when redis $version drops the connections during teardown`,
  ({ createClient }) => {
    /**
     * What happened in production: the source ended, and the server dropped every connection
     * while the producer's teardown was marking the generation done.
     */
    test(`should not leave an unhandled rejection when the sentinel write fails`, async () => {
      // Arrange
      const proxy = await createDroppingProxy({
        dropOn: /SET\r\n\$\d+\r\n[^\r]*:rs:sentinel:chat-4:turn-1\r\n\$4\r\nDONE\r\n/,
      });
      onTestFinished(() => proxy.close());

      const publisher = createClient(proxy.url);
      const subscriber = createClient(proxy.url);
      for (const client of [publisher, subscriber]) {
        /** Without a listener, the client's error event would throw on its own */
        client.on(`error`, () => {});
        redisClients.push(client);
      }
      const adapter = createRedisAdapter({ publisher, subscriber });
      const context = createResumableStream({ adapter, codec, waitUntil });
      const source = new ReadableStream<string>({
        start(controller) {
          controller.enqueue(`a`);
          controller.close();
        },
      });

      // Act
      const { stream } = await context.startStream(source, {
        streamId: `chat-4`,
        generationId: `turn-1`,
      });
      await stream.pipeTo(new WritableStream());
      await proxy.dropped;

      /** Gives a rejection of the teardown time to surface, and the clients to come back */
      await vi.waitFor(() => {
        expect(publisher.isReady && subscriber.isReady).toBe(true);
      });
      await sleep(100);

      // Assert
      expect(unhandledRejections).toEqual([]);
      expect(await adapter.resumeStream({ streamId: `chat-4` })).toBeNull();
    });
  },
);
