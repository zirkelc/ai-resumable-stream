import { createClient } from "redis";
import { RedisMemoryServer } from "redis-memory-server";
import { afterAll, afterEach, beforeAll, describe, expect, onTestFinished, test, vi } from "vitest";
import {
  AckTimeoutError,
  createRuntime,
  type CreateRuntimeOptions,
  type Publisher,
  type Subscriber,
} from "./runtime.js";

/**
 * The runtime against a real server. Faults are injected at the client, the boundary the
 * runtime talks to, so every test runs the real protocol.
 */

/**
 * The structural type of a client, because the generic defaults of the redis client type
 * are not assignable to the concrete client that `createClient` gives.
 */
type RedisClient = Publisher & Subscriber & { quit(): Promise<unknown> };

let redisServer: RedisMemoryServer;
let redisUrl: string;
const redisClients: Array<RedisClient> = [];

/**
 * Rejections nobody handled while a test ran. In production, each one ends the process.
 */
const unhandledRejections: Array<unknown> = [];
const onUnhandledRejection = (reason: unknown) => {
  unhandledRejections.push(reason);
};

beforeAll(async () => {
  redisServer = await RedisMemoryServer.create();
  redisUrl = `redis://${await redisServer.getHost()}:${await redisServer.getPort()}`;
  process.on(`unhandledRejection`, onUnhandledRejection);
}, 60_000);

afterAll(async () => {
  process.off(`unhandledRejection`, onUnhandledRejection);
  await redisServer.stop();
}, 30_000);

afterEach(async () => {
  vi.restoreAllMocks();
  /** Gives a rejection raised after the test body time to surface */
  await sleep(50);
  await Promise.all(redisClients.splice(0).map((client) => client.quit()));
  expect(unhandledRejections.splice(0)).toEqual([]);
});

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function connectClients() {
  const publisher = createClient({ url: redisUrl });
  const subscriber = createClient({ url: redisUrl });
  await Promise.all([publisher.connect(), subscriber.connect()]);
  const clients: { publisher: RedisClient; subscriber: RedisClient } = { publisher, subscriber };
  redisClients.push(clients.publisher, clients.subscriber);
  return clients;
}

function createPublisher(client: RedisClient, overrides: Partial<Publisher> = {}): Publisher {
  return {
    publish: (channel, message) => client.publish(channel, message),
    set: (key, value, options) => client.set(key, value, options),
    get: (key) => client.get(key),
    ...overrides,
  };
}

function createSubscriber(client: RedisClient, overrides: Partial<Subscriber> = {}): Subscriber {
  return {
    subscribe: (channel, listener) => client.subscribe(channel, listener),
    unsubscribe: (channel, listener) => client.unsubscribe(channel, listener),
    ...overrides,
  };
}

/**
 * Two runtimes on the same server, so the producer and the consumer can get different
 * faults. Each test has its own prefix, so no test sees the keys of another.
 */
async function setup(
  options: {
    producer?: { publisher?: Partial<Publisher>; subscriber?: Partial<Subscriber> } & Partial<
      Pick<CreateRuntimeOptions, `heartbeatIntervalMs`>
    >;
    consumer?: { publisher?: Partial<Publisher>; subscriber?: Partial<Subscriber> } & Partial<
      Pick<CreateRuntimeOptions, `ackTimeoutMs` | `doneWatchdogIntervalMs`>
    >;
  } = {},
) {
  const clients = await connectClients();
  const keyPrefix = `test-${crypto.randomUUID()}`;
  const finished: Array<Promise<unknown>> = [];

  const producer = createRuntime({
    keyPrefix,
    publisher: createPublisher(clients.publisher, options.producer?.publisher),
    subscriber: createSubscriber(clients.subscriber, options.producer?.subscriber),
    waitUntil: (promise) => finished.push(promise),
    expireAfterMs: 60_000,
    heartbeatIntervalMs: options.producer?.heartbeatIntervalMs,
  });
  const consumer = createRuntime({
    keyPrefix,
    publisher: createPublisher(clients.publisher, options.consumer?.publisher),
    subscriber: createSubscriber(clients.subscriber, options.consumer?.subscriber),
    expireAfterMs: 60_000,
    ackTimeoutMs: options.consumer?.ackTimeoutMs,
    doneWatchdogIntervalMs: options.consumer?.doneWatchdogIntervalMs,
  });

  return {
    ...clients,
    producer,
    consumer,
    sentinelKey: (streamId: string) => `${keyPrefix}:rs:sentinel:${streamId}`,
    requestChannel: (streamId: string) => `${keyPrefix}:rs:request:${streamId}`,
    /** Settles once every producer has finished its teardown */
    finished: () => Promise.all(finished),
  };
}

/**
 * A source that the test writes to.
 */
function createSource() {
  let controller!: ReadableStreamDefaultController<string>;
  const stream = new ReadableStream<string>({
    start(c) {
      controller = c;
    },
  });
  return { stream, controller };
}

async function readText(stream: ReadableStream<string>): Promise<string> {
  const reader = stream.getReader();
  let text = ``;
  while (true) {
    const { done, value } = await reader.read();
    if (done) return text;
    text += value;
  }
}

describe(`createNewResumableStream`, () => {
  test(`should resume a running stream with the past and the live chunks`, async () => {
    // Arrange
    const { producer, consumer } = await setup();
    const source = createSource();
    const stream = await producer.createNewResumableStream(`s1`, () => source.stream);
    source.controller.enqueue(`a`);
    await sleep(20);

    // Act
    const resumed = await consumer.resumeExistingStream(`s1`);
    source.controller.enqueue(`b`);
    source.controller.close();

    // Assert
    expect(await readText(stream)).toBe(`ab`);
    expect(await readText(resumed!)).toBe(`ab`);
  });

  test(`should resume a finished stream as null and an unknown stream as undefined`, async () => {
    // Arrange
    const { producer, consumer, finished } = await setup();
    const stream = await producer.createNewResumableStream(
      `s1`,
      () =>
        new ReadableStream({
          start(controller) {
            controller.enqueue(`a`);
            controller.close();
          },
        }),
    );
    await readText(stream);
    await finished();

    // Act
    const done = await consumer.resumeExistingStream(`s1`);
    const unknown = await consumer.resumeExistingStream(`s2`);

    // Assert
    expect(done).toBeNull();
    expect(unknown).toBeUndefined();
  });

  /** https://github.com/vercel/resumable-stream/issues/53 */
  test(`should error the stream and mark it done when the source fails`, async () => {
    // Arrange
    const { producer, publisher, sentinelKey, finished } = await setup();
    const source = createSource();
    const stream = await producer.createNewResumableStream(`s1`, () => source.stream);

    // Act
    source.controller.enqueue(`a`);
    source.controller.error(new Error(`source failed`));
    const result = readText(stream);

    // Assert
    await expect(result).rejects.toThrow();
    await finished();
    expect(await publisher.get(sentinelKey(`s1`))).toBe(`DONE`);
  });

  /** https://github.com/vercel/resumable-stream/issues/53 */
  test(`should finish the teardown when marking the stream done fails`, async () => {
    // Arrange
    const { producer, finished } = await setup({
      producer: {
        publisher: {
          set: async (_key, value) => {
            if (value === `DONE`) throw new Error(`Socket closed unexpectedly`);
            return `OK`;
          },
        },
      },
    });
    const source = createSource();
    const stream = await producer.createNewResumableStream(`s1`, () => source.stream);

    // Act
    source.controller.enqueue(`a`);
    source.controller.close();
    const text = await readText(stream);

    // Assert
    expect(text).toBe(`a`);
    await finished();
  });

  /** https://github.com/vercel/resumable-stream/issues/53 */
  test(`should keep reading the source when a publish to a consumer fails`, async () => {
    // Arrange
    const { producer, consumer, finished } = await setup({
      producer: {
        publisher: {
          publish: async () => {
            throw new Error(`Socket closed unexpectedly`);
          },
        },
      },
      consumer: { ackTimeoutMs: 100 },
    });
    const source = createSource();
    const stream = await producer.createNewResumableStream(`s1`, () => source.stream);
    const resumed = consumer.resumeExistingStream(`s1`);

    // Act
    source.controller.enqueue(`a`);
    source.controller.enqueue(`b`);
    source.controller.close();

    // Assert
    expect(await readText(stream)).toBe(`ab`);
    await finished();
    /** No answer arrived, and by the end of the wait the stream is done */
    expect(await resumed).toBeNull();
  });

  /** https://github.com/vercel/resumable-stream/issues/52 */
  test(`should not wait for the publishes of a chunk before it reads the next one`, async () => {
    // Arrange
    let release!: () => void;
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });
    /**
     * From chunk b on, every publish waits for the release. They still reach the server in
     * the order they were called, as on one real connection.
     */
    let gate: Promise<void> | undefined;
    const {
      producer,
      consumer,
      publisher: client,
    } = await setup({
      producer: {
        publisher: {
          publish: (channel, message) => {
            if (message === `b`) gate ??= released;
            if (!gate) return client.publish(channel, message);
            return gate.then(() => client.publish(channel, message));
          },
        },
      },
    });
    const source = createSource();
    const stream = await producer.createNewResumableStream(`s1`, () => source.stream);
    source.controller.enqueue(`a`);
    await sleep(20);
    const resumed = await consumer.resumeExistingStream(`s1`);

    // Act
    source.controller.enqueue(`b`);
    source.controller.enqueue(`c`);
    source.controller.close();

    /** Ends while the publishes of b and c still wait */
    const text = await readText(stream);
    release();

    // Assert
    expect(text).toBe(`abc`);
    expect(await readText(resumed!)).toBe(`abc`);
  });

  test(`should ignore a resume request that is not JSON`, async () => {
    // Arrange
    const { producer, consumer, publisher, requestChannel } = await setup();
    const source = createSource();
    const stream = await producer.createNewResumableStream(`s1`, () => source.stream);
    source.controller.enqueue(`a`);

    // Act
    await publisher.publish(requestChannel(`s1`), `not json`);
    const resumed = await consumer.resumeExistingStream(`s1`);
    source.controller.close();

    // Assert
    expect(await readText(stream)).toBe(`a`);
    expect(await readText(resumed!)).toBe(`a`);
  });

  test(`should not leave an unhandled rejection when the answer to a resume fails`, async () => {
    // Arrange
    const { producer, consumer } = await setup({
      producer: {
        publisher: {
          publish: async () => {
            throw new Error(`Socket closed unexpectedly`);
          },
        },
      },
      consumer: { ackTimeoutMs: 100 },
    });
    const source = createSource();
    await producer.createNewResumableStream(`s1`, () => source.stream);
    source.controller.enqueue(`a`);

    // Act
    const resumed = consumer.resumeExistingStream(`s1`);

    // Assert
    await expect(resumed).rejects.toThrow(AckTimeoutError);
    source.controller.close();
  });
});

describe(`resumeExistingStream`, () => {
  test(`should reject with an AckTimeoutError when the producer does not answer`, async () => {
    // Arrange
    const { consumer, publisher, sentinelKey } = await setup({ consumer: { ackTimeoutMs: 50 } });
    await publisher.set(sentinelKey(`s1`), `1`);

    // Act
    const resumed = consumer.resumeExistingStream(`s1`);

    // Assert
    await expect(resumed).rejects.toThrow(AckTimeoutError);
  });

  /** https://github.com/vercel/resumable-stream/issues/50 */
  test(`should reject with an AckTimeoutError when the clock does not move`, async () => {
    // Arrange
    const { consumer, publisher, sentinelKey } = await setup({ consumer: { ackTimeoutMs: 50 } });
    await publisher.set(sentinelKey(`s1`), `1`);
    vi.spyOn(Date, `now`).mockReturnValue(1_000_000);

    // Act
    const resumed = consumer.resumeExistingStream(`s1`);

    // Assert
    await expect(resumed).rejects.toThrow(AckTimeoutError);
  });

  /** https://github.com/vercel/resumable-stream/issues/50 */
  test(`should resolve null when the stream is done by the time the wait ends`, async () => {
    // Arrange
    const { consumer, publisher, sentinelKey } = await setup({ consumer: { ackTimeoutMs: 50 } });
    await publisher.set(sentinelKey(`s1`), `1`);

    // Act
    const resumed = consumer.resumeExistingStream(`s1`);
    await publisher.set(sentinelKey(`s1`), `DONE`);

    // Assert
    expect(await resumed).toBeNull();
  });

  /** https://github.com/vercel/resumable-stream/issues/50 */
  test(`should reject with an AckTimeoutError when the sentinel read fails after the wait`, async () => {
    // Arrange
    let reads = 0;
    const {
      consumer,
      publisher: client,
      sentinelKey,
    } = await setup({
      consumer: {
        ackTimeoutMs: 50,
        publisher: {
          get: async (key) => {
            reads += 1;
            if (reads > 1) throw new Error(`Socket closed unexpectedly`);
            return client.get(key);
          },
        },
      },
    });
    await client.set(sentinelKey(`s1`), `1`);

    // Act
    const resumed = consumer.resumeExistingStream(`s1`);

    // Assert
    await expect(resumed).rejects.toThrow(AckTimeoutError);
  });

  /** https://github.com/vercel/resumable-stream/issues/42 */
  test(`should resume a producer that answers within the configured wait`, async () => {
    // Arrange
    const {
      producer,
      consumer,
      publisher: client,
    } = await setup({
      producer: {
        publisher: {
          publish: async (channel, message) => {
            await sleep(200);
            return client.publish(channel, message);
          },
        },
      },
      consumer: { ackTimeoutMs: 2_000 },
    });
    const source = createSource();
    await producer.createNewResumableStream(`s1`, () => source.stream);
    source.controller.enqueue(`a`);

    // Act
    const resumed = await consumer.resumeExistingStream(`s1`);
    source.controller.close();

    // Assert
    expect(await readText(resumed!)).toBe(`a`);
  });

  test(`should keep the sentinel of a running stream alive with a heartbeat`, async () => {
    // Arrange
    const { producer, publisher, sentinelKey } = await setup({
      producer: { heartbeatIntervalMs: 50 },
    });
    const source = createSource();
    await producer.createNewResumableStream(`s1`, () => source.stream);
    onTestFinished(() => source.controller.close());

    // Act
    /** Five times the lifetime of the sentinel without a renewal */
    await sleep(750);

    // Assert
    expect(await publisher.get(sentinelKey(`s1`))).toBe(`1`);
  });

  test(`should let the stream of a producer that died expire`, async () => {
    // Arrange
    let dead = false;
    const {
      producer,
      consumer,
      publisher: client,
      sentinelKey,
    } = await setup({
      producer: {
        heartbeatIntervalMs: 50,
        publisher: {
          /** A process that died sends nothing more */
          set: (key, value, options) =>
            dead ? new Promise(() => {}) : client.set(key, value, options),
          publish: (channel, message) =>
            dead ? new Promise(() => {}) : client.publish(channel, message),
        },
      },
      consumer: { doneWatchdogIntervalMs: 50 },
    });
    const source = createSource();
    await producer.createNewResumableStream(`s1`, () => source.stream);
    onTestFinished(() => source.controller.close());
    source.controller.enqueue(`a`);
    await sleep(20);
    const attached = await consumer.resumeExistingStream(`s1`);

    // Act
    dead = true;
    const text = await readText(attached!);
    const later = await consumer.resumeExistingStream(`s1`);

    // Assert
    expect(text).toBe(`a`);
    expect(await client.get(sentinelKey(`s1`))).toBeNull();
    expect(later).toBeUndefined();
  });

  test(`should keep the DONE sentinel after the heartbeat stopped`, async () => {
    // Arrange
    const { producer, publisher, sentinelKey, finished } = await setup({
      producer: { heartbeatIntervalMs: 50 },
    });
    const source = createSource();
    const stream = await producer.createNewResumableStream(`s1`, () => source.stream);
    await sleep(120);

    // Act
    source.controller.close();
    await readText(stream);
    await finished();
    await sleep(200);

    // Assert
    expect(await publisher.get(sentinelKey(`s1`))).toBe(`DONE`);
  });

  /** https://github.com/vercel/resumable-stream/pull/47 */
  test(`should close the stream when the DONE message is lost`, async () => {
    // Arrange
    const {
      producer,
      consumer,
      publisher: client,
    } = await setup({
      producer: {
        publisher: {
          publish: async (channel, message) => {
            if (message.includes(`DONE_SENTINEL`)) return 0;
            return client.publish(channel, message);
          },
        },
      },
      consumer: { doneWatchdogIntervalMs: 25 },
    });
    const source = createSource();
    await producer.createNewResumableStream(`s1`, () => source.stream);
    source.controller.enqueue(`a`);
    await sleep(20);
    const resumed = await consumer.resumeExistingStream(`s1`);

    // Act
    source.controller.close();

    // Assert
    expect(await readText(resumed!)).toBe(`a`);
  });

  test(`should not leave an unhandled rejection when the unsubscribe fails`, async () => {
    // Arrange
    const { producer, consumer } = await setup({
      consumer: {
        subscriber: {
          unsubscribe: async () => {
            throw new Error(`Socket closed unexpectedly`);
          },
        },
      },
    });
    const source = createSource();
    await producer.createNewResumableStream(`s1`, () => source.stream);
    source.controller.enqueue(`a`);
    await sleep(20);
    const resumed = await consumer.resumeExistingStream(`s1`);

    // Act
    source.controller.close();

    // Assert
    expect(await readText(resumed!)).toBe(`a`);
  });
});
