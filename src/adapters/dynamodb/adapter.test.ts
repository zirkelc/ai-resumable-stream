import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createLocalDynamoDB, type LocalDynamoDB } from "../../__tests__/local-dynamodb.js";
import { createStreamAdapter } from "./adapter.js";
import { createDynamoOperations, type DynamoOperations, ItemExistsError } from "./client.js";
import { FORMAT_VERSION } from "./log.js";

/**
 * Every chunk is written on its own and every poll happens at once, so a test never waits
 * on an interval. The heartbeat window is collapsed too, since two tests turn on it.
 */
const FAST = {
  prefix: `test`,
  flushIntervalMs: 0,
  batchSize: 1,
  resumePollIntervalMs: 10,
  stopPollIntervalMs: 10,
  heartbeatMs: 40,
  deadAfterMs: 100,
};

/** The layout the adapter writes, restated so a test fails when the layout changes. */
const partition = (streamId: string, generationId?: string) =>
  generationId === undefined
    ? `${FAST.prefix}#${streamId}`
    : `${FAST.prefix}#${streamId}#${generationId}`;

const logSortKey = (sequence: number) => `LOG#${String(sequence).padStart(12, `0`)}`;

function createControlledSource() {
  let controller!: ReadableStreamDefaultController<string>;
  const stream = new ReadableStream<string>({
    start(streamController) {
      controller = streamController;
    },
  });

  return {
    stream,
    push: (chunk: string) => controller.enqueue(chunk),
    close: () => controller.close(),
  };
}

async function collect(stream: ReadableStream<string>): Promise<Array<string>> {
  const chunks: Array<string> = [];
  const reader = stream.getReader();
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
  }
  return chunks;
}

/**
 * Collects the work an adapter defers, so a test can wait for a producer to finish.
 */
function createWaitUntil() {
  const pending: Array<Promise<unknown>> = [];
  return {
    waitUntil: (promise: Promise<unknown>) => pending.push(promise),
    settled: () => Promise.all(pending),
  };
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe(`dynamodb adapter`, () => {
  let table: LocalDynamoDB;
  let dynamo: DynamoOperations;

  beforeAll(async () => {
    table = await createLocalDynamoDB();
    dynamo = createDynamoOperations({
      client: table.client,
      tableName: table.tableName,
      partitionKeyName: `pk`,
      sortKeyName: `sk`,
      ttlAttributeName: `expiresAt`,
      ttlSeconds: 60,
    });
  }, 30_000);

  afterAll(async () => {
    await table.stop();
  });

  /**
   * Writes the items of a producer directly, which is how a test gets a stream that
   * nobody is producing any more: a real producer keeps beating for as long as it drains.
   */
  async function writeAbandonedStream(streamId: string, chunks: Array<string>, writtenAt: number) {
    await dynamo.put(partition(streamId, `gen`), logSortKey(0), {
      version: FORMAT_VERSION,
      at: writtenAt,
    });
    await dynamo.put(partition(streamId, `gen`), logSortKey(1), { chunks, at: writtenAt });
    await dynamo.put(partition(streamId), `POINTER`, { generationId: `gen` });
  }

  test(`should refuse a death threshold that is too close to the beat`, () => {
    // Arrange
    const options = { heartbeatMs: 5_000, deadAfterMs: 6_000 };

    // Act
    const configure = () => createStreamAdapter(dynamo, options);

    // Assert
    expect(configure).toThrow();
    expect(() =>
      createStreamAdapter(dynamo, { heartbeatMs: 5_000, deadAfterMs: 30_000 }),
    ).not.toThrow();
  });

  test(`should refuse an empty generation id`, async () => {
    // Arrange
    const adapter = createStreamAdapter(dynamo, FAST);
    const source = createControlledSource();

    // Act
    const result = adapter.createStream({
      streamId: `empty-generation`,
      generationId: ``,
      chunks: source.stream,
    });

    // Assert
    await expect(result).rejects.toThrow();
  });

  test(`should refuse a generation id another producer already opened`, async () => {
    // Arrange
    const adapter = createStreamAdapter(dynamo, FAST);
    const first = createControlledSource();
    const second = createControlledSource();
    const { waitUntil, settled } = createWaitUntil();
    await adapter.createStream({
      streamId: `collision`,
      generationId: `gen-a`,
      chunks: first.stream,
      waitUntil,
    });

    // Act
    const result = adapter.createStream({
      streamId: `collision`,
      generationId: `gen-a`,
      chunks: second.stream,
      waitUntil,
    });

    // Assert
    await expect(result).rejects.toThrow(ItemExistsError);
    first.close();
    await settled();
  });

  test(`should write one item per flush rather than one per chunk`, async () => {
    // Arrange
    const adapter = createStreamAdapter(dynamo, { ...FAST, batchSize: 4, flushIntervalMs: 50 });
    const produced = [`a`, `b`, `c`, `d`];
    const source = createControlledSource();
    const { waitUntil, settled } = createWaitUntil();
    await adapter.createStream({
      streamId: `batched`,
      generationId: `gen`,
      chunks: source.stream,
      waitUntil,
    });

    // Act
    const before = table.counts().writes;
    produced.forEach(source.push);
    source.close();
    await settled();
    const written = table.counts().writes - before;

    // Assert
    /** One item for the four chunks, and one for the end. */
    expect(written).toBe(2);
  });

  test(`should cut a chunk that is too large for one item and give it back whole`, async () => {
    // Arrange
    const adapter = createStreamAdapter(dynamo, { ...FAST, maxItemBytes: 16 });
    const produced = [`x`.repeat(50), `small`];
    const source = createControlledSource();
    const { waitUntil, settled } = createWaitUntil();
    await adapter.createStream({
      streamId: `oversized`,
      generationId: `gen`,
      chunks: source.stream,
      waitUntil,
    });

    // Act
    const resumed = await adapter.resumeStream({ streamId: `oversized` });
    const collected = collect(resumed!);
    produced.forEach(source.push);
    source.close();
    await settled();

    // Assert
    expect(await collected).toEqual(produced);
  });

  test(`should keep a stream in the partitions of its id and its generation`, async () => {
    // Arrange
    const adapter = createStreamAdapter(dynamo, FAST);
    const source = createControlledSource();
    const { waitUntil, settled } = createWaitUntil();

    // Act
    await adapter.createStream({
      streamId: `layout`,
      generationId: `gen`,
      chunks: source.stream,
      waitUntil,
    });
    source.push(`only`);
    source.close();
    await settled();
    await adapter.requestStop({ streamId: `layout` });
    const keys = (await table.keys()).filter((key) => key.includes(`#layout`));

    // Assert
    expect(keys.sort()).toEqual([
      `${partition(`layout`, `gen`)}/${logSortKey(0)}`,
      `${partition(`layout`, `gen`)}/${logSortKey(1)}`,
      `${partition(`layout`, `gen`)}/${logSortKey(2)}`,
      `${partition(`layout`, `gen`)}/STOP`,
      `${partition(`layout`)}/POINTER`,
    ]);
  });

  test(`should return null for a stream whose producer went quiet`, async () => {
    // Arrange
    const adapter = createStreamAdapter(dynamo, FAST);
    await writeAbandonedStream(`abandoned`, [`orphan`], Date.now() - 10 * FAST.deadAfterMs);

    // Act
    const resumed = await adapter.resumeStream({ streamId: `abandoned` });

    // Assert
    expect(resumed).toBeNull();
  });

  test(`should end a resumed stream once its producer stops writing`, async () => {
    // Arrange
    const adapter = createStreamAdapter(dynamo, FAST);
    await writeAbandonedStream(`goes-quiet`, [`last`], Date.now());

    // Act
    const resumed = await adapter.resumeStream({ streamId: `goes-quiet` });
    const received = await collect(resumed!);

    // Assert
    expect(received).toEqual([`last`]);
  });

  test(`should refuse a stream written in a format it does not know`, async () => {
    // Arrange
    const adapter = createStreamAdapter(dynamo, FAST);
    await dynamo.put(partition(`from-the-future`, `gen`), logSortKey(0), {
      version: FORMAT_VERSION + 1,
      at: Date.now(),
    });
    await dynamo.put(partition(`from-the-future`), `POINTER`, { generationId: `gen` });

    // Act
    const result = adapter.resumeStream({ streamId: `from-the-future` });

    // Assert
    await expect(result).rejects.toThrow();
  });

  test(`should leave a stop request for a generation that has not started`, async () => {
    // Arrange
    const adapter = createStreamAdapter(dynamo, FAST);
    let stopped = false;

    // Act
    await adapter.requestStop({ streamId: `early-stop`, generationId: `gen` });
    const unsubscribe = await adapter.onStopRequested({
      streamId: `early-stop`,
      generationId: `gen`,
      onStop: () => {
        stopped = true;
      },
    });
    await sleep(100);
    unsubscribe();

    // Assert
    expect(stopped).toBe(true);
  });

  test(`should ignore a stop for a stream id that points at nothing`, async () => {
    // Arrange
    const adapter = createStreamAdapter(dynamo, FAST);

    // Act
    const result = adapter.requestStop({ streamId: `never-started` });

    // Assert
    await expect(result).resolves.toBeUndefined();
  });
});
