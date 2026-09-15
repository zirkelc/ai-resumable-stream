import { describe, expect, test, vi } from "vitest";
import { createFakeS3, type FakeS3 } from "../../__tests__/fake-s3.js";
import type { S3Operations } from "./client.js";
import { WriteOffsetMismatchError } from "./client.js";
import { createStreamAdapter } from "./adapter.js";
import { decodeRecords, encodeChunk, encodeVersion, joinRecords, RecordType } from "./log.js";

/**
 * Every chunk is written on its own and every poll happens at once, so a test never waits
 * on an interval. The heartbeat window is collapsed too, since two tests turn on it.
 */
const FAST = {
  prefix: `test`,
  flushIntervalMs: 0,
  batchSize: 1,
  pollIntervalMs: 10,
  stopPollIntervalMs: 10,
  heartbeatMs: 40,
  deadAfterMs: 100,
};

const encoder = new TextEncoder();

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
function createContext() {
  const pending: Array<Promise<unknown>> = [];
  return {
    context: { waitUntil: (promise: Promise<unknown>) => pending.push(promise) },
    settled: () => Promise.all(pending),
  };
}

function pointerKey(streamId: string) {
  return `${FAST.prefix}/${streamId}/current`;
}

function segmentKeys(s3: FakeS3, streamId: string): Array<string> {
  return s3
    .keys()
    .filter((key) => key.startsWith(`${FAST.prefix}/${streamId}/`) && /\/\d+$/.test(key));
}

function chunksIn(s3: FakeS3, key: string): Array<string> {
  const { records } = decodeRecords(s3.body(key) ?? new Uint8Array());
  return records
    .filter((record) => record.type === RecordType.CHUNK)
    .map((record) => (record.type === RecordType.CHUNK ? record.data : ``));
}

/**
 * Writes the state of a live producer directly, which is how a test gets a stream that
 * nobody is producing any more: a real producer keeps beating for as long as it drains.
 */
async function writeAbandonedStream(s3: FakeS3, streamId: string, chunks: Array<string>) {
  await s3.put(
    `${FAST.prefix}/${streamId}/gen/0`,
    joinRecords([encodeVersion(), ...chunks.map((chunk) => encodeChunk(chunk))]),
  );
  await s3.put(pointerKey(streamId), encoder.encode(JSON.stringify({ generationId: `gen` })));
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe(`s3-express adapter`, () => {
  test(`should refuse a death threshold that is too close to the beat`, () => {
    // Arrange
    const s3 = createFakeS3();

    // Act
    const configure = () => createStreamAdapter(s3, { heartbeatMs: 5_000, deadAfterMs: 6_000 });

    // Assert
    expect(configure).toThrow();
    expect(() =>
      createStreamAdapter(s3, { heartbeatMs: 5_000, deadAfterMs: 30_000 }),
    ).not.toThrow();
  });

  test(`should continue the log in another object once a segment runs out of parts`, async () => {
    // Arrange
    const s3 = createFakeS3({ partLimit: 20 });
    const adapter = createStreamAdapter(s3, { ...FAST, maxPartsPerSegment: 3 });
    const produced = Array.from({ length: 10 }, (_, index) => `chunk-${index}`);
    const source = createControlledSource();
    const { context, settled } = createContext();

    // Act
    await adapter.createStream(`rolling`, source.stream, context);
    for (const chunk of produced) source.push(chunk);

    const resumed = await vi.waitFor(
      async () => {
        const candidate = await adapter.resumeStream(`rolling`);
        expect(candidate).not.toBeNull();
        return candidate!;
      },
      { timeout: 5_000 },
    );

    const collected = collect(resumed);
    source.close();
    await settled();

    // Assert
    expect(await collected).toEqual(produced);
    expect(segmentKeys(s3, `rolling`).length).toBeGreaterThan(1);
  });

  test(`should keep an append whose response was lost`, async () => {
    // Arrange
    const s3 = createFakeS3();
    let appends = 0;
    /** The write lands, and only the answer goes missing. */
    const lossy: S3Operations = {
      ...s3,
      async append(key, offset, body) {
        appends += 1;
        await s3.append(key, offset, body);
        if (appends === 2) throw new WriteOffsetMismatchError(key);
      },
    };
    const adapter = createStreamAdapter(lossy, FAST);
    const produced = [`one`, `two`, `three`];
    const source = createControlledSource();
    const { context, settled } = createContext();

    // Act
    await adapter.createStream(`lossy`, source.stream, context);
    for (const chunk of produced) source.push(chunk);

    const resumed = await vi.waitFor(
      async () => {
        const candidate = await adapter.resumeStream(`lossy`);
        expect(candidate).not.toBeNull();
        return candidate!;
      },
      { timeout: 5_000 },
    );

    const collected = collect(resumed);
    source.close();
    await settled();

    // Assert
    expect(await collected).toEqual(produced);
  });

  test(`should send an append again when it never landed`, async () => {
    // Arrange
    const s3 = createFakeS3();
    let appends = 0;
    /** The write is refused outright, so the object is left as it was. */
    const flaky: S3Operations = {
      ...s3,
      async append(key, offset, body) {
        appends += 1;
        if (appends === 2) throw new WriteOffsetMismatchError(key);
        await s3.append(key, offset, body);
      },
    };
    const adapter = createStreamAdapter(flaky, FAST);
    const produced = [`one`, `two`, `three`];
    const source = createControlledSource();
    const { context, settled } = createContext();

    // Act
    await adapter.createStream(`flaky`, source.stream, context);
    for (const chunk of produced) source.push(chunk);

    const resumed = await vi.waitFor(
      async () => {
        const candidate = await adapter.resumeStream(`flaky`);
        expect(candidate).not.toBeNull();
        return candidate!;
      },
      { timeout: 5_000 },
    );

    const collected = collect(resumed);
    source.close();
    await settled();

    // Assert
    expect(await collected).toEqual(produced);
  });

  test(`should keep a superseded producer out of the generation that replaced it`, async () => {
    // Arrange
    const s3 = createFakeS3();
    const adapter = createStreamAdapter(s3, FAST);
    const first = createControlledSource();
    const second = createControlledSource();
    const { context, settled } = createContext();

    await adapter.createStream(`reused`, first.stream, context);
    first.push(`stale`);
    await vi.waitFor(() => expect(segmentKeys(s3, `reused`).length).toBe(1));
    const [staleSegment] = segmentKeys(s3, `reused`);

    // Act
    await adapter.createStream(`reused`, second.stream, context);
    second.push(`fresh`);

    /** The first producer shuts down well after the second one took the id. */
    first.close();
    second.close();
    await settled();

    // Assert
    const freshSegment = segmentKeys(s3, `reused`).find((key) => key !== staleSegment);
    expect(chunksIn(s3, staleSegment!)).toEqual([`stale`]);
    expect(chunksIn(s3, freshSegment!)).toEqual([`fresh`]);
  });

  test(`should end a resumed stream once the producer stops writing`, async () => {
    // Arrange
    const s3 = createFakeS3();
    const adapter = createStreamAdapter(s3, FAST);
    await writeAbandonedStream(s3, `abandoned`, [`only`]);

    // Act
    const resumed = await adapter.resumeStream(`abandoned`);
    const collected = await collect(resumed!);

    // Assert
    expect(collected).toEqual([`only`]);
  });

  test(`should report a stream whose producer died before the resume as gone`, async () => {
    // Arrange
    const s3 = createFakeS3();
    const adapter = createStreamAdapter(s3, FAST);
    await writeAbandonedStream(s3, `long-gone`, [`only`]);

    // Act
    await sleep(FAST.deadAfterMs + 50);
    const resumed = await adapter.resumeStream(`long-gone`);

    // Assert
    expect(resumed).toBeNull();
  });

  test(`should replay the whole backlog in a single read`, async () => {
    // Arrange
    const s3 = createFakeS3();
    const adapter = createStreamAdapter(s3, FAST);
    await writeAbandonedStream(s3, `backlog`, [`one`, `two`, `three`]);
    const before = s3.counts().reads;

    // Act
    const resumed = await adapter.resumeStream(`backlog`);

    // Assert
    /** The pointer, then every chunk written so far. */
    expect(s3.counts().reads - before).toBe(2);
    await resumed?.cancel();
  });
});
