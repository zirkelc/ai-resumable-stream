import { describe, expect, test, vi } from "vitest";
import type { StreamAdapter, StreamCodec } from "./adapter.js";
import { createFakeS3 } from "./__tests__/fake-s3.js";
import { FAST_POLLING } from "./__tests__/conformance-suite.js";
import { createStreamAdapter } from "./adapters/s3-express/adapter.js";
import { createResumableStream } from "./core.js";

const codec: StreamCodec<string> = {
  encode: (chunk) => chunk,
  decode: (data) => data,
};

/**
 * A working adapter whose stop subscription a test can replace.
 */
function createAdapter(overrides: Partial<StreamAdapter> = {}): StreamAdapter {
  return { ...createStreamAdapter(createFakeS3(), FAST_POLLING), ...overrides };
}

function createSource(chunks: Array<string>) {
  return new ReadableStream<string>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
}

async function collect(stream: AsyncIterable<string>): Promise<Array<string>> {
  const chunks: Array<string> = [];
  for await (const chunk of stream) chunks.push(chunk);
  return chunks;
}

/**
 * A promise the test settles by hand.
 */
function createDeferred<VALUE>() {
  let resolve!: (value: VALUE) => void;
  const promise = new Promise<VALUE>((settle) => {
    resolve = settle;
  });
  return { promise, resolve };
}

describe(`startStream`, () => {
  test(`should stream when the stop subscription rejects`, async () => {
    // Arrange
    const onStopSubscriptionError = vi.fn();
    const error = new Error(`subscribe failed`);
    const adapter = createAdapter({ onStopRequested: () => Promise.reject(error) });
    const context = createResumableStream({ adapter, codec });

    // Act
    const { stream } = await context.startStream(createSource([`a`, `b`]), {
      onStopSubscriptionError,
    });
    const received = await collect(stream);

    // Assert
    expect(received).toEqual([`a`, `b`]);
    await vi.waitFor(() => expect(onStopSubscriptionError.mock.calls[0]).toEqual([error]));
  });

  test(`should stream when the stop subscription never resolves`, async () => {
    // Arrange
    const adapter = createAdapter({ onStopRequested: () => new Promise(() => {}) });
    const context = createResumableStream({ adapter, codec });

    // Act
    const { stream } = await context.startStream(createSource([`a`, `b`]));
    const received = await collect(stream);

    // Assert
    expect(received).toEqual([`a`, `b`]);
  });

  test(`should remove a stop subscription that resolves after the source ended`, async () => {
    // Arrange
    const unsubscribe = vi.fn();
    const subscription = createDeferred<() => void>();
    const onFinish = vi.fn();
    const adapter = createAdapter({ onStopRequested: () => subscription.promise });
    const context = createResumableStream({ adapter, codec });
    const { stream } = await context.startStream(createSource([`a`]), { onFinish });
    await collect(stream);
    await vi.waitFor(() => expect(onFinish).toHaveBeenCalled());

    // Act
    subscription.resolve(unsubscribe);

    // Assert
    await vi.waitFor(() => expect(unsubscribe.mock.calls.length).toBe(1));
  });

  test(`should remove the stop subscription once the source ends`, async () => {
    // Arrange
    const unsubscribe = vi.fn();
    const adapter = createAdapter({ onStopRequested: async () => unsubscribe });
    const context = createResumableStream({ adapter, codec });

    /** Stays open until the subscription has certainly resolved. */
    let controller!: ReadableStreamDefaultController<string>;
    const source = new ReadableStream<string>({
      start(streamController) {
        controller = streamController;
      },
    });
    const { stream } = await context.startStream(source);
    await new Promise((resolve) => setTimeout(resolve, 10));

    // Act
    controller.close();
    await collect(stream);

    // Assert
    await vi.waitFor(() => expect(unsubscribe.mock.calls.length).toBe(1));
  });

  test(`should not abort the controller for a stop that arrives after the source ended`, async () => {
    // Arrange
    let onStop!: () => void;
    const onFinish = vi.fn();
    const abortController = new AbortController();
    const adapter = createAdapter({
      onStopRequested: async (options) => {
        onStop = options.onStop;
        return () => {};
      },
    });
    const context = createResumableStream({ adapter, codec });
    const { stream } = await context.startStream(createSource([`a`]), {
      abortController,
      onFinish,
    });
    await collect(stream);
    await vi.waitFor(() => expect(onFinish).toHaveBeenCalled());

    // Act
    onStop();

    // Assert
    expect(abortController.signal.aborted).toBe(false);
  });

  test(`should pass the generation id to the stop subscription`, async () => {
    // Arrange
    const onStopRequested = vi.fn(async () => () => {});
    const adapter = createAdapter({ onStopRequested });
    const context = createResumableStream({ adapter, codec });

    // Act
    const { stream } = await context.startStream(createSource([`a`]), {
      streamId: `chat`,
      generationId: `turn-1`,
    });
    await collect(stream);

    // Assert
    const [input] = onStopRequested.mock.calls[0] as unknown as [
      { streamId: string; generationId: string },
    ];
    expect([input.streamId, input.generationId]).toEqual([`chat`, `turn-1`]);
  });

  test(`should generate a generation id when none is given`, async () => {
    // Arrange
    const context = createResumableStream({ adapter: createAdapter(), codec });

    // Act
    const { stream, generationId } = await context.startStream(createSource([`a`]));
    await collect(stream);

    // Assert
    expect(typeof generationId).toBe(`string`);
    expect(generationId.length > 0).toBe(true);
  });
});

describe(`resumeStream`, () => {
  test(`should pass the generation id to the adapter`, async () => {
    // Arrange
    const resumeStream = vi.fn(async () => null);
    const context = createResumableStream({ adapter: createAdapter({ resumeStream }), codec });

    // Act
    await context.resumeStream({ streamId: `chat`, generationId: `turn-1` });

    // Assert
    const input = resumeStream.mock.calls[0] as unknown;
    expect(input).toEqual([{ streamId: `chat`, generationId: `turn-1` }]);
  });
});

describe(`stopStream`, () => {
  test(`should pass the generation id to the adapter`, async () => {
    // Arrange
    const requestStop = vi.fn(async () => {});
    const context = createResumableStream({ adapter: createAdapter({ requestStop }), codec });

    // Act
    await context.stopStream({ streamId: `chat`, generationId: `turn-1` });

    // Assert
    const input = requestStop.mock.calls[0] as unknown;
    expect(input).toEqual([{ streamId: `chat`, generationId: `turn-1` }]);
  });
});
