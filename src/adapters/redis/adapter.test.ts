import { describe, expect, test, vi } from "vitest";
import { createRedisAdapter } from "./adapter.js";

type Options = Parameters<typeof createRedisAdapter>[0];

/**
 * An in-memory stand-in for the commands the stop path uses, recording the order they
 * are issued in. Ordering is the whole of what makes a stored stop reliable, and a real
 * server is too quick to show a wrong order.
 */
function createFakeRedis() {
  const calls: Array<string> = [];
  const values = new Map<string, string>();
  const listeners = new Map<string, Set<(message: string) => void>>();

  const client = {
    isOpen: true,
    connect: async () => {},
    get: async (key: string) => {
      calls.push(`get ${key}`);
      return values.get(key) ?? null;
    },
    set: async (key: string, value: string) => {
      calls.push(`set ${key}`);
      values.set(key, value);
      return `OK`;
    },
    del: async (key: string) => {
      calls.push(`del ${key}`);
      values.delete(key);
    },
    eval: async () => 0,
    incr: async () => 1,
    publish: async (channel: string, message: string) => {
      calls.push(`publish ${channel}`);
      for (const listener of listeners.get(channel) ?? []) listener(message);
      return 1;
    },
    subscribe: async (channel: string, listener: (message: string) => void) => {
      calls.push(`subscribe ${channel}`);
      listeners.set(channel, (listeners.get(channel) ?? new Set()).add(listener));
    },
    /** Without a listener, every listener of the channel is removed, as in `redis`. */
    unsubscribe: async (channel: string, listener?: (message: string) => void) => {
      calls.push(`unsubscribe ${channel}`);
      if (listener) listeners.get(channel)?.delete(listener);
      else listeners.delete(channel);
    },
  };

  return { calls, values, client: client as unknown as Options[`publisher`] };
}

describe(`redis adapter stop`, () => {
  test(`should store the stop before publishing it`, async () => {
    // Arrange
    const redis = createFakeRedis();
    const adapter = createRedisAdapter({ publisher: redis.client, subscriber: redis.client });

    // Act
    await adapter.requestStop({ streamId: `chat`, generationId: `turn-1` });

    // Assert
    expect(redis.calls).toEqual([
      `set ai-resumable-stream:stop:chat:turn-1`,
      `publish ai-resumable-stream:stop:chat:turn-1`,
    ]);
  });

  test(`should subscribe before reading a stored stop`, async () => {
    // Arrange
    const redis = createFakeRedis();
    const adapter = createRedisAdapter({ publisher: redis.client, subscriber: redis.client });

    // Act
    await adapter.onStopRequested({ streamId: `chat`, generationId: `turn-1`, onStop: vi.fn() });

    // Assert
    expect(redis.calls).toEqual([
      `subscribe ai-resumable-stream:stop:chat:turn-1`,
      `get ai-resumable-stream:stop:chat:turn-1`,
    ]);
  });

  test(`should report a stop stored before the listener registered`, async () => {
    // Arrange
    const redis = createFakeRedis();
    const adapter = createRedisAdapter({ publisher: redis.client, subscriber: redis.client });
    const onStop = vi.fn();
    await adapter.requestStop({ streamId: `chat`, generationId: `turn-1` });

    // Act
    await adapter.onStopRequested({ streamId: `chat`, generationId: `turn-1`, onStop });

    // Assert
    expect(onStop.mock.calls.length).toBe(1);
  });

  test(`should store nothing for a stream without a current generation`, async () => {
    // Arrange
    const redis = createFakeRedis();
    const adapter = createRedisAdapter({ publisher: redis.client, subscriber: redis.client });

    // Act
    await adapter.requestStop({ streamId: `chat` });

    // Assert
    expect(redis.calls).toEqual([`get ai-resumable-stream:generation:chat`]);
  });

  test(`should stop the current generation when no generation id is given`, async () => {
    // Arrange
    const redis = createFakeRedis();
    const adapter = createRedisAdapter({ publisher: redis.client, subscriber: redis.client });
    redis.values.set(`ai-resumable-stream:generation:chat`, `chat:turn-1`);

    // Act
    await adapter.requestStop({ streamId: `chat` });

    // Assert
    expect(redis.calls.slice(1)).toEqual([
      `set ai-resumable-stream:stop:chat:turn-1`,
      `publish ai-resumable-stream:stop:chat:turn-1`,
    ]);
  });

  test(`should not let a stop reach another stream whose ids join to the same name`, async () => {
    // Arrange
    const redis = createFakeRedis();
    const adapter = createRedisAdapter({ publisher: redis.client, subscriber: redis.client });
    const onStop = vi.fn();
    await adapter.requestStop({ streamId: `chat`, generationId: `a:b` });

    // Act
    await adapter.onStopRequested({ streamId: `chat:a`, generationId: `b`, onStop });

    // Assert
    expect(onStop.mock.calls.length).toBe(0);
  });

  test(`should stop the current generation when its id contains a colon`, async () => {
    // Arrange
    const redis = createFakeRedis();
    const adapter = createRedisAdapter({ publisher: redis.client, subscriber: redis.client });
    const onStop = vi.fn();
    await adapter.onStopRequested({ streamId: `chat`, generationId: `a:b`, onStop });
    redis.values.set(`ai-resumable-stream:generation:chat`, `chat:a%3Ab`);

    // Act
    await adapter.requestStop({ streamId: `chat` });

    // Assert
    expect(onStop.mock.calls.length).toBe(1);
  });

  test(`should remove only its own listener from a shared channel`, async () => {
    // Arrange
    const redis = createFakeRedis();
    const adapter = createRedisAdapter({ publisher: redis.client, subscriber: redis.client });
    const onFirst = vi.fn();
    const onSecond = vi.fn();
    await adapter.onStopRequested({ streamId: `chat`, generationId: `turn-1`, onStop: onFirst });
    const unsubscribeSecond = await adapter.onStopRequested({
      streamId: `chat`,
      generationId: `turn-1`,
      onStop: onSecond,
    });

    // Act
    await unsubscribeSecond();
    await adapter.requestStop({ streamId: `chat`, generationId: `turn-1` });

    // Assert
    expect(onFirst.mock.calls.length).toBe(1);
    expect(onSecond.mock.calls.length).toBe(0);
  });

  test(`should drop a stored stop once the generation ends`, async () => {
    // Arrange
    const redis = createFakeRedis();
    const adapter = createRedisAdapter({ publisher: redis.client, subscriber: redis.client });
    let close!: () => void;
    const chunks = new ReadableStream<string>({
      start(controller) {
        close = () => controller.close();
      },
    });
    await adapter.createStream({ streamId: `chat`, generationId: `turn-1`, chunks });
    await adapter.requestStop({ streamId: `chat`, generationId: `turn-1` });

    // Act
    close();

    // Assert
    await vi.waitFor(() =>
      expect(redis.values.has(`ai-resumable-stream:stop:chat:turn-1`)).toBe(false),
    );
  });
});
