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

/**
 * An in-memory stand-in for the client built into Bun, which differs from `redis` in the
 * commands the adapter needs: `connected` in place of `isOpen`, the expiry of `set` as
 * positional arguments, and `EVAL` only through `send`. Like Bun, a client rejects an
 * argument that is not a string or a number, refuses commands while it is in subscriber
 * mode, and throws on an unsubscribe outside subscriber mode. The clients of one fake share
 * their keys and channels, as clients of one server do.
 */
function createFakeBunRedis() {
  type Listener = (message: string, channel: string) => void;
  const values = new Map<string, string>();
  const subscriptions = new Map<string, Array<Listener>>();

  function createClient() {
    const calls: Array<string> = [];
    const channels = new Map<string, Array<Listener>>();
    let failingSubscribes = 0;

    const record = ({ name, args }: { name: string; args: Array<unknown> }) => {
      if (args.some((arg) => typeof arg !== `string` && typeof arg !== `number`)) {
        throw new TypeError(`Expected arguments to be a string or buffer for '${name}'.`);
      }
      calls.push([name, ...args].join(` `));
    };

    const command = ({ name, args }: { name: string; args: Array<unknown> }) => {
      if (channels.size > 0) {
        throw Object.assign(new Error(`${name} cannot be called while in subscriber mode.`), {
          code: `ERR_REDIS_INVALID_STATE`,
        });
      }
      record({ name, args });
    };

    const client = {
      connected: false,
      onconnect: null as (() => void) | null,
      connect: async () => {
        calls.push(`connect`);
        client.connected = true;
        client.onconnect?.();
      },
      get: async (key: string) => {
        command({ name: `get`, args: [key] });
        return values.get(key) ?? null;
      },
      set: async (key: string, value: string, ...options: Array<string | number>) => {
        command({ name: `set`, args: [key, value, ...options] });
        values.set(key, value);
        return `OK`;
      },
      del: async (key: string) => {
        command({ name: `del`, args: [key] });
        values.delete(key);
        return 1;
      },
      send: async (name: string, args: Array<string>) => {
        command({ name: `send`, args: [name, ...args] });
        return 0;
      },
      publish: async (channel: string, message: string) => {
        command({ name: `publish`, args: [channel, message] });
        for (const listener of subscriptions.get(channel) ?? []) listener(message, channel);
        return 1;
      },
      subscribe: async (channel: string, listener: Listener) => {
        record({ name: `subscribe`, args: [channel] });
        if (failingSubscribes > 0) {
          failingSubscribes -= 1;
          throw new Error(`The connection was closed.`);
        }
        channels.set(channel, [...(channels.get(channel) ?? []), listener]);
        subscriptions.set(channel, [...(subscriptions.get(channel) ?? []), listener]);
        return 1;
      },
      unsubscribe: (channel: string, listener: Listener) => {
        if (channels.size === 0) {
          throw Object.assign(new Error(`unsubscribe can only be called in subscriber mode.`), {
            code: `ERR_REDIS_INVALID_STATE`,
          });
        }
        record({ name: `unsubscribe`, args: [channel] });
        const remove = (list?: Array<Listener>) => (list ?? []).filter((item) => item !== listener);
        channels.set(channel, remove(channels.get(channel)));
        if (channels.get(channel)?.length === 0) channels.delete(channel);
        subscriptions.set(channel, remove(subscriptions.get(channel)));
        return Promise.resolve();
      },
    };

    /**
     * Like Bun after the server drops the connection: the client keeps its listeners and
     * connects again, but the server has none of its subscriptions.
     */
    const reconnect = () => {
      for (const [channel, listeners] of channels) {
        const kept = (subscriptions.get(channel) ?? []).filter((item) => !listeners.includes(item));
        subscriptions.set(channel, kept);
      }
      client.onconnect?.();
    };

    const failSubscribes = (count: number) => {
      failingSubscribes = count;
    };

    return { calls, client, reconnect, failSubscribes };
  }

  return { values, publisher: createClient(), subscriber: createClient() };
}

describe(`redis adapter with the Bun client`, () => {
  test(`should store a stop with a positional expiry`, async () => {
    // Arrange
    const redis = createFakeBunRedis();
    const adapter = createRedisAdapter({
      publisher: redis.publisher.client,
      subscriber: redis.subscriber.client,
    });

    // Act
    await adapter.requestStop({ streamId: `chat`, generationId: `turn-1` });

    // Assert
    expect(redis.publisher.calls).toEqual([
      `connect`,
      `set ai-resumable-stream:stop:chat:turn-1 1 EX 3600`,
      `publish ai-resumable-stream:stop:chat:turn-1 stop`,
    ]);
  });

  test(`should connect a client only while it is not connected`, async () => {
    // Arrange
    const redis = createFakeBunRedis();
    const adapter = createRedisAdapter({
      publisher: redis.publisher.client,
      subscriber: redis.subscriber.client,
    });
    await adapter.requestStop({ streamId: `chat`, generationId: `turn-1` });

    // Act
    await adapter.requestStop({ streamId: `chat`, generationId: `turn-2` });

    // Assert
    expect(redis.publisher.calls.filter((call) => call === `connect`).length).toBe(1);
  });

  test(`should retire the pointer with EVAL once the generation ends`, async () => {
    // Arrange
    const redis = createFakeBunRedis();
    const adapter = createRedisAdapter({
      publisher: redis.publisher.client,
      subscriber: redis.subscriber.client,
    });
    let close!: () => void;
    const chunks = new ReadableStream<string>({
      start(controller) {
        close = () => controller.close();
      },
    });
    await adapter.createStream({ streamId: `chat`, generationId: `turn-1`, chunks });

    // Act
    close();

    // Assert
    await vi.waitFor(() =>
      expect(redis.publisher.calls).toContain(
        `send EVAL if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) end return 0 1 ai-resumable-stream:generation:chat chat:turn-1`,
      ),
    );
  });

  test(`should forget a listener whose subscribe failed`, async () => {
    // Arrange
    const redis = createFakeBunRedis();
    const adapter = createRedisAdapter({
      publisher: redis.publisher.client,
      subscriber: redis.subscriber.client,
    });
    const onFailed = vi.fn();
    const onStop = vi.fn();
    redis.subscriber.failSubscribes(1);
    const failed = adapter.onStopRequested({
      streamId: `chat`,
      generationId: `turn-1`,
      onStop: onFailed,
    });
    await expect(failed).rejects.toThrow();
    await adapter.onStopRequested({ streamId: `chat`, generationId: `turn-1`, onStop });

    // Act
    await adapter.requestStop({ streamId: `chat`, generationId: `turn-1` });

    // Assert
    expect(onFailed.mock.calls.length).toBe(0);
    expect(onStop.mock.calls.length).toBe(1);
  });

  test(`should restore a subscription on a later reconnect when a restore failed`, async () => {
    // Arrange
    const redis = createFakeBunRedis();
    const adapter = createRedisAdapter({
      publisher: redis.publisher.client,
      subscriber: redis.subscriber.client,
    });
    const onStop = vi.fn();
    await adapter.onStopRequested({ streamId: `chat`, generationId: `turn-1`, onStop });
    redis.subscriber.failSubscribes(1);
    redis.subscriber.reconnect();
    const subscribes = () => redis.subscriber.calls.filter((call) => call.startsWith(`subscribe`));
    await vi.waitFor(() => expect(subscribes().length).toBe(2));
    // Lets the failed restore settle
    await new Promise((resolve) => setTimeout(resolve, 0));

    // Act
    redis.subscriber.reconnect();
    await vi.waitFor(() => expect(subscribes().length).toBe(3));
    await adapter.requestStop({ streamId: `chat`, generationId: `turn-1` });

    // Assert
    expect(onStop.mock.calls.length).toBe(1);
  });

  test(`should restore subscriptions when the previous connect handler throws`, async () => {
    // Arrange
    const redis = createFakeBunRedis();
    await redis.subscriber.client.connect();
    redis.subscriber.client.onconnect = () => {
      throw new Error(`The handler of the application failed.`);
    };
    const adapter = createRedisAdapter({
      publisher: redis.publisher.client,
      subscriber: redis.subscriber.client,
    });
    const onStop = vi.fn();
    await adapter.onStopRequested({ streamId: `chat`, generationId: `turn-1`, onStop });

    // Act
    const reconnect = () => redis.subscriber.reconnect();

    // Assert
    expect(reconnect).toThrow();
    await vi.waitFor(() =>
      expect(redis.subscriber.calls.filter((call) => call.startsWith(`subscribe`)).length).toBe(2),
    );
    await adapter.requestStop({ streamId: `chat`, generationId: `turn-1` });
    expect(onStop.mock.calls.length).toBe(1);
  });

  test(`should leave the clients unchanged when an option is invalid`, () => {
    // Arrange
    const redis = createFakeBunRedis();

    // Act
    const create = () =>
      createRedisAdapter({
        publisher: redis.publisher.client,
        subscriber: redis.subscriber.client,
        heartbeatMs: 0,
      });

    // Assert
    expect(create).toThrow();
    expect(redis.subscriber.client.onconnect).toBe(null);
  });
});
