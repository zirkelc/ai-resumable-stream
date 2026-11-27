/**
 * A client from the `redis` package, described by the commands the adapter actually calls
 * rather than by the client type of a specific `redis` release. The generic parameters of
 * `RedisClientType` are not mutually assignable across redis v5 and v6, so a nominal type
 * would pin consumers to one of them.
 *
 * `unsubscribe` takes the listener because a listener removes itself alone, never every
 * listener of its channel.
 */
export type NodeRedis = {
  isOpen: boolean;
  connect(): Promise<unknown>;
  publish(channel: string, message: string): Promise<unknown>;
  subscribe(channel: string, listener: (message: string) => void): Promise<unknown>;
  unsubscribe(channel: string, listener?: (message: string) => void): Promise<unknown>;
  get(key: string): Promise<string | null>;
  set(
    key: string,
    value: string,
    options?: { expiration?: { type: `EX` | `PX`; value: number } },
  ): Promise<unknown>;
  del(key: string): Promise<unknown>;
  eval(
    script: string,
    options?: { keys?: Array<string>; arguments?: Array<string> },
  ): Promise<unknown>;
};

/**
 * The client built into Bun (`Bun.RedisClient`), described by the commands the adapter
 * calls, so that a consumer on Node does not need the types of Bun.
 */
export type BunRedis = {
  readonly connected: boolean;
  onconnect: (() => void) | null;
  connect(): Promise<unknown>;
  send(command: string, args: Array<string>): Promise<unknown>;
  publish(channel: string, message: string): Promise<unknown>;
  subscribe(channel: string, listener: BunListener): Promise<unknown>;
  unsubscribe(channel: string, listener: BunListener): Promise<unknown>;
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<unknown>;
  set(key: string, value: string, type: `EX` | `PX`, duration: number): Promise<unknown>;
  del(key: string): Promise<unknown>;
};

type BunListener = (message: string, channel: string) => void;

export type RedisClient = NodeRedis | BunRedis;

/**
 * The Bun client has `connected` in place of `isOpen`. The `isOpen` check comes first, so a
 * `redis` client is never taken for a Bun client.
 */
function isBunRedis(client: RedisClient): client is BunRedis {
  return !(`isOpen` in client) && `connected` in client && typeof client.send === `function`;
}

/**
 * One wrapper per client, because a wrapper keeps the subscriptions of its client and
 * hooks into its reconnects. A new adapter on the same clients must not add a second hook.
 */
const bunWrappers = new WeakMap<BunRedis, NodeRedis>();

/**
 * Gives a client the commands of a `redis` client. A `redis` client is returned unchanged.
 */
export function toNodeRedis(client: RedisClient): NodeRedis {
  if (!isBunRedis(client)) return client;

  let wrapper = bunWrappers.get(client);
  if (!wrapper) {
    wrapper = wrapBunRedis(client);
    bunWrappers.set(client, wrapper);
  }
  return wrapper;
}

/**
 * Bun rejects an unsubscribe when the client is not in subscriber mode, sometimes with a
 * synchronous throw. Then there is nothing to remove, as with a `redis` client.
 */
async function unsubscribeQuietly({
  client,
  channel,
  listener,
}: {
  client: BunRedis;
  channel: string;
  listener: BunListener;
}) {
  try {
    await client.unsubscribe(channel, listener);
  } catch (error) {
    if ((error as { code?: unknown } | null)?.code !== `ERR_REDIS_INVALID_STATE`) throw error;
  }
}

/**
 * Gives the expiry of `set` as positional arguments and sends `EVAL` through `send`.
 *
 * Subscriptions need more. After a reconnect, Bun keeps its listeners but does not
 * subscribe again on the server, so no message arrives. And Bun keeps a list, not a set, of
 * listeners: a second subscribe with the same listener calls it twice. So the wrapper keeps
 * the listeners of each channel itself and gives Bun one listener for all channels. After
 * each reconnect, it unsubscribes and subscribes that listener again for each channel that
 * has listeners, also for a channel whose last subscribe failed.
 *
 * The calls for one channel run one after the other, and each call makes Bun match the
 * listeners that the channel has at that time. So a subscribe and an unsubscribe that
 * overlap, or a reconnect between them, cannot leave the channel in the wrong state.
 */
function wrapBunRedis(client: BunRedis): NodeRedis {
  type Channel = {
    listeners: Set<(message: string) => void>;
    // The dispatch listener is registered in Bun for this channel
    registered: boolean;
    // A reconnect happened, so the server no longer has the subscription, and Bun can still
    // keep the listener of a subscribe that was in progress or failed
    stale: boolean;
    queue: Promise<void>;
  };
  const channels = new Map<string, Channel>();

  const dispatch: BunListener = (message, channel) => {
    for (const listener of channels.get(channel)?.listeners ?? []) listener(message);
  };

  const reconcile = async ({ name, channel }: { name: string; channel: Channel }) => {
    if (channel.stale) {
      await unsubscribeQuietly({ client, channel: name, listener: dispatch });
      channel.registered = false;
      channel.stale = false;
    }
    if (channel.listeners.size > 0 && !channel.registered) {
      await client.subscribe(name, dispatch);
      channel.registered = true;
    } else if (channel.listeners.size === 0 && channel.registered) {
      await unsubscribeQuietly({ client, channel: name, listener: dispatch });
      channel.registered = false;
    }
    forgetIfUnused({ name, channel });
  };

  const forgetIfUnused = ({ name, channel }: { name: string; channel: Channel }) => {
    if (channel.listeners.size === 0 && !channel.registered && channels.get(name) === channel) {
      channels.delete(name);
    }
  };

  const enqueue = ({ name, channel }: { name: string; channel: Channel }) => {
    const result = channel.queue.then(() => reconcile({ name, channel }));
    channel.queue = result.catch(() => {
      // The caller gets the error, and the next call tries again
    });
    return result;
  };

  const channelOf = (name: string) => {
    let channel = channels.get(name);
    if (!channel) {
      channel = { listeners: new Set(), registered: false, stale: false, queue: Promise.resolve() };
      channels.set(name, channel);
    }
    return channel;
  };

  /**
   * Restores before it calls the handler that was there before, so a handler that throws
   * cannot stop the restore. On the first connect, no channel is registered yet, so there
   * is nothing to restore.
   */
  const previous = client.onconnect;
  client.onconnect = function (this: unknown) {
    for (const [name, channel] of channels) {
      if (channel.listeners.size === 0 && !channel.registered) continue;
      channel.stale = true;
      void enqueue({ name, channel }).catch(() => {
        // The next subscribe, unsubscribe or reconnect of the channel tries again
      });
    }
    previous?.call(this);
  };

  return {
    get isOpen() {
      return client.connected;
    },
    connect: () => client.connect(),
    publish: (channel, message) => client.publish(channel, message),
    subscribe: (name, listener) => {
      const channel = channelOf(name);
      channel.listeners.add(listener);
      /**
       * A failed subscribe registers nothing, as with a `redis` client, so the caller does
       * not have to unsubscribe the listener.
       */
      return enqueue({ name, channel }).catch((error: unknown) => {
        channel.listeners.delete(listener);
        forgetIfUnused({ name, channel });
        throw error;
      });
    },
    unsubscribe: (name, listener) => {
      const channel = channels.get(name);
      if (!channel) return Promise.resolve();
      if (listener) channel.listeners.delete(listener);
      else channel.listeners.clear();
      return enqueue({ name, channel });
    },
    get: (key) => client.get(key),
    set: (key, value, options) =>
      options?.expiration
        ? client.set(key, value, options.expiration.type, options.expiration.value)
        : client.set(key, value),
    del: (key) => client.del(key),
    eval: (script, { keys = [], arguments: args = [] } = {}) =>
      client.send(`EVAL`, [script, String(keys.length), ...keys, ...args]),
  };
}
