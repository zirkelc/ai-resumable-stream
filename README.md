<div align='center'>

# ai-resumable-stream

<p align="center">AI SDK: Resume and stop UI message streams</p>
<p align="center">
  <a href="https://www.npmjs.com/package/ai-resumable-stream" alt="ai-resumable-stream"><img src="https://img.shields.io/npm/dt/ai-resumable-stream?label=ai-resumable-stream"></a> <a href="https://github.com/zirkelc/ai-resumable-stream/actions/workflows/ci.yml" alt="CI"><img src="https://img.shields.io/github/actions/workflow/status/zirkelc/ai-resumable-stream/ci.yml?branch=main"></a>
</p>

</div>

This library provides resumable streaming for UI message streams created by [`streamText()`](https://ai-sdk.dev/docs/reference/ai-sdk-core/stream-text) in the AI SDK. The library stores chunks as they are produced. Clients can then resume an interrupted stream, and any request can stop an active stream.

You choose where the chunks are stored. The library includes adapters for Redis and S3 Express. You can also write your own adapter by implementing four methods.

**Why?**

A stream does not keep its data. After a chunk is sent, it is gone. This causes two problems.

**Resume:** The server does not record which chunks it has sent. When a client disconnects (network drop, page reload, tab switch), the stream continues on the server, but the client misses the chunks that are sent during that time. The server can only replay the missed chunks if they are stored somewhere.

**Stop:** The request that stops a stream is not the request that started it. When the user clicks "Stop generating", the client sends a new HTTP request, but the stream runs in a different request or process. The two requests need a shared place to pass the stop signal.

## Install

```sh
npm install ai-resumable-stream
```

Install the optional peer dependencies for the features you use:

| Package              | Needed for                                |
| -------------------- | ----------------------------------------- |
| `redis`              | `ai-resumable-stream/adapters/redis`      |
| `@aws-sdk/client-s3` | `ai-resumable-stream/adapters/s3-express` |
| `ai`                 | `ai-resumable-stream/ai-sdk`              |

> [!NOTE]
> Version compatibility:
>
> - Use [`ai-resumable-stream@1.x`](https://github.com/zirkelc/ai-resumable-stream/tree/v1.x) for AI SDK v6
> - Use [`ai-resumable-stream@2.x`](https://github.com/zirkelc/ai-resumable-stream/tree/v2.x) or [`ai-resumable-stream@3.x`](https://github.com/zirkelc/ai-resumable-stream/tree/v3.x) and later for AI SDK v7

## Quick start

Create one context and reuse it for every stream.

```ts
import { createClient } from "redis";
import { createRedisAdapter } from "ai-resumable-stream/adapters/redis";
import { createResumableUIMessageStream } from "ai-resumable-stream/ai-sdk";

const publisher = createClient({ url: process.env.REDIS_URL });
const subscriber = createClient({ url: process.env.REDIS_URL });

// Optional: connect clients immediately or let the library connect on demand
await publisher.connect();
await subscriber.connect();

const context = createResumableUIMessageStream({
  adapter: createRedisAdapter({ publisher, subscriber }),
});
```

Then use the context in your three routes:

```ts
import { streamText, toUIMessageStream, type UIMessage } from "ai";

// POST /chat/:chatId
export async function send(chatId: string, messages: Array<UIMessage>) {
  const abortController = new AbortController();

  const result = streamText({
    model: openai(`gpt-4o`),
    messages,
    abortSignal: abortController.signal,
  });

  const { stream } = await context.startStream(toUIMessageStream({ stream: result.stream }), {
    streamId: chatId,
    abortController,
  });

  return stream;
}

// GET /chat/:chatId/stream
export async function resume(chatId: string) {
  const stream = await context.resumeStream({ streamId: chatId });

  return stream ?? new Response(null, { status: 204 });
}

// POST /chat/:chatId/stop
export async function stop(chatId: string) {
  await context.stopStream({ streamId: chatId });
}
```

## Usage

### `startStream`

Starts a stream and stores the chunks as they are produced. Returns the stream for the client, the stream id and the generation id.

> [!TIP]
> The returned stream is both a `ReadableStream` and an async iterable, so `return stream` and `yield* stream` both work.

```ts
import { streamText, toUIMessageStream } from "ai";

async function sendMessage(chatId: string, messages: UIMessage[]) {
  // Optional: `streamText` sees the stop signal and aborts the HTTP request
  const abortController = new AbortController();

  const result = streamText({
    model: openai(`gpt-4o`),
    messages,
    abortSignal: abortController.signal,
  });

  const { stream } = await context.startStream(toUIMessageStream({ stream: result.stream }), {
    // Optional: generates a stream id if not supplied
    streamId: chatId,
    abortController,
  });

  // Return stream to client
  return stream;
}
```

| Option                    | Type                          | Description                                                                                                                                                       |
| ------------------------- | ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `streamId`                | `string`                      | Id of the stream. Use it to resume or stop the stream. Defaults to a generated id, returned as `streamId`                                                         |
| `generationId`            | `string`                      | Id of this generation. Use it to resume or stop only this generation. Must be unique within the stream id. Defaults to a generated id, returned as `generationId` |
| `abortController`         | `AbortController`             | Aborted when the stream is stopped. Created by the library if you do not pass one                                                                                 |
| `stopTimeoutMs`           | `number`                      | Time that a stop waits for the source to end before it cancels the source. Defaults to `1000` with a supplied `abortController`, `0` without one                  |
| `onStopSubscriptionError` | `(error: unknown) => void`    | Called when the subscription to stop requests fails. The stream continues, but it cannot be stopped                                                               |
| `onFinish`                | `() => void \| Promise<void>` | Called after the source stream has ended, also after an error or a stop. Errors thrown by the callback are ignored                                                |

#### Abort controller

You can always stop a stream. If you do not pass an `abortController`, the library creates one.

```ts
// Without an abortController: a stop cancels the source stream
await context.startStream(toUIMessageStream({ stream: result.stream }), { streamId });

// With abortController: `streamText` sees the signal and aborts the HTTP request
const abortController = new AbortController();
const result = streamText({ model, messages, abortSignal: abortController.signal });
await context.startStream(toUIMessageStream({ stream: result.stream }), {
  streamId,
  abortController,
});
```

We recommend that you pass your own `abortController` and give its signal to `streamText`. A stop then aborts the request to the provider directly, and the AI SDK emits its `abort` chunk and calls `onAbort`. Without it, a stop only cancels the source stream, and the cancellation must propagate back to `streamText`.

#### Stop timeout

A stop aborts the `abortController` and waits up to `stopTimeoutMs` for the source to end. This lets `streamText` emit its `abort` chunk, so `onEnd` and `onFinish` get `isAborted: true`. A source that does not end in time is cancelled.

With `stopTimeoutMs: 0`, or without an `abortController`, a stop cancels the source immediately and `isAborted` is `false`.

```ts
const abortController = new AbortController();
const result = streamText({ model, messages, abortSignal: abortController.signal });
const source = toUIMessageStream({
  stream: result.stream,
  onFinish: ({ isAborted, responseMessage }) => {
    // `isAborted` is `true` after a stop
  },
});
await context.startStream(source, { streamId, abortController });
```

### `resumeStream`

Resumes an existing stream. The returned stream first replays all chunks produced so far and then continues with the new chunks. Pass `generationId` to resume a specific generation. Omit it to resume the current generation of the stream id. Returns `null` when there is nothing to resume.

```ts
async function resumeMessage(chatId: string) {
  const stream = await context.resumeStream({ streamId: chatId });

  // If no stream exists, return early
  if (!stream) {
    console.log("No active stream to resume");
    return;
  }

  // Return resumed stream to client
  return stream;
}
```

The result is `null` in three cases. The library does not tell you which case applies:

- no stream was started with that id
- the stream has already completed
- the producer died or was stopped before the stream completed

If your application must know whether a stream completed or was cut off, store that information together with the message.

### `stopStream`

Stops a running stream. Pass `generationId` to stop a specific generation. Omit it to stop the current generation of the stream id.

```ts
async function stopMessage(chatId: string, messageId?: string) {
  await context.stopStream({ streamId: chatId, generationId: messageId });
}
```

A stop request with a generation id can arrive before that generation listens for stop requests, or before it starts. Both included adapters store the request, and the generation stops as soon as it starts to listen. A stop request without a generation id applies to the generation that is current at the time of the request. If there is no current generation, the request is not stored and has no effect.

See [Identifiers](#identifiers) for details about stream ids and generation ids.

## Identifiers

A stream has two ids. If you do not pass an id, `startStream` generates it and returns both ids.

- **`streamId`** identifies the stream. A stream can run more than once. Each run is a generation.
- **`generationId`** identifies one generation of the stream.

Each call to `startStream` creates a new generation. The generation that started last is the current generation of the stream id.

> [!TIP]
> Most applications only need the stream id. Use an id that the client already knows, for example the chat id or the session id. A client that reconnects can then call `resumeStream({ streamId: chatId })` without any other id.

### Stream ID

With only a stream id, `resumeStream` and `stopStream` apply to the current generation.

```
startStream({ streamId: "chat-1" })   ──▶ generation A
startStream({ streamId: "chat-1" })   ──▶ generation B (current)

resumeStream({ streamId: "chat-1" })  ──▶ generation B
stopStream({ streamId: "chat-1" })    ──▶ generation B
```

If you start a stream with a stream id that is already in use, the new generation becomes the current generation. A resume then returns only the chunks of the new generation. Generation A continues to run until its source stream ends, but the stream id alone no longer reaches it. When generation A ends, its cleanup does not affect generation B.

### Generation ID

With a stream id and a generation id, `resumeStream` and `stopStream` apply only to that generation, also after a newer generation has started.

```
startStream({ streamId: "chat-1", generationId: "msg-1" })   ──▶ generation msg-1
startStream({ streamId: "chat-1", generationId: "msg-2" })   ──▶ generation msg-2 (current)

resumeStream({ streamId: "chat-1" })                         ──▶ generation msg-2
resumeStream({ streamId: "chat-1", generationId: "msg-1" })  ──▶ generation msg-1
stopStream({ streamId: "chat-1", generationId: "msg-1" })    ──▶ generation msg-1
```

Use a generation id when generations of the same stream id can overlap. Example: a client stops a generation while a new generation with the same stream id starts. The stop request must stop only the old generation, not the new one.

Use an id that the client already knows, for example the id of the user message that it sent. The client can then resume or stop that generation without a server-generated id.

> [!WARNING]
> A generation id must be unique within its stream id. Do not use it again, for example when you retry the same message. The library does not check this, and resume and stop do not work correctly for a generation id that is used twice.

## Adapters

| Adapter                   | Import                                    | Where chunks live                   | Resume needs a live producer | Cost model                       |
| ------------------------- | ----------------------------------------- | ----------------------------------- | ---------------------------- | -------------------------------- |
| [Redis](#redis)           | `ai-resumable-stream/adapters/redis`      | Memory of the producing process     | Yes                          | Your Redis instance              |
| [S3 Express](#s3-express) | `ai-resumable-stream/adapters/s3-express` | One appendable object in the bucket | No                           | One billed `PutObject` per flush |

### Redis

This adapter requires two Redis clients, because pub/sub needs a separate connection. The library connects the clients if they are not connected yet, but it never disconnects them. You manage the connection lifecycle in your application and can reuse the clients for all streams.

> [!NOTE]
> You need to install `redis` to use this adapter. Both v5 and v6 are supported.

```ts
import { createClient } from "redis";
import { createRedisAdapter } from "ai-resumable-stream/adapters/redis";
import { createResumableUIMessageStream } from "ai-resumable-stream/ai-sdk";

// Important: publisher and subscriber must be separate clients
const publisher = createClient({ url: process.env.REDIS_URL });
const subscriber = createClient({ url: process.env.REDIS_URL });

const adapter = createRedisAdapter({ publisher, subscriber });

const context = createResumableUIMessageStream({
  adapter,
});
```

| Option       | Type          | Required | Description                                                                                               |
| ------------ | ------------- | -------- | --------------------------------------------------------------------------------------------------------- |
| `publisher`  | `RedisClient` | Yes      | Client that sends commands and publishes messages                                                         |
| `subscriber` | `RedisClient` | Yes      | Client for subscriptions. Must be a separate client, because a subscribed connection cannot send commands |
| `keyPrefix`  | `string`      | No       | Prefix for all keys and channels. Defaults to `ai-resumable-stream`                                       |

> [!IMPORTANT]
> **A stream is only resumable while its producer is alive.**

See [docs/adapter-redis.md](https://github.com/zirkelc/ai-resumable-stream/blob/main/docs/adapter-redis.md) for how it works, a sequence diagram and a full example.

### S3 Express

This adapter requires an [S3 Express One Zone](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-one-zone.html) directory bucket and an `S3Client`. You create the client, so you control the credentials, the region and the retry behaviour.

The chunks are stored in the bucket, not in the memory of the producing process. A resume reads the chunks from the bucket. It does not need a response from the producing process, and any instance with access to the bucket can serve it.

> [!NOTE]
> You need to install `@aws-sdk/client-s3` to use this adapter.

```ts
import { S3Client } from "@aws-sdk/client-s3";
import { createS3ExpressAdapter } from "ai-resumable-stream/adapters/s3-express";

const adapter = createS3ExpressAdapter({
  client: new S3Client({ region: `us-east-1` }),
  bucket: `my-streams--use1-az4--x-s3`,
});
```

| Option                 | Type       | Default               | Description                                                                                         |
| ---------------------- | ---------- | --------------------- | --------------------------------------------------------------------------------------------------- |
| `client`               | `S3Client` |                       | The S3 client. You create and configure it                                                          |
| `bucket`               | `string`   |                       | A directory bucket in an Availability Zone                                                          |
| `prefix`               | `string`   | `ai-resumable-stream` | Prefix for all keys. Use it as the filter of the lifecycle rule                                     |
| `flushIntervalMs`      | `number`   | `250`                 | Maximum time that chunks stay in memory before they are written                                     |
| `batchSize`            | `number`   | `50`                  | Number of buffered chunks that triggers a write                                                     |
| `resumePollIntervalMs` | `number`   | `500`                 | Interval at which a resumed stream checks for new chunks                                            |
| `stopPollIntervalMs`   | `number`   | `1000`                | Interval at which a producer checks for a stop request                                              |
| `heartbeatMs`          | `number`   | `5000`                | Interval at which an idle producer records that it is alive                                         |
| `deadAfterMs`          | `number`   | `30000`               | Time without writes after which a producer is considered dead. Must be at least twice `heartbeatMs` |

> [!WARNING]
> The adapter does not delete objects when a stream finishes. Set a lifecycle expiration rule on the bucket with `prefix` as the filter, and grant `s3express:CreateSession` with `ReadWrite` to `lifecycle.s3.amazonaws.com` in the bucket policy. Without this grant, the rule does nothing and the objects are never deleted. See [Cleanup](https://github.com/zirkelc/ai-resumable-stream/blob/main/docs/adapter-s3-express.md#cleanup).

See [docs/adapter-s3-express.md](https://github.com/zirkelc/ai-resumable-stream/blob/main/docs/adapter-s3-express.md) for how it works, the storage layout, a sequence diagram, cost and liveness details, and a full example.

### Custom `StreamAdapter`

Implement the `StreamAdapter` interface against the store you want and pass the object as `adapter`.

```ts
import type { StreamAdapter } from "ai-resumable-stream";

const adapter: StreamAdapter = {
  createStream({ streamId, generationId, chunks, waitUntil }) { ... },
  resumeStream({ streamId, generationId }) { ... },
  requestStop({ streamId, generationId }) { ... },
  onStopRequested({ streamId, generationId, onStop }) { ... },
};
```

- **`createStream`** is called when a stream starts. It receives the stream id, the generation id and a `ReadableStream<string>` of encoded chunks. First, it discards any state left over from an earlier stream with the same id, so a reused id never replays old chunks. Then it makes `generationId` the current generation of the stream id and consumes the chunks in the background until the stream ends. The returned promise resolves as soon as the stream can be resumed, not when it is complete.
- **`resumeStream`** is called when a client reconnects. It returns every chunk produced so far, in the original order, and then continues with the new chunks until the stream ends. With a `generationId`, it resumes that generation. Without one, it resumes the current generation of the stream id. It returns `null` when the id is unknown, the stream has already finished, or its data has expired.
- **`requestStop`** is called by the process that wants to stop a stream. With a `generationId`, it stops that generation. Without one, it stops the current generation of the stream id. It must not fail when the stream is unknown or already finished, and it should store the request if the generation does not listen yet.
- **`onStopRequested`** is called by the producing process when a stream starts. It registers a listener for one generation that calls `onStop` when a stop signal arrives. If a stop request was stored before the listener was registered, the listener also calls `onStop` for that request. It returns a function that removes only this listener.

The adapter must treat the chunks as opaque strings. If your store needs a special format, for example escaping or a length prefix, the adapter adds it on write and removes it on read.

`requestStop` and `onStopRequested` usually run in different processes, so the stop signal must also go through the store, for example by a subscription or a poll. Store the stop signal per generation, so that a stop never reaches a different generation of the same stream id. `startStream` does not await `onStopRequested`. If `onStopRequested` rejects, the only effect is that the generation cannot be stopped.

The tests in [`src/__tests__/conformance-suite.ts`](./src/__tests__/conformance-suite.ts) run against both included adapters. They cover the behaviour that an adapter must have:

- a resume replays the past chunks and then continues with the new chunks
- chunks are still stored after the client disconnects
- a reused stream id does not replay the chunks of the previous stream
- a stop also ends the stream of a client that resumed
- a stop with a generation id only stops that generation, also when it was requested before that generation started
- an older generation can be resumed while a newer generation is current

## Advanced

### Chunk types

The root export works with any chunk type and does not depend on `ai`. The `ai-sdk` subpath export is preconfigured for the `UIMessageChunk` type of the AI SDK:

```ts
import { createResumableUIMessageStream } from "ai-resumable-stream/ai-sdk";

const context = createResumableUIMessageStream({ adapter });
```

This is the same as `createResumableStream({ adapter, codec: uiMessageChunkCodec })`. The codec validates each chunk when it is read. An invalid chunk, for example one written by an older version of your application, is dropped and does not fail the resume.

For other chunk types, pass your own `StreamCodec`:

```ts
import { createResumableStream, type StreamCodec } from "ai-resumable-stream";

const codec: StreamCodec<MyChunk> = {
  encode: (chunk) => JSON.stringify(chunk),
  decode: (data) => JSON.parse(data) as MyChunk,
};

const context = createResumableStream({ adapter, codec });
```

`decode` can return `undefined` to drop a chunk that it cannot decode. One invalid chunk then does not fail the full resume.

### Serverless

The library continues to store chunks after the response has been sent. On serverless runtimes, pass `waitUntil` so that the runtime keeps the function alive until all chunks are stored:

```ts
import { waitUntil } from "@vercel/functions";

const context = createResumableUIMessageStream({ adapter, waitUntil });
```

### Finish

The `onFinish` callback is invoked after the source stream has ended and the adapter stream was closed. Use it for cleanup tasks like removing the active stream ID from the database. Errors thrown by `onFinish` are caught and ignored.

```typescript
const { stream } = await context.startStream(toUIMessageStream({ stream: result.stream }), {
  onFinish: async () => {
    await saveChat({ chatId, activeStreamId: null });
  },
});
```

## Examples

The [`examples/`](./examples) folder contains one runnable example for each adapter. Each example starts a stream, disconnects the client before the stream ends, resumes the stream by its id, and then stops a second stream with `stopStream`.

```sh
pnpm example:redis        # starts a temporary Redis server
pnpm example:s3-express   # uses an in-memory bucket
```

### tRPC

Server-side procedures to send, resume and stop. The chat id is used as the stream id, so you do not have to store a separate stream id.

```ts
// server/router.ts
import { z } from "zod";
import { streamText, toUIMessageStream, type UIMessage, type UIMessageChunk } from "ai";
import { createClient } from "redis";
import { createResumableUIMessageStream } from "ai-resumable-stream/ai-sdk";
import { createRedisAdapter } from "ai-resumable-stream/adapters/redis";
import { publicProcedure, router } from "./trpc";

const publisher = createClient({ url: process.env.REDIS_URL });
const subscriber = createClient({ url: process.env.REDIS_URL });

const context = createResumableUIMessageStream({
  adapter: createRedisAdapter({ publisher, subscriber }),
});

export const appRouter = router({
  sendMessage: publicProcedure
    .input(z.object({ chatId: z.string(), message: z.custom<UIMessage>() }))
    .mutation(async function* ({ input }): AsyncGenerator<UIMessageChunk> {
      const { chatId, message } = input;

      const abortController = new AbortController();

      const result = streamText({
        model: openai(`gpt-4o`),
        messages: [message],
        abortSignal: abortController.signal,
      });

      const { stream } = await context.startStream(toUIMessageStream({ stream: result.stream }), {
        streamId: chatId,
        abortController,
        onFinish: async () => {
          await saveAssistantMessage(chatId, await result.text);
        },
      });

      yield* stream;
    }),

  resumeMessage: publicProcedure.input(z.object({ chatId: z.string() })).mutation(async function* ({
    input,
  }): AsyncGenerator<UIMessageChunk> {
    const stream = await context.resumeStream({ streamId: input.chatId });
    if (!stream) return;

    yield* stream;
  }),

  stopMessage: publicProcedure
    .input(z.object({ chatId: z.string() }))
    .mutation(async ({ input }) => {
      await context.stopStream({ streamId: input.chatId });

      return { success: true };
    }),
});
```

## API Reference

### `createResumableStream`

```ts
function createResumableStream<CHUNK>(options: CreateResumableStreamOptions<CHUNK>): {
  startStream: (
    source: ReadableStream<CHUNK>,
    options?: StartStreamOptions,
  ) => Promise<StartStreamResult<CHUNK>>;
  resumeStream: (options: ResumeStreamOptions) => Promise<AsyncIterableStream<CHUNK> | null>;
  stopStream: (options: StopStreamOptions) => Promise<void>;
};

type CreateResumableStreamOptions<CHUNK> = {
  adapter: StreamAdapter;
  codec: StreamCodec<CHUNK>;
  waitUntil?: (promise: Promise<unknown>) => void;
  generateId?: () => string;
};

type StartStreamOptions = {
  streamId?: string;
  generationId?: string;
  abortController?: AbortController;
  stopTimeoutMs?: number;
  onStopSubscriptionError?: (error: unknown) => void;
  onFinish?: () => void | Promise<void>;
};

type StartStreamResult<CHUNK> = {
  streamId: string;
  generationId: string;
  stream: AsyncIterableStream<CHUNK>;
};

type ResumeStreamOptions = {
  streamId: string;
  generationId?: string;
};

type StopStreamOptions = {
  streamId: string;
  generationId?: string;
};
```

| Option       | Type                 | Required | Description                                                                                                                                                  |
| ------------ | -------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `adapter`    | `StreamAdapter`      | Yes      | Stores the chunks and transports stop requests                                                                                                               |
| `codec`      | `StreamCodec<CHUNK>` | Yes      | Converts chunks to strings for the adapter, and back                                                                                                         |
| `waitUntil`  | `(promise) => void`  | No       | Keeps the process alive until all chunks are stored. Only needed on serverless runtimes                                                                      |
| `generateId` | `() => string`       | No       | Generates the stream id and the generation id when you do not pass them to `startStream`. Must return a new id on each call. Defaults to `crypto.randomUUID` |

### `createResumableUIMessageStream`

Same as `createResumableStream`, with `codec` set to `uiMessageChunkCodec`.

```ts
function createResumableUIMessageStream(
  options: Omit<CreateResumableStreamOptions<UIMessageChunk>, `codec`>,
): { startStream; resumeStream; stopStream };
```

### `StreamAdapter`

```ts
type StreamAdapter = {
  createStream(options: {
    streamId: string;
    generationId: string;
    chunks: ReadableStream<string>;
    waitUntil?: (promise: Promise<unknown>) => void;
  }): Promise<void>;
  resumeStream(options: {
    streamId: string;
    generationId?: string;
  }): Promise<ReadableStream<string> | null>;
  requestStop(options: { streamId: string; generationId?: string }): Promise<void>;
  onStopRequested(options: {
    streamId: string;
    generationId: string;
    onStop: () => void;
  }): Promise<() => void>;
};
```

### `StreamCodec`

```ts
type StreamCodec<CHUNK> = {
  encode(chunk: CHUNK): string;
  decode(data: string): CHUNK | undefined | Promise<CHUNK | undefined>;
};
```

## License

MIT
