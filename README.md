<div align='center'>

# ai-resumable-stream

<p align="center">Resume and stop AI SDK streams, backed by S3, Redis, or any store you provide</p>
<p align="center">
  <a href="https://www.npmjs.com/package/ai-resumable-stream" alt="ai-resumable-stream"><img src="https://img.shields.io/npm/dt/ai-resumable-stream?label=ai-resumable-stream"></a> <a href="https://github.com/zirkelc/ai-resumable-stream/actions/workflows/ci.yml" alt="CI"><img src="https://img.shields.io/github/actions/workflow/status/zirkelc/ai-resumable-stream/ci.yml?branch=main"></a>
</p>

</div>

This library makes streams from [`streamText()`](https://ai-sdk.dev/docs/reference/ai-sdk-core/stream-text) resumable and stoppable. Chunks are persisted as they are produced, so a client that disconnects can pick the stream up again, and a stop request from any process reaches the one producing it.

Where the chunks go is up to you. Pick an adapter, or write one.

**Why?**

Streams are ephemeral. Once data flows through, it is gone. That creates two hard problems.

**Resume is hard** because the server does not track what it has sent. If a client disconnects (network drop, page reload, tab switch), the stream keeps running on the server but the client loses everything that arrived while it was away.

**Stop is hard** because the request that says "stop" is not the request that started the stream. Without a coordination point, one cannot signal the other.

## Install

```sh
npm install ai-resumable-stream
```

Optional peer dependencies: `redis` for the Redis adapter, `@aws-sdk/client-s3` for the S3 Express adapter, and `ai` for the `ai-sdk` subpath. Nothing else is required.

## Quick start

```ts
import { createResumableStream } from "ai-resumable-stream";
import { uiMessageChunkCodec } from "ai-resumable-stream/ai-sdk";
import { createRedisAdapter } from "ai-resumable-stream/adapters/redis";

const streams = createResumableStream({
  codec: uiMessageChunkCodec,
  adapter: createRedisAdapter({ publisher, subscriber }),
});
```

Then, in three routes:

```ts
/** POST /chat: start */
const abortController = new AbortController();
const result = streamText({ model, messages, abortSignal: abortController.signal });

const { stream } = await streams.startStream(result.toUIMessageStream(), {
  streamId: chatId,
  abortController,
});
return stream;

/** GET /chat/:chatId/stream: resume */
const stream = await streams.resumeStream(chatId);
if (!stream) return new Response(null, { status: 204 });
return stream;

/** POST /chat/:chatId/stop: stop */
await streams.stopStream(chatId);
```

That is the whole API: `startStream`, `resumeStream`, `stopStream`.

## Stream ids

A stream is addressed by a single `streamId` that you choose. There is no separate pointer or key to maintain.

Most applications pass the **chat id**, which encodes the invariant "at most one active stream per chat". Resuming is then just `resumeStream(chatId)`, which is all a reconnecting client needs to know. Pass the **assistant message id** instead when you need to address one specific stream.

Reusing an id is expected and safe: starting a new stream under an existing id discards the previous stream's chunks. A producer that is still shutting down cannot corrupt or terminate the stream that replaced it.

Omit `streamId` and one is generated for you; `startStream` returns it.

## Stopping

`stopStream(streamId)` reaches the producer wherever it runs. Stop is always wired, so it works without extra configuration.

```ts
/** Without a controller. Stopping cancels the source, which propagates upstream. */
await streams.startStream(result.toUIMessageStream(), { streamId });

/** With one, so streamText sees the signal. Preferred. */
const abortController = new AbortController();
const result = streamText({ model, messages, abortSignal: abortController.signal });
await streams.startStream(result.toUIMessageStream(), { streamId, abortController });
```

Both stop. Pass your own controller anyway: handing the signal to `streamText` aborts the provider request directly and lets the AI SDK emit its `abort` chunk and run `onAbort`, rather than relying on cancellation propagating up the pipe.

## Adapters

Two adapters ship with the package:

| Import                                    | Store                                                                       | Durable |
| ----------------------------------------- | --------------------------------------------------------------------------- | ------- |
| `ai-resumable-stream/adapters/redis`      | Redis, via [`resumable-stream`](https://github.com/vercel/resumable-stream) | No      |
| `ai-resumable-stream/adapters/s3-express` | One appendable object per stream, in an S3 Express One Zone bucket          | Yes     |

Anything else is a `StreamAdapter` of your own, which is four methods.

Whichever you use, a stream that ended abnormally is reported the same way as one that completed: `resumeStream` returns `null`, and a reader already following it simply ends, so a truncated stream is indistinguishable from a complete one. If your application needs to tell them apart, record that alongside the message you persist in `onFinish`. **A finished stream keeps its chunks**, because a reader that started while it was live may still be draining them; wiping on completion would truncate its message.

### Redis

```ts
import { createRedisAdapter } from "ai-resumable-stream/adapters/redis";

const adapter = createRedisAdapter({ publisher, subscriber });
```

Two clients are required, because a subscribed connection cannot issue commands. They are connected on first use and never disconnected, so they stay reusable across streams.

Chunks are buffered in the memory of the producing process and replayed to late subscribers over pub/sub. **A stream is only resumable while its producer is alive.** If a stream must survive the process that started it, use the S3 Express adapter.

Nothing is retained in Redis, so there is no TTL to configure. The pointer to the running stream is dropped as soon as the source ends, and expires after 24 hours if the producer dies before it can clean up.

### S3 Express

A bucket is the only dependency. No Redis, no database, nothing to run.

```ts
import { S3Client } from "@aws-sdk/client-s3";
import { createS3ExpressAdapter } from "ai-resumable-stream/adapters/s3-express";

const adapter = createS3ExpressAdapter({
  client: new S3Client({ region: "us-east-1" }),
  bucket: "my-streams--use1-az4--x-s3",
});
```

**The bucket must be an [S3 Express One Zone](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-one-zone.html) directory bucket in an Availability Zone.** A stream is one object that the producer appends to, and appends exist nowhere else in S3. That is what keeps a stream to one object instead of one per batch, and what lets a resuming reader replay the whole backlog with a single ranged read and then follow the tail with one request per poll.

Chunks are written to the bucket rather than held in the producer's memory, so **a stream survives the process that started it**. A producer that dies mid-stream leaves a truncated log rather than nothing, and readers following it deliver what was written and then end.

Writes are what cost. Every flush is one billed `PutObject`, so `flushIntervalMs` (250ms) is the dial: doubling it roughly halves what a stream costs and adds that much to how far a resuming reader lags behind. Reads are close to free. Put the producer in the bucket's Availability Zone; AWS is explicit that reaching a directory bucket from another one is slower.

Liveness is judged on S3's clock at both ends, never on the caller's. The producer records that it is alive every `heartbeatMs` (5s), and a log that has gone unwritten for `deadAfterMs` (30s) has a dead producer, so `resumeStream` returns `null` and a reader already following it ends.

Nothing is deleted when a stream finishes, since a reader may still be draining it. **Give the bucket a [lifecycle expiration rule](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-objects-lifecycle.html) covering the adapter's `prefix`**, and mind the trap: lifecycle on a directory bucket does nothing at all unless the bucket policy grants `s3express:CreateSession` with `ReadWrite` to `lifecycle.s3.amazonaws.com`. Without that, objects accumulate silently.

A stream stops being resumable when its producer stops, which is `deadAfterMs` at the outside. How long the objects then sit in the bucket after that is the lifecycle rule's business, not the adapter's.

Each log carries a format version in its first bytes, so a reader refuses a log written by a version of this package it does not understand rather than misreading it. That matters during a rolling deploy, when two versions can meet the same stream.

There is no local emulator for appends, so the tests run against an in-memory bucket. To check that model against the real thing:

```sh
S3_EXPRESS_BUCKET=my-streams--use1-az4--x-s3 AWS_REGION=us-east-1 pnpm test integration
```

### Examples

Two runnable demos live in [`examples/`](./examples), one per adapter. Each starts a stream, disconnects the client part way through, resumes it by id to show the backlog replayed and the rest followed live, and then stops a second stream from somewhere else.

```sh
pnpm example:redis        # a throwaway Redis, started for you
pnpm example:s3-express   # an in-memory bucket, since no emulator implements appends
```

### Custom `StreamAdapter`

To put streams anywhere else, implement `StreamAdapter` directly. It is four methods, and both adapters above are built this way.

```ts
import type { StreamAdapter } from "ai-resumable-stream";

const adapter: StreamAdapter = {
  createStream(streamId, chunks, context) { ... },
  resumeStream(streamId) { ... },
  requestStop(streamId) { ... },
  onStopRequested(streamId, onStop) { ... },
};
```

`createStream` must discard any state left over from a previous stream with the same id, and `resumeStream` returns `null` when there is nothing to resume. Chunks are transported as opaque strings and their order must be preserved.

## Chunk types

The root export is generic over the chunk type and has no dependency on `ai`. The `ai-sdk` subpath binds it to the AI SDK:

```ts
import { createResumableUIMessageStream } from "ai-resumable-stream/ai-sdk";

const streams = createResumableUIMessageStream({ adapter });
```

This is exactly `createResumableStream({ adapter, codec: uiMessageChunkCodec })`. Chunks are validated on the way back, so a chunk written by an older version of your application is dropped rather than failing the resume.

To stream something else, supply a `StreamCodec`:

```ts
const codec: StreamCodec<MyChunk> = {
  encode: (chunk) => JSON.stringify(chunk),
  decode: (data) => JSON.parse(data) as MyChunk,
};
```

## Serverless

Persistence outlives the response, so the runtime must be told to wait for it:

```ts
import { waitUntil } from "@vercel/functions";

const streams = createResumableStream({ adapter, codec, waitUntil });
```

## API

### `createResumableStream({ adapter, codec, waitUntil?, generateId? })`

Returns `{ startStream, resumeStream, stopStream }`.

### `startStream(source, options?)`

`source` is a `ReadableStream<CHUNK>` or a factory `({ streamId, signal }) => ReadableStream<CHUNK>`.

Options: `streamId`, `abortController`, `onFinish`. All optional.

Returns `{ streamId, stream }`. Cancelling `stream` disconnects the client without stopping persistence, so the stream stays resumable.

### `resumeStream(streamId)`

Returns the chunks produced so far followed by those still to come, or `null` when the stream is unknown, already finished, or expired.

### `stopStream(streamId)`

Asks the producing process to stop. Resolves once the request is recorded, which may be before the producer has observed it.

## License

MIT
