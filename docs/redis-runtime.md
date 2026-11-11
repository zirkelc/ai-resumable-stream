# Redis runtime

The Redis adapter uses a runtime that is derived from [`resumable-stream`](https://github.com/vercel/resumable-stream) 2.2.13 (commit [`770cdeb`](https://github.com/vercel/resumable-stream/commit/770cdeb), file `src/runtime.ts`). The runtime is in [`src/adapters/redis/runtime/runtime.ts`](../src/adapters/redis/runtime/runtime.ts). This package does not depend on `resumable-stream` any more.

This document tells what is the same as upstream, what is different, and why.

## Why the runtime is part of this package

Upstream has bugs that crash a Node process. One of them crashed a production app. Redis dropped all connections while a stream ended, a teardown command of the producer failed, and nothing handled the rejection (see [#53](https://github.com/vercel/resumable-stream/issues/53)). This adapter cannot catch such a rejection, because it occurs inside upstream code that nothing awaits. Fixes for these bugs are open upstream, but no maintainer has reviewed them since the last release in September 2026. A patch of the dependency (for example with `pnpm patch`) applies only in this repository, not in the applications that install this package.

## License

`resumable-stream` is © 2025 Vercel, Inc. Its `package.json` declares the ISC license, and its `LICENSE` file contains the MIT license text. Both licenses require that the copyright notice and the permission notice stay with the code. This package keeps the notice in two places:

- a `/*! @license */` header at the top of `runtime.ts`, which the bundler keeps in the published files
- [`src/adapters/redis/runtime/LICENSE`](../src/adapters/redis/runtime/LICENSE), a copy of the upstream `LICENSE` file

## What is the same

The runtime uses the same keys, channels and messages as upstream:

| Name                             | Type    | Content                                                                          |
| -------------------------------- | ------- | -------------------------------------------------------------------------------- |
| `<prefix>:rs:sentinel:<id>`      | key     | `1` while the producer runs, `DONE` after it, expires after `ttlSeconds`         |
| `<prefix>:rs:request:<id>`       | channel | A consumer asks for the chunks with `{"listenerId": "..."}`                      |
| `<prefix>:rs:chunk:<listenerId>` | channel | The producer sends all chunks so far, then each new chunk, then the DONE message |

The `:rs` part stays. Thus a process that runs this version and a process that runs an earlier version of this package can resume the streams of each other, for example during a rolling deploy. The producer still honors `skipCharacters` in a request, as upstream does, but the consumer of this package never sends it.

The runtime includes these upstream changes after 2.2.12:

- The producer subscribes to the request channel before it sets the sentinel ([#41](https://github.com/vercel/resumable-stream/pull/41)).
- A consumer checks the sentinel from time to time and closes its stream if the DONE message is lost ([#47](https://github.com/vercel/resumable-stream/pull/47)).

## What is left out

The runtime has only the parts that the adapter uses. It leaves out these parts of upstream:

- `resumableStream`, the combined call that creates or resumes a stream. It counts listeners with `INCR` on the sentinel. The adapter has its own generation pointer and does not need it.
- `hasExistingStream`. The adapter does not use it.
- The default clients that upstream creates from `REDIS_URL` or `KV_URL`, and the `connect` calls. The adapter gets its clients from you and connects them itself. Upstream `index.js` also loads `redis` when it is imported, although its `package.json` declares no dependency ([#8](https://github.com/vercel/resumable-stream/issues/8)).
- The ioredis wrappers. The adapter uses the command signatures of node-redis v5 and v6, so ioredis was not supported before either.
- The debug logs. Upstream logs the chunk content when `DEBUG` has any value. Many applications set `DEBUG` for other libraries, so chat content could go to the logs ([#32](https://github.com/vercel/resumable-stream/issues/32)).

## What is different

The code marks each change with "Differs from upstream" and a link to the issue. Each change has a test in [`runtime.test.ts`](../src/adapters/redis/runtime/runtime.test.ts). The production crash also has a test in [`failure.test.ts`](../src/adapters/redis/failure.test.ts), with a proxy that closes the Redis connections while the producer marks the stream done.

### Producer

| Problem in upstream                                                                                                                                                                                                                                                                                | Change                                                                                                                                                                                                                                                            | Issue                                                                                                                  |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| The read loop has no rejection handler. If the source errors, or if a teardown command fails (for example on a dropped connection), the rejection is unhandled and ends the Node process. The teardown never finishes, the sentinel stays `1`, and the promise given to `waitUntil` never settles. | The loop has a handler. A source that errors errors the stream of the producer. The teardown runs only once and cannot reject. Each command can fail alone, and the `waitUntil` promise always settles. If the setup fails, the `waitUntil` promise also settles. | [#53](https://github.com/vercel/resumable-stream/issues/53), [#54](https://github.com/vercel/resumable-stream/pull/54) |
| A failed publish of a chunk to a consumer is unhandled, and the loop stops. The stream of the producer then hangs.                                                                                                                                                                                 | The loop catches a failed publish and reads on.                                                                                                                                                                                                                   | [#53](https://github.com/vercel/resumable-stream/issues/53)                                                            |
| The listener of the request channel is an async function. The Redis client ignores its promise, so a failed publish is an unhandled rejection. A message that is not JSON throws inside the client.                                                                                                | The listener is a plain function that cannot throw. It ignores a message that is not JSON.                                                                                                                                                                        | none                                                                                                                   |
| The loop awaits the publishes of a chunk before it reads the next chunk. While a consumer is attached, the producer reads one chunk per Redis round trip, and its own client gets the chunks more slowly too.                                                                                      | The loop does not await the publishes. The client sends the commands of one connection in the order that the code calls them, so the consumer still gets the chunks in order. The teardown waits for the publishes still in flight.                               | [#52](https://github.com/vercel/resumable-stream/issues/52)                                                            |
| The teardown unsubscribes every listener of the request channel on the shared subscriber.                                                                                                                                                                                                          | The teardown removes only the listener of the producer.                                                                                                                                                                                                           | none                                                                                                                   |
| The sentinel expires after a fixed 24 hours. A stream that runs longer is not resumable, and the keys of a producer that died stay that long.                                                                                                                                                      | The expiry is the adapter option `ttlSeconds`. The default stays 24 hours.                                                                                                                                                                                        | [#46](https://github.com/vercel/resumable-stream/issues/46), [#18](https://github.com/vercel/resumable-stream/pull/18) |

### Consumer

| Problem in upstream                                                                                                                                                                                                                                                                                                    | Change                                                                                                                                                                                                  | Issue                                                                                                                                                                                        |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The ack timer is an async function with no handler, so a failed Redis call in it is an unhandled rejection. It rejects only if the wall clock moved on by more than 1000 ms. When the timer fires at exactly 1000 ms, or when the clock goes back, the resume never settles. After `resolve(null)` it does not return. | The timer always settles the resume. It gives `null` if the stream is done, else an `AckTimeoutError`. The resume settles only once. The consumer ignores an answer that arrives after the timer fired. | [#50](https://github.com/vercel/resumable-stream/issues/50), [#43](https://github.com/vercel/resumable-stream/pull/43), [#21](https://github.com/zirkelc/ai-resumable-stream/issues/21) here |
| The wait for the first answer is fixed at 1000 ms. A busy producer or a slow network gives a false "nothing to resume".                                                                                                                                                                                                | The wait is the adapter option `ackTimeoutMs`. The default stays 1000 ms.                                                                                                                               | [#42](https://github.com/vercel/resumable-stream/issues/42)                                                                                                                                  |
| The message listener and the timer await the cleanup (the unsubscribe of the chunk channel) with no handler. A failed unsubscribe is an unhandled rejection.                                                                                                                                                           | The cleanup cannot reject. If the unsubscribe fails, the subscription is gone with the connection.                                                                                                      | none                                                                                                                                                                                         |
| The timeout is a plain `Error`, which a caller can only identify by its message.                                                                                                                                                                                                                                       | The timeout is an `AckTimeoutError`. Its message stays `Timeout waiting for ack`.                                                                                                                       | none                                                                                                                                                                                         |

### Commands

The runtime sets the expiry of the sentinel with the `expiration` option of node-redis v5 and v6, the same option that the adapter uses for its own keys. Upstream uses the older `EX` option. Redis gets the same command in both cases.

## Known limits

- If the command that marks the stream done fails, the sentinel stays `1` until it expires after `ttlSeconds`. The process does not crash. A resume through the pointer returns `null` at once, because the adapter deletes the pointer before the runtime marks the stream done. A resume with an explicit generation id waits `ackTimeoutMs` and then returns `null`.
- The producer does not wait for Redis before it reads the next chunk. If Redis is slow, the publishes wait in the memory of the client. The output of a language model is small, so this memory stays small too.
- This runtime does not fix these upstream issues:
  - A consumer that disconnects stays in the list of the producer, and the producer publishes each later chunk to it ([#48](https://github.com/vercel/resumable-stream/issues/48), [#49](https://github.com/vercel/resumable-stream/pull/49)).
  - The producer does not answer a resume that arrives after the stream is done ([#44](https://github.com/vercel/resumable-stream/issues/44)).

## Compare with upstream

To see the upstream code that a part of the runtime comes from, compare it with `src/runtime.ts` at commit [`770cdeb`](https://github.com/vercel/resumable-stream/blob/770cdeb/src/runtime.ts). If upstream releases a fix, check it against the changes above before you apply it here.
