/**
 * A resumable stream on Redis, end to end, in one process.
 *
 *     pnpm example:redis
 *
 * `redis-memory-server` starts a throwaway Redis and `ai-test-kit` stands in for a
 * provider, so nothing has to be installed, paid for, or left running. In an application
 * the two clients and the model are your own, and nothing else changes.
 */
import { streamText, type UIMessageChunk } from "ai";
import { Language, MockLanguageModel } from "ai-test-kit/language";
import { createClient } from "redis";
import { RedisMemoryServer } from "redis-memory-server";
import { createRedisAdapter } from "../src/adapters/redis/index.js";
import { createResumableUIMessageStream } from "../src/ai-sdk/index.js";

/** Stands in for a provider. Slow enough that a client can disconnect mid-answer. */
const model = MockLanguageModel.from({
  doStream: {
    chunks: [
      ...Language.streamText(`Hello there, how can I help?`, { separator: ` ` }),
      Language.streamFinish(),
    ],
    chunkDelayInMs: 120,
  },
});

function log(actor: string, message: string) {
  console.log(`${actor.padEnd(8)} ${message}`);
}

function describe(chunk: UIMessageChunk): string {
  return chunk.type === `text-delta` ? `"${chunk.delta}"` : chunk.type;
}

async function main() {
  const server = await RedisMemoryServer.create();
  const url = `redis://${await server.getHost()}:${await server.getPort()}`;
  const publisher = createClient({ url });
  const subscriber = createClient({ url });

  /**
   * Persisting a stream outlives the request that started it, so the work is handed to
   * `waitUntil` rather than awaited. A serverless runtime passes its own; here the
   * promises are collected so the clients are not closed underneath them.
   */
  const pending: Array<Promise<unknown>> = [];

  const streams = createResumableUIMessageStream({
    adapter: createRedisAdapter({ publisher, subscriber }),
    waitUntil: (promise) => {
      pending.push(promise);
    },
  });

  try {
    /**
     * A client starts a stream and reads part of it, then loses its connection. Chunks
     * keep being persisted: the producer is not tied to the client that asked for it.
     */
    const answer = streamText({ model, prompt: `Say hello` });
    const { stream } = await streams.startStream(answer.toUIMessageStream(), {
      streamId: `chat-1`,
    });

    const reader = stream.getReader();
    for (let seen = 0; seen < 5; seen += 1) {
      const { value } = await reader.read();
      log(`client`, describe(value!));
    }
    await reader.cancel();
    log(`client`, `disconnected`);

    /**
     * A later request picks the same stream up by its id. It replays what the client
     * missed and then follows the rest live.
     */
    const resumed = await streams.resumeStream({ streamId: `chat-1` });
    if (!resumed) throw new Error(`chat-1 should still be running`);

    for await (const chunk of resumed) log(`resume`, describe(chunk));
    log(`resume`, `ended`);

    /** Once a stream is over there is nothing to resume. */
    log(`resume`, `resuming again gives ${await streams.resumeStream({ streamId: `chat-1` })}`);

    /**
     * Stopping reaches the producer from anywhere, which is the point: the request that
     * says stop is never the request that started the stream. Handing the signal to
     * `streamText` aborts the provider request itself rather than waiting for the
     * cancellation to travel back up the pipe.
     */
    const abortController = new AbortController();
    const second = streamText({
      model,
      prompt: `Say hello`,
      abortSignal: abortController.signal,
    });
    const { stream: live } = await streams.startStream(second.toUIMessageStream(), {
      streamId: `chat-2`,
      abortController,
    });

    const liveReader = live.getReader();
    let received = 0;
    while (received < 3) {
      const { value } = await liveReader.read();
      log(`client`, describe(value!));
      received += 1;
    }

    log(`stop`, `stopping chat-2`);
    await streams.stopStream({ streamId: `chat-2` });

    while (true) {
      const { done, value } = await liveReader.read();
      if (done) break;
      log(`client`, describe(value!));
      received += 1;
    }
    log(`stop`, `producer stopped after ${received} chunks`);
  } finally {
    await Promise.allSettled(pending);
    publisher.destroy();
    subscriber.destroy();
    await server.stop();
  }
}

await main();
