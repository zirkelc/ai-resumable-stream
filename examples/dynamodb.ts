/**
 * A resumable stream on DynamoDB, end to end, in one process.
 *
 *     pnpm example:dynamodb
 *
 * `dynalite` serves the DynamoDB API over an in-memory LevelDB and `ai-test-kit` stands
 * in for a provider, so nothing has to be installed, paid for, or left running. The
 * client, the commands and the conditional writes are the real ones. In an application
 * the client, the table and the model are your own, and nothing else changes.
 *
 * In an application the adapter is built from a client and a table:
 *
 *     import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
 *     import { createDynamoDBAdapter } from "ai-resumable-stream/adapters/dynamodb";
 *
 *     const adapter = createDynamoDBAdapter({
 *       client: new DynamoDBClient({ region: "us-east-1" }),
 *       tableName: "streams",
 *     });
 */
import { streamText, type UIMessageChunk } from "ai";
import { Language, MockLanguageModel } from "ai-test-kit/language";
import { createLocalDynamoDB } from "../src/__tests__/local-dynamodb.js";
import { createDynamoDBAdapter } from "../src/adapters/dynamodb/index.js";
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
  const table = await createLocalDynamoDB();

  /**
   * Writing a stream outlives the request that started it, so the work is handed to
   * `waitUntil` rather than awaited. A serverless runtime passes its own; here the
   * promises are collected so the process stays alive until the last item is written.
   */
  const pending: Array<Promise<unknown>> = [];

  /**
   * Tighter than the defaults so the demo does not sit waiting. Against a real table
   * every interval here is a billed request, and the defaults (250ms, 500ms, 1s) are the
   * ones to keep.
   */
  const streams = createResumableUIMessageStream({
    adapter: createDynamoDBAdapter({
      client: table.client,
      tableName: table.tableName,
      flushIntervalMs: 50,
      batchSize: 1,
      resumePollIntervalMs: 50,
      stopPollIntervalMs: 50,
    }),
    waitUntil: (promise) => {
      pending.push(promise);
    },
  });

  try {
    /**
     * A client starts a stream and reads part of it, then loses its connection. Chunks
     * keep being written to the table: the producer is not tied to the client that asked
     * for it, and unlike Redis the chunks are not held in its memory either.
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
     * A later request picks the same stream up by its id. The backlog comes back from the
     * stream's own partition, and the rest is followed one query per poll.
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

    await Promise.allSettled(pending);

    /**
     * What the two streams cost. A write of an item is billed at roughly five times a
     * read of one, so the flush interval is the dial that matters. The reads here are
     * inflated by this demo polling ten times faster than the defaults do.
     */
    const { reads, writes } = table.counts();
    log(`table`, `${writes} writes and ${reads} reads for two streams`);
    for (const key of (await table.keys()).sort()) log(`table`, key);
  } finally {
    await Promise.allSettled(pending);
    await table.stop();
  }
}

await main();
