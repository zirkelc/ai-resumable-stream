/**
 * A resumable stream on S3 Express One Zone, end to end, in one process.
 *
 *     pnpm example:s3-express
 *
 * No local emulator implements S3 Express appends: LocalStack closed the request as not
 * planned and open-source MinIO has no server-side append. So this runs the adapter
 * against the in-memory bucket the tests use, which models append offsets, unsatisfiable
 * ranges and S3's clock. `ai-test-kit` stands in for a provider. Everything below the
 * adapter is identical against a real bucket and a real model.
 *
 * In an application the adapter is built from a client and a directory bucket:
 *
 *     import { S3Client } from "@aws-sdk/client-s3";
 *     import { createS3ExpressAdapter } from "ai-resumable-stream/adapters/s3-express";
 *
 *     const adapter = createS3ExpressAdapter({
 *       client: new S3Client({ region: "us-east-1" }),
 *       bucket: "my-streams--use1-az4--x-s3",
 *     });
 */
import { streamText, type UIMessageChunk } from "ai";
import { Language, MockLanguageModel } from "ai-test-kit/language";
import { createFakeS3 } from "../src/__tests__/fake-s3.js";
import { createStreamAdapter } from "../src/adapters/s3-express/adapter.js";
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
  const bucket = createFakeS3();

  /**
   * Writing a stream outlives the request that started it, so the work is handed to
   * `waitUntil` rather than awaited. A serverless runtime passes its own; here the
   * promises are collected so the process stays alive until the last chunk is written.
   */
  const pending: Array<Promise<unknown>> = [];

  /**
   * Tighter than the defaults so the demo does not sit waiting. Against a real bucket
   * every interval here is a billed request, and the defaults (250ms, 500ms, 1s) are the
   * ones to keep.
   */
  const streams = createResumableUIMessageStream({
    adapter: createStreamAdapter(bucket, {
      flushIntervalMs: 50,
      batchSize: 1,
      resumePollIntervalMs: 50,
      stopPollIntervalMs: 50,
    }),
    waitUntil: (promise) => {
      pending.push(promise);
    },
  });

  /**
   * A client starts a stream and reads part of it, then loses its connection. Chunks keep
   * being written to the bucket: the producer is not tied to the client that asked for it,
   * and unlike Redis the chunks are not held in its memory either.
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
   * A later request picks the same stream up by its id. The backlog comes back in a
   * single ranged read, and the rest is followed one request per poll.
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
  const second = streamText({ model, prompt: `Say hello`, abortSignal: abortController.signal });
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
   * What the two streams cost. Writes are the ones that matter on a real bucket, at
   * roughly 4x the price of a read. The reads here are inflated by this demo polling
   * twenty times faster than the defaults do.
   */
  const { reads, writes } = bucket.counts();
  log(`bucket`, `${writes} writes and ${reads} reads for two streams`);
  for (const key of bucket.keys()) log(`bucket`, key);
}

await main();
