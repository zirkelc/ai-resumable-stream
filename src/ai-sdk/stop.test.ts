import {
  streamText,
  toUIMessageStream,
  type UIMessage,
  type UIMessageStreamOnEndCallback,
} from "ai";
import { Errors } from "ai-test-kit";
import { Language, MockLanguageModel } from "ai-test-kit/language";
import { describe, expect, test } from "vitest";
import { FAST_POLLING } from "../__tests__/conformance-suite.js";
import { createFakeS3 } from "../__tests__/fake-s3.js";
import { createStreamAdapter } from "../adapters/s3-express/adapter.js";
import { createResumableUIMessageStream } from "./index.js";

type OnEndEvent = Parameters<UIMessageStreamOnEndCallback<UIMessage>>[0];

/**
 * A model that streams one text delta and then waits, and errors with an abort error when
 * its signal aborts, the way a provider's HTTP response body does.
 */
function createHangingModel() {
  return MockLanguageModel.from({
    doStream: async ({ abortSignal }) =>
      Language.streamResult(
        new ReadableStream({
          start(controller) {
            for (const part of [Language.streamStart(), ...Language.streamText(`Hello`)]) {
              controller.enqueue(part);
            }
            abortSignal?.addEventListener(`abort`, () => controller.error(Errors.abort()), {
              once: true,
            });
          },
        }),
      ),
  });
}

function startGeneration(abortController: AbortController) {
  let resolveEnd!: (event: OnEndEvent) => void;
  const ended = new Promise<OnEndEvent>((resolve) => {
    resolveEnd = resolve;
  });
  const result = streamText({
    model: createHangingModel(),
    prompt: `Hi`,
    abortSignal: abortController.signal,
  });
  const stream = toUIMessageStream({ stream: result.stream, onEnd: resolveEnd });
  return { stream, ended };
}

async function readUntilText(iterator: AsyncIterator<{ type: string }>) {
  while (true) {
    const { done, value } = await iterator.next();
    if (done || value.type === `text-delta`) return;
  }
}

describe(`stopping a streamText generation`, () => {
  test(`should report isAborted without the library`, async () => {
    // Arrange
    const abortController = new AbortController();
    const { stream, ended } = startGeneration(abortController);
    const iterator = stream[Symbol.asyncIterator]();
    await readUntilText(iterator);

    // Act
    abortController.abort();
    while (!(await iterator.next()).done);
    const event = await ended;

    // Assert
    expect(event.isAborted).toBe(true);
  });

  test(`should report isAborted when stopped through the library`, async () => {
    // Arrange
    const adapter = createStreamAdapter(createFakeS3(), FAST_POLLING);
    const context = createResumableUIMessageStream({ adapter });
    const abortController = new AbortController();
    const { stream: source, ended } = startGeneration(abortController);
    const { stream } = await context.startStream(source, { streamId: `chat`, abortController });
    const iterator = stream[Symbol.asyncIterator]();
    await readUntilText(iterator);

    // Act
    await context.stopStream({ streamId: `chat` });
    const received: Array<string> = [];
    for await (const chunk of { [Symbol.asyncIterator]: () => iterator }) {
      received.push(chunk.type);
    }
    const event = await ended;

    // Assert
    expect(event.isAborted).toBe(true);
    const text = event.responseMessage.parts.find((part) => part.type === `text`);
    expect(text?.text).toBe(`Hello`);
    expect(received.at(-1)).toBe(`abort`);
  });
});
