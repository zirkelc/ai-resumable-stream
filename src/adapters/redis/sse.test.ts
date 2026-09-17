import { JsonToSseTransformStream, type UIMessageChunk } from "ai";
import { describe, expect, test } from "vitest";
import { chunksToSSE, sseToChunks } from "./sse.js";

/**
 * Chunks that would break a naive framing: embedded newlines, a carriage return, a
 * payload that looks like the terminator event, and an empty delta.
 */
const chunks: Array<UIMessageChunk> = [
  { type: `text-delta`, id: `1`, delta: `line1\nline2\n\nline3` },
  { type: `text-delta`, id: `1`, delta: `carriage\r\nreturn` },
  /** JSON.stringify leaves these unescaped, but they are not SSE line terminators. */
  { type: `text-delta`, id: `1`, delta: `separator \u2028 and \u2029` },
  { type: `text-delta`, id: `1`, delta: `data: [DONE]\n\n` },
  { type: `text-delta`, id: `1`, delta: `emoji \u{1F389} and "quotes"` },
  { type: `text-delta`, id: `1`, delta: `` },
];

function fromArray<T>(items: Array<T>): ReadableStream<T> {
  return new ReadableStream({
    start(controller) {
      items.forEach((item) => controller.enqueue(item));
      controller.close();
    },
  });
}

async function toArray<T>(stream: ReadableStream<T>): Promise<Array<T>> {
  const items: Array<T> = [];
  const reader = stream.getReader();
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    items.push(value);
  }
  return items;
}

function encode(input: Array<UIMessageChunk>): Promise<Array<string>> {
  return toArray(fromArray(input.map((chunk) => JSON.stringify(chunk))).pipeThrough(chunksToSSE()));
}

describe(`chunksToSSE`, () => {
  test(`should produce the same bytes as the AI SDK`, async () => {
    // Arrange
    const expected = await toArray(fromArray(chunks).pipeThrough(new JsonToSseTransformStream()));

    // Act
    const actual = await encode(chunks);

    // Assert
    expect(actual.join(``)).toBe(expected.join(``));
  });
});

describe(`sseToChunks`, () => {
  /**
   * `resumable-stream` concatenates every chunk it has seen and re-slices the result
   * at an arbitrary offset, so chunk boundaries never survive the transport.
   */
  test.each([1, 3, 7, 64, 100_000])(
    `should recover every chunk when re-sliced at %i characters`,
    async (size) => {
      // Arrange
      const sse = (await encode(chunks)).join(``);
      const pieces: Array<string> = [];
      for (let index = 0; index < sse.length; index += size) {
        pieces.push(sse.slice(index, index + size));
      }

      // Act
      const recovered = await toArray(fromArray(pieces).pipeThrough(sseToChunks()));

      // Assert
      expect(recovered.map((chunk) => JSON.parse(chunk))).toEqual(chunks);
    },
  );

  test(`should ignore the done event`, async () => {
    // Arrange
    const sse = (await encode([])).join(``);

    // Act
    const recovered = await toArray(fromArray([sse]).pipeThrough(sseToChunks()));

    // Assert
    expect(recovered.length).toBe(0);
  });

  test(`should ignore the sentinel resumable-stream publishes on completion`, async () => {
    // Arrange
    const doneMessage = `\n\n\nDONE_SENTINEL_hasdfasudfyge374%$%^$EDSATRTYFtydryrte\n`;
    const sse = (await encode(chunks)).join(``);

    // Act
    const recovered = await toArray(fromArray([sse, doneMessage]).pipeThrough(sseToChunks()));

    // Assert
    expect(recovered.map((chunk) => JSON.parse(chunk))).toEqual(chunks);
  });
});
