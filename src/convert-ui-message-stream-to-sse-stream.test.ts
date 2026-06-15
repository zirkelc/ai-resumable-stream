import type { UIMessageChunk } from "ai";
import { Streams } from "ai-test-kit";
import { UIChunks } from "ai-test-kit/ui";
import { describe, expect, test } from "vitest";
import { convertSSEToUIMessageStream } from "./convert-sse-stream-to-ui-message-stream.js";
import { convertUIMessageToSSEStream } from "./convert-ui-message-stream-to-sse-stream.js";

describe(`convertUIMessageToSSEStream`, () => {
  test(`should convert UI message chunks to SSE-formatted strings`, async () => {
    // Arrange
    const chunks: Array<UIMessageChunk> = UIChunks.text(`Hello`, { id: `1` });
    const uiStream = Streams.from(chunks);

    // Act
    const sseStream = convertUIMessageToSSEStream(uiStream);
    const result = await Streams.toArray(sseStream);

    // Assert
    expect(result.length).toBe(4);
    expect(result[0]).toBe(`data: {"type":"text-start","id":"1"}\n\n`);
    expect(result[1]).toBe(`data: {"type":"text-delta","id":"1","delta":"Hello"}\n\n`);
    expect(result[2]).toBe(`data: {"type":"text-end","id":"1"}\n\n`);
    expect(result[3]).toBe(`data: [DONE]\n\n`);
  });

  test(`should handle empty stream`, async () => {
    // Arrange
    const chunks: Array<UIMessageChunk> = [];
    const uiStream = Streams.from(chunks);

    // Act
    const sseStream = convertUIMessageToSSEStream(uiStream);
    const result = await Streams.toArray(sseStream);

    // Assert
    expect(result.length).toBe(1);
    expect(result[0]).toBe(`data: [DONE]\n\n`);
  });

  test(`should call onFlush callback when stream ends`, async () => {
    // Arrange
    const chunks: Array<UIMessageChunk> = [
      UIChunks.textStart({ id: `1` }),
      UIChunks.textEnd({ id: `1` }),
    ];
    const uiStream = Streams.from(chunks);
    let completed = false;

    // Act
    const sseStream = convertUIMessageToSSEStream(uiStream, () => {
      completed = true;
    });
    await Streams.toArray(sseStream);

    // Assert
    expect(completed).toBe(true);
  });
});

describe(`round-trip conversion`, () => {
  test(`should preserve chunks through UI → SSE → UI conversion`, async () => {
    // Arrange
    const originalChunks: Array<UIMessageChunk> = [
      UIChunks.textStart({ id: `1` }),
      UIChunks.textDelta({ id: `1`, delta: `Hello` }),
      UIChunks.textDelta({ id: `1`, delta: ` world` }),
      UIChunks.textEnd({ id: `1` }),
    ];
    const uiStream = Streams.from(originalChunks);

    // Act
    const sseStream = convertUIMessageToSSEStream(uiStream);
    const restoredUiStream = convertSSEToUIMessageStream(sseStream);
    const result = await Streams.toArray(restoredUiStream);

    // Assert
    expect(result.length).toBe(4);
    expect(result).toEqual(originalChunks);
  });

  test(`should handle various chunk types`, async () => {
    // Arrange
    const originalChunks: Array<UIMessageChunk> = [
      UIChunks.startStep(),
      ...UIChunks.text(`Thinking...`, { id: `1` }),
      UIChunks.finishStep(),
      UIChunks.finish({ finishReason: `stop` }),
    ];
    const uiStream = Streams.from(originalChunks);

    // Act
    const sseStream = convertUIMessageToSSEStream(uiStream);
    const restoredUiStream = convertSSEToUIMessageStream(sseStream);
    const result = await Streams.toArray(restoredUiStream);

    // Assert
    expect(result.length).toBe(6);
    expect(result).toEqual(originalChunks);
  });
});
