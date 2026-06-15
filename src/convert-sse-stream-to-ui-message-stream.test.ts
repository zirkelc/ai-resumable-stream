import { Streams } from "ai-test-kit";
import { UIChunks } from "ai-test-kit/ui";
import { describe, expect, test } from "vitest";
import { convertSSEToUIMessageStream } from "./convert-sse-stream-to-ui-message-stream.js";

describe(`convertSSEToUIMessageStream`, () => {
  test(`should convert SSE-formatted strings to UI message chunks`, async () => {
    // Arrange
    const sseStrings = [
      `data: {"type":"text-start","id":"1"}\n\n`,
      `data: {"type":"text-delta","id":"1","delta":"Hello"}\n\n`,
      `data: {"type":"text-end","id":"1"}\n\n`,
    ];
    const sseStream = Streams.from(sseStrings);

    // Act
    const uiStream = convertSSEToUIMessageStream(sseStream);
    const result = await Streams.toArray(uiStream);

    // Assert
    expect(result.length).toBe(3);
    expect(result[0]).toEqual(UIChunks.textStart({ id: `1` }));
    expect(result[1]).toEqual(UIChunks.textDelta({ id: `1`, delta: `Hello` }));
    expect(result[2]).toEqual(UIChunks.textEnd({ id: `1` }));
  });

  test(`should handle empty stream`, async () => {
    // Arrange
    const sseStrings: Array<string> = [];
    const sseStream = Streams.from(sseStrings);

    // Act
    const uiStream = convertSSEToUIMessageStream(sseStream);
    const result = await Streams.toArray(uiStream);

    // Assert
    expect(result.length).toBe(0);
  });

  test(`should call onFlush callback when stream ends`, async () => {
    // Arrange
    const sseStrings = [
      `data: {"type":"text-start","id":"1"}\n\n`,
      `data: {"type":"text-end","id":"1"}\n\n`,
    ];
    const sseStream = Streams.from(sseStrings);
    let completed = false;

    // Act
    const uiStream = convertSSEToUIMessageStream(sseStream, () => {
      completed = true;
    });
    await Streams.toArray(uiStream);

    // Assert
    expect(completed).toBe(true);
  });
});
