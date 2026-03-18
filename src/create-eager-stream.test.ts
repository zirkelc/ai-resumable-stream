import { describe, expect, test, vi } from "vitest";
import { convertArrayToStream, convertStreamToArray } from "ai-stream-utils";
import { createEagerStream } from "./create-eager-stream.js";

describe("createEagerStream", () => {
  test("returns all chunks from source stream in order", async () => {
    // Arrange
    const chunks = ["a", "b", "c", "d", "e"];
    const source = convertArrayToStream(chunks);

    // Act
    const buffered = createEagerStream(source);
    const result = await convertStreamToArray(buffered);

    // Assert
    expect(result).toEqual(chunks);
  });

  test("handles empty source stream", async () => {
    // Arrange
    const source = convertArrayToStream<string>([]);

    // Act
    const buffered = createEagerStream(source);
    const result = await convertStreamToArray(buffered);

    // Assert
    expect(result).toEqual([]);
  });

  test("calls onComplete when stream is fully consumed", async () => {
    // Arrange
    const onComplete = vi.fn();
    const chunks = ["a", "b", "c"];
    const source = convertArrayToStream(chunks);

    // Act
    const buffered = createEagerStream(source, onComplete);
    await convertStreamToArray(buffered);

    // Assert
    expect(onComplete).toHaveBeenCalledTimes(1);
  });

  test("calls onComplete when consumer cancels stream", async () => {
    // Arrange
    const onComplete = vi.fn();
    const chunks = ["a", "b", "c", "d", "e"];
    const source = convertArrayToStream(chunks);

    // Act
    const buffered = createEagerStream(source, onComplete);
    const reader = buffered.getReader();
    await reader.read();
    await reader.cancel();

    // Assert
    expect(onComplete).toHaveBeenCalledTimes(1);
  });

  test("propagates errors from source stream to consumer", async () => {
    // Arrange
    let pullCount = 0;
    const source = new ReadableStream<string>({
      async pull(controller) {
        pullCount++;
        if (pullCount === 1) {
          controller.enqueue("a");
        } else {
          throw new Error("source error");
        }
      },
    });

    // Act
    const buffered = createEagerStream(source);
    const result = convertStreamToArray(buffered);

    // Assert
    await expect(result).rejects.toThrow();
  });

  test("eagerly drains source even if consumer reads slowly", async () => {
    // Arrange
    const sourceReadCount = { value: 0 };
    const chunks = ["a", "b", "c", "d", "e"];
    const source = new ReadableStream<string>({
      async pull(controller) {
        if (sourceReadCount.value < chunks.length) {
          controller.enqueue(chunks[sourceReadCount.value]!);
          sourceReadCount.value++;
        } else {
          controller.close();
        }
      },
    });

    // Act
    const buffered = createEagerStream(source);
    const reader = buffered.getReader();

    /** Read first chunk */
    await reader.read();

    /** Wait a bit for eager draining to complete */
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Assert
    /** Source should be fully consumed even though consumer only read one chunk */
    expect(sourceReadCount.value).toBe(5);

    /** Clean up - read remaining chunks */
    await reader.cancel();
  });
});
