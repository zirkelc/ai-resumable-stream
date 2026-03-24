import { type UIMessageChunk } from "ai";

export const SSE_DONE = `data: [DONE]\n\n`;

/**
 * Converts a single UI message chunk to SSE format.
 */
export function convertUIMessageChunkToSSE(chunk: UIMessageChunk): string {
  return `data: ${JSON.stringify(chunk)}\n\n`;
}

/**
 * Converts a UI message stream to an SSE stream.
 */
export function convertUIMessageToSSEStream(
  stream: ReadableStream<UIMessageChunk>,
  onComplete?: () => void,
): ReadableStream<string> {
  return stream.pipeThrough(
    new TransformStream({
      transform(chunk, controller) {
        controller.enqueue(convertUIMessageChunkToSSE(chunk));
      },
      flush(controller) {
        controller.enqueue(SSE_DONE);
        onComplete?.();
      },
    }),
  );
}
