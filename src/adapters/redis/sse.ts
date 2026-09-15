/**
 * `resumable-stream` transports a stream of strings that it is free to concatenate and
 * re-slice, so chunks have to carry their own boundaries. SSE framing supplies them:
 * chunks are JSON, which never contains a raw newline, so a blank line always ends one.
 */

const DATA_PREFIX = `data: `;
const CHUNK_SEPARATOR = `\n\n`;
const DONE = `[DONE]`;

/**
 * Frames each chunk as an SSE event and terminates the stream with a done event.
 */
export function chunksToSSE(): TransformStream<string, string> {
  return new TransformStream({
    transform(chunk, controller) {
      controller.enqueue(`${DATA_PREFIX}${chunk}${CHUNK_SEPARATOR}`);
    },
    flush(controller) {
      controller.enqueue(`${DATA_PREFIX}${DONE}${CHUNK_SEPARATOR}`);
    },
  });
}

/**
 * Recovers chunks from SSE framing, tolerating arbitrary chunk boundaries.
 */
export function sseToChunks(): TransformStream<string, string> {
  let buffer = ``;

  return new TransformStream({
    transform(chunk, controller) {
      buffer += chunk;

      while (true) {
        const end = buffer.indexOf(CHUNK_SEPARATOR);
        if (end === -1) break;

        const event = buffer.slice(0, end);
        buffer = buffer.slice(end + CHUNK_SEPARATOR.length);

        if (!event.startsWith(DATA_PREFIX)) continue;

        const data = event.slice(DATA_PREFIX.length);
        if (data === DONE) continue;

        controller.enqueue(data);
      }
    },
  });
}
