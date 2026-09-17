import { asSchema, type UIMessageChunk, uiMessageChunkSchema } from "ai";
import type { StreamCodec } from "../adapter.js";
import { createResumableStream, type CreateResumableStreamOptions } from "../core.js";

const schema = asSchema(uiMessageChunkSchema);

/**
 * Serializes UI message chunks as JSON and validates them on the way back.
 *
 * Chunks are validated rather than trusted because they may have been written by an
 * older version of the application. A chunk that no longer parses is dropped instead
 * of failing the resume.
 */
export const uiMessageChunkCodec: StreamCodec<UIMessageChunk> = {
  encode(chunk) {
    return JSON.stringify(chunk);
  },
  async decode(data) {
    let value: unknown;
    try {
      value = JSON.parse(data);
    } catch {
      return undefined;
    }

    const result = await schema.validate!(value);
    return result.success ? result.value : undefined;
  },
};

export type CreateResumableUIMessageStreamOptions = Omit<
  CreateResumableStreamOptions<UIMessageChunk>,
  `codec`
>;

/**
 * Creates a resumable stream context for AI SDK UI message streams.
 */
export function createResumableUIMessageStream(options: CreateResumableUIMessageStreamOptions) {
  return createResumableStream({ ...options, codec: uiMessageChunkCodec });
}
