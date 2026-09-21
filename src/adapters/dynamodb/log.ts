/**
 * The version every generation declares in its first item.
 *
 * A stream outlives the process that wrote it, so a deploy can put two versions of this
 * package in front of the same table. The marker is what lets a reader refuse a stream it
 * does not understand instead of misreading it.
 */
export const FORMAT_VERSION = 1;

/**
 * What a single item carries.
 *
 * `partial` says that the last payload in `chunks` is only the front of a chunk and
 * continues in the item that follows. It exists because an item holds at most 400 KB,
 * which a single chunk can exceed on its own.
 */
export type ItemBody = {
  chunks: Array<string>;
  partial: boolean;
};

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * The shortest budget a payload can be cut to. One UTF-8 character is four bytes at the
 * most, so anything smaller could fail to advance.
 */
const MIN_SPLIT_BYTES = 4;

export function byteLength(value: string): number {
  return encoder.encode(value).length;
}

/**
 * Cuts a payload into pieces of at most `maxBytes` each.
 *
 * The cuts land between UTF-8 sequences, never inside one, so every piece decodes on its
 * own and joining them back gives the original string.
 */
export function splitPayload(value: string, maxBytes: number): Array<string> {
  if (maxBytes < MIN_SPLIT_BYTES) {
    throw new Error(`maxBytes (${maxBytes}) must be at least ${MIN_SPLIT_BYTES}`);
  }

  const bytes = encoder.encode(value);
  if (bytes.length <= maxBytes) return [value];

  const pieces: Array<string> = [];
  let start = 0;

  while (start < bytes.length) {
    let end = Math.min(start + maxBytes, bytes.length);
    /** A byte of the form 10xxxxxx continues the character in front of it. */
    while (end < bytes.length && (bytes[end]! & 0xc0) === 0x80) end -= 1;

    pieces.push(decoder.decode(bytes.subarray(start, end)));
    start = end;
  }

  return pieces;
}

/**
 * Groups payloads into the items they are written as, keeping each one inside `maxBytes`.
 *
 * Whole payloads are packed together for as long as they fit. One that is too large for
 * an item of its own is cut, and every item but the last of that payload is marked
 * partial, so a reader knows to join them.
 */
export function splitIntoItems(payloads: Array<string>, maxBytes: number): Array<ItemBody> {
  const items: Array<ItemBody> = [];
  let current: Array<string> = [];
  let used = 0;

  function close(partial: boolean): void {
    if (current.length === 0) return;
    items.push({ chunks: current, partial });
    current = [];
    used = 0;
  }

  for (const payload of payloads) {
    const size = byteLength(payload);

    if (used + size <= maxBytes) {
      current.push(payload);
      used += size;
      continue;
    }

    /** It does not fit beside what is already buffered, but it may fit on its own. */
    close(false);

    if (size <= maxBytes) {
      current.push(payload);
      used = size;
      continue;
    }

    const pieces = splitPayload(payload, maxBytes);
    for (const piece of pieces.slice(0, -1)) {
      items.push({ chunks: [piece], partial: true });
    }
    current.push(pieces.at(-1)!);
    used = byteLength(pieces.at(-1)!);
  }

  close(false);
  return items;
}

export type Assembler = {
  /**
   * Returns the chunks an item completes. A payload that is still being carried is held
   * back until the item that finishes it arrives.
   */
  take(body: ItemBody): Array<string>;
};

/**
 * Joins payloads that were cut across items back into the chunks they came from.
 */
export function createAssembler(): Assembler {
  let carry: string | undefined;

  return {
    take({ chunks, partial }) {
      if (chunks.length === 0) return [];

      const payloads = [...chunks];
      if (carry !== undefined) {
        payloads[0] = carry + payloads[0];
        carry = undefined;
      }

      if (partial) carry = payloads.pop();

      return payloads;
    },
  };
}
