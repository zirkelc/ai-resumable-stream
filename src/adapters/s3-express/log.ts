/**
 * The version every segment declares in its first record.
 *
 * A log lives in a bucket and outlives the process that wrote it, so a deploy can put two
 * versions of this package in front of the same stream. The marker is what lets a reader
 * refuse a log it does not understand instead of misreading it, and it is the only reason
 * the framing below can ever be changed.
 */
export const FORMAT_VERSION = 1;

/**
 * The record types a stream log holds. A record is a header line, and for a chunk the
 * exact payload bytes that follow it.
 *
 *     V<version>\n               the first record of every segment
 *     C<byteLength>\n<payload>   a chunk
 *     B\n                        the producer is alive but idle
 *     E\n                        the generation ended
 *     N<segment key>\n           the log continues in another object
 *
 * A chunk is framed by its byte length rather than by a separator, so nothing inside it
 * has to be escaped. The codec already produces JSON, and wrapping that in JSON again
 * costs about 1.4x the bytes and a second parse of every chunk on the way back.
 */
export const RecordType = {
  VERSION: `V`,
  CHUNK: `C`,
  BEAT: `B`,
  END: `E`,
  NEXT: `N`,
} as const;

export type RecordType = (typeof RecordType)[keyof typeof RecordType];

export type LogRecord =
  | { type: typeof RecordType.VERSION; version: number }
  | { type: typeof RecordType.CHUNK; data: string }
  | { type: typeof RecordType.BEAT }
  | { type: typeof RecordType.END }
  | { type: typeof RecordType.NEXT; key: string };

/**
 * What a reader should do once it has taken the chunks out of a batch of records.
 */
export const Outcome = {
  MORE: `more`,
  END: `end`,
  NEXT: `next`,
} as const;

export type Outcome = (typeof Outcome)[keyof typeof Outcome];

export type CollectResult =
  | { outcome: typeof Outcome.MORE }
  | { outcome: typeof Outcome.END }
  | { outcome: typeof Outcome.NEXT; key: string };

const NEWLINE = 0x0a;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * Joins encoded records so a batch of them is written by a single append.
 */
export function joinRecords(parts: Array<Uint8Array>): Uint8Array {
  const total = parts.reduce((sum, part) => sum + part.length, 0);
  const result = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
  }
  return result;
}

/**
 * Opens a segment. Written when the object is created, so a segment is never empty and a
 * reader always gets bytes, and with them S3's clock, from its very first read.
 */
export function encodeVersion(): Uint8Array {
  return encoder.encode(`${RecordType.VERSION}${FORMAT_VERSION}\n`);
}

export function encodeChunk(data: string): Uint8Array {
  const payload = encoder.encode(data);
  return joinRecords([encoder.encode(`${RecordType.CHUNK}${payload.length}\n`), payload]);
}

/**
 * A beat carries no payload, but it is still a real record: S3 rejects an append with an
 * empty body.
 */
export function encodeBeat(): Uint8Array {
  return encoder.encode(`${RecordType.BEAT}\n`);
}

/**
 * Written however a generation ends. A stream that failed is reported exactly like one
 * that completed, so the outcome itself is not recorded.
 */
export function encodeEnd(): Uint8Array {
  return encoder.encode(`${RecordType.END}\n`);
}

/**
 * Links a full segment to the one the log continues in. Segments form a chain, and this
 * record is the only thing that holds it together: nothing is ever listed, so a reader
 * finds the next object by being told its key rather than by looking for it.
 */
export function encodeNext(key: string): Uint8Array {
  return encoder.encode(`${RecordType.NEXT}${key}\n`);
}

/**
 * Reads whole records from the front of `bytes` and reports how many bytes they used.
 *
 * A trailing record that is not yet complete is left alone and `consumed` stops in front
 * of it, so a reader that starts again from there loses nothing. That matters because AWS
 * does not promise that a reader never sees a partly committed append.
 */
export function decodeRecords(bytes: Uint8Array): {
  records: Array<LogRecord>;
  consumed: number;
} {
  const records: Array<LogRecord> = [];
  let consumed = 0;

  while (consumed < bytes.length) {
    const newline = bytes.indexOf(NEWLINE, consumed);
    if (newline === -1) break;

    const header = decoder.decode(bytes.subarray(consumed, newline));
    const rest = header.slice(1);

    if (header[0] === RecordType.CHUNK) {
      const length = Number(rest);
      if (!Number.isInteger(length) || length < 0) {
        throw new Error(`Malformed chunk header in stream log: ${header}`);
      }

      const start = newline + 1;
      const end = start + length;
      /** The payload has not been written in full yet. */
      if (end > bytes.length) break;

      records.push({ type: RecordType.CHUNK, data: decoder.decode(bytes.subarray(start, end)) });
      consumed = end;
      continue;
    }

    if (header[0] === RecordType.VERSION) {
      const version = Number(rest);
      /**
       * Refusing is the point of the marker. A log written by a later version may frame
       * its records differently, and reading it as if it did not would hand the codec
       * nonsense rather than fail.
       */
      if (version !== FORMAT_VERSION) {
        throw new Error(`Stream log is format ${rest}, which this version cannot read`);
      }
      records.push({ type: RecordType.VERSION, version });
    } else if (header[0] === RecordType.BEAT) {
      records.push({ type: RecordType.BEAT });
    } else if (header[0] === RecordType.END) {
      records.push({ type: RecordType.END });
    } else if (header[0] === RecordType.NEXT) {
      records.push({ type: RecordType.NEXT, key: rest });
    } else {
      /**
       * Nothing can be recovered from here: the offset of every later record is unknown,
       * so the log has to be abandoned rather than silently truncated.
       */
      throw new Error(`Unknown record type in stream log: ${header[0]}`);
    }

    consumed = newline + 1;
  }

  return { records, consumed };
}

/**
 * Drains records into `chunks` and reports what the reader should do next. A link to the
 * next segment, like an end record, is always the last thing written to an object, so
 * nothing is skipped by stopping at one.
 */
export function collectChunks(records: Array<LogRecord>, chunks: Array<string>): CollectResult {
  for (const record of records) {
    if (record.type === RecordType.CHUNK) {
      chunks.push(record.data);
      continue;
    }
    if (record.type === RecordType.END) return { outcome: Outcome.END };
    if (record.type === RecordType.NEXT) return { outcome: Outcome.NEXT, key: record.key };
  }

  return { outcome: Outcome.MORE };
}
