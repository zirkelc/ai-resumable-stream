import { describe, expect, test } from "vitest";
import {
  collectChunks,
  decodeRecords,
  encodeBeat,
  encodeChunk,
  encodeEnd,
  encodeNext,
  encodeVersion,
  FORMAT_VERSION,
  joinRecords,
  Outcome,
  RecordType,
} from "./log.js";

const encoder = new TextEncoder();

function chunksOf(bytes: Uint8Array): Array<string> {
  const { records } = decodeRecords(bytes);
  const chunks: Array<string> = [];
  collectChunks(records, chunks);
  return chunks;
}

describe(`stream log`, () => {
  test(`should read back every record it wrote`, () => {
    // Arrange
    const bytes = joinRecords([
      encodeVersion(),
      encodeChunk(`one`),
      encodeBeat(),
      encodeChunk(`two`),
      encodeEnd(),
    ]);

    // Act
    const { records, consumed } = decodeRecords(bytes);

    // Assert
    expect(records).toEqual([
      { type: RecordType.VERSION, version: FORMAT_VERSION },
      { type: RecordType.CHUNK, data: `one` },
      { type: RecordType.BEAT },
      { type: RecordType.CHUNK, data: `two` },
      { type: RecordType.END },
    ]);
    expect(consumed).toBe(bytes.length);
  });

  test(`should carry a payload that contains its own separator`, () => {
    // Arrange
    const data = `{"delta":"line\nbreak"}\nC5\nfake`;

    // Act
    const chunks = chunksOf(encodeChunk(data));

    // Assert
    expect(chunks).toEqual([data]);
  });

  test(`should carry a payload of multi-byte characters`, () => {
    // Arrange
    const data = `héllo 🌍 世界`;

    // Act
    const chunks = chunksOf(encodeChunk(data));

    // Assert
    expect(chunks).toEqual([data]);
  });

  test(`should leave a record whose header is not finished for the next read`, () => {
    // Arrange
    const complete = joinRecords([encodeChunk(`one`), encodeChunk(`two`)]);
    const torn = complete.subarray(0, complete.length - 5);

    // Act
    const { records, consumed } = decodeRecords(torn);

    // Assert
    expect(records).toEqual([{ type: RecordType.CHUNK, data: `one` }]);
    expect(consumed).toBe(encodeChunk(`one`).length);
  });

  test(`should leave a record whose payload is not finished for the next read`, () => {
    // Arrange
    const record = encodeChunk(`a long payload`);
    const torn = record.subarray(0, record.length - 3);

    // Act
    const { records, consumed } = decodeRecords(torn);

    // Assert
    expect(records.length).toBe(0);
    expect(consumed).toBe(0);
  });

  test(`should refuse a log written in a format it does not know`, () => {
    // Arrange
    const bytes = encoder.encode(`${RecordType.VERSION}${FORMAT_VERSION + 1}\n`);

    // Act
    const decode = () => decodeRecords(bytes);

    // Assert
    expect(decode).toThrow();
  });

  test(`should refuse a record type it does not know`, () => {
    // Arrange
    const bytes = encoder.encode(`Z\n`);

    // Act
    const decode = () => decodeRecords(bytes);

    // Assert
    expect(decode).toThrow();
  });

  test(`should stop collecting at the end of a generation`, () => {
    // Arrange
    const bytes = joinRecords([encodeChunk(`one`), encodeEnd(), encodeChunk(`unreachable`)]);
    const { records } = decodeRecords(bytes);
    const chunks: Array<string> = [];

    // Act
    const result = collectChunks(records, chunks);

    // Assert
    expect(result).toEqual({ outcome: Outcome.END });
    expect(chunks).toEqual([`one`]);
  });

  test(`should report where the log continues`, () => {
    // Arrange
    const bytes = joinRecords([encodeChunk(`one`), encodeNext(`streams/abc/1`)]);
    const { records } = decodeRecords(bytes);
    const chunks: Array<string> = [];

    // Act
    const result = collectChunks(records, chunks);

    // Assert
    expect(result).toEqual({ outcome: Outcome.NEXT, key: `streams/abc/1` });
    expect(chunks).toEqual([`one`]);
  });

  test(`should ask for more when a batch holds only chunks`, () => {
    // Arrange
    const bytes = joinRecords([encodeChunk(`one`), encodeBeat()]);
    const { records } = decodeRecords(bytes);
    const chunks: Array<string> = [];

    // Act
    const result = collectChunks(records, chunks);

    // Assert
    expect(result).toEqual({ outcome: Outcome.MORE });
    expect(chunks).toEqual([`one`]);
  });
});
