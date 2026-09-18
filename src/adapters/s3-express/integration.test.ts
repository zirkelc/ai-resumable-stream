import { randomUUID } from "node:crypto";
import { S3Client } from "@aws-sdk/client-s3";
import { describe, expect, test } from "vitest";
import { defineConformanceTests } from "../../__tests__/conformance-suite.js";
import { createS3ExpressAdapter } from "./adapter.js";
import { createS3Operations, ObjectExistsError, WriteOffsetMismatchError } from "./client.js";
import { encodeChunk, joinRecords } from "./log.js";

/**
 * The same contract every other adapter meets, run against a real directory bucket.
 *
 * Nothing here runs by default: it needs an AWS account, and CI has no credentials. It
 * exists because no local emulator implements appends, so the in-memory bucket the unit
 * tests use is a model of S3 rather than S3. This is what checks the model.
 *
 *     S3_EXPRESS_BUCKET=my-streams--use1-az4--x-s3 AWS_REGION=us-east-1 pnpm test integration
 *
 * Objects are written under a fresh prefix each run. Give the bucket a lifecycle
 * expiration rule, or the runs accumulate.
 */
const bucket = process.env.S3_EXPRESS_BUCKET;

if (!bucket) {
  test.skip(`needs S3_EXPRESS_BUCKET and AWS credentials`, () => {});
} else {
  const client = new S3Client({});
  const prefix = `test/${randomUUID()}`;

  /**
   * Faster than the defaults so the suite finishes, but not as fast as the in-memory runs:
   * every interval here is a real request.
   */
  const LIVE_POLLING = {
    flushIntervalMs: 0,
    batchSize: 1,
    resumePollIntervalMs: 100,
    stopPollIntervalMs: 100,
  };

  defineConformanceTests({
    name: `s3-express (live)`,
    createAdapter: async () =>
      createS3ExpressAdapter({
        client,
        bucket,
        prefix: `${prefix}/${randomUUID()}`,
        ...LIVE_POLLING,
      }),
    teardown: async () => {
      client.destroy();
    },
  });

  /**
   * The behaviour the adapter rests on that AWS does not document, or documents only as a
   * recommendation. A change to any of these breaks the adapter silently, so they are
   * asserted rather than assumed.
   */
  describe(`s3-express (live) storage`, () => {
    const s3 = createS3Operations({ client, bucket });

    test(`should return an appended record to a reader straight away`, async () => {
      // Arrange
      const key = `${prefix}/${randomUUID()}`;
      const record = encodeChunk(`hello`);
      await s3.put(key, new Uint8Array());

      // Act
      await s3.append(key, 0, record);
      const result = await s3.read(key, 0);

      // Assert
      expect(result?.bytes).toEqual(record);
    });

    test(`should report S3's own clock on a read`, async () => {
      // Arrange
      const key = `${prefix}/${randomUUID()}`;
      await s3.put(key, encodeChunk(`hello`));

      // Act
      const result = await s3.read(key, 0);

      // Assert
      expect(typeof result?.date).toBe(`number`);
      expect(typeof result?.lastModified).toBe(`number`);
    });

    test(`should report a read past the end as no bytes`, async () => {
      // Arrange
      const key = `${prefix}/${randomUUID()}`;
      const record = encodeChunk(`hello`);
      await s3.put(key, record);

      // Act
      const result = await s3.read(key, record.length);

      // Assert
      expect(result?.bytes.length).toBe(0);
    });

    test(`should report a missing object as gone`, async () => {
      // Arrange
      const key = `${prefix}/${randomUUID()}`;

      // Act
      const result = await s3.read(key, 0);

      // Assert
      expect(result).toBeUndefined();
    });

    test(`should refuse an append whose offset is not the object's size`, async () => {
      // Arrange
      const key = `${prefix}/${randomUUID()}`;
      await s3.put(key, encodeChunk(`hello`));

      // Act
      const result = s3.append(key, 1, encodeChunk(`world`));

      // Assert
      await expect(result).rejects.toThrow(WriteOffsetMismatchError);
    });

    test(`should refuse to create an object that already exists`, async () => {
      // Arrange
      const key = `${prefix}/${randomUUID()}`;
      await s3.create(key, encodeChunk(`first`));

      // Act
      const result = s3.create(key, encodeChunk(`second`));

      // Assert
      await expect(result).rejects.toThrow(ObjectExistsError);
      expect((await s3.read(key, 0))?.bytes).toEqual(encodeChunk(`first`));
    });

    test(`should show a reader every committed append and nothing else`, async () => {
      // Arrange
      const key = `${prefix}/${randomUUID()}`;
      const records = Array.from({ length: 50 }, (_, index) => encodeChunk(`chunk-${index}`));
      await s3.put(key, new Uint8Array());
      let written: Uint8Array = new Uint8Array();

      // Act
      /**
       * A read chases every append. AWS documents that an object is never seen half
       * updated, but says nothing about an append in particular, and the whole design
       * rests on the answer.
       */
      for (const record of records) {
        await s3.append(key, written.length, record);
        written = joinRecords([written, record]);

        const result = await s3.read(key, 0);

        // Assert
        expect(result?.bytes).toEqual(written);
      }
    }, 120_000);
  });
}
