import {
  ObjectExistsError,
  type S3HeadResult,
  type S3Operations,
  type S3ReadResult,
  TooManyPartsError,
  WriteOffsetMismatchError,
} from "../adapters/s3-express/client.js";

/**
 * An in-memory directory bucket.
 *
 * It exists because no local emulator implements appends: LocalStack closed the request
 * as not planned and open-source MinIO has no server-side append. So the behaviour the
 * adapter leans on is modelled here, and an opt-in run against a real bucket is what
 * keeps the model honest.
 */
export type FakeS3 = S3Operations & {
  /**
   * Every key currently held, for asserting what a stream left behind.
   */
  keys(): Array<string>;
  /**
   * How many reads and writes have been made, for asserting the request budget.
   */
  counts(): { reads: number; writes: number };
  /**
   * The number of parts a key has used. Each append is one part, as in S3.
   */
  parts(key: string): number | undefined;
  /**
   * The raw bytes held under a key, for asserting what a producer wrote where.
   */
  body(key: string): Uint8Array | undefined;
};

export type CreateFakeS3Options = {
  /**
   * How many parts an object may hold. S3 allows 10,000; tests lower it to force the log
   * to continue in another object without writing ten thousand records.
   */
  partLimit?: number;
};

type FakeObject = {
  body: Uint8Array;
  parts: number;
  lastModified: number;
};

const DEFAULT_PART_LIMIT = 10_000;

export function createFakeS3(options: CreateFakeS3Options = {}): FakeS3 {
  const { partLimit = DEFAULT_PART_LIMIT } = options;

  const objects = new Map<string, FakeObject>();
  let reads = 0;
  let writes = 0;

  return {
    keys: () => [...objects.keys()],
    counts: () => ({ reads, writes }),
    parts: (key) => objects.get(key)?.parts,
    body: (key) => objects.get(key)?.body,

    async put(key, body) {
      writes += 1;
      objects.set(key, { body: new Uint8Array(body), parts: 1, lastModified: Date.now() });
    },

    async create(key, body) {
      writes += 1;
      if (objects.has(key)) throw new ObjectExistsError(key);
      objects.set(key, { body: new Uint8Array(body), parts: 1, lastModified: Date.now() });
    },

    async append(key, offset, body) {
      writes += 1;

      /** S3 rejects an append with no body. */
      if (body.length === 0) throw new Error(`Append to ${key} had an empty body`);

      const object = objects.get(key);
      if (!object) {
        if (offset !== 0) throw new WriteOffsetMismatchError(key);
        objects.set(key, { body: new Uint8Array(body), parts: 1, lastModified: Date.now() });
        return;
      }

      if (offset !== object.body.length) throw new WriteOffsetMismatchError(key);
      if (object.parts >= partLimit) throw new TooManyPartsError(key);

      const next = new Uint8Array(object.body.length + body.length);
      next.set(object.body, 0);
      next.set(body, object.body.length);

      object.body = next;
      object.parts += 1;
      object.lastModified = Date.now();
    },

    async read(key, offset): Promise<S3ReadResult | undefined> {
      reads += 1;

      const object = objects.get(key);
      if (!object) return undefined;

      /** S3 answers a range that starts at or past the end as unsatisfiable. */
      if (offset >= object.body.length) {
        return { bytes: new Uint8Array(), date: undefined, lastModified: undefined };
      }

      return {
        bytes: object.body.slice(offset),
        date: Date.now(),
        lastModified: object.lastModified,
      };
    },

    async head(key): Promise<S3HeadResult | undefined> {
      reads += 1;

      const object = objects.get(key);
      if (!object) return undefined;

      return { size: object.body.length, date: Date.now(), lastModified: object.lastModified };
    },
  };
}
