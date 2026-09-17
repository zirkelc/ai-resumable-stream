import {
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  type S3Client,
} from "@aws-sdk/client-s3";

/**
 * Raised when an append is rejected because the offset it carried is not the object's
 * current size. Also what a retry of an append that already landed looks like.
 */
export class WriteOffsetMismatchError extends Error {
  constructor(key: string) {
    super(`Append to ${key} did not match the object's current size`);
    this.name = `WriteOffsetMismatchError`;
  }
}

/**
 * Raised when a segment has used all of its 10,000 parts.
 */
export class TooManyPartsError extends Error {
  constructor(key: string) {
    super(`Segment ${key} has no parts left`);
    this.name = `TooManyPartsError`;
  }
}

export type S3ReadResult = {
  /**
   * The bytes from the requested offset onwards. Empty when the object has nothing past
   * that offset, which S3 reports as an unsatisfiable range.
   */
  bytes: Uint8Array;
  /**
   * S3's own clock when it answered, from the response's `Date` header. Undefined when
   * the response carried no usable date, which includes every unsatisfiable range.
   */
  date: number | undefined;
  /**
   * When the object was last written, on S3's clock. Undefined for the same reason.
   */
  lastModified: number | undefined;
};

export type S3HeadResult = {
  size: number;
  date: number | undefined;
  lastModified: number | undefined;
};

/**
 * The S3 calls the adapter makes. Narrow on purpose: it is the seam the tests replace
 * with an in-memory bucket, since no local emulator implements appends.
 */
export type S3Operations = {
  /**
   * Writes an object, replacing whatever was there.
   */
  put(key: string, body: Uint8Array): Promise<void>;
  /**
   * Adds bytes to the end of an object. `offset` must be the object's current size.
   */
  append(key: string, offset: number, body: Uint8Array): Promise<void>;
  /**
   * Reads from `offset` to the end. Resolves `undefined` when the object is gone.
   */
  read(key: string, offset: number): Promise<S3ReadResult | undefined>;
  /**
   * Resolves `undefined` when the object is gone.
   */
  head(key: string): Promise<S3HeadResult | undefined>;
};

export type CreateS3OperationsOptions = {
  client: S3Client;
  bucket: string;
};

type ErrorLike = { name?: string; $metadata?: { httpStatusCode?: number } };

function statusOf(error: unknown): number | undefined {
  return (error as ErrorLike)?.$metadata?.httpStatusCode;
}

function nameOf(error: unknown): string | undefined {
  return (error as ErrorLike)?.name;
}

function isMissing(error: unknown): boolean {
  return statusOf(error) === 404 || nameOf(error) === `NoSuchKey` || nameOf(error) === `NotFound`;
}

function isUnsatisfiableRange(error: unknown): boolean {
  return statusOf(error) === 416 || nameOf(error) === `InvalidRange`;
}

type DateSink = { date: number | undefined };

/**
 * The deserialize step of an SDK command, reduced to the one field that is read here.
 */
type Deserialize = (args: never) => Promise<{ response: unknown }>;

type WithMiddleware = {
  middlewareStack: {
    add: (
      middleware: (next: Deserialize) => Deserialize,
      options: { step: `deserialize`; name: string },
    ) => void;
  };
};

/**
 * Records the response's `Date` header, which is S3's own now and the only clock the
 * adapter trusts for deadlines. The SDK does not put it in command output, so it is read
 * from the raw response.
 *
 * The middleware is added to the command rather than to the client, so a caller's client
 * is left exactly as they built it. Only successful responses are read, which is why the
 * adapter falls back to a `HeadObject` when it needs the clock and the range was empty.
 */
function captureDate(command: unknown, sink: DateSink): void {
  (command as WithMiddleware).middlewareStack.add(
    (next) => async (args) => {
      const result = await next(args);
      const headers = (result.response as { headers?: Record<string, string> })?.headers;
      const parsed = headers?.date ? Date.parse(headers.date) : Number.NaN;
      if (!Number.isNaN(parsed)) sink.date = parsed;
      return result;
    },
    { step: `deserialize`, name: `captureResponseDate` },
  );
}

/**
 * Binds the adapter's S3 calls to a caller-built client and a directory bucket.
 *
 * The bucket must be an S3 Express One Zone directory bucket in an Availability Zone.
 * Appends exist nowhere else, and they are what makes a stream one object instead of
 * thousands.
 */
export function createS3Operations(options: CreateS3OperationsOptions): S3Operations {
  const { client, bucket } = options;

  return {
    async put(key, body) {
      await client.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }));
    },

    async append(key, offset, body) {
      try {
        await client.send(
          new PutObjectCommand({
            Bucket: bucket,
            Key: key,
            Body: body,
            WriteOffsetBytes: offset,
          }),
        );
      } catch (error) {
        if (nameOf(error) === `InvalidWriteOffset`) throw new WriteOffsetMismatchError(key);
        if (nameOf(error) === `TooManyParts`) throw new TooManyPartsError(key);
        throw error;
      }
    },

    async read(key, offset) {
      const sink: DateSink = { date: undefined };
      const command = new GetObjectCommand({
        Bucket: bucket,
        Key: key,
        Range: `bytes=${offset}-`,
      });
      captureDate(command, sink);

      try {
        const response = await client.send(command);
        return {
          bytes: (await response.Body?.transformToByteArray()) ?? new Uint8Array(),
          date: sink.date,
          lastModified: response.LastModified?.getTime(),
        };
      } catch (error) {
        if (isUnsatisfiableRange(error)) {
          return { bytes: new Uint8Array(), date: undefined, lastModified: undefined };
        }
        if (isMissing(error)) return undefined;
        throw error;
      }
    },

    async head(key) {
      const sink: DateSink = { date: undefined };
      const command = new HeadObjectCommand({ Bucket: bucket, Key: key });
      captureDate(command, sink);

      try {
        const response = await client.send(command);
        return {
          size: response.ContentLength ?? 0,
          date: sink.date,
          lastModified: response.LastModified?.getTime(),
        };
      } catch (error) {
        if (isMissing(error)) return undefined;
        throw error;
      }
    },
  };
}
