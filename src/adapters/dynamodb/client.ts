import {
  type AttributeValue,
  ConditionalCheckFailedException,
  type DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  QueryCommand,
} from "@aws-sdk/client-dynamodb";

/**
 * Thrown when an item that had to be new was already there. The adapter uses it to refuse
 * a generation id that another producer is already writing under.
 */
export class ItemExistsError extends Error {
  constructor(key: string) {
    super(`Item already exists: ${key}`);
    this.name = `ItemExistsError`;
  }
}

/**
 * An item as the adapter thinks of it, with the key attributes left out: which attributes
 * hold the partition key, the sort key and the expiry is the table's business, not the
 * stream's.
 *
 * The attributes are short because DynamoDB charges for their names on every write.
 */
export type StreamItem = {
  /** Chunk payloads, in the order they were produced. */
  chunks?: Array<string>;
  /** The last payload in `chunks` is cut short and continues in the item that follows. */
  partial?: boolean;
  /** The generation ended, however it ended. */
  end?: boolean;
  /** The producer's clock when it wrote the item, in epoch milliseconds. */
  at?: number;
  /** The generation a stream id points at. Pointer items only. */
  generationId?: string;
  /** The format the generation is written in. The first item of a generation only. */
  version?: number;
};

export type StoredItem = StreamItem & {
  sortKey: string;
};

/**
 * Storage operations the adapter needs, named after what it uses them for rather than
 * after the commands behind them.
 */
export type DynamoOperations = {
  /**
   * Writes an item, replacing whatever was under the key.
   */
  put(partitionKey: string, sortKey: string, item: StreamItem): Promise<void>;
  /**
   * Writes an item only if nothing is under the key yet, and throws `ItemExistsError`
   * when something is.
   */
  create(partitionKey: string, sortKey: string, item: StreamItem): Promise<void>;
  /**
   * Reads one item, or `undefined` when there is none.
   */
  get(
    partitionKey: string,
    sortKey: string,
    options?: { consistent?: boolean },
  ): Promise<StoredItem | undefined>;
  /**
   * Reads the items of one partition whose sort key falls between `from` and `to`, both
   * ends included, in sort key order.
   *
   * A single call returns what fits in one page, so the caller reads on from the last
   * sort key it received rather than being handed the whole range at once.
   */
  query(
    partitionKey: string,
    range: { from: string; to: string },
    options?: { consistent?: boolean },
  ): Promise<Array<StoredItem>>;
};

export type CreateDynamoOperationsOptions = {
  client: DynamoDBClient;
  tableName: string;
  /** The attribute holding the partition key. */
  partitionKeyName: string;
  /** The attribute holding the sort key. */
  sortKeyName: string;
  /** The attribute the table's time to live is configured on. */
  ttlAttributeName: string;
  /** How long an item lives, in seconds. */
  ttlSeconds: number;
};

/**
 * The attribute names an item is stored under. Single letters, because DynamoDB counts
 * attribute names towards the size of every item it writes.
 */
const Attribute = {
  CHUNKS: `c`,
  PARTIAL: `p`,
  END: `e`,
  AT: `t`,
  GENERATION_ID: `g`,
  VERSION: `v`,
} as const;

function marshall(item: StreamItem): Record<string, AttributeValue> {
  const attributes: Record<string, AttributeValue> = {};

  if (item.chunks !== undefined) {
    attributes[Attribute.CHUNKS] = { L: item.chunks.map((chunk) => ({ S: chunk })) };
  }
  if (item.partial !== undefined) attributes[Attribute.PARTIAL] = { BOOL: item.partial };
  if (item.end !== undefined) attributes[Attribute.END] = { BOOL: item.end };
  if (item.at !== undefined) attributes[Attribute.AT] = { N: String(item.at) };
  if (item.generationId !== undefined) {
    attributes[Attribute.GENERATION_ID] = { S: item.generationId };
  }
  if (item.version !== undefined) attributes[Attribute.VERSION] = { N: String(item.version) };

  return attributes;
}

function unmarshall(attributes: Record<string, AttributeValue>, sortKeyName: string): StoredItem {
  const chunks = attributes[Attribute.CHUNKS]?.L;
  const at = attributes[Attribute.AT]?.N;
  const version = attributes[Attribute.VERSION]?.N;

  return {
    sortKey: attributes[sortKeyName]?.S ?? ``,
    chunks: chunks?.map((value) => value.S ?? ``),
    partial: attributes[Attribute.PARTIAL]?.BOOL,
    end: attributes[Attribute.END]?.BOOL,
    at: at === undefined ? undefined : Number(at),
    generationId: attributes[Attribute.GENERATION_ID]?.S,
    version: version === undefined ? undefined : Number(version),
  };
}

/**
 * Binds the operations above to one table, so the adapter never builds a command itself
 * and the key and expiry attributes are named in a single place.
 */
export function createDynamoOperations(options: CreateDynamoOperationsOptions): DynamoOperations {
  const { client, tableName, partitionKeyName, sortKeyName, ttlAttributeName, ttlSeconds } =
    options;

  function keys(partitionKey: string, sortKey: string): Record<string, AttributeValue> {
    return { [partitionKeyName]: { S: partitionKey }, [sortKeyName]: { S: sortKey } };
  }

  function toItem(
    partitionKey: string,
    sortKey: string,
    item: StreamItem,
  ): Record<string, AttributeValue> {
    return {
      ...keys(partitionKey, sortKey),
      ...marshall(item),
      /**
       * Every item expires, including the ones a producer never got to finish. Nothing
       * here deletes, so this is the only thing that keeps the table from growing forever.
       */
      [ttlAttributeName]: { N: String(Math.floor(Date.now() / 1_000) + ttlSeconds) },
    };
  }

  return {
    async put(partitionKey, sortKey, item) {
      await client.send(
        new PutItemCommand({ TableName: tableName, Item: toItem(partitionKey, sortKey, item) }),
      );
    },

    async create(partitionKey, sortKey, item) {
      try {
        await client.send(
          new PutItemCommand({
            TableName: tableName,
            Item: toItem(partitionKey, sortKey, item),
            ConditionExpression: `attribute_not_exists(#sk)`,
            ExpressionAttributeNames: { "#sk": sortKeyName },
          }),
        );
      } catch (error) {
        if (error instanceof ConditionalCheckFailedException) {
          throw new ItemExistsError(`${partitionKey}/${sortKey}`);
        }
        throw error;
      }
    },

    async get(partitionKey, sortKey, { consistent = false } = {}) {
      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: keys(partitionKey, sortKey),
          ConsistentRead: consistent,
        }),
      );

      return result.Item ? unmarshall(result.Item, sortKeyName) : undefined;
    },

    async query(partitionKey, { from, to }, { consistent = false } = {}) {
      const result = await client.send(
        new QueryCommand({
          TableName: tableName,
          KeyConditionExpression: `#pk = :pk AND #sk BETWEEN :from AND :to`,
          ExpressionAttributeNames: { "#pk": partitionKeyName, "#sk": sortKeyName },
          ExpressionAttributeValues: {
            ":pk": { S: partitionKey },
            ":from": { S: from },
            ":to": { S: to },
          },
          ConsistentRead: consistent,
        }),
      );

      return (result.Items ?? []).map((item) => unmarshall(item, sortKeyName));
    },
  };
}
