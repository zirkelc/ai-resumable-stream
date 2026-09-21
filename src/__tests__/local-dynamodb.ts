import type { AddressInfo } from "node:net";
import { CreateTableCommand, DynamoDBClient, ScanCommand } from "@aws-sdk/client-dynamodb";
import dynalite from "dynalite";

/**
 * A DynamoDB table that lives in memory.
 *
 * `dynalite` implements the DynamoDB API over an in-memory LevelDB, so the adapter talks
 * to a real `DynamoDBClient` and its commands, conditional writes and range queries are
 * all exercised. Nothing has to be installed, paid for or left running.
 *
 * It does not implement time to live, so the expiry attribute is written and nothing acts
 * on it. On a real table it is enabled once, on the table rather than per stream.
 */
export type LocalDynamoDB = {
  client: DynamoDBClient;
  tableName: string;
  /**
   * How many reads and writes the client has made, for asserting the request budget.
   */
  counts(): { reads: number; writes: number };
  /**
   * Every key currently held, for showing what a stream left behind.
   */
  keys(): Promise<Array<string>>;
  stop(): Promise<void>;
};

export type CreateLocalDynamoDBOptions = {
  tableName?: string;
  partitionKeyName?: string;
  sortKeyName?: string;
};

const READ_COMMANDS = new Set([`GetItemCommand`, `QueryCommand`, `ScanCommand`]);

export async function createLocalDynamoDB(
  options: CreateLocalDynamoDBOptions = {},
): Promise<LocalDynamoDB> {
  const { tableName = `streams`, partitionKeyName = `pk`, sortKeyName = `sk` } = options;

  /** A table that is never `CREATING` keeps the tests and the example from waiting. */
  const server = dynalite({ createTableMs: 0 });
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const { port } = server.address() as AddressInfo;

  const client = new DynamoDBClient({
    endpoint: `http://127.0.0.1:${port}`,
    region: `local`,
    credentials: { accessKeyId: `local`, secretAccessKey: `local` },
  });

  let reads = 0;
  let writes = 0;

  /**
   * Counts commands as they are sent, which is what a real table bills for.
   */
  client.middlewareStack.add(
    (next, context) => async (args) => {
      if (READ_COMMANDS.has(context.commandName ?? ``)) reads += 1;
      else writes += 1;
      return next(args);
    },
    { step: `initialize`, name: `countRequests` },
  );

  await client.send(
    new CreateTableCommand({
      TableName: tableName,
      AttributeDefinitions: [
        { AttributeName: partitionKeyName, AttributeType: `S` },
        { AttributeName: sortKeyName, AttributeType: `S` },
      ],
      KeySchema: [
        { AttributeName: partitionKeyName, KeyType: `HASH` },
        { AttributeName: sortKeyName, KeyType: `RANGE` },
      ],
      BillingMode: `PAY_PER_REQUEST`,
    }),
  );

  /** Only the table's own setup, which an application does once and not per stream. */
  reads = 0;
  writes = 0;

  return {
    client,
    tableName,

    counts: () => ({ reads, writes }),

    async keys() {
      const result = await client.send(
        new ScanCommand({
          TableName: tableName,
          ProjectionExpression: `#pk, #sk`,
          ExpressionAttributeNames: { "#pk": partitionKeyName, "#sk": sortKeyName },
        }),
      );

      return (result.Items ?? []).map(
        (item) => `${item[partitionKeyName]?.S}/${item[sortKeyName]?.S}`,
      );
    },

    async stop() {
      client.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}
