import { createClient } from "redis";
import { RedisMemoryServer } from "redis-memory-server";
import { createDynamoDBAdapter } from "../adapters/dynamodb/index.js";
import { createRedisAdapter } from "../adapters/redis/index.js";
import { createStreamAdapter } from "../adapters/s3-express/adapter.js";
import { defineConformanceTests, FAST_POLLING, type Harness } from "./conformance-suite.js";
import { createFakeS3 } from "./fake-s3.js";
import { createLocalDynamoDB, type LocalDynamoDB } from "./local-dynamodb.js";

/**
 * Every adapter the package ships, held to one contract.
 */

/** Redis */
let redisServer: RedisMemoryServer | undefined;
let redisUrl: string;
/**
 * Only what teardown needs, since the client type of one `redis` release does not
 * describe the other.
 */
const redisClients: Array<{ isOpen: boolean; quit: () => Promise<unknown> }> = [];

const redisHarness: Harness = {
  name: `redis`,
  setup: async () => {
    redisServer = await RedisMemoryServer.create();
    redisUrl = `redis://${await redisServer.getHost()}:${await redisServer.getPort()}`;
  },
  teardown: async () => {
    await redisServer?.stop();
  },
  afterEach: async () => {
    /**
     * `quit` rather than `destroy`: it lets the commands already on the wire finish,
     * where `destroy` rejects them and the rejection has no caller left to catch it.
     */
    await Promise.all(
      redisClients.splice(0).map((client) => (client.isOpen ? client.quit() : undefined)),
    );
  },
  createAdapter: async () => {
    const publisher = createClient({ url: redisUrl });
    const subscriber = createClient({ url: redisUrl });
    redisClients.push(publisher, subscriber);
    return createRedisAdapter({ publisher, subscriber });
  },
};

/**
 * The S3 Express adapter over an in-memory bucket. No local emulator implements appends,
 * so the fake is the only way to run the contract without an AWS account; the opt-in
 * integration run against a real bucket is what proves the fake matches S3.
 */
const s3ExpressHarness: Harness = {
  name: `s3-express`,
  createAdapter: async () => createStreamAdapter(createFakeS3(), FAST_POLLING),
};

/**
 * The DynamoDB adapter over an in-memory table. `dynalite` serves the real DynamoDB API,
 * so the conditional writes and range queries the adapter leans on are the real ones.
 */
let dynamo: LocalDynamoDB | undefined;

const dynamoHarness: Harness = {
  name: `dynamodb`,
  setup: async () => {
    dynamo = await createLocalDynamoDB();
  },
  teardown: async () => {
    await dynamo?.stop();
  },
  createAdapter: async () =>
    createDynamoDBAdapter({
      client: dynamo!.client,
      tableName: dynamo!.tableName,
      ...FAST_POLLING,
    }),
};

defineConformanceTests(redisHarness);
defineConformanceTests(s3ExpressHarness);
defineConformanceTests(dynamoHarness);
