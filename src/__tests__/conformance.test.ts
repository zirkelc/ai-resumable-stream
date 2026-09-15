import { createClient } from "redis";
import { RedisMemoryServer } from "redis-memory-server";
import { createRedisAdapter } from "../adapters/redis/index.js";
import { createStreamAdapter } from "../adapters/s3-express/adapter.js";
import { defineConformanceTests, FAST_POLLING, type Harness } from "./conformance-suite.js";
import { createFakeS3 } from "./fake-s3.js";

/**
 * Both adapters the package ships, held to one contract.
 */

/** Redis */
let redisServer: RedisMemoryServer | undefined;
let redisUrl: string;
const redisClients: Array<ReturnType<typeof createClient>> = [];

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
    await Promise.all(redisClients.splice(0).map((client) => client.isOpen && client.destroy()));
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

defineConformanceTests(redisHarness);
defineConformanceTests(s3ExpressHarness);
