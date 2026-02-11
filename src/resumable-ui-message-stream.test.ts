import type { LanguageModelV3StreamPart } from "@ai-sdk/provider";
import type { UIMessageChunk } from "ai";
import { simulateReadableStream, streamText } from "ai";
import { MockLanguageModelV3 } from "ai/test";
import { consumeUIMessageStream } from "ai-stream-utils";
import { convertAsyncIterableToArray } from "ai-stream-utils/utils";
import { createClient } from "redis";
import { RedisMemoryServer } from "redis-memory-server";
import {
	afterAll,
	afterEach,
	beforeAll,
	beforeEach,
	describe,
	expect,
	test,
	vi,
} from "vitest";
import { createResumableUIMessageStream } from "./resumable-ui-message-stream.js";

type Redis = ReturnType<typeof createClient>;

let redisServer: RedisMemoryServer;
let redisUrl: string;
let publisher: Redis;
let subscriber: Redis;
let pendingPromises: Array<Promise<unknown>>;

function waitUntil(promise: Promise<unknown>): void {
	pendingPromises.push(promise);
}

function sleep(ms: number): Promise<void> {
	return new Promise((r) => setTimeout(r, ms));
}

/**
 * Create a stream from chunks with optional delay
 */
function createStream<CHUNK>(
	chunks: Array<CHUNK>,
	delayMs = 0,
): ReadableStream<CHUNK> {
	return simulateReadableStream({
		chunks,
		initialDelayInMs: delayMs,
		chunkDelayInMs: delayMs,
	});
}

/**
 * Create a mock model with optional delay and abort signal support
 */
function createMockModel(options: {
	chunks: Array<LanguageModelV3StreamPart>;
	delay?: number;
	abortSignal?: AbortSignal;
}) {
	const { chunks, delay = 0, abortSignal } = options;
	let pullCount = 0;

	return new MockLanguageModelV3({
		doStream: async () => ({
			stream: new ReadableStream({
				async pull(controller) {
					if (abortSignal?.aborted) {
						controller.error(
							new DOMException(`The user aborted a request.`, `AbortError`),
						);
						return;
					}

					await sleep(delay);

					if (abortSignal?.aborted) {
						controller.error(
							new DOMException(`The user aborted a request.`, `AbortError`),
						);
						return;
					}

					if (pullCount < chunks.length) {
						controller.enqueue(chunks[pullCount]);
						pullCount++;
					} else {
						controller.close();
					}
				},
			}),
		}),
	});
}

beforeAll(async () => {
	redisServer = new RedisMemoryServer();
	const host = await redisServer.getHost();
	const port = await redisServer.getPort();
	redisUrl = `redis://${host}:${port}`;
}, 60_000);

afterAll(async () => {
	await redisServer.stop();
}, 30_000);

beforeEach(async () => {
	publisher = createClient({ url: redisUrl });
	subscriber = createClient({ url: redisUrl });
	pendingPromises = [];
	await Promise.all([publisher.connect(), subscriber.connect()]);
	await publisher.flushDb();
});

afterEach(async () => {
	await Promise.allSettled(pendingPromises);
	await sleep(25);
	await Promise.all([
		publisher.isOpen ? publisher.quit() : Promise.resolve(),
		subscriber.isOpen ? subscriber.quit() : Promise.resolve(),
	]);
});

describe(`createResumableUIMessageStream`, () => {
	describe(`Redis connection`, () => {
		test(`should connect clients if disconnected`, async () => {
			// Arrange - create fresh disconnected clients
			const publisher = createClient({ url: redisUrl });
			const subscriber = createClient({ url: redisUrl });

			// Verify not connected
			expect(publisher.isOpen).toBe(false);
			expect(subscriber.isOpen).toBe(false);

			// Act
			await createResumableUIMessageStream({
				streamId: `test-stream`,
				publisher,
				subscriber,
				waitUntil,
			});

			// Assert - now connected
			expect(publisher.isOpen).toBe(true);
			expect(subscriber.isOpen).toBe(true);

			// Cleanup
			await publisher.quit();
			await subscriber.quit();
		});

		test(`should not disconnect clients after stream completes`, async () => {
			// Arrange
			// Arrange - create fresh disconnected clients
			const publisher = createClient({ url: redisUrl });
			const subscriber = createClient({ url: redisUrl });
			await Promise.all([publisher.connect(), subscriber.connect()]);

			// Verify connected
			expect(publisher.isOpen).toBe(true);
			expect(subscriber.isOpen).toBe(true);

			const streamId = `test-stream`;
			const chunks: Array<UIMessageChunk> = [
				{ type: `start` },
				{ type: `finish`, finishReason: `stop` },
			];
			const stream = createStream(chunks);

			const context = await createResumableUIMessageStream({
				streamId,
				publisher,
				subscriber,
				waitUntil,
			});

			// Act - complete the stream
			const resultStream = await context.startStream(stream);
			await convertAsyncIterableToArray(resultStream);

			// Assert - clients still connected
			expect(publisher.isOpen).toBe(true);
			expect(subscriber.isOpen).toBe(true);

			// Cleanup
			await publisher.quit();
			await subscriber.quit();
		});

		test(`should not reconnect already-connected clients`, async () => {
			// Arrange - clients already connected from beforeEach
			expect(publisher.isOpen).toBe(true);
			expect(subscriber.isOpen).toBe(true);

			// Spy on connect to verify it's not called
			const publisherConnect = vi.spyOn(publisher, `connect`);
			const subscriberConnect = vi.spyOn(subscriber, `connect`);

			// Act
			await createResumableUIMessageStream({
				streamId: `test-stream`,
				publisher,
				subscriber,
				waitUntil,
			});

			// Assert - connect was never called
			expect(publisherConnect).not.toHaveBeenCalled();
			expect(subscriberConnect).not.toHaveBeenCalled();
		});

		test(`should unsubscribe from stop channel after stream completes`, async () => {
			// Arrange
			const streamId = `test-stream`;
			const abortController = new AbortController();
			const chunks: Array<UIMessageChunk> = [
				{ type: `start` },
				{ type: `finish`, finishReason: `stop` },
			];
			const stream = createStream(chunks);

			const unsubscribeSpy = vi.spyOn(subscriber, `unsubscribe`);

			const context = await createResumableUIMessageStream({
				streamId,
				publisher,
				subscriber,
				abortController,
				waitUntil,
			});

			// Act - complete the stream
			const resultStream = await context.startStream(stream);
			await convertAsyncIterableToArray(resultStream);

			// Assert - unsubscribe was called
			expect(unsubscribeSpy).toHaveBeenCalled();
		});
	});

	describe(`waitUntil`, () => {
		test(`should call waitUntil with background promise`, async () => {
			// Arrange
			const streamId = `test-stream`;
			const chunks: Array<UIMessageChunk> = [
				{ type: `start` },
				{ type: `finish`, finishReason: `stop` },
			];
			const stream = createStream(chunks);
			const waitUntilSpy = vi.fn();

			const context = await createResumableUIMessageStream({
				streamId,
				publisher,
				subscriber,
				waitUntil: waitUntilSpy,
			});

			// Act
			const resultStream = await context.startStream(stream);
			await convertAsyncIterableToArray(resultStream);

			// Assert - waitUntil was called with a promise
			expect(waitUntilSpy).toHaveBeenCalled();
			expect(waitUntilSpy.mock.calls[0]?.[0]).toBeInstanceOf(Promise);
		});
	});

	describe(`startStream`, () => {
		test(`should start stream and consume all chunks`, async () => {
			// Arrange
			const streamId = `test-stream`;
			const chunks: Array<UIMessageChunk> = [
				{ type: `start` },
				{ type: `start-step` },
				{ type: `text-start`, id: `1` },
				{ type: `text-delta`, id: `1`, delta: `Hello` },
				{ type: `text-delta`, id: `1`, delta: ` ` },
				{ type: `text-delta`, id: `1`, delta: `World` },
				{ type: `text-end`, id: `1` },
				{ type: `finish-step` },
				{ type: `finish`, finishReason: `stop` },
			];
			const stream = createStream(chunks);
			const context = await createResumableUIMessageStream({
				streamId,
				publisher,
				subscriber,
				waitUntil,
			});

			// Act
			const resultStream = await context.startStream(stream);
			const result = await convertAsyncIterableToArray(resultStream);

			// Assert
			expect(result.length).toBe(chunks.length);
			expect(result).toEqual(chunks);
		});

		test(`should handle empty stream`, async () => {
			// Arrange
			const streamId = `test-stream`;
			const chunks: Array<UIMessageChunk> = [];
			const stream = createStream(chunks);
			const context = await createResumableUIMessageStream({
				streamId,
				publisher,
				subscriber,
				waitUntil,
			});

			// Act
			const resultStream = await context.startStream(stream);
			const result = await convertAsyncIterableToArray(resultStream);

			// Assert
			expect(result.length).toBe(0);
		});

		test(`should handle control-only chunks (start/finish)`, async () => {
			// Arrange
			const streamId = `test-stream`;
			const chunks: Array<UIMessageChunk> = [
				{ type: `start` },
				{ type: `finish`, finishReason: `stop` },
			];
			const stream = createStream(chunks);
			const context = await createResumableUIMessageStream({
				streamId,
				publisher,
				subscriber,
				waitUntil,
			});

			// Act
			const resultStream = await context.startStream(stream);
			const result = await convertAsyncIterableToArray(resultStream);

			// Assert
			expect(result.length).toBe(2);
			expect(result[0]).toEqual({ type: `start` });
			expect(result[1]).toEqual({ type: `finish`, finishReason: `stop` });
		});
	});

	describe(`resumeStream`, () => {
		test(`should return null when no active stream`, async () => {
			// Arrange
			const streamId = `test-stream`;
			const context = await createResumableUIMessageStream({
				streamId,
				publisher,
				subscriber,
				waitUntil,
			});

			// Act
			const result = await context.resumeStream();

			// Assert
			expect(result).toBeNull();
		});

		test(`should return null for non-existent streamId`, async () => {
			// Arrange
			const streamId = `non-existent-stream`;
			const context = await createResumableUIMessageStream({
				streamId,
				publisher,
				subscriber,
				waitUntil,
			});

			// Act
			const result = await context.resumeStream();

			// Assert
			expect(result).toBeNull();
		});

		test(`should return null when stream is already completed`, async () => {
			// Arrange
			const streamId = `test-stream`;
			const chunks: Array<UIMessageChunk> = [
				{ type: `start` },
				{ type: `start-step` },
				{ type: `text-start`, id: `1` },
				{ type: `text-delta`, id: `1`, delta: `Hello` },
				{ type: `text-delta`, id: `1`, delta: ` ` },
				{ type: `text-delta`, id: `1`, delta: `World` },
				{ type: `text-end`, id: `1` },
				{ type: `finish-step` },
				{ type: `finish`, finishReason: `stop` },
			];
			const stream = createStream(chunks);
			const context = await createResumableUIMessageStream({
				streamId,
				publisher,
				subscriber,
				waitUntil,
			});

			// Start and complete the stream
			const resultStream = await context.startStream(stream);
			await convertAsyncIterableToArray(resultStream);

			// Wait for background operations
			await sleep(25);

			// Act
			const resumedStream = await context.resumeStream();

			// Assert
			expect(resumedStream).toBeNull();
		});
	});

	describe(`stopStream`, () => {
		test(`should stop active stream and fire abort signal`, async () => {
			// Arrange
			const streamId = `test-stream`;
			const chunks: Array<UIMessageChunk> = [
				{ type: `start` },
				{ type: `start-step` },
				{ type: `text-start`, id: `1` },
				{ type: `text-delta`, id: `1`, delta: `Hello` },
				{ type: `text-delta`, id: `1`, delta: ` ` },
				{ type: `text-delta`, id: `1`, delta: `World` },
				{ type: `text-end`, id: `1` },
				{ type: `finish-step` },
				{ type: `finish`, finishReason: `stop` },
			];
			const delayedStream = createStream(chunks, 25);
			const abortController = new AbortController();
			let abortFired = false;

			abortController.signal.addEventListener(`abort`, () => {
				abortFired = true;
			});

			const context = await createResumableUIMessageStream({
				streamId,
				publisher,
				subscriber,
				abortController,
				waitUntil,
			});

			// Act
			const resultStream = await context.startStream(delayedStream);
			const reader = resultStream.getReader();
			await reader.read();

			await context.stopStream();

			// Wait for abort to propagate
			await sleep(25);

			// Assert
			expect(abortController.signal.aborted).toBe(true);
			expect(abortFired).toBe(true);

			reader.releaseLock();
		});

		test(`should work without abortController (no-op)`, async () => {
			// Arrange
			const streamId = `test-stream`;
			const context = await createResumableUIMessageStream({
				streamId,
				publisher,
				subscriber,
				waitUntil,
			});

			const chunks: Array<UIMessageChunk> = [
				{ type: `start` },
				{ type: `start-step` },
				{ type: `text-start`, id: `1` },
				{ type: `text-delta`, id: `1`, delta: `Hello` },
				{ type: `text-delta`, id: `1`, delta: ` ` },
				{ type: `text-delta`, id: `1`, delta: `World` },
				{ type: `text-end`, id: `1` },
				{ type: `finish-step` },
				{ type: `finish`, finishReason: `stop` },
			];
			const stream = createStream(chunks);
			await context.startStream(stream);

			// Act & Assert - should not throw
			await expect(context.stopStream()).resolves.not.toThrow();
		});
	});

	describe(`streamText`, () => {
		let secondPublisher: Redis;
		let secondSubscriber: Redis;

		const modelChunks: Array<LanguageModelV3StreamPart> = [
			{ type: `text-start`, id: `1` },
			{ type: `text-delta`, id: `1`, delta: `Hello` },
			{ type: `text-delta`, id: `1`, delta: ` ` },
			{ type: `text-delta`, id: `1`, delta: `World` },
			{ type: `text-end`, id: `1` },
			{
				type: `finish`,
				finishReason: { raw: `stop`, unified: `stop` },
				usage: {
					inputTokens: {
						total: 10,
						noCache: undefined,
						cacheRead: undefined,
						cacheWrite: undefined,
					},
					outputTokens: { total: 5, text: undefined, reasoning: undefined },
				},
			},
		];

		beforeEach(async () => {
			secondPublisher = createClient({ url: redisUrl });
			secondSubscriber = createClient({ url: redisUrl });
			await Promise.all([
				secondPublisher.connect(),
				secondSubscriber.connect(),
			]);
		});

		afterEach(async () => {
			await Promise.all([
				secondPublisher.isOpen ? secondPublisher.quit() : Promise.resolve(),
				secondSubscriber.isOpen ? secondSubscriber.quit() : Promise.resolve(),
			]);
		});

		test(`should resume an active stream from another client`, async () => {
			// Arrange
			const streamId = `test-stream`;
			const abortController = new AbortController();

			// Start streaming in background
			const startPromise = (async () => {
				const context = await createResumableUIMessageStream({
					streamId,
					publisher,
					subscriber,
					abortController,
					waitUntil,
				});

				const model = createMockModel({ chunks: modelChunks, delay: 25 });
				const result = streamText({ model, prompt: `Generate text` });
				const uiStream = result.toUIMessageStream();
				return context.startStream(uiStream);
			})();

			// Wait for stream to start, then resume from new context
			await sleep(25);

			const resumeContext = await createResumableUIMessageStream({
				streamId,
				publisher: secondPublisher,
				subscriber: secondSubscriber,
				waitUntil,
			});

			// Act
			const resumedStream = await resumeContext.resumeStream();

			// Assert
			if (resumedStream === null) {
				throw new Error(`Expected resumedStream to not be null`);
			}

			const resumedChunks = await convertAsyncIterableToArray(resumedStream);
			const originalStream = await startPromise;
			const originalChunks = await convertAsyncIterableToArray(originalStream);

			expect(resumedChunks.length).toBe(originalChunks.length);
			expect(resumedChunks).toEqual(originalChunks);
		});

		test(`should stop an active stream from another client`, async () => {
			// Arrange
			const streamId = `test-stream`;
			const abortController = new AbortController();
			let isAborted = false;

			// Start streaming in background (first client)
			const startPromise = (async () => {
				const context = await createResumableUIMessageStream({
					streamId,
					publisher,
					subscriber,
					abortController,
					waitUntil,
				});

				// Create abort-aware model and pass abortSignal to streamText
				const model = createMockModel({
					chunks: modelChunks,
					delay: 25,
					abortSignal: abortController.signal,
				});
				const result = streamText({
					model,
					prompt: `Generate text`,
					abortSignal: abortController.signal,
					onAbort: () => {
						isAborted = true;
					},
				});
				const uiStream = result.toUIMessageStream();
				return context.startStream(uiStream);
			})();

			// Wait for stream to start
			await sleep(25);

			// Stop from second client
			const stopContext = await createResumableUIMessageStream({
				streamId,
				publisher: secondPublisher,
				subscriber: secondSubscriber,
				waitUntil,
			});

			// Act - stop from second client
			await stopContext.stopStream();
			await sleep(25);

			// Assert
			expect(abortController.signal.aborted).toBe(true);
			expect(isAborted).toBe(true);

			const originalStream = await startPromise;
			const originalChunks = await convertAsyncIterableToArray(originalStream);

			// Stream was aborted early, so fewer chunks than full output
			// Full stream: start + start-step + text-start + 3 text-deltas + text-end + finish-step + finish = 9+ chunks
			expect(originalChunks.length).toBeLessThan(9);

			// Last chunk should be abort chunk
			const lastChunk = originalChunks[originalChunks.length - 1];
			expect(lastChunk?.type).toBe(`abort`);
		});

		test(`should produce same final UIMessage from startStream and resumeStream via readUIMessageStream`, async () => {
			// Arrange
			const streamId = `test-stream`;
			const abortController = new AbortController();

			// Start streaming from first client
			const startPromise = (async () => {
				const context = await createResumableUIMessageStream({
					streamId,
					publisher,
					subscriber,
					abortController,
					waitUntil,
				});

				const model = createMockModel({ chunks: modelChunks, delay: 25 });
				const result = streamText({ model, prompt: `Generate text` });
				const uiStream = result.toUIMessageStream();
				return context.startStream(uiStream);
			})();

			// Wait for stream to start, then resume from second client
			await sleep(25);

			const resumeContext = await createResumableUIMessageStream({
				streamId,
				publisher: secondPublisher,
				subscriber: secondSubscriber,
				waitUntil,
			});

			// Act
			const resumedStream = await resumeContext.resumeStream();

			if (resumedStream === null) {
				throw new Error(`Expected resumedStream to not be null`);
			}

			// Consume resumed stream via readUIMessageStream and get final UIMessage
			const originalStream = await startPromise;
			const originalMessage = await consumeUIMessageStream(originalStream);
			const resumedMessage = await consumeUIMessageStream(resumedStream);

			// Assert
			expect(resumedMessage).toBeDefined();
			expect(originalMessage).toBeDefined();
			expect(resumedMessage).toEqual(originalMessage);
		});
	});
});
