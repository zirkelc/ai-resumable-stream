import { describe, expect, test } from "vitest";
import { createSerialQueue } from "./serial.js";

function flushMicrotasks() {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

function deferred() {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

describe(`serial queue`, () => {
  test(`should not start work until the work before it has settled`, async () => {
    // Arrange
    const queue = createSerialQueue();
    const first = deferred();
    const started: Array<string> = [];

    // Act
    const one = queue.run(async () => {
      started.push(`one`);
      await first.promise;
    });
    const two = queue.run(async () => {
      started.push(`two`);
    });

    /** Work is queued rather than called, so it starts a turn later. */
    await flushMicrotasks();

    // Assert
    expect(started).toEqual([`one`]);

    first.resolve();
    await Promise.all([one, two]);
    expect(started).toEqual([`one`, `two`]);
  });

  test(`should keep the queue running after a failure`, async () => {
    // Arrange
    const queue = createSerialQueue();
    const done: Array<string> = [];

    // Act
    const failed = queue.run(async () => {
      throw new Error(`refused`);
    });
    const after = queue.run(async () => {
      done.push(`after`);
    });

    // Assert
    await expect(failed).rejects.toThrow();
    await after;
    expect(done).toEqual([`after`]);
  });

  test(`should report a failure to whoever queued it and to nobody else`, async () => {
    // Arrange
    const queue = createSerialQueue();

    // Act
    const failed = queue.run(async () => {
      throw new Error(`refused`);
    });
    const fine = queue.run(async () => {});

    // Assert
    await expect(failed).rejects.toThrow();
    await expect(fine).resolves.toBeUndefined();
  });

  test(`should run work in the order it was queued`, async () => {
    // Arrange
    const queue = createSerialQueue();
    const order: Array<number> = [];

    // Act
    const all = [3, 1, 2].map((value, index) =>
      queue.run(async () => {
        await new Promise((resolve) => setTimeout(resolve, value));
        order.push(index);
      }),
    );
    await Promise.all(all);

    // Assert
    expect(order).toEqual([0, 1, 2]);
  });
});
