import { describe, expect, test } from "vitest";
import { createAssembler, splitIntoItems, splitPayload } from "./log.js";

/**
 * Runs payloads through the split and back through the assembler, which is the only
 * property that matters: whatever is written must come back unchanged.
 */
function roundTrip(payloads: Array<string>, maxBytes: number): Array<string> {
  const assembler = createAssembler();
  const received: Array<string> = [];
  for (const body of splitIntoItems(payloads, maxBytes)) {
    received.push(...assembler.take(body));
  }
  return received;
}

describe(`splitPayload`, () => {
  test(`should leave a payload that fits alone`, () => {
    // Arrange
    const value = `hello`;

    // Act
    const pieces = splitPayload(value, 10);

    // Assert
    expect(pieces).toEqual([value]);
  });

  test(`should cut a payload into pieces within the budget`, () => {
    // Arrange
    const value = `abcdefghij`;

    // Act
    const pieces = splitPayload(value, 4);

    // Assert
    expect(pieces).toEqual([`abcd`, `efgh`, `ij`]);
  });

  test(`should never cut inside a multi-byte character`, () => {
    // Arrange
    /** Four bytes each, so a cut on the budget would land inside one. */
    const value = `🙂🙂🙂`;

    // Act
    const pieces = splitPayload(value, 6);

    // Assert
    expect(pieces).toEqual([`🙂`, `🙂`, `🙂`]);
  });

  test(`should refuse a budget too small for a single character`, () => {
    // Arrange
    const value = `abc`;

    // Act
    const split = () => splitPayload(value, 3);

    // Assert
    expect(split).toThrow();
  });
});

describe(`splitIntoItems`, () => {
  test(`should pack payloads that fit into one item`, () => {
    // Arrange
    const payloads = [`ab`, `cd`, `ef`];

    // Act
    const items = splitIntoItems(payloads, 10);

    // Assert
    expect(items).toEqual([{ chunks: payloads, partial: false }]);
  });

  test(`should open a new item once the budget is used up`, () => {
    // Arrange
    const payloads = [`aaaa`, `bbbb`, `cccc`];

    // Act
    const items = splitIntoItems(payloads, 5);

    // Assert
    expect(items).toEqual([
      { chunks: [`aaaa`], partial: false },
      { chunks: [`bbbb`], partial: false },
      { chunks: [`cccc`], partial: false },
    ]);
  });

  test(`should mark every item but the last of a cut payload as partial`, () => {
    // Arrange
    const payloads = [`abcdefghij`];

    // Act
    const items = splitIntoItems(payloads, 4);

    // Assert
    expect(items).toEqual([
      { chunks: [`abcd`], partial: true },
      { chunks: [`efgh`], partial: true },
      { chunks: [`ij`], partial: false },
    ]);
  });

  test(`should return the same payloads a reader started with`, () => {
    // Arrange
    const payloads = [`short`, `a`.repeat(30), `🙂`.repeat(10), `tail`];

    // Act
    const received = roundTrip(payloads, 8);

    // Assert
    expect(received).toEqual(payloads);
  });

  test(`should write nothing for no payloads`, () => {
    // Arrange
    const payloads: Array<string> = [];

    // Act
    const items = splitIntoItems(payloads, 10);

    // Assert
    expect(items.length).toBe(0);
  });
});

describe(`createAssembler`, () => {
  test(`should hold back a payload until the item that finishes it arrives`, () => {
    // Arrange
    const assembler = createAssembler();

    // Act
    const first = assembler.take({ chunks: [`whole`, `cut-`], partial: true });
    const second = assembler.take({ chunks: [`here`], partial: false });

    // Assert
    expect(first).toEqual([`whole`]);
    expect(second).toEqual([`cut-here`]);
  });

  test(`should pass an item without chunks straight through`, () => {
    // Arrange
    const assembler = createAssembler();

    // Act
    const received = assembler.take({ chunks: [], partial: false });

    // Assert
    expect(received.length).toBe(0);
  });
});
