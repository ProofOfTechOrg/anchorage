// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import {
  type BoundedPlainDataOptions,
  cloneBoundedPlainData,
} from '../src/strict-plain-data.js';

function limits(error: () => Error): BoundedPlainDataOptions {
  return {
    maxDepth: 64,
    maxNodes: 8_192,
    maxScalarBytes: 65_536,
    maxSerializedBytes: 65_536,
    error,
  };
}

describe('bounded plain data', () => {
  it('normalizes the JSON data shapes accepted by attachment progress', () => {
    const nested = Object.assign(Object.create(null), {
      active: true,
      values: [null, 3, 'value'],
    });

    const cloned = cloneBoundedPlainData(
      { version: 1, nested },
      limits(() => new Error('malformed')),
    ) as { version: number; nested: { active: boolean; values: unknown[] } };

    expect(cloned).toEqual({
      version: 1,
      nested: { active: true, values: [null, 3, 'value'] },
    });
    expect(Object.getPrototypeOf(cloned)).toBeNull();
    expect(Object.getPrototypeOf(cloned.nested)).toBeNull();
    expect(Object.getPrototypeOf(cloned.nested.values)).toBe(Array.prototype);
  });

  it('rejects deep, wide, accessor-backed, and cyclic data causally', () => {
    const refusal = new Error('malformed');
    const deepTrap = vi.fn(() => Object.prototype);
    let deep: unknown = new Proxy({}, { getPrototypeOf: deepTrap });
    for (let depth = 0; depth < 65; depth += 1) deep = [deep];
    expect(() =>
      cloneBoundedPlainData(
        deep,
        limits(() => refusal),
      ),
    ).toThrow(refusal);
    expect(deepTrap).not.toHaveBeenCalled();

    const ownKeys = vi.fn(Reflect.ownKeys);
    let itemDescriptorReads = 0;
    const wide = new Proxy(Array(8_192).fill(0), {
      ownKeys,
      getOwnPropertyDescriptor: (target, property) => {
        if (property !== 'length') itemDescriptorReads += 1;
        return Reflect.getOwnPropertyDescriptor(target, property);
      },
    });
    expect(() =>
      cloneBoundedPlainData(
        wide,
        limits(() => refusal),
      ),
    ).toThrow(refusal);
    expect(ownKeys).not.toHaveBeenCalled();
    expect(itemDescriptorReads).toBe(0);

    const accessor = vi.fn(() => 'secret');
    const accessorBacked = Object.defineProperty({}, 'value', {
      enumerable: true,
      get: accessor,
    });
    expect(() =>
      cloneBoundedPlainData(
        accessorBacked,
        limits(() => refusal),
      ),
    ).toThrow(refusal);
    expect(accessor).not.toHaveBeenCalled();

    const cyclic: { self?: unknown } = {};
    cyclic.self = cyclic;
    expect(() =>
      cloneBoundedPlainData(
        cyclic,
        limits(() => refusal),
      ),
    ).toThrow(refusal);
  });

  it('preflights raw and cumulative scalars and enforces final bytes', () => {
    const refusal = new Error('malformed');
    const encode = vi.spyOn(TextEncoder.prototype, 'encode');
    try {
      expect(() =>
        cloneBoundedPlainData(
          'x'.repeat(65_537),
          limits(() => refusal),
        ),
      ).toThrow(refusal);
      expect(encode).not.toHaveBeenCalled();

      expect(() =>
        cloneBoundedPlainData(
          { ['x'.repeat(65_537)]: 0 },
          limits(() => refusal),
        ),
      ).toThrow(refusal);
      expect(encode).not.toHaveBeenCalled();

      expect(() =>
        cloneBoundedPlainData(['1234', '5678'], {
          ...limits(() => refusal),
          maxScalarBytes: 8,
        }),
      ).toThrow(refusal);
      expect(encode).toHaveBeenCalledTimes(1);
    } finally {
      encode.mockRestore();
    }

    expect(() =>
      cloneBoundedPlainData(
        { a: 0 },
        {
          ...limits(() => refusal),
          maxSerializedBytes: 6,
        },
      ),
    ).toThrow(refusal);
  });

  it('throws the exact error supplied by the caller for every refusal', () => {
    const refusal = new Error('fixed refusal');
    let captured: unknown;
    try {
      cloneBoundedPlainData(
        Symbol('not-json'),
        limits(() => refusal),
      );
    } catch (error) {
      captured = error;
    }
    expect(captured).toBe(refusal);

    const trapped = new Proxy(
      {},
      {
        getPrototypeOf: () => {
          throw new Error('hostile trap');
        },
      },
    );
    captured = undefined;
    try {
      cloneBoundedPlainData(
        trapped,
        limits(() => refusal),
      );
    } catch (error) {
      captured = error;
    }
    expect(captured).toBe(refusal);
  });
});
