// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { nestedArray, nestedObject } from '../../test-support/deep-json.js';
import { exceedsRunInputDepth } from './run-input-depth.js';

describe('exceedsRunInputDepth', () => {
  it.each([
    ['objects', nestedObject],
    ['arrays', nestedArray],
  ] as const)('allows %s nested 256 levels and refuses 257', (_, nest) => {
    // #given values nested exactly at, and one past, the bound
    const atBound = nest(256);
    const pastBound = nest(257);

    // #then only the deeper one exceeds it
    expect(exceedsRunInputDepth(atBound)).toBe(false);
    expect(exceedsRunInputDepth(pastBound)).toBe(true);
  });

  it('allows scalars and empty containers', () => {
    expect(
      [null, undefined, 'text', 7, true, {}, []].map(exceedsRunInputDepth),
    ).toEqual([false, false, false, false, false, false, false]);
  });

  it('refuses a cycle', () => {
    // #given an object that contains itself
    const cycle: Record<string, unknown> = {};
    cycle.self = cycle;

    // #then it exceeds the bound
    expect(exceedsRunInputDepth(cycle)).toBe(true);
  });

  it('counts a typed array as one level', () => {
    // #given a typed array as the innermost value of 255 nested objects, and
    // of 256
    const wrap = (levels: number) => {
      let value: unknown = new Uint8Array(4);
      for (let level = 0; level < levels; level++) value = { next: value };
      return value;
    };

    // #then it adds one level, like an array
    expect(exceedsRunInputDepth(wrap(255))).toBe(false);
    expect(exceedsRunInputDepth(wrap(256))).toBe(true);
  });
});
