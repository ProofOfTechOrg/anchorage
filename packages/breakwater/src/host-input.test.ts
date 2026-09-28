// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import {
  assertKnownFields,
  readFrozenList,
  readNumberInRange,
  unknownFieldOf,
} from './host-input.js';

describe('unknownFieldOf', () => {
  const fields = { id: true, role: true } as const;

  it('names the first own key the table lacks', () => {
    // #when / #then
    expect(unknownFieldOf({ id: 'a', Kind: 'x', type: 'y' }, fields)).toBe(
      'Kind',
    );
    expect(
      unknownFieldOf(JSON.parse('{"id":"a","__proto__":{}}'), fields),
    ).toBe('__proto__');
  });

  it('answers undefined for known keys, symbols and inherited keys', () => {
    // #given
    const branded = { id: 'a', [Symbol('brand')]: true };
    const inherited = Object.create({ extra: true }) as object;

    // #when / #then
    expect(unknownFieldOf({ id: 'a', role: 'b' }, fields)).toBeUndefined();
    expect(unknownFieldOf(branded, fields)).toBeUndefined();
    expect(unknownFieldOf(inherited, fields)).toBeUndefined();
  });
});

// A Proxy over a real array whose `length` read answers `length`.
// `Array.isArray` is true for it, and an index read reaches the array.
function withLength(entries: readonly unknown[], length: unknown): unknown[] {
  return new Proxy([...entries], {
    get(target, key, receiver) {
      return key === 'length' ? length : Reflect.get(target, key, receiver);
    },
  });
}

// Stops a read that runs past any length a row reports, so a coerced
// unbounded length fails the row instead of exhausting memory.
const keep = (entry: unknown, index: number) => {
  if (index >= 8) throw new Error('read past the reported entries');
  return entry;
};

describe('readFrozenList length', () => {
  it.each<[string, unknown]>([
    ['NaN', Number.NaN],
    ['a negative number', -1],
    ['a fraction', 1.5],
    ['Infinity', Number.POSITIVE_INFINITY],
    ['a number above the safe-integer range', 2 ** 53],
    ['a numeric string', '1'],
    ['null', null],
  ])('refuses %s as the length', (_label, length) => {
    // #when / #then
    expect(() =>
      readFrozenList('probe: list', withLength(['a'], length), keep),
    ).toThrow(
      new TypeError('probe: list must be an array with a valid length'),
    );
  });

  it('refuses NaN on a list that must not be empty, which a zero check alone passes', () => {
    // #given
    const list = withLength([], Number.NaN);
    // #when / #then
    expect(() => readFrozenList('probe: list', list, keep, true)).toThrow(
      new TypeError('probe: list must be an array with a valid length'),
    );
  });

  it('refuses an object length without calling its valueOf', () => {
    // #given
    let calls = 0;
    const length = {
      valueOf() {
        calls += 1;
        return 1;
      },
    };
    // #when / #then
    expect(() =>
      readFrozenList('probe: list', withLength(['a'], length), keep),
    ).toThrow(
      new TypeError('probe: list must be an array with a valid length'),
    );
    expect(calls).toBe(0);
  });

  it('reads as many entries as a valid length reports', () => {
    // #when
    const copy = readFrozenList('probe: list', withLength(['a', 'b'], 1), keep);
    // #then
    expect(copy).toEqual(['a']);
    expect(Object.isFrozen(copy)).toBe(true);
  });
});

const PROBE_FIELDS = { alpha: true, beta: true } satisfies Record<
  'alpha' | 'beta',
  true
>;

describe('assertKnownFields', () => {
  it('refuses an own key outside the table and lists the table', () => {
    // #when / #then
    expect(() =>
      assertKnownFields('probe: options', { alpha: 1, alpah: 2 }, PROBE_FIELDS),
    ).toThrow(
      new TypeError(
        'probe: options has unknown field "alpah" (valid fields: alpha, beta)',
      ),
    );
  });

  it('accepts every key of the table', () => {
    // #when / #then
    expect(() =>
      assertKnownFields('probe: options', { alpha: 1, beta: 2 }, PROBE_FIELDS),
    ).not.toThrow();
  });

  it('does not read a symbol key, so a brand passes', () => {
    // #given
    const branded = { alpha: 1, [Symbol('brand')]: true };
    // #when / #then
    expect(() =>
      assertKnownFields('probe: options', branded, PROBE_FIELDS),
    ).not.toThrow();
  });

  it('does not require a key the object inherits', () => {
    // #given — a class instance keeps its methods on the prototype
    class Probe {
      alpha = 1;
      beta(): number {
        return 2;
      }
    }
    // #when / #then
    expect(() =>
      assertKnownFields('probe: options', new Probe(), PROBE_FIELDS),
    ).not.toThrow();
  });

  it.each<[string, unknown, string]>([
    ['a string', 'alpha', '"alpha"'],
    ['a number', 5, 'number'],
    ['null', null, 'null'],
    ['an array', ['alpha'], 'an array'],
    ['a function', () => undefined, 'function'],
  ])('refuses %s as the object', (_label, value, got) => {
    // #when / #then
    expect(() =>
      assertKnownFields('probe: options', value, PROBE_FIELDS),
    ).toThrow(new TypeError(`probe: options must be an object (got ${got})`));
  });
});

describe('readNumberInRange', () => {
  const positive = (value: number) => value > 0;

  it('returns a number the predicate accepts', () => {
    // #when / #then
    expect(readNumberInRange('probe: size', 3, positive, 'positive')).toBe(3);
  });

  it.each<[string, unknown, string]>([
    ['a numeric string, without coercing it', '3', '"3"'],
    ['a number the predicate refuses', -1, '-1'],
    ['NaN', Number.NaN, 'NaN'],
    ['null', null, 'null'],
  ])('refuses %s', (_label, value, got) => {
    // #when / #then
    expect(() =>
      readNumberInRange('probe: size', value, positive, 'positive'),
    ).toThrow(new TypeError(`probe: size must be positive (got ${got})`));
  });
});
