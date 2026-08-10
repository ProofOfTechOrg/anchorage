// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import { isPathSafeId, PATH_SAFE_ID_PATTERN } from './path-safe-id.js';

describe('PATH_SAFE_ID_PATTERN', () => {
  it.each([
    'run-1',
    'thread_1',
    'resource.example',
    'a~b',
    'a'.repeat(200),
  ])('accepts an RFC 3986 unreserved id: %s', (id) => {
    expect(PATH_SAFE_ID_PATTERN.test(id)).toBe(true);
  });

  it.each([
    '',
    '.',
    '..',
    'a/b',
    'a:b',
    'a b',
    'a?b',
    'a'.repeat(201),
  ])('rejects an unsafe or ambiguous id: %s', (id) => {
    expect(PATH_SAFE_ID_PATTERN.test(id)).toBe(false);
  });
});

describe('isPathSafeId', () => {
  it('rejects non-strings instead of applying RegExp coercion', () => {
    expect(isPathSafeId(123)).toBe(false);
    expect(isPathSafeId(null)).toBe(false);
    expect(isPathSafeId({ toString: () => 'run-1' })).toBe(false);
  });
});
