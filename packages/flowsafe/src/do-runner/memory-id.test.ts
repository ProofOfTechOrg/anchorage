// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';

import { mintThreadId, resourceIdFromKey } from './memory-id.js';

describe('memory id construction', () => {
  it('mints an opaque thread id from the host generator exactly once', () => {
    const mint = vi.fn(() => 'thread-1');

    expect(mintThreadId(mint)).toBe('thread-1');
    expect(mint).toHaveBeenCalledOnce();
  });

  it.each([
    '',
    '.',
    '..',
    'thread/1',
    'thread 1',
  ])('rejects a generated non-path-safe thread id: %s', (id) => {
    expect(() => mintThreadId(() => id)).toThrow(/PATH_SAFE_ID_PATTERN/);
  });

  it('rejects a non-string generated thread id without RegExp coercion', () => {
    expect(() => mintThreadId((() => 123) as never)).toThrow(
      /PATH_SAFE_ID_PATTERN/,
    );
  });

  it('uses the stable path-safe business key as the resource id', () => {
    expect(resourceIdFromKey('user-1')).toBe('user-1');
  });

  it.each([
    '',
    '.',
    '..',
    'user/1',
    'user 1',
  ])('rejects a non-path-safe resource key: %s', (key) => {
    expect(() => resourceIdFromKey(key)).toThrow(/PATH_SAFE_ID_PATTERN/);
  });

  it('rejects a non-string resource key without RegExp coercion', () => {
    expect(() => resourceIdFromKey(123 as never)).toThrow(
      /PATH_SAFE_ID_PATTERN/,
    );
  });
});
