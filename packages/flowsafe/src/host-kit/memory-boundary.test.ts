// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import {
  assertNoClientMemoryIds,
  requireMemoryId,
  TCB_ONLY_MEMORY_FIELDS,
} from './memory-boundary.js';
import { RunRouteError } from './run-route-error.js';

describe('assertNoClientMemoryIds', () => {
  it.each(
    TCB_ONLY_MEMORY_FIELDS,
  )('rejects a nested client-owned %s key', (field) => {
    expect(() =>
      assertNoClientMemoryIds({ input: [{ nested: { [field]: 'x' } }] }),
    ).toThrow(RunRouteError);
  });

  it('rejects a key even when its value is undefined', () => {
    expect(() => assertNoClientMemoryIds({ threadId: undefined })).toThrow(
      /server-assigned/,
    );
  });

  it('handles cycles and deeply nested safe input without recursion', () => {
    const root: Record<string, unknown> = {};
    let cursor = root;
    for (let depth = 0; depth < 10_000; depth += 1) {
      const next: Record<string, unknown> = {};
      cursor.next = next;
      cursor = next;
    }
    cursor.root = root;

    expect(() => assertNoClientMemoryIds(root)).not.toThrow();
  });
});

describe('requireMemoryId', () => {
  it('returns a path-safe host id unchanged', () => {
    expect(requireMemoryId('thread-1')).toBe('thread-1');
    expect(requireMemoryId('resource_1', 'resourceId')).toBe('resource_1');
  });

  it.each([
    '',
    '.',
    '..',
    'thread/1',
    'thread 1',
  ])('maps a malformed id to a 404: %s', (id) => {
    try {
      requireMemoryId(id);
      throw new Error('expected requireMemoryId to throw');
    } catch (error) {
      expect(error).toBeInstanceOf(RunRouteError);
      expect((error as RunRouteError).status).toBe(404);
    }
  });
});
