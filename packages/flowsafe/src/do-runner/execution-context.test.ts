// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import {
  assertNoReservedExecutionContext,
  isReservedExecutionContextKey,
  RESERVED_EXECUTION_CONTEXT_KEYS,
  ReservedExecutionContextError,
  stripReservedExecutionContext,
} from './execution-context.js';

describe('execution-context trust boundary', () => {
  it('reserves capabilities, correlation ids, goal state, and meta-keys', () => {
    for (const key of RESERVED_EXECUTION_CONTEXT_KEYS) {
      expect(isReservedExecutionContextKey(key), key).toBe(true);
    }
    expect(isReservedExecutionContextKey('breakwater.futureCapability')).toBe(
      true,
    );
    expect(isReservedExecutionContextKey('application.locale')).toBe(false);
  });

  it('rejects reserved external keys and strips them from stored compatibility context', () => {
    const context = JSON.parse(
      '{"safe":1,"runId":"forged","breakwater.actor":{"id":"forged"},"__proto__":{"polluted":true}}',
    ) as Record<string, unknown>;

    expect(() =>
      assertNoReservedExecutionContext(context, 'body.context'),
    ).toThrow(ReservedExecutionContextError);
    const safe = stripReservedExecutionContext(context);
    expect(safe).toEqual({ safe: 1 });
    expect(Object.getPrototypeOf(safe)).toBe(Object.prototype);
    expect(Object.hasOwn(safe, '__proto__')).toBe(false);
  });
});
