// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import {
  assertNoReservedExecutionContext,
  isReservedExecutionContextKey,
  RESERVED_EXECUTION_CONTEXT_KEYS,
  ReservedExecutionContextError,
  RUN_PROVENANCE_CONTEXT_KEY,
  stripReservedExecutionContext,
} from './execution-context.js';

const EXPECTED_RESERVED_EXECUTION_CONTEXT_KEYS = [
  'breakwater.connectorGrants',
  'breakwater.connectorExecution',
  'breakwater.actor',
  'breakwater.principalPermissions',
  'breakwater.workflowScope',
  'breakwater.isolationScope',
  'mastra:goal',
  'flowsafe.runProvenance',
  'flowsafe.runLifecycle',
  'startReservation',
  'runId',
  'threadId',
  'resourceId',
  '__proto__',
  'constructor',
  'prototype',
] as const;

describe('execution-context trust boundary', () => {
  it('reserves the independent capability, correlation, goal, lifecycle, and meta-key inventory', () => {
    expect([...RESERVED_EXECUTION_CONTEXT_KEYS].sort()).toEqual(
      [...EXPECTED_RESERVED_EXECUTION_CONTEXT_KEYS].sort(),
    );
    expect(isReservedExecutionContextKey('breakwater.futureCapability')).toBe(
      true,
    );
    expect(isReservedExecutionContextKey('application.locale')).toBe(false);
    expect(isReservedExecutionContextKey(RUN_PROVENANCE_CONTEXT_KEY)).toBe(
      true,
    );
  });

  it.each(
    EXPECTED_RESERVED_EXECUTION_CONTEXT_KEYS,
  )('rejects and strips the independent reserved key %s', (key) => {
    expect(isReservedExecutionContextKey(key)).toBe(true);
    const external = Object.fromEntries([[key, 'forged']]);
    expect(() =>
      assertNoReservedExecutionContext(external, 'body.context'),
    ).toThrow(ReservedExecutionContextError);
    const stored = Object.fromEntries([
      [key, 'forged'],
      ['application.locale', 'en-US'],
    ]);
    const safe = stripReservedExecutionContext(stored);
    expect(safe).toEqual({ 'application.locale': 'en-US' });
    expect(Object.getPrototypeOf(safe)).toBe(Object.prototype);
    expect(Object.hasOwn(safe, key)).toBe(false);
  });

  it('rejects reserved external keys and strips them from stored compatibility context', () => {
    const context = JSON.parse(
      '{"safe":1,"runId":"forged","flowsafe.runProvenance":{"requestedBy":"forged"},"breakwater.actor":{"id":"forged"},"__proto__":{"polluted":true}}',
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

describe('FS8 D3 protected replay stored authority boundary', () => {
  it('strips a top-level original-claim lookalike while preserving application payloads', () => {
    const payload = { startReservation: { key: 'application-key' } };
    const context = {
      startReservation: { key: 'authority-key', binding: { kind: 'unbound' } },
      payload,
    };
    expect(stripReservedExecutionContext(context)).toEqual({ payload });
    expect(() => assertNoReservedExecutionContext(context)).toThrow(
      ReservedExecutionContextError,
    );
  });
});
