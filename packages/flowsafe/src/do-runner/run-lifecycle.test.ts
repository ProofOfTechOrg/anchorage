// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  canonicalEconomicOperations,
  canonicalReplayPrincipals,
  canonicalScheduleDispatch,
  hasDisputedSettlement,
  lifecycleFromRequestContext,
  nextLifecycleRevision,
  parseRunLifecycle,
  projectTerminalLifecycle,
  RUN_LIFECYCLE_CONTEXT_KEY,
  RunLifecycleBlockedError,
  type RunLifecycleBlockedReason,
  type RunLifecycleState,
  type RunTerminalStatus,
  terminalCleanupFor,
} from './run-lifecycle.js';

const MAX_REVISION = Number.MAX_SAFE_INTEGER;

function recordedIntent(
  status: RunTerminalStatus,
  revision = 7,
): RunLifecycleState & {
  transitionIntent: NonNullable<RunLifecycleState['transitionIntent']>;
} {
  return {
    version: 1,
    revision,
    deadlineAt: 0,
    economicOperations: [
      { id: 'operation-1', settlementState: 'settled' },
      { id: 'operation-2', settlementState: 'awaiting-receipt' },
    ],
    scheduleDispatch: { scheduleId: 'schedule-1', dispatchId: 'dispatch-1' },
    transitionIntent: {
      status,
      requestedAt: 100,
      replayPrincipals: [
        { kind: 'human', id: 'original-owner' },
        { kind: 'system', id: 'deadline-worker' },
      ],
      expectedRevision: revision,
      expectedDeadlineAt: 0,
    },
  };
}

describe('projectTerminalLifecycle', () => {
  it.each([
    ['cancelled', { code: 'CANCELLED', message: 'run was cancelled' }],
    ['timed_out', { code: 'TIMED_OUT', message: 'run deadline expired' }],
  ] as const)('projects recorded %s intent, preserving principals and lifecycle metadata', (status, error) => {
    const lifecycle = recordedIntent(status);
    const before = structuredClone(lifecycle);
    const { transitionIntent, ...base } = lifecycle;
    const principals = canonicalReplayPrincipals(
      transitionIntent.replayPrincipals,
    );
    const projected = projectTerminalLifecycle(
      lifecycle,
      status,
      0,
      principals,
    );
    expect(projected).toStrictEqual({
      ...base,
      revision: 8,
      terminal: {
        status,
        error,
        transitionedAt: 0,
        replayPrincipals: [
          { kind: 'human', id: 'original-owner' },
          { kind: 'system', id: 'deadline-worker' },
        ],
      },
    });
    expect(projected).not.toHaveProperty('transitionIntent');
    expect(projected.terminal).not.toHaveProperty('cleanupCompletedAt');
    expect(parseRunLifecycle(projected)).toStrictEqual(projected);
    expect(lifecycle).toStrictEqual(before);
    expect(terminalCleanupFor(projected)).toStrictEqual({
      revision: 8,
      status,
      cleanupCompleted: false,
      scheduleDispatch: { scheduleId: 'schedule-1', dispatchId: 'dispatch-1' },
    });
  });

  it('starts absent lifecycle metadata at revision one with the supplied principal', () => {
    const principals = canonicalReplayPrincipals([
      { kind: 'service', id: 'service-runner' },
    ]);
    const projected = projectTerminalLifecycle(
      undefined,
      'cancelled',
      50,
      principals,
    );
    expect(projected).toStrictEqual({
      version: 1,
      revision: 1,
      terminal: {
        status: 'cancelled',
        error: { code: 'CANCELLED', message: 'run was cancelled' },
        transitionedAt: 50,
        replayPrincipals: [{ kind: 'service', id: 'service-runner' }],
      },
    });
    expect(terminalCleanupFor(projected)).toStrictEqual({
      revision: 1,
      status: 'cancelled',
      cleanupCompleted: false,
    });
  });

  it('advances MAX minus one exactly to a still-readable MAX revision', () => {
    const lifecycle = recordedIntent('timed_out', MAX_REVISION - 1);
    const projected = projectTerminalLifecycle(
      lifecycle,
      'timed_out',
      20,
      canonicalReplayPrincipals(lifecycle.transitionIntent.replayPrincipals),
    );
    expect(projected.revision).toBe(MAX_REVISION);
    expect(parseRunLifecycle(projected)).toStrictEqual(projected);
    expect(terminalCleanupFor(projected)?.revision).toBe(MAX_REVISION);
    expect(lifecycle.revision).toBe(MAX_REVISION - 1);
  });

  it.each([
    'cancelled',
    'timed_out',
  ] as const)('refuses exhausted %s projection without changing readable stored intent', (status) => {
    const lifecycle = recordedIntent(status, MAX_REVISION);
    const before = structuredClone(lifecycle);
    expect(parseRunLifecycle(lifecycle)).toStrictEqual(lifecycle);
    expect(() =>
      projectTerminalLifecycle(
        lifecycle,
        status,
        200,
        canonicalReplayPrincipals(lifecycle.transitionIntent.replayPrincipals),
      ),
    ).toThrowError('run lifecycle revision cannot advance');
    expect(lifecycle).toStrictEqual(before);
    expect(parseRunLifecycle(lifecycle)).toStrictEqual(before);
    expect(terminalCleanupFor(lifecycle)).toBeUndefined();
  });
});

describe('terminalCleanupFor', () => {
  it('returns no cleanup for absent lifecycle or a nonterminal recorded intent', () => {
    expect(terminalCleanupFor(undefined)).toBeUndefined();
    expect(
      terminalCleanupFor({ version: 1, revision: MAX_REVISION }),
    ).toBeUndefined();
    expect(
      terminalCleanupFor(recordedIntent('cancelled', MAX_REVISION)),
    ).toBeUndefined();
  });

  it.each([
    'cancelled',
    'timed_out',
  ] as const)('keeps the current %s terminal revision and exact dispatch without incrementing', (status) => {
    const lifecycle = projectTerminalLifecycle(
      recordedIntent(status, MAX_REVISION - 1),
      status,
      100,
      canonicalReplayPrincipals([{ kind: 'agent', id: 'agent-runner' }]),
    );
    const before = structuredClone(lifecycle);
    const expected = {
      revision: MAX_REVISION,
      status,
      cleanupCompleted: false,
      scheduleDispatch: { scheduleId: 'schedule-1', dispatchId: 'dispatch-1' },
    };
    expect(terminalCleanupFor(lifecycle)).toStrictEqual(expected);
    expect(terminalCleanupFor(lifecycle)).toStrictEqual(expected);
    expect(lifecycle).toStrictEqual(before);
  });

  it.each([
    ['cancelled', 0],
    ['timed_out', 0],
    ['cancelled', 500],
    ['timed_out', 500],
    ['cancelled', Number.MAX_SAFE_INTEGER],
    ['timed_out', Number.MAX_SAFE_INTEGER],
  ] as const)('recognizes %s completion timestamp %s without a revision write', (status, cleanupCompletedAt) => {
    const lifecycle: RunLifecycleState = {
      version: 1,
      revision: MAX_REVISION,
      terminal: {
        status,
        error:
          status === 'cancelled'
            ? { code: 'CANCELLED', message: 'run was cancelled' }
            : { code: 'TIMED_OUT', message: 'run deadline expired' },
        transitionedAt: 0,
        replayPrincipals: [{ kind: 'human', id: 'original-owner' }],
        cleanupCompletedAt,
      },
    };
    const before = structuredClone(lifecycle);
    expect(parseRunLifecycle(lifecycle)).toStrictEqual(lifecycle);
    expect(terminalCleanupFor(lifecycle)).toStrictEqual({
      revision: MAX_REVISION,
      status,
      cleanupCompleted: true,
    });
    expect(lifecycle).toStrictEqual(before);
  });
});

describe('nextLifecycleRevision', () => {
  it.each([
    [0, 1],
    [1, 2],
    [23, 24],
    [MAX_REVISION - 1, MAX_REVISION],
  ])('advances %s exactly to %s', (current, expected) => {
    expect(nextLifecycleRevision(current)).toBe(expected);
  });

  it.each([
    MAX_REVISION,
    MAX_REVISION + 1,
    -1,
    -0.5,
    0.5,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.NEGATIVE_INFINITY,
  ])('rejects invalid or exhausted revision %s before arithmetic', (revision) => {
    expect(() => nextLifecycleRevision(revision)).toThrowError(
      'run lifecycle revision cannot advance',
    );
  });

  it('keeps exhaustion a plain internal error rather than disputed settlement', () => {
    try {
      nextLifecycleRevision(MAX_REVISION);
      throw new Error('expected exhaustion');
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect((error as Error).constructor).toBe(Error);
      expect((error as Error).message).toBe(
        'run lifecycle revision cannot advance',
      );
      expect(error).not.toHaveProperty('reason');
    }
  });

  it('does not narrow the existing persisted revision boundary', () => {
    expect(
      parseRunLifecycle({ version: 1, revision: MAX_REVISION }),
    ).toStrictEqual({
      version: 1,
      revision: MAX_REVISION,
    });
    expect(() => parseRunLifecycle({ version: 1, revision: 0 })).toThrowError(
      'stored run lifecycle is malformed',
    );
  });
});

describe('lifecycle metadata compatibility', () => {
  it('canonicalizes principals by kind and id without changing their first order', () => {
    const principals = [
      { kind: 'human' as const, id: 'same-id' },
      { kind: 'system' as const, id: 'same-id' },
      { kind: 'human' as const, id: 'same-id' },
    ];
    expect(canonicalReplayPrincipals(principals)).toStrictEqual([
      { kind: 'human', id: 'same-id' },
      { kind: 'system', id: 'same-id' },
    ]);
    expect(principals).toHaveLength(3);
  });

  it('preserves schedule and host-defined economic metadata through the existing codecs', () => {
    const lifecycle = recordedIntent('timed_out');
    expect(
      canonicalEconomicOperations(lifecycle.economicOperations),
    ).toStrictEqual(lifecycle.economicOperations);
    expect(canonicalScheduleDispatch(lifecycle.scheduleDispatch)).toStrictEqual(
      lifecycle.scheduleDispatch,
    );
    expect(
      lifecycleFromRequestContext({
        unrelated: 'untouched',
        [RUN_LIFECYCLE_CONTEXT_KEY]: lifecycle,
      }),
    ).toStrictEqual(lifecycle);
    expect(lifecycleFromRequestContext(undefined)).toBeUndefined();
    expect(canonicalEconomicOperations(undefined)).toBeUndefined();
    expect(canonicalScheduleDispatch(undefined)).toBeUndefined();
  });

  it('continues to interpret only the exact disputed settlement state', () => {
    const lifecycle: RunLifecycleState = {
      version: 1,
      revision: MAX_REVISION,
      economicOperations: [{ id: 'operation-1', settlementState: 'disputed' }],
    };
    expect(hasDisputedSettlement(undefined)).toBe(false);
    expect(hasDisputedSettlement(recordedIntent('cancelled'))).toBe(false);
    expect(hasDisputedSettlement(lifecycle)).toBe(true);
    expect(
      hasDisputedSettlement({
        ...lifecycle,
        economicOperations: [
          { id: 'operation-1', settlementState: 'DISPUTED' },
        ],
      }),
    ).toBe(false);
    expect(parseRunLifecycle(lifecycle)).toStrictEqual(lifecycle);
  });

  it('preserves the moved blocked error name, message and reason object', () => {
    const reason: RunLifecycleBlockedReason = {
      code: 'DISPUTED_SETTLEMENT',
      message:
        'run termination is blocked while an economic operation is disputed',
    };
    const error = new RunLifecycleBlockedError(reason);
    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe('RunLifecycleBlockedError');
    expect(error.message).toBe(reason.message);
    expect(error.reason).toBe(reason);
    expect(error).not.toHaveProperty('status');
  });
});
