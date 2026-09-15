// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
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

describe('C dense economic-operation format', () => {
  const entries = [
    { id: 'first', settlementState: 'settled' },
    { id: 'second', settlementState: 'held' },
    { id: 'third', settlementState: 'disputed' },
  ];

  it.each([
    'leading',
    'interior',
    'trailing',
    'all-hole',
  ] as const)('C rejects sparse economic operations in the shared lifecycle parser: %s', (shape) => {
    const operations = shape === 'all-hole' ? new Array(3) : [...entries];
    if (shape !== 'all-hole')
      delete operations[{ leading: 0, interior: 1, trailing: 2 }[shape]];
    for (const read of [
      () => canonicalEconomicOperations(operations),
      () =>
        parseRunLifecycle({
          version: 1,
          revision: 1,
          economicOperations: operations,
        }),
    ]) {
      let error: unknown;
      try {
        read();
      } catch (cause) {
        error = cause;
      }
      expect(error).toBeInstanceOf(Error);
      expect(Object.getPrototypeOf(error)).toBe(Error.prototype);
      expect(error).toEqual(new Error('stored run lifecycle is malformed'));
    }
  });

  it.each([
    'dense',
    'inherited',
    'empty',
  ] as const)('C keeps dense and inherited economic data readable: %s', (shape) => {
    const operations = shape === 'empty' ? [] : [...entries];
    if (shape === 'inherited') {
      const prototype = Object.create(Array.prototype);
      Object.defineProperty(prototype, '1', { value: entries[1] });
      Object.setPrototypeOf(operations, prototype);
      delete operations[1];
    }
    const expected = shape === 'empty' ? [] : entries;
    const captured = canonicalEconomicOperations(operations);
    expect(captured).toEqual(expected);
    expect(captured).not.toBe(operations);
    expect(Object.getPrototypeOf(captured)).toBe(Array.prototype);
    const lifecycle = { version: 1, revision: 1, economicOperations: captured };
    expect(parseRunLifecycle(JSON.parse(JSON.stringify(lifecycle)))).toEqual(
      lifecycle,
    );
    expect(Object.isFrozen(operations)).toBe(false);
  });

  it('C shared economic parsing ignores caller methods and reads primitives once', () => {
    const id = vi
      .fn()
      .mockReturnValueOnce('first')
      .mockImplementation(() => {
        throw new Error('second id read');
      });
    const settlementState = vi
      .fn()
      .mockReturnValueOnce('settled')
      .mockImplementation(() => {
        throw new Error('second state read');
      });
    const operations = [
      {
        get id() {
          return id();
        },
        get settlementState() {
          return settlementState();
        },
      },
    ];
    const map = vi.fn(() => []);
    const iterator = vi.fn(() => {
      throw new Error('caller iterator');
    });
    Object.defineProperty(operations, 'map', { value: map });
    Object.defineProperty(operations, Symbol.iterator, { value: iterator });
    expect(canonicalEconomicOperations(operations)).toEqual([
      { id: 'first', settlementState: 'settled' },
    ]);
    expect(id).toHaveBeenCalledTimes(1);
    expect(settlementState).toHaveBeenCalledTimes(1);
    expect(map).not.toHaveBeenCalled();
    expect(iterator).not.toHaveBeenCalled();
  });

  it.each([
    null,
    '1',
    true,
    -1,
    1.5,
    NaN,
    Infinity,
    Number.MAX_SAFE_INTEGER,
  ])('C shared economic parsing refuses malformed array length: %s', (length) => {
    const operations = new Proxy([...entries], {
      get(target, key, receiver) {
        return key === 'length' ? length : Reflect.get(target, key, receiver);
      },
    });
    let error: unknown;
    try {
      canonicalEconomicOperations(operations);
    } catch (cause) {
      error = cause;
    }
    expect(error).toBeInstanceOf(Error);
    expect(Object.getPrototypeOf(error)).toBe(Error.prototype);
    expect(error).toEqual(new Error('stored run lifecycle is malformed'));
  });

  it('C shared economic parsing preserves first getter faults and avoids array species', () => {
    const fault = new Error('first economic id read');
    const operations = [
      {
        get id(): string {
          throw fault;
        },
        settlementState: 'held',
      },
    ];
    let error: unknown;
    try {
      canonicalEconomicOperations(operations);
    } catch (cause) {
      error = cause;
    }
    expect(error).toBe(fault);
    const species = vi.fn();
    class Operations extends Array<(typeof entries)[number]> {
      static get [Symbol.species]() {
        species();
        return Array;
      }
    }
    expect(canonicalEconomicOperations(new Operations(...entries))).toEqual(
      entries,
    );
    expect(species).not.toHaveBeenCalled();
  });
});

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
