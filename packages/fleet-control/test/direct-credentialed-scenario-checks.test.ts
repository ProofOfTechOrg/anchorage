// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { describe, expect, it } from 'vitest';
import type { DirectWorkerVersionObservation } from '../scripts/direct-credentialed-observations.mjs';
import { DIRECT_SCENARIO_ARRAY_MAXIMA } from '../scripts/direct-credentialed-run-state.mjs';
import {
  DIRECT_SCENARIO_INVOCATION_BUDGET,
  DIRECT_SCENARIO_MIN_INVOCATIONS,
  DIRECT_SCENARIO_PHASES,
} from '../scripts/direct-credentialed-scenario-budget.mjs';
import {
  changedBy,
  checkFootprint,
  checkInterruptedItems,
  checkInterruptionWitness,
  checkInvocationHeadroom,
  checkItemConvergence,
  checkTrafficDistribution,
  type DirectScenarioFootprint,
  type DirectScenarioInterruptedItem,
  type DirectScenarioPhase,
  equal,
  expectedVersion,
  hash,
  jsonHash,
  migrationInterruptSettled,
  migrationStartSettled,
  NORMAL_ROLES,
  operationFacts,
  parse,
  phaseInvocationReserve,
  recordFacts,
  requireFact,
  SCENARIO_ROLES,
  zeroAttempts,
} from '../scripts/direct-credentialed-scenario-checks.mjs';
import { directCleanupReceiptDigest } from '../scripts/direct-reference-receipt.mjs';
import type { CleanupTerminalReceipt } from '../src/types.js';

function refusal(action: () => unknown): string {
  try {
    action();
  } catch (error) {
    return (error as { code?: string }).code ?? 'uncoded';
  }
  return 'accepted';
}

function cause(action: () => unknown): Readonly<{
  code?: string;
  detail?: string;
}> {
  try {
    action();
  } catch (error) {
    const { code, detail } = error as { code?: string; detail?: string };
    return { code, detail };
  }
  return { code: 'accepted' };
}

function observation(
  versionId: string,
  trafficPercentage: number,
  versions: readonly { versionId: string; percentage: number }[],
  activeVersionId = versionId,
): DirectWorkerVersionObservation {
  return {
    role: 'a',
    versionId,
    databaseId: 'database',
    specDigest: 'a'.repeat(64),
    applicationRelease: '1',
    accountId: 'account',
    tenantTag: 'tenant',
    environment: 'production',
    scriptName: 'script',
    currentDeployment: {
      deploymentId: 'deployment',
      activeVersionId,
      versions,
    },
    trafficPercentage,
    cpuLimitMs: 50,
    subrequestLimit: 50,
    schemaVersion: 2,
    namespaces: [],
    bucket: {
      name: 'bucket',
      jurisdiction: 'default',
      creationDate: '2026-09-10T00:00:00.000Z',
    },
  };
}

const item = (ordinal: number, status: string, planCursor = 0) => ({
  ordinal,
  status,
  planCursor,
});

describe('scenario fact helpers', () => {
  it('reports the refusal code the caller supplied and defaults to an observation mismatch', () => {
    expect(refusal(() => requireFact(true))).toBe('accepted');
    expect(refusal(() => requireFact(false, 'proof-unavailable'))).toBe(
      'proof-unavailable',
    );
    expect(refusal(() => requireFact(false))).toBe('observation-mismatch');
    expect(refusal(() => equal({ a: 1 }, { a: 1 }))).toBe('accepted');
    expect(refusal(() => equal({ a: 1 }, { a: 2 }))).toBe(
      'observation-mismatch',
    );
    expect(refusal(() => parse(42))).toBe('observation-mismatch');
    expect(parse('{"a":1}')).toEqual({ a: 1 });
  });

  it('derives digests and the zero attempt triple', () => {
    expect(hash('value')).toBe(
      createHash('sha256').update('value').digest('hex'),
    );
    expect(jsonHash({ a: 1 })).toBe(hash(JSON.stringify({ a: 1 })));
    expect(zeroAttempts()).toEqual({
      provider: 0,
      maintenance: 0,
      application: 0,
    });
    expect([...NORMAL_ROLES]).toEqual(['a', 'b']);
    expect([...SCENARIO_ROLES]).toEqual(['a', 'b', 'recovery']);
  });

  it('projects operation anchors without the frozen input and records without undefined', () => {
    expect(
      operationFacts({
        slot: 'migration-next',
        operationId: 'operation',
        inputJson: '{"records":[]}',
        tokenRevision: 2,
      }),
    ).toEqual({
      slot: 'migration-next',
      operationId: 'operation',
      inputSha256: hash('{"records":[]}'),
      tokenRevision: 2,
    });
    expect(recordFacts({ role: 'a', present: false })).toEqual({
      role: 'a',
      present: false,
      phase: null,
      desiredSpecDigest: null,
      pendingSpecDigest: null,
      artifactVersion: null,
      pendingArtifactVersion: null,
      databaseId: null,
    });
  });

  it('selects the pending artifact only for a candidate expectation and refuses an absent record', () => {
    const record = {
      role: 'a' as const,
      present: true,
      artifactVersion: 'current',
      pendingArtifactVersion: 'next',
      desiredSpecDigest: 'a'.repeat(64),
      pendingSpecDigest: 'b'.repeat(64),
      databaseId: 'database',
    };
    expect(expectedVersion(record, '1')).toEqual({
      role: 'a',
      versionId: 'current',
      databaseId: 'database',
      specDigest: 'a'.repeat(64),
      applicationRelease: '1',
    });
    expect(expectedVersion(record, '2', true)).toEqual({
      role: 'a',
      versionId: 'next',
      databaseId: 'database',
      specDigest: 'b'.repeat(64),
      applicationRelease: '2',
    });
    expect(
      refusal(() => expectedVersion({ role: 'a', present: false }, '1')),
    ).toBe('observation-mismatch');
    expect(refusal(() => expectedVersion(null, '1'))).toBe(
      'observation-mismatch',
    );
  });
});

describe('scenario migration guards', () => {
  it('requires item a to complete before item b leaves pending', () => {
    expect(
      refusal(() =>
        checkItemConvergence([item(0, 'active'), item(1, 'pending')]),
      ),
    ).toBe('accepted');
    expect(
      refusal(() =>
        checkItemConvergence([item(0, 'complete'), item(1, 'active')]),
      ),
    ).toBe('accepted');
    expect(
      refusal(() =>
        checkItemConvergence([item(0, 'active'), item(1, 'active')]),
      ),
    ).toBe('observation-mismatch');
    expect(
      refusal(() =>
        checkItemConvergence([item(0, 'pending'), item(1, 'complete')]),
      ),
    ).toBe('observation-mismatch');
  });

  it('requires the candidate at zero percent beside the original at one hundred in one deployment', () => {
    const previous = observation('old', 100, [
      { versionId: 'old', percentage: 100 },
    ]);
    const candidate = observation(
      'new',
      0,
      [
        { versionId: 'old', percentage: 100 },
        { versionId: 'new', percentage: 0 },
      ],
      'old',
    );
    expect(refusal(() => checkTrafficDistribution(candidate, previous))).toBe(
      'accepted',
    );
    expect(
      refusal(() =>
        checkTrafficDistribution(
          observation(
            'new',
            0,
            [
              { versionId: 'old', percentage: 100 },
              { versionId: 'new', percentage: 0 },
              { versionId: 'new', percentage: 0 },
            ],
            'old',
          ),
          previous,
        ),
      ),
    ).toBe('observation-mismatch');
    expect(
      refusal(() =>
        checkTrafficDistribution(
          observation(
            'new',
            0,
            [
              { versionId: 'old', percentage: 0 },
              { versionId: 'new', percentage: 0 },
            ],
            'old',
          ),
          previous,
        ),
      ),
    ).toBe('observation-mismatch');
    expect(
      refusal(() =>
        checkTrafficDistribution(
          observation('new', 0, [{ versionId: 'new', percentage: 0 }], 'old'),
          previous,
        ),
      ),
    ).toBe('observation-mismatch');
    expect(
      refusal(() =>
        checkTrafficDistribution(
          observation(
            'new',
            100,
            [
              { versionId: 'old', percentage: 100 },
              { versionId: 'new', percentage: 0 },
            ],
            'old',
          ),
          previous,
        ),
      ),
    ).toBe('observation-mismatch');
  });
});

describe('scenario one-shot mutation reconciliation', () => {
  const started = {
    slot: 'migration-next' as const,
    operationId: 'operation',
    inputJson: '{}',
    tokenRevision: 1,
  };
  const startCall = {
    ordinal: 9,
    action: { kind: 'migration-start' } as const,
    outcome: 'returned',
  };

  it('treats a present migration slot as the settled start so a resume issues no second start', () => {
    expect(migrationStartSettled({ operations: [] }, startCall)).toBe(false);
    expect(
      migrationStartSettled(
        {
          operations: [
            {
              slot: 'inventory-before',
              operationId: 'other',
              inputJson: '{}',
              tokenRevision: 0,
            },
          ],
        },
        startCall,
      ),
    ).toBe(false);
    expect(migrationStartSettled({ operations: [started] }, startCall)).toBe(
      true,
    );
  });

  it('refuses a migration slot the recorded mutation does not account for', () => {
    expect(
      refusal(() =>
        migrationStartSettled(
          { operations: [{ ...started, operationId: null }] },
          startCall,
        ),
      ),
    ).toBe('proof-unavailable');
    expect(
      refusal(() => migrationStartSettled({ operations: [started] }, null)),
    ).toBe('proof-unavailable');
    expect(
      refusal(() =>
        migrationStartSettled(
          { operations: [started] },
          { ...startCall, outcome: 'reference-refused' },
        ),
      ),
    ).toBe('proof-unavailable');
    expect(
      refusal(() =>
        migrationStartSettled(
          { operations: [started] },
          { ...startCall, action: { kind: 'control-read' } },
        ),
      ),
    ).toBe('proof-unavailable');
  });

  it('treats an existing interruption witness as the consumed injection so a resume issues no second continue', () => {
    const loss = {
      ordinal: 12,
      action: { kind: 'migration-continue' } as const,
      outcome: 'injected-response-loss',
    };
    expect(migrationInterruptSettled({ interruption: null }, loss)).toBe(false);
    expect(
      migrationInterruptSettled({ interruption: '{"version":1}' }, loss),
    ).toBe(true);
  });

  it('refuses an interruption witness that the recorded mutation does not account for', () => {
    expect(
      refusal(() =>
        migrationInterruptSettled({ interruption: '{"version":1}' }, null),
      ),
    ).toBe('proof-unavailable');
    expect(
      refusal(() =>
        migrationInterruptSettled(
          { interruption: '{"version":1}' },
          {
            ordinal: 12,
            action: { kind: 'migration-continue' },
            outcome: 'returned',
          },
        ),
      ),
    ).toBe('proof-unavailable');
    expect(
      refusal(() =>
        migrationInterruptSettled(
          { interruption: '{"version":1}' },
          {
            ordinal: 12,
            action: { kind: 'control-read' },
            outcome: 'injected-response-loss',
          },
        ),
      ),
    ).toBe('proof-unavailable');
  });
});

describe('scenario reconciliation allowance', () => {
  it('maps each action family to the slot and roles it may change', () => {
    expect(changedBy(null)).toEqual({ slots: [], roles: [] });
    expect(changedBy({ kind: 'audit-start', slot: 'audit-before' })).toEqual({
      slots: ['audit-before'],
      roles: [],
    });
    expect(
      changedBy({ kind: 'audit-page', slot: 'audit-after', limit: 32 }),
    ).toEqual({ slots: ['audit-after'], roles: [] });
    expect(
      changedBy({ kind: 'provision', role: 'a', release: 'initial' }),
    ).toEqual({ slots: ['cleanup-a'], roles: ['a'] });
    expect(
      changedBy({ kind: 'provision', role: 'recovery', release: 'initial' }),
    ).toEqual({ slots: ['cleanup-recovery-initial'], roles: ['recovery'] });
    expect(
      changedBy({
        kind: 'provision',
        role: 'recovery',
        release: 'failed-recovery',
      }),
    ).toEqual({ slots: ['cleanup-recovery'], roles: ['recovery'] });
    expect(
      changedBy({ kind: 'inventory-start', slot: 'inventory-before' }),
    ).toEqual({ slots: ['inventory-before'], roles: [] });
    expect(changedBy({ kind: 'migration-continue' })).toEqual({
      slots: ['migration-next'],
      roles: ['a', 'b'],
    });
    expect(changedBy({ kind: 'cleanup-continue', role: 'recovery' })).toEqual({
      slots: ['cleanup-recovery'],
      roles: ['recovery'],
    });
    expect(changedBy({ kind: 'decommission-start', role: 'b' })).toEqual({
      slots: ['decommission-b'],
      roles: ['b'],
    });
    expect(changedBy({ kind: 'force-recovery' })).toEqual({
      slots: [],
      roles: ['recovery'],
    });
    expect(changedBy({ kind: 'recover-force-residual' })).toEqual({
      slots: [],
      roles: ['recovery'],
    });
    expect(
      changedBy({ kind: 'tenant-probe', role: 'a', operation: 'object-put' }),
    ).toEqual({ slots: [], roles: [] });
    expect(changedBy({ kind: 'control-read' })).toEqual({
      slots: [],
      roles: [],
    });
  });
});

const DECLARED_PHASES = [
  'provision-a',
  'provision-b',
  'inventory-before',
  'audit-before',
  'fence-drain',
  'migration-start',
  'migration-interrupt',
  'migration-restart',
  'migration',
  'post-migration',
  'fence-reopen',
  'fence-proofs',
  'inventory-after',
  'audit-after',
  'failed-recovery',
  'cleanup-recovery',
  'provision-recovery',
  'delete-objects',
  'decommission-a',
  'decommission-b',
  'force-recovery',
  'force-observe',
  'recover-force-residual',
  'complete',
] as const;

describe('scenario invocation budget', () => {
  const zeroCalls = () =>
    Object.fromEntries(
      DIRECT_SCENARIO_PHASES.map((phase) => [phase, 0]),
    ) as Record<DirectScenarioPhase, number>;
  const unknownPhase = 'not-a-phase' as DirectScenarioPhase;
  const reserveTotal = () =>
    Object.values(DIRECT_SCENARIO_INVOCATION_BUDGET).reduce(
      (sum, entry) => sum + entry.reserve,
      0,
    );

  it('derives the phase list from the budget table and keeps both columns on the rule', () => {
    console.log(
      'A1_FENCE_BUDGET',
      JSON.stringify(
        Object.fromEntries(
          ['fence-drain', 'fence-reopen', 'fence-proofs'].map((phase) => [
            phase,
            DIRECT_SCENARIO_INVOCATION_BUDGET[phase as DirectScenarioPhase],
          ]),
        ),
      ),
    );
    console.log('A1_SCENARIO_MIN_INVOCATIONS', DIRECT_SCENARIO_MIN_INVOCATIONS);
    expect(Object.keys(DIRECT_SCENARIO_INVOCATION_BUDGET)).toEqual([
      ...DIRECT_SCENARIO_PHASES,
    ]);
    for (const [phase, entry] of Object.entries(
      DIRECT_SCENARIO_INVOCATION_BUDGET,
    )) {
      expect({ phase, ...entry }).toEqual({
        phase,
        measured: entry.measured,
        ceiling: Math.max(16, Math.ceil((entry.measured * 2) / 8) * 8),
        reserve: Math.max(4, Math.ceil((entry.measured * 1.25) / 4) * 4),
      });
      expect(entry.ceiling - entry.measured).toBeGreaterThanOrEqual(8);
    }
  });

  it('reserves the reserve of every phase from the current one onward', () => {
    expect(phaseInvocationReserve('provision-a')).toBe(reserveTotal());
    expect(phaseInvocationReserve('complete')).toBe(
      DIRECT_SCENARIO_INVOCATION_BUDGET.complete.reserve,
    );
    expect(phaseInvocationReserve('force-observe')).toBe(
      DIRECT_SCENARIO_INVOCATION_BUDGET['force-observe'].reserve +
        DIRECT_SCENARIO_INVOCATION_BUDGET['recover-force-residual'].reserve +
        DIRECT_SCENARIO_INVOCATION_BUDGET.complete.reserve,
    );
    expect(refusal(() => phaseInvocationReserve(unknownPhase))).toBe(
      'invalid-input',
    );
  });

  it('separates a spent phase ceiling from a run that cannot cover the phases left', () => {
    const calls = zeroCalls();
    const total = phaseInvocationReserve('provision-a');
    expect(
      refusal(() => checkInvocationHeadroom('provision-a', calls, total)),
    ).toBe('accepted');
    expect(
      cause(() => checkInvocationHeadroom('provision-a', calls, total - 1)),
    ).toEqual({ code: 'budget-exhausted', detail: 'run-reserve' });
    expect(
      refusal(() => checkInvocationHeadroom('provision-a', calls, 0)),
    ).toBe('invocation-budget-exhausted');
    const spent = {
      ...calls,
      'provision-a': DIRECT_SCENARIO_INVOCATION_BUDGET['provision-a'].ceiling,
    };
    expect(
      cause(() => checkInvocationHeadroom('provision-a', spent, total)),
    ).toEqual({ code: 'budget-exhausted', detail: 'phase-ceiling' });
    expect(
      refusal(() =>
        checkInvocationHeadroom(
          'provision-a',
          { ...calls, 'provision-a': 1 },
          total - 1,
        ),
      ),
    ).toBe('accepted');
    expect(
      refusal(() => checkInvocationHeadroom(unknownPhase, calls, total)),
    ).toBe('invalid-input');
  });

  it('orders the phases the journal proof rules and resume check index by', () => {
    const declared: readonly DirectScenarioPhase[] = DECLARED_PHASES;
    const runtime: readonly (typeof DECLARED_PHASES)[number][] =
      DIRECT_SCENARIO_PHASES;
    expect([...runtime]).toEqual([...declared]);
    expect(new Set(DECLARED_PHASES).size).toBe(DECLARED_PHASES.length);
    expect(DIRECT_SCENARIO_PHASES.at(-1)).toBe('complete');
    const ordered = (
      earlier: DirectScenarioPhase,
      later: DirectScenarioPhase,
    ) =>
      DIRECT_SCENARIO_PHASES.indexOf(earlier) <
      DIRECT_SCENARIO_PHASES.indexOf(later);
    for (const [producer, consumer] of [
      ['provision-a', 'migration'],
      ['provision-b', 'migration'],
      ['migration', 'post-migration'],
      ['post-migration', 'delete-objects'],
      ['delete-objects', 'decommission-a'],
      ['delete-objects', 'decommission-b'],
      ['inventory-before', 'audit-before'],
      ['audit-before', 'fence-drain'],
      ['fence-drain', 'migration-start'],
      ['post-migration', 'fence-reopen'],
      ['fence-reopen', 'fence-proofs'],
      ['fence-proofs', 'inventory-after'],
      ['inventory-after', 'audit-after'],
      ['migration-interrupt', 'migration-restart'],
      ['cleanup-recovery', 'provision-recovery'],
      ['force-recovery', 'force-observe'],
      ['force-observe', 'recover-force-residual'],
    ] as const)
      expect({
        producer,
        consumer,
        ordered: ordered(producer, consumer),
      }).toEqual({ producer, consumer, ordered: true });
  });

  it('holds back the later-phase reserve once a phase spends past its own', () => {
    const calls = zeroCalls();
    const observe = DIRECT_SCENARIO_INVOCATION_BUDGET['force-observe'];
    expect({ reserve: observe.reserve, ceiling: observe.ceiling }).toEqual({
      reserve: 4,
      ceiling: 16,
    });
    expect(phaseInvocationReserve('force-observe')).toBe(12);
    expect(phaseInvocationReserve('recover-force-residual')).toBe(8);
    for (const [spent, minimum] of [
      [0, 12],
      [4, 8],
      [11, 8],
      [15, 8],
    ] as const) {
      const phaseCalls = { ...calls, 'force-observe': spent };
      expect(
        refusal(() =>
          checkInvocationHeadroom('force-observe', phaseCalls, minimum),
        ),
      ).toBe('accepted');
      expect(
        cause(() =>
          checkInvocationHeadroom('force-observe', phaseCalls, minimum - 1),
        ),
      ).toEqual({ code: 'budget-exhausted', detail: 'run-reserve' });
    }
    const migration = DIRECT_SCENARIO_INVOCATION_BUDGET.migration;
    expect({ reserve: migration.reserve, ceiling: migration.ceiling }).toEqual({
      reserve: 132,
      ceiling: 216,
    });
    expect(phaseInvocationReserve('migration')).toBe(508);
    const overspent = { ...calls, migration: 200 };
    expect(
      refusal(() => checkInvocationHeadroom('migration', overspent, 376)),
    ).toBe('accepted');
    expect(
      cause(() => checkInvocationHeadroom('migration', overspent, 375)),
    ).toEqual({ code: 'budget-exhausted', detail: 'run-reserve' });
  });

  it('refuses a phase call map without a usable count for the phase', () => {
    const total = phaseInvocationReserve('provision-a');
    const absent = {} as Record<DirectScenarioPhase, number>;
    expect(
      refusal(() => checkInvocationHeadroom('provision-a', absent, total)),
    ).toBe('invalid-input');
    expect(
      refusal(() =>
        checkInvocationHeadroom(
          'provision-a',
          null as unknown as Record<DirectScenarioPhase, number>,
          total,
        ),
      ),
    ).toBe('invalid-input');
    for (const value of [-1, 1.5, Number.MAX_SAFE_INTEGER + 1])
      expect(
        refusal(() =>
          checkInvocationHeadroom(
            'provision-a',
            { ...zeroCalls(), 'provision-a': value },
            total,
          ),
        ),
      ).toBe('invalid-input');
  });

  it('refuses an inherited phase key and a remaining count that is not an integer', () => {
    const calls = zeroCalls();
    const total = phaseInvocationReserve('provision-a');
    expect(
      refusal(() =>
        checkInvocationHeadroom(
          'constructor' as DirectScenarioPhase,
          calls,
          total,
        ),
      ),
    ).toBe('invalid-input');
    for (const remaining of [1.5, Number.MAX_SAFE_INTEGER + 1])
      expect(
        refusal(() => checkInvocationHeadroom('provision-a', calls, remaining)),
      ).toBe('invalid-input');
  });

  it('declares the invocation floor the shipped configuration clears', async () => {
    const config = JSON.parse(
      await readFile(
        new URL(
          '../scripts/direct-credentialed-conformance.example.json',
          import.meta.url,
        ),
        'utf8',
      ),
    ) as { referenceWorker: { maxInvocations: number } };
    expect(DIRECT_SCENARIO_MIN_INVOCATIONS).toBe(reserveTotal() + 1 + 8);
    expect(config.referenceWorker.maxInvocations).toBeGreaterThanOrEqual(
      DIRECT_SCENARIO_MIN_INVOCATIONS,
    );
  });
});

describe('scenario interruption witness', () => {
  const OPERATION = 'migration-operation';
  const ENTRY_DIGEST = 'e'.repeat(64);
  const TARGET_DIGEST = 'f'.repeat(64);
  const expected = {
    operationId: OPERATION,
    tenantTag: 'tenant',
    environment: 'production',
  };
  const value = () => ({
    version: 1,
    boundary: 'after-migration-admission',
    slot: 'migration-next',
    operationId: OPERATION,
    claimJson: JSON.stringify({ operationId: OPERATION, revision: 1 }),
    returnedTokenJson: JSON.stringify({ operationId: OPERATION, revision: 2 }),
    item: {
      ordinal: 0,
      beforeStatus: 'pending',
      afterStatus: 'active',
      planCursor: 0,
      tenantTag: 'tenant',
      environment: 'production',
      entryRecordDigest: ENTRY_DIGEST,
      targetSpecDigest: TARGET_DIGEST,
    },
  });
  const serialized = (mutate: (current: ReturnType<typeof value>) => void) => {
    const current = value();
    mutate(current);
    return JSON.stringify(current);
  };

  it('reads the claim and the successor the boundary recorded', () => {
    const result = checkInterruptionWitness(JSON.stringify(value()), expected);
    expect(result.value).toEqual(value());
    expect(result.claim).toEqual({ operationId: OPERATION, revision: 1 });
    expect(result.successor).toEqual({ operationId: OPERATION, revision: 2 });
  });

  it('refuses a witness the recorded operation and item do not account for', () => {
    expect(refusal(() => checkInterruptionWitness(42, expected))).toBe(
      'observation-mismatch',
    );
    for (const mutate of [
      (current) => {
        current.version = 2;
      },
      (current) => {
        current.boundary = 'before-migration-admission';
      },
      (current) => {
        current.slot = 'inventory-before';
      },
      (current) => {
        current.operationId = 'other-operation';
      },
      (current) => {
        current.claimJson = JSON.stringify({
          operationId: 'other-operation',
          revision: 1,
        });
      },
      (current) => {
        current.returnedTokenJson = JSON.stringify({
          operationId: OPERATION,
          revision: 1,
        });
      },
      (current) => {
        current.returnedTokenJson = JSON.stringify({
          operationId: 'other-operation',
          revision: 2,
        });
      },
      (current) => {
        current.item.ordinal = 1;
      },
      (current) => {
        current.item.beforeStatus = 'active';
      },
      (current) => {
        current.item.afterStatus = 'pending';
      },
      (current) => {
        current.item.planCursor = 1;
      },
      (current) => {
        current.item.tenantTag = 'other-tenant';
      },
      (current) => {
        current.item.environment = 'staging';
      },
    ] satisfies ((current: ReturnType<typeof value>) => void)[])
      expect(
        refusal(() => checkInterruptionWitness(serialized(mutate), expected)),
      ).toBe('observation-mismatch');
  });

  it('requires the interrupted item active at its recorded cursor beside a pending successor', () => {
    const witness = checkInterruptionWitness(
      JSON.stringify(value()),
      expected,
    ).value;
    const active = {
      status: 'active',
      planCursor: 0,
      entryRecordDigest: ENTRY_DIGEST,
      targetSpecDigest: TARGET_DIGEST,
    };
    const pending = { ...active, status: 'pending' };
    expect(
      refusal(() => checkInterruptedItems(witness, [active, pending])),
    ).toBe('accepted');
    const pairs: readonly (readonly [
      DirectScenarioInterruptedItem,
      DirectScenarioInterruptedItem,
    ])[] = [
      [{ ...active, status: 'complete' }, pending],
      [{ ...active, planCursor: 1 }, pending],
      [{ ...active, entryRecordDigest: 'a'.repeat(64) }, pending],
      [{ ...active, targetSpecDigest: 'a'.repeat(64) }, pending],
      [active, { ...pending, status: 'active' }],
    ];
    for (const items of pairs)
      expect(refusal(() => checkInterruptedItems(witness, items))).toBe(
        'observation-mismatch',
      );
  });
});

describe('scenario recovery footprint', () => {
  const DATABASE_NAME = 'recovery-database-name';
  const CREATED = '2026-09-10T00:00:00.000Z';
  const NAMESPACES = ['maintenance-namespace', 'runner-namespace'];
  const receipt: CleanupTerminalReceipt = {
    version: 1,
    operationId: 'cleanup-operation',
    tenantTag: 'tenant',
    environment: 'production',
    backend: 'plain-worker',
    scriptName: 'recovery-script',
    databaseId: 'recovery-database',
    databaseName: DATABASE_NAME,
    authority: 'provisioning-rollback',
    admittedPhase: 'database-reserved',
    disposition: 'reservation-cleared',
    evidence: {
      eligibility: 'reservation-only',
      ingressRemoved: true,
      workerAbsent: true,
      platformResourcesAbsent: true,
      applicationR2Settled: true,
      databaseAbsentReadback: true,
    },
    completedAtMs: 1,
  };
  const resource: DirectWorkerVersionObservation = {
    ...observation('recovery-version', 100, [
      { versionId: 'recovery-version', percentage: 100 },
    ]),
    role: 'recovery',
    scriptName: 'recovery-script',
    databaseId: 'recovery-database',
    namespaces: [
      {
        binding: 'RUNNER',
        className: 'Runner',
        namespaceId: 'runner-namespace',
      },
      {
        binding: 'MAINTENANCE',
        className: 'Maintenance',
        namespaceId: 'maintenance-namespace',
      },
    ],
    bucket: { name: 'bucket', jurisdiction: 'default', creationDate: CREATED },
  };
  const footprint = (retained: boolean): DirectScenarioFootprint => ({
    version: 1,
    role: 'recovery',
    beforeIdentitySha256: 'b'.repeat(64),
    fleetRecordPresent: false,
    deploymentClaimsPresent: false,
    database: {
      id: 'recovery-database',
      expectedName: DATABASE_NAME,
      observedName: null,
    },
    worker: {
      scriptName: 'recovery-script',
      scriptPresent: retained,
      workersDevEnabled: retained ? false : null,
      previewUrlsEnabled: retained ? false : null,
      customDomains: [],
      zoneRoutes: [],
      currentSecretNames: [],
      currentVersionIds: retained ? ['recovery-version'] : null,
      currentNamespaceIds: retained ? NAMESPACES : [],
      survivingRecordedNamespaceIds: retained ? NAMESPACES : [],
    },
    buckets: [
      {
        bindingName: 'PROBE_BUCKET',
        bucketName: 'bucket',
        jurisdiction: 'default',
        expectedCreationDate: CREATED,
        observedCreationDate: retained ? CREATED : null,
      },
    ],
    priorCleanup: {
      operationId: 'cleanup-operation',
      observedReceiptSha256: directCleanupReceiptDigest(receipt),
      matchesBefore: true,
    },
  });
  const context = {
    resource,
    databaseName: DATABASE_NAME,
    cleanup: receipt,
    versionIdMaximum: DIRECT_SCENARIO_ARRAY_MAXIMA.footprintVersionIds,
  };

  it('accepts the retained footprint and the residual footprint it carries forward', () => {
    const force = checkFootprint(footprint(true), {
      ...context,
      retained: true,
    });
    expect(force).toEqual(footprint(true));
    expect(
      checkFootprint(footprint(false), {
        ...context,
        retained: false,
        force,
      }),
    ).toEqual(footprint(false));
  });

  it('refuses a footprint the recovery resources and the cleanup receipt do not account for', () => {
    const retained = footprint(true);
    for (const observed of [
      { ...retained, role: 'a' },
      { ...retained, fleetRecordPresent: true },
      {
        ...retained,
        database: { ...retained.database, observedName: 'present' },
      },
      { ...retained, worker: { ...retained.worker, scriptPresent: false } },
      {
        ...retained,
        worker: { ...retained.worker, workersDevEnabled: null },
      },
      { ...retained, worker: { ...retained.worker, currentNamespaceIds: [] } },
      {
        ...retained,
        worker: { ...retained.worker, currentVersionIds: ['other-version'] },
      },
      {
        ...retained,
        worker: {
          ...retained.worker,
          currentVersionIds: [
            'recovery-version',
            ...Array.from(
              { length: DIRECT_SCENARIO_ARRAY_MAXIMA.footprintVersionIds },
              () => 'other-version',
            ),
          ],
        },
      },
      { ...retained, buckets: [] },
      {
        ...retained,
        priorCleanup: { ...retained.priorCleanup, operationId: 'other' },
      },
      {
        ...retained,
        priorCleanup: {
          ...retained.priorCleanup,
          observedReceiptSha256: 'a'.repeat(64),
        },
      },
    ])
      expect(
        refusal(() => checkFootprint(observed, { ...context, retained: true })),
      ).toBe('observation-mismatch');
    expect(
      refusal(() =>
        checkFootprint(footprint(false), {
          ...context,
          retained: false,
          force: {
            ...footprint(true),
            beforeIdentitySha256: 'c'.repeat(64),
          },
        }),
      ),
    ).toBe('observation-mismatch');
  });
});
