// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { describe, expect, it } from 'vitest';
import type { DirectWorkerVersionObservation } from '../scripts/direct-credentialed-observations.mjs';
import {
  DIRECT_SCENARIO_INVOCATION_BUDGET,
  DIRECT_SCENARIO_MIN_INVOCATIONS,
  DIRECT_SCENARIO_PHASES,
} from '../scripts/direct-credentialed-scenario-budget.mjs';
import {
  changedBy,
  checkInvocationHeadroom,
  checkItemConvergence,
  checkTrafficDistribution,
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
  it('allows no record change during an audit action', () => {
    expect(changedBy({ kind: 'audit-start', slot: 'audit-before' })).toEqual({
      slots: ['audit-before'],
      roles: [],
    });
    expect(
      changedBy({ kind: 'audit-page', slot: 'audit-after', limit: 32 }),
    ).toEqual({ slots: ['audit-after'], roles: [] });
  });

  it('maps every other action to the slot and roles it may change', () => {
    expect(changedBy(null)).toEqual({ slots: [], roles: [] });
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
