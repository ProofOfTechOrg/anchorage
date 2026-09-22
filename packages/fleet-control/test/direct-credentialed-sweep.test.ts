// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { DirectInvocationError } from '../scripts/direct-credentialed-invocation.mjs';
import { runDirectCredentialedSweep } from '../scripts/direct-credentialed-sweep.mjs';

const attempts = Object.freeze({
  provider: 0,
  maintenance: 0,
  application: 0,
});
const digest = 'a'.repeat(64);

function harness(input?: {
  replacement?: boolean;
  records?: Record<string, Record<string, unknown> | null>;
  forceBefore?: Record<string, unknown> | null;
  forceAfter?: Record<string, unknown> | null;
  maxInvocations?: number;
  blockDecommission?: boolean;
}) {
  const records = input?.records ?? { a: null, b: null, recovery: null };
  let forceBefore = input?.forceBefore ?? null;
  let forceAfter = input?.forceAfter ?? null;
  const actions: Record<string, unknown>[] = [];
  let snapshot = {
    version: 2 as const,
    binding: {
      accountId: 'account',
      configSha256: digest,
      referenceModuleSetSha256: digest,
      resourcePrefix: 'fc000000000000000000000000',
      maxInvocations: input?.maxInvocations ?? 100,
    },
    invocationCount: 0,
    lastInvocation: null as null | {
      ordinal: number;
      requestSha256: string;
      action: Record<string, unknown>;
      state: 'settled';
    },
    bootstrap: {} as Record<string, unknown>,
    scenario: {
      failure: {
        code: 'proof-unavailable',
        ordinal: 0,
        detail: 'prepared-invocation-abandoned',
      },
      proofs: {
        terminalForce: { a: input?.replacement === false ? null : {} },
        objectDeletions: { a: null, b: null },
      },
    },
    sweep: undefined as Record<string, unknown> | undefined,
  };
  const control = () => ({
    records: ['a', 'b', 'recovery'].map((role) =>
      records[role]
        ? { role, present: true, ...records[role] }
        : { role, present: false },
    ),
    operations: [],
    forceBefore,
    forceAfter,
  });
  const journal = {
    snapshot: () => snapshot,
    recordSweep: vi.fn(async (sweep: Record<string, unknown>) => {
      snapshot = { ...snapshot, sweep: structuredClone(sweep) };
    }),
    assertSweepCapacity: vi.fn(async () => {}),
    teardownStarted: () => false,
  };
  const invocation = {
    invoke: vi.fn(async (action: Record<string, unknown>) => {
      actions.push(structuredClone(action));
      if (snapshot.invocationCount >= snapshot.binding.maxInvocations)
        throw new DirectInvocationError('invocation-budget-exhausted');
      const ordinal = snapshot.invocationCount + 1;
      snapshot = {
        ...snapshot,
        invocationCount: ordinal,
        lastInvocation: {
          ordinal,
          requestSha256: digest,
          action,
          state: 'settled',
        },
      };
      if (action.kind === 'control-read')
        return { result: control(), attempts };
      if (
        action.kind === 'tenant-probe' &&
        action.operation === 'object-delete'
      )
        return {
          result: {
            role: action.role,
            operation: action.operation,
            returned: true,
          },
          attempts,
        };
      if (action.kind === 'decommission-start') {
        if (input?.blockDecommission)
          return { result: { status: 'blocked' }, attempts };
        records[String(action.role)] = {
          phase: 'decommissioning',
          desiredSpecDigest: digest,
          pendingSpecDigest: null,
        };
        return { result: { status: 'pending' }, attempts };
      }
      if (action.kind === 'decommission-continue') {
        records[String(action.role)] = {
          phase: 'decommissioned',
          desiredSpecDigest: digest,
          pendingSpecDigest: null,
        };
        return { result: { status: 'complete' }, attempts };
      }
      if (action.kind === 'cleanup-start') {
        records[String(action.role)] = {
          phase: 'cleanup-advancing',
          desiredSpecDigest: digest,
          pendingSpecDigest: null,
        };
        return { result: { status: 'pending' }, attempts };
      }
      if (action.kind === 'cleanup-continue') {
        records[String(action.role)] = null;
        return { result: { status: 'complete' }, attempts };
      }
      if (action.kind === 'force-recovery') {
        records.recovery = null;
        forceBefore = {
          identityJson: JSON.stringify({ beforeIdentitySha256: digest }),
        };
        return {
          result: { returned: true, beforeIdentitySha256: digest },
          attempts,
        };
      }
      if (action.kind === 'force-observe') {
        forceAfter = {
          identityJson: JSON.stringify({ beforeIdentitySha256: digest }),
        };
        return {
          result: {
            observation: { beforeIdentitySha256: digest },
            provenance: { version: 1 },
          },
          attempts,
        };
      }
      if (action.kind === 'recover-force-residual')
        return {
          result: {
            returned: true,
            observation: { beforeIdentitySha256: digest },
          },
          attempts,
        };
      throw new Error(`unhandled ${String(action.kind)}`);
    }),
  };
  const prepared = {
    configSha256: digest,
    referenceModuleSetSha256: digest,
    config: {
      referenceWorker: { maxInvocations: snapshot.binding.maxInvocations },
    },
  };
  return {
    actions,
    invocation,
    journal,
    records,
    seedSettledCall() {
      snapshot = {
        ...snapshot,
        invocationCount: 1,
        lastInvocation: {
          ordinal: 1,
          requestSha256: digest,
          action: { kind: 'control-read' },
          state: 'settled',
        },
        sweep: {
          phase: 'sweeping',
          roles: { a: null, b: null, recovery: null },
          lastCall: {
            ordinal: 1,
            action: 'control-read',
            outcome: 'prepared',
            attempts: null,
          },
          failure: null,
        },
      };
    },
    run: () =>
      runDirectCredentialedSweep({
        prepared: prepared as unknown as Parameters<
          typeof runDirectCredentialedSweep
        >[0]['prepared'],
        journal: journal as unknown as Parameters<
          typeof runDirectCredentialedSweep
        >[0]['journal'],
        invocation: invocation as unknown as Parameters<
          typeof runDirectCredentialedSweep
        >[0]['invocation'],
        apiToken: 'unused',
      }),
  };
}

describe('direct abandoned-run lifecycle sweep', () => {
  it.each([
    ['prepared continuation start', 'ready'],
    ['prepared continuation lock', 'ready'],
    ['migration mid-flight', 'migrating'],
    ['migration response lost after completion', 'ready'],
  ] as const)('sweeps %s within the removal reserves', async (_name, phase) => {
    const target = harness({
      records: {
        a: {
          phase,
          desiredSpecDigest: digest,
          pendingSpecDigest: phase === 'migrating' ? digest : null,
        },
        b: null,
        recovery: {
          phase: 'ready',
          desiredSpecDigest: digest,
          pendingSpecDigest: null,
        },
      },
    });
    await expect(target.run()).resolves.toEqual({ status: 'complete' });
    expect(target.invocation.invoke).toHaveBeenCalledTimes(10);
  });

  it('deletes an original fixture object before decommission starts', async () => {
    const target = harness({
      replacement: false,
      records: {
        a: {
          phase: 'ready',
          desiredSpecDigest: digest,
          pendingSpecDigest: null,
        },
        b: null,
        recovery: null,
      },
    });
    await expect(target.run()).resolves.toEqual({ status: 'complete' });
    const kinds = target.actions.map((action) =>
      action.kind === 'tenant-probe' ? action.operation : action.kind,
    );
    expect(kinds).toContain('object-delete');
    expect(kinds.indexOf('object-delete')).toBeLessThan(
      kinds.indexOf('decommission-start'),
    );
    expect(target.journal.snapshot().sweep).toMatchObject({
      phase: 'complete',
      roles: { a: { kind: 'completed', action: 'decommission' } },
    });
  });

  it('re-drives force-owned recovery before normal roles', async () => {
    const forceEvidence = {
      identityJson: JSON.stringify({ beforeIdentitySha256: digest }),
    };
    const target = harness({
      records: {
        a: null,
        b: null,
        recovery: {
          phase: 'database-deleting',
          desiredSpecDigest: digest,
          pendingSpecDigest: null,
        },
      },
      forceBefore: forceEvidence,
    });
    await expect(target.run()).resolves.toEqual({ status: 'complete' });
    expect(target.actions.map(({ kind }) => kind)).toEqual(
      expect.arrayContaining([
        'force-recovery',
        'force-observe',
        'recover-force-residual',
      ]),
    );
    expect(
      target.actions.some(({ kind }) => kind === 'decommission-start'),
    ).toBe(false);
  });

  it.each([
    ['restart-blocked', 'unsupported-lifecycle-phase'],
    ['decommissioned', 'missing-force-evidence'],
  ] as const)('refuses recovery state %s and retains the control plane', async (phase, reason) => {
    const target = harness({
      records: {
        a: null,
        b: null,
        recovery: { phase, desiredSpecDigest: digest, pendingSpecDigest: null },
      },
    });
    await expect(target.run()).resolves.toEqual({
      status: 'refused',
      reason,
    });
    expect(target.journal.teardownStarted()).toBe(false);
    expect(target.journal.snapshot().sweep).toMatchObject({
      phase: 'refused',
      failure: { reason },
    });
  });

  it('refuses a blocked decommission answer', async () => {
    const target = harness({
      replacement: false,
      blockDecommission: true,
      records: {
        a: {
          phase: 'ready',
          desiredSpecDigest: digest,
          pendingSpecDigest: null,
        },
        b: null,
        recovery: null,
      },
    });
    await expect(target.run()).resolves.toEqual({
      status: 'refused',
      reason: 'blocked',
    });
  });

  it('re-reads state after a settled sweep invocation and continues', async () => {
    const target = harness();
    target.seedSettledCall();
    await expect(target.run()).resolves.toEqual({ status: 'complete' });
    expect(target.journal.recordSweep).toHaveBeenCalledWith(
      expect.objectContaining({
        lastCall: expect.objectContaining({
          ordinal: 1,
          action: 'control-read',
          outcome: 'returned',
        }),
      }),
    );
  });

  it('refuses closed when the invocation budget is exhausted mid-sweep', async () => {
    const target = harness({
      maxInvocations: 2,
      records: {
        a: null,
        b: null,
        recovery: {
          phase: 'ready',
          desiredSpecDigest: digest,
          pendingSpecDigest: null,
        },
      },
    });
    await expect(target.run()).resolves.toEqual({
      status: 'refused',
      reason: 'invocation-budget-exhausted',
    });
    expect(target.journal.snapshot().sweep).toMatchObject({
      phase: 'refused',
      failure: { reason: 'invocation-budget-exhausted' },
    });
  });
});
