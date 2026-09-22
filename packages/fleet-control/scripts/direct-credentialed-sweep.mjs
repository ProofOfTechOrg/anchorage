// SPDX-License-Identifier: Apache-2.0

import { DirectInvocationError } from './direct-credentialed-invocation.mjs';
import {
  DIRECT_SWEEP_BEFORE_PHASES,
  DIRECT_SWEEP_FAILURES,
  DIRECT_SWEEP_MAX_CONTINUES,
} from './direct-credentialed-reference-vocabulary.mjs';
import {
  isAbandonedDirectScenario,
  mutationPending,
} from './direct-credentialed-run-state.mjs';

const busy = new WeakSet();
const roles = Object.freeze(['recovery', 'a', 'b']);
const prepublication = new Set([
  'database-reserved',
  'database-create-authorized',
  'database-created',
  'identity-seeded',
  'migrated',
  'application-resources-create-authorized',
  'application-resources-deployed',
  'platform-resources-deployed',
  'worker-deployed',
  'maintenance-armed',
]);
const decommissioning = new Set([
  'publishing',
  'migrating',
  'ready',
  'decommission-advancing',
  'decommissioning',
  'traffic-removed',
  'credentials-revoked',
  'worker-deleted',
  'platform-credentials-revoked',
  'platform-resources-deleted',
  'application-resources-deleting',
  'application-resources-deleted',
  'database-exported',
  'database-deleting',
  'rolling-back',
]);
const forcePhases = new Set([
  'ready',
  'decommissioning',
  'traffic-removed',
  'credentials-revoked',
  'database-deleting',
  'decommissioned',
]);
const zeroAttempts = Object.freeze({
  provider: 0,
  maintenance: 0,
  application: 0,
});

class SweepRefusal extends Error {
  constructor(reason) {
    super(reason);
    this.name = 'SweepRefusal';
    this.reason = DIRECT_SWEEP_FAILURES.includes(reason)
      ? reason
      : 'unrecognized-answer';
  }
}

function refusal(reason) {
  throw new SweepRefusal(reason);
}

function callName(action) {
  return action.kind === 'tenant-probe' ? 'object-delete' : action.kind;
}

function cycleFor(role, replacement) {
  return role === 'a' && replacement ? 'reprovision' : 'original';
}

function cycleInput(cycle) {
  return cycle === 'reprovision' ? { cycle } : {};
}

function invocationReason(error) {
  if (!(error instanceof DirectInvocationError)) return 'unrecognized-answer';
  if (error.code === 'invocation-budget-exhausted')
    return 'invocation-budget-exhausted';
  if (error.code === 'outcome-unknown') return 'outcome-unknown';
  if (error.code !== 'reference-refused') return 'unrecognized-answer';
  if (error.referenceCode === 'budget-exhausted')
    return 'invocation-budget-exhausted';
  if (
    error.referenceCode === 'missing-continuation' ||
    error.referenceCode === 'missing-start' ||
    error.referenceCode === 'prerequisite-unavailable'
  )
    return 'prerequisite-unavailable';
  if (
    error.referenceCode === 'wrong-operation' ||
    error.referenceCode === 'operation-mismatch'
  )
    return 'frozen-lifecycle-mismatch';
  if (error.referenceCode === 'run-binding-mismatch')
    return 'identity-attestation-failed';
  return 'unrecognized-answer';
}

function validateControl(value) {
  if (
    !value ||
    typeof value !== 'object' ||
    !Array.isArray(value.records) ||
    !Array.isArray(value.operations)
  )
    refusal('unrecognized-answer');
  const found = value.records.map((record) => record?.role);
  if (
    found.length !== 3 ||
    !['a', 'b', 'recovery'].every((role) => found.includes(role))
  )
    refusal('corrupt-lifecycle-state');
  return value;
}

function recordFor(control, role) {
  const record = control.records.find((entry) => entry.role === role);
  if (!record || typeof record.present !== 'boolean')
    refusal('corrupt-lifecycle-state');
  if (!record.present) return null;
  if (!DIRECT_SWEEP_BEFORE_PHASES.includes(record.phase))
    refusal('unsupported-lifecycle-phase');
  return record;
}

function operationFor(control, slot) {
  return control.operations.find((entry) => entry.slot === slot);
}

function validateFrozenOperation(operation, role, cycle, record, kind) {
  if (!operation) return;
  let input;
  try {
    input = JSON.parse(operation.inputJson);
  } catch {
    refusal('frozen-lifecycle-mismatch');
  }
  const digest =
    record.phase === 'migrating'
      ? record.pendingSpecDigest
      : record.desiredSpecDigest;
  if (
    operation.kind !== kind ||
    input?.role !== role ||
    input?.cycle !== (cycle === 'reprovision' ? cycle : undefined) ||
    typeof digest !== 'string' ||
    input.specDigest !== digest
  )
    refusal('frozen-lifecycle-mismatch');
}

function maximalSweep(invocationCount) {
  const attempts = {
    provider: Number.MAX_SAFE_INTEGER,
    maintenance: Number.MAX_SAFE_INTEGER,
    application: Number.MAX_SAFE_INTEGER,
  };
  return {
    phase: 'refused',
    roles: {
      a: {
        kind: 'completed',
        cycle: 'reprovision',
        before: 'application-resources-create-authorized',
        action: 'decommission',
        after: 'decommissioned',
        ordinal: invocationCount,
      },
      b: {
        kind: 'completed',
        cycle: 'original',
        before: 'application-resources-create-authorized',
        action: 'decommission',
        after: 'decommissioned',
        ordinal: invocationCount,
      },
      recovery: {
        kind: 'refused',
        cycle: 'original',
        before: 'application-resources-create-authorized',
        action: 'cleanup',
        reason: 'incomplete-application-r2-reservation',
      },
    },
    lastCall: {
      ordinal: invocationCount,
      action: 'recover-force-residual',
      outcome: 'reference-refused',
      attempts,
    },
    failure: {
      code: 'sweep-refused',
      role: 'recovery',
      reason: 'incomplete-application-r2-reservation',
    },
  };
}

export async function runDirectCredentialedSweep(input) {
  const journal = input?.journal;
  const invocation = input?.invocation;
  let acquired = false;
  let state;
  let activeRole = 'recovery';
  let activeBefore = 'absent';
  let activeAction = 'none';
  const persist = async () => {
    await journal.recordSweep(state);
    state = structuredClone(journal.snapshot().sweep);
  };
  const fail = async (reason) => {
    const accepted = DIRECT_SWEEP_FAILURES.includes(reason)
      ? reason
      : 'unrecognized-answer';
    state.roles[activeRole] = {
      kind: 'refused',
      cycle: cycleFor(
        activeRole,
        activeRole === 'a' &&
          Boolean(journal.snapshot().scenario?.proofs.terminalForce.a),
      ),
      before: activeBefore,
      action: activeAction,
      reason: accepted,
    };
    state.phase = 'refused';
    state.failure = {
      code: 'sweep-refused',
      role: activeRole,
      reason: accepted,
    };
    await persist();
    return Object.freeze({ status: 'refused', reason: accepted });
  };
  const invoke = async (action) => {
    const snapshot = journal.snapshot();
    if (snapshot.invocationCount >= snapshot.binding.maxInvocations)
      refusal('invocation-budget-exhausted');
    const call = {
      ordinal: snapshot.invocationCount + 1,
      action: callName(action),
      outcome: 'prepared',
      attempts: null,
    };
    state.lastCall = call;
    await persist();
    let answer;
    let error;
    try {
      answer = await invocation.invoke(action);
    } catch (caught) {
      error = caught;
    }
    const after = journal.snapshot();
    if (
      after.invocationCount !== call.ordinal ||
      after.lastInvocation?.state !== 'settled'
    )
      refusal('outcome-unknown');
    const attempts = error?.attempts ?? answer?.attempts ?? zeroAttempts;
    state.lastCall = {
      ...call,
      outcome:
        error instanceof DirectInvocationError
          ? error.code
          : error
            ? 'invalid-input'
            : 'returned',
      attempts,
    };
    await persist();
    if (error) refusal(invocationReason(error));
    return answer.result;
  };
  let control;
  const sync = async () => {
    control = validateControl(await invoke({ kind: 'control-read' }));
    return control;
  };
  const complete = async (role, cycle, before, action, after) => {
    state.roles[role] = {
      kind: 'completed',
      cycle,
      before,
      action,
      after,
      ordinal: state.lastCall.ordinal,
    };
    await persist();
  };
  const none = async (role, cycle, before) => {
    state.roles[role] = { kind: 'none', cycle, before };
    await persist();
  };
  const driveLifecycle = async (role, cycle, before, action) => {
    const cleanup = action === 'cleanup';
    const slot = cleanup
      ? role === 'recovery'
        ? operationFor(control, 'cleanup-recovery-initial')
          ? 'cleanup-recovery-initial'
          : 'cleanup-recovery'
        : cycle === 'reprovision'
          ? 'cleanup-a-reprovision'
          : `cleanup-${role}`
      : cycle === 'reprovision'
        ? 'decommission-a-reprovision'
        : `decommission-${role}`;
    const frozen = operationFor(control, slot);
    if (
      recordFor(control, role)?.phase ===
        (cleanup ? 'cleanup-advancing' : 'decommission-advancing') &&
      !frozen
    )
      refusal('prerequisite-unavailable');
    validateFrozenOperation(
      frozen,
      role,
      cycle,
      recordFor(control, role),
      action,
    );
    let result = await invoke({
      kind: cleanup ? 'cleanup-start' : 'decommission-start',
      role,
      ...cycleInput(cycle),
    });
    for (let count = 0; result?.status !== 'complete'; count += 1) {
      if (result?.status === 'blocked') refusal('blocked');
      if (result?.status !== 'pending') refusal('unrecognized-answer');
      if (count >= DIRECT_SWEEP_MAX_CONTINUES) refusal('restart-blocked');
      result = await invoke({
        kind: cleanup ? 'cleanup-continue' : 'decommission-continue',
        role,
        ...cycleInput(cycle),
      });
    }
    await sync();
    const terminal = recordFor(control, role);
    if (cleanup ? terminal !== null : terminal?.phase !== 'decommissioned')
      refusal('identity-attestation-failed');
    await complete(
      role,
      cycle,
      before,
      action,
      cleanup ? 'absent' : 'decommissioned',
    );
  };
  const sweepRecovery = async () => {
    activeRole = 'recovery';
    activeAction = 'none';
    const cycle = 'original';
    let record = recordFor(control, 'recovery');
    const forceCaptured = Boolean(control.forceBefore);
    activeBefore = forceCaptured
      ? 'force-captured'
      : (record?.phase ?? 'absent');
    if (!record && !forceCaptured) {
      if (control.forceAfter) refusal('inconsistent-force-evidence');
      if (
        state.lastCall?.outcome === 'returned' &&
        ['cleanup-start', 'cleanup-continue'].includes(state.lastCall.action)
      ) {
        await complete('recovery', cycle, activeBefore, 'cleanup', 'absent');
        return;
      }
      await none('recovery', cycle, 'absent');
      return;
    }
    if (
      !forceCaptured &&
      record &&
      (record.phase === 'cleanup-advancing' || prepublication.has(record.phase))
    ) {
      activeAction = 'cleanup';
      await driveLifecycle('recovery', cycle, record.phase, 'cleanup');
      return;
    }
    if (
      (!forceCaptured && record?.phase !== 'ready') ||
      (forceCaptured && record && !forcePhases.has(record.phase))
    )
      refusal(
        record && ['database-deleting', 'decommissioned'].includes(record.phase)
          ? 'missing-force-evidence'
          : 'inconsistent-force-evidence',
      );
    activeAction = 'force';
    if (record) {
      const result = await invoke({ kind: 'force-recovery' });
      if (result?.returned !== true) refusal('unrecognized-answer');
      await sync();
      record = recordFor(control, 'recovery');
      if (record) refusal('inconsistent-force-evidence');
    }
    const observed = await invoke({ kind: 'force-observe' });
    if (
      !observed?.observation ||
      !observed?.provenance ||
      !control.forceBefore?.identityJson
    )
      refusal('missing-force-evidence');
    let beforeIdentity;
    try {
      beforeIdentity = JSON.parse(control.forceBefore.identityJson);
    } catch {
      refusal('inconsistent-force-evidence');
    }
    if (
      observed.observation.beforeIdentitySha256 !==
      beforeIdentity.beforeIdentitySha256
    )
      refusal('inconsistent-force-evidence');
    await sync();
    if (!control.forceAfter?.identityJson) refusal('missing-force-evidence');
    let forceAfter;
    try {
      forceAfter = JSON.parse(control.forceAfter.identityJson);
    } catch {
      refusal('inconsistent-force-evidence');
    }
    if (forceAfter.beforeIdentitySha256 !== beforeIdentity.beforeIdentitySha256)
      refusal('inconsistent-force-evidence');
    const recovered = await invoke({ kind: 'recover-force-residual' });
    if (recovered?.returned !== true || !recovered.observation)
      refusal('identity-attestation-failed');
    await sync();
    if (recordFor(control, 'recovery')) refusal('identity-attestation-failed');
    await complete('recovery', cycle, activeBefore, 'force', 'absent');
  };
  const sweepNormal = async (role) => {
    activeRole = role;
    activeAction = 'none';
    const replacement =
      role === 'a' &&
      Boolean(journal.snapshot().scenario.proofs.terminalForce.a);
    const cycle = cycleFor(role, replacement);
    const cycleValue = cycle === 'reprovision' ? cycle : undefined;
    const record = recordFor(control, role);
    activeBefore = record?.phase ?? 'absent';
    if (!record || record.phase === 'decommissioned') {
      if (
        record?.phase === 'decommissioned' &&
        state.lastCall?.outcome === 'returned' &&
        ['decommission-start', 'decommission-continue'].includes(
          state.lastCall.action,
        )
      ) {
        await complete(
          role,
          cycle,
          activeBefore,
          'decommission',
          'decommissioned',
        );
        return;
      }
      if (
        !record &&
        state.lastCall?.outcome === 'returned' &&
        ['cleanup-start', 'cleanup-continue'].includes(state.lastCall.action)
      ) {
        await complete(role, cycle, activeBefore, 'cleanup', 'absent');
        return;
      }
      await none(role, cycle, activeBefore);
      return;
    }
    if (
      record.phase === 'cleanup-advancing' ||
      prepublication.has(record.phase)
    ) {
      activeAction = 'cleanup';
      await driveLifecycle(role, cycle, activeBefore, 'cleanup');
      return;
    }
    if (!decommissioning.has(record.phase))
      refusal('unsupported-lifecycle-phase');
    activeAction = 'decommission';
    // The scenario fixture object must be removed while tenant ingress still
    // exists. The pre-route ignores the execution fence, and the scenario is
    // never resumed after abandonment, so this record stays frozen thereafter.
    if (
      cycle === 'original' &&
      journal.snapshot().scenario.proofs.objectDeletions[role] === null &&
      ['publishing', 'ready', 'migrating', 'rolling-back'].includes(
        record.phase,
      )
    ) {
      const deleted = await invoke({
        kind: 'tenant-probe',
        role,
        operation: 'object-delete',
      });
      if (
        deleted?.role !== role ||
        deleted?.operation !== 'object-delete' ||
        deleted?.returned !== true
      )
        refusal('identity-attestation-failed');
      await sync();
    }
    if (
      cycle === 'original' &&
      journal.snapshot().scenario.proofs.objectDeletions[role] === null &&
      !['publishing', 'ready', 'migrating', 'rolling-back'].includes(
        record.phase,
      )
    )
      refusal('prerequisite-unavailable');
    await driveLifecycle(
      role,
      cycleValue ?? 'original',
      activeBefore,
      'decommission',
    );
  };

  try {
    if (
      !journal ||
      typeof journal.recordSweep !== 'function' ||
      typeof journal.assertSweepCapacity !== 'function' ||
      typeof journal.teardownStarted !== 'function' ||
      !invocation ||
      typeof invocation.invoke !== 'function' ||
      busy.has(journal)
    )
      return Object.freeze({ status: 'refused', reason: 'bootstrap-refused' });
    busy.add(journal);
    acquired = true;
    const snapshot = journal.snapshot();
    if (
      mutationPending(snapshot) ||
      journal.teardownStarted() ||
      !isAbandonedDirectScenario(snapshot.scenario) ||
      snapshot.binding.configSha256 !== input.prepared?.configSha256 ||
      snapshot.binding.referenceModuleSetSha256 !==
        input.prepared?.referenceModuleSetSha256 ||
      snapshot.binding.maxInvocations !==
        input.prepared?.config?.referenceWorker?.maxInvocations
    )
      return Object.freeze({
        status: 'refused',
        reason: 'prerequisite-unavailable',
      });
    state = structuredClone(
      snapshot.sweep ?? {
        phase: 'sweeping',
        roles: { a: null, b: null, recovery: null },
        lastCall: null,
        failure: null,
      },
    );
    if (state.phase !== 'sweeping')
      return Object.freeze({
        status: state.phase === 'complete' ? 'complete' : 'refused',
        ...(state.failure ? { reason: state.failure.reason } : {}),
      });
    if (state.lastCall?.outcome === 'prepared') {
      const last = journal.snapshot().lastInvocation;
      if (last?.state !== 'settled' || last.ordinal !== state.lastCall.ordinal)
        refusal('outcome-unknown');
      state.lastCall = {
        ...state.lastCall,
        outcome: 'returned',
        attempts: zeroAttempts,
      };
      await persist();
    }
    await journal.assertSweepCapacity(maximalSweep(snapshot.invocationCount));
    await sync();
    for (const role of roles) {
      if (state.roles[role] !== null) continue;
      if (role === 'recovery') await sweepRecovery();
      else await sweepNormal(role);
    }
    state.phase = 'complete';
    state.failure = null;
    await persist();
    return Object.freeze({ status: 'complete' });
  } catch (error) {
    if (!state || !journal || typeof journal.recordSweep !== 'function')
      return Object.freeze({ status: 'refused', reason: 'bootstrap-refused' });
    try {
      return await fail(
        error instanceof SweepRefusal ? error.reason : 'unrecognized-answer',
      );
    } catch {
      return Object.freeze({
        status: 'refused',
        reason:
          error instanceof SweepRefusal ? error.reason : 'unrecognized-answer',
      });
    }
  } finally {
    if (acquired) busy.delete(journal);
  }
}
