// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import {
  DIRECT_SCENARIO_INVOCATION_BUDGET,
  DIRECT_SCENARIO_PHASES,
} from './direct-credentialed-scenario-budget.mjs';

export const NORMAL_ROLES = Object.freeze(['a', 'b']);
export const SCENARIO_ROLES = Object.freeze([...NORMAL_ROLES, 'recovery']);

export function requireFact(condition, code = 'observation-mismatch', detail) {
  if (!condition)
    throw Object.assign(
      new Error(code),
      { code },
      detail === undefined ? {} : { detail },
    );
}

export function equal(actual, expected) {
  requireFact(isDeepStrictEqual(actual, expected));
}

export function parse(value) {
  requireFact(typeof value === 'string');
  return JSON.parse(value);
}

export const hash = (value) => createHash('sha256').update(value).digest('hex');

export const jsonHash = (value) => hash(JSON.stringify(value));

export const zeroAttempts = () => ({
  provider: 0,
  maintenance: 0,
  application: 0,
});

export function phaseInvocationReserve(phase) {
  const index = DIRECT_SCENARIO_PHASES.indexOf(phase);
  requireFact(index >= 0, 'invalid-input');
  return DIRECT_SCENARIO_PHASES.slice(index).reduce(
    (total, entry) => total + DIRECT_SCENARIO_INVOCATION_BUDGET[entry].reserve,
    0,
  );
}

export function checkInvocationHeadroom(phase, phaseCalls, remaining) {
  requireFact(remaining > 0, 'invocation-budget-exhausted');
  const budget = DIRECT_SCENARIO_INVOCATION_BUDGET[phase];
  requireFact(budget, 'invalid-input');
  const spent = phaseCalls[phase];
  requireFact(spent < budget.ceiling, 'budget-exhausted', 'phase-ceiling');
  requireFact(
    remaining >= phaseInvocationReserve(phase) - spent,
    'budget-exhausted',
    'run-reserve',
  );
}

export function changedBy(action) {
  if (!action) return { slots: [], roles: [] };
  const { kind, role, slot, release } = action;
  if (kind === 'provision')
    return {
      slots: [
        role === 'recovery' && release === 'initial'
          ? 'cleanup-recovery-initial'
          : `cleanup-${role}`,
      ],
      roles: [role],
    };
  if (kind.startsWith('inventory-')) return { slots: [slot], roles: [] };
  if (kind.startsWith('audit-')) return { slots: [slot], roles: [] };
  if (kind.startsWith('migration-'))
    return { slots: ['migration-next'], roles: [...NORMAL_ROLES] };
  if (kind.startsWith('cleanup-'))
    return { slots: [`cleanup-${role}`], roles: [role] };
  if (kind.startsWith('decommission-'))
    return { slots: [`decommission-${role}`], roles: [role] };
  if (kind === 'force-recovery' || kind === 'recover-force-residual')
    return { slots: [], roles: ['recovery'] };
  return { slots: [], roles: [] };
}

export function recordFacts(record) {
  return {
    role: record.role,
    present: record.present,
    phase: record.phase ?? null,
    desiredSpecDigest: record.desiredSpecDigest ?? null,
    pendingSpecDigest: record.pendingSpecDigest ?? null,
    artifactVersion: record.artifactVersion ?? null,
    pendingArtifactVersion: record.pendingArtifactVersion ?? null,
    databaseId: record.databaseId ?? null,
  };
}

export function operationFacts(operation) {
  return {
    slot: operation.slot,
    operationId: operation.operationId,
    inputSha256: hash(operation.inputJson),
    tokenRevision: operation.tokenRevision,
  };
}

export function expectedVersion(record, release, candidate = false) {
  requireFact(record?.present);
  return {
    role: record.role,
    versionId: candidate
      ? record.pendingArtifactVersion
      : record.artifactVersion,
    databaseId: record.databaseId,
    specDigest: candidate ? record.pendingSpecDigest : record.desiredSpecDigest,
    applicationRelease: release,
  };
}

export function checkItemConvergence(items) {
  if (items[1].status !== 'pending') equal(items[0].status, 'complete');
}

export function checkTrafficDistribution(candidate, previous) {
  equal(candidate.trafficPercentage, 0);
  equal(candidate.currentDeployment.activeVersionId, previous.versionId);
  equal(
    new Map(
      candidate.currentDeployment.versions.map((entry) => [
        entry.versionId,
        entry.percentage,
      ]),
    ),
    new Map([
      [previous.versionId, 100],
      [candidate.versionId, 0],
    ]),
  );
}

export function migrationStartSettled(control, mutation) {
  const started = control.operations.find(
    (entry) => entry.slot === 'migration-next',
  );
  if (!started) return false;
  requireFact(
    started.operationId &&
      mutation?.outcome === 'returned' &&
      mutation.action.kind === 'migration-start',
    'proof-unavailable',
  );
  return true;
}

export function migrationInterruptSettled(control, mutation) {
  if (!control.interruption) return false;
  requireFact(
    mutation?.outcome === 'injected-response-loss' &&
      mutation.action.kind === 'migration-continue',
    'proof-unavailable',
  );
  return true;
}
