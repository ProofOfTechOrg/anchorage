// SPDX-License-Identifier: Apache-2.0

import { createHash, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { open, readFile, rename, unlink } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { DIRECT_INVOCATION_FAILURE_DETAILS } from './direct-credentialed-invocation.mjs';
import { DIRECT_RESIDUAL_SURFACES } from './direct-credentialed-run-state.mjs';
import { DIRECT_SCENARIO_PHASES } from './direct-credentialed-scenario-budget.mjs';

const { version: packageVersion } = createRequire(import.meta.url)(
  '../package.json',
);
export const DIRECT_EVIDENCE_LITERALS = Object.freeze([
  'Bearer ',
  'claimJson',
  'tokenJson',
  'SELECT ',
  'CREATE TABLE ',
  'INSERT INTO ',
  'X-Auth-',
  'X-Direct-',
]);
const pick = (value, keys) =>
  Object.fromEntries(keys.map((key) => [key, value[key]]));
const nullable = (value, project) => (value == null ? null : project(value));
const roles = (value, keys, project) =>
  Object.fromEntries(keys.map((key) => [key, nullable(value[key], project)]));
const suffix = (value) =>
  createHash('sha256').update(value).digest('hex').slice(-8);
const reading = (value) =>
  pick(value, [
    'state',
    'mutationEpoch',
    'requireMutationEpoch',
    'transitionRevision',
  ]);
const transition = (value) => ({
  before: reading(value.before),
  after: nullable(value.after, reading),
});
const sweep = (value) => ({
  fence: reading(value.fence),
  categoryCount: value.categories.length,
  workCount: value.categories.filter((category) => category.class === 'work')
    .length,
  standingCount: value.categories.filter(
    (category) => category.class === 'standing',
  ).length,
  emptyCount: value.categories.filter((category) => category.empty).length,
});
const settlement = (value) => pick(value, ['settledByReread']);

export function buildDirectEvidence({
  snapshot,
  invocationFailureDetail,
  prepared,
  mode,
  outcome,
  times,
  commit,
}) {
  const bootstrap = snapshot.bootstrap;
  const scenario = snapshot.scenario;
  const invocationFailure =
    snapshot.lastInvocation?.state === 'pending' &&
    DIRECT_INVOCATION_FAILURE_DETAILS.includes(invocationFailureDetail)
      ? {
          code: 'outcome-unknown',
          ordinal: snapshot.lastInvocation.ordinal,
          detail: invocationFailureDetail,
        }
      : null;
  const teardown = snapshot.teardown;
  const receipts = teardown?.receipts;
  const observation = (value) => pick(value, ['versionId', 'cpuLimitMs']);
  const traffic = (value) =>
    pick(value, ['versionId', 'cpuLimitMs', 'trafficPercentage']);
  return {
    version: 1,
    contractVersion: prepared.manifest.contractVersion,
    packageVersion,
    commit,
    mode,
    status: outcome.status,
    exitCode: outcome.exitCode,
    startedAt: snapshot.createdAt ?? null,
    finishedAt: times.finishedAt,
    resumeCount: snapshot.resumeCount ?? 0,
    accountIdSha256Suffix: suffix(snapshot.binding.accountId),
    zoneIdSha256Suffix: bootstrap ? suffix(bootstrap.context.zoneId) : null,
    resourcePrefix: snapshot.binding.resourcePrefix,
    maxInvocations: snapshot.binding.maxInvocations,
    disposableAccount: prepared.config.disposableAccount,
    configSha256: prepared.configSha256,
    referenceModuleSetSha256: prepared.referenceModuleSetSha256,
    referenceUploadBytes: prepared.referenceUploadBytes,
    commands: [
      'pnpm fleet-control:credentialed:direct -- --run',
      'pnpm fleet-control:credentialed:direct -- --resume',
    ],
    bootstrap: nullable(bootstrap, (value) => ({
      dispatch: pick(value.context.dispatch, ['kind', 'count']),
      activeVersionId: value.active?.versionId ?? null,
    })),
    scenario: nullable(scenario, (value) => ({
      phase: value.phase,
      failure: nullable(value.failure ?? invocationFailure, (failure) => ({
        code: failure.code,
        ordinal: failure.ordinal,
        detail: failure.detail ?? null,
      })),
      invocationCount: snapshot.invocationCount,
      sdkRequests: value.sdkRequests,
      attempts: pick(value.attempts, [
        'provider',
        'maintenance',
        'application',
      ]),
      phaseCalls: pick(value.phaseCalls, DIRECT_SCENARIO_PHASES),
      restart: nullable(value.proofs.restart, (restart) => ({
        lossOrdinal: restart.lossOrdinal,
        replayOrdinal: restart.replayOrdinal,
        resumedProcess: restart.resumedProcess !== null,
      })),
      initial: roles(value.proofs.initial, ['a', 'b', 'recovery'], observation),
      candidate: roles(value.proofs.candidate, ['a', 'b'], traffic),
      final: roles(value.proofs.final, ['a', 'b'], traffic),
      fence: {
        drain: roles(value.proofs.fence.drain, ['a', 'b'], transition),
        sweeps: roles(value.proofs.fence.sweeps, ['a', 'b'], (sweeps) => ({
          first: sweep(sweeps.first),
          second: nullable(sweeps.second, sweep),
          intervalMs: sweeps.intervalMs,
        })),
        reopen: roles(value.proofs.fence.reopen, ['a', 'b'], transition),
        probes: roles(value.proofs.fence.probes, ['a', 'b'], (probe) =>
          pick(probe, [
            'current',
            'missing',
            'stale',
            'future',
            'mutationEpoch',
          ]),
        ),
      },
      exports: roles(value.proofs.exports, ['a', 'b'], (proof) =>
        pick(proof, ['location', 'size', 'sha256']),
      ),
      inventories: roles(
        value.proofs.inventories,
        ['before', 'after'],
        (proof) => pick(proof, ['routeHostnames']),
      ),
      terminalForce: roles(value.proofs.terminalForce, ['a'], (proof) => ({
        ...pick(proof, ['databaseId', 'scriptName', 'ordinal']),
        attempts: pick(proof.attempts, [
          'provider',
          'maintenance',
          'application',
        ]),
      })),
    })),
    teardown: nullable(teardown, (value) => ({
      failure: value.failure,
      phase: value.phase,
      providerRequests: value.providerRequests,
      receipts: {
        ingress: nullable(value.receipts.ingress, settlement),
        worker: nullable(value.receipts.worker, (worker) => ({
          settledByReread: worker.settledByReread,
          secretNameCount: worker.secretNames.length,
        })),
        fleet: nullable(value.receipts.fleet, settlement),
        quota: nullable(value.receipts.quota, settlement),
        exports: nullable(value.receipts.exports, settlement),
        exportObjects: {
          count: value.receipts.exportObjects.length,
          settledByReread: value.receipts.exportObjects.filter(
            (receipt) => receipt.settledByReread,
          ).length,
        },
      },
      residual: nullable(value.residual, (residual) => ({
        surfaces: Object.fromEntries(
          DIRECT_RESIDUAL_SURFACES.map((key) => [
            key,
            pick(residual.surfaces[key], [
              'prefixCount',
              'globalCount',
              'exhaustive',
            ]),
          ]),
        ),
        bucketJurisdictions: [...residual.bucketJurisdictions],
        dispatch: pick(residual.dispatch, [
          'kind',
          'count',
          'status',
          'prefixCount',
        ]),
        versionsGone: residual.versionsGone,
        settleAttempts: residual.settleAttempts,
      })),
    })),
    teardownCall: outcome.teardownCall,
    retainedIdentities: {
      fleetUuid: receipts?.fleet ? null : (bootstrap?.fleet?.uuid ?? null),
      quotaUuid: receipts?.quota ? null : (bootstrap?.quota?.uuid ?? null),
      exportBucket: receipts?.exports
        ? null
        : (bootstrap?.exports?.name ?? null),
      scriptName: receipts?.worker
        ? null
        : (bootstrap?.upload?.scriptName ?? null),
      activeVersionId: receipts?.worker
        ? null
        : (bootstrap?.active?.versionId ?? null),
    },
    cost: {
      basis: 'request-counters',
      referenceProvider: scenario?.attempts.provider ?? null,
      referenceMaintenance: scenario?.attempts.maintenance ?? null,
      referenceApplication: scenario?.attempts.application ?? null,
      sdkRequests: scenario?.sdkRequests ?? null,
      referenceInvocations: snapshot.invocationCount,
      teardownProvider: teardown?.providerRequests ?? null,
      billed: null,
    },
  };
}

const identityPaths = new Set([
  'retainedIdentities.fleetUuid',
  'retainedIdentities.quotaUuid',
  'retainedIdentities.activeVersionId',
  'bootstrap.activeVersionId',
  'scenario.initial.a.versionId',
  'scenario.initial.b.versionId',
  'scenario.initial.recovery.versionId',
  'scenario.candidate.a.versionId',
  'scenario.candidate.b.versionId',
  'scenario.final.a.versionId',
  'scenario.final.b.versionId',
]);

export function inspectDirectEvidence(evidence, sentinels) {
  const serialized = `${JSON.stringify(evidence)}\n`;
  const decoded = JSON.parse(serialized);
  const entries = [
    ...sentinels.secrets
      .filter((value) => typeof value === 'string' && value.length > 0)
      .map((value) => ({ value, sentinelClass: 'env-secret' })),
    ...sentinels.literals.map((value) => ({ value, sentinelClass: 'literal' })),
  ];
  const hit = (value, keyPath) => {
    const found = entries.find((entry) => value.includes(entry.value));
    return found ? { sentinelClass: found.sentinelClass, keyPath } : null;
  };
  const walk = (value, path) => {
    if (typeof value === 'string') {
      const found = hit(value, path);
      if (found) return found;
      if (
        identityPaths.has(path) &&
        !/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u.test(value)
      )
        return { sentinelClass: 'identity-shape', keyPath: path };
    }
    if (value && typeof value === 'object') {
      for (const [key, child] of Object.entries(value)) {
        const keyPath = path ? `${path}.${key}` : key;
        const found = hit(key, keyPath) ?? walk(child, keyPath);
        if (found) return found;
      }
    }
    return null;
  };
  const found = walk(decoded, '');
  if (found) return { hit: found, serialized };
  const byteHit = hit(serialized, '');
  if (!byteHit) return { hit: null, serialized };
  for (const [key, value] of Object.entries(decoded)) {
    const member = `${JSON.stringify(key)}:${JSON.stringify(value)}`;
    const memberHit = hit(member, key);
    if (memberHit) return { hit: memberHit, serialized };
  }
  return { hit: byteHit, serialized };
}

export function scanDirectEvidence(evidence, sentinels) {
  return inspectDirectEvidence(evidence, sentinels).hit;
}

export class DirectEvidenceWriteError extends Error {
  constructor() {
    super('evidence-failed');
    this.written = true;
  }
}

export async function writeDirectEvidence({
  directory,
  evidence,
  sentinels,
  readBack = readFile,
}) {
  let file;
  let parent;
  let temporary;
  let written = false;
  let result;
  try {
    const { hit, serialized } = inspectDirectEvidence(evidence, sentinels);
    if (hit) return { written: false, ...hit };
    const bytes = Buffer.from(serialized);
    parent = await open(
      directory,
      constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW,
    );
    const path = join(directory, `.evidence-${randomUUID()}.tmp`);
    file = await open(
      path,
      constants.O_WRONLY |
        constants.O_CREAT |
        constants.O_EXCL |
        constants.O_NOFOLLOW,
      0o600,
    );
    temporary = path;
    await file.writeFile(bytes);
    await file.sync();
    if (!bytes.equals(await readBack(path))) throw new Error('evidence-failed');
    await file.close();
    file = undefined;
    await rename(path, join(directory, 'evidence.json'));
    temporary = undefined;
    written = true;
    await parent.sync();
    result = { written, failed: false };
  } catch {
    result = { written, failed: true };
  } finally {
    const cleanup = await Promise.allSettled([
      ...(file ? [file.close()] : []),
      ...(temporary ? [unlink(temporary)] : []),
      ...(parent ? [parent.close()] : []),
    ]);
    if (cleanup.some((entry) => entry.status === 'rejected'))
      result = { written, failed: true };
  }
  if (result.failed && written) throw new DirectEvidenceWriteError();
  return { written: result.written };
}
