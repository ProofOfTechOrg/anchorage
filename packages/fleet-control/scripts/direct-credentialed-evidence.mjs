// SPDX-License-Identifier: Apache-2.0

import { createHash, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { open, readdir, readFile, rename, unlink } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { DIRECT_INVOCATION_FAILURE_DETAILS } from './direct-credentialed-invocation.mjs';
import { DIRECT_RESIDUAL_SURFACES } from './direct-credentialed-reference-vocabulary.mjs';
import { assertPrivate, fileFlags } from './direct-credentialed-run-state.mjs';
import { DIRECT_SCENARIO_PHASES } from './direct-credentialed-scenario-budget.mjs';
import { survivingIdentities } from './direct-credentialed-teardown.mjs';

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
/** The commands the artifact publishes and the runtime names in a restart summary. */
export const DIRECT_CONFORMANCE_COMMANDS = Object.freeze({
  run: 'pnpm fleet-control:credentialed:direct -- --run',
  resume: 'pnpm fleet-control:credentialed:direct -- --resume',
});
/**
 * Every key name the projection writes. The scan compares each key against the
 * run's credentials, so a credential one of these contains ends the run at
 * publication; live-mode admission refuses such a credential instead.
 * `test/direct-credentialed-evidence.test.ts` walks a maximal artifact against
 * this list, so a projected key absent here is a red test rather than a gap.
 * Array elements carry an index in place of a name, which admission answers
 * with its digits-only rule rather than with a member here.
 */
export const DIRECT_EVIDENCE_KEYS = Object.freeze([
  'a',
  'accountIdSha256Suffix',
  'activeVersionId',
  'after',
  'application',
  'attempts',
  'b',
  'basis',
  'before',
  'billed',
  'bootstrap',
  'bucketJurisdictions',
  'candidate',
  'categoryCount',
  'code',
  'commands',
  'commit',
  'configSha256',
  'contractVersion',
  'cost',
  'count',
  'cpuLimitMs',
  'current',
  'databaseId',
  'detail',
  'disposableAccount',
  'dispatch',
  'drain',
  'emptyCount',
  'exhaustive',
  'exitCode',
  'exportBucket',
  'exportObjects',
  'exports',
  'failure',
  'fence',
  'final',
  'finishedAt',
  'first',
  'fleet',
  'fleetUuid',
  'future',
  'globalCount',
  'ingress',
  'initial',
  'intervalMs',
  'inventories',
  'invocationCount',
  'kind',
  'location',
  'lossOrdinal',
  'maintenance',
  'maxInvocations',
  'missing',
  'mode',
  'mutationEpoch',
  'ordinal',
  'packageVersion',
  'phase',
  'phaseCalls',
  'prefixCount',
  'probes',
  'provider',
  'providerRequests',
  'quota',
  'quotaUuid',
  'receipts',
  'recovery',
  'referenceApplication',
  'referenceInvocations',
  'referenceMaintenance',
  'referenceModuleSetSha256',
  'referenceProvider',
  'referenceUploadBytes',
  'reopen',
  'replayOrdinal',
  'requireMutationEpoch',
  'residual',
  'resourcePrefix',
  'restart',
  'resumeCount',
  'resumedProcess',
  'retainedIdentities',
  'routeHostnames',
  'scenario',
  'scriptName',
  'sdkRequests',
  'second',
  'secretNameCount',
  'settleAttempts',
  'settledByReread',
  'sha256',
  'size',
  'stale',
  'standingCount',
  'startedAt',
  'state',
  'status',
  'surfaces',
  'sweeps',
  'teardown',
  'teardownCall',
  'teardownProvider',
  'terminalForce',
  'trafficPercentage',
  'transitionRevision',
  'version',
  'versionId',
  'versionsGone',
  'workCount',
  'worker',
  'zoneIdSha256Suffix',
  ...DIRECT_SCENARIO_PHASES,
  ...DIRECT_RESIDUAL_SURFACES,
]);
// The transports a reference invocation can take, and therefore the counters
// every attempt record carries.
const TRANSPORTS = ['provider', 'maintenance', 'application'];
// The staging name a publication writes under, before the rename that makes it
// `evidence.json`.
const TEMPORARY_PREFIX = '.evidence-';
const TEMPORARY_SUFFIX = '.tmp';
const pick = (value, keys) =>
  Object.fromEntries(keys.map((key) => [key, value[key]]));
const nullable = (value, project) => (value == null ? null : project(value));
// Keyed projection over a fixed member set: roles for most proofs, and a
// before/after pair for the inventories.
const members = (value, keys, project) =>
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
  const scenarioSection = nullable(scenario, (value) => ({
    phase: value.phase,
    failure: nullable(value.failure ?? invocationFailure, (failure) => ({
      code: failure.code,
      ordinal: failure.ordinal,
      detail: failure.detail ?? null,
    })),
    invocationCount: snapshot.invocationCount,
    sdkRequests: value.sdkRequests,
    attempts: pick(value.attempts, TRANSPORTS),
    phaseCalls: pick(value.phaseCalls, DIRECT_SCENARIO_PHASES),
    restart: nullable(value.proofs.restart, (restart) => ({
      lossOrdinal: restart.lossOrdinal,
      replayOrdinal: restart.replayOrdinal,
      resumedProcess: restart.resumedProcess !== null,
    })),
    initial: members(value.proofs.initial, ['a', 'b', 'recovery'], observation),
    candidate: members(value.proofs.candidate, ['a', 'b'], traffic),
    final: members(value.proofs.final, ['a', 'b'], traffic),
    fence: {
      drain: members(value.proofs.fence.drain, ['a', 'b'], transition),
      sweeps: members(value.proofs.fence.sweeps, ['a', 'b'], (sweeps) => ({
        first: sweep(sweeps.first),
        second: nullable(sweeps.second, sweep),
        intervalMs: sweeps.intervalMs,
      })),
      reopen: members(value.proofs.fence.reopen, ['a', 'b'], transition),
      probes: members(value.proofs.fence.probes, ['a', 'b'], (probe) =>
        pick(probe, ['current', 'missing', 'stale', 'future', 'mutationEpoch']),
      ),
    },
    exports: members(value.proofs.exports, ['a', 'b'], (proof) =>
      pick(proof, ['location', 'size', 'sha256']),
    ),
    inventories: members(
      value.proofs.inventories,
      ['before', 'after'],
      (proof) => pick(proof, ['routeHostnames']),
    ),
    terminalForce: members(value.proofs.terminalForce, ['a'], (proof) => ({
      ...pick(proof, ['databaseId', 'scriptName', 'ordinal']),
      attempts: pick(proof.attempts, TRANSPORTS),
    })),
  }));
  const teardownSection = nullable(teardown, (value) => ({
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
  }));
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
      DIRECT_CONFORMANCE_COMMANDS.run,
      DIRECT_CONFORMANCE_COMMANDS.resume,
    ],
    bootstrap: nullable(bootstrap, (value) => ({
      dispatch: pick(value.context.dispatch, ['kind', 'count']),
      activeVersionId: value.active?.versionId ?? null,
    })),
    scenario: scenarioSection,
    teardown: teardownSection,
    teardownCall: outcome.teardownCall,
    // The survival rule is teardown's: a receipt for a resource means the
    // resource is gone, so the identity it had is no longer retained.
    retainedIdentities: survivingIdentities(bootstrap, receipts ?? {}),
    cost: {
      basis: 'request-counters',
      referenceProvider: scenarioSection?.attempts.provider ?? null,
      referenceMaintenance: scenarioSection?.attempts.maintenance ?? null,
      referenceApplication: scenarioSection?.attempts.application ?? null,
      sdkRequests: scenarioSection?.sdkRequests ?? null,
      // Read from the snapshot rather than from the scenario section, which
      // is the one counter that survives a run with no scenario at all.
      referenceInvocations: snapshot.invocationCount,
      teardownProvider: teardownSection?.providerRequests ?? null,
      billed: null,
    },
  };
}

/**
 * The dotted paths of the projected provider strings the journal decodes with
 * `identifier` — any trimmed, control-free string up to 128 characters — which
 * is wider than the identity shape below, so these are the paths at which a
 * decoded journal can still carry a value the evidence boundary must refuse.
 * Two rules keep a path out of the list: a value the journal already bounds to
 * the scenario charset (`run-state.mjs`'s `scenarioId`, whose accepted set the
 * identity shape contains) reaches this boundary narrower than the guard, and a
 * prefix-derived name such as `retainedIdentities.exportBucket` or
 * `retainedIdentities.scriptName` is the run's own, not the provider's.
 * `test/direct-credentialed-evidence.test.ts` resolves every member against a
 * maximal artifact, so a renamed or moved projection key is a red test rather
 * than a guard that silently stops matching.
 */
export const DIRECT_EVIDENCE_IDENTITY_PATHS = Object.freeze([
  'retainedIdentities.fleetUuid',
  'retainedIdentities.quotaUuid',
  'retainedIdentities.activeVersionId',
  'bootstrap.activeVersionId',
]);
const identityPaths = new Set(DIRECT_EVIDENCE_IDENTITY_PATHS);

/**
 * `hit.keyPath` is diagnostic provenance, not withheld output: an object key
 * that is itself a credential becomes part of the path, so a caller that
 * prints a hit scans the rendered line against the same sentinels first. The
 * CLI's own line guard is what makes its refusal output safe.
 */
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
        return { refusalClass: 'identity-shape', keyPath: path };
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
  // No member span carries it, so the sentinel is completed by the
  // serialization's own structure — a separator, a brace, the trailing
  // newline. The empty path names that: the hit belongs to the document, not
  // to any member of it.
  return { hit: byteHit, serialized };
}

/** Raised only after the artifact has replaced its predecessor, so the class is the publication proof. */
export class DirectEvidenceWriteError extends Error {
  constructor() {
    super('evidence-failed');
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
  let failed = false;
  try {
    const { hit, serialized } = inspectDirectEvidence(evidence, sentinels);
    if (hit) return { written: false, ...hit };
    const bytes = Buffer.from(serialized);
    parent = await open(
      directory,
      fileFlags(constants.O_RDONLY | constants.O_DIRECTORY),
    );
    assertPrivate(await parent.stat(), true);
    // A signal between the create below and the rename leaves the temporary
    // file behind. The run holds the directory's lock, so any sibling left
    // there is a dead one from an interrupted publication, and it is swept
    // before a new one is created.
    for (const name of await readdir(directory))
      if (name.startsWith(TEMPORARY_PREFIX) && name.endsWith(TEMPORARY_SUFFIX))
        await unlink(join(directory, name));
    const path = join(
      directory,
      `${TEMPORARY_PREFIX}${randomUUID()}${TEMPORARY_SUFFIX}`,
    );
    file = await open(
      path,
      fileFlags(constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL),
      0o600,
    );
    temporary = path;
    assertPrivate(await file.stat(), false);
    await file.writeFile(bytes);
    await file.sync();
    if (!bytes.equals(await readBack(path))) throw new Error('evidence-failed');
    await file.close();
    file = undefined;
    await rename(path, join(directory, 'evidence.json'));
    temporary = undefined;
    written = true;
    await parent.sync();
  } catch {
    failed = true;
  } finally {
    const cleanup = await Promise.allSettled([
      ...(file ? [file.close()] : []),
      ...(temporary ? [unlink(temporary)] : []),
      ...(parent ? [parent.close()] : []),
    ]);
    if (cleanup.some((entry) => entry.status === 'rejected')) failed = true;
  }
  if (failed && written) throw new DirectEvidenceWriteError();
  return { written };
}
