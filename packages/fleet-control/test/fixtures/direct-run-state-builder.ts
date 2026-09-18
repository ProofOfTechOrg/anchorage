// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { preflightDirectConformance } from '../../scripts/direct-credentialed-conformance-preflight.mjs';
import {
  DIRECT_RESIDUAL_SURFACES,
  DIRECT_TEARDOWN_MAXIMA,
} from '../../scripts/direct-credentialed-reference-vocabulary.mjs';
import {
  DIRECT_SCENARIO_ARRAY_MAXIMA,
  DIRECT_SCENARIO_OPERATION_SLOTS,
  type DirectBootstrapContext,
  type DirectBootstrapMutationReceipt,
  type DirectResidualObservation,
  type DirectRunJournal,
  type DirectTeardownState,
  openDirectRunState,
} from '../../scripts/direct-credentialed-run-state.mjs';
import type { DirectScenarioState } from '../../scripts/direct-credentialed-scenario.mjs';
import { DIRECT_SCENARIO_PHASES } from '../../scripts/direct-credentialed-scenario-budget.mjs';

export const directories: string[] = [];
export const journals = new Set<DirectRunJournal>();
export const hash = (value: string) =>
  createHash('sha256').update(value).digest('hex');
export type Mutable<T> = T extends readonly (infer Entry)[]
  ? Mutable<Entry>[]
  : T extends object
    ? { -readonly [Key in keyof T]: Mutable<T[Key]> }
    : T;
export type MutableScenario = Mutable<DirectScenarioState>;

export function first<Entry>(entries: readonly Entry[]): Entry {
  const [entry] = entries;
  if (entry === undefined) throw new Error('empty scenario fixture array');
  return entry;
}

export function present<Value>(value: Value | null | undefined): Value {
  if (value === null || value === undefined)
    throw new Error('absent scenario fixture value');
  return value;
}

export async function fixture(limit = 3, disposableAccount = true) {
  const directory = await mkdtemp(join(tmpdir(), 'direct-run-state-'));
  directories.push(directory);
  const config = JSON.parse(
    await readFile(
      new URL(
        '../../scripts/direct-credentialed-conformance.example.json',
        import.meta.url,
      ),
      'utf8',
    ),
  );
  config.disposableAccount = disposableAccount;
  const reference =
    "import manifest from './direct-run-manifest.js'; export default {fetch(){return Response.json(manifest.contractVersion)}};";
  const tenant =
    'export class Maintenance {} export class Runner {} export default {};';
  config.referenceWorker.artifact = {
    bundle: './reference.mjs',
    mainModule: 'worker.js',
    sha256: hash(reference),
  };
  config.referenceWorker.maxInvocations = limit;
  config.deployment.artifact = {
    bundle: './tenant.mjs',
    mainModule: 'worker.js',
    sha256: hash(tenant),
  };
  const configPath = join(directory, 'config.json');
  await writeFile(configPath, JSON.stringify(config));
  await writeFile(join(directory, 'reference.mjs'), reference);
  await writeFile(join(directory, 'tenant.mjs'), tenant);
  const prepared = await preflightDirectConformance({
    configPath,
    now: Date.parse('2026-09-10T12:00:00Z'),
  });
  const base = join(directory, '.direct-conformance');
  const runDirectory = join(base, config.resourcePrefix);
  const lockPath = join(base, `${config.resourcePrefix}.lock`);
  const input = { configPath, prepared, accountId: 'account' };
  const request = (action: unknown = { kind: 'control-read' }) =>
    JSON.stringify({
      contractVersion: 1,
      configSha256: prepared.configSha256,
      action,
    });
  return {
    directory,
    configPath,
    prepared,
    base,
    runDirectory,
    lockPath,
    input,
    request,
  };
}

export async function opened(input: Parameters<typeof openDirectRunState>[0]) {
  const journal = await openDirectRunState(input);
  journals.add(journal);
  return journal;
}

export async function closed(journal: DirectRunJournal) {
  await journal.close();
  journals.delete(journal);
}

export async function cleanupDirectRunState() {
  try {
    await Promise.all([...journals].map((journal) => journal.close()));
  } finally {
    journals.clear();
    await Promise.all(
      directories
        .splice(0)
        .map((directory) => rm(directory, { recursive: true, force: true })),
    );
  }
}

export function bootstrapContext(
  f: Awaited<ReturnType<typeof fixture>>,
): DirectBootstrapContext {
  return {
    names: f.prepared.names,
    zoneId: 'zone',
    zoneName: 'example.test',
    accountWorkersDevSubdomain: 'attested-account',
    dispatch: { kind: 'empty', count: 0 },
  };
}

export function receipts(f: Awaited<ReturnType<typeof fixture>>) {
  return [
    {
      kind: 'create-fleet-d1',
      receipt: { uuid: 'fleet-uuid', name: f.prepared.names.fleetDatabase },
    },
    {
      kind: 'create-quota-d1',
      receipt: { uuid: 'quota-uuid', name: f.prepared.names.quotaDatabase },
    },
    {
      kind: 'create-export-r2',
      receipt: {
        name: f.prepared.names.exportBucket,
        jurisdiction: 'default',
        creationDate: '2026-09-10T00:00:00.000Z',
      },
    },
    {
      kind: 'upload-reference',
      receipt: {
        scriptName: f.prepared.names.referenceWorker,
        tag: null,
        etag: null,
      },
    },
    {
      kind: 'enable-reference-ingress',
      receipt: { enabled: true, previewsEnabled: false },
    },
  ] as const satisfies readonly DirectBootstrapMutationReceipt[];
}

export async function confirmedBootstrap(
  f: Awaited<ReturnType<typeof fixture>>,
  journal: DirectRunJournal,
) {
  await journal.bindBootstrapContext(bootstrapContext(f));
  for (const value of receipts(f)) {
    if (value.kind === 'enable-reference-ingress')
      await journal.recordBootstrapObservation({
        kind: 'active',
        deploymentId: 'deployment',
        versionId: 'version',
      });
    await journal.beginBootstrapMutation(value.kind);
    await journal.confirmBootstrapMutation(value);
  }
}

export const MAX_ID = 'z'.repeat(128);
export const MAX_COUNT = Number.MAX_SAFE_INTEGER;
export const DIGEST = 'a'.repeat(64);
export const LOCATION = `r2://${'p'.repeat(700)}`;
export const DATE = '2026-09-10T00:00:00.000Z';
export const PROCESS = {
  pid: MAX_COUNT,
  startTicks: '1000',
  bootId: MAX_ID,
};
export const RESUMED = {
  pid: MAX_COUNT - 1,
  startTicks: '2000',
  bootId: MAX_ID,
};

export function workerVersion(
  role: 'a' | 'b' | 'recovery',
  applicationRelease: '1' | '2',
  trafficPercentage: number,
) {
  return {
    role,
    versionId: MAX_ID,
    databaseId: MAX_ID,
    specDigest: DIGEST,
    applicationRelease,
    accountId: MAX_ID,
    tenantTag: MAX_ID,
    environment: MAX_ID,
    scriptName: MAX_ID,
    currentDeployment: {
      deploymentId: MAX_ID,
      activeVersionId: MAX_ID,
      versions: [
        { versionId: MAX_ID, percentage: 100 },
        { versionId: MAX_ID, percentage: 0 },
      ],
    },
    trafficPercentage,
    cpuLimitMs: MAX_COUNT,
    subrequestLimit: MAX_COUNT,
    schemaVersion: 2,
    namespaces: [
      {
        binding: 'MAINTENANCE' as const,
        className: 'Maintenance' as const,
        namespaceId: MAX_ID,
      },
      {
        binding: 'RUNNER' as const,
        className: 'Runner' as const,
        namespaceId: MAX_ID,
      },
    ],
    bucket: {
      name: MAX_ID,
      jurisdiction: 'default' as const,
      creationDate: DATE,
    },
  };
}

export function exportProof(role: 'a' | 'b') {
  return {
    verified: true as const,
    role,
    receipt: {
      version: 1 as const,
      authority: LOCATION,
      databaseId: MAX_ID,
      operationId: MAX_ID,
    },
    location: LOCATION,
    size: MAX_COUNT,
    sha256: DIGEST,
    sourceInvocationOrdinal: 1,
  };
}

export function footprint() {
  return {
    version: 1 as const,
    role: 'recovery' as const,
    beforeIdentitySha256: DIGEST,
    fleetRecordPresent: false as const,
    deploymentClaimsPresent: false as const,
    database: { id: MAX_ID, expectedName: MAX_ID, observedName: null },
    worker: {
      scriptName: MAX_ID,
      scriptPresent: true,
      workersDevEnabled: false as const,
      previewUrlsEnabled: false as const,
      customDomains: [],
      zoneRoutes: [],
      currentSecretNames: [],
      currentVersionIds: Array.from({ length: 8 }, () => MAX_ID),
      currentNamespaceIds: [MAX_ID, MAX_ID],
      survivingRecordedNamespaceIds: [MAX_ID, MAX_ID],
    },
    buckets: [
      {
        bindingName: 'PROBE_BUCKET' as const,
        bucketName: MAX_ID,
        jurisdiction: 'default' as const,
        expectedCreationDate: DATE,
        observedCreationDate: DATE,
      },
    ],
    priorCleanup: {
      operationId: MAX_ID,
      observedReceiptSha256: DIGEST,
      matchesBefore: true as const,
    },
  };
}

export function inventoryProof() {
  return {
    operationId: MAX_ID,
    generation: MAX_COUNT,
    calls: MAX_COUNT,
    databaseIds: [MAX_ID, MAX_ID],
    namespaceIds: Array.from({ length: 4 }, () => MAX_ID),
    scriptNames: [MAX_ID, MAX_ID],
    routeHostnames: Array.from({ length: 2 }, () =>
      [63, 63, 63, 61].map((length) => 'a'.repeat(length)).join('.'),
    ),
    bucketNames: [MAX_ID, MAX_ID],
    findings: Array.from({ length: 32 }, () => ({
      kind: MAX_ID,
      detailSha256: DIGEST,
    })),
  };
}

export function auditProof() {
  return {
    operationId: MAX_ID,
    generation: MAX_COUNT,
    recordCount: 2 as const,
    findingCount: 16,
    finalizedAtMs: MAX_COUNT,
    findings: Array.from({ length: 16 }, () => ({
      tenantTag: MAX_ID,
      environment: MAX_ID,
      kind: MAX_ID,
      detailSha256: DIGEST,
    })),
  };
}

/**
 * The largest scenario the journal admits. `callKind` selects the kind of the
 * settled call it carries in `lastCall` and `mutation`; the default is the
 * larger of the two, so a caller that does not choose still bounds the journal.
 */
export function maximalScenario(
  callKind: 'audit-page' | 'force-terminal' = 'force-terminal',
): MutableScenario {
  const fenceReading = () => ({
    state: 'migration-locked' as const,
    mutationEpoch: MAX_COUNT,
    requireMutationEpoch: false,
    transitionRevision: MAX_COUNT,
  });
  const fenceTransition = () => ({
    before: fenceReading(),
    after: fenceReading(),
    ordinal: 3,
  });
  const fenceSweep = () => ({
    fence: fenceReading(),
    categories: Array.from(
      { length: DIRECT_SCENARIO_ARRAY_MAXIMA.inventoryCategories },
      () => ({ category: MAX_ID, class: 'standing' as const, empty: false }),
    ),
    observedAt: MAX_COUNT,
    ordinal: 3,
  });
  const fenceSweeps = () => ({
    first: fenceSweep(),
    second: fenceSweep(),
    intervalMs: MAX_COUNT,
  });
  const fenceProbes = () => ({
    current: 'accepted' as const,
    missing: 'missing' as const,
    stale: 'stale' as const,
    future: 'future' as const,
    mutationEpoch: MAX_COUNT,
    ordinal: 3,
  });
  const phaseCalls = Object.fromEntries(
    DIRECT_SCENARIO_PHASES.map((phase) => [phase, 0]),
  ) as MutableScenario['phaseCalls'];
  phaseCalls['provision-a'] = 3;
  const settled = {
    ordinal: 3,
    outcome: 'returned' as const,
    attempts: {
      provider: MAX_COUNT,
      maintenance: MAX_COUNT,
      application: MAX_COUNT,
    },
  };
  // Each kind carries the largest call of its own shape, built here and
  // nowhere else: `force-terminal` is the larger of the two, which is why it
  // is the default the journal-capacity fixtures take.
  const call =
    callKind === 'force-terminal'
      ? {
          ...settled,
          action: { kind: 'force-terminal' as const, role: 'a' as const },
          migration: null,
          before: { databaseId: MAX_ID, scriptName: MAX_ID },
        }
      : {
          ...settled,
          action: {
            kind: 'audit-page' as const,
            slot: 'audit-after' as const,
            limit: 32,
            afterOrdinal: 1,
          },
          migration: {
            itemOrdinal: 0 as const,
            cursor: MAX_COUNT,
            step: MAX_ID,
            itemsSha256: DIGEST,
          },
        };
  return {
    version: 1,
    phase: 'provision-a',
    startedOrdinal: 0,
    callCount: 3,
    phaseCalls,
    attempts: {
      provider: MAX_COUNT,
      maintenance: MAX_COUNT,
      application: MAX_COUNT,
    },
    sdkRequests: MAX_COUNT,
    inventoryCalls: { before: MAX_COUNT, after: MAX_COUNT },
    lastCall: call,
    mutation: call,
    reconciledOrdinal: 3,
    operations: DIRECT_SCENARIO_OPERATION_SLOTS.map((slot) => ({
      slot,
      operationId: MAX_ID,
      inputSha256: DIGEST,
      tokenRevision: 3,
    })),
    records: (['a', 'b', 'recovery'] as const).map((role) => ({
      role,
      present: true,
      phase: MAX_ID,
      desiredSpecDigest: DIGEST,
      pendingSpecDigest: DIGEST,
      artifactVersion: MAX_ID,
      pendingArtifactVersion: MAX_ID,
      databaseId: MAX_ID,
    })),
    failure: {
      code: 'observation-mismatch',
      ordinal: MAX_COUNT,
      detail: 'below-scenario-floor' as const,
    },
    proofs: {
      initial: {
        a: workerVersion('a', '1', 100),
        b: workerVersion('b', '1', 100),
        recovery: workerVersion('recovery', '1', 100),
      },
      candidate: {
        a: workerVersion('a', '2', 0),
        b: workerVersion('b', '2', 0),
      },
      final: {
        a: workerVersion('a', '2', 100),
        b: workerVersion('b', '2', 100),
      },
      objects: {
        a: { size: MAX_COUNT, sha256: DIGEST },
        b: { size: MAX_COUNT, sha256: DIGEST },
      },
      objectDeletions: { a: 1, b: 1 },
      recoveryExportAbsent: { beforeOrdinal: 1, afterOrdinal: 2 },
      health: [
        {
          role: 'a' as const,
          release: '1' as const,
          marker: 'initial' as const,
          ordinal: 1,
        },
        {
          role: 'b' as const,
          release: '1' as const,
          marker: 'initial' as const,
          ordinal: 1,
        },
        {
          role: 'recovery' as const,
          release: '1' as const,
          marker: 'initial' as const,
          ordinal: 1,
        },
        {
          role: 'a' as const,
          release: '2' as const,
          marker: 'next' as const,
          ordinal: 1,
        },
        {
          role: 'b' as const,
          release: '2' as const,
          marker: 'next' as const,
          ordinal: 1,
        },
      ],
      inventories: { before: inventoryProof(), after: inventoryProof() },
      audits: { before: auditProof(), after: auditProof() },
      fence: {
        drain: { a: fenceTransition(), b: fenceTransition() },
        sweeps: { a: fenceSweeps(), b: fenceSweeps() },
        reopen: { a: fenceTransition(), b: fenceTransition() },
        probes: { a: fenceProbes(), b: fenceProbes() },
      },
      restart: {
        process: { ...PROCESS },
        resumedProcess: { ...RESUMED },
        lossOrdinal: 1,
        operationId: MAX_ID,
        witnessSha256: DIGEST,
        claimSha256: DIGEST,
        successorSha256: DIGEST,
        itemsSha256: DIGEST,
        replayOrdinal: 2,
      },
      steps: Array.from({ length: 64 }, (_entry, index) => ({
        ordinal: 1,
        itemOrdinal: (index % 2) as 0 | 1,
        step: MAX_ID,
        beforeCursor: MAX_COUNT,
        afterCursor: MAX_COUNT,
        provider: MAX_COUNT,
        maintenance: MAX_COUNT,
        application: MAX_COUNT,
      })),
      effects: (['a', 'b'] as const).map((role) => ({
        role,
        tenantTag: MAX_ID,
        environment: MAX_ID,
        scriptName: MAX_ID,
        databaseId: MAX_ID,
        versionId: MAX_ID,
        specDigest: DIGEST,
        schemaVersion: 2,
        settlementKey: DIGEST,
        identitySha256: DIGEST,
        provenanceSha256: DIGEST,
      })),
      cleanup: {
        version: 1 as const,
        operationId: MAX_ID,
        tenantTag: MAX_ID,
        environment: MAX_ID,
        backend: 'plain-worker' as const,
        scriptName: MAX_ID,
        databaseId: MAX_ID,
        databaseName: MAX_ID,
        authority: 'provisioning-rollback' as const,
        admittedPhase: MAX_ID,
        disposition: 'reservation-cleared' as const,
        evidence: {
          eligibility: 'reservation-only' as const,
          ingressRemoved: true,
          workerAbsent: true,
          platformResourcesAbsent: true,
          applicationR2Settled: true,
          databaseAbsentReadback: true,
          scan: {
            discover: { evidenceSha256: DIGEST, evidenceCount: MAX_COUNT },
            verify: { evidenceSha256: DIGEST, evidenceCount: MAX_COUNT },
          },
        },
        completedAtMs: MAX_COUNT,
      },
      exports: { a: exportProof('a'), b: exportProof('b') },
      exportVerifications: Array.from({ length: 16 }, () => exportProof('a')),
      decommission: {
        a: {
          operationId: MAX_ID,
          databaseId: MAX_ID,
          scriptName: MAX_ID,
          phase: 'decommissioned' as const,
        },
        b: {
          operationId: MAX_ID,
          databaseId: MAX_ID,
          scriptName: MAX_ID,
          phase: 'decommissioned' as const,
        },
      },
      terminalForce: {
        a: {
          databaseId: MAX_ID,
          scriptName: MAX_ID,
          ordinal: 3,
          attempts: {
            provider: MAX_COUNT,
            maintenance: MAX_COUNT,
            application: MAX_COUNT,
          },
        },
      },
      force: footprint(),
      residual: footprint(),
    },
  } as MutableScenario;
}

export function scenarioWith(
  mutate: (state: MutableScenario) => unknown,
  callKind: 'audit-page' | 'force-terminal' = 'force-terminal',
): MutableScenario {
  const state = maximalScenario(callKind);
  mutate(state);
  return state;
}

export async function scenarioJournal(limit = 8, disposableAccount = true) {
  const f = await fixture(limit, disposableAccount);
  const journal = await opened({ ...f.input, mode: 'run' });
  await confirmedBootstrap(f, journal);
  let ordinal = 0;
  for (let index = 0; index < 3; index++) {
    const reservation = await journal.reserveInvocation(f.request());
    await journal.settleInvocation(reservation);
    ordinal = reservation.ordinal;
  }
  await journal.recordBootstrapObservation({ kind: 'control-read', ordinal });
  return { f, journal };
}

export const EXPORT_IDENTITY = {
  a: { databaseId: 'database-a', operationId: 'operation-a' },
  b: { databaseId: 'database-b', operationId: 'operation-b' },
} as const;

export function exportKey(prefix: string, role: 'a' | 'b') {
  const { databaseId, operationId } = EXPORT_IDENTITY[role];
  return `${prefix}/receipts/v1/${databaseId}/${operationId}.sql`;
}

export function completeScenario(): MutableScenario {
  return scenarioWith((state) => {
    state.phase = 'complete';
    state.failure = null;
    const [armA, armB] = state.proofs.steps;
    if (!armA || !armB) throw new Error('scenario fixture lost its steps');
    armA.step = 'arm-maintenance';
    armB.step = 'arm-maintenance';
    for (const role of ['a', 'b'] as const) {
      const proof = present(state.proofs.exports[role]);
      proof.receipt.databaseId = EXPORT_IDENTITY[role].databaseId;
      proof.receipt.operationId = EXPORT_IDENTITY[role].operationId;
    }
    state.proofs.exportVerifications = state.proofs.exportVerifications.map(
      (_entry, index) =>
        structuredClone(present(state.proofs.exports[index % 2 ? 'b' : 'a'])),
    );
  });
}

export async function completeScenarioJournal(
  limit = 8,
  disposableAccount = true,
) {
  const opening = await scenarioJournal(limit, disposableAccount);
  await opening.journal.recordScenario(completeScenario());
  return opening;
}

export type MutableResidual = Omit<
  Mutable<DirectResidualObservation>,
  'bucketJurisdictions'
> & { bucketJurisdictions: ['default'] };
export type MutableTeardown = Omit<Mutable<DirectTeardownState>, 'residual'> & {
  residual: MutableResidual | null;
};
export const MAX_NAME = 'n'.repeat(DIRECT_TEARDOWN_MAXIMA.nameBytes);
export const MAX_KEY = `k/${'k'.repeat(DIRECT_TEARDOWN_MAXIMA.keyBytes - 2)}`;

export function residualObservation(): MutableResidual {
  return {
    version: 1,
    surfaces: Object.fromEntries(
      DIRECT_RESIDUAL_SURFACES.map((surface) => [
        surface,
        {
          prefixCount: MAX_COUNT,
          prefixNames: Array.from(
            { length: DIRECT_TEARDOWN_MAXIMA.prefixNames },
            () => MAX_NAME,
          ),
          globalCount: MAX_COUNT,
          exhaustive: true,
        },
      ]),
    ) as MutableResidual['surfaces'],
    bucketJurisdictions: ['default'],
    dispatch: {
      kind: 'enumerated',
      count: MAX_COUNT,
      status: MAX_COUNT,
      prefixCount: MAX_COUNT,
    },
    versionsGone: false,
    settleAttempts: DIRECT_TEARDOWN_MAXIMA.settleAttempts,
  };
}

export function maximalTeardown(): MutableTeardown {
  const settlement = { ordinal: MAX_COUNT, settledByReread: true };
  return {
    version: 1,
    phase: 'complete',
    pending: null,
    receipts: {
      ingress: { ...settlement },
      worker: {
        scriptName: MAX_NAME,
        secretNames: Array.from(
          { length: DIRECT_TEARDOWN_MAXIMA.secretNames },
          () => MAX_NAME,
        ),
        ...settlement,
      },
      fleet: { uuid: MAX_NAME, ...settlement },
      quota: { uuid: MAX_NAME, ...settlement },
      exportObjects: Array.from(
        { length: DIRECT_TEARDOWN_MAXIMA.exportObjects },
        (_entry, index) => ({
          key: `${index}${MAX_KEY.slice(1)}`,
          ...settlement,
        }),
      ),
      exports: { name: MAX_NAME, ...settlement },
    },
    residual: residualObservation(),
    providerRequests: MAX_COUNT,
    failure: 'residual-present',
  };
}

export function teardownState(): MutableTeardown {
  return {
    version: 1,
    phase: 'ingress',
    pending: null,
    receipts: {
      ingress: null,
      worker: null,
      fleet: null,
      quota: null,
      exportObjects: [],
      exports: null,
    },
    residual: null,
    providerRequests: 0,
    failure: null,
  };
}

export function teardownWith(
  mutate: (state: MutableTeardown) => unknown,
): MutableTeardown {
  const state = teardownState();
  mutate(state);
  return state;
}
