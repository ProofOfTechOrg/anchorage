// SPDX-License-Identifier: Apache-2.0

import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { lstat, mkdir, open, rename, rmdir, unlink } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import PQueue from 'p-queue';
import { deriveDirectConformanceNames } from './direct-credentialed-conformance-config.mjs';
import { DIRECT_SCENARIO_PHASES } from './direct-credentialed-scenario-budget.mjs';
import {
  DIRECT_REFERENCE_BODY_LIMIT,
  readDirectReferenceRequest,
} from './direct-reference-contract.mjs';

const ERROR_CODES = new Set([
  'invalid-state',
  'run-exists',
  'run-missing',
  'lock-unavailable',
  'outcome-unknown',
  'invocation-budget-exhausted',
  'unsupported-scenario-version',
]);
const SUMMARY_FIELDS = [
  'kind',
  'role',
  'slot',
  'operation',
  'release',
  'limit',
  'afterOrdinal',
  'expectedMutationEpoch',
  'expectedRevision',
];
const NUMERIC_SUMMARY_FIELDS = new Set([
  'limit',
  'afterOrdinal',
  'expectedMutationEpoch',
  'expectedRevision',
]);
export const DIRECT_RUN_MAX_JOURNAL_BYTES = 256 * 1024;
export const DIRECT_SCENARIO_ARRAY_MAXIMA = Object.freeze({
  health: 5,
  steps: 64,
  exportVerifications: 16,
  auditFindings: 16,
  footprintVersionIds: 8,
  deploymentVersions: 2,
  inventoryCategories: 9,
  inventory: Object.freeze({
    databaseIds: 2,
    namespaceIds: 4,
    scriptNames: 2,
    routeHostnames: 2,
    bucketNames: 2,
    findings: 32,
  }),
});

export class DirectRunStateError extends Error {
  constructor(code = 'invalid-state') {
    const accepted = ERROR_CODES.has(code) ? code : 'invalid-state';
    super(accepted);
    this.name = 'DirectRunStateError';
    this.code = accepted;
  }
}

function invalid() {
  throw new DirectRunStateError();
}

function stateError(error) {
  return error instanceof DirectRunStateError
    ? error
    : new DirectRunStateError();
}

function object(value, keys) {
  if (
    !value ||
    typeof value !== 'object' ||
    Array.isArray(value) ||
    Object.keys(value).length !== keys.length ||
    keys.some((key) => !Object.hasOwn(value, key))
  )
    invalid();
  return value;
}

function digest(value) {
  if (typeof value !== 'string' || !/^[a-f0-9]{64}$/u.test(value)) invalid();
  return value;
}

export const DIRECT_RUN_MAX_RESUME_COUNT = 999_999;
const RUN_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u;

function runTimestamp(value) {
  if (typeof value !== 'string' || !RUN_TIMESTAMP.test(value)) invalid();
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed) || new Date(parsed).toISOString() !== value)
    invalid();
  return value;
}

function resumeCounter(value) {
  if (
    !Number.isSafeInteger(value) ||
    value < 0 ||
    value > DIRECT_RUN_MAX_RESUME_COUNT
  )
    invalid();
  return value;
}

function runClock(now) {
  if (now === undefined) return Date.now();
  if (!Number.isSafeInteger(now) || now < 0) invalid();
  return now;
}

function bindingFromInput(input) {
  const accountId = input.accountId;
  if (
    typeof accountId !== 'string' ||
    !accountId ||
    accountId !== accountId.trim() ||
    accountId.length > 128 ||
    [...accountId].some(
      (character) =>
        character.charCodeAt(0) <= 31 || character.charCodeAt(0) === 127,
    )
  )
    invalid();
  const resourcePrefix = input.prepared.config.resourcePrefix;
  const maxInvocations = input.prepared.config.referenceWorker.maxInvocations;
  if (
    typeof resourcePrefix !== 'string' ||
    !/^fc[a-f0-9]{24}$/u.test(resourcePrefix) ||
    !Number.isSafeInteger(maxInvocations) ||
    maxInvocations < 1
  )
    invalid();
  return Object.freeze({
    accountId,
    configSha256: digest(input.prepared.configSha256),
    referenceModuleSetSha256: digest(input.prepared.referenceModuleSetSha256),
    resourcePrefix,
    maxInvocations,
  });
}

export function actionSummary(action) {
  return Object.freeze(
    Object.fromEntries(
      SUMMARY_FIELDS.filter((key) => Object.hasOwn(action, key)).map((key) => [
        key,
        action[key],
      ]),
    ),
  );
}

function replayableAction(action) {
  const result = { ...action };
  if (
    result.kind === 'cleanup-restart-blocked' ||
    result.kind === 'decommission-restart-blocked'
  )
    result.token = null;
  return result;
}

async function decodeRequest(serialized, configSha256) {
  if (
    typeof serialized !== 'string' ||
    Buffer.byteLength(serialized) > DIRECT_REFERENCE_BODY_LIMIT
  )
    invalid();
  try {
    return await readDirectReferenceRequest(
      new Request('https://direct-conformance.invalid/', {
        method: 'POST',
        body: serialized,
      }),
      configSha256,
    );
  } catch {
    invalid();
  }
}

async function decodeSnapshot(value, binding) {
  const keys = ['version', 'binding', 'invocationCount', 'lastInvocation'];
  object(
    value,
    value.version === 1
      ? keys
      : [
          ...keys,
          'bootstrap',
          ...(Object.hasOwn(value, 'createdAt') ? ['createdAt'] : []),
          ...(Object.hasOwn(value, 'resumeCount') ? ['resumeCount'] : []),
          ...(Object.hasOwn(value, 'scenario') ? ['scenario'] : []),
          ...(Object.hasOwn(value, 'teardown') ? ['teardown'] : []),
        ],
  );
  const createdAt = Object.hasOwn(value, 'createdAt')
    ? runTimestamp(value.createdAt)
    : undefined;
  const resumeCount = Object.hasOwn(value, 'resumeCount')
    ? resumeCounter(value.resumeCount)
    : undefined;
  object(value.binding, Object.keys(binding));
  if (
    ![1, 2].includes(value.version) ||
    Object.entries(binding).some(
      ([key, expected]) => value.binding[key] !== expected,
    ) ||
    !Number.isSafeInteger(value.invocationCount) ||
    value.invocationCount < 0 ||
    value.invocationCount > binding.maxInvocations
  )
    invalid();
  let lastInvocation = null;
  if (value.invocationCount === 0) {
    if (value.lastInvocation !== null) invalid();
  } else {
    const last = object(value.lastInvocation, [
      'ordinal',
      'requestSha256',
      'action',
      'state',
    ]);
    if (
      last.ordinal !== value.invocationCount ||
      (last.state !== 'pending' && last.state !== 'settled') ||
      !last.action ||
      typeof last.action !== 'object' ||
      Array.isArray(last.action) ||
      Object.hasOwn(last.action, 'token')
    )
      invalid();
    const decoded = await decodeRequest(
      JSON.stringify({
        contractVersion: 1,
        configSha256: binding.configSha256,
        action: replayableAction(last.action),
      }),
      binding.configSha256,
    );
    lastInvocation = Object.freeze({
      ordinal: last.ordinal,
      requestSha256: digest(last.requestSha256),
      action: actionSummary(decoded.action),
      state: last.state,
    });
  }
  const scenario = Object.hasOwn(value, 'scenario')
    ? decodeScenario(value.scenario, value.invocationCount)
    : undefined;
  if (scenario) await validateScenarioActions(scenario, binding);
  const teardown = Object.hasOwn(value, 'teardown')
    ? decodeTeardown(value.teardown)
    : undefined;
  return Object.freeze({
    version: 2,
    ...(createdAt !== undefined ? { createdAt } : {}),
    ...(resumeCount !== undefined ? { resumeCount } : {}),
    binding,
    invocationCount: value.invocationCount,
    lastInvocation,
    bootstrap: decodeBootstrap(
      value.version === 1 ? null : value.bootstrap,
      binding,
      value.invocationCount,
      lastInvocation,
    ),
    ...(scenario ? { scenario } : {}),
    ...(teardown ? { teardown } : {}),
  });
}

function identifier(value, max = 128) {
  if (
    typeof value !== 'string' ||
    !value ||
    value !== value.trim() ||
    value.length > max ||
    [...value].some(
      (character) =>
        character.charCodeAt(0) <= 31 || character.charCodeAt(0) === 127,
    )
  )
    invalid();
  return value;
}

function equalShape(value, expected) {
  if (Array.isArray(expected)) {
    if (!Array.isArray(value) || value.length !== expected.length) invalid();
    expected.forEach((entry, index) => {
      equalShape(value[index], entry);
    });
  } else if (expected !== null && typeof expected === 'object') {
    object(value, Object.keys(expected));
    for (const key of Object.keys(expected))
      equalShape(value[key], expected[key]);
  } else if (value !== expected) invalid();
}

export const DIRECT_SCENARIO_FAILURES = Object.freeze([
  'observation-mismatch',
  'outcome-unknown',
  'proof-unavailable',
  'budget-exhausted',
  'invocation-budget-exhausted',
  'reference-refused',
  'invalid-input',
  'provider-unavailable',
  'journal-failed',
  'blocked',
]);

export const DIRECT_SCENARIO_FAILURE_DETAILS = Object.freeze([
  'platform-page',
  'transport-failure',
  'non-contract-answer',
  'delivery-window-expired',
  'phase-ceiling',
  'run-reserve',
  'below-scenario-floor',
]);

export const DIRECT_SCENARIO_OPERATION_SLOTS = Object.freeze([
  'inventory-before',
  'inventory-after',
  'audit-before',
  'audit-after',
  'migration-next',
  'cleanup-a',
  'cleanup-b',
  'cleanup-recovery',
  'cleanup-recovery-initial',
  'decommission-a',
  'decommission-b',
  'decommission-recovery',
]);

export const DIRECT_TEARDOWN_PHASES = Object.freeze([
  'refused',
  'ingress',
  'worker',
  'fleet',
  'quota',
  'export-objects',
  'exports',
  'residual',
  'complete',
]);

export const DIRECT_TEARDOWN_MUTATIONS = Object.freeze([
  'disable-reference-ingress',
  'delete-reference-worker',
  'delete-fleet-d1',
  'delete-quota-d1',
  'delete-export-object',
  'delete-export-r2',
]);

export const DIRECT_TEARDOWN_FAILURES = Object.freeze([
  'scenario-incomplete',
  'outcome-unknown',
  'unexpected-object',
  'identity-mismatch',
  'residual-present',
  'forbidden',
  'provider-unavailable',
  'budget-exhausted',
  'invalid-state',
]);

export const DIRECT_RESIDUAL_SURFACES = Object.freeze([
  'databases',
  'durableObjectNamespaces',
  'scripts',
  'buckets',
  'domains',
  'routes',
  'queues',
]);

export const DIRECT_TEARDOWN_MAXIMA = Object.freeze({
  nameBytes: 255,
  keyBytes: 1024,
  prefixNames: 16,
  secretNames: 8,
  exportObjects: 2,
  settleAttempts: 5,
});

const TEARDOWN_RECEIPT_FIELD = Object.freeze({
  'disable-reference-ingress': 'ingress',
  'delete-reference-worker': 'worker',
  'delete-fleet-d1': 'fleet',
  'delete-quota-d1': 'quota',
  'delete-export-object': 'exportObjects',
  'delete-export-r2': 'exports',
});

const TEARDOWN_RECEIPT_ORDER = Object.freeze([
  'ingress',
  'worker',
  'fleet',
  'quota',
  'exportObjects',
  'exports',
]);

const RESIDUAL_PHASES = Object.freeze(['refused', 'residual', 'complete']);

const scenarioNumber = (value) => {
  if (!Number.isSafeInteger(value) || value < 0) invalid();
  return value;
};
const scenarioId = (value) => {
  if (typeof value !== 'string' || !/^[A-Za-z0-9_.:-]{1,128}$/u.test(value))
    invalid();
  return value;
};
const scenarioHostname = (value) => {
  if (
    typeof value !== 'string' ||
    value.length > 253 ||
    !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$/u.test(
      value,
    ) ||
    !/[a-z]/u.test(value)
  )
    invalid();
  return value;
};
const scenarioEnum =
  (...values) =>
  (value) => {
    if (!values.includes(value)) invalid();
    return value;
  };
const nullable = (schema) => (value) =>
  value === null ? null : scenarioShape(value, schema);
const scenarioFlag = scenarioEnum(true, false);
const boundedArray = (schema, max) => {
  if (!Number.isSafeInteger(max) || max < 0) invalid();
  return (value) => {
    if (!Array.isArray(value) || value.length > max) invalid();
    return Object.freeze(value.map((entry) => scenarioShape(entry, schema)));
  };
};
const OPTIONAL = Symbol('optional');
const optional = (schema) => ({ [OPTIONAL]: schema });
const optionalSchema = (field) =>
  field && typeof field === 'object' && OPTIONAL in field
    ? field[OPTIONAL]
    : null;
function scenarioShape(value, schema) {
  if (typeof schema === 'function') return schema(value);
  if (schema === null || typeof schema !== 'object') {
    if (value !== schema) invalid();
    return value;
  }
  const fields = Object.entries(schema).filter(
    ([key, field]) =>
      !optionalSchema(field) ||
      (Boolean(value) &&
        typeof value === 'object' &&
        Object.hasOwn(value, key)),
  );
  object(
    value,
    fields.map(([key]) => key),
  );
  return Object.freeze(
    Object.fromEntries(
      fields.map(([key, field]) => [
        key,
        scenarioShape(value[key], optionalSchema(field) ?? field),
      ]),
    ),
  );
}
const scenarioDate = (value) => {
  identifier(value, 32);
  if (
    !Number.isFinite(Date.parse(value)) ||
    new Date(value).toISOString() !== value
  )
    invalid();
  return value;
};
const scenarioLocation = (value) => {
  if (
    typeof value !== 'string' ||
    !/^r2:\/\/[A-Za-z0-9_./:-]{1,700}$/u.test(value)
  )
    invalid();
  return value;
};
const normalRole = scenarioEnum('a', 'b');
const scenarioRole = scenarioEnum('a', 'b', 'recovery');
const attemptsShape = {
  provider: scenarioNumber,
  maintenance: scenarioNumber,
  application: scenarioNumber,
};
const expectedVersionShape = {
  role: scenarioRole,
  versionId: scenarioId,
  databaseId: scenarioId,
  specDigest: digest,
  applicationRelease: scenarioEnum('1', '2'),
};
const workerVersionShape = {
  ...expectedVersionShape,
  accountId: scenarioId,
  tenantTag: scenarioId,
  environment: scenarioId,
  scriptName: scenarioId,
  currentDeployment: {
    deploymentId: scenarioId,
    activeVersionId: scenarioId,
    versions: boundedArray(
      {
        versionId: scenarioId,
        percentage: (value) => {
          if (
            typeof value !== 'number' ||
            !Number.isFinite(value) ||
            value < 0 ||
            value > 100
          )
            invalid();
          return value;
        },
      },
      DIRECT_SCENARIO_ARRAY_MAXIMA.deploymentVersions,
    ),
  },
  trafficPercentage: scenarioEnum(0, 100),
  cpuLimitMs: scenarioNumber,
  subrequestLimit: scenarioNumber,
  schemaVersion: scenarioEnum(1, 2),
  namespaces: boundedArray(
    {
      binding: scenarioEnum('MAINTENANCE', 'RUNNER'),
      className: scenarioEnum('Maintenance', 'Runner'),
      namespaceId: scenarioId,
    },
    2,
  ),
  bucket: {
    name: scenarioId,
    jurisdiction: 'default',
    creationDate: scenarioDate,
  },
};
const receiptShape = {
  version: 1,
  authority: scenarioLocation,
  databaseId: scenarioId,
  operationId: scenarioId,
};
const exportShape = {
  verified: true,
  role: normalRole,
  receipt: receiptShape,
  location: scenarioLocation,
  size: scenarioNumber,
  sha256: digest,
  sourceInvocationOrdinal: scenarioNumber,
};
const cleanupShape = {
  version: 1,
  operationId: scenarioId,
  tenantTag: scenarioId,
  environment: scenarioId,
  backend: 'plain-worker',
  scriptName: scenarioId,
  databaseId: scenarioId,
  databaseName: scenarioId,
  authority: 'provisioning-rollback',
  admittedPhase: scenarioId,
  disposition: scenarioEnum(
    'prepublication-owned-no-export',
    'reservation-cleared',
  ),
  evidence: {
    eligibility: scenarioEnum(
      'carrier-null',
      'legacy-phase-impossible',
      'reservation-only',
    ),
    ingressRemoved: true,
    workerAbsent: true,
    platformResourcesAbsent: true,
    applicationR2Settled: true,
    databaseAbsentReadback: true,
    scan: optional({
      discover: { evidenceSha256: digest, evidenceCount: scenarioNumber },
      verify: { evidenceSha256: digest, evidenceCount: scenarioNumber },
    }),
  },
  completedAtMs: scenarioNumber,
};
const noEntries = boundedArray(scenarioId, 0);
const footprintShape = {
  version: 1,
  role: 'recovery',
  beforeIdentitySha256: digest,
  fleetRecordPresent: false,
  deploymentClaimsPresent: false,
  database: { id: scenarioId, expectedName: scenarioId, observedName: null },
  worker: {
    scriptName: scenarioId,
    scriptPresent: scenarioEnum(true, false),
    workersDevEnabled: scenarioEnum(false, null),
    previewUrlsEnabled: scenarioEnum(false, null),
    customDomains: noEntries,
    zoneRoutes: noEntries,
    currentSecretNames: noEntries,
    currentVersionIds: nullable(
      boundedArray(
        scenarioId,
        DIRECT_SCENARIO_ARRAY_MAXIMA.footprintVersionIds,
      ),
    ),
    currentNamespaceIds: boundedArray(scenarioId, 2),
    survivingRecordedNamespaceIds: boundedArray(scenarioId, 2),
  },
  buckets: boundedArray(
    {
      bindingName: 'PROBE_BUCKET',
      bucketName: scenarioId,
      jurisdiction: 'default',
      expectedCreationDate: scenarioDate,
      observedCreationDate: nullable(scenarioDate),
    },
    1,
  ),
  priorCleanup: {
    operationId: scenarioId,
    observedReceiptSha256: digest,
    matchesBefore: true,
  },
};
const operationSlots = scenarioEnum(...DIRECT_SCENARIO_OPERATION_SLOTS);
const processShape = {
  pid: scenarioNumber,
  startTicks: scenarioId,
  bootId: scenarioId,
};
const decommissionShape = {
  operationId: scenarioId,
  databaseId: scenarioId,
  scriptName: scenarioId,
  phase: 'decommissioned',
};
const callShape = {
  ordinal: scenarioNumber,
  action: (value) => {
    if (
      !value ||
      typeof value !== 'object' ||
      Array.isArray(value) ||
      Object.keys(value).some((key) => !SUMMARY_FIELDS.includes(key))
    )
      invalid();
    const result = {};
    for (const [key, field] of Object.entries(value))
      result[key] = NUMERIC_SUMMARY_FIELDS.has(key)
        ? scenarioNumber(field)
        : scenarioId(field);
    if (typeof value.kind !== 'string') invalid();
    return Object.freeze(result);
  },
  outcome: scenarioEnum(
    'prepared',
    'returned',
    'injected-response-loss',
    'reference-refused',
  ),
  attempts: nullable(attemptsShape),
  migration: nullable({
    itemOrdinal: scenarioEnum(0, 1),
    cursor: scenarioNumber,
    step: scenarioId,
    itemsSha256: digest,
  }),
  before: optional(
    nullable({ databaseId: scenarioId, scriptName: scenarioId }),
  ),
};
const inventoryShape = {
  operationId: scenarioId,
  generation: scenarioNumber,
  calls: scenarioNumber,
  databaseIds: boundedArray(
    scenarioId,
    DIRECT_SCENARIO_ARRAY_MAXIMA.inventory.databaseIds,
  ),
  namespaceIds: boundedArray(
    scenarioId,
    DIRECT_SCENARIO_ARRAY_MAXIMA.inventory.namespaceIds,
  ),
  scriptNames: boundedArray(
    scenarioId,
    DIRECT_SCENARIO_ARRAY_MAXIMA.inventory.scriptNames,
  ),
  routeHostnames: boundedArray(
    scenarioHostname,
    DIRECT_SCENARIO_ARRAY_MAXIMA.inventory.routeHostnames,
  ),
  bucketNames: boundedArray(
    scenarioId,
    DIRECT_SCENARIO_ARRAY_MAXIMA.inventory.bucketNames,
  ),
  findings: boundedArray(
    { kind: scenarioId, detailSha256: digest },
    DIRECT_SCENARIO_ARRAY_MAXIMA.inventory.findings,
  ),
};
const auditShape = {
  operationId: scenarioId,
  generation: scenarioNumber,
  recordCount: 2,
  findingCount: scenarioNumber,
  finalizedAtMs: scenarioNumber,
  findings: boundedArray(
    {
      tenantTag: scenarioId,
      environment: scenarioId,
      kind: scenarioId,
      detailSha256: digest,
    },
    DIRECT_SCENARIO_ARRAY_MAXIMA.auditFindings,
  ),
};
const operationsShape = boundedArray(
  {
    slot: operationSlots,
    operationId: nullable(scenarioId),
    inputSha256: digest,
    tokenRevision: nullable(scenarioNumber),
  },
  DIRECT_SCENARIO_OPERATION_SLOTS.length,
);
const recordsShape = boundedArray(
  {
    role: scenarioRole,
    present: scenarioEnum(true, false),
    phase: nullable(scenarioId),
    desiredSpecDigest: nullable(digest),
    pendingSpecDigest: nullable(digest),
    artifactVersion: nullable(scenarioId),
    pendingArtifactVersion: nullable(scenarioId),
    databaseId: nullable(scenarioId),
  },
  3,
);
const healthShape = boundedArray(
  {
    role: scenarioRole,
    release: scenarioEnum('1', '2'),
    marker: scenarioEnum('initial', 'next'),
    ordinal: scenarioNumber,
  },
  DIRECT_SCENARIO_ARRAY_MAXIMA.health,
);
const stepsShape = boundedArray(
  {
    ordinal: scenarioNumber,
    itemOrdinal: scenarioEnum(0, 1),
    step: scenarioId,
    beforeCursor: scenarioNumber,
    afterCursor: scenarioNumber,
    provider: scenarioNumber,
    maintenance: scenarioNumber,
    application: scenarioNumber,
  },
  DIRECT_SCENARIO_ARRAY_MAXIMA.steps,
);
const effectsShape = boundedArray(
  {
    role: normalRole,
    tenantTag: scenarioId,
    environment: scenarioId,
    scriptName: scenarioId,
    databaseId: scenarioId,
    versionId: scenarioId,
    specDigest: digest,
    schemaVersion: 2,
    settlementKey: digest,
    identitySha256: digest,
    provenanceSha256: digest,
  },
  2,
);
const exportVerificationsShape = boundedArray(
  exportShape,
  DIRECT_SCENARIO_ARRAY_MAXIMA.exportVerifications,
);
const categoryShape = {
  category: scenarioId,
  class: scenarioEnum('work', 'standing'),
  empty: scenarioFlag,
};
const sweepCategoriesShape = boundedArray(
  categoryShape,
  DIRECT_SCENARIO_ARRAY_MAXIMA.inventoryCategories,
);
const fenceReadingShape = {
  state: scenarioEnum('open', 'draining', 'migration-locked', 'proof-only'),
  mutationEpoch: scenarioNumber,
  requireMutationEpoch: scenarioFlag,
  transitionRevision: scenarioNumber,
};
const fenceTransitionShape = {
  before: fenceReadingShape,
  after: nullable(fenceReadingShape),
  ordinal: nullable(scenarioNumber),
};
const fenceSweepShape = {
  fence: fenceReadingShape,
  categories: sweepCategoriesShape,
  observedAt: scenarioNumber,
  ordinal: scenarioNumber,
};
const fenceSweepsShape = {
  first: fenceSweepShape,
  second: nullable(fenceSweepShape),
  intervalMs: nullable(scenarioNumber),
};
const fenceProbesShape = {
  current: 'accepted',
  missing: 'missing',
  stale: 'stale',
  future: 'future',
  mutationEpoch: scenarioNumber,
  ordinal: scenarioNumber,
};
function decodeScenario(value, invocationCount) {
  if (
    value &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    Object.hasOwn(value, 'version') &&
    value.version !== 1
  )
    throw new DirectRunStateError('unsupported-scenario-version');
  const result = scenarioShape(value, {
    version: 1,
    phase: scenarioEnum(...DIRECT_SCENARIO_PHASES),
    startedOrdinal: scenarioNumber,
    callCount: scenarioNumber,
    phaseCalls: Object.fromEntries(
      DIRECT_SCENARIO_PHASES.map((phase) => [phase, scenarioNumber]),
    ),
    attempts: attemptsShape,
    sdkRequests: scenarioNumber,
    inventoryCalls: { before: scenarioNumber, after: scenarioNumber },
    lastCall: nullable(callShape),
    mutation: nullable(callShape),
    reconciledOrdinal: scenarioNumber,
    operations: operationsShape,
    records: recordsShape,
    failure: nullable({
      code: scenarioEnum(...DIRECT_SCENARIO_FAILURES),
      ordinal: scenarioNumber,
      detail: optional(scenarioEnum(...DIRECT_SCENARIO_FAILURE_DETAILS)),
    }),
    proofs: {
      initial: {
        a: nullable(workerVersionShape),
        b: nullable(workerVersionShape),
        recovery: nullable(workerVersionShape),
      },
      candidate: {
        a: nullable(workerVersionShape),
        b: nullable(workerVersionShape),
      },
      final: {
        a: nullable(workerVersionShape),
        b: nullable(workerVersionShape),
      },
      objects: {
        a: nullable({ size: scenarioNumber, sha256: digest }),
        b: nullable({ size: scenarioNumber, sha256: digest }),
      },
      objectDeletions: {
        a: nullable(scenarioNumber),
        b: nullable(scenarioNumber),
      },
      recoveryExportAbsent: {
        beforeOrdinal: nullable(scenarioNumber),
        afterOrdinal: nullable(scenarioNumber),
      },
      health: healthShape,
      inventories: {
        before: nullable(inventoryShape),
        after: nullable(inventoryShape),
      },
      audits: { before: nullable(auditShape), after: nullable(auditShape) },
      fence: {
        drain: {
          a: nullable(fenceTransitionShape),
          b: nullable(fenceTransitionShape),
        },
        sweeps: {
          a: nullable(fenceSweepsShape),
          b: nullable(fenceSweepsShape),
        },
        reopen: {
          a: nullable(fenceTransitionShape),
          b: nullable(fenceTransitionShape),
        },
        probes: {
          a: nullable(fenceProbesShape),
          b: nullable(fenceProbesShape),
        },
      },
      restart: nullable({
        process: processShape,
        resumedProcess: nullable(processShape),
        lossOrdinal: scenarioNumber,
        operationId: scenarioId,
        witnessSha256: digest,
        claimSha256: digest,
        successorSha256: digest,
        itemsSha256: digest,
        replayOrdinal: nullable(scenarioNumber),
      }),
      steps: stepsShape,
      effects: effectsShape,
      cleanup: nullable(cleanupShape),
      exports: { a: nullable(exportShape), b: nullable(exportShape) },
      exportVerifications: exportVerificationsShape,
      decommission: {
        a: nullable(decommissionShape),
        b: nullable(decommissionShape),
      },
      terminalForce: {
        a: nullable({
          databaseId: scenarioId,
          scriptName: scenarioId,
          ordinal: scenarioNumber,
          attempts: attemptsShape,
        }),
      },
      force: nullable(footprintShape),
      residual: nullable(footprintShape),
    },
  });
  if (
    result.startedOrdinal > invocationCount ||
    result.callCount > invocationCount - result.startedOrdinal ||
    Object.values(result.phaseCalls).reduce(
      (total, count) => total + count,
      0,
    ) !== result.callCount
  )
    invalid();
  for (const call of [result.lastCall, result.mutation]) {
    if (!call) continue;
    if (
      call.ordinal > invocationCount + (call.outcome === 'prepared' ? 1 : 0) ||
      call.ordinal < 1 ||
      (call.outcome === 'prepared') !== (call.attempts === null) ||
      Object.hasOwn(call, 'before') !==
        (call.action.kind === 'force-terminal' && call.outcome !== 'prepared')
    )
      invalid();
  }
  for (const proof of Object.values(result.proofs.exports))
    if (
      proof &&
      (proof.sourceInvocationOrdinal < 1 ||
        proof.sourceInvocationOrdinal > invocationCount ||
        proof.size < 1)
    )
      invalid();
  if (
    new Set(result.operations.map((entry) => entry.slot)).size !==
      result.operations.length ||
    new Set(result.records.map((entry) => entry.role)).size !==
      result.records.length
  )
    invalid();
  validateScenarioProofs(result, invocationCount);
  return result;
}

function validateScenarioProofs(state, invocationCount) {
  const proof = state.proofs;
  const past = (phase) =>
    DIRECT_SCENARIO_PHASES.indexOf(state.phase) >
    DIRECT_SCENARIO_PHASES.indexOf(phase);
  const need = (condition) => {
    if (!condition) invalid();
  };
  for (const role of ['a', 'b', 'recovery']) {
    if (past(`provision-${role}`))
      need(
        proof.initial[role] &&
          proof.health.some(
            (entry) =>
              entry.role === role &&
              entry.release === '1' &&
              entry.marker === 'initial',
          ),
      );
    if (role === 'recovery') continue;
    if (past(`provision-${role}`)) need(proof.objects[role]);
    if (past('migration'))
      need(
        proof.candidate[role]?.trafficPercentage === 0 &&
          proof.final[role]?.trafficPercentage === 100 &&
          proof.steps.some(
            (entry) =>
              entry.itemOrdinal === (role === 'a' ? 0 : 1) &&
              entry.step === 'arm-maintenance' &&
              entry.maintenance > 0,
          ),
      );
    if (past('post-migration'))
      need(
        proof.health.some(
          (entry) =>
            entry.role === role &&
            entry.release === '2' &&
            entry.marker === 'next',
        ),
      );
    if (past('delete-objects')) need(proof.objectDeletions[role] > 0);
    if (past(`decommission-${role}`))
      need(proof.exports[role] && proof.decommission[role]);
  }
  for (const when of ['before', 'after']) {
    if (past(`inventory-${when}`)) need(proof.inventories[when]?.calls > 1);
    if (past(`audit-${when}`))
      need(
        proof.audits[when]?.recordCount === 2 &&
          proof.audits[when].findingCount ===
            proof.audits[when].findings.length &&
          proof.audits[when].generation === proof.inventories[when].generation,
      );
  }
  if (past('migration-interrupt')) need(proof.restart);
  if (past('migration-restart'))
    need(proof.restart?.replayOrdinal && proof.restart.resumedProcess);
  if (past('migration')) need(proof.effects.length === 2);
  if (past('cleanup-recovery')) need(proof.cleanup);
  if (past('force-terminal-a')) need(proof.terminalForce.a);
  if (past('force-recovery'))
    need(proof.recoveryExportAbsent.beforeOrdinal > 0);
  if (past('force-observe'))
    need(proof.force && proof.recoveryExportAbsent.afterOrdinal > 0);
  if (past('recover-force-residual')) need(proof.residual);
  if (past('fence-drain'))
    need(
      ['a', 'b'].every(
        (role) =>
          proof.fence.drain[role]?.after &&
          proof.fence.drain[role].ordinal !== null &&
          proof.fence.sweeps[role]?.first,
      ),
    );
  if (past('fence-reopen'))
    need(
      ['a', 'b'].every(
        (role) =>
          proof.fence.sweeps[role]?.second &&
          proof.fence.sweeps[role].intervalMs !== null &&
          proof.fence.reopen[role]?.after &&
          proof.fence.reopen[role].ordinal !== null,
      ),
    );
  if (past('fence-proofs'))
    need(['a', 'b'].every((role) => proof.fence.probes[role]));
  for (const ordinal of [
    state.reconciledOrdinal,
    proof.terminalForce.a?.ordinal ?? null,
    ...Object.values(proof.objectDeletions),
    ...Object.values(proof.recoveryExportAbsent),
    ...proof.health.map((entry) => entry.ordinal),
    ...proof.steps.map((entry) => entry.ordinal),
    ...Object.values(proof.fence.drain).flatMap((entry) =>
      entry && entry.ordinal !== null ? [entry.ordinal] : [],
    ),
    ...Object.values(proof.fence.reopen).flatMap((entry) =>
      entry && entry.ordinal !== null ? [entry.ordinal] : [],
    ),
    ...Object.values(proof.fence.probes).flatMap((entry) =>
      entry ? [entry.ordinal] : [],
    ),
    ...Object.values(proof.fence.sweeps).flatMap((entry) =>
      entry ? [entry.first.ordinal] : [],
    ),
    ...Object.values(proof.fence.sweeps).flatMap((entry) =>
      entry && entry.second !== null ? [entry.second.ordinal] : [],
    ),
  ])
    if (ordinal !== null) need(ordinal <= invocationCount);
  if (proof.restart) {
    need(
      proof.restart.lossOrdinal > 0 &&
        proof.restart.lossOrdinal <= invocationCount,
    );
    if (proof.restart.replayOrdinal !== null)
      need(
        proof.restart.replayOrdinal > proof.restart.lossOrdinal &&
          proof.restart.replayOrdinal <= invocationCount &&
          proof.restart.resumedProcess &&
          JSON.stringify(proof.restart.process) !==
            JSON.stringify(proof.restart.resumedProcess),
      );
  }
}

async function validateScenarioActions(state, binding) {
  for (const call of [state.lastCall, state.mutation]) {
    if (!call) continue;
    await decodeRequest(
      JSON.stringify({
        contractVersion: 1,
        configSha256: binding.configSha256,
        action: replayableAction(call.action),
      }),
      binding.configSha256,
    );
  }
}

function bootstrapContext(value, binding) {
  object(value, [
    'names',
    'zoneId',
    'zoneName',
    'accountWorkersDevSubdomain',
    'dispatch',
  ]);
  const hostname = identifier(value.names?.referenceHostname, 253);
  const prefix = `${binding.resourcePrefix}-reference.`;
  if (!hostname.startsWith(prefix)) invalid();
  const ownedHostname = hostname.slice(prefix.length);
  const names = deriveDirectConformanceNames({
    resourcePrefix: binding.resourcePrefix,
    ownedHostname,
  });
  equalShape(value.names, names);
  const zoneId = identifier(value.zoneId);
  const zoneName = identifier(value.zoneName, 253);
  if (
    !/^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$/u.test(zoneName) ||
    zoneName
      .split('.')
      .some(
        (label) => !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/u.test(label),
      ) ||
    (ownedHostname !== zoneName && !ownedHostname.endsWith(`.${zoneName}`))
  )
    invalid();
  const subdomain = identifier(value.accountWorkersDevSubdomain);
  if (!/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/u.test(subdomain)) invalid();
  object(value.dispatch, ['kind', 'count']);
  const { kind, count } = value.dispatch;
  if (
    !Number.isSafeInteger(count) ||
    count < 0 ||
    count > 10_000 ||
    !['first-page-404', 'empty', 'enumerated'].includes(kind) ||
    (kind === 'enumerated' ? count === 0 : count !== 0)
  )
    invalid();
  return Object.freeze({
    names,
    zoneId,
    zoneName,
    accountWorkersDevSubdomain: subdomain,
    dispatch: Object.freeze({ kind, count }),
  });
}

const MUTATION_FIELD = Object.freeze({
  'create-fleet-d1': 'fleet',
  'create-quota-d1': 'quota',
  'create-export-r2': 'exports',
  'upload-reference': 'upload',
  'enable-reference-ingress': 'ingress',
});

function mutationField(kind) {
  if (typeof kind !== 'string' || !Object.hasOwn(MUTATION_FIELD, kind))
    invalid();
  return MUTATION_FIELD[kind];
}

function decodeBootstrap(value, binding, invocationCount, lastInvocation) {
  if (value === null) return null;
  object(value, [
    'context',
    'fleet',
    'quota',
    'exports',
    'upload',
    'active',
    'ingress',
    'controlReadOrdinal',
    'pending',
  ]);
  const context = bootstrapContext(value.context, binding);
  const result = { context };
  for (const [key, name] of [
    ['fleet', context.names.fleetDatabase],
    ['quota', context.names.quotaDatabase],
  ]) {
    const receipt = value[key];
    if (receipt === null) result[key] = null;
    else {
      object(receipt, ['uuid', 'name']);
      if (receipt.name !== name) invalid();
      result[key] = Object.freeze({ uuid: identifier(receipt.uuid), name });
    }
  }
  if (result.fleet && result.quota && result.fleet.uuid === result.quota.uuid)
    invalid();
  if (value.exports !== null) {
    object(value.exports, ['name', 'jurisdiction', 'creationDate']);
    const { name, jurisdiction, creationDate } = value.exports;
    if (
      name !== context.names.exportBucket ||
      jurisdiction !== 'default' ||
      typeof creationDate !== 'string' ||
      creationDate.length > 32 ||
      !Number.isFinite(Date.parse(creationDate)) ||
      new Date(creationDate).toISOString() !== creationDate
    )
      invalid();
    result.exports = Object.freeze({ name, jurisdiction, creationDate });
  } else result.exports = null;
  if (value.upload !== null) {
    object(value.upload, ['scriptName', 'tag', 'etag']);
    if (value.upload.scriptName !== context.names.referenceWorker) invalid();
    result.upload = Object.freeze({
      scriptName: value.upload.scriptName,
      tag: value.upload.tag === null ? null : identifier(value.upload.tag),
      etag: value.upload.etag === null ? null : identifier(value.upload.etag),
    });
  } else result.upload = null;
  if (value.active !== null) {
    object(value.active, ['deploymentId', 'versionId']);
    result.active = Object.freeze({
      deploymentId: identifier(value.active.deploymentId),
      versionId: identifier(value.active.versionId),
    });
  } else result.active = null;
  if (value.ingress !== null) {
    equalShape(value.ingress, { enabled: true, previewsEnabled: false });
    result.ingress = Object.freeze({ enabled: true, previewsEnabled: false });
  } else result.ingress = null;
  const ordinal = value.controlReadOrdinal;
  if (
    ordinal !== null &&
    (!Number.isSafeInteger(ordinal) ||
      ordinal < 1 ||
      ordinal > invocationCount ||
      (ordinal === invocationCount &&
        (lastInvocation?.state !== 'settled' ||
          lastInvocation.action.kind !== 'control-read')))
  )
    invalid();
  result.controlReadOrdinal = ordinal;
  const fields = [
    'fleet',
    'quota',
    'exports',
    'upload',
    'active',
    'ingress',
    'controlReadOrdinal',
  ];
  for (let index = 1; index < fields.length; index++) {
    if (result[fields[index]] !== null && result[fields[index - 1]] === null)
      invalid();
  }
  result.pending = value.pending;
  if (value.pending !== null) {
    const index = fields.indexOf(mutationField(value.pending));
    if (
      lastInvocation?.state === 'pending' ||
      (index > 0 && result[fields[index - 1]] === null) ||
      fields.slice(index + 1).some((field) => result[field] !== null)
    )
      invalid();
  }
  return Object.freeze(result);
}

const teardownText = (max) => (value) => {
  identifier(value, max);
  // Journal capacity is budgeted in bytes; `identifier` bounds UTF-16 units.
  if (Buffer.byteLength(value) > max) invalid();
  return value;
};
const teardownFlag = scenarioEnum(true, false);
const teardownSettleAttempts = (value) => {
  scenarioNumber(value);
  if (value < 1 || value > DIRECT_TEARDOWN_MAXIMA.settleAttempts) invalid();
  return value;
};
const teardownJurisdictions = (value) => {
  if (!Array.isArray(value) || value.length !== 1 || value[0] !== 'default')
    invalid();
  return Object.freeze(['default']);
};
const residualSurfaceShape = {
  prefixCount: scenarioNumber,
  prefixNames: boundedArray(
    teardownText(DIRECT_TEARDOWN_MAXIMA.nameBytes),
    DIRECT_TEARDOWN_MAXIMA.prefixNames,
  ),
  globalCount: nullable(scenarioNumber),
  exhaustive: teardownFlag,
};
const residualShape = {
  version: 1,
  surfaces: Object.fromEntries(
    DIRECT_RESIDUAL_SURFACES.map((surface) => [surface, residualSurfaceShape]),
  ),
  bucketJurisdictions: teardownJurisdictions,
  dispatch: {
    kind: scenarioEnum('first-page-404', 'empty', 'enumerated', 'fail-closed'),
    count: scenarioNumber,
    status: nullable(scenarioNumber),
    prefixCount: scenarioNumber,
  },
  versionsGone: nullable(teardownFlag),
  settleAttempts: teardownSettleAttempts,
};
const teardownSettlement = {
  ordinal: scenarioNumber,
  settledByReread: teardownFlag,
};
const teardownShape = {
  version: 1,
  phase: scenarioEnum(...DIRECT_TEARDOWN_PHASES),
  pending: nullable({
    kind: scenarioEnum(...DIRECT_TEARDOWN_MUTATIONS),
    key: optional(teardownText(DIRECT_TEARDOWN_MAXIMA.keyBytes)),
  }),
  receipts: {
    ingress: nullable(teardownSettlement),
    worker: nullable({
      scriptName: teardownText(DIRECT_TEARDOWN_MAXIMA.nameBytes),
      secretNames: boundedArray(
        teardownText(DIRECT_TEARDOWN_MAXIMA.nameBytes),
        DIRECT_TEARDOWN_MAXIMA.secretNames,
      ),
      ...teardownSettlement,
    }),
    fleet: nullable({
      uuid: teardownText(DIRECT_TEARDOWN_MAXIMA.nameBytes),
      ...teardownSettlement,
    }),
    quota: nullable({
      uuid: teardownText(DIRECT_TEARDOWN_MAXIMA.nameBytes),
      ...teardownSettlement,
    }),
    exportObjects: boundedArray(
      {
        key: teardownText(DIRECT_TEARDOWN_MAXIMA.keyBytes),
        ...teardownSettlement,
      },
      DIRECT_TEARDOWN_MAXIMA.exportObjects,
    ),
    exports: nullable({
      name: teardownText(DIRECT_TEARDOWN_MAXIMA.nameBytes),
      ...teardownSettlement,
    }),
  },
  residual: nullable(residualShape),
  providerRequests: scenarioNumber,
  failure: nullable(scenarioEnum(...DIRECT_TEARDOWN_FAILURES)),
};

function teardownReceiptSet(receipts, field) {
  return field === 'exportObjects'
    ? receipts.exportObjects.length > 0
    : receipts[field] !== null;
}

function teardownOrdinals(state) {
  return [
    ...TEARDOWN_RECEIPT_ORDER.filter((field) => field !== 'exportObjects')
      .map((field) => state.receipts[field]?.ordinal)
      .filter((ordinal) => ordinal !== undefined),
    ...state.receipts.exportObjects.map((entry) => entry.ordinal),
  ];
}

function decodeTeardown(value) {
  const result = scenarioShape(value, teardownShape);
  const { pending, receipts } = result;
  if (pending) {
    const keyed = pending.kind === 'delete-export-object';
    if (keyed !== Object.hasOwn(pending, 'key')) invalid();
    // One atomic write publishes a receipt and clears the pending that names
    // it, so this intermediate never reaches disk.
    if (
      keyed
        ? receipts.exportObjects.some((entry) => entry.key === pending.key)
        : receipts[TEARDOWN_RECEIPT_FIELD[pending.kind]] !== null
    )
      invalid();
  }
  const keys = new Set(receipts.exportObjects.map((entry) => entry.key));
  if (keys.size !== receipts.exportObjects.length) invalid();
  TEARDOWN_RECEIPT_ORDER.forEach((field, index) => {
    if (!teardownReceiptSet(receipts, field)) return;
    for (const earlier of TEARDOWN_RECEIPT_ORDER.slice(0, index))
      if (
        !(field === 'exports' && earlier === 'exportObjects') &&
        !teardownReceiptSet(receipts, earlier)
      )
        invalid();
  });
  if (
    result.phase === 'refused' &&
    (pending !== null ||
      result.failure === null ||
      TEARDOWN_RECEIPT_ORDER.some((field) =>
        teardownReceiptSet(receipts, field),
      ))
  )
    invalid();
  if (result.residual !== null && !RESIDUAL_PHASES.includes(result.phase))
    invalid();
  return result;
}

function assertPrivate(stat, directory) {
  if (
    stat.uid !== process.getuid() ||
    (stat.mode & 0o7777) !== (directory ? 0o700 : 0o600) ||
    (directory ? !stat.isDirectory() : !stat.isFile() || stat.nlink !== 1)
  )
    invalid();
}

function fileFlags(access) {
  return access | constants.O_NOFOLLOW | constants.O_NONBLOCK;
}

async function privateDirectory(path) {
  const handle = await open(
    path,
    fileFlags(constants.O_RDONLY | constants.O_DIRECTORY),
  );
  try {
    assertPrivate(await handle.stat(), true);
    return handle;
  } catch (error) {
    await handle.close();
    throw error;
  }
}

async function ensureBase(path) {
  try {
    await mkdir(path, { mode: 0o700 });
  } catch (error) {
    if (error.code !== 'EEXIST') throw error;
  }
  const handle = await privateDirectory(path);
  try {
    const parent = await open(
      dirname(path),
      constants.O_RDONLY | constants.O_DIRECTORY,
    );
    try {
      await parent.sync();
    } finally {
      await parent.close();
    }
    return handle;
  } catch (error) {
    await handle.close();
    throw error;
  }
}

async function acquireLock(path, base) {
  const handle = await open(
    path,
    fileFlags(constants.O_RDWR | constants.O_CREAT),
    0o600,
  );
  try {
    assertPrivate(await handle.stat(), false);
    const acquired = await new Promise((fulfill) => {
      const child = spawn(
        '/usr/bin/flock',
        ['--exclusive', '--nonblock', '3'],
        { stdio: ['ignore', 'ignore', 'ignore', handle.fd], env: {} },
      );
      let failed = false;
      child.once('error', () => {
        failed = true;
      });
      child.once('close', (status, signal) => {
        fulfill(!failed && status === 0 && signal === null);
      });
    });
    if (!acquired) throw new DirectRunStateError('lock-unavailable');
    await handle.sync();
    await base.sync();
    return handle;
  } catch (error) {
    await handle.close();
    throw error;
  }
}

async function readSnapshot(path, binding) {
  const handle = await open(path, fileFlags(constants.O_RDONLY));
  try {
    const stat = await handle.stat();
    assertPrivate(stat, false);
    if (stat.size < 1 || stat.size > DIRECT_RUN_MAX_JOURNAL_BYTES) invalid();
    const bytes = Buffer.alloc(stat.size);
    let offset = 0;
    while (offset < bytes.length) {
      const { bytesRead } = await handle.read(
        bytes,
        offset,
        bytes.length - offset,
        offset,
      );
      if (bytesRead === 0) invalid();
      offset += bytesRead;
    }
    if ((await handle.read(Buffer.alloc(1), 0, 1, offset)).bytesRead !== 0)
      invalid();
    return await decodeSnapshot(
      JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)),
      binding,
    );
  } finally {
    await handle.close();
  }
}

function serialize(snapshot) {
  return `${JSON.stringify(snapshot)}\n`;
}

async function writeSnapshot(directory, handle, snapshot) {
  const temporary = join(directory, `.journal-${randomUUID()}.tmp`);
  let file;
  let created = false;
  try {
    file = await open(
      temporary,
      fileFlags(constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL),
      0o600,
    );
    created = true;
    assertPrivate(await file.stat(), false);
    const serialized = serialize(snapshot);
    if (Buffer.byteLength(serialized) > DIRECT_RUN_MAX_JOURNAL_BYTES) invalid();
    await file.writeFile(serialized);
    await file.sync();
    await file.close();
    file = undefined;
    await rename(temporary, join(directory, 'journal.json'));
    created = false;
    await handle.sync();
  } finally {
    const cleanup = await Promise.allSettled([
      ...(file ? [file.close()] : []),
      ...(created ? [unlink(temporary)] : []),
    ]);
    if (cleanup.some((result) => result.status === 'rejected')) invalid();
  }
}

async function exists(path) {
  try {
    await lstat(path);
    return true;
  } catch (error) {
    if (error.code === 'ENOENT') return false;
    throw error;
  }
}

async function initializeRun(basePath, base, directory, binding, createdAt) {
  if (await exists(directory)) throw new DirectRunStateError('run-exists');
  const staging = join(
    basePath,
    `.${binding.resourcePrefix}-${randomUUID()}.tmp`,
  );
  await mkdir(staging, { mode: 0o700 });
  let handle;
  let published = false;
  try {
    handle = await privateDirectory(staging);
    const snapshot = Object.freeze({
      version: 2,
      createdAt,
      binding,
      invocationCount: 0,
      lastInvocation: null,
      bootstrap: null,
    });
    await writeSnapshot(staging, handle, snapshot);
    if (await exists(directory)) throw new DirectRunStateError('run-exists');
    await rename(staging, directory);
    published = true;
    await base.sync();
    return { handle, snapshot };
  } catch (error) {
    await handle?.close();
    if (!published) {
      if (await exists(join(staging, 'journal.json')))
        await unlink(join(staging, 'journal.json'));
      await rmdir(staging);
    }
    throw error;
  }
}

function runJournal(directory, directoryHandle, base, lock, initial) {
  const queue = new PQueue({ concurrency: 1 });
  let snapshot = initial;
  let poisoned = false;
  let closePromise;
  const enqueue = (operation) => {
    if (closePromise) return Promise.reject(new DirectRunStateError());
    return queue.add(async () => {
      try {
        if (poisoned) invalid();
        const current = await readSnapshot(
          join(directory, 'journal.json'),
          snapshot.binding,
        );
        if (JSON.stringify(current) !== JSON.stringify(snapshot)) invalid();
        return await operation();
      } catch (error) {
        throw stateError(error);
      }
    });
  };
  const publish = async (next) => {
    try {
      await writeSnapshot(directory, directoryHandle, next);
      snapshot = next;
    } catch (error) {
      poisoned = true;
      throw error;
    }
  };
  const publishSnapshot = async (fields) => {
    const next = await decodeSnapshot(
      { ...snapshot, ...fields },
      snapshot.binding,
    );
    await publish(next);
  };
  const publishBootstrap = (bootstrap) => publishSnapshot({ bootstrap });
  const assertSettled = () => {
    if (
      snapshot.lastInvocation?.state === 'pending' ||
      snapshot.bootstrap?.pending ||
      snapshot.teardown?.pending
    )
      throw new DirectRunStateError('outcome-unknown');
  };
  const teardownStarted = () =>
    Boolean(snapshot.teardown) &&
    TEARDOWN_RECEIPT_ORDER.some((field) =>
      teardownReceiptSet(snapshot.teardown.receipts, field),
    );
  const withinCapacity = async (fields) => {
    const next = await decodeSnapshot(
      { ...snapshot, ...fields },
      snapshot.binding,
    );
    if (Buffer.byteLength(serialize(next)) > DIRECT_RUN_MAX_JOURNAL_BYTES)
      invalid();
    return next;
  };
  return Object.freeze({
    directory,
    snapshot() {
      return snapshot;
    },
    recordScenario(value) {
      return enqueue(async () => {
        assertSettled();
        if (teardownStarted()) invalid();
        const scenario = decodeScenario(value, snapshot.invocationCount);
        await validateScenarioActions(scenario, snapshot.binding);
        if (!snapshot.bootstrap?.controlReadOrdinal) invalid();
        const previous = snapshot.scenario;
        if (previous) {
          if (
            scenario.startedOrdinal !== previous.startedOrdinal ||
            scenario.callCount < previous.callCount ||
            DIRECT_SCENARIO_PHASES.indexOf(scenario.phase) <
              DIRECT_SCENARIO_PHASES.indexOf(previous.phase) ||
            DIRECT_SCENARIO_PHASES.indexOf(scenario.phase) >
              DIRECT_SCENARIO_PHASES.indexOf(previous.phase) + 1
          )
            invalid();
          for (const kind of ['provider', 'maintenance', 'application'])
            if (scenario.attempts[kind] < previous.attempts[kind]) invalid();
          if (scenario.sdkRequests < previous.sdkRequests) invalid();
          for (const phase of DIRECT_SCENARIO_PHASES)
            if (scenario.phaseCalls[phase] < previous.phaseCalls[phase])
              invalid();
          for (const group of [
            'initial',
            'candidate',
            'final',
            'objects',
            'objectDeletions',
            'recoveryExportAbsent',
            'inventories',
            'audits',
            'decommission',
            'terminalForce',
          ])
            for (const [key, proof] of Object.entries(previous.proofs[group]))
              if (proof !== null)
                equalShape(scenario.proofs[group][key], proof);
          for (const key of ['cleanup', 'force', 'residual'])
            if (previous.proofs[key] !== null)
              equalShape(scenario.proofs[key], previous.proofs[key]);
          if (previous.failure) equalShape(scenario.failure, previous.failure);
          if (previous.proofs.restart) {
            const { resumedProcess, replayOrdinal, ...fixed } =
              previous.proofs.restart;
            for (const [key, expected] of Object.entries(fixed))
              equalShape(scenario.proofs.restart?.[key], expected);
            if (replayOrdinal !== null) {
              equalShape(scenario.proofs.restart.replayOrdinal, replayOrdinal);
              equalShape(
                scenario.proofs.restart.resumedProcess,
                resumedProcess,
              );
            }
          }
          for (const [group, later] of [
            ['drain', ['after', 'ordinal']],
            ['reopen', ['after', 'ordinal']],
            ['sweeps', ['second', 'intervalMs']],
            ['probes', []],
          ])
            for (const role of ['a', 'b']) {
              const entry = previous.proofs.fence[group][role];
              if (entry === null) continue;
              const fresh = scenario.proofs.fence[group][role];
              if (fresh === null) invalid();
              for (const [key, expected] of Object.entries(entry))
                if (!later.includes(key) || expected !== null)
                  equalShape(fresh[key], expected);
            }
          for (const role of ['a', 'b']) {
            if (previous.proofs.exports[role]) {
              const { sourceInvocationOrdinal, ...proof } =
                previous.proofs.exports[role];
              const { sourceInvocationOrdinal: freshOrdinal, ...freshProof } =
                scenario.proofs.exports[role] ?? {};
              equalShape(freshProof, proof);
              if (freshOrdinal < sourceInvocationOrdinal) invalid();
            }
          }
          for (const key of [
            'steps',
            'effects',
            'health',
            'exportVerifications',
          ]) {
            if (scenario.proofs[key].length < previous.proofs[key].length)
              invalid();
            previous.proofs[key].forEach((proof, index) => {
              equalShape(scenario.proofs[key][index], proof);
            });
          }
        }
        await publishSnapshot({ scenario });
      });
    },
    recordResume() {
      return enqueue(async () => {
        if (
          snapshot.lastInvocation?.state === 'pending' ||
          snapshot.bootstrap?.pending
        )
          throw new DirectRunStateError('outcome-unknown');
        const current = snapshot.resumeCount ?? 0;
        if (current >= DIRECT_RUN_MAX_RESUME_COUNT) return;
        await publishSnapshot({ resumeCount: current + 1 });
      });
    },
    recordTeardown(value) {
      return enqueue(async () => {
        // Receipts publish while their own mutation is still pending, so this
        // path checks the invocation and bootstrap gates without `assertSettled`.
        if (
          snapshot.lastInvocation?.state === 'pending' ||
          snapshot.bootstrap?.pending
        )
          throw new DirectRunStateError('outcome-unknown');
        const teardown = decodeTeardown(value);
        const previous = snapshot.teardown;
        if (previous) {
          const position = (phase) => DIRECT_TEARDOWN_PHASES.indexOf(phase);
          if (
            previous.phase === 'refused'
              ? teardown.phase !== 'refused'
              : teardown.phase === 'refused' ||
                position(teardown.phase) < position(previous.phase) ||
                position(teardown.phase) > position(previous.phase) + 1
          )
            invalid();
          if (
            teardown.providerRequests < previous.providerRequests ||
            Math.max(0, ...teardownOrdinals(teardown)) <
              Math.max(0, ...teardownOrdinals(previous))
          )
            invalid();
          for (const field of TEARDOWN_RECEIPT_ORDER) {
            if (field === 'exportObjects') {
              if (
                teardown.receipts.exportObjects.length <
                previous.receipts.exportObjects.length
              )
                invalid();
              previous.receipts.exportObjects.forEach((entry, index) => {
                equalShape(teardown.receipts.exportObjects[index], entry);
              });
            } else if (previous.receipts[field] !== null)
              equalShape(teardown.receipts[field], previous.receipts[field]);
          }
        }
        await publish(await withinCapacity({ teardown }));
      });
    },
    assertTeardownCapacity(worstCase) {
      return enqueue(async () => {
        await withinCapacity({ teardown: worstCase });
      });
    },
    bindBootstrapContext(context) {
      return enqueue(async () => {
        assertSettled();
        const checked = bootstrapContext(context, snapshot.binding);
        if (snapshot.bootstrap) {
          const { dispatch: _historical, ...previous } =
            snapshot.bootstrap.context;
          const { dispatch: _fresh, ...current } = checked;
          equalShape(current, previous);
          return;
        }
        if (snapshot.invocationCount !== 0) invalid();
        await publishBootstrap({
          context: checked,
          fleet: null,
          quota: null,
          exports: null,
          upload: null,
          active: null,
          ingress: null,
          controlReadOrdinal: null,
          pending: null,
        });
      });
    },
    beginBootstrapMutation(kind) {
      return enqueue(async () => {
        assertSettled();
        const field = mutationField(kind);
        if (!snapshot.bootstrap || snapshot.bootstrap[field] !== null)
          invalid();
        await publishBootstrap({ ...snapshot.bootstrap, pending: kind });
      });
    },
    confirmBootstrapMutation(value) {
      return enqueue(async () => {
        object(value, ['kind', 'receipt']);
        if (value.receipt === null) invalid();
        const field = mutationField(value.kind);
        if (
          !snapshot.bootstrap ||
          snapshot.bootstrap.pending !== value.kind ||
          snapshot.bootstrap[field] !== null
        )
          invalid();
        await publishBootstrap({
          ...snapshot.bootstrap,
          [field]: value.receipt,
        });
        await publishBootstrap({ ...snapshot.bootstrap, pending: null });
      });
    },
    recordBootstrapObservation(value) {
      return enqueue(async () => {
        assertSettled();
        const bootstrap = snapshot.bootstrap;
        if (!bootstrap) invalid();
        if (value?.kind === 'active') {
          object(value, ['kind', 'deploymentId', 'versionId']);
          const active = {
            deploymentId: value.deploymentId,
            versionId: value.versionId,
          };
          if (bootstrap.active) {
            equalShape(active, bootstrap.active);
            return;
          }
          await publishBootstrap({ ...bootstrap, active });
        } else if (value?.kind === 'control-read') {
          object(value, ['kind', 'ordinal']);
          const last = snapshot.lastInvocation;
          if (
            last?.state !== 'settled' ||
            last.action.kind !== 'control-read' ||
            last.ordinal !== value.ordinal
          )
            invalid();
          await publishBootstrap({
            ...bootstrap,
            controlReadOrdinal: value.ordinal,
          });
        } else invalid();
      });
    },
    reserveInvocation(serializedRequest) {
      return enqueue(async () => {
        assertSettled();
        if (teardownStarted()) invalid();
        if (snapshot.invocationCount >= snapshot.binding.maxInvocations)
          throw new DirectRunStateError('invocation-budget-exhausted');
        const request = await decodeRequest(
          serializedRequest,
          snapshot.binding.configSha256,
        );
        const reservation = Object.freeze({
          ordinal: snapshot.invocationCount + 1,
          requestSha256: createHash('sha256')
            .update(serializedRequest)
            .digest('hex'),
        });
        await publish(
          Object.freeze({
            ...snapshot,
            invocationCount: reservation.ordinal,
            lastInvocation: Object.freeze({
              ...reservation,
              action: actionSummary(request.action),
              state: 'pending',
            }),
          }),
        );
        return reservation;
      });
    },
    settleInvocation(reservation) {
      return enqueue(async () => {
        object(reservation, ['ordinal', 'requestSha256']);
        const last = snapshot.lastInvocation;
        if (
          !last ||
          reservation.ordinal !== last.ordinal ||
          reservation.requestSha256 !== last.requestSha256
        )
          invalid();
        if (last.state === 'settled') return;
        await publish(
          Object.freeze({
            ...snapshot,
            lastInvocation: Object.freeze({ ...last, state: 'settled' }),
          }),
        );
      });
    },
    close() {
      closePromise ??= (async () => {
        await queue.onIdle();
        const closed = await Promise.allSettled([
          directoryHandle.close(),
          base.close(),
          lock.close(),
        ]);
        if (closed.some((result) => result.status === 'rejected')) invalid();
      })();
      return closePromise;
    },
  });
}

async function attachRunState(input, modes) {
  let base;
  let lock;
  try {
    if (
      process.platform !== 'linux' ||
      typeof process.getuid !== 'function' ||
      !Number.isInteger(constants.O_NOFOLLOW) ||
      !Number.isInteger(constants.O_NONBLOCK) ||
      !Number.isInteger(constants.O_DIRECTORY)
    )
      throw new DirectRunStateError('lock-unavailable');
    if (!modes.includes(input.mode)) invalid();
    const binding = bindingFromInput(input);
    const basePath = join(
      dirname(resolve(input.configPath)),
      '.direct-conformance',
    );
    const directory = join(basePath, binding.resourcePrefix);
    base = await ensureBase(basePath);
    lock = await acquireLock(
      join(basePath, `${binding.resourcePrefix}.lock`),
      base,
    );
    return { binding, basePath, directory, base, lock };
  } catch (error) {
    await Promise.allSettled(
      [base, lock]
        .filter((handle) => handle !== undefined)
        .map((handle) => handle.close()),
    );
    throw stateError(error);
  }
}

export async function openDirectRunState(input) {
  const attached = await attachRunState(input, ['run', 'resume']);
  const { binding, basePath, directory, base, lock } = attached;
  let directoryHandle;
  try {
    let snapshot;
    if (input.mode === 'run') {
      const initialized = await initializeRun(
        basePath,
        base,
        directory,
        binding,
        runTimestamp(new Date(runClock(input.now)).toISOString()),
      );
      directoryHandle = initialized.handle;
      snapshot = initialized.snapshot;
    } else {
      if (!(await exists(directory)))
        throw new DirectRunStateError('run-missing');
      directoryHandle = await privateDirectory(directory);
      snapshot = await readSnapshot(join(directory, 'journal.json'), binding);
      if (
        snapshot.lastInvocation?.state === 'pending' ||
        snapshot.bootstrap?.pending
      )
        throw new DirectRunStateError('outcome-unknown');
    }
    return runJournal(directory, directoryHandle, base, lock, snapshot);
  } catch (error) {
    await Promise.allSettled(
      [directoryHandle, base, lock]
        .filter((handle) => handle !== undefined)
        .map((handle) => handle.close()),
    );
    throw stateError(error);
  }
}

export async function inspectDirectRunState(input) {
  const attached = await attachRunState(input, ['inspect']);
  let directoryHandle;
  try {
    if (!(await exists(attached.directory)))
      throw new DirectRunStateError('run-missing');
    directoryHandle = await privateDirectory(attached.directory);
    const snapshot = await readSnapshot(
      join(attached.directory, 'journal.json'),
      attached.binding,
    );
    let closePromise;
    return Object.freeze({
      snapshot,
      close() {
        closePromise ??= (async () => {
          const closed = await Promise.allSettled([
            directoryHandle.close(),
            attached.base.close(),
            attached.lock.close(),
          ]);
          if (closed.some((result) => result.status === 'rejected')) invalid();
        })();
        return closePromise;
      },
    });
  } catch (error) {
    await Promise.allSettled(
      [directoryHandle, attached.base, attached.lock]
        .filter((handle) => handle !== undefined)
        .map((handle) => handle.close()),
    );
    throw stateError(error);
  }
}
