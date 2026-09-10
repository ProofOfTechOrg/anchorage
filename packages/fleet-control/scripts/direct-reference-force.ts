// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import {
  type CleanupTerminalReceipt,
  type FleetRecord,
  type FleetStateLease,
  forceDecommissionDeployment,
} from '@proofoftech/fleet-control';
import { deploymentSpecDigest } from '@proofoftech/fleet-control/cloudflare-control-plane';
import type { DirectRunManifest } from './direct-credentialed-conformance-preflight.mjs';
import type { DirectReferenceContext } from './direct-reference-context.js';
import { DirectReferenceExecutionError } from './direct-reference-http.js';
import { DirectReferenceJournalError } from './direct-reference-journal.js';
import {
  readFrozenLifecycleSpec,
  readHistoricalRecoveryReceipt,
} from './direct-reference-lifecycle.js';
import {
  type DirectResourceIdentity,
  directResourceObservation,
  recordDirectResource,
} from './direct-reference-observations.js';

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new DirectReferenceJournalError();
  return value as Record<string, unknown>;
}

function digest(value: unknown): string {
  if (typeof value !== 'string' || !/^[a-f0-9]{64}$/u.test(value))
    throw new DirectReferenceJournalError();
  return value;
}

function text(value: unknown): string {
  if (typeof value !== 'string' || !value || value !== value.trim())
    throw new DirectReferenceJournalError();
  return value;
}

function array(value: unknown): unknown[] {
  if (!Array.isArray(value)) throw new DirectReferenceJournalError();
  return value;
}

function forceResource(context: DirectReferenceContext, encoded: string) {
  const value = object(JSON.parse(encoded));
  const database = object(value.database);
  const spec = context.spec('recovery', 'initial');
  const identity = {
    version: 1,
    role: 'recovery',
    backend: 'plain-worker',
    tenantTag: spec.tenantTag,
    environment: spec.environment,
    scriptName: spec.scriptName,
    database: { name: spec.databaseName, id: text(database.id) },
    knownVersionIds: [
      ...new Set(array(value.knownVersionIds).map(text)),
    ].sort(),
    localNamespaces: array(value.localNamespaces).map((entry) => {
      const binding = object(entry);
      return {
        name: text(binding.name),
        className: text(binding.className),
        namespaceId: text(binding.namespaceId),
      };
    }),
    applicationBuckets: array(value.applicationBuckets).map((entry) => {
      const bucket = object(entry);
      const jurisdiction = bucket.jurisdiction;
      if (
        jurisdiction !== 'default' &&
        jurisdiction !== 'eu' &&
        jurisdiction !== 'fedramp'
      )
        throw new DirectReferenceJournalError();
      const creationDate = text(bucket.creationDate);
      if (
        !Number.isFinite(Date.parse(creationDate)) ||
        new Date(creationDate).toISOString() !== creationDate
      )
        throw new DirectReferenceJournalError();
      return {
        name: text(bucket.name),
        bucketName: text(bucket.bucketName),
        jurisdiction,
        reservationNonce: text(bucket.reservationNonce),
        creationDate,
      };
    }),
  } satisfies DirectResourceIdentity;
  if (
    JSON.stringify(identity) !== encoded ||
    identity.database.id.startsWith('reserved-') ||
    identity.knownVersionIds.length === 0 ||
    identity.knownVersionIds.includes('pending')
  )
    throw new DirectReferenceJournalError();
  return identity;
}

function fulfilled<T>(result: PromiseSettledResult<T>): T {
  if (result.status === 'rejected') throw result.reason;
  return result.value;
}

function receiptDigest(receipt: CleanupTerminalReceipt): string {
  if (!Number.isSafeInteger(receipt.completedAtMs))
    throw new DirectReferenceJournalError();
  const evidence = receipt.evidence;
  return createHash('sha256')
    .update(
      JSON.stringify([
        receipt.version,
        receipt.operationId,
        receipt.tenantTag,
        receipt.environment,
        receipt.backend,
        receipt.scriptName,
        receipt.databaseId,
        receipt.databaseName,
        receipt.authority,
        receipt.admittedPhase,
        receipt.disposition,
        evidence.eligibility,
        evidence.ingressRemoved,
        evidence.workerAbsent,
        evidence.platformResourcesAbsent,
        evidence.applicationR2Settled,
        evidence.databaseAbsentReadback,
        evidence.scan
          ? [
              evidence.scan.discover.evidenceSha256,
              evidence.scan.discover.evidenceCount,
              evidence.scan.verify.evidenceSha256,
              evidence.scan.verify.evidenceCount,
            ]
          : null,
        receipt.completedAtMs,
      ]),
    )
    .digest('hex');
}

async function beforeIdentity(
  context: DirectReferenceContext,
  beforeIdentitySha256: string,
  receiptSha256: string,
) {
  const stored = await context.journal.readOperation('cleanup-recovery');
  if (!stored?.operationId)
    throw new DirectReferenceJournalError('prerequisite-unavailable');
  const spec = readFrozenLifecycleSpec(context, stored, 'recovery');
  if (spec !== context.spec('recovery', 'failed-recovery'))
    throw new DirectReferenceJournalError();
  return {
    version: 1 as const,
    role: 'recovery' as const,
    beforeIdentitySha256,
    priorCleanup: {
      slot: 'cleanup-recovery' as const,
      operationId: stored.operationId,
      requestedSpecDigest: deploymentSpecDigest(spec),
      receiptSha256,
    },
  };
}

async function readBefore(context: DirectReferenceContext) {
  const stored = await context.journal.readForceBefore();
  if (!stored) return undefined;
  const value = object(JSON.parse(stored.identityJson));
  const prior = object(value.priorCleanup);
  const identity = await beforeIdentity(
    context,
    digest(value.beforeIdentitySha256),
    digest(prior.receiptSha256),
  );
  if (stored.identityJson !== JSON.stringify(identity))
    throw new DirectReferenceJournalError();
  const resource = await context.journal.readResource(
    'recovery',
    identity.beforeIdentitySha256,
  );
  if (!resource) throw new DirectReferenceJournalError();
  return { identity, resource };
}

export async function recoverDirectForce(
  context: DirectReferenceContext,
  manifest: DirectRunManifest,
) {
  const names = manifest.names.roles.recovery;
  const plane = context.createForcePlane();
  const underLease = plane.store.withDeploymentLease.bind(plane.store);
  let observed: Awaited<ReturnType<typeof readBefore>>;
  const capture = async (
    record: FleetRecord | undefined,
    lease: FleetStateLease,
  ) => {
    context.transport.assertWithinBudget();
    await lease.assertOwned();
    observed = await readBefore(context);
    if (!record && !observed)
      throw new DirectReferenceJournalError('prerequisite-unavailable');
    const history = await readHistoricalRecoveryReceipt(context);
    const historyDigest = receiptDigest(history.receipt);
    if (
      observed &&
      historyDigest !== observed.identity.priorCleanup.receiptSha256
    )
      throw new DirectReferenceJournalError();
    if (record) {
      if (
        context.roleFor(record) !== 'recovery' ||
        context.specFor(record) !== context.spec('recovery', 'initial') ||
        record.cleanupIntent ||
        record.decommissionIntent ||
        record.pendingSpecDigest ||
        record.pendingArtifactVersion ||
        ![
          'ready',
          'decommissioning',
          'traffic-removed',
          'credentials-revoked',
          'database-deleting',
          'decommissioned',
        ].includes(record.phase)
      )
        throw new DirectReferenceExecutionError();
      if (observed) {
        if (
          directResourceObservation(context, record, 'before-force')
            .identityJson !== observed.resource.identityJson
        )
          throw new DirectReferenceExecutionError();
      } else {
        const initial = await context.journal.readOperation(
          'cleanup-recovery-initial',
        );
        if (
          record.phase !== 'ready' ||
          record.artifactVersion === 'pending' ||
          !record.artifactVersion ||
          record.databaseId.startsWith('reserved-') ||
          !initial ||
          initial.operationId !== null ||
          initial.tokenJson !== null ||
          readFrozenLifecycleSpec(context, initial, 'recovery') !==
            context.spec('recovery', 'initial') ||
          (record.applicationResources ?? []).some(
            (resource) =>
              resource.state !== 'created' || !resource.creationDate,
          )
        )
          throw new DirectReferenceJournalError('prerequisite-unavailable');
        const resource = await recordDirectResource(
          context,
          record,
          'before-force',
        );
        const identity = await beforeIdentity(
          context,
          resource.identitySha256,
          historyDigest,
        );
        await context.journal.recordForceBefore(
          JSON.stringify(identity),
          JSON.stringify({
            phase: record.phase,
            recordUpdatedAt: record.updatedAt,
            capturedAtMs: Date.now(),
          }),
        );
        observed = { identity, resource };
      }
    }
    await lease.assertOwned();
    context.transport.assertWithinBudget();
  };
  plane.store.withDeploymentLease = (tenantTag, environment, operation) => {
    if (tenantTag !== names.tenantTag || environment !== manifest.environment)
      throw new DirectReferenceExecutionError();
    return underLease(tenantTag, environment, async (lease) => {
      await capture(await plane.store.get(tenantTag, environment), lease);
      return operation(lease);
    });
  };
  await forceDecommissionDeployment({
    backend: plane.backend,
    store: plane.store,
    tenantTag: names.tenantTag,
    environment: manifest.environment,
  });
  if (!observed) throw new DirectReferenceJournalError();
  return {
    returned: true,
    beforeIdentitySha256: observed.identity.beforeIdentitySha256,
  };
}

export async function observeDirectForce(context: DirectReferenceContext) {
  context.transport.assertWithinBudget();
  const before = await readBefore(context);
  if (!before)
    throw new DirectReferenceJournalError('prerequisite-unavailable');
  const retained = await context.journal.readForceAfter();
  if (retained) {
    const observation = object(JSON.parse(retained.identityJson));
    if (
      observation.version !== 1 ||
      observation.role !== 'recovery' ||
      observation.beforeIdentitySha256 !== before.identity.beforeIdentitySha256
    )
      throw new DirectReferenceJournalError();
    return {
      observation,
      provenance: object(JSON.parse(retained.provenanceJson)),
    };
  }
  const resource = forceResource(context, before.resource.identityJson);
  const { client } = context.createForcePlane();
  const startedAtMs = Date.now();
  const reads = await Promise.allSettled([
    client.getDatabase(resource.database.id),
    client.inspectOrdinaryWorkerFootprint(resource.scriptName),
    client.listOrdinaryWorkerSecretNames(resource.scriptName),
    client.listOrdinaryWorkerVersions(resource.scriptName),
    client.listDurableObjectNamespaces(resource.scriptName),
    client.existingDurableObjectNamespaceIds(
      resource.localNamespaces.map((binding) => binding.namespaceId),
    ),
    context.control.getDeployment(resource.tenantTag, resource.environment),
    context.recoveryClaimSetPresent(),
    context.control.readCleanupReceipt(
      before.identity.priorCleanup.operationId,
    ),
    Promise.allSettled(
      resource.applicationBuckets.map((bucket) =>
        client.getR2Bucket(bucket.bucketName, bucket.jurisdiction),
      ),
    ),
  ] as const);
  context.transport.assertWithinBudget();
  const database = fulfilled(reads[0]);
  if (database && database.id !== resource.database.id)
    throw new DirectReferenceExecutionError();
  const footprint = fulfilled(reads[1]);
  const secretNames = fulfilled(reads[2]);
  const versions = fulfilled(reads[3]);
  const namespaces = fulfilled(reads[4]);
  const surviving = fulfilled(reads[5]);
  const record = fulfilled(reads[6]);
  const claims = fulfilled(reads[7]);
  const receipt = fulfilled(reads[8]);
  const bucketReads = fulfilled(reads[9]);
  const observedReceiptSha256 = receipt ? receiptDigest(receipt) : null;
  const observation = {
    version: 1,
    role: 'recovery',
    beforeIdentitySha256: before.identity.beforeIdentitySha256,
    fleetRecordPresent: record !== undefined,
    deploymentClaimsPresent: claims,
    database: {
      id: resource.database.id,
      expectedName: resource.database.name,
      observedName: database ? text(database.name) : null,
    },
    worker: {
      scriptName: resource.scriptName,
      scriptPresent: footprint.scriptPresent,
      workersDevEnabled: footprint.workersDevEnabled ?? null,
      previewUrlsEnabled: footprint.previewUrlsEnabled ?? null,
      customDomains: footprint.customDomains
        .map((domain) => ({
          id: text(domain.id),
          hostname: text(domain.hostname),
          service: text(domain.service),
        }))
        .sort((a, b) => a.id.localeCompare(b.id)),
      zoneRoutes: footprint.zoneRoutes
        .map((route) => ({
          zoneId: text(route.zoneId),
          routeId: text(route.routeId),
          pattern: text(route.pattern),
        }))
        .sort(
          (a, b) =>
            a.zoneId.localeCompare(b.zoneId) ||
            a.routeId.localeCompare(b.routeId),
        ),
      currentSecretNames: [...new Set(secretNames.map(text))].sort(),
      currentVersionIds: versions
        ? [
            ...new Set(versions.map((version) => text(version.versionId))),
          ].sort()
        : null,
      currentNamespaceIds: [...new Set(namespaces.map(text))].sort(),
      survivingRecordedNamespaceIds: [...new Set(surviving.map(text))].sort(),
    },
    buckets: resource.applicationBuckets.map((bucket, index) => {
      const result = bucketReads[index];
      if (!result) throw new DirectReferenceExecutionError();
      const observed = fulfilled(result);
      return {
        bindingName: bucket.name,
        bucketName: bucket.bucketName,
        jurisdiction: bucket.jurisdiction,
        expectedCreationDate: bucket.creationDate,
        observedCreationDate: observed ? text(observed.creationDate) : null,
      };
    }),
    priorCleanup: {
      operationId: before.identity.priorCleanup.operationId,
      observedReceiptSha256,
      matchesBefore:
        observedReceiptSha256 === before.identity.priorCleanup.receiptSha256,
    },
  };
  const stored = await context.journal.recordForceAfter(
    JSON.stringify(observation),
    JSON.stringify({ startedAtMs, completedAtMs: Date.now() }),
  );
  context.transport.assertWithinBudget();
  return {
    observation: object(JSON.parse(stored.identityJson)),
    provenance: object(JSON.parse(stored.provenanceJson)),
  };
}
