// SPDX-License-Identifier: Apache-2.0

import {
  type ApplicationR2Binding,
  type CleanupTerminalReceipt,
  type DatabaseReference,
  type ExternalMutationFence,
  type ExternalReleaseSnapshot,
  type FleetRecord,
  type FleetStateLease,
  forceDecommissionDeployment,
  type R2Jurisdiction,
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
import { directCleanupReceiptDigest } from './direct-reference-receipt.mjs';

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

function boolean(value: unknown): boolean {
  if (typeof value !== 'boolean') throw new DirectReferenceJournalError();
  return value;
}

function date(value: unknown): string {
  const result = text(value);
  if (
    !Number.isFinite(Date.parse(result)) ||
    new Date(result).toISOString() !== result
  )
    throw new DirectReferenceJournalError();
  return result;
}

function strings(value: unknown): string[] {
  return [...new Set(array(value).map(text))].sort();
}

function compare(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
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
    knownVersionIds: strings(value.knownVersionIds),
    localNamespaces: array(value.localNamespaces)
      .map((entry) => {
        const binding = object(entry);
        return {
          name: text(binding.name),
          className: text(binding.className),
          namespaceId: text(binding.namespaceId),
        };
      })
      .sort((left, right) => compare(left.name, right.name)),
    applicationBuckets: array(value.applicationBuckets)
      .map((entry) => {
        const bucket = object(entry);
        const jurisdiction = bucket.jurisdiction;
        if (
          jurisdiction !== 'default' &&
          jurisdiction !== 'eu' &&
          jurisdiction !== 'fedramp'
        )
          throw new DirectReferenceJournalError();
        return {
          name: text(bucket.name),
          bucketName: text(bucket.bucketName),
          jurisdiction,
          reservationNonce: text(bucket.reservationNonce),
          creationDate: date(bucket.creationDate),
        } satisfies DirectResourceIdentity['applicationBuckets'][number];
      })
      .sort((left, right) => compare(left.name, right.name)),
  } satisfies DirectResourceIdentity;
  if (
    JSON.stringify(identity) !== encoded ||
    identity.database.id.startsWith('reserved-') ||
    identity.knownVersionIds.length === 0 ||
    identity.knownVersionIds.includes('pending') ||
    new Set(identity.localNamespaces.map((binding) => binding.namespaceId))
      .size !== identity.localNamespaces.length ||
    new Set(identity.applicationBuckets.map((bucket) => bucket.bucketName))
      .size !== identity.applicationBuckets.length ||
    JSON.stringify(
      identity.localNamespaces.map(({ name, className }) => ({
        name,
        className,
      })),
    ) !==
      JSON.stringify(
        spec.durableObjectBindings
          .map(({ name, className }) => ({ name, className }))
          .sort((left, right) => compare(left.name, right.name)),
      ) ||
    JSON.stringify(
      identity.applicationBuckets.map(({ name, jurisdiction }) => ({
        name,
        jurisdiction,
      })),
    ) !==
      JSON.stringify(
        (spec.application?.r2Buckets ?? [])
          .map(({ name, jurisdiction }) => ({
            name,
            jurisdiction: jurisdiction ?? 'default',
          }))
          .sort((left, right) => compare(left.name, right.name)),
      )
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
  return directCleanupReceiptDigest(receipt);
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

type ForcePlane = ReturnType<DirectReferenceContext['createForcePlane']>;

/**
 * Refuses a lease on any deployment but `tenantTag`/`environment`, and hands
 * `capture` the record read under the held lease before the caller's own
 * operation runs.
 */
function interceptDeploymentLease(
  plane: ForcePlane,
  tenantTag: string,
  environment: string,
  capture: (
    record: FleetRecord | undefined,
    lease: FleetStateLease,
  ) => Promise<void>,
): void {
  const underLease = plane.store.withDeploymentLease.bind(plane.store);
  plane.store.withDeploymentLease = (
    leasedTag,
    leasedEnvironment,
    operation,
  ) => {
    if (leasedTag !== tenantTag || leasedEnvironment !== environment)
      throw new DirectReferenceExecutionError();
    return underLease(leasedTag, leasedEnvironment, async (lease) => {
      await capture(await plane.store.get(leasedTag, leasedEnvironment), lease);
      return operation(lease);
    });
  };
}

export async function forceDirectTerminal(
  context: DirectReferenceContext,
  manifest: DirectRunManifest,
  role: 'a',
): Promise<{
  returned: true;
  before: { databaseId: string; scriptName: string } | null;
  after: { present: false };
}> {
  const names = manifest.names.roles[role];
  const plane = context.createForcePlane();
  let before: { databaseId: string; scriptName: string } | null = null;
  interceptDeploymentLease(
    plane,
    names.tenantTag,
    manifest.environment,
    async (record) => {
      if (!record) return;
      if (record.phase !== 'decommissioned' || context.roleFor(record) !== role)
        throw new DirectReferenceExecutionError();
      before = {
        databaseId: record.databaseId,
        scriptName: record.scriptName,
      };
    },
  );
  await forceDecommissionDeployment({
    backend: plane.backend,
    store: plane.store,
    tenantTag: names.tenantTag,
    environment: manifest.environment,
  });
  if (await plane.store.get(names.tenantTag, manifest.environment))
    throw new DirectReferenceExecutionError();
  return { returned: true, before, after: { present: false } };
}

export async function recoverDirectForce(
  context: DirectReferenceContext,
  manifest: DirectRunManifest,
) {
  const names = manifest.names.roles.recovery;
  const plane = context.createForcePlane();
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
  interceptDeploymentLease(
    plane,
    names.tenantTag,
    manifest.environment,
    capture,
  );
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

type ForceBefore = NonNullable<Awaited<ReturnType<typeof readBefore>>>;
type ForceResource = ReturnType<typeof forceResource>;

interface DirectForceFootprint {
  readonly version: 1;
  readonly role: 'recovery';
  readonly beforeIdentitySha256: string;
  readonly fleetRecordPresent: boolean;
  readonly deploymentClaimsPresent: boolean;
  readonly database: Readonly<{
    id: string;
    expectedName: string;
    observedName: string | null;
  }>;
  readonly worker: Readonly<{
    scriptName: string;
    scriptPresent: boolean;
    workersDevEnabled: boolean | null;
    previewUrlsEnabled: boolean | null;
    customDomains: readonly Readonly<{
      id: string;
      hostname: string;
      service: string;
    }>[];
    zoneRoutes: readonly Readonly<{
      zoneId: string;
      routeId: string;
      pattern: string;
    }>[];
    currentSecretNames: readonly string[];
    currentVersionIds: readonly string[] | null;
    currentNamespaceIds: readonly string[];
    survivingRecordedNamespaceIds: readonly string[];
  }>;
  readonly buckets: readonly Readonly<{
    bindingName: string;
    bucketName: string;
    jurisdiction: R2Jurisdiction;
    expectedCreationDate: string;
    observedCreationDate: string | null;
  }>[];
  readonly priorCleanup: Readonly<{
    operationId: string;
    observedReceiptSha256: string | null;
    matchesBefore: boolean;
  }>;
}

type DirectForceReading = Readonly<{
  observation: DirectForceFootprint;
  provenance: Readonly<{ startedAtMs: number; completedAtMs: number }>;
}>;

function decodeForceFootprint(
  encoded: string,
  before: ForceBefore,
  resource: ForceResource,
): DirectForceFootprint {
  const value = object(JSON.parse(encoded));
  const database = object(value.database);
  const worker = object(value.worker);
  const buckets = array(value.buckets);
  const priorCleanup = object(value.priorCleanup);
  const observation: DirectForceFootprint = {
    version: 1,
    role: 'recovery',
    beforeIdentitySha256: before.identity.beforeIdentitySha256,
    fleetRecordPresent: boolean(value.fleetRecordPresent),
    deploymentClaimsPresent: boolean(value.deploymentClaimsPresent),
    database: {
      id: resource.database.id,
      expectedName: resource.database.name,
      observedName:
        database.observedName === null ? null : text(database.observedName),
    },
    worker: {
      scriptName: resource.scriptName,
      scriptPresent: boolean(worker.scriptPresent),
      workersDevEnabled:
        worker.workersDevEnabled === null
          ? null
          : boolean(worker.workersDevEnabled),
      previewUrlsEnabled:
        worker.previewUrlsEnabled === null
          ? null
          : boolean(worker.previewUrlsEnabled),
      customDomains: array(worker.customDomains)
        .map((entry) => {
          const domain = object(entry);
          return {
            id: text(domain.id),
            hostname: text(domain.hostname),
            service: text(domain.service),
          };
        })
        .sort((left, right) => left.id.localeCompare(right.id)),
      zoneRoutes: array(worker.zoneRoutes)
        .map((entry) => {
          const route = object(entry);
          return {
            zoneId: text(route.zoneId),
            routeId: text(route.routeId),
            pattern: text(route.pattern),
          };
        })
        .sort(
          (left, right) =>
            left.zoneId.localeCompare(right.zoneId) ||
            left.routeId.localeCompare(right.routeId),
        ),
      currentSecretNames: strings(worker.currentSecretNames),
      currentVersionIds:
        worker.currentVersionIds === null
          ? null
          : strings(worker.currentVersionIds),
      currentNamespaceIds: strings(worker.currentNamespaceIds),
      survivingRecordedNamespaceIds: strings(
        worker.survivingRecordedNamespaceIds,
      ),
    },
    buckets: resource.applicationBuckets.map((bucket, index) => {
      const observed = object(buckets[index]);
      return {
        bindingName: bucket.name,
        bucketName: bucket.bucketName,
        jurisdiction: bucket.jurisdiction,
        expectedCreationDate: bucket.creationDate,
        observedCreationDate:
          observed.observedCreationDate === null
            ? null
            : date(observed.observedCreationDate),
      };
    }),
    priorCleanup: {
      operationId: before.identity.priorCleanup.operationId,
      observedReceiptSha256:
        priorCleanup.observedReceiptSha256 === null
          ? null
          : digest(priorCleanup.observedReceiptSha256),
      matchesBefore: boolean(priorCleanup.matchesBefore),
    },
  };
  if (
    JSON.stringify(observation) !== encoded ||
    new Set(observation.worker.customDomains.map((domain) => domain.id))
      .size !== observation.worker.customDomains.length ||
    new Set(
      observation.worker.zoneRoutes.map((route) =>
        JSON.stringify([route.zoneId, route.routeId]),
      ),
    ).size !== observation.worker.zoneRoutes.length ||
    observation.worker.currentVersionIds?.includes('pending') ||
    observation.priorCleanup.matchesBefore !==
      (observation.priorCleanup.observedReceiptSha256 ===
        before.identity.priorCleanup.receiptSha256)
  )
    throw new DirectReferenceJournalError();
  return observation;
}

function decodeForceProvenance(
  encoded: string,
): DirectForceReading['provenance'] {
  const value = object(JSON.parse(encoded));
  const milliseconds = (input: unknown): number => {
    if (typeof input !== 'number' || !Number.isSafeInteger(input) || input < 0)
      throw new DirectReferenceJournalError();
    return input;
  };
  const provenance = {
    startedAtMs: milliseconds(value.startedAtMs),
    completedAtMs: milliseconds(value.completedAtMs),
  };
  if (JSON.stringify(provenance) !== encoded)
    throw new DirectReferenceJournalError();
  return provenance;
}

async function readForceFootprint(
  context: Pick<
    DirectReferenceContext,
    'transport' | 'recoveryClaimSetPresent'
  >,
  before: ForceBefore,
  resource: ForceResource,
  plane: Pick<ForcePlane, 'client' | 'store'>,
): Promise<DirectForceReading> {
  context.transport.assertWithinBudget();
  const { client, store } = plane;
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
    store.get(resource.tenantTag, resource.environment),
    context.recoveryClaimSetPresent(),
    store.readCleanupReceipt(before.identity.priorCleanup.operationId),
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
  const observation: DirectForceFootprint = {
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
        observedCreationDate: observed ? date(observed.creationDate) : null,
      };
    }),
    priorCleanup: {
      operationId: before.identity.priorCleanup.operationId,
      observedReceiptSha256,
      matchesBefore:
        observedReceiptSha256 === before.identity.priorCleanup.receiptSha256,
    },
  };
  context.transport.assertWithinBudget();
  return {
    observation: decodeForceFootprint(
      JSON.stringify(observation),
      before,
      resource,
    ),
    provenance: decodeForceProvenance(
      JSON.stringify({ startedAtMs, completedAtMs: Date.now() }),
    ),
  };
}

export async function observeDirectForce(context: DirectReferenceContext) {
  context.transport.assertWithinBudget();
  const before = await readBefore(context);
  if (!before)
    throw new DirectReferenceJournalError('prerequisite-unavailable');
  const resource = forceResource(context, before.resource.identityJson);
  let stored = await context.journal.readForceAfter();
  if (!stored) {
    const reading = await readForceFootprint(
      context,
      before,
      resource,
      context.createForcePlane(),
    );
    stored = await context.journal.recordForceAfter(
      JSON.stringify(reading.observation),
      JSON.stringify(reading.provenance),
    );
  }
  const result = {
    observation: decodeForceFootprint(stored.identityJson, before, resource),
    provenance: decodeForceProvenance(stored.provenanceJson),
  };
  context.transport.assertWithinBudget();
  return result;
}

function assertForceRecoveryState(observation: DirectForceFootprint): void {
  const { worker } = observation;
  if (
    observation.fleetRecordPresent ||
    observation.deploymentClaimsPresent ||
    observation.database.observedName !== null ||
    !observation.priorCleanup.matchesBefore ||
    worker.customDomains.length > 0 ||
    worker.zoneRoutes.length > 0 ||
    worker.currentSecretNames.length > 0 ||
    (worker.scriptPresent
      ? worker.workersDevEnabled !== false ||
        worker.previewUrlsEnabled !== false
      : worker.workersDevEnabled === true ||
        worker.previewUrlsEnabled === true) ||
    observation.buckets.some(
      (bucket) =>
        bucket.observedCreationDate !== null &&
        bucket.observedCreationDate !== bucket.expectedCreationDate,
    )
  )
    throw new DirectReferenceExecutionError();
}

function assertRetainedForceWorker(
  observation: DirectForceFootprint,
  resource: ForceResource,
): void {
  const { worker } = observation;
  const namespaces = resource.localNamespaces
    .map((binding) => binding.namespaceId)
    .sort();
  if (
    !worker.scriptPresent ||
    !worker.currentVersionIds?.length ||
    !resource.knownVersionIds.some((id) =>
      worker.currentVersionIds?.includes(id),
    ) ||
    JSON.stringify(worker.currentNamespaceIds) !== JSON.stringify(namespaces) ||
    JSON.stringify(worker.survivingRecordedNamespaceIds) !==
      JSON.stringify(namespaces) ||
    observation.buckets.some(
      (bucket) => bucket.observedCreationDate !== bucket.expectedCreationDate,
    )
  )
    throw new DirectReferenceExecutionError();
}

function assertForceWorkerAbsent(observation: DirectForceFootprint): void {
  const { worker } = observation;
  if (
    worker.scriptPresent ||
    (worker.currentVersionIds?.length ?? 0) > 0 ||
    worker.currentNamespaceIds.length > 0 ||
    worker.survivingRecordedNamespaceIds.length > 0
  )
    throw new DirectReferenceExecutionError();
}

async function assertForceAttachments(
  context: Pick<DirectReferenceContext, 'transport'>,
  plane: Pick<ForcePlane, 'client'>,
  resource: ForceResource,
  allowRetainedWorker: boolean,
): Promise<void> {
  const reads = await Promise.allSettled([
    plane.client.listWorkerDatabaseAttachments(resource.database.id),
    ...resource.applicationBuckets.map((bucket) =>
      plane.client.listWorkerR2Attachments(bucket.bucketName),
    ),
  ]);
  context.transport.assertWithinBudget();
  for (const read of reads) {
    if (
      fulfilled(read).some(
        (attachment) =>
          !allowRetainedWorker ||
          attachment.plane !== 'ordinary' ||
          attachment.scriptName !== resource.scriptName ||
          attachment.dispatchNamespace !== undefined,
      )
    )
      throw new DirectReferenceExecutionError();
  }
}

export async function recoverDirectForceResidual(
  context: DirectReferenceContext,
) {
  context.transport.assertWithinBudget();
  const spec = context.spec('recovery', 'initial');
  const plane = context.createForcePlane();
  return plane.store.withDeploymentLease(
    spec.tenantTag,
    spec.environment,
    async (lease) => {
      await lease.assertOwned();
      const before = await readBefore(context);
      if (!before)
        throw new DirectReferenceJournalError('prerequisite-unavailable');
      const resource = forceResource(context, before.resource.identityJson);
      const buckets: ApplicationR2Binding[] = resource.applicationBuckets.map(
        ({ name, bucketName, jurisdiction, creationDate }) => ({
          name,
          bucketName,
          jurisdiction,
          creationDate,
        }),
      );
      const fence: ExternalMutationFence = {
        mutationLeaseTtlMs: lease.mutationLeaseTtlMs,
        async assertOwned() {
          await lease.assertOwned();
          context.transport.assertWithinBudget();
          const reads = await Promise.allSettled([
            plane.store.get(resource.tenantTag, resource.environment),
            context.recoveryResidualClaimsPresent(
              resource.scriptName,
              buckets.map((bucket) => bucket.bucketName),
            ),
          ] as const);
          context.transport.assertWithinBudget();
          if (fulfilled(reads[0]) || fulfilled(reads[1]))
            throw new DirectReferenceExecutionError();
          await lease.assertOwned();
          context.transport.assertWithinBudget();
        },
      };
      return plane.client.withMutationFence(fence, async () => {
        await fence.assertOwned();
        const retained = await context.journal.readForceAfter();
        if (!retained)
          throw new DirectReferenceJournalError('prerequisite-unavailable');
        const initial = decodeForceFootprint(
          retained.identityJson,
          before,
          resource,
        );
        decodeForceProvenance(retained.provenanceJson);
        assertForceRecoveryState(initial);
        assertRetainedForceWorker(initial, resource);
        const current = await readForceFootprint(
          context,
          before,
          resource,
          plane,
        );
        assertForceRecoveryState(current.observation);
        if (current.observation.worker.scriptPresent) {
          assertRetainedForceWorker(current.observation, resource);
          if (
            JSON.stringify(current.observation.worker.currentVersionIds) !==
            JSON.stringify(initial.worker.currentVersionIds)
          )
            throw new DirectReferenceExecutionError();
        } else {
          assertForceWorkerAbsent(current.observation);
        }
        await assertForceAttachments(
          context,
          plane,
          resource,
          current.observation.worker.scriptPresent,
        );
        const emptyReads = await Promise.allSettled(
          buckets.map((bucket, index) =>
            current.observation.buckets[index]?.observedCreationDate === null
              ? Promise.resolve()
              : plane.backend.assertApplicationR2Empty(bucket, fence),
          ),
        );
        context.transport.assertWithinBudget();
        for (const read of emptyReads) fulfilled(read);
        const database: DatabaseReference = {
          id: resource.database.id,
          name: resource.database.name,
          created: false,
        };
        const releases: ExternalReleaseSnapshot[] =
          resource.knownVersionIds.map((artifactVersion) => ({
            physicalScriptName: resource.scriptName,
            specDigest: deploymentSpecDigest(spec),
            releaseSchemaVersion: spec.schemaVersion,
            artifactVersion,
          }));
        await fence.assertOwned();
        await plane.backend.deleteWorker(
          spec,
          releases.slice(1),
          database,
          releases[0],
          fence,
        );
        const afterWorker = await readForceFootprint(
          context,
          before,
          resource,
          plane,
        );
        assertForceRecoveryState(afterWorker.observation);
        assertForceWorkerAbsent(afterWorker.observation);
        for (const bucket of buckets) {
          await fence.assertOwned();
          const reads = await Promise.allSettled([
            plane.client.getR2Bucket(bucket.bucketName, bucket.jurisdiction),
            plane.client.getDatabase(resource.database.id),
            plane.client.listWorkerDatabaseAttachments(resource.database.id),
            plane.backend.assertApplicationR2Detached(bucket, fence),
          ] as const);
          context.transport.assertWithinBudget();
          const present = fulfilled(reads[0]);
          if (fulfilled(reads[1]) || fulfilled(reads[2]).length > 0)
            throw new DirectReferenceExecutionError();
          fulfilled(reads[3]);
          if (!present) continue;
          if (present.creationDate !== bucket.creationDate)
            throw new DirectReferenceExecutionError();
          await plane.backend.assertApplicationR2Empty(bucket, fence);
          await fence.assertOwned();
          await plane.backend.deleteApplicationR2Bucket(bucket, fence);
        }
        const final = await readForceFootprint(
          context,
          before,
          resource,
          plane,
        );
        assertForceRecoveryState(final.observation);
        assertForceWorkerAbsent(final.observation);
        if (
          final.observation.buckets.some(
            (bucket) => bucket.observedCreationDate !== null,
          )
        )
          throw new DirectReferenceExecutionError();
        await assertForceAttachments(context, plane, resource, false);
        await fence.assertOwned();
        return { returned: true, ...final };
      });
    },
  );
}
