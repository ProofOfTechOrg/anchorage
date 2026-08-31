// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import { assertInitialExecutionFenceState } from '@proofoftech/flowsafe/deployment-identity-protocol';

import {
  type AttestConvergedActiveRouteOptions,
  attestConvergedActiveRoute,
  PENDING_ARTIFACT_VERSION,
} from './active-route.js';
import {
  applicationBindingTopology,
  assertApplicationR2EmptyBeforeDecommission,
  convergeApplicationR2Creation,
  convergeApplicationR2Deletion,
  DEPLOYMENT_PLATFORM_VARIABLE_NAMES,
  liveApplicationTopologyMatches,
  reserveApplicationR2Resources,
} from './application-bindings.js';
import {
  assertBackendSwitchInactive,
  BACKEND_SWITCH_RECORD_ERROR,
  type BackendSwitchProvider,
  backendSwitchFleetRecordFromUnknown,
  commitInvocationAuthority,
  decommissionBackendSwitch,
  type FinalizedOrdinaryStateProvider,
  finalizedBridgeForRecord,
  reconcileFinalizedBackendSwitchState,
  structuralBackendSwitchFleetRecordFromUnknown,
} from './backend-switch.js';
import {
  type AdvanceCleanupDeploymentOptions,
  advanceCleanupDeployment,
  advanceCleanupUnderLease,
  type CleanupAdvanceAction,
  type CleanupAdvanceResult,
  startProvisioningRollbackCleanup,
} from './cleanup-advance.js';
import {
  activeExternalRelease,
  advanceDecommissionDeployment,
  assertImmutableDeploymentMapping,
  assertNormalDecommissionD1ResourcesDeleted,
  type DecommissionAdvanceAction,
  reconcilePersistedDatabase,
  retainedExternalReleases,
} from './decommission-advance.js';
import { decommissionAdvanceIntentFromUnknown } from './decommission-intent.js';
import { isSha256 } from './deployment-context.js';
import { WorkerDeploymentError } from './deployment-error.js';
import {
  assertExternalPlatformTarget,
  assertPlatformResourcesMatchTarget,
  canonicalDurableObjectMigrationHistory,
  defaultDeploymentEgressPolicy,
  describeExternalPlatformTarget,
  durableObjectMigrationHistoryDigest,
  effectiveAppliedPlatformTarget,
  externalReleaseTopology,
} from './platform-resources.js';
import { buildPromotionGuard } from './promotion-guard.js';
import { assertProviderBindingIdentitiesMatchInspection } from './provider-binding-inventory.js';
import { deploymentSpecDigest } from './spec-digest.js';
import type {
  CleanupAdvanceToken,
  DatabaseExport,
  DatabaseReference,
  DecommissionAuditSink,
  DecommissionResult,
  DeploymentSecrets,
  DeploymentSpec,
  ExternalPlatformTargetDescription,
  FleetRecord,
  FleetStateLease,
  FleetStateStore,
  InitialExecutionFenceState,
  MaintenanceHealth,
  ProvisioningBackend,
  ProvisioningPhase,
  ProvisioningResult,
} from './types.js';
import { assertNoActiveCleanup, assertNoActiveDecommission } from './types.js';
import {
  targetDurableObjectTag,
  validateDeploymentSecrets,
  validateDeploymentSpec,
} from './validation.js';

function canonicalNormalDecommissionRecord(record: FleetRecord): FleetRecord {
  const { decommissionIntent, ...source } = record;
  try {
    // A cleanup intent never rides beside a decommission shell; fail closed.
    if (record.cleanupIntent !== undefined) {
      throw new Error(BACKEND_SWITCH_RECORD_ERROR);
    }
    const intent = decommissionAdvanceIntentFromUnknown(
      decommissionIntent,
      source,
    );
    if (intent.identity.mode.kind !== 'normal') {
      throw new Error(BACKEND_SWITCH_RECORD_ERROR);
    }
    return { ...source, decommissionIntent: intent };
  } catch {
    throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  }
}

export {
  assertImmutableDeploymentMapping,
  reconcilePersistedDatabase,
} from './decommission-advance.js';

export class ProvisioningError extends Error {
  readonly cleanupErrors: readonly unknown[];
  /**
   * The bounded rollback outcome, present only when the failed provision ran
   * with `failureCleanup: 'bounded'` and the rollback admitted the engine.
   */
  readonly cleanup?: CleanupAdvanceResult;

  constructor(
    message: string,
    cause: unknown,
    cleanupErrors: readonly unknown[],
    cleanup?: CleanupAdvanceResult,
  ) {
    super(message, { cause });
    this.name = 'ProvisioningError';
    this.cleanupErrors = cleanupErrors;
    if (cleanup !== undefined) this.cleanup = cleanup;
  }
}

function canonicalStructuralValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalStructuralValue);
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(
    Object.keys(value as Record<string, unknown>)
      .sort((left, right) => (left < right ? -1 : left > right ? 1 : 0))
      .map((key) => [
        key,
        canonicalStructuralValue((value as Record<string, unknown>)[key]),
      ]),
  );
}

function sameCanonicalStructure(left: unknown, right: unknown): boolean {
  return (
    JSON.stringify(canonicalStructuralValue(left)) ===
    JSON.stringify(canonicalStructuralValue(right))
  );
}

function nowIso(clock: () => number): string {
  return new Date(clock()).toISOString();
}

function recordAt(
  backend: ProvisioningBackend,
  spec: DeploymentSpec,
  database: DatabaseReference,
  phase: ProvisioningPhase,
  clock: () => number,
  options: {
    readonly schemaVersion?: number;
    readonly artifactVersion?: string;
    readonly durableObjectBindings?: FleetRecord['durableObjectBindings'];
    readonly applicationResources?: NonNullable<
      FleetRecord['applicationResources']
    >;
  } = {},
): FleetRecord {
  const applicationResources =
    options.applicationResources ?? reserveApplicationR2Resources(spec);
  return {
    tenantTag: spec.tenantTag,
    backend: backend.kind,
    environment: spec.environment,
    scriptName: spec.scriptName,
    databaseId: database.id,
    databaseName: database.name,
    schemaVersion: options.schemaVersion ?? 0,
    artifactVersion: options.artifactVersion ?? PENDING_ARTIFACT_VERSION,
    desiredSpecDigest: deploymentSpecDigest(spec),
    durableObjectBindings: options.durableObjectBindings ?? [],
    applicationResources,
    applicationBindings: applicationBindingTopology(spec, applicationResources),
    routeHostname: spec.routeHostname,
    ...(backend.kind === 'workers-for-platforms'
      ? { outboundPolicy: defaultDeploymentEgressPolicy(spec) }
      : {}),
    phase,
    updatedAt: nowIso(clock),
  };
}

function expectedBindingKeys(
  bindings: readonly Readonly<{
    name: string;
    className: string;
    namespaceId?: string;
    scriptName?: string;
    dispatchNamespace?: string;
  }>[],
  includeNamespaceId = false,
): readonly string[] {
  return bindings
    .map(
      ({ name, className, namespaceId, scriptName, dispatchNamespace }) =>
        `${name}:${className}:${includeNamespaceId ? (namespaceId ?? '') : ''}:${scriptName ?? ''}:${dispatchNamespace ?? ''}`,
    )
    .sort();
}

export function assertLiveDeploymentMatches(
  live: import('./types.js').LiveDeployment,
  record: Pick<FleetRecord, 'tenantTag' | 'environment' | 'databaseId'> &
    Partial<Pick<FleetRecord, 'platformResources' | 'applicationBindings'>>,
  spec: DeploymentSpec,
  expectedDigest: string,
  expectedApplication:
    | import('./types.js').ApplicationBindingTopology
    | undefined = record.applicationBindings,
): void {
  if (live.plainTextBindings === undefined || live.secretNames === undefined) {
    throw new Error(
      `deployment '${record.tenantTag}:${record.environment}' live binding inventory is incomplete`,
    );
  }
  assertProviderBindingIdentitiesMatchInspection(
    {
      databaseIds: [live.databaseId],
      durableObjectBindings: live.durableObjectBindings,
      serviceBindings: live.serviceBindings,
      queueProducerBindings: live.queueProducerBindings,
      r2BucketBindings: live.r2BucketBindings,
      plainTextBindings: live.plainTextBindings,
      secretNames: live.secretNames,
      providerBindingIdentities: live.providerBindingIdentities,
    },
    `deployment '${record.tenantTag}:${record.environment}'`,
  );
  const externalTopology =
    spec.authoredBy === 'external' && record.platformResources
      ? externalReleaseTopology(spec, record.platformResources)
      : undefined;
  const expectedDurableObjectBindings =
    externalTopology?.durableObjectBindings ??
    spec.durableObjectBindings.map((binding) => ({
      ...binding,
      ...(record.platformResources
        ? { scriptName: record.platformResources.stateWorker.scriptName }
        : {}),
    }));
  const expectedServiceBindings =
    externalTopology?.serviceBindings ??
    (spec.authoredBy === 'external'
      ? []
      : spec.egressProxyService
        ? [{ name: 'EGRESS_PROXY', service: spec.egressProxyService }]
        : []);
  const expectedQueueProducerBindings =
    externalTopology?.queueProducerBindings ??
    (spec.authoredBy === 'external'
      ? []
      : spec.queueProducer
        ? [
            {
              name: spec.queueProducer.binding,
              queueName: spec.queueProducer.queueName,
            },
          ]
        : []);
  const application = expectedApplication;
  const expectedApplicationSecrets =
    application?.secrets.map(({ name }) => name) ?? [];
  const expectedSecretNames = [
    'DEPLOYMENT_IDENTITY_SECRET',
    ...(spec.authoredBy === 'external' ? [] : ['MAINTENANCE_ADMIN_SECRET']),
    ...expectedApplicationSecrets,
  ].sort();
  if (
    live.tenantTag !== record.tenantTag ||
    live.environment !== record.environment ||
    live.databaseId !== record.databaseId ||
    live.schemaVersion !== spec.schemaVersion ||
    live.desiredSpecDigest !== expectedDigest ||
    JSON.stringify(
      expectedBindingKeys(
        live.durableObjectBindings,
        externalTopology !== undefined,
      ),
    ) !==
      JSON.stringify(
        expectedBindingKeys(
          expectedDurableObjectBindings,
          externalTopology !== undefined,
        ),
      ) ||
    JSON.stringify(live.serviceBindings ?? []) !==
      JSON.stringify(expectedServiceBindings) ||
    JSON.stringify(live.queueProducerBindings ?? []) !==
      JSON.stringify(expectedQueueProducerBindings) ||
    !liveApplicationTopologyMatches(
      application,
      live,
      DEPLOYMENT_PLATFORM_VARIABLE_NAMES,
    ) ||
    JSON.stringify([...live.secretNames].sort()) !==
      JSON.stringify(expectedSecretNames)
  ) {
    throw new Error(
      `deployment '${record.tenantTag}:${record.environment}' live state does not exactly match the desired specification`,
    );
  }
}

export function assertExternalReleaseArtifactVersion(
  live: import('./types.js').LiveDeployment | undefined,
  release: import('./types.js').ExternalReleaseSnapshot,
  context: string,
): void {
  if (release.artifactVersion === PENDING_ARTIFACT_VERSION) return;
  if (
    !live ||
    live.scriptName !== release.physicalScriptName ||
    live.artifactVersion !== release.artifactVersion
  ) {
    throw new Error(
      `${context} immutable release '${release.physicalScriptName}' does not match persisted artifact version '${release.artifactVersion}'`,
    );
  }
}

export function assertPlatformDurableObjectHistory(
  record: Pick<
    FleetRecord,
    | 'durableObjectTag'
    | 'durableObjectMigrationHistory'
    | 'durableObjectMigrationHistoryDigest'
  >,
  spec: DeploymentSpec,
): void {
  if (spec.authoredBy !== 'platform') return;
  const persisted = record.durableObjectMigrationHistory;
  if (!persisted) {
    if (record.durableObjectTag !== undefined) {
      throw new Error(
        'platform-authored Durable Object state has no persisted migration history',
      );
    }
  } else if (
    record.durableObjectMigrationHistoryDigest !==
      durableObjectMigrationHistoryDigest(persisted) ||
    persisted.at(-1)?.tag !== record.durableObjectTag
  ) {
    throw new Error(
      'platform-authored Durable Object migration history is internally inconsistent',
    );
  }
  const prior = persisted ?? [];
  if (
    prior.length > spec.durableObjectMigrations.length ||
    (persisted !== undefined &&
      durableObjectMigrationHistoryDigest(
        spec.durableObjectMigrations.slice(0, prior.length),
      ) !== record.durableObjectMigrationHistoryDigest)
  ) {
    throw new Error(
      'platform-authored Durable Object migration history is not an exact append-only extension',
    );
  }
}

async function rollbackProvisioning(
  lease: FleetStateLease,
  backend: ProvisioningBackend,
  spec: DeploymentSpec,
  database: DatabaseReference | undefined,
  workerCreated: boolean,
  workerResourceState: 'absent' | 'present' | 'unknown',
  databaseCreated: boolean,
  platformResources?: FleetRecord['platformResources'],
  record?: FleetRecord,
): Promise<Readonly<{ errors: readonly unknown[]; record?: FleetRecord }>> {
  const errors: unknown[] = [];
  let latestRecord = record;
  let workerDeleted = !workerCreated && workerResourceState === 'absent';
  if (workerCreated) {
    if (!database) {
      errors.push(new Error('created Worker has no persisted database'));
      return { errors, ...(latestRecord ? { record: latestRecord } : {}) };
    }
    const rollbackRetainedReleases = record
      ? retainedExternalReleases(record)
      : undefined;
    const rollbackActiveRelease = record
      ? activeExternalRelease(record)
      : undefined;
    let trafficRemoved = false;
    try {
      await backend.removeTraffic(
        spec,
        rollbackRetainedReleases,
        rollbackActiveRelease,
        database,
        lease,
      );
      await backend.assertTrafficRemoved(spec);
      await assertApplicationR2EmptyBeforeDecommission({
        resources: record?.applicationResources ?? [],
        backend,
        fence: lease,
      });
      trafficRemoved = true;
    } catch (error) {
      errors.push(error);
    }
    if (trafficRemoved) {
      try {
        await backend.revokeCredentials(
          spec,
          rollbackRetainedReleases,
          rollbackActiveRelease,
          database,
          lease,
        );
        await backend.deleteWorker(
          spec,
          rollbackRetainedReleases,
          database,
          rollbackActiveRelease,
          lease,
        );
        workerDeleted = true;
      } catch (error) {
        errors.push(error);
      }
    }
  }
  const platformResourceGroupReserved =
    platformResources !== undefined || record?.platformTarget !== undefined;
  let platformResourcesDeleted = !platformResourceGroupReserved;
  if (platformResourceGroupReserved && workerDeleted) {
    if (!database || !record) {
      errors.push(
        new Error('trusted platform resources have no durable deployment'),
      );
      return { errors, ...(latestRecord ? { record: latestRecord } : {}) };
    }
    if (
      !backend.revokePlatformResourceCredentials ||
      !backend.deletePlatformResources
    ) {
      errors.push(
        new Error('backend cannot clean persisted trusted platform resources'),
      );
      platformResourcesDeleted = false;
    } else {
      try {
        await lease.assertOwned();
        await backend.revokePlatformResourceCredentials(
          spec,
          record,
          database,
          lease,
        );
      } catch (error) {
        errors.push(error);
      }
      try {
        await lease.assertOwned();
        await backend.deletePlatformResources(spec, record, database, lease);
        platformResourcesDeleted = true;
      } catch (error) {
        errors.push(error);
      }
    }
  }
  let applicationResourcesDeleted =
    (record?.applicationResources ?? []).length === 0;
  if (
    !applicationResourcesDeleted &&
    record &&
    workerDeleted &&
    platformResourcesDeleted
  ) {
    let cleanupRecord = latestRecord ?? record;
    try {
      const resources = await convergeApplicationR2Deletion({
        spec,
        resources: cleanupRecord.applicationResources ?? [],
        backend,
        fence: lease,
        persist: async (applicationResources) => {
          cleanupRecord = {
            ...cleanupRecord,
            applicationResources,
            updatedAt: nowIso(Date.now),
          };
          latestRecord = cleanupRecord;
          await lease.put(latestRecord);
        },
      });
      applicationResourcesDeleted = resources.every(
        (resource) =>
          resource.state === 'reserved' || resource.state === 'deleted',
      );
    } catch (error) {
      errors.push(error);
    }
  }
  if (
    database &&
    databaseCreated &&
    workerDeleted &&
    platformResourcesDeleted &&
    applicationResourcesDeleted
  ) {
    try {
      if (!latestRecord) {
        throw new Error('created database has no durable cleanup record');
      }
      await backend.assertDatabaseDetached(spec, latestRecord, database, lease);
      await backend.deleteDatabase(database, lease);
    } catch (error) {
      errors.push(error);
    }
  }
  return { errors, ...(latestRecord ? { record: latestRecord } : {}) };
}

const RESUMABLE_PROVISIONING_PHASES = new Set<ProvisioningPhase>([
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
  'publishing',
  'ready',
]);

export interface ProvisionDeploymentOptions {
  readonly backend: ProvisioningBackend;
  readonly store: FleetStateStore;
  readonly spec: DeploymentSpec;
  readonly secrets: DeploymentSecrets;
  /**
   * The execution-fence state this deployment is born in. REQUIRED; no default.
   *
   * It lives on the OPTIONS and deliberately not on `DeploymentSpec`. The spec
   * is the digested canonical description of a deployment
   * (`deploymentSpecDigest`), and every stored record, drift audit, and
   * migration decision compares against that digest — adding a field to it
   * would change the digest of every deployment already in the fleet and
   * present as fleet-wide drift, scheduling migrations for artifacts nobody
   * touched.
   *
   * It is also provisioning-time-only by nature: after the first pass the fence
   * is live operational state an operator moves through
   * `POST /admin/execution-fence`, so `migrateFleet`, `rollbackExternalRelease`
   * and `decommissionDeployment` neither take it nor need it — re-seeding is
   * INSERT-if-absent and can only ever repair a missing row.
   */
  readonly initialExecutionFenceState: InitialExecutionFenceState;
  readonly finalizedStateProvider?: FinalizedOrdinaryStateProvider;
  /**
   * Tuning for the convergence wait the ready-commit attestation performs.
   * The defaults suit every provider this package targets.
   */
  readonly routeAttestation?: AttestConvergedActiveRouteOptions;
  /**
   * How a failed provision rolls back once the bounded cleanup engine admits
   * it. `'drain'` (the default) drains the engine to its terminal receipt
   * within the failing call; `'bounded'` performs at most one bounded advance
   * and surfaces the outcome through `ProvisioningError.cleanup` so the caller
   * resumes the durable operation with `advanceCleanupDeployment()`.
   */
  readonly failureCleanup?: 'drain' | 'bounded';
  readonly clock?: () => number;
}

// `async` so the entry validation below REJECTS rather than throwing
// synchronously: every caller and every test treats this as a promise-returning
// function, and a synchronous throw would escape an unguarded `.catch()`.
export async function provisionDeployment(
  options: ProvisionDeploymentOptions,
): Promise<ProvisioningResult> {
  // Validated HERE, before the lease and before a single provider call. The
  // protocol validates it too, but only when the seeding statements are built —
  // which is after `database-created`, so a garbage value would have created a
  // Worker and a D1 database first and then failed, leaving a half-provisioned
  // deployment for an operator to reconcile over a typo. Nothing about this
  // value depends on anything the lease reads, so the cheapest place to refuse
  // it is the entry.
  assertInitialExecutionFenceState(
    options.initialExecutionFenceState,
    'provisionDeployment',
  );
  return options.store.withDeploymentLease(
    options.spec.tenantTag,
    options.spec.environment,
    (lease) => provisionDeploymentUnderLease(options, lease),
  );
}

async function provisionDeploymentUnderLease(
  options: ProvisionDeploymentOptions,
  lease: FleetStateLease,
): Promise<ProvisioningResult> {
  const { backend, store, spec, secrets } = options;
  const clock = options.clock ?? Date.now;
  if (backend.kind === 'plain-worker' && spec.authoredBy === 'external') {
    throw new Error(
      `plain-worker backend refuses externally authored artifact '${spec.scriptName}'`,
    );
  }
  validateDeploymentSpec(spec);
  validateDeploymentSecrets(spec, secrets);
  let externalTarget: ExternalPlatformTargetDescription | undefined;
  const immutableExternal =
    backend.immutableExternalArtifacts === true &&
    spec.authoredBy === 'external';
  if (new URL(spec.maintenanceBaseUrl).hostname === spec.routeHostname) {
    throw new Error(
      'maintenanceBaseUrl must use a control-plane hostname distinct from routeHostname',
    );
  }
  const prior = await store.get(spec.tenantTag, spec.environment);
  if (prior) {
    assertNoActiveDecommission(prior, 'provisionDeployment');
    // The fixed redirect IS this entry's cleanup guard: it must fire before
    // the generic non-resumable-phase refusal so a rollback that admitted the
    // bounded engine routes to cleanup instead of a provisioning retry.
    if (
      prior.phase === 'cleanup-advancing' ||
      prior.cleanupIntent !== undefined
    ) {
      throw new Error(
        `deployment '${spec.tenantTag}:${spec.environment}' has an active bounded cleanup; complete it with cleanupDeploymentArtifacts() or advanceCleanupDeployment() before provisioning again`,
      );
    }
    assertBackendSwitchInactive(prior);
  }
  const finalizedOrdinaryState =
    prior?.backendSwitchIntent?.subphase === 'finalized' &&
    prior.platformResources?.stateWorker.plane === 'ordinary';
  const finalizedStateProvider = finalizedOrdinaryState
    ? options.finalizedStateProvider
    : undefined;
  if (finalizedOrdinaryState) {
    if (!finalizedStateProvider) {
      throw new Error(
        'finalized ordinary state requires its backend-switch provider',
      );
    }
    finalizedBridgeForRecord(prior);
    externalTarget = finalizedStateProvider.describeFinalizedBridgeTarget(
      spec,
      prior,
    );
  } else if (spec.authoredBy === 'external') {
    externalTarget = describeExternalPlatformTarget(backend, spec);
  }
  if (!prior && spec.previousDurableObjectTag !== undefined) {
    throw new Error(
      'a new deployment cannot declare a previous Durable Object migration tag',
    );
  }
  if (prior) {
    assertImmutableDeploymentMapping(prior, backend, spec);
    assertPlatformDurableObjectHistory(prior, spec);
    if (!RESUMABLE_PROVISIONING_PHASES.has(prior.phase)) {
      throw new Error(
        `deployment '${spec.tenantTag}:${spec.environment}' cannot be provisioned from phase '${prior.phase}'`,
      );
    }
  }
  if (prior?.phase === 'ready') {
    if (prior.desiredSpecDigest !== deploymentSpecDigest(spec)) {
      throw new Error(
        `deployment '${spec.tenantTag}:${spec.environment}' requires migrateFleet() for artifact changes`,
      );
    }
    if (
      prior.schemaVersion !== spec.schemaVersion &&
      !(
        backend.immutableExternalArtifacts === true &&
        spec.authoredBy === 'external' &&
        prior.activeRelease?.releaseSchemaVersion === spec.schemaVersion &&
        prior.schemaVersion > spec.schemaVersion
      )
    ) {
      throw new Error(
        `deployment '${spec.tenantTag}:${spec.environment}' requires migrateFleet() for schema changes`,
      );
    }
    const database = await reconcilePersistedDatabase(
      backend,
      prior,
      false,
      lease,
    );
    if (!database) {
      throw new Error(`persisted database '${prior.databaseId}' is absent`);
    }
    let converged = prior;
    if (spec.authoredBy === 'external') {
      if (!backend.ensurePlatformResources || !externalTarget) {
        throw new Error(
          'external backend cannot provision trusted platform resources',
        );
      }
      if (!prior.platformResources) {
        throw new Error('ready external deployment has no platform resources');
      }
      const rollbackCompatibleTarget = effectiveAppliedPlatformTarget(
        prior,
        externalTarget,
      );
      assertPlatformResourcesMatchTarget(
        prior.platformResources,
        rollbackCompatibleTarget,
      );
      if (!prior.platformTarget) {
        converged = {
          ...prior,
          platformTarget: rollbackCompatibleTarget,
          outboundPolicy: rollbackCompatibleTarget.outboundPolicy,
          updatedAt: nowIso(clock),
        };
        await lease.put(converged);
      } else {
        assertExternalPlatformTarget(
          prior.platformTarget,
          rollbackCompatibleTarget,
          'ready deployment',
        );
      }
      await lease.assertOwned();
      if (finalizedOrdinaryState) {
        if (!finalizedStateProvider) {
          throw new Error(
            'finalized ordinary state requires its backend-switch provider',
          );
        }
        converged = await reconcileFinalizedBackendSwitchState({
          provider: finalizedStateProvider,
          targetSpec: spec,
          target: rollbackCompatibleTarget,
          record: converged,
          lease,
          clock,
        });
      } else {
        const platform = await backend.ensurePlatformResources(
          spec,
          database,
          secrets,
          rollbackCompatibleTarget,
          converged,
          lease,
        );
        assertPlatformResourcesMatchTarget(
          platform.resources,
          rollbackCompatibleTarget,
        );
        if (
          !sameCanonicalStructure(
            platform.resources,
            converged.platformResources,
          ) ||
          !sameCanonicalStructure(
            rollbackCompatibleTarget,
            converged.platformTarget,
          ) ||
          !sameCanonicalStructure(
            rollbackCompatibleTarget.outboundPolicy,
            converged.outboundPolicy,
          )
        ) {
          converged = {
            ...converged,
            platformResources: platform.resources,
            platformTarget: rollbackCompatibleTarget,
            outboundPolicy: rollbackCompatibleTarget.outboundPolicy,
            updatedAt: nowIso(clock),
          };
          await lease.put(converged);
        }
      }
    }
    const readyArtifactVersion =
      converged.activeRelease?.artifactVersion ?? converged.artifactVersion;
    const live = await backend.inspect(
      spec,
      secrets.maintenanceAdmin,
      readyArtifactVersion,
    );
    if (!live) throw new Error('ready deployment is missing from the backend');
    assertLiveDeploymentMatches(live, converged, spec, prior.desiredSpecDigest);
    if (immutableExternal) {
      if (!converged.activeRelease) {
        throw new Error('ready immutable deployment has no active release');
      }
      assertExternalReleaseArtifactVersion(
        live,
        converged.activeRelease,
        'ready provisioning',
      );
    }
    let maintenance = live.maintenance;
    if (!maintenance.armed) {
      converged = await commitInvocationAuthority(lease, converged, clock);
      await lease.assertOwned();
      maintenance = await backend.ensureMaintenance(
        spec,
        secrets.maintenanceAdmin,
        lease,
        readyArtifactVersion,
      );
    }
    return { record: converged, maintenance };
  }

  let database: DatabaseReference | undefined;
  let databaseOwnershipProven = false;
  let workerCreated = false;
  let workerResourceState: 'absent' | 'present' | 'unknown' = 'absent';
  let record = prior;
  const databaseReservationOwned =
    prior === undefined ||
    prior.phase === 'database-reserved' ||
    prior.phase === 'database-create-authorized';
  try {
    if (!record) {
      const reservation: DatabaseReference = {
        id: `reserved-${deploymentSpecDigest(spec).slice(0, 48)}`,
        name: spec.databaseName,
        created: true,
      };
      record = {
        ...recordAt(backend, spec, reservation, 'database-reserved', clock),
        // A new deployment carries the never-authorized carrier from its
        // FIRST durable put so later cleanup can distinguish "no candidate
        // invocation ever authorized" from a legacy row.
        invocationAuthority: { version: 1, authorizedAt: null },
      };
      await lease.put(record);
    }
    if (record.phase === 'database-reserved') {
      const existingDatabase = await backend.findDatabase(spec);
      if (existingDatabase) {
        throw new Error(
          `refusing to claim pre-existing database '${existingDatabase.id}:${existingDatabase.name}' for reserved name '${record.databaseName}'`,
        );
      }
      record = {
        ...record,
        phase: 'database-create-authorized',
        updatedAt: nowIso(clock),
      };
      await lease.put(record);
    }
    if (record.phase === 'database-create-authorized') {
      await lease.assertOwned();
      database = await backend.ensureDatabase(spec, lease);
      record = {
        ...recordAt(backend, spec, database, 'database-created', clock, {
          applicationResources: record.applicationResources,
        }),
        ...(record.invocationAuthority
          ? { invocationAuthority: record.invocationAuthority }
          : {}),
      };
      await lease.put(record);
    } else {
      database = await reconcilePersistedDatabase(
        backend,
        record,
        false,
        lease,
        record.phase !== 'database-created',
      );
      if (!database) {
        throw new Error(`persisted database '${record.databaseId}' is absent`);
      }
      databaseOwnershipProven = record.phase !== 'database-created';
    }

    if (record.phase === 'database-created') {
      await lease.assertOwned();
      await backend.seedDeploymentIdentity(database, spec.tenantTag, lease, {
        initialExecutionFenceState: options.initialExecutionFenceState,
      });
      databaseOwnershipProven = true;
      record = {
        ...record,
        phase: 'identity-seeded',
        updatedAt: nowIso(clock),
      };
      await lease.put(record);
    }

    if (record.phase === 'identity-seeded') {
      const currentSchemaVersion = record.schemaVersion;
      const pendingMigrations = spec.migrations.filter(
        (candidate) => candidate.version > currentSchemaVersion,
      );
      if (pendingMigrations.length === 0) {
        await lease.assertOwned();
        await backend.applyMigrations(database, spec.migrations, lease);
      }
      for (const migration of pendingMigrations) {
        await lease.assertOwned();
        await backend.applyMigrations(
          database,
          spec.migrations.slice(0, migration.version),
          lease,
        );
        record = {
          ...record,
          schemaVersion: migration.version,
          updatedAt: nowIso(clock),
        };
        await lease.put(record);
      }
      if (record.schemaVersion !== spec.schemaVersion) {
        throw new Error(
          `missing D1 migration path from ${record.schemaVersion} to ${spec.schemaVersion}`,
        );
      }
      record = { ...record, phase: 'migrated', updatedAt: nowIso(clock) };
      await lease.put(record);
    }

    if (record.phase === 'migrated') {
      record = {
        ...record,
        phase: 'application-resources-create-authorized',
        updatedAt: nowIso(clock),
      };
      await lease.put(record);
    }

    if (record.phase === 'application-resources-create-authorized') {
      let applicationRecord: FleetRecord = record;
      const resolved = await convergeApplicationR2Creation({
        spec,
        resources: applicationRecord.applicationResources ?? [],
        backend,
        fence: lease,
        persist: async (applicationResources) => {
          applicationRecord = {
            ...applicationRecord,
            applicationResources,
            updatedAt: nowIso(clock),
          };
          record = applicationRecord;
          await lease.put(record);
        },
      });
      record = {
        ...applicationRecord,
        phase: 'application-resources-deployed',
        applicationResources: resolved,
        applicationBindings: applicationBindingTopology(spec, resolved),
        updatedAt: nowIso(clock),
      };
      await lease.put(record);
    }

    if (record.phase === 'application-resources-deployed') {
      if (spec.authoredBy === 'external') {
        if (!backend.ensurePlatformResources || !externalTarget) {
          throw new Error(
            'external backend cannot provision trusted platform resources',
          );
        }
        if (record.platformTarget) {
          assertExternalPlatformTarget(
            record.platformTarget,
            externalTarget,
            'provisioning retry',
          );
        } else {
          record = {
            ...record,
            platformTarget: externalTarget,
            outboundPolicy: externalTarget.outboundPolicy,
            updatedAt: nowIso(clock),
          };
          await lease.put(record);
        }
        await lease.assertOwned();
        const platform = await backend.ensurePlatformResources(
          spec,
          database,
          secrets,
          externalTarget,
          record,
          lease,
        );
        assertPlatformResourcesMatchTarget(platform.resources, externalTarget);
        record = {
          ...record,
          phase: 'platform-resources-deployed',
          platformResources: platform.resources,
          platformTarget: externalTarget,
          outboundPolicy: externalTarget.outboundPolicy,
          updatedAt: nowIso(clock),
        };
        await lease.put(record);
      }
    }

    if (
      spec.authoredBy === 'external' &&
      record.phase !== 'database-created' &&
      record.phase !== 'identity-seeded' &&
      record.phase !== 'migrated'
    ) {
      if (!externalTarget || !record.platformResources) {
        throw new Error(
          'external provisioning state has no trusted platform target',
        );
      }
      assertPlatformResourcesMatchTarget(
        record.platformResources,
        externalTarget,
      );
      if (record.platformTarget) {
        assertExternalPlatformTarget(
          record.platformTarget,
          externalTarget,
          'provisioning retry',
        );
      } else {
        record = {
          ...record,
          platformTarget: externalTarget,
          outboundPolicy: externalTarget.outboundPolicy,
          updatedAt: nowIso(clock),
        };
        await lease.put(record);
      }
    }

    if (
      record.phase === 'platform-resources-deployed' ||
      (record.phase === 'application-resources-deployed' &&
        spec.authoredBy !== 'external')
    ) {
      if (backend.immutableExternalArtifacts === true) {
        // External upload is the dispatch trigger, so the flip commits before
        // it; the trusted plain initial deploy is deliberately NOT a flip
        // site, preserving no-export cleanup at worker-deployed.
        record = await commitInvocationAuthority(lease, record, clock);
      }
      await lease.assertOwned();
      const deployed = await backend.deployWorker(
        spec,
        database,
        secrets,
        record.platformResources,
        lease,
        immutableExternal ? PENDING_ARTIFACT_VERSION : undefined,
        record.applicationBindings,
      );
      workerCreated = deployed.created;
      workerResourceState = deployed.created ? 'present' : 'absent';
      const deployedDurableObjectTag = targetDurableObjectTag(spec);
      record = {
        ...record,
        phase: 'worker-deployed',
        artifactVersion: deployed.artifactVersion,
        ...(deployed.physicalScriptName
          ? {
              pendingRelease: {
                physicalScriptName: deployed.physicalScriptName,
                specDigest: record.desiredSpecDigest,
                artifactVersion: deployed.artifactVersion,
                releaseSchemaVersion: spec.schemaVersion,
                application: record.applicationBindings ?? {
                  vars: [],
                  secrets: [],
                  r2Buckets: [],
                },
                topology: externalReleaseTopology(
                  spec,
                  record.platformResources,
                  record.applicationResources,
                ),
              },
            }
          : {}),
        ...(deployedDurableObjectTag
          ? { durableObjectTag: deployedDurableObjectTag }
          : {}),
        ...(spec.authoredBy === 'platform'
          ? {
              durableObjectMigrationHistory:
                canonicalDurableObjectMigrationHistory(
                  spec.durableObjectMigrations,
                ),
              durableObjectMigrationHistoryDigest:
                durableObjectMigrationHistoryDigest(
                  spec.durableObjectMigrations,
                ),
            }
          : {}),
        updatedAt: nowIso(clock),
      };
      await lease.put(record);
    }

    if (
      record.phase === 'worker-deployed' ||
      record.phase === 'maintenance-armed'
    ) {
      const pendingArtifactVersion =
        record.pendingRelease?.artifactVersion ?? record.artifactVersion;
      const live = await backend.inspect(
        spec,
        secrets.maintenanceAdmin,
        pendingArtifactVersion,
      );
      if (immutableExternal) {
        if (!record.pendingRelease) {
          throw new Error(
            'immutable provisioning retry has no pending release metadata',
          );
        }
        assertExternalReleaseArtifactVersion(
          live,
          record.pendingRelease,
          'provisioning retry',
        );
      }
      if (!live || live.desiredSpecDigest !== record.desiredSpecDigest) {
        if (backend.immutableExternalArtifacts === true) {
          record = await commitInvocationAuthority(lease, record, clock);
        }
        await lease.assertOwned();
        const deployed = await backend.deployWorker(
          spec,
          database,
          secrets,
          record.platformResources,
          lease,
          immutableExternal
            ? (record.pendingRelease?.artifactVersion ??
                PENDING_ARTIFACT_VERSION)
            : undefined,
          record.applicationBindings,
        );
        workerCreated ||= deployed.created;
        if (deployed.created) workerResourceState = 'present';
        record = {
          ...record,
          phase: 'worker-deployed',
          artifactVersion: deployed.artifactVersion,
          ...(deployed.physicalScriptName
            ? {
                pendingRelease: {
                  physicalScriptName: deployed.physicalScriptName,
                  specDigest: record.desiredSpecDigest,
                  artifactVersion: deployed.artifactVersion,
                  releaseSchemaVersion: spec.schemaVersion,
                  topology: externalReleaseTopology(
                    spec,
                    record.platformResources,
                    record.applicationResources,
                  ),
                },
              }
            : {}),
          updatedAt: nowIso(clock),
        };
        await lease.put(record);
      }
    }

    let maintenance: MaintenanceHealth | undefined;
    if (record.phase === 'worker-deployed') {
      if (immutableExternal) {
        const preflight = await backend.inspect(
          spec,
          secrets.maintenanceAdmin,
          record.pendingRelease?.artifactVersion ?? record.artifactVersion,
        );
        if (!record.pendingRelease) {
          throw new Error(
            'immutable provisioning has no pending release metadata',
          );
        }
        assertExternalReleaseArtifactVersion(
          preflight,
          record.pendingRelease,
          'maintenance bootstrap',
        );
      }
      // Dedicated flip put: maintenance is the first candidate-invoking
      // request for trusted plain deployments, and the flip must never ride
      // the worker-deployed put, which stays no-export-eligible.
      record = await commitInvocationAuthority(lease, record, clock);
      await lease.assertOwned();
      maintenance = await backend.ensureMaintenance(
        spec,
        secrets.maintenanceAdmin,
        lease,
        record.pendingRelease?.artifactVersion ?? record.artifactVersion,
      );
      if (!maintenance.armed) {
        throw new Error('maintenance bootstrap returned an unarmed scheduler');
      }
      const live = await backend.inspect(
        spec,
        secrets.maintenanceAdmin,
        record.pendingRelease?.artifactVersion ?? record.artifactVersion,
      );
      if (!live) throw new Error('deployed Worker is missing during bootstrap');
      assertLiveDeploymentMatches(live, record, spec, record.desiredSpecDigest);
      if (immutableExternal && record.pendingRelease) {
        assertExternalReleaseArtifactVersion(
          live,
          record.pendingRelease,
          'maintenance bootstrap',
        );
      }
      const liveDurableObjectTag = targetDurableObjectTag(spec);
      record = {
        ...record,
        phase: 'maintenance-armed',
        artifactVersion: live.artifactVersion,
        ...(liveDurableObjectTag
          ? { durableObjectTag: liveDurableObjectTag }
          : {}),
        ...(spec.authoredBy === 'platform'
          ? {
              durableObjectMigrationHistory:
                canonicalDurableObjectMigrationHistory(
                  spec.durableObjectMigrations,
                ),
              durableObjectMigrationHistoryDigest:
                durableObjectMigrationHistoryDigest(
                  spec.durableObjectMigrations,
                ),
            }
          : {}),
        durableObjectBindings: live.durableObjectBindings,
        updatedAt: nowIso(clock),
      };
      await lease.put(record);
    }

    if (record.phase === 'maintenance-armed') {
      record = {
        ...record,
        // The flip rides this already export-required transition; here it is
        // consistency for legacy rows, not an eligibility change.
        ...(typeof record.invocationAuthority?.authorizedAt === 'string'
          ? {}
          : {
              invocationAuthority: {
                version: 1 as const,
                authorizedAt: nowIso(clock),
              },
            }),
        phase: 'publishing',
        updatedAt: nowIso(clock),
      };
      await lease.put(record);
    }

    if (record.phase === 'publishing') {
      if (immutableExternal) {
        const preflight = await backend.inspect(
          spec,
          secrets.maintenanceAdmin,
          record.pendingRelease?.artifactVersion ?? record.artifactVersion,
        );
        if (!record.pendingRelease) {
          throw new Error(
            'immutable publication has no pending release metadata',
          );
        }
        if (!preflight) {
          throw new Error('immutable publication release is missing');
        }
        assertLiveDeploymentMatches(
          preflight,
          record,
          spec,
          record.desiredSpecDigest,
        );
        assertExternalReleaseArtifactVersion(
          preflight,
          record.pendingRelease,
          'release publication',
        );
      }
      // A no-op unless a legacy row resumed directly at 'publishing' without
      // riding the flip on the transition put above.
      record = await commitInvocationAuthority(lease, record, clock);
      await lease.assertOwned();
      await backend.promoteWorker(
        spec,
        buildPromotionGuard(
          record,
          backend.releaseScriptName?.(spec) ?? spec.scriptName,
          record.activeRelease === undefined,
        ),
        record.outboundPolicy,
        lease,
        record.pendingRelease?.artifactVersion ?? record.artifactVersion,
      );
      const live = await backend.inspect(
        spec,
        secrets.maintenanceAdmin,
        record.pendingRelease?.artifactVersion ?? record.artifactVersion,
      );
      if (!live) throw new Error('deployment disappeared before ready commit');
      assertLiveDeploymentMatches(live, record, spec, record.desiredSpecDigest);
      if (immutableExternal && record.pendingRelease) {
        assertExternalReleaseArtifactVersion(
          live,
          record.pendingRelease,
          'ready settlement',
        );
      }
      if (live.maintenance.armed) {
        maintenance = live.maintenance;
      } else {
        await lease.assertOwned();
        maintenance = await backend.ensureMaintenance(
          spec,
          secrets.maintenanceAdmin,
          lease,
          record.pendingRelease?.artifactVersion ?? record.artifactVersion,
        );
      }
      if (!maintenance.armed) {
        throw new Error('maintenance is unarmed before ready commit');
      }
      // The committed artifact version is the ROUTED one, not the one an
      // inspection reported. Inspection deliberately pins the candidate it was
      // asked about, so committing its answer would record a version this
      // deployment merely uploaded as the version it serves.
      const attestation = await attestConvergedActiveRoute(
        backend,
        spec,
        {
          specDigest: record.desiredSpecDigest,
          artifactVersion:
            record.pendingRelease?.artifactVersion ?? record.artifactVersion,
        },
        { clock, ...options.routeAttestation },
      );
      const readyRecord = { ...record };
      if (readyRecord.pendingRelease) {
        readyRecord.activeRelease = readyRecord.pendingRelease;
        delete readyRecord.pendingRelease;
      }
      const routedDurableObjectTag = targetDurableObjectTag(spec);
      record = {
        ...readyRecord,
        phase: 'ready',
        artifactVersion: attestation.artifactVersion,
        ...(routedDurableObjectTag
          ? { durableObjectTag: routedDurableObjectTag }
          : {}),
        durableObjectBindings: live.durableObjectBindings,
        updatedAt: nowIso(clock),
      };
      await lease.put(record);
    }

    if (!maintenance) {
      const live = await backend.inspect(
        spec,
        secrets.maintenanceAdmin,
        record.activeRelease?.artifactVersion ?? record.artifactVersion,
      );
      if (!live) throw new Error('ready deployment is missing');
      maintenance = live.maintenance;
    }
    return { record, maintenance };
  } catch (cause) {
    if (cause instanceof WorkerDeploymentError) {
      workerCreated ||= cause.createdByAttempt;
      workerResourceState = cause.resourceState;
    }
    if (record?.phase === 'publishing') {
      throw new ProvisioningError(
        `failed to provision '${spec.tenantTag}:${spec.environment}'; publishing state is preserved for retry or export-backed decommissioning`,
        cause,
        [],
      );
    }
    let cleanupErrors: readonly unknown[];
    let boundedOutcome: CleanupAdvanceResult | undefined;
    if (
      record !== undefined &&
      databaseReservationOwned &&
      database !== undefined &&
      // Today's rollback is destructive only once ownership is proven or the
      // attempt created the Worker; an unproven no-worker failure keeps its
      // resumable row (the engine could never delete that database and would
      // wedge the operation at 'cleanup-advancing').
      (databaseOwnershipProven || workerCreated) &&
      // Legacy stacks without the bounded capabilities keep the in-memory
      // rollback: admitting the engine without a terminal or scan capability
      // would strand a durable 'cleanup-advancing' row it can never finish
      // (the same capability split decommissionDeployment already applies).
      Reflect.has(backend, 'advanceDecommissionAttachmentScan') &&
      Reflect.has(backend, 'assertDatabaseDeletionResidualsRemoved') &&
      Reflect.has(lease, 'completeCleanup')
    ) {
      // The attempt owns the reservation and the database outcome is
      // resolved: rollback runs through the bounded cleanup engine under the
      // held lease. Refusals (external candidates, WFP, authorized
      // invocation) throw before any mutation, persist no intent, and leave
      // the row at its phase; an admitted rollback is durably
      // 'cleanup-advancing' and its all-clean terminal writes the receipt in
      // place of the old bare row delete.
      const rollback = await rollbackThroughBoundedCleanup({
        lease,
        backend,
        store,
        spec,
        record,
        mode: options.failureCleanup ?? 'drain',
        authority: {
          kind: 'provisioning-rollback',
          reservationOwned: databaseReservationOwned,
          databaseOwned: databaseOwnershipProven,
          workerCreatedByAttempt: workerCreated,
          workerResourceState,
          requestedSpecDigest: deploymentSpecDigest(spec),
        },
        ...(options.clock ? { clock: options.clock } : {}),
      });
      cleanupErrors = rollback.errors;
      // The default drain keeps the historical error shape; only the
      // explicitly bounded mode surfaces the resumable outcome.
      if ((options.failureCleanup ?? 'drain') === 'bounded') {
        boundedOutcome = rollback.cleanup;
      }
    } else if (
      (record?.phase === 'database-reserved' ||
        record?.phase === 'database-create-authorized') &&
      !database
    ) {
      cleanupErrors = [
        new Error('reserved database creation outcome is unresolved'),
      ];
      await lease.put(record);
    } else {
      // Record-less, non-owned, or worker-without-database rollbacks keep the
      // pre-engine in-memory branch verbatim; the engine handles only
      // record-bearing owned rollbacks with a resolved database outcome.
      const legacy = await rollbackProvisioning(
        lease,
        backend,
        spec,
        database,
        databaseReservationOwned && workerCreated,
        workerResourceState,
        databaseReservationOwned &&
          database !== undefined &&
          databaseOwnershipProven,
        databaseReservationOwned ? record?.platformResources : undefined,
        databaseReservationOwned ? record : undefined,
      );
      record = legacy.record ?? record;
      cleanupErrors = legacy.errors;
      if (
        databaseReservationOwned &&
        cleanupErrors.length === 0 &&
        (!database || databaseOwnershipProven)
      ) {
        await lease.delete();
      } else if (record) {
        await lease.put(record);
      }
    }
    throw new ProvisioningError(
      `failed to provision '${spec.tenantTag}:${spec.environment}'${
        cleanupErrors.length > 0
          ? `; ${cleanupErrors.length} cleanup operation(s) also failed`
          : ''
      }`,
      cause,
      cleanupErrors,
      boundedOutcome,
    );
  }
}

async function rollbackThroughBoundedCleanup(input: {
  readonly lease: FleetStateLease;
  readonly backend: ProvisioningBackend;
  readonly store: FleetStateStore;
  readonly spec: DeploymentSpec;
  readonly record: FleetRecord;
  readonly mode: 'drain' | 'bounded';
  readonly authority: Readonly<{
    kind: 'provisioning-rollback';
    reservationOwned: boolean;
    databaseOwned: boolean;
    workerCreatedByAttempt: boolean;
    workerResourceState: 'absent' | 'present' | 'unknown';
    requestedSpecDigest: string;
  }>;
  readonly clock?: () => number;
}): Promise<
  Readonly<{ errors: readonly unknown[]; cleanup?: CleanupAdvanceResult }>
> {
  const { lease, backend, store, spec } = input;
  const errors: unknown[] = [];
  let cleanup: CleanupAdvanceResult | undefined;
  try {
    const admitted = await startProvisioningRollbackCleanup(
      lease,
      input.record,
      input.authority,
      {
        backend,
        spec,
        randomUUID,
        ...(input.clock ? { clock: input.clock } : {}),
      },
    );
    const intent = admitted.cleanupIntent;
    if (!intent) {
      throw new Error('bounded rollback did not persist its cleanup intent');
    }
    let token: CleanupAdvanceToken = {
      version: 1,
      tenantTag: spec.tenantTag,
      environment: spec.environment,
      operationId: intent.operationId,
      revision: intent.revision,
    };
    let action: CleanupAdvanceAction = { kind: 'continue', token };
    let restarted = false;
    while (true) {
      const engineOptions: AdvanceCleanupDeploymentOptions = {
        backend,
        store,
        spec,
        action,
        maxProviderRequests: 1_000,
        randomUUID,
        ...(input.clock ? { clock: input.clock } : {}),
      };
      const result = await advanceCleanupUnderLease(
        engineOptions,
        action,
        token,
        lease,
      );
      cleanup = result;
      if (result.status === 'complete') break;
      if (result.status === 'blocked') {
        if (restarted) {
          errors.push(
            new Error('bounded cleanup remains blocked by a Worker attachment'),
          );
          break;
        }
        restarted = true;
        token = result.token;
        action = { kind: 'restart-blocked', token };
      } else {
        token = result.token;
        action = { kind: 'continue', token };
      }
      if (input.mode === 'bounded') break;
    }
  } catch (error) {
    errors.push(error);
  }
  return { errors, ...(cleanup ? { cleanup } : {}) };
}

export interface CleanupDeploymentArtifactsOptions {
  readonly backend: ProvisioningBackend;
  readonly store: FleetStateStore;
  readonly spec: DeploymentSpec;
}

/**
 * Drains one deployment's bounded no-export cleanup to its terminal receipt.
 *
 * Holds no outer lease: each bounded advance acquires its own deployment
 * lease. An absent row is a no-op; an active cleanup intent of either
 * authority resumes; admission refusals propagate unchanged; a bounded group
 * failure surfaces as the historical `AggregateError` and leaves the durable
 * intent for retry.
 */
export async function cleanupDeploymentArtifacts(
  options: CleanupDeploymentArtifactsOptions,
): Promise<void> {
  const { backend, store, spec } = options;
  validateDeploymentSpec(spec);
  const record = await store.get(spec.tenantTag, spec.environment);
  if (!record) return;
  // Reservation-phase cleanups historically threw their provider refusals
  // directly; teardown-phase group failures aggregated. Preserve both shapes.
  const admittedPhase =
    record.cleanupIntent?.identity.admittedPhase ?? record.phase;
  const reservation =
    admittedPhase === 'database-reserved' ||
    admittedPhase === 'database-create-authorized';
  let action: CleanupAdvanceAction = { kind: 'start' };
  let restarted = false;
  while (true) {
    let result: CleanupAdvanceResult;
    try {
      result = await advanceCleanupDeployment({
        backend,
        store,
        spec,
        action,
        maxProviderRequests: 1_000,
        randomUUID,
      });
    } catch (error) {
      if (action.kind === 'start' || reservation) throw error;
      throw new AggregateError(
        [error],
        `failed to clean 1 deployment artifact(s) for '${spec.scriptName}'`,
      );
    }
    if (result.status === 'complete') return;
    if (result.status === 'blocked') {
      if (restarted) {
        throw new Error(
          'bounded cleanup remains blocked by a Worker attachment',
        );
      }
      restarted = true;
      action = { kind: 'restart-blocked', token: result.token };
      continue;
    }
    action = { kind: 'continue', token: result.token };
  }
}

export interface DecommissionDeploymentOptions {
  readonly backend: ProvisioningBackend;
  readonly store: FleetStateStore;
  readonly spec: DeploymentSpec;
  readonly clock?: () => number;
  readonly audit?: DecommissionAuditSink;
  readonly backendSwitch?: Readonly<{
    provider: BackendSwitchProvider;
    priorSpec: DeploymentSpec;
    targetSpec: DeploymentSpec;
  }>;
}

export async function decommissionDeployment(
  options: DecommissionDeploymentOptions,
): Promise<DecommissionResult> {
  const loaded = await options.store.get(
    options.spec.tenantTag,
    options.spec.environment,
  );
  const reconstructed =
    loaded === undefined
      ? undefined
      : structuralBackendSwitchFleetRecordFromUnknown(loaded);
  let current = reconstructed?.record;
  if (reconstructed?.carriesBackendSwitchAuthority) {
    current = backendSwitchFleetRecordFromUnknown(current).record;
    const currentSwitch = current.backendSwitchIntent;
    if (!currentSwitch) {
      throw new Error('backend switch decommission record is malformed');
    }
    if (currentSwitch.subphase === 'decommissioned') {
      if (!currentSwitch.databaseExport) {
        throw new Error(
          'backend switch decommission did not commit its export',
        );
      }
      const result = {
        record: current,
        databaseExport: currentSwitch.databaseExport,
      };
      await emitDecommissionAudit(options.audit, result.record, false);
      return result;
    }
    if (!options.backendSwitch) {
      throw new Error(
        'active backend switch decommission requires its dedicated provider and both specifications',
      );
    }
    const intent = await decommissionBackendSwitch({
      store: options.store,
      provider: options.backendSwitch.provider,
      priorSpec: options.backendSwitch.priorSpec,
      targetSpec: options.backendSwitch.targetSpec,
      currentSpec: options.spec,
    });
    const stored = await options.store.get(
      options.spec.tenantTag,
      options.spec.environment,
    );
    const record = backendSwitchFleetRecordFromUnknown(stored).record;
    if (!intent.databaseExport) {
      throw new Error('backend switch decommission did not commit its export');
    }
    const result = { record, databaseExport: intent.databaseExport };
    await emitDecommissionAudit(options.audit, result.record, false);
    return result;
  }
  let hasNormalIntent = false;
  if (current?.decommissionIntent !== undefined) {
    current = canonicalNormalDecommissionRecord(current);
    hasNormalIntent = true;
  }
  // After the canonicalizers: a hostile decommission-shell record carrying
  // cleanup material keeps its malformed-record refusal; a clean record with
  // an active cleanup refuses here, before any lease or backend dispatch.
  if (current) {
    assertNoActiveCleanup(current, 'decommissionDeployment');
  }
  const shellLessLatePhase =
    current !== undefined &&
    current.decommissionIntent === undefined &&
    (current.phase === 'database-exported' ||
      current.phase === 'database-deleting' ||
      current.phase === 'decommissioned');
  let useBounded = hasNormalIntent;
  if (current && !hasNormalIntent && !shellLessLatePhase) {
    useBounded =
      Reflect.has(options.backend, 'advanceDecommissionAttachmentScan') &&
      (Reflect.has(options.backend, 'databaseExportReceiptAuthority') ||
        Reflect.has(options.backend, 'exportDatabaseReceipt'));
  }
  const result = useBounded
    ? await drainBoundedDecommission(options)
    : await options.store.withDeploymentLease(
        options.spec.tenantTag,
        options.spec.environment,
        (lease) => decommissionDeploymentUnderLease(options, lease),
      );
  await emitDecommissionAudit(options.audit, result.record, false);
  return result;
}

async function drainBoundedDecommission(
  options: DecommissionDeploymentOptions,
): Promise<DecommissionResult> {
  let action: DecommissionAdvanceAction = { kind: 'start' };
  let firstResult = true;
  while (true) {
    const result = await advanceDecommissionDeployment({
      backend: options.backend,
      store: options.store,
      spec: options.spec,
      action,
      maxProviderRequests: 1_000,
      ...(options.clock ? { clock: options.clock } : {}),
      randomUUID,
    });
    if (result.status === 'complete') return result.result;
    if (result.status === 'blocked') {
      if (firstResult) {
        firstResult = false;
        action = { kind: 'restart-blocked', token: result.token };
        continue;
      }
      throw new Error(
        'bounded decommission remains blocked by a Worker attachment',
      );
    }
    firstResult = false;
    action = { kind: 'continue', token: result.token };
  }
}

function emitDecommissionAudit(
  audit: DecommissionAuditSink | undefined,
  record: FleetRecord,
  forced: boolean,
): Promise<void> {
  return Promise.resolve(
    audit?.({
      action: 'deployment-decommissioned',
      tenantTag: record.tenantTag,
      environment: record.environment,
      backend: record.backend,
      scriptName: record.scriptName,
      databaseId: record.databaseId,
      forced,
    }),
  );
}

export interface ForceDecommissionDeploymentOptions {
  readonly backend: ProvisioningBackend;
  readonly store: FleetStateStore;
  readonly tenantTag: string;
  readonly environment: string;
  readonly options?: Readonly<{
    readonly audit?: DecommissionAuditSink;
    readonly clock?: () => number;
  }>;
}

export async function forceDecommissionDeployment(
  input: ForceDecommissionDeploymentOptions,
): Promise<void> {
  await input.store.withDeploymentLease(
    input.tenantTag,
    input.environment,
    async (lease) => {
      const current = await input.store.get(input.tenantTag, input.environment);
      if (!current) return;
      assertNoActiveDecommission(current, 'forceDecommissionDeployment');
      // Blocked-cleanup plus refused-force is intentional: restart-blocked
      // after remediation is the only resolution path for a blocked cleanup.
      assertNoActiveCleanup(current, 'forceDecommissionDeployment');
      if (current.backend !== input.backend.kind) {
        throw new Error(
          'force-decommission backend does not own this deployment',
        );
      }
      if (current.phase === 'decommissioned') {
        await lease.delete();
        return;
      }
      if (current.phase === 'database-reserved') {
        await emitDecommissionAudit(input.options?.audit, current, true);
        await lease.delete();
        return;
      }
      if (current.phase === 'database-create-authorized') {
        throw new Error(
          `cannot force-decommission '${current.tenantTag}:${current.environment}' while exact D1 creation outcome is unresolved`,
        );
      }
      const forceStep = input.backend.forceDecommissionStep;
      if (!forceStep) {
        throw new Error(
          `backend '${input.backend.kind}' does not support spec-free force decommission`,
        );
      }
      assertBackendSwitchInactive(current);
      const clock = input.options?.clock ?? Date.now;
      let record = current;

      if (
        record.phase !== 'decommissioning' &&
        record.phase !== 'traffic-removed' &&
        record.phase !== 'credentials-revoked' &&
        record.phase !== 'worker-deleted' &&
        record.phase !== 'platform-credentials-revoked' &&
        record.phase !== 'platform-resources-deleted' &&
        record.phase !== 'application-resources-deleting' &&
        record.phase !== 'application-resources-deleted' &&
        record.phase !== 'database-exported' &&
        record.phase !== 'database-deleting'
      ) {
        record = {
          ...record,
          phase: 'decommissioning',
          updatedAt: nowIso(clock),
        };
        await lease.put(record);
      }

      if (
        record.phase === 'decommissioning' ||
        record.phase === 'traffic-removed'
      ) {
        await forceStep.call(input.backend, record, 'remove-traffic', lease);
        if (record.phase === 'decommissioning') {
          record = {
            ...record,
            phase: 'traffic-removed',
            updatedAt: nowIso(clock),
          };
          await lease.put(record);
        }
      }

      if (record.phase === 'traffic-removed') {
        await forceStep.call(
          input.backend,
          record,
          'revoke-credentials',
          lease,
        );
        record = {
          ...record,
          phase: 'credentials-revoked',
          updatedAt: nowIso(clock),
        };
        await lease.put(record);
      }

      if (record.phase !== 'database-deleting') {
        record = {
          ...record,
          phase: 'database-deleting',
          updatedAt: nowIso(clock),
        };
        await lease.put(record);
      }
      await forceStep.call(input.backend, record, 'delete-database', lease);
      record = {
        ...record,
        phase: 'decommissioned',
        updatedAt: nowIso(clock),
      };
      await lease.put(record);
      await emitDecommissionAudit(input.options?.audit, record, true);
      // Capable stores release this deployment's current claims with the row;
      // legacy lease implementations keep tombstone claims through delete().
      if (
        Reflect.has(lease, 'deleteReleasingClaims') &&
        typeof lease.deleteReleasingClaims === 'function'
      ) {
        await lease.deleteReleasingClaims();
      } else {
        await lease.delete();
      }
    },
  );
}

async function decommissionDeploymentUnderLease(
  options: DecommissionDeploymentOptions,
  lease: FleetStateLease,
): Promise<DecommissionResult> {
  const { backend, store, spec } = options;
  const clock = options.clock ?? Date.now;
  validateDeploymentSpec(spec);
  const current = await store.get(spec.tenantTag, spec.environment);
  if (!current) throw new Error('deployment is not registered');
  assertNoActiveDecommission(current, 'decommissionDeployment');
  assertNoActiveCleanup(current, 'decommissionDeployment');
  assertBackendSwitchInactive(current);
  if (current.backend !== backend.kind) {
    throw new Error('decommission backend does not own this deployment');
  }
  assertImmutableDeploymentMapping(current, backend, spec);
  if (
    current.phase !== 'publishing' &&
    current.phase !== 'ready' &&
    current.phase !== 'migrating' &&
    current.phase !== 'rolling-back' &&
    current.phase !== 'decommissioning' &&
    current.phase !== 'traffic-removed' &&
    current.phase !== 'credentials-revoked' &&
    current.phase !== 'worker-deleted' &&
    current.phase !== 'platform-credentials-revoked' &&
    current.phase !== 'platform-resources-deleted' &&
    current.phase !== 'application-resources-deleting' &&
    current.phase !== 'application-resources-deleted' &&
    current.phase !== 'database-exported' &&
    current.phase !== 'database-deleting' &&
    current.phase !== 'decommissioned'
  ) {
    throw new Error(
      `cannot decommission deployment in phase '${current.phase}'`,
    );
  }
  const database: DatabaseReference = {
    id: current.databaseId,
    name: current.databaseName,
    created: false,
  };
  const liveDatabase =
    current.phase === 'decommissioned'
      ? undefined
      : await reconcilePersistedDatabase(
          backend,
          current,
          current.phase === 'database-deleting',
          lease,
        );
  let record = current;
  if (
    record.phase === 'publishing' ||
    record.phase === 'ready' ||
    record.phase === 'migrating' ||
    record.phase === 'rolling-back'
  ) {
    await assertApplicationR2EmptyBeforeDecommission({
      resources: record.applicationResources ?? [],
      backend,
      fence: lease,
    });
    record = { ...record, phase: 'decommissioning', updatedAt: nowIso(clock) };
    await lease.put(record);
  }
  if (record.phase === 'decommissioning') {
    await lease.assertOwned();
    await backend.removeTraffic(
      spec,
      retainedExternalReleases(record),
      activeExternalRelease(record),
      database,
      lease,
    );
    await backend.assertTrafficRemoved(spec);
    record = {
      ...record,
      phase: 'traffic-removed',
      updatedAt: nowIso(clock),
    };
    await lease.put(record);
  }
  if (record.phase === 'traffic-removed') {
    await backend.assertTrafficRemoved(spec);
    await assertApplicationR2EmptyBeforeDecommission({
      resources: record.applicationResources ?? [],
      backend,
      fence: lease,
    });
    await lease.assertOwned();
    await backend.revokeCredentials(
      spec,
      retainedExternalReleases(record),
      activeExternalRelease(record),
      database,
      lease,
    );
    record = {
      ...record,
      phase: 'credentials-revoked',
      updatedAt: nowIso(clock),
    };
    await lease.put(record);
  }
  if (record.phase === 'credentials-revoked') {
    await lease.assertOwned();
    await backend.deleteWorker(
      spec,
      retainedExternalReleases(record),
      database,
      activeExternalRelease(record),
      lease,
    );
    if (!record.platformResources) {
      await backend.assertDatabaseDetached(spec, record, database, lease);
    }
    record = { ...record, phase: 'worker-deleted', updatedAt: nowIso(clock) };
    await lease.put(record);
  }
  if (record.phase === 'worker-deleted') {
    if (record.platformResources) {
      if (!backend.revokePlatformResourceCredentials) {
        throw new Error(
          'backend cannot revoke trusted platform resource credentials',
        );
      }
      await lease.assertOwned();
      await backend.revokePlatformResourceCredentials(
        spec,
        record,
        database,
        lease,
      );
    }
    record = {
      ...record,
      phase: 'platform-credentials-revoked',
      updatedAt: nowIso(clock),
    };
    await lease.put(record);
  }
  if (record.phase === 'platform-credentials-revoked') {
    if (record.platformResources) {
      if (!backend.deletePlatformResources) {
        throw new Error('backend cannot delete trusted platform resources');
      }
      await lease.assertOwned();
      await backend.deletePlatformResources(spec, record, database, lease);
      await backend.assertDatabaseDetached(spec, record, database, lease);
    }
    record = {
      ...record,
      phase: 'platform-resources-deleted',
      updatedAt: nowIso(clock),
    };
    await lease.put(record);
  }
  if (record.phase === 'platform-resources-deleted') {
    record = {
      ...record,
      phase: 'application-resources-deleting',
      updatedAt: nowIso(clock),
    };
    await lease.put(record);
  }
  if (record.phase === 'application-resources-deleting') {
    const applicationResources = await convergeApplicationR2Deletion({
      spec,
      resources: record.applicationResources ?? [],
      backend,
      fence: lease,
      persist: async (resources) => {
        record = {
          ...record,
          applicationResources: resources,
          updatedAt: nowIso(clock),
        };
        await lease.put(record);
      },
    });
    record = {
      ...record,
      phase: 'application-resources-deleted',
      applicationResources,
      updatedAt: nowIso(clock),
    };
    await lease.put(record);
  }
  if (
    record.phase === 'application-resources-deleted' ||
    record.phase === 'database-exported' ||
    record.phase === 'database-deleting' ||
    record.phase === 'decommissioned'
  ) {
    assertNormalDecommissionD1ResourcesDeleted(record);
  }
  if (record.phase === 'application-resources-deleted') {
    await backend.assertDatabaseDetached(spec, record, database, lease);
    const exported = await backend.exportDatabase(database, lease);
    if (exported.databaseId !== record.databaseId) {
      throw new Error(
        `database export returned unexpected database '${exported.databaseId}' instead of persisted database '${record.databaseId}'`,
      );
    }
    record = {
      ...record,
      phase: 'database-exported',
      databaseExportLocation: exported.location,
      databaseExportSha256: exported.sha256,
      databaseExportSize: exported.size,
      updatedAt: nowIso(clock),
    };
    await lease.put(record);
  }
  const exportLocation = record.databaseExportLocation;
  const exportSha256 = record.databaseExportSha256;
  const exportSize = record.databaseExportSize;
  if (
    !exportLocation ||
    !exportSha256 ||
    !isSha256(exportSha256) ||
    exportSize === undefined ||
    !Number.isSafeInteger(exportSize) ||
    exportSize < 1
  ) {
    throw new Error(
      'decommissioned deployment has no durable, non-empty database export with SHA-256 integrity',
    );
  }
  const databaseExport: DatabaseExport = {
    databaseId: database.id,
    location: exportLocation,
    sha256: exportSha256,
    size: exportSize,
  };
  if (record.phase === 'database-exported') {
    record = {
      ...record,
      phase: 'database-deleting',
      updatedAt: nowIso(clock),
    };
    await lease.put(record);
  }
  if (record.phase === 'database-deleting') {
    if (liveDatabase) {
      await backend.assertDatabaseDetached(spec, record, liveDatabase, lease);
      await backend.deleteDatabase(liveDatabase, lease);
    }
    if (await backend.getDatabase(record.databaseId)) {
      throw new Error(`database '${record.databaseId}' remains after deletion`);
    }
    record = { ...record, phase: 'decommissioned', updatedAt: nowIso(clock) };
    await lease.put(record);
  }
  return { record, databaseExport };
}
