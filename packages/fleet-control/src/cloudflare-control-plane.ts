// SPDX-License-Identifier: Apache-2.0

import type { D1Database } from '@cloudflare/workers-types';
import type { AttestConvergedActiveRouteOptions } from './active-route.js';
import {
  advanceCleanupDeployment,
  type CleanupAdvanceAction,
  type CleanupAdvanceResult,
} from './cleanup-advance.js';
import { CloudflareApiPlainWorkerBackend } from './cloudflare-api-plain-worker-backend.js';
import {
  CloudflareProvisioningClient,
  cloudflareFleetInventoryContext,
} from './cloudflare-client.js';
import { D1CloudflareApiRateCoordinator } from './cloudflare-rate-coordinator.js';
import { D1FleetInventoryRunStore } from './d1-fleet-inventory-run-store.js';
import { D1FleetOperationStore } from './d1-fleet-operation-store.js';
import { D1FleetStateDatabase } from './d1-fleet-state-database.js';
import {
  advanceDecommissionDeployment,
  type DecommissionAdvanceAction,
  type DecommissionAdvanceResult,
} from './decommission-advance.js';
import { rollbackExternalRelease } from './fleet.js';
import {
  abandonFleetAuditOperation,
  advanceFleetAudit,
  type FleetAuditAdvanceAction,
  type FleetAuditAdvanceResult,
  type FleetAuditFindingsPage,
  readFleetAuditFindingsPage,
} from './fleet-audit-advance.js';
import {
  advanceFleetInventory,
  type FleetInventoryAdvanceResult,
  readFleetInventoryGeneration,
} from './fleet-inventory-advance.js';
import type { FleetInventoryGenerationRef } from './fleet-inventory-state.js';
import {
  abandonFleetMigrationOperation,
  advanceFleetMigration,
  type FleetMigrationAdvanceAction,
  type FleetMigrationAdvanceResult,
  type FleetMigrationResultRef,
  readFleetMigrationItemsPage,
} from './fleet-migration-advance.js';
import type { FleetMigrationItem } from './fleet-migration-state.js';
import type { FleetOperationKind } from './fleet-operation-state.js';
import { provisionDeployment } from './provision.js';
import {
  R2DatabaseExportStore,
  type R2DatabaseExportStoreOptions,
} from './r2-export-store.js';
import { D1FleetStateStore } from './state-store.js';
import type {
  CleanupTerminalReceipt,
  DeploymentSecrets,
  DeploymentSpec,
  ExternalPlatformProfile,
  FleetRecord,
  FleetResourceInventory,
  FleetSettlementHost,
  FleetStateStore,
  InitialExecutionFenceState,
  ProvisioningBackend,
  ProvisioningResult,
} from './types.js';
import { WorkersForPlatformsBackend } from './workers-for-platforms-backend.js';

export { ActiveRouteAttestationError } from './active-route.js';
export {
  type CleanupAdvanceCapability,
  CleanupAdvanceCapabilityError,
  CleanupAdvanceRestartError,
} from './cleanup-advance.js';
export {
  CleanupAdvanceTokenDeploymentError,
  CleanupAdvanceTokenError,
  CleanupAdvanceTokenFutureError,
  CleanupAdvanceTokenOperationError,
} from './cleanup-intent.js';
export {
  type CloudflareApiRateCoordinator,
  D1CloudflareApiRateCoordinator,
  type D1CloudflareApiRateCoordinatorOptions,
} from './cloudflare-rate-coordinator.js';
export { D1FleetStateDatabase } from './d1-fleet-state-database.js';
export type { DurableDatabaseExportStore } from './database-export-store.js';
export {
  type DecommissionAdvanceCapability,
  DecommissionAdvanceCapabilityError,
  DecommissionAdvanceRestartError,
} from './decommission-advance.js';
export {
  DecommissionAdvanceTokenDeploymentError,
  DecommissionAdvanceTokenError,
  DecommissionAdvanceTokenFutureError,
  DecommissionAdvanceTokenOperationError,
} from './decommission-intent.js';
export { WorkerDeploymentError } from './deployment-error.js';
export type { DriftFinding } from './fleet.js';
export {
  type FleetAuditAdvanceCapability,
  FleetAuditAdvanceCapabilityError,
  type FleetAuditResultRef,
} from './fleet-audit-advance.js';
export type { FleetAuditStage } from './fleet-audit-state.js';
export {
  type FleetInventoryAdvanceCapability,
  FleetInventoryAdvanceCapabilityError,
} from './fleet-inventory-advance.js';
export {
  FleetInventoryFindingValueError,
  type FleetInventoryRowKind,
  type FleetInventoryRunToken,
  FleetInventoryRunTokenError,
  FleetInventoryRunTokenFutureError,
  FleetInventoryRunTokenOperationError,
  FleetInventoryStateError,
} from './fleet-inventory-state.js';
export {
  FleetMigrationAdvanceCapabilityError,
  type FleetMigrationResultRef,
} from './fleet-migration-advance.js';
export type {
  FleetMigrationPlanEntry,
  FleetMigrationStep,
} from './fleet-migration-state.js';
export {
  type FleetOperationFailure,
  FleetOperationStateError,
  FleetOperationStoreCapabilityError,
  type FleetOperationToken,
  FleetOperationTokenError,
  FleetOperationTokenFutureError,
  FleetOperationTokenKindError,
  FleetOperationTokenOperationError,
} from './fleet-operation-state.js';
export type { HostRoutingTarget } from './host-routing.js';
export { ProvisioningError } from './provision.js';
export {
  type DigestStreamConstructor,
  type FixedLengthStreamConstructor,
  R2DatabaseExportStore,
  type R2DatabaseExportStoreStreamPrimitives,
} from './r2-export-store.js';
export { generateDeploymentSecrets } from './secrets.js';
export { deploymentSpecDigest } from './spec-digest.js';
export type { FleetStateDatabase } from './state-store.js';
export type {
  ActiveRouteAttestation,
  ApplicationBindingTopology,
  ApplicationR2Binding,
  ApplicationR2Resource,
  BackendSwitchApplicationR2Progress,
  BackendSwitchCandidateSnapshot,
  BackendSwitchDecommissionRelease,
  BackendSwitchDecommissionRouteTarget,
  BackendSwitchDecommissionSnapshot,
  BackendSwitchIntent,
  BackendSwitchSubphase,
  BridgeMutationPlan,
  BridgeSnapshot,
  CleanupAdvanceIntent,
  CleanupAdvanceState,
  CleanupAdvanceToken,
  CleanupAttachmentProgress,
  CleanupAttachmentPurpose,
  CleanupAttachmentScan,
  CleanupAuthority,
  CleanupReceiptEvidence,
  D1Migration,
  DatabaseExport,
  DatabaseExportIntegrity,
  DatabaseExportReceiptIdentity,
  DecommissionAdvanceIntent,
  DecommissionAdvanceToken,
  DecommissionAttachmentProgress,
  DecommissionAttachmentPurpose,
  DecommissionAttachmentScanEvidence,
  DecommissionBlockedAttachment,
  DecommissionIntentCommon,
  DecommissionOperationIdentity,
  DecommissionOperationMode,
  DecommissionRecordIdentity,
  DecommissionResult,
  DeploymentApplicationBindings,
  DeploymentEgressPolicy,
  DurableObjectBindingInventory,
  DurableObjectMigration,
  ExternalMigrationIntent,
  ExternalMigrationSubphase,
  ExternalPlatformProfile,
  ExternalPlatformResources,
  ExternalPlatformTargetDescription,
  ExternalReleaseSnapshot,
  ExternalReleaseTopology,
  FleetInventoryDeployment,
  FleetInventoryFinding,
  FleetInventoryR2Jurisdiction,
  FleetSettlementContext,
  FleetSettlementEntry,
  InvocationAuthorityCarrier,
  MaintenanceHealth,
  NormalDecommissionLifecyclePhase,
  ObservedActiveRoute,
  PlainBackendSnapshot,
  PlatformWorkerSnapshot,
  ProvisioningBackendKind,
  ProvisioningPhase,
  R2Jurisdiction,
  TrustedWorkerArtifact,
  WorkerModule,
  WorkerZoneRoute,
} from './types.js';
export type {
  AttestConvergedActiveRouteOptions,
  CleanupAdvanceAction,
  CleanupAdvanceResult,
  CleanupTerminalReceipt,
  DecommissionAdvanceAction,
  DecommissionAdvanceResult,
  DeploymentSecrets,
  DeploymentSpec,
  FleetAuditAdvanceAction,
  FleetAuditAdvanceResult,
  FleetAuditFindingsPage,
  FleetInventoryAdvanceResult,
  FleetInventoryGenerationRef,
  FleetMigrationAdvanceAction,
  FleetMigrationAdvanceResult,
  FleetMigrationItem,
  FleetOperationKind,
  FleetRecord,
  FleetResourceInventory,
  FleetSettlementHost,
  InitialExecutionFenceState,
  ProvisioningResult,
  R2DatabaseExportStoreOptions,
};

export interface CloudflareControlPlaneOptions {
  readonly accountId: string;
  readonly apiToken: string;
  readonly fleetDatabase: D1Database;
  readonly quotaDatabase: D1Database;
  readonly quotaScope: string;
  readonly databaseExports: R2DatabaseExportStoreOptions;
  readonly leaseTtlMs?: number;
  readonly leaseRenewalIntervalMs?: number;
  readonly concurrency?: number;
  readonly requestTimeoutMs?: number;
  readonly fetch?: typeof fetch;
  readonly maintenanceFetch?: typeof fetch;
  readonly maintenanceRequestTimeoutMs?: number;
  readonly clock?: () => number;
  readonly randomUUID?: () => string;
}

export type CloudflareDeploymentSpec = Omit<
  DeploymentSpec,
  'authoredBy' | 'durableObjectBindings'
> & {
  readonly authoredBy: 'platform';
  readonly durableObjectBindings: readonly Readonly<{
    name: string;
    className: string;
    scriptName?: string;
  }>[];
};

export interface CloudflareProvisionDeploymentOptions<
  Spec extends DeploymentSpec = CloudflareDeploymentSpec,
> {
  readonly spec: Spec;
  readonly secrets: DeploymentSecrets;
  readonly initialExecutionFenceState: InitialExecutionFenceState;
  readonly routeAttestation?: AttestConvergedActiveRouteOptions;
  readonly clock?: () => number;
}

export interface CloudflareAdvanceCleanupDeploymentOptions<
  Spec extends DeploymentSpec = CloudflareDeploymentSpec,
> {
  readonly spec: Spec;
  readonly action: CleanupAdvanceAction;
  readonly maxProviderRequests: number;
  readonly signal?: AbortSignal;
  readonly clock?: () => number;
}

export interface CloudflareAdvanceDecommissionDeploymentOptions<
  Spec extends DeploymentSpec = CloudflareDeploymentSpec,
> {
  readonly spec: Spec;
  readonly action: DecommissionAdvanceAction;
  readonly maxProviderRequests: number;
  readonly signal?: AbortSignal;
  readonly clock?: () => number;
}

export interface CloudflareFleetInventoryOptions {
  readonly databaseNamePrefix: string;
  readonly scriptNamePrefix: string;
  readonly includeR2Buckets?: boolean;
}

export type CloudflareFleetInventoryAdvanceAction =
  | Readonly<{
      kind: 'start';
      operationId: string;
      options: CloudflareFleetInventoryOptions;
    }>
  | Readonly<{ kind: 'continue'; token: unknown }>;

export interface CloudflareAdvanceFleetInventoryOptions {
  readonly action: CloudflareFleetInventoryAdvanceAction;
  readonly maxProviderRequests: number;
  readonly maxStagedRowsPerChunk?: number;
  readonly signal?: AbortSignal;
}

export interface CloudflareAdvanceFleetAuditOptions<
  Spec extends DeploymentSpec = CloudflareDeploymentSpec,
> {
  readonly action: FleetAuditAdvanceAction;
  readonly specFor: (record: FleetRecord) => Spec;
  readonly maintenanceSecretFor: (record: FleetRecord) => string;
  readonly maxItemsPerCall?: number;
  readonly auditClock?: () => number;
  readonly authorityClock?: () => number;
  readonly signal?: AbortSignal;
}

export interface CloudflareAdvanceFleetMigrationOptions<
  Spec extends DeploymentSpec = CloudflareDeploymentSpec,
> {
  readonly action: FleetMigrationAdvanceAction;
  readonly specFor: (record: FleetRecord) => Spec;
  readonly secretsFor: (record: FleetRecord) => DeploymentSecrets;
  readonly settlementFor?: (
    record: FleetRecord,
  ) => FleetSettlementHost | undefined;
  readonly routeAttestation?: AttestConvergedActiveRouteOptions;
  readonly clock?: () => number;
  readonly signal?: AbortSignal;
  /**
   * Carries the delivery contract of
   * {@link index!AdvanceFleetMigrationOptions.onComplete |
   * AdvanceFleetMigrationOptions.onComplete}.
   */
  readonly onComplete?: (
    result: FleetMigrationResultRef,
  ) => void | Promise<void>;
}

export interface CloudflareFleetOperationPageOptions {
  readonly operationId: string;
  readonly afterOrdinal?: number;
  readonly limit: number;
}

export interface CloudflareFleetMigrationItemsPage {
  readonly items: readonly FleetMigrationItem[];
  readonly done: boolean;
}

export interface CloudflareControlPlane<
  Spec extends DeploymentSpec = CloudflareDeploymentSpec,
> {
  provisionDeployment(
    options: CloudflareProvisionDeploymentOptions<Spec>,
  ): Promise<ProvisioningResult>;
  advanceCleanupDeployment(
    options: CloudflareAdvanceCleanupDeploymentOptions<Spec>,
  ): Promise<CleanupAdvanceResult>;
  advanceDecommissionDeployment(
    options: CloudflareAdvanceDecommissionDeploymentOptions<Spec>,
  ): Promise<DecommissionAdvanceResult>;
  advanceFleetInventory(
    options: CloudflareAdvanceFleetInventoryOptions,
  ): Promise<FleetInventoryAdvanceResult>;
  readFleetInventoryGeneration(
    generation: number,
  ): Promise<FleetResourceInventory>;
  latestFinalizedInventoryGeneration(): Promise<
    FleetInventoryGenerationRef | undefined
  >;
  advanceFleetAudit(
    options: CloudflareAdvanceFleetAuditOptions<Spec>,
  ): Promise<FleetAuditAdvanceResult>;
  readFleetAuditFindingsPage(
    options: CloudflareFleetOperationPageOptions,
  ): Promise<FleetAuditFindingsPage>;
  abandonFleetAuditOperation(operationId: string): Promise<void>;
  advanceFleetMigration(
    options: CloudflareAdvanceFleetMigrationOptions<Spec>,
  ): Promise<FleetMigrationAdvanceResult>;
  readFleetMigrationItemsPage(
    options: CloudflareFleetOperationPageOptions,
  ): Promise<CloudflareFleetMigrationItemsPage>;
  abandonFleetMigrationOperation(operationId: string): Promise<void>;
  getDeployment(
    tenantTag: string,
    environment: string,
  ): Promise<FleetRecord | undefined>;
  readCleanupReceipt(
    operationId: string,
  ): Promise<CleanupTerminalReceipt | undefined>;
  pruneCleanupReceipts(
    input: Readonly<{ completedBeforeMs: number; limit: number }>,
  ): Promise<Readonly<{ deleted: number }>>;
  pruneInventoryGenerations(
    input: Readonly<{ limit: number }>,
  ): Promise<Readonly<{ deleted: number }>>;
  pruneFleetOperations(
    input: Readonly<{ kind: FleetOperationKind; limit: number }>,
  ): Promise<Readonly<{ deleted: number; releasedPins: number }>>;
}

export type CloudflareWorkersForPlatformsDeploymentSpec = Omit<
  DeploymentSpec,
  'authoredBy' | 'durableObjectBindings'
> & {
  readonly authoredBy: 'external';
  readonly durableObjectBindings: readonly Readonly<{
    name: string;
    className: string;
  }>[];
};

export interface CloudflareWorkersForPlatformsControlPlaneOptions
  extends CloudflareControlPlaneOptions {
  readonly dispatchNamespace: string;
  readonly hostRoutingKvId: string;
  readonly auditQueueName: string;
  readonly sharedOutboundWorkerName: string;
  readonly stateEgressRootSecret: string;
  readonly platformProfileFor: (
    spec: CloudflareWorkersForPlatformsDeploymentSpec,
  ) => ExternalPlatformProfile;
}

export interface CloudflareRollbackExternalReleaseOptions {
  readonly currentSpec: CloudflareWorkersForPlatformsDeploymentSpec;
  readonly rollbackSpec: CloudflareWorkersForPlatformsDeploymentSpec;
  readonly secrets: DeploymentSecrets;
  readonly settlement?: FleetSettlementHost;
  readonly routeAttestation?: AttestConvergedActiveRouteOptions;
  readonly clock?: () => number;
}

export interface CloudflareWorkersForPlatformsControlPlane
  extends Omit<
    CloudflareControlPlane<CloudflareWorkersForPlatformsDeploymentSpec>,
    'advanceCleanupDeployment'
  > {
  rollbackExternalRelease(
    options: CloudflareRollbackExternalReleaseOptions,
  ): Promise<FleetRecord>;
}

function ordinarySpec(spec: DeploymentSpec): CloudflareDeploymentSpec {
  if (spec.authoredBy !== 'platform') {
    throw new TypeError(
      'Cloudflare control plane requires platform-authored specifications',
    );
  }
  for (const binding of spec.durableObjectBindings) {
    if (binding.dispatchNamespace !== undefined) {
      throw new TypeError(
        'Cloudflare control plane cannot use dispatch namespace bindings',
      );
    }
  }
  return spec as CloudflareDeploymentSpec;
}

export function createCloudflareControlPlane(
  options: CloudflareControlPlaneOptions,
): CloudflareControlPlane {
  const { maintenanceFetch, maintenanceRequestTimeoutMs } = options;
  return createControlPlane(options, {
    clientPlane: { plane: 'plain-worker' },
    spec: ordinarySpec,
    inventoryScope: { includeDispatchNamespace: false },
    backend: (client, clock) =>
      new CloudflareApiPlainWorkerBackend({
        client,
        fetch: maintenanceFetch,
        maintenanceRequestTimeoutMs,
        clock,
      }),
    record(record) {
      if (record.backend !== 'plain-worker') {
        throw new TypeError(
          'Cloudflare control plane requires plain-worker records',
        );
      }
    },
  }).controlPlane;
}

function externalSpec(
  spec: DeploymentSpec,
): CloudflareWorkersForPlatformsDeploymentSpec {
  if (spec.authoredBy !== 'external') {
    throw new TypeError(
      'Workers for Platforms control plane requires external specifications',
    );
  }
  for (const binding of spec.durableObjectBindings) {
    if (
      binding.scriptName !== undefined ||
      binding.dispatchNamespace !== undefined
    ) {
      throw new TypeError(
        'Workers for Platforms control plane owns Durable Object binding targets',
      );
    }
  }
  return spec as CloudflareWorkersForPlatformsDeploymentSpec;
}

export function createCloudflareWorkersForPlatformsControlPlane(
  options: CloudflareWorkersForPlatformsControlPlaneOptions,
): CloudflareWorkersForPlatformsControlPlane {
  const {
    dispatchNamespace,
    hostRoutingKvId,
    auditQueueName,
    sharedOutboundWorkerName,
    stateEgressRootSecret,
    maintenanceFetch,
    maintenanceRequestTimeoutMs,
  } = options;
  if (!auditQueueName) throw new TypeError('auditQueueName is required');
  const platformProfileFor = options.platformProfileFor.bind(options);
  const { controlPlane, fleetStore, backend } = createControlPlane(options, {
    clientPlane: { dispatchNamespace },
    spec: externalSpec,
    inventoryScope: {
      includeDispatchNamespace: true,
      dispatchNamespace,
      hostRoutingKvId,
    },
    backend: (client, clock) =>
      new WorkersForPlatformsBackend({
        client,
        fetch: maintenanceFetch,
        maintenanceRequestTimeoutMs,
        clock,
        hostRoutingKvId,
        auditQueueName,
        platformProfileFor: (spec) => platformProfileFor(externalSpec(spec)),
        namespacedState: {
          dispatchNamespace,
          sharedOutboundWorkerName,
          stateEgressRootSecret,
        },
      }),
    guardLeases: true,
    record(record) {
      if (
        record.backend !== 'workers-for-platforms' ||
        record.wfpMode === 'platform-catalog' ||
        record.backendSwitchIntent !== undefined
      ) {
        throw new TypeError(
          'Workers for Platforms control plane requires dispatch-native external records',
        );
      }
      const resources = record.platformResources;
      if (
        resources &&
        (resources.stateWorker.plane !== 'dispatch' ||
          resources.stateWorker.dispatchNamespace !== dispatchNamespace ||
          resources.sharedOutboundWorkerName !== sharedOutboundWorkerName ||
          (resources.auditQueueName !== undefined &&
            resources.auditQueueName !== auditQueueName))
      ) {
        throw new TypeError(
          'Workers for Platforms record does not match the configured platform',
        );
      }
    },
  });
  const { advanceCleanupDeployment: _cleanup, ...supported } = controlPlane;
  return Object.freeze({
    ...supported,
    async rollbackExternalRelease(
      input: CloudflareRollbackExternalReleaseOptions,
    ) {
      return rollbackExternalRelease({
        store: fleetStore,
        backend,
        currentSpec: externalSpec(input.currentSpec),
        rollbackSpec: externalSpec(input.rollbackSpec),
        secrets: input.secrets,
        settlement: input.settlement,
        routeAttestation: input.routeAttestation,
        clock: input.clock?.bind(input),
      });
    },
  });
}

function createControlPlane<Spec extends DeploymentSpec>(
  options: CloudflareControlPlaneOptions,
  configuration: Readonly<{
    clientPlane:
      | Readonly<{ plane: 'plain-worker' }>
      | Readonly<{ dispatchNamespace: string }>;
    spec: (spec: DeploymentSpec) => Spec;
    backend: (
      client: CloudflareProvisioningClient,
      clock: (() => number) | undefined,
    ) => ProvisioningBackend;
    record: (record: FleetRecord) => void;
    guardLeases?: boolean;
    inventoryScope: Readonly<{
      includeDispatchNamespace: boolean;
      dispatchNamespace?: string;
      hostRoutingKvId?: string;
    }>;
  }>,
): Readonly<{
  controlPlane: CloudflareControlPlane<Spec>;
  fleetStore: FleetStateStore;
  backend: ProvisioningBackend;
}> {
  const {
    accountId,
    apiToken,
    fleetDatabase,
    quotaDatabase,
    quotaScope,
    databaseExports,
    leaseTtlMs,
    leaseRenewalIntervalMs,
    concurrency,
    requestTimeoutMs,
    fetch: providerFetch,
  } = options;
  const clock = options.clock?.bind(options);
  const randomUUID =
    options.randomUUID?.bind(options) ?? (() => crypto.randomUUID());
  const database = new D1FleetStateDatabase(fleetDatabase);
  const storeOptions = { accountId, leaseTtlMs, leaseRenewalIntervalMs };
  const durableFleetStore = new D1FleetStateStore(database, storeOptions);
  const fleetStore: FleetStateStore = configuration.guardLeases
    ? {
        async withDeploymentLease(tenantTag, environment, operation) {
          return durableFleetStore.withDeploymentLease(
            tenantTag,
            environment,
            async (lease) => {
              const record = await durableFleetStore.get(
                tenantTag,
                environment,
              );
              if (record) configuration.record(record);
              return operation(lease);
            },
          );
        },
        async get(tenantTag, environment) {
          const record = await durableFleetStore.get(tenantTag, environment);
          if (record) configuration.record(record);
          return record;
        },
        async list() {
          const records = await durableFleetStore.list();
          for (const record of records) configuration.record(record);
          return records;
        },
        readCleanupReceipt: (operationId) =>
          durableFleetStore.readCleanupReceipt(operationId),
        pruneCleanupReceipts: (input) =>
          durableFleetStore.pruneCleanupReceipts(input),
      }
    : durableFleetStore;
  const inventoryStore = new D1FleetInventoryRunStore(database, storeOptions);
  const operationStore = new D1FleetOperationStore(database, {
    accountId,
    leaseTtlMs,
    leaseRenewalIntervalMs,
    inventoryStore,
  });
  const rateCoordinator = new D1CloudflareApiRateCoordinator(quotaDatabase, {
    quotaScope,
  });
  const exportStore = new R2DatabaseExportStore(databaseExports);
  const client = new CloudflareProvisioningClient({
    accountId,
    apiToken,
    ...configuration.clientPlane,
    rateCoordinator,
    exportStore,
    concurrency,
    requestTimeoutMs,
    fetch: providerFetch,
  });
  const backend = configuration.backend(client, clock);
  const backendFor = (record: FleetRecord): ProvisioningBackend => {
    configuration.record(record);
    return backend;
  };

  const controlPlane: CloudflareControlPlane<Spec> = Object.freeze({
    async provisionDeployment(
      input: CloudflareProvisionDeploymentOptions<Spec>,
    ) {
      return provisionDeployment({
        backend,
        store: fleetStore,
        spec: configuration.spec(input.spec),
        secrets: input.secrets,
        initialExecutionFenceState: input.initialExecutionFenceState,
        routeAttestation: input.routeAttestation,
        failureCleanup: 'bounded',
        clock: input.clock?.bind(input),
      });
    },
    async advanceCleanupDeployment(
      input: CloudflareAdvanceCleanupDeploymentOptions<Spec>,
    ) {
      return advanceCleanupDeployment({
        backend,
        store: fleetStore,
        spec: configuration.spec(input.spec),
        action: input.action,
        maxProviderRequests: input.maxProviderRequests,
        signal: input.signal,
        clock: input.clock?.bind(input),
        randomUUID,
      });
    },
    async advanceDecommissionDeployment(
      input: CloudflareAdvanceDecommissionDeploymentOptions<Spec>,
    ) {
      return advanceDecommissionDeployment({
        backend,
        store: fleetStore,
        spec: configuration.spec(input.spec),
        action: input.action,
        maxProviderRequests: input.maxProviderRequests,
        signal: input.signal,
        clock: input.clock?.bind(input),
        randomUUID,
      });
    },
    async advanceFleetInventory(input: CloudflareAdvanceFleetInventoryOptions) {
      const context = cloudflareFleetInventoryContext(client);
      const action = input.action;
      return advanceFleetInventory({
        context,
        store: inventoryStore,
        expectedScope: configuration.inventoryScope,
        action:
          action.kind === 'start'
            ? {
                kind: 'start',
                operationId: action.operationId,
                options: {
                  databaseNamePrefix: action.options.databaseNamePrefix,
                  scriptNamePrefix: action.options.scriptNamePrefix,
                  includeR2Buckets: action.options.includeR2Buckets,
                  ...configuration.inventoryScope,
                },
              }
            : { kind: action.kind, token: action.token },
        maxProviderRequests: input.maxProviderRequests,
        maxStagedRowsPerChunk: input.maxStagedRowsPerChunk,
        signal: input.signal,
      });
    },
    readFleetInventoryGeneration: (generation: number) =>
      readFleetInventoryGeneration(inventoryStore, generation),
    latestFinalizedInventoryGeneration: () =>
      inventoryStore.latestFinalizedGeneration(),
    async advanceFleetAudit(input: CloudflareAdvanceFleetAuditOptions<Spec>) {
      const specFor = input.specFor.bind(input);
      return advanceFleetAudit({
        operationStore,
        inventoryStore,
        fleetStore,
        backendFor,
        specFor: (record) => configuration.spec(specFor(record)),
        maintenanceSecretFor: input.maintenanceSecretFor.bind(input),
        action: input.action,
        maxItemsPerCall: input.maxItemsPerCall,
        auditClock: input.auditClock?.bind(input),
        authorityClock: input.authorityClock?.bind(input),
        signal: input.signal,
      });
    },
    readFleetAuditFindingsPage: (input: CloudflareFleetOperationPageOptions) =>
      readFleetAuditFindingsPage(operationStore, {
        operationId: input.operationId,
        afterOrdinal: input.afterOrdinal,
        limit: input.limit,
      }),
    abandonFleetAuditOperation: (operationId: string) =>
      abandonFleetAuditOperation({
        operationStore,
        inventoryStore,
        operationId,
      }),
    async advanceFleetMigration(
      input: CloudflareAdvanceFleetMigrationOptions<Spec>,
    ) {
      const specFor = input.specFor.bind(input);
      return advanceFleetMigration({
        operationStore,
        fleetStore,
        backendFor,
        specFor: (record) => configuration.spec(specFor(record)),
        secretsFor: input.secretsFor.bind(input),
        settlementFor: input.settlementFor?.bind(input),
        action: input.action,
        routeAttestation: input.routeAttestation,
        clock: input.clock?.bind(input),
        signal: input.signal,
        onComplete: input.onComplete?.bind(input),
      });
    },
    readFleetMigrationItemsPage: (input: CloudflareFleetOperationPageOptions) =>
      readFleetMigrationItemsPage(operationStore, {
        operationId: input.operationId,
        afterOrdinal: input.afterOrdinal,
        limit: input.limit,
      }),
    abandonFleetMigrationOperation: (operationId: string) =>
      abandonFleetMigrationOperation({ operationStore, operationId }),
    getDeployment: (tenantTag: string, environment: string) =>
      fleetStore.get(tenantTag, environment),
    readCleanupReceipt: (operationId: string) =>
      durableFleetStore.readCleanupReceipt(operationId),
    pruneCleanupReceipts: (
      input: Readonly<{ completedBeforeMs: number; limit: number }>,
    ) =>
      durableFleetStore.pruneCleanupReceipts({
        completedBeforeMs: input.completedBeforeMs,
        limit: input.limit,
      }),
    pruneInventoryGenerations: (input: Readonly<{ limit: number }>) =>
      inventoryStore.pruneInventoryGenerations({ limit: input.limit }),
    pruneFleetOperations: (
      input: Readonly<{ kind: FleetOperationKind; limit: number }>,
    ) =>
      operationStore.pruneFleetOperations({
        kind: input.kind,
        limit: input.limit,
      }),
  });
  return { controlPlane, fleetStore, backend };
}
