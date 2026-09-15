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
  FleetRecord,
  FleetResourceInventory,
  FleetSettlementHost,
  InitialExecutionFenceState,
  ProvisioningResult,
} from './types.js';

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
export type { FleetInventoryR2Jurisdiction } from './cloudflare-fleet-inventory.js';
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
  ExternalPlatformResources,
  ExternalPlatformTargetDescription,
  ExternalReleaseSnapshot,
  ExternalReleaseTopology,
  FleetInventoryDeployment,
  FleetInventoryFinding,
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

export interface CloudflareProvisionDeploymentOptions {
  readonly spec: CloudflareDeploymentSpec;
  readonly secrets: DeploymentSecrets;
  readonly initialExecutionFenceState: InitialExecutionFenceState;
  readonly routeAttestation?: AttestConvergedActiveRouteOptions;
  readonly clock?: () => number;
}

export interface CloudflareAdvanceCleanupDeploymentOptions {
  readonly spec: CloudflareDeploymentSpec;
  readonly action: CleanupAdvanceAction;
  readonly maxProviderRequests: number;
  readonly signal?: AbortSignal;
  readonly clock?: () => number;
}

export interface CloudflareAdvanceDecommissionDeploymentOptions {
  readonly spec: CloudflareDeploymentSpec;
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

export interface CloudflareAdvanceFleetAuditOptions {
  readonly action: FleetAuditAdvanceAction;
  readonly specFor: (record: FleetRecord) => CloudflareDeploymentSpec;
  readonly maintenanceSecretFor: (record: FleetRecord) => string;
  readonly maxItemsPerCall?: number;
  readonly auditClock?: () => number;
  readonly authorityClock?: () => number;
  readonly signal?: AbortSignal;
}

export interface CloudflareAdvanceFleetMigrationOptions {
  readonly action: FleetMigrationAdvanceAction;
  readonly specFor: (record: FleetRecord) => CloudflareDeploymentSpec;
  readonly secretsFor: (record: FleetRecord) => DeploymentSecrets;
  readonly settlementFor?: (
    record: FleetRecord,
  ) => FleetSettlementHost | undefined;
  readonly routeAttestation?: AttestConvergedActiveRouteOptions;
  readonly clock?: () => number;
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

export interface CloudflareControlPlane {
  provisionDeployment(
    options: CloudflareProvisionDeploymentOptions,
  ): Promise<ProvisioningResult>;
  advanceCleanupDeployment(
    options: CloudflareAdvanceCleanupDeploymentOptions,
  ): Promise<CleanupAdvanceResult>;
  advanceDecommissionDeployment(
    options: CloudflareAdvanceDecommissionDeploymentOptions,
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
    options: CloudflareAdvanceFleetAuditOptions,
  ): Promise<FleetAuditAdvanceResult>;
  readFleetAuditFindingsPage(
    options: CloudflareFleetOperationPageOptions,
  ): Promise<FleetAuditFindingsPage>;
  abandonFleetAuditOperation(operationId: string): Promise<void>;
  advanceFleetMigration(
    options: CloudflareAdvanceFleetMigrationOptions,
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
    maintenanceFetch,
    maintenanceRequestTimeoutMs,
  } = options;
  const clock = options.clock?.bind(options);
  const randomUUID =
    options.randomUUID?.bind(options) ?? (() => crypto.randomUUID());
  const database = new D1FleetStateDatabase(fleetDatabase);
  const storeOptions = { accountId, leaseTtlMs, leaseRenewalIntervalMs };
  const fleetStore = new D1FleetStateStore(database, storeOptions);
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
    plane: 'plain-worker',
    rateCoordinator,
    exportStore,
    concurrency,
    requestTimeoutMs,
    fetch: providerFetch,
  });
  const backend = new CloudflareApiPlainWorkerBackend({
    client,
    fetch: maintenanceFetch,
    maintenanceRequestTimeoutMs,
    clock,
  });
  const backendFor = (record: FleetRecord): CloudflareApiPlainWorkerBackend => {
    if (record.backend !== 'plain-worker') {
      throw new TypeError(
        'Cloudflare control plane requires plain-worker records',
      );
    }
    return backend;
  };

  return Object.freeze({
    async provisionDeployment(input: CloudflareProvisionDeploymentOptions) {
      return provisionDeployment({
        backend,
        store: fleetStore,
        spec: ordinarySpec(input.spec),
        secrets: input.secrets,
        initialExecutionFenceState: input.initialExecutionFenceState,
        routeAttestation: input.routeAttestation,
        failureCleanup: 'bounded',
        clock: input.clock?.bind(input),
      });
    },
    async advanceCleanupDeployment(
      input: CloudflareAdvanceCleanupDeploymentOptions,
    ) {
      return advanceCleanupDeployment({
        backend,
        store: fleetStore,
        spec: ordinarySpec(input.spec),
        action: input.action,
        maxProviderRequests: input.maxProviderRequests,
        signal: input.signal,
        clock: input.clock?.bind(input),
        randomUUID,
      });
    },
    async advanceDecommissionDeployment(
      input: CloudflareAdvanceDecommissionDeploymentOptions,
    ) {
      return advanceDecommissionDeployment({
        backend,
        store: fleetStore,
        spec: ordinarySpec(input.spec),
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
        context: {
          async advanceStage(stageInput) {
            if (
              stageInput.options.includeDispatchNamespace ||
              stageInput.options.hostRoutingKvId !== undefined
            ) {
              throw new TypeError(
                'Cloudflare control plane cannot advance dispatch or host-routing inventory',
              );
            }
            return context.advanceStage(stageInput);
          },
        },
        store: inventoryStore,
        action:
          action.kind === 'start'
            ? {
                kind: 'start',
                operationId: action.operationId,
                options: {
                  databaseNamePrefix: action.options.databaseNamePrefix,
                  scriptNamePrefix: action.options.scriptNamePrefix,
                  includeR2Buckets: action.options.includeR2Buckets,
                  includeDispatchNamespace: false,
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
    async advanceFleetAudit(input: CloudflareAdvanceFleetAuditOptions) {
      const specFor = input.specFor.bind(input);
      return advanceFleetAudit({
        operationStore,
        inventoryStore,
        fleetStore,
        backendFor,
        specFor: (record) => ordinarySpec(specFor(record)),
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
    async advanceFleetMigration(input: CloudflareAdvanceFleetMigrationOptions) {
      const specFor = input.specFor.bind(input);
      return advanceFleetMigration({
        operationStore,
        fleetStore,
        backendFor,
        specFor: (record) => ordinarySpec(specFor(record)),
        secretsFor: input.secretsFor.bind(input),
        settlementFor: input.settlementFor?.bind(input),
        action: input.action,
        routeAttestation: input.routeAttestation,
        clock: input.clock?.bind(input),
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
      fleetStore.readCleanupReceipt(operationId),
    pruneCleanupReceipts: (
      input: Readonly<{ completedBeforeMs: number; limit: number }>,
    ) =>
      fleetStore.pruneCleanupReceipts({
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
}
