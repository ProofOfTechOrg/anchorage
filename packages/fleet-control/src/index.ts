// SPDX-License-Identifier: Apache-2.0

export {
  applicationBindingTopology,
  applicationR2Bindings,
  applicationSecretNames,
  applicationSecretValues,
  canonicalApplicationBindings,
  reserveApplicationR2Resources,
} from './application-bindings.js';
export {
  BACKEND_SWITCH_SUBPHASES,
  type BackendSwitchApplicationR2Progress,
  type BackendSwitchCandidateSnapshot,
  type BackendSwitchIntent,
  type BackendSwitchLease,
  type BackendSwitchMutationFence,
  type BackendSwitchProvider,
  type BackendSwitchSubphase,
  type BridgeMutationPlan,
  type BridgeSnapshot,
  decommissionBackendSwitch,
  type FinalizedOrdinaryStateProvider,
  finalizeBackendSwitch,
  type PlainBackendSnapshot,
  reconcileFinalizedBackendSwitchState,
  rollbackBackendSwitch,
  switchPlainDeploymentToWorkersForPlatforms,
} from './backend-switch.js';
export {
  type CloudflareClientOptions,
  CloudflareProvisioningClient,
  type ControlWorkerSpec,
  type DurableDatabaseExportStore,
  type OrdinaryWorkerFootprint,
  type ScriptInventoryTarget,
} from './cloudflare-client.js';
export {
  type CloudflareApiRateCoordinator,
  D1CloudflareApiRateCoordinator,
  type D1CloudflareApiRateCoordinatorOptions,
  ProcessLocalCloudflareApiRateCoordinator,
} from './cloudflare-rate-coordinator.js';
export { FileSystemDatabaseExportStore } from './export-store.js';
export {
  auditFleetDrift,
  type DriftFinding,
  type FleetVersionRow,
  fleetVersionReport,
  migrateFleet,
  rollbackExternalRelease,
} from './fleet.js';
export {
  type PlatformPlaneClient,
  type PlatformPlaneResult,
  type PlatformPlaneSpec,
  provisionPlatformPlane,
} from './platform-plane.js';
export {
  defaultDeploymentEgressPolicy,
  externalEgressProxyScriptName,
  externalPlatformResourceGroupId,
  externalStateScriptName,
  trustedArtifactDigest,
  validateExternalPlatformProfile,
} from './platform-resources.js';
export {
  cleanupDeploymentArtifacts,
  decommissionDeployment,
  ProvisioningError,
  provisionDeployment,
} from './provision.js';
export { generateDeploymentSecrets } from './secrets.js';
export { deploymentSpecDigest } from './spec-digest.js';
export {
  D1FleetStateStore,
  type D1FleetStateStoreOptions,
  type FleetStateDatabase,
} from './state-store.js';
export type {
  ApplicationBindingTopology,
  ApplicationR2Binding,
  ApplicationR2BucketSnapshot,
  ApplicationR2Resource,
  D1Migration,
  DatabaseExport,
  DatabaseReference,
  DecommissionResult,
  DeploymentApplicationBindings,
  DeploymentEgressPolicy,
  DeploymentSecrets,
  DeploymentSpec,
  DurableObjectBindingInventory,
  DurableObjectMigration,
  ExternalMigrationIntent,
  ExternalMigrationSubphase,
  ExternalMutationFence,
  ExternalPlatformProfile,
  ExternalPlatformResources,
  ExternalPlatformTargetDescription,
  ExternalReleaseSnapshot,
  FleetInventoryDeployment,
  FleetRecord,
  FleetResourceInventory,
  FleetStateLease,
  FleetStateStore,
  LiveDeployment,
  MaintenanceHealth,
  PlatformPlaneLease,
  PlatformPlaneResourceSet,
  PlatformPlaneStateStore,
  PromotionGuard,
  ProviderBindingIdentity,
  ProvisioningBackend,
  ProvisioningBackendKind,
  ProvisioningPhase,
  ProvisioningResult,
  TrustedWorkerArtifact,
  WorkerModule,
  WorkerZoneRoute,
} from './types.js';
export {
  deploymentKey,
  validateDeploymentSecrets,
  validateDeploymentSpec,
} from './validation.js';
export {
  deriveStateEgressCredential,
  externalReleaseScriptName,
  type WorkersForPlatformsApi,
  WorkersForPlatformsBackend,
} from './workers-for-platforms-backend.js';
export {
  type BackendSwitchApi,
  composeLegacyBridgeArtifact,
  LEGACY_APPLICATION_MODULE_PLACEHOLDER,
  WorkersForPlatformsBackendSwitchProvider,
  type WorkersForPlatformsBackendSwitchProviderOptions,
} from './workers-for-platforms-backend-switch-provider.js';
export {
  type PlainWorkerCustomDomain,
  type PlainWorkerRouteApi,
  WranglerLoopBackend,
} from './wrangler-loop-backend.js';
export {
  type CommandResult,
  type CommandRunner,
  WranglerCommandRunner,
} from './wrangler-runner.js';
