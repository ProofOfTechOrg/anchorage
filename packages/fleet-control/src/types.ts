// SPDX-License-Identifier: Apache-2.0

export type ProvisioningBackendKind = 'plain-worker' | 'workers-for-platforms';

export interface WorkerModule {
  readonly name: string;
  readonly content: string | Uint8Array;
  readonly contentType?: string;
}

/** Platform-authored bytes that are never sourced from an external release. */
export interface TrustedWorkerArtifact {
  readonly mainModule: string;
  readonly modules: readonly WorkerModule[];
  readonly compatibilityDate: string;
  readonly compatibilityFlags?: readonly string[];
}

export interface ExternalPlatformProfile {
  readonly runtimeContractVersion: 1;
  readonly backwardCompatibleWithRetainedReleases: true;
  readonly maintenanceCapabilityPublicKey: string;
  readonly maintenanceCapabilityPrivateKey: Readonly<{
    kty: 'OKP';
    crv: 'Ed25519';
    alg: 'EdDSA';
    kid: string;
    x: string;
    d: string;
  }>;
  readonly stateWorker: TrustedWorkerArtifact;
  /** Same-name guarded application/state bridge used only for backend switch. */
  readonly legacyBridgeWorker?: TrustedWorkerArtifact;
  /** Legacy-only bridge artifact. Fresh namespaced state uses shared outbound. */
  readonly egressProxyWorker?: TrustedWorkerArtifact;
  readonly stateDurableObjectMigrations: readonly DurableObjectMigration[];
  readonly organizationEgressHosts: readonly string[];
}

export interface DurableObjectMigration {
  readonly tag: string;
  readonly newSqliteClasses?: readonly string[];
  readonly newClasses?: readonly string[];
  readonly deletedClasses?: readonly string[];
  readonly renamedClasses?: readonly Readonly<{
    from: string;
    to: string;
  }>[];
}

export interface D1Migration {
  readonly version: number;
  readonly sql: string;
  readonly rollbackCompatible?: true;
}

export type R2Jurisdiction = 'default' | 'eu' | 'fedramp';

export interface DeploymentApplicationBindings {
  readonly vars: readonly Readonly<{ name: string; value: string }>[];
  readonly secrets: readonly Readonly<{
    name: string;
    valueSha256: string;
  }>[];
  readonly r2Buckets: readonly Readonly<{
    name: string;
    jurisdiction?: R2Jurisdiction;
  }>[];
}

export interface ApplicationR2Binding {
  readonly name: string;
  readonly bucketName: string;
  readonly jurisdiction: R2Jurisdiction;
  readonly creationDate?: string;
}

export interface ApplicationR2BucketSnapshot extends ApplicationR2Binding {
  readonly creationDate: string;
}

export interface ApplicationBindingTopology {
  readonly vars: readonly Readonly<{ name: string; value: string }>[];
  readonly secrets: readonly Readonly<{
    name: string;
    valueSha256: string;
  }>[];
  readonly r2Buckets: readonly ApplicationR2Binding[];
}

export interface ApplicationR2Resource extends ApplicationR2Binding {
  readonly state:
    | 'reserved'
    | 'create-authorized'
    | 'created'
    | 'detach-authorized'
    | 'detached'
    | 'empty-authorized'
    | 'empty'
    | 'delete-authorized'
    | 'deleted';
  readonly reservationNonce: string;
  readonly creationDate?: string;
}

export interface DeploymentSpec {
  readonly tenantTag: string;
  readonly environment: string;
  readonly scriptName: string;
  readonly databaseName: string;
  readonly compatibilityDate: string;
  readonly compatibilityFlags?: readonly string[];
  readonly mainModule: string;
  readonly modules: readonly WorkerModule[];
  readonly authoredBy: 'platform' | 'external';
  readonly schemaVersion: number;
  readonly migrations: readonly D1Migration[];
  readonly durableObjectMigrations: readonly DurableObjectMigration[];
  readonly previousDurableObjectTag?: string;
  readonly durableObjectBindings: readonly Readonly<{
    name: string;
    className: string;
    scriptName?: string;
    dispatchNamespace?: string;
  }>[];
  readonly queueProducer?: Readonly<{
    binding: string;
    queueName: string;
  }>;
  readonly egressProxyService?: string;
  readonly maintenanceBaseUrl: string;
  readonly routeHostname: string;
  readonly cpuLimitMs?: number;
  readonly subrequestLimit?: number;
  readonly application?: DeploymentApplicationBindings;
}

export interface DeploymentSecrets {
  readonly deploymentIdentity: string;
  readonly maintenanceAdmin: string;
  readonly application?: Readonly<Record<string, string>>;
}

export interface DatabaseReference {
  readonly id: string;
  readonly name: string;
  readonly created: boolean;
}

export interface PromotionGuard {
  readonly allowedCurrentScriptNames: readonly string[];
  readonly allowUnrouted: boolean;
}

export interface DurableObjectBindingInventory {
  readonly name: string;
  readonly className: string;
  readonly namespaceId: string;
  readonly scriptName?: string;
  readonly dispatchNamespace?: string;
}

export const PROVISIONING_PHASES = [
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
  'migrating',
  'ready',
  'decommissioning',
  'traffic-removed',
  'credentials-revoked',
  'worker-deleted',
  'platform-credentials-revoked',
  'platform-resources-deleted',
  'application-resources-deleting',
  'application-resources-deleted',
  'database-exported',
  'database-deleting',
  'decommissioned',
  'rolling-back',
] as const;

export type ProvisioningPhase = (typeof PROVISIONING_PHASES)[number];

export interface ExternalReleaseSnapshot {
  readonly physicalScriptName: string;
  readonly specDigest: string;
  readonly artifactVersion: string;
  readonly releaseSchemaVersion: number;
  /** Immutable application bindings owned by this release. */
  readonly application?: ApplicationBindingTopology;
  /** Absent only while a planned release still has artifactVersion `pending`. */
  readonly topology?: ExternalReleaseTopology;
}

export interface ExternalReleaseTopology {
  readonly durableObjectBindings: readonly DurableObjectBindingInventory[];
  readonly serviceBindings: readonly Readonly<{
    name: string;
    service: string;
  }>[];
  readonly queueProducerBindings: readonly Readonly<{
    name: string;
    queueName: string;
  }>[];
  readonly secretNames: readonly string[];
  readonly application?: ApplicationBindingTopology;
}

export interface PlatformWorkerSnapshot {
  readonly scriptName: string;
  readonly artifactVersion: string;
  readonly artifactDigest: string;
}

export interface DeploymentEgressPolicy {
  readonly policyId: string;
  readonly policyHosts: readonly string[];
  readonly policyDigest: string;
}

export interface ExternalPlatformResources {
  readonly auditQueueName?: string;
  readonly maintenanceCapabilityPublicKey: string;
  readonly stateWorker: PlatformWorkerSnapshot & {
    readonly plane?: 'ordinary' | 'dispatch';
    readonly dispatchNamespace?: string;
    readonly durableObjectTag?: string;
    readonly durableObjectBindings: readonly DurableObjectBindingInventory[];
    readonly namespaceIds: readonly string[];
  };
  readonly outboundPolicy?: DeploymentEgressPolicy;
  readonly sharedOutboundWorkerName?: string;
  /** Legacy ordinary per-deployment proxy retained only during bridge rollout. */
  readonly egressProxy?: PlatformWorkerSnapshot & DeploymentEgressPolicy;
}

export interface PlatformResourceProvisioningResult {
  readonly resources: ExternalPlatformResources;
  readonly created: Readonly<{
    stateWorker: boolean;
    egressProxy: boolean;
  }>;
}

export interface ExternalPlatformTargetDescription {
  readonly auditQueueName?: string;
  readonly maintenanceCapabilityPublicKey: string;
  readonly stateArtifactDigest: string;
  readonly stateDurableObjectHistoryDigest: string;
  readonly stateDurableObjectTag?: string;
  readonly stateEgressCredentialDigest?: string;
  readonly egressArtifactDigest?: string;
  readonly sharedOutboundWorkerName?: string;
  readonly d1SchemaVersion: number;
  readonly d1SchemaHistoryDigest: string;
  readonly outboundPolicy: DeploymentEgressPolicy;
}

export type ExternalMigrationSubphase =
  | 'planned'
  | 'schema-applied'
  | 'platform-applied'
  | 'candidate-deployed'
  | 'candidate-armed'
  | 'route-published';

export interface ExternalMigrationIntent {
  readonly platformOnly?: true;
  readonly targetSpecDigest: string;
  readonly priorRelease: ExternalReleaseSnapshot;
  readonly priorTarget: ExternalPlatformTargetDescription;
  readonly priorOutboundPolicy: DeploymentEgressPolicy;
  readonly targetRelease: ExternalReleaseSnapshot;
  readonly target: ExternalPlatformTargetDescription;
  readonly subphase: ExternalMigrationSubphase;
}

export interface FleetRecord {
  readonly tenantTag: string;
  readonly backend: ProvisioningBackendKind;
  readonly environment: string;
  readonly scriptName: string;
  readonly databaseId: string;
  readonly databaseName: string;
  readonly schemaVersion: number;
  readonly artifactVersion: string;
  readonly desiredSpecDigest: string;
  readonly pendingSpecDigest?: string;
  readonly pendingArtifactVersion?: string;
  readonly activeRelease?: ExternalReleaseSnapshot;
  readonly pendingRelease?: ExternalReleaseSnapshot;
  readonly migrationPriorRelease?: ExternalReleaseSnapshot;
  readonly rollbackRelease?: ExternalReleaseSnapshot;
  readonly retiringRelease?: ExternalReleaseSnapshot;
  readonly outboundPolicy?: DeploymentEgressPolicy;
  readonly platformResources?: ExternalPlatformResources;
  readonly platformTarget?: ExternalPlatformTargetDescription;
  readonly migrationIntent?: ExternalMigrationIntent;
  readonly backendSwitchIntent?: import('./backend-switch.js').BackendSwitchIntent;
  readonly applicationResources?: readonly ApplicationR2Resource[];
  readonly applicationBindings?: ApplicationBindingTopology;
  readonly durableObjectTag?: string;
  readonly durableObjectMigrationHistory?: readonly DurableObjectMigration[];
  readonly durableObjectMigrationHistoryDigest?: string;
  readonly durableObjectBindings: readonly DurableObjectBindingInventory[];
  readonly routeHostname: string;
  readonly phase: ProvisioningPhase;
  readonly databaseExportLocation?: string;
  readonly databaseExportSha256?: string;
  readonly databaseExportSize?: number;
  readonly updatedAt: string;
}

export interface MaintenanceHealth {
  readonly armed: boolean;
  readonly nextAlarmAt: number | null;
  readonly deploymentSpecDigest?: string;
  readonly lastSweepAt: number | null;
  readonly lastPurgeAt: number | null;
  readonly lastTickAt?: number | null;
  readonly lastSweepAttemptAt?: number | null;
  readonly lastPurgeAttemptAt?: number | null;
  readonly lastTickAttemptAt?: number | null;
  readonly lastSweepError?: string;
  readonly lastPurgeError?: string;
  readonly lastTickError?: string;
}

export interface LiveDeployment {
  readonly tenantTag: string;
  readonly environment: string;
  readonly scriptName: string;
  readonly databaseId: string;
  readonly durableObjectBindings: readonly DurableObjectBindingInventory[];
  readonly serviceBindings?: readonly Readonly<{
    name: string;
    service: string;
  }>[];
  readonly queueProducerBindings?: readonly Readonly<{
    name: string;
    queueName: string;
  }>[];
  readonly plainTextBindings: Readonly<Record<string, string>>;
  readonly r2BucketBindings?: readonly ApplicationR2Binding[];
  readonly secretNames: readonly string[];
  readonly artifactVersion: string;
  readonly desiredSpecDigest: string;
  readonly schemaVersion: number;
  readonly maintenance: MaintenanceHealth;
}

export interface DatabaseExport {
  readonly databaseId: string;
  readonly location: string;
  readonly sha256: string;
  readonly size: number;
}

export interface FleetInventoryDeployment {
  readonly backend: ProvisioningBackendKind;
  readonly resourceRole?: 'platform-state' | 'deployment-egress';
  readonly resourceGroupId?: string;
  readonly scriptName: string;
  readonly tenantTag: string;
  readonly environment: string;
  readonly databaseIds: readonly string[];
  readonly durableObjectBindings: readonly DurableObjectBindingInventory[];
  readonly serviceBindings?: readonly Readonly<{
    name: string;
    service: string;
  }>[];
  readonly queueProducerBindings?: readonly Readonly<{
    name: string;
    queueName: string;
  }>[];
  readonly kvNamespaceBindings?: readonly Readonly<{
    name: string;
    namespaceId: string;
  }>[];
  readonly r2BucketBindings?: readonly ApplicationR2Binding[];
  readonly secretNames: readonly string[];
  readonly plainTextBindings: Readonly<Record<string, string>>;
  readonly routeHostnames: readonly string[];
  readonly zoneRoutes?: readonly WorkerZoneRoute[];
  readonly artifactVersion: string;
  readonly desiredSpecDigest?: string;
  readonly schemaVersion: number;
}

export interface FleetInventoryFinding {
  readonly tenantTag: string;
  readonly environment: string;
  readonly kind:
    | 'malformed-script-registration'
    | 'stale-script-registration'
    | 'malformed-route'
    | 'stale-route'
    | 'incomplete-deployment'
    | 'trusted-dispatch-namespace'
    | 'unknown-dispatch-scripts';
  readonly detail: string;
}

export interface FleetResourceInventory {
  readonly findings: readonly FleetInventoryFinding[];
  /** Canonical HOSTS namespace assigned by the fleet control plane. */
  readonly hostRoutingKvId?: string;
  readonly dispatchScriptCount?: number;
  readonly dispatchNamespace?: Readonly<{
    name: string;
    namespaceId?: string;
    trustedWorkers: boolean | undefined;
    scriptCount: number;
  }>;
  readonly scriptRegistrations: readonly Readonly<{
    scriptName: string;
    tenantTag: string;
    environment: string;
    databaseId: string;
    routeHostname: string;
  }>[];
  readonly deployments: readonly FleetInventoryDeployment[];
  readonly databaseIds: readonly string[];
  readonly namespaceIds: readonly string[];
  readonly r2Buckets?: readonly Readonly<{
    bucketName: string;
    jurisdiction: R2Jurisdiction;
    creationDate: string;
  }>[];
  readonly routes: readonly Readonly<{
    backend: ProvisioningBackendKind;
    hostname: string;
    scriptName: string;
    tenantTag: string;
    environment: string;
    surface?: 'custom-domain' | 'host-registry' | 'zone-route';
    zoneId?: string;
    routeId?: string;
    policyId?: string;
    policyDigest?: string;
    policyHosts?: readonly string[];
    stateEgress?: Readonly<{
      resourceGroupId: string;
      stateScriptName: string;
      credentialDigest: string;
    }>;
  }>[];
}

export interface WorkerZoneRoute {
  readonly zoneId: string;
  readonly routeId: string;
  readonly pattern: string;
}

export interface ExternalMutationFence {
  /** Full lease lifetime after a successful assertion or renewal. */
  readonly mutationLeaseTtlMs: number;
  /** Renew ownership and fail before the provider request is dispatched. */
  assertOwned(): Promise<void>;
}

export interface FleetStateLease extends ExternalMutationFence {
  readonly tenantTag: string;
  readonly environment: string;
  renew(): Promise<void>;
  put(record: FleetRecord): Promise<void>;
  delete(): Promise<void>;
}

export interface FleetStateStore {
  withDeploymentLease<T>(
    tenantTag: string,
    environment: string,
    operation: (lease: FleetStateLease) => Promise<T>,
  ): Promise<T>;
  get(tenantTag: string, environment: string): Promise<FleetRecord | undefined>;
  list(): Promise<readonly FleetRecord[]>;
}

export interface PlatformPlaneResourceSet {
  readonly accountId: string;
  readonly dispatchNamespace: string;
  readonly dispatchScriptName: string;
  readonly outboundScriptName: string;
  readonly auditScriptName: string;
  readonly hostRoutingKvId: string;
  readonly auditQueueName: string;
  readonly maintenanceCapabilityPublicKey: string;
  readonly auditDeadLetterQueue?: string;
}

export type PlatformPlaneResourceKind =
  | 'dispatch-namespace'
  | 'worker-script'
  | 'kv-namespace'
  | 'queue';

export interface PlatformPlaneLease extends ExternalMutationFence {
  readonly resourceSetKey: string;
  renew(): Promise<void>;
}

export interface PlatformPlaneStateStore {
  withPlatformPlaneLease<T>(
    resourceSet: PlatformPlaneResourceSet,
    platformPlaneIdentity: string,
    operation: (lease: PlatformPlaneLease) => Promise<T>,
  ): Promise<T>;
}

export interface ProvisioningBackend {
  readonly kind: ProvisioningBackendKind;
  readonly immutableExternalArtifacts?: true;
  releaseScriptName?(spec: DeploymentSpec): string;
  findDatabase(spec: DeploymentSpec): Promise<DatabaseReference | undefined>;
  getDatabase(databaseId: string): Promise<DatabaseReference | undefined>;
  ensureDatabase(
    spec: DeploymentSpec,
    fence: ExternalMutationFence,
  ): Promise<DatabaseReference>;
  seedDeploymentIdentity(
    database: DatabaseReference,
    tenantTag: string,
    fence: ExternalMutationFence,
  ): Promise<void>;
  readDeploymentIdentity(
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<string | undefined>;
  applyMigrations(
    database: DatabaseReference,
    migrations: readonly D1Migration[],
    fence: ExternalMutationFence,
  ): Promise<void>;
  findApplicationR2Bucket?(
    resource: ApplicationR2Binding,
  ): Promise<ApplicationR2BucketSnapshot | undefined>;
  ensureApplicationR2Bucket?(
    resource: ApplicationR2Binding,
    fence: ExternalMutationFence,
  ): Promise<ApplicationR2BucketSnapshot>;
  assertApplicationR2Detached?(
    resource: ApplicationR2Binding,
    fence: ExternalMutationFence,
  ): Promise<void>;
  assertApplicationR2Empty?(
    resource: ApplicationR2Binding,
    fence: ExternalMutationFence,
  ): Promise<void>;
  deleteApplicationR2Bucket?(
    resource: ApplicationR2Binding,
    fence: ExternalMutationFence,
  ): Promise<void>;
  ensurePlatformResources?(
    spec: DeploymentSpec,
    database: DatabaseReference,
    secrets: DeploymentSecrets,
    expectedTarget: ExternalPlatformTargetDescription | undefined,
    currentRecord: FleetRecord,
    fence: ExternalMutationFence,
  ): Promise<PlatformResourceProvisioningResult>;
  inspectPlatformResources?(
    spec: DeploymentSpec,
    database: DatabaseReference,
  ): Promise<ExternalPlatformResources | undefined>;
  describeExternalPlatformTarget?(
    spec: DeploymentSpec,
  ): ExternalPlatformTargetDescription;
  revokePlatformResourceCredentials?(
    spec: DeploymentSpec,
    record: FleetRecord,
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void>;
  deletePlatformResources?(
    spec: DeploymentSpec,
    record: FleetRecord,
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void>;
  deployWorker(
    spec: DeploymentSpec,
    database: DatabaseReference,
    secrets: DeploymentSecrets,
    platformResources: ExternalPlatformResources | undefined,
    fence: ExternalMutationFence,
    expectedArtifactVersion: string | undefined,
    application?: ApplicationBindingTopology,
  ): Promise<{
    artifactVersion: string;
    created: boolean;
    physicalScriptName?: string;
  }>;
  promoteWorker(
    spec: DeploymentSpec,
    guard: PromotionGuard,
    outboundPolicy: DeploymentEgressPolicy | undefined,
    fence: ExternalMutationFence,
    expectedArtifactVersion: string | undefined,
  ): Promise<void>;
  ensureMaintenance(
    spec: DeploymentSpec,
    maintenanceAdminSecret: string,
    fence: ExternalMutationFence,
    expectedArtifactVersion: string | undefined,
  ): Promise<MaintenanceHealth>;
  inspect(
    spec: DeploymentSpec,
    maintenanceAdminSecret: string,
    expectedArtifactVersion: string | undefined,
  ): Promise<LiveDeployment | undefined>;
  removeTraffic(
    spec: DeploymentSpec,
    retainedReleases: readonly ExternalReleaseSnapshot[] | undefined,
    activeRelease: ExternalReleaseSnapshot | undefined,
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void>;
  assertTrafficRemoved(spec: DeploymentSpec): Promise<void>;
  revokeCredentials(
    spec: DeploymentSpec,
    retainedReleases: readonly ExternalReleaseSnapshot[] | undefined,
    activeRelease: ExternalReleaseSnapshot | undefined,
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void>;
  deleteWorker(
    spec: DeploymentSpec,
    retainedReleases: readonly ExternalReleaseSnapshot[] | undefined,
    database: DatabaseReference,
    activeRelease: ExternalReleaseSnapshot | undefined,
    fence: ExternalMutationFence,
  ): Promise<void>;
  deleteRetainedRelease?(
    spec: DeploymentSpec,
    release: ExternalReleaseSnapshot,
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void>;
  assertDatabaseDetached(
    spec: DeploymentSpec,
    record: FleetRecord,
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void>;
  exportDatabase(
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<DatabaseExport>;
  deleteDatabase(
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void>;
}

export interface ProvisioningResult {
  readonly record: FleetRecord;
  readonly maintenance: MaintenanceHealth;
}

export interface DecommissionResult {
  readonly record: FleetRecord;
  readonly databaseExport: DatabaseExport;
}
