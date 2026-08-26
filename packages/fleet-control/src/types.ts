// SPDX-License-Identifier: Apache-2.0

import type { InitialExecutionFenceState } from '@proofoftech/flowsafe/deployment-identity-protocol';

/**
 * The execution-fence state a freshly provisioned deployment is born in —
 * 'open' or 'migration-locked'.
 *
 * Re-exported so a control plane names the choice in its own types without
 * reaching into flowsafe's protocol subpath. Deliberately NOT part of
 * `DeploymentSpec`: see `provisionDeployment`'s option for why.
 */
export type { InitialExecutionFenceState };

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

/**
 * One Worker's identity plus its D1 and routing claims, as the authoritative
 * bidirectional inventory compares them. Declared here rather than beside
 * either user, because the provider API interface and the client that
 * implements it both need it and neither should depend on the other.
 */
export interface ScriptInventoryTarget {
  readonly scriptName: string;
  readonly tenantTag: string;
  readonly environment: string;
  readonly databaseId: string;
  readonly routeHostname: string;
}

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
  /**
   * The key of the last settlement this deployment completed.
   *
   * Persisted for one reason: the convergence entry is the steady-state path,
   * so a fleet that reconciles on a schedule re-enters it forever. Without a
   * durable marker every one of those converges would re-fire settlement on an
   * unchanged deployment, and a host charging per settlement would charge per
   * reconcile. Absent on any deployment that has not settled since this field
   * existed, which reads as "settle once more", never as "already settled".
   */
  readonly settledSettlementKey?: string;
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

export interface ProviderBindingIdentity {
  readonly type: string;
  readonly name: string;
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
  readonly providerBindingIdentities: readonly ProviderBindingIdentity[];
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

export interface PlainWorkerCustomDomain {
  readonly id: string;
  readonly hostname: string;
  readonly service: string;
}

/** Provider facts for one D1 database inventory entry. */
export interface PlainWorkerDatabaseInventoryEntry {
  /** Provider database identifier, when present and well formed. */
  readonly databaseId: string | undefined;
  /** Provider database name, when present and well formed. */
  readonly name: string | undefined;
}

/** Provider facts for one ordinary Worker version binding. */
export type PlainWorkerVersionBinding =
  | Readonly<{
      type: 'd1';
      name: string | undefined;
      databaseId: string | undefined;
    }>
  | Readonly<{
      type: 'durable-object';
      name: string | undefined;
      className: string | undefined;
      namespaceId: string | undefined;
    }>
  | Readonly<{
      type: 'service';
      name: string | undefined;
      service: string | undefined;
    }>
  | Readonly<{
      type: 'queue-producer';
      name: string | undefined;
      queueName: string | undefined;
    }>
  | Readonly<{
      type: 'r2-bucket';
      name: string | undefined;
      bucketName: string | undefined;
    }>
  | Readonly<{
      type: 'plain-text';
      name: string | undefined;
      value: string | undefined;
    }>
  | Readonly<{
      type: 'secret-text';
      name: string | undefined;
    }>
  | Readonly<{
      type: 'unsupported';
      name: string | undefined;
      /** The provider binding was not an object, so it has no raw type fact. */
      issue: 'not-object';
    }>
  | Readonly<{
      type: 'unsupported';
      name: string | undefined;
      /** Raw provider binding type, when the invalid wire value was a string. */
      providerType: string | undefined;
      issue: 'invalid-type';
    }>
  | Readonly<{
      type: 'unsupported';
      name: string | undefined;
      /** Raw unsupported provider binding type. */
      providerType: string;
      issue: 'unsupported-type';
    }>;

/** Provider facts for one ordinary Worker version summary. */
export interface PlainWorkerVersionSummary {
  /** Provider version identifier, when present and well formed. */
  readonly versionId: string | undefined;
  /** Fleet tag attached to the version, when present and well formed. */
  readonly tag: string | undefined;
}

/** Provider facts for one ordinary Worker version and its bindings. */
export interface PlainWorkerVersionDetail extends PlainWorkerVersionSummary {
  /** Provider bindings attached to the version. */
  readonly bindings: readonly PlainWorkerVersionBinding[];
}

/** Provider facts for an ordinary Worker's deployment status. */
export interface PlainWorkerDeploymentStatus {
  /** Traffic assignments reported by the provider. */
  readonly versions: readonly {
    /** Provider version identifier, when present and well formed. */
    readonly versionId: string | undefined;
    /** Provider traffic percentage, when present. */
    readonly percentage: number | undefined;
  }[];
}

/** Result of a durable ordinary-Worker database export. */
export interface PlainWorkerDatabaseExportResult {
  /** Durable location returned by the export store. */
  readonly location: string;
  /** Committed export size in bytes. */
  readonly size: number;
  /** Lowercase hexadecimal SHA-256 digest of the complete export. */
  readonly sha256: string;
}

/** Outcome of a provider mutation that may have been dispatched. */
export type PlainWorkerMutationOutcome =
  | Readonly<{ status: 'succeeded' }>
  | Readonly<{ status: 'failed'; error: unknown }>;

/**
 * Adapter-owned post-dispatch cleanup outcome. A failure is never thrown by
 * the adapter after dispatch; the caller surfaces it after reconciliation. An
 * adapter with no adapter-owned scratch always reports `succeeded`.
 */
export type PlainWorkerCleanupOutcome =
  | Readonly<{ status: 'succeeded' }>
  | Readonly<{ status: 'failed'; error: unknown }>;

/** Upload outcome including the adapter scratch-cleanup outcome. */
export type PlainWorkerUploadOutcome = PlainWorkerMutationOutcome &
  Readonly<{ cleanup: PlainWorkerCleanupOutcome }>;

/** Shared provider intent for an ordinary Worker candidate upload. */
export interface PlainWorkerUploadIntentBase {
  /** Provider script name. */
  readonly scriptName: string;
  /** Fleet tag attached to the uploaded candidate. */
  readonly candidateTag: string;
  /** Main module selected from the uploaded modules. */
  readonly mainModule: string;
  /** Worker modules to upload. */
  readonly modules: readonly WorkerModule[];
  /** Worker compatibility date. */
  readonly compatibilityDate: string;
  /** Worker compatibility flags, preserving provider-config omission. */
  readonly compatibilityFlags: readonly string[] | undefined;
  /** Desired Worker bindings. */
  readonly bindings: {
    readonly plainText: readonly {
      readonly name: string;
      readonly value: string;
    }[];
    readonly secrets: readonly {
      readonly name: string;
      readonly value: string;
    }[];
    readonly d1: readonly {
      readonly name: string;
      readonly databaseId: string;
      readonly databaseName: string;
    }[];
    readonly durableObjects: readonly {
      readonly name: string;
      readonly className: string;
    }[];
    readonly services: readonly {
      readonly name: string;
      readonly service: string;
    }[];
    readonly queueProducers: readonly {
      readonly name: string;
      readonly queueName: string;
    }[];
    readonly r2Buckets: readonly {
      readonly name: string;
      readonly bucketName: string;
    }[];
  };
  /** Desired Worker resource limits. */
  readonly limits: { readonly cpuMs: number | undefined };
  /** Ordinary Worker public-access mechanics applied by this upload. */
  readonly publicAccess: {
    readonly workersDevEnabled: boolean;
    readonly previewUrlsEnabled: boolean;
  };
}

/** Intent for an initial deploy or staged ordinary Worker version upload. */
export type PlainWorkerUploadIntent = PlainWorkerUploadIntentBase &
  (
    | Readonly<{
        mode: 'initial';
        durableObjectMigrations: readonly DurableObjectMigration[];
      }>
    | Readonly<{ mode: 'staged' }>
  );

/**
 * Provider operations shared by ordinary-Worker adapters.
 *
 * Every mutating member that takes an `ExternalMutationFence` must assert it
 * immediately before each provider request it issues. The Cloudflare transport
 * enforces this requirement for `CloudflareProvisioningClient`.
 * `withMutationFence` carries the active fence through nested provider calls.
 */
export interface PlainWorkerRouteApi {
  withMutationFence<T>(
    fence: ExternalMutationFence,
    operation: () => Promise<T>,
  ): Promise<T>;
  queryDatabase(
    databaseId: string,
    sql: string,
    bindings?: readonly string[],
  ): Promise<readonly Readonly<Record<string, unknown>>[]>;
  batchDatabase(
    databaseId: string,
    statements: readonly {
      readonly sql: string;
      readonly bindings?: readonly string[];
    }[],
  ): Promise<void>;
  getDatabase?(databaseId: string): Promise<DatabaseReference | undefined>;
  deleteDatabase?(databaseId: string): Promise<void>;
  listWorkerDatabaseAttachments(databaseId: string): Promise<
    readonly Readonly<{
      scriptName: string;
      plane: 'ordinary' | 'dispatch';
      dispatchNamespace?: string;
    }>[]
  >;
  listWorkerR2Attachments?(bucketName: string): Promise<
    readonly Readonly<{
      scriptName: string;
      plane: 'ordinary' | 'dispatch';
      dispatchNamespace?: string;
    }>[]
  >;
  getR2Bucket?(
    bucketName: string,
    jurisdiction: R2Jurisdiction,
  ): Promise<ApplicationR2BucketSnapshot | undefined>;
  createR2Bucket?(
    resource: ApplicationR2Binding,
    fence: ExternalMutationFence,
  ): Promise<void>;
  assertR2BucketEmpty?(resource: ApplicationR2Binding): Promise<void>;
  deleteR2Bucket?(
    resource: ApplicationR2Binding,
    fence: ExternalMutationFence,
  ): Promise<void>;
  /**
   * The version currently taking all of an ordinary Worker's traffic, plus the
   * fleet specification digest that version was built from.
   *
   * Narrower than the version read `inspect()` performs, deliberately: an
   * attestation needs the routed version and one binding, and every provider
   * call it makes is charged against the account-wide request window the rate
   * coordinator fences. It also goes through the provider API rather than the
   * wrangler CLI, which runs outside that coordinator entirely.
   *
   * Returns undefined when the script does not exist. Throws
   * `ActiveRouteAttestationError` when it exists but no single version holds
   * 100% of the traffic — that ambiguity is the refusal, not a tie to break.
   */
  inspectActiveWorkerRoute(scriptName: string): Promise<
    | Readonly<{
        artifactVersion: string;
        specDigest: string | undefined;
      }>
    | undefined
  >;
  listCustomDomains(): Promise<readonly PlainWorkerCustomDomain[]>;
  inspectOrdinaryWorkerFootprint(scriptName: string): Promise<{
    readonly scriptPresent: boolean;
    readonly workersDevEnabled?: boolean;
    readonly previewUrlsEnabled?: boolean;
    readonly customDomains: readonly PlainWorkerCustomDomain[];
    readonly zoneRoutes: readonly WorkerZoneRoute[];
  }>;
  listDurableObjectNamespaces(scriptName: string): Promise<readonly string[]>;
  listOrdinaryWorkerSecretNames(scriptName: string): Promise<readonly string[]>;
  deleteControlSecrets(
    scriptName: string,
    secretNames: readonly string[],
    fence: ExternalMutationFence,
  ): Promise<void>;
  attachCustomDomain(
    target: {
      readonly hostname: string;
      readonly service: string;
    },
    fence: ExternalMutationFence,
  ): Promise<void>;
  detachCustomDomain(
    domainId: string,
    fence: ExternalMutationFence,
  ): Promise<void>;
  disableOrdinaryWorkerPublicAccess(
    scriptName: string,
    fence: ExternalMutationFence,
  ): Promise<void>;
}

/**
 * Provider-neutral port whose methods return provider facts and perform
 * provider mechanics. Absence policy, malformed-fact refusals, ordering,
 * reconciliation, and compensation belong to the caller.
 *
 * A method whose result type cannot represent a malformed provider fact refuses
 * it in the adapter (e.g. `getDatabase` (`DatabaseReference` has required string
 * fields) and `exportDatabase` (`location` is required)); every other
 * malformed-fact refusal belongs to the caller.
 *
 * A method declared here that accepts an `ExternalMutationFence`, other than
 * `deleteDatabaseFenced`, asserts it immediately before every provider request
 * it issues through the command runner or route API. `deleteDatabaseFenced`
 * runs inside `withMutationFence` and relies on the route API's per-request
 * assertion (parity with the pre-port Wrangler CLI loop); whether the
 * direct-API adapter additionally pre-asserts remains an open question.
 *
 * A method that resolves a `PlainWorkerMutationOutcome` over a transport that
 * asserts per request must ALSO assert explicitly before dispatch, so a lost
 * lease rejects instead of resolving `failed` and triggering readback.
 *
 * `withMutationFence` is re-entrant with the same fence; a
 * `PlainWorkerRouteApi.withMutationFence` implementation is not required to
 * treat entry as an ownership assertion (assertion belongs to each mutating
 * request). An implementation that also asserts on entry is tolerated — the
 * adapter's re-entrancy short-circuit keeps the assertion count stable. Inherited
 * `PlainWorkerRouteApi` members retain their own contract.
 *
 * `undefined` and `'absent'` mean provider absence only. Other failures reject
 * with the provider error unmodified, and fence failures are never classified
 * as absence. Methods without an absence-typed result do not classify absence.
 *
 * `createDatabase`, `uploadCandidate`, and `createDeployment` resolve a failed
 * outcome only after a provider request was dispatched and failed or its result
 * became unknown. They reject failures that provably predate dispatch, including
 * fence assertion and local preparation failures. Other mutations reject on
 * failure except for their documented absence result.
 */
export interface PlainWorkerProvisioningApi extends PlainWorkerRouteApi {
  /** Maximum duration of any one provider mutation request after assertion. */
  readonly maxMutationDurationMs: number;
  /** Whether immutable-ID D1 reads and deletion are both available. */
  readonly supportsExactDatabaseDeletion: boolean;
  /** Lists all D1 database inventory facts visible to the adapter. */
  listDatabases(): Promise<readonly PlainWorkerDatabaseInventoryEntry[]>;
  /** Reads a D1 database, returning undefined only for provider absence. */
  getDatabase(databaseId: string): Promise<DatabaseReference | undefined>;
  /** Creates a D1 database and reports a dispatched mutation outcome. */
  createDatabase(
    name: string,
    fence: ExternalMutationFence,
  ): Promise<PlainWorkerMutationOutcome>;
  /** Prevents the shared core from using the inherited unfenced deletion. */
  readonly deleteDatabase?: never;
  /** Deletes a D1 database by immutable ID through the fenced route API. */
  deleteDatabaseFenced(
    databaseId: string,
    fence: ExternalMutationFence,
  ): Promise<void>;
  /** Reads deployment facts, returning undefined only for provider absence. */
  deploymentStatus(
    scriptName: string,
  ): Promise<PlainWorkerDeploymentStatus | undefined>;
  /**
   * Lists the provider's Worker version inventory, or `undefined` for provider
   * absence. Bounded pagination is an open contract question for the direct-API
   * adapter.
   */
  listVersions(
    scriptName: string,
  ): Promise<readonly PlainWorkerVersionSummary[] | undefined>;
  /** Strictly reads one version and never classifies provider absence. */
  viewVersion(
    scriptName: string,
    versionId: string,
  ): Promise<PlainWorkerVersionDetail>;
  /** Reads one version, returning undefined only for provider absence. */
  findVersion(
    scriptName: string,
    versionId: string,
  ): Promise<PlainWorkerVersionDetail | undefined>;
  /**
   * Uploads a candidate and reports dispatch and cleanup outcomes separately.
   * Scratch, if any, is adapter-owned and exists only for the duration of the
   * call. If a pre-dispatch failure (fence assertion or local preparation) and
   * scratch cleanup both occur, rejects with an `AggregateError` containing the
   * pre-dispatch error followed by the cleanup error; neither failure is
   * discarded.
   */
  uploadCandidate(
    intent: PlainWorkerUploadIntent,
    fence: ExternalMutationFence,
  ): Promise<PlainWorkerUploadOutcome>;
  /** Creates a deployment and reports a dispatched mutation outcome. */
  createDeployment(
    scriptName: string,
    versions: readonly { versionId: string; percentage: number }[],
    fence: ExternalMutationFence,
  ): Promise<PlainWorkerMutationOutcome>;
  /** Deletes a Worker script, returning absent only for provider absence. */
  deleteWorkerScript(
    scriptName: string,
    fence: ExternalMutationFence,
  ): Promise<'deleted' | 'absent'>;
  /** Exports a D1 database into the durable store with independent integrity. */
  exportDatabase(
    database: { readonly id: string; readonly name: string },
    fence: ExternalMutationFence,
  ): Promise<PlainWorkerDatabaseExportResult>;
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

export type ForceDecommissionStep =
  | 'remove-traffic'
  | 'revoke-credentials'
  | 'delete-database';

export interface DecommissionAuditEvent {
  readonly action: 'deployment-decommissioned';
  readonly tenantTag: string;
  readonly environment: string;
  readonly backend: ProvisioningBackendKind;
  readonly scriptName: string;
  readonly databaseId: string;
  readonly forced: boolean;
}

export type DecommissionAuditSink = (
  event: DecommissionAuditEvent,
) => void | Promise<void>;

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

/** The provisioning context `seedDeploymentIdentity` carries past the fence. */
export interface SeedDeploymentIdentityOptions {
  /** The execution-fence state this deployment is born in. Required. */
  readonly initialExecutionFenceState: InitialExecutionFenceState;
}

/**
 * What the provider actually reported when a route could not be attested.
 *
 * Every field is optional because an attestation failure is defined by what is
 * MISSING. A Worker splitting traffic across two versions has percentages but
 * no single routed version; an unrouted hostname has neither; a routed script
 * built without the fleet digest binding has everything except `specDigest`.
 * The refusal carries whichever of those the provider did answer, so an
 * operator reading the failure sees the drift rather than a bare "mismatch".
 */
export interface ObservedActiveRoute {
  readonly trafficSplit?: readonly Readonly<{
    artifactVersion: string;
    percentage: number;
  }>[];
  readonly routedScriptName?: string;
  readonly artifactVersion?: string;
  readonly specDigest?: string;
}

/**
 * Provider truth about which artifact is serving a deployment's hostname right
 * now, and which fleet specification that artifact was built from.
 *
 * `physicalScriptName` is the script traffic ACTUALLY reaches, which is not
 * necessarily the release the control plane expected — reporting the difference
 * is the whole point. `observedAt` comes from the backend's injected clock, so
 * a caller comparing two attestations is comparing one clock, not two.
 */
export interface ActiveRouteAttestation {
  readonly specDigest: string;
  readonly artifactVersion: string;
  readonly physicalScriptName: string;
  readonly source: 'workers-deployments' | 'dispatch-route';
  readonly observedAt: string;
}

/** Which promote path settled, so a host can tell four arrivals apart. */
export type FleetSettlementEntry =
  | 'migration'
  | 'platform-only'
  | 'ready-convergence'
  | 'rollback';

export interface FleetSettlementContext {
  readonly tenantTag: string;
  readonly environment: string;
  /** Proof of what is routed, read after the promotion converged. */
  readonly attestation: ActiveRouteAttestation;
  /**
   * The release now serving traffic, and the only deployment identity
   * `settle()` may depend on.
   *
   * That limit is forced, not stylistic: on the convergence entry the prior
   * release has already been retired by the time any settlement point is
   * reached, and the plain backend retains no prior release at all. A host that
   * needed the outgoing release to compute what it settles would work on some
   * entries and silently misbehave on others.
   *
   * On a backend that retains no release snapshots this is synthesized: its
   * script name, specification digest, and artifact version are the
   * attestation's, but `releaseSchemaVersion` and `application` are copied from
   * the control-plane record — this deployment's belief about what it deployed,
   * not something the provider confirmed.
   */
  readonly target: ExternalReleaseSnapshot;
  /**
   * Optional-normal, and its meaning is defined per entry: on 'migration',
   * 'platform-only', and 'ready-convergence' it is the release this one
   * replaced, where the deployment still retains it; on 'rollback' it is the
   * release being ABANDONED, so a host reversing its own effects reverses the
   * right one. Absent whenever no prior release is retained.
   */
  readonly prior?: ExternalReleaseSnapshot;
  readonly entry: FleetSettlementEntry;
  /**
   * Identifies this settlement by what was settled — the deployment and the
   * target release — not by when it happened. Every retry of the same
   * settlement carries the same key, which is what makes at-least-once
   * delivery safe to deduplicate on.
   */
  readonly settlementKey: string;
  /**
   * True only when an earlier successful settling write durably recorded this
   * exact key on the fleet record. False includes the re-fire window where
   * `settle()` succeeded but that write was lost, so false is never proof of a
   * first delivery. Hosts must deduplicate on `settlementKey` and may use this
   * field only for logging or alerting.
   */
  readonly alreadySettled: boolean;
}

export interface FleetSettlementHost {
  /**
   * Called while the deployment lease is held, after the route attested and
   * matched, and before the state write that records the settlement.
   *
   * AT-LEAST-ONCE and KEYED. A crash between this returning and that write
   * replays it with the same `settlementKey`, so anything with an external
   * effect — a charge, an entitlement, a notification — must be idempotent on
   * that key. This package cannot make the callback and its own durable write
   * atomic, so it guarantees the direction that fails safe: never settled
   * without being attempted.
   *
   * NO CALLBACK TIMEOUT. By default, the lease renews on a five-minute
   * heartbeat against a fifteen-minute TTL; both figures are configurable on
   * the state store. This package imposes no timeout on `settle()`: it renews
   * the lease for as long as the callback runs, so a hung callback that keeps
   * renewing holds the lease indefinitely and blocks every other operation on
   * the deployment, including decommission. Keep `settle()` well inside the
   * default renewal interval and enqueue slow work. Renewal errors are
   * inspected only after the callback returns; the heartbeat cannot interrupt
   * or time it out.
   *
   * If the process dies mid-callback, the lease expires one TTL after the last
   * successful renewal — fifteen minutes by default — and re-entry re-fires
   * `settle()` under the same `settlementKey`.
   *
   * A throw propagates. The branch's durable state is left where a re-entry
   * resumes it, and that re-entry re-attests and settles again under the same
   * key.
   */
  settle(context: FleetSettlementContext): Promise<void>;
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
  /**
   * Stamp the database's ownership sentinel and seed its initial execution
   * fence row.
   *
   * `initialExecutionFenceState` is a REQUIRED option rather than backend
   * configuration because it is a per-provisioning decision: the same control
   * plane brings ordinary deployments up open and migration targets up locked.
   * It carries no default anywhere on this path — a migration target that came
   * up open would be executing exactly when it must not.
   *
   * It rides an OPTIONS object rather than a fourth positional because this
   * method is implemented by every backend and faked by every test that builds
   * one: the next piece of provisioning context to reach the seeding protocol
   * would otherwise mean a fifth positional and the same fan-out again, and
   * positional four and five of a five-argument call are exactly where a
   * transposed argument compiles and provisions the wrong thing.
   */
  seedDeploymentIdentity(
    database: DatabaseReference,
    tenantTag: string,
    fence: ExternalMutationFence,
    options: SeedDeploymentIdentityOptions,
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
  /**
   * Provider truth about what is ROUTED, never desired state.
   *
   * `inspect()` answers a different question — "does the deployment the control
   * plane WANTS exist and match?" — and deliberately pins a staged candidate
   * that is receiving none of the traffic, so a converge can compare against it
   * before promoting it. That makes it the wrong instrument for asking what is
   * serving requests: a candidate uploaded and never promoted answers as though
   * it were live. This method answers only the second question and never pins a
   * candidate; the two views stay separate on purpose.
   *
   * Read-only and lease-free. Provider GET/HEAD requests bypass the external
   * mutation fence, so a caller holding no deployment lease may attest, and a
   * caller already inside one does not nest a second.
   *
   * Refuses rather than guessing: a Worker splitting traffic across versions,
   * a hostname routed nowhere, or a routed artifact carrying no fleet
   * specification digest all throw `ActiveRouteAttestationError` carrying what
   * the provider did report. There is no highest-percentage fallback — an
   * attestation is either unambiguous or it is a refusal.
   */
  attestActiveRoute(spec: DeploymentSpec): Promise<ActiveRouteAttestation>;
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
  forceDecommissionStep?(
    record: FleetRecord,
    step: ForceDecommissionStep,
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
