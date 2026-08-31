// SPDX-License-Identifier: Apache-2.0

import type { InitialExecutionFenceState } from '@proofoftech/flowsafe/deployment-identity-protocol';
import type { HostRoutingTarget } from './host-routing.js';

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

/** One immutable Worker version and its intended deployment percentage. */
export type OrdinaryWorkerDeploymentVersion = Readonly<{
  versionId: string;
  percentage: number;
}>;

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
  'decommission-advancing',
  'cleanup-advancing',
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

export const BACKEND_SWITCH_SUBPHASES = [
  'planned',
  'bridge-upload-authorized',
  'bridge-deployed',
  'candidate-deploy-authorized',
  'candidate-deployed',
  'host-publish-authorized',
  'host-published',
  'domain-detach-authorized',
  'dispatch-serving',
  'bridge-private-authorized',
  'bridge-private',
  'ownership-commit-authorized',
  'ready',
  'rollback-route-authorized',
  'rollback-routed',
  'rollback-drain-authorized',
  'rollback-drained',
  'rollback-restore-authorized',
  'rollback-restored',
  'rollback-ownership-authorized',
  'rolled-back',
  'finalize-authorized',
  'finalized',
  'decommission-traffic-authorized',
  'decommission-traffic-removed',
  'decommission-candidate-authorized',
  'decommission-candidate-removed',
  'decommission-bridge-authorized',
  'decommission-bridge-removed',
  'decommission-application-r2-authorized',
  'decommission-application-r2-removed',
  'decommission-export-authorized',
  'decommission-exported',
  'decommission-database-authorized',
  'decommissioned',
] as const;

export type BackendSwitchSubphase = (typeof BACKEND_SWITCH_SUBPHASES)[number];

export interface PlainBackendSnapshot {
  readonly scriptName: string;
  readonly artifactVersion: string;
  readonly specDigest: string;
  readonly databaseId: string;
  readonly databaseName: string;
  readonly durableObjectBindings: readonly DurableObjectBindingInventory[];
  readonly namespaceIds: readonly string[];
  readonly secretNames: readonly string[];
  readonly application?: ApplicationBindingTopology;
  readonly applicationResources: readonly ApplicationR2Resource[];
  readonly customDomain: Readonly<{ id: string; hostname: string }>;
}

export interface BridgeSnapshot {
  readonly scriptName: string;
  readonly artifactVersion: string;
  readonly artifactDigest: string;
  readonly databaseId: string;
  readonly durableObjectBindings: readonly DurableObjectBindingInventory[];
  readonly namespaceIds: readonly string[];
  readonly secretNames: readonly string[];
  readonly application?: ApplicationBindingTopology;
  readonly publicRouteAttached: boolean;
  readonly stateOnly: boolean;
}

export interface BridgeMutationPlan {
  readonly artifactDigest: string;
  readonly durableObjectMigrations: readonly DurableObjectMigration[];
  readonly priorDurableObjectTag?: string;
  readonly targetDurableObjectTag?: string;
  readonly secretNames: readonly string[];
  readonly mutationDigest: string;
}

export interface BackendSwitchCandidateSnapshot
  extends ExternalReleaseSnapshot {
  readonly maintenance: Readonly<{
    receipt: string;
    specDigest: string;
  }>;
}

export interface BackendSwitchApplicationR2Progress {
  readonly resource: ApplicationR2Resource;
  readonly subphase: ApplicationR2Resource['state'];
}

export interface BackendSwitchDecommissionRelease {
  readonly release: ExternalReleaseSnapshot;
  readonly subphase: 'present' | 'delete-authorized' | 'deleted';
}

export interface BackendSwitchDecommissionRouteTarget {
  readonly release: ExternalReleaseSnapshot;
  readonly target: ExternalPlatformTargetDescription;
  readonly routeTarget: HostRoutingTarget;
}

export interface BackendSwitchDecommissionSnapshot {
  readonly prior?: PlainBackendSnapshot;
  readonly restoredArtifactVersion?: string | null;
  readonly entryPendingArtifactVersion?: string | null;
  readonly entryPendingNamespaceIds?: readonly string[] | null;
  readonly providerTargetSpecDigest?: string;
  readonly routeHostname: string;
  readonly routeTargets: readonly BackendSwitchDecommissionRouteTarget[];
  readonly desiredSpecDigest: string;
  readonly target: ExternalPlatformTargetDescription;
  readonly releases: readonly BackendSwitchDecommissionRelease[];
  readonly applicationResources: readonly ApplicationR2Resource[];
  readonly bridge?: BridgeSnapshot;
  readonly resources?: ExternalPlatformResources;
  readonly bridgePlan?: BridgeMutationPlan;
}

export interface BackendSwitchIntent {
  readonly kind: 'backend-switch';
  readonly tenantTag: string;
  readonly environment: string;
  readonly prior: PlainBackendSnapshot;
  readonly targetSpecDigest: string;
  readonly targetApplication: ApplicationBindingTopology;
  readonly target: ExternalPlatformTargetDescription;
  readonly rollbackUntil: string;
  readonly subphase: BackendSwitchSubphase;
  readonly bridgePlan?: BridgeMutationPlan;
  readonly bridge?: BridgeSnapshot;
  readonly candidate?: BackendSwitchCandidateSnapshot;
  readonly restoredArtifactVersion?: string;
  readonly databaseExport?: DatabaseExport;
  readonly applicationR2Progress?: readonly BackendSwitchApplicationR2Progress[];
  readonly stateReconcileIntent?: Readonly<{
    targetSpecDigest: string;
    plan: BridgeMutationPlan;
    subphase: 'upload-authorized' | 'uploaded';
  }>;
  readonly decommissionSnapshot?: BackendSwitchDecommissionSnapshot;
  readonly decommissionSnapshotSha256?: string;
  readonly decommissionEntrySubphase?: BackendSwitchSubphase;
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

export type NormalDecommissionLifecyclePhase =
  | 'publishing'
  | 'ready'
  | 'migrating'
  | 'rolling-back'
  | 'decommissioning'
  | 'traffic-removed'
  | 'credentials-revoked'
  | 'worker-deleted'
  | 'platform-credentials-revoked'
  | 'platform-resources-deleted'
  | 'application-resources-deleting'
  | 'application-resources-deleted'
  | 'database-exported'
  | 'database-deleting';

export interface DecommissionRecordIdentity {
  readonly tenantTag: string;
  readonly environment: string;
  readonly backend: ProvisioningBackendKind;
  readonly scriptName: string;
  readonly databaseId: string;
  readonly databaseName: string;
  readonly routeHostname: string;
}

export type DecommissionOperationMode =
  | Readonly<{
      kind: 'normal';
      requestedSpecDigest: string;
      entryLifecyclePhase: NormalDecommissionLifecyclePhase;
    }>
  | Readonly<{
      kind: 'backend-switch';
      priorSpecDigest: string;
      targetSpecDigest: string;
      decommissionSnapshotSha256: string;
      /** Immutable entry subphase, not the switch's current progress. */
      backendSwitchSubphase: BackendSwitchSubphase;
    }>;

export interface DecommissionOperationIdentity {
  readonly record: DecommissionRecordIdentity;
  readonly mode: DecommissionOperationMode;
}

export type DecommissionAttachmentPurpose =
  | Readonly<{
      kind: 'application-r2-detach';
      resourceIndex: number;
      name: string;
      bucketName: string;
      jurisdiction: R2Jurisdiction;
      reservationNonce: string;
      creationDate: string;
    }>
  | Readonly<{ kind: 'database-pre-export'; databaseId: string }>
  | Readonly<{
      kind: 'database-pre-delete';
      databaseId: string;
      exportLocation: string;
      exportSha256: string;
      exportSize: number;
    }>;

export type DecommissionBlockedAttachment =
  | Readonly<{ plane: 'ordinary'; scriptName: string }>
  | Readonly<{
      plane: 'dispatch';
      scriptName: string;
      dispatchNamespace: string;
    }>;

export type DecommissionAttachmentProgress =
  | Readonly<{
      version: 1;
      target:
        | Readonly<{ kind: 'd1'; databaseId: string }>
        | Readonly<{ kind: 'r2'; bucketName: string }>;
      evidenceSha256: string;
      evidenceCount: number;
      stage: 'ordinary-script-inventory';
      ordinaryInventorySha256?: string;
      scriptIndex: number;
    }>
  | Readonly<{
      version: 1;
      target:
        | Readonly<{ kind: 'd1'; databaseId: string }>
        | Readonly<{ kind: 'r2'; bucketName: string }>;
      evidenceSha256: string;
      evidenceCount: number;
      stage: 'ordinary-deployment';
      ordinaryInventorySha256: string;
      scriptIndex: number;
      scriptName: string;
    }>
  | Readonly<{
      version: 1;
      target:
        | Readonly<{ kind: 'd1'; databaseId: string }>
        | Readonly<{ kind: 'r2'; bucketName: string }>;
      evidenceSha256: string;
      evidenceCount: number;
      stage: 'ordinary-version';
      ordinaryInventorySha256: string;
      scriptIndex: number;
      scriptName: string;
      deploymentSha256: string;
      versionIndex: number;
    }>
  | Readonly<{
      version: 1;
      target:
        | Readonly<{ kind: 'd1'; databaseId: string }>
        | Readonly<{ kind: 'r2'; bucketName: string }>;
      evidenceSha256: string;
      evidenceCount: number;
      stage: 'dispatch-namespace-inventory';
      ordinaryInventorySha256: string;
      namespaceInventorySha256?: string;
      namespaceIndex: number;
    }>
  | Readonly<{
      version: 1;
      target:
        | Readonly<{ kind: 'd1'; databaseId: string }>
        | Readonly<{ kind: 'r2'; bucketName: string }>;
      evidenceSha256: string;
      evidenceCount: number;
      stage: 'dispatch-script-page';
      ordinaryInventorySha256: string;
      namespaceInventorySha256: string;
      namespaceIndex: number;
      namespaceName: string;
      pageStartCursor?: string;
      pageNumber: number;
      seenCursorSha256: readonly string[];
      totalDispatchItems: number;
      dispatchEvidenceSum256: string;
      dispatchEvidenceCount: number;
    }>
  | Readonly<{
      version: 1;
      target:
        | Readonly<{ kind: 'd1'; databaseId: string }>
        | Readonly<{ kind: 'r2'; bucketName: string }>;
      evidenceSha256: string;
      evidenceCount: number;
      stage: 'dispatch-script-settings';
      ordinaryInventorySha256: string;
      namespaceInventorySha256: string;
      namespaceIndex: number;
      namespaceName: string;
      pageStartCursor?: string;
      nextCursor?: string;
      pageSha256: string;
      pageItemCount: number;
      itemOffset: number;
      pageNumber: number;
      seenCursorSha256: readonly string[];
      totalDispatchItems: number;
      dispatchEvidenceSum256: string;
      dispatchEvidenceCount: number;
    }>;

export interface DecommissionAttachmentScanEvidence {
  readonly evidenceSha256: string;
  readonly evidenceCount: number;
}

export interface DecommissionIntentCommon {
  readonly version: 1;
  readonly operationId: string;
  readonly revision: number;
  readonly generation: number;
  readonly updatedAt: string;
  readonly identity: DecommissionOperationIdentity;
  readonly databaseExportReceiptAuthority?: string;
  readonly lifecyclePhase: NormalDecommissionLifecyclePhase;
}

export type DecommissionAdvanceIntent =
  | (DecommissionIntentCommon & Readonly<{ state: 'transitioning' }>)
  | (DecommissionIntentCommon &
      Readonly<{
        state: 'discover';
        purpose: DecommissionAttachmentPurpose;
        progress: DecommissionAttachmentProgress;
      }>)
  | (DecommissionIntentCommon &
      Readonly<{
        state: 'verify';
        purpose: DecommissionAttachmentPurpose;
        progress: DecommissionAttachmentProgress;
        discoverEvidence: DecommissionAttachmentScanEvidence;
      }>)
  | (DecommissionIntentCommon &
      Readonly<{
        state: 'blocked';
        purpose: DecommissionAttachmentPurpose;
        attachment: DecommissionBlockedAttachment;
      }>)
  | Readonly<{
      version: 1;
      operationId: string;
      revision: number;
      generation: number;
      updatedAt: string;
      identity: DecommissionOperationIdentity;
      databaseExportReceiptAuthority: string;
      lifecyclePhase: 'decommissioned';
      state: 'complete';
    }>;

export interface DecommissionAdvanceToken {
  readonly version: 1;
  readonly tenantTag: string;
  readonly environment: string;
  readonly operationId: string;
  readonly revision: number;
}

export type DecommissionAdvanceTokenClassification = 'current' | 'stale';

/** Call-local request for one read-only bounded provider scan chunk. */
export interface DecommissionAttachmentScanInput {
  readonly progress: DecommissionAttachmentProgress;
  /** Reserved provider-attempt ceiling; an integer from 9 through 1,000. */
  readonly maxProviderRequests: number;
  /** Call-local cancellation; never persisted in a shell or Queue token. */
  readonly signal?: AbortSignal;
}

/** Read-only provider facts; never durable absence or deletion authority. */
export type DecommissionAttachmentScanResult =
  | Readonly<{
      /** More read-only provider work remains. */
      status: 'pending';
      progress: DecommissionAttachmentProgress;
      providerFetchAttemptsReserved: number;
    }>
  | Readonly<{
      /** The first safe Worker attachment found by this chunk. */
      status: 'attached';
      attachment: DecommissionBlockedAttachment;
      providerFetchAttemptsReserved: number;
    }>
  | Readonly<{
      /** This pass completed; evidence is not durable deletion authority. */
      status: 'complete';
      evidenceSha256: string;
      evidenceCount: number;
      providerFetchAttemptsReserved: number;
    }>
  | Readonly<{
      /** Provider inventory changed; the caller must start a new generation. */
      status: 'drift';
    }>;

/** Versioned provider-neutral candidate-invocation authority carrier. */
export interface InvocationAuthorityCarrier {
  readonly version: 1;
  /** ISO timestamp of the durable authorization commit, or null when never authorized. */
  readonly authorizedAt: string | null;
}

/** Purpose binding that decommission codecs structurally reject. */
export interface CleanupAttachmentPurpose {
  readonly kind: 'cleanup-database-pre-delete';
  readonly databaseId: string;
  readonly operationId: string;
}

/**
 * Durable scan progress DEFINED here, mirroring the existing
 * `DecommissionAttachmentProgress` precedent field for field. types.ts does
 * NOT import cloudflare-worker-attachment-scan-state.ts. There is NO
 * conversion function: exactly like decommission today, the engine passes
 * `intent.progress` to the backend scan capability directly and, on a pending
 * chunk, validates the returned value with `parseWorkerAttachmentScanProgress`
 * and assigns it structurally (the shapes are identical).
 */
export type CleanupAttachmentProgress =
  | Readonly<{
      version: 1;
      target:
        | Readonly<{ kind: 'd1'; databaseId: string }>
        | Readonly<{ kind: 'r2'; bucketName: string }>;
      evidenceSha256: string;
      evidenceCount: number;
      stage: 'ordinary-script-inventory';
      ordinaryInventorySha256?: string;
      scriptIndex: number;
    }>
  | Readonly<{
      version: 1;
      target:
        | Readonly<{ kind: 'd1'; databaseId: string }>
        | Readonly<{ kind: 'r2'; bucketName: string }>;
      evidenceSha256: string;
      evidenceCount: number;
      stage: 'ordinary-deployment';
      ordinaryInventorySha256: string;
      scriptIndex: number;
      scriptName: string;
    }>
  | Readonly<{
      version: 1;
      target:
        | Readonly<{ kind: 'd1'; databaseId: string }>
        | Readonly<{ kind: 'r2'; bucketName: string }>;
      evidenceSha256: string;
      evidenceCount: number;
      stage: 'ordinary-version';
      ordinaryInventorySha256: string;
      scriptIndex: number;
      scriptName: string;
      deploymentSha256: string;
      versionIndex: number;
    }>
  | Readonly<{
      version: 1;
      target:
        | Readonly<{ kind: 'd1'; databaseId: string }>
        | Readonly<{ kind: 'r2'; bucketName: string }>;
      evidenceSha256: string;
      evidenceCount: number;
      stage: 'dispatch-namespace-inventory';
      ordinaryInventorySha256: string;
      namespaceInventorySha256?: string;
      namespaceIndex: number;
    }>
  | Readonly<{
      version: 1;
      target:
        | Readonly<{ kind: 'd1'; databaseId: string }>
        | Readonly<{ kind: 'r2'; bucketName: string }>;
      evidenceSha256: string;
      evidenceCount: number;
      stage: 'dispatch-script-page';
      ordinaryInventorySha256: string;
      namespaceInventorySha256: string;
      namespaceIndex: number;
      namespaceName: string;
      pageStartCursor?: string;
      pageNumber: number;
      seenCursorSha256: readonly string[];
      totalDispatchItems: number;
      dispatchEvidenceSum256: string;
      dispatchEvidenceCount: number;
    }>
  | Readonly<{
      version: 1;
      target:
        | Readonly<{ kind: 'd1'; databaseId: string }>
        | Readonly<{ kind: 'r2'; bucketName: string }>;
      evidenceSha256: string;
      evidenceCount: number;
      stage: 'dispatch-script-settings';
      ordinaryInventorySha256: string;
      namespaceInventorySha256: string;
      namespaceIndex: number;
      namespaceName: string;
      pageStartCursor?: string;
      nextCursor?: string;
      pageSha256: string;
      pageItemCount: number;
      itemOffset: number;
      pageNumber: number;
      seenCursorSha256: readonly string[];
      totalDispatchItems: number;
      dispatchEvidenceSum256: string;
      dispatchEvidenceCount: number;
    }>;

/** One bounded cleanup attachment-scan pass and its durable progress. */
export interface CleanupAttachmentScan {
  readonly purpose: CleanupAttachmentPurpose;
  readonly pass: 'discover' | 'verify';
  readonly progress: CleanupAttachmentProgress;
  /** Present only during the verify pass. */
  readonly discoverEvidence?: Readonly<{
    evidenceSha256: string;
    evidenceCount: number;
  }>;
}

/** Who authorized this cleanup, with persisted rollback attempt facts. */
export type CleanupAuthority =
  | Readonly<{ kind: 'manual-cleanup' }>
  | Readonly<{
      kind: 'provisioning-rollback';
      reservationOwned: boolean;
      databaseOwned: boolean;
      workerCreatedByAttempt: boolean;
      workerResourceState: 'absent' | 'present' | 'unknown';
      requestedSpecDigest: string;
    }>;

/** One durable cleanup step; one call performs at most one step's group. */
export type CleanupAdvanceState =
  | Readonly<{ step: 'teardown-traffic' }>
  | Readonly<{ step: 'teardown-worker' }>
  | Readonly<{ step: 'teardown-platform' }>
  | Readonly<{
      step: 'r2-deletion';
      startResourceIndex: number;
      verifiedDetachmentResourceIndex?: number;
    }>
  | Readonly<{ step: 'attachment-scan'; scan: CleanupAttachmentScan }>
  | Readonly<{
      step: 'blocked';
      purpose: CleanupAttachmentPurpose;
      attachment: DecommissionBlockedAttachment;
    }>
  | Readonly<{ step: 'database-deletion' }>;

/**
 * Durable authority for one bounded cleanup or provisioning-rollback
 * operation. Cleanup has no terminal intent state — the terminal deletes the
 * Fleet row — so any present intent is active.
 */
export interface CleanupAdvanceIntent {
  readonly version: 1;
  readonly operationId: string;
  readonly revision: number;
  readonly generation: number;
  readonly updatedAt: string;
  readonly authority: CleanupAuthority;
  readonly identity: Readonly<{
    record: Readonly<{
      tenantTag: string;
      environment: string;
      backend: ProvisioningBackendKind;
      scriptName: string;
      databaseId: string;
      databaseName: string;
      routeHostname: string;
    }>;
    admittedPhase: ProvisioningPhase;
    /** backend.immutableExternalArtifacts === true at admission. */
    externalArtifact: boolean;
  }>;
  readonly state: CleanupAdvanceState;
}

/** Transport-neutral non-authoritative continuation token for one cleanup. */
export interface CleanupAdvanceToken {
  readonly version: 1;
  readonly tenantTag: string;
  readonly environment: string;
  readonly operationId: string;
  readonly revision: number;
}

/** Terminal evidence recorded on the immutable cleanup receipt. */
export interface CleanupReceiptEvidence {
  readonly eligibility:
    | 'carrier-null'
    | 'legacy-phase-impossible'
    | 'reservation-only';
  readonly ingressRemoved: boolean;
  readonly workerAbsent: boolean;
  readonly platformResourcesAbsent: boolean;
  readonly applicationR2Settled: boolean;
  readonly databaseAbsentReadback: boolean;
  readonly scan?: Readonly<{
    discover: Readonly<{ evidenceSha256: string; evidenceCount: number }>;
    verify: Readonly<{ evidenceSha256: string; evidenceCount: number }>;
  }>;
}

/** Operation-keyed immutable terminal receipt persisted outside the Fleet row. */
export interface CleanupTerminalReceipt {
  readonly version: 1;
  readonly operationId: string;
  readonly tenantTag: string;
  readonly environment: string;
  readonly backend: ProvisioningBackendKind;
  readonly scriptName: string;
  readonly databaseId: string;
  readonly databaseName: string;
  readonly authority: 'manual-cleanup' | 'provisioning-rollback';
  readonly admittedPhase: ProvisioningPhase;
  readonly disposition:
    | 'prepublication-owned-no-export'
    | 'reservation-cleared';
  readonly evidence: CleanupReceiptEvidence;
  /** D1-assigned; present on every read/return path, absent only on the caller-constructed input. */
  readonly completedAtMs?: number;
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
  readonly backendSwitchIntent?: BackendSwitchIntent;
  readonly decommissionIntent?: DecommissionAdvanceIntent;
  readonly cleanupIntent?: CleanupAdvanceIntent;
  readonly invocationAuthority?: InvocationAuthorityCarrier;
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

export function effectiveLifecyclePhase(
  record: FleetRecord,
): ProvisioningPhase {
  if (record.phase === 'cleanup-advancing') {
    if (!record.cleanupIntent) {
      throw new Error('cleanup-advancing record has no active cleanup intent');
    }
    return 'cleanup-advancing';
  }
  if (record.cleanupIntent) {
    throw new Error('fleet record has inconsistent cleanup intent state');
  }
  const intent = record.decommissionIntent;
  if (record.phase === 'decommission-advancing') {
    if (!intent || intent.state === 'complete') {
      throw new Error(
        'decommission-advancing record has no active decommission intent',
      );
    }
    return intent.lifecyclePhase;
  }
  if (
    intent &&
    !(record.phase === 'decommissioned' && intent.state === 'complete')
  ) {
    throw new Error('fleet record has inconsistent decommission intent state');
  }
  return record.phase;
}

export function assertNoActiveDecommission(
  record: FleetRecord,
  operation: string,
): void {
  if (
    record.phase === 'decommission-advancing' ||
    (record.decommissionIntent &&
      !(
        record.phase === 'decommissioned' &&
        record.decommissionIntent.state === 'complete'
      ))
  ) {
    throw new Error(`${operation} cannot run during an active decommission`);
  }
}

/**
 * Refuses lifecycle entries while a bounded cleanup is active. Cleanup has no
 * terminal intent state — the terminal deletes the Fleet row — so ANY present
 * intent is active.
 */
export function assertNoActiveCleanup(
  record: FleetRecord,
  operation: string,
): void {
  if (
    record.phase === 'cleanup-advancing' ||
    record.cleanupIntent !== undefined
  ) {
    throw new Error(`${operation} cannot run during an active cleanup`);
  }
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

/** Independently verified byte integrity for one complete database export. */
export interface DatabaseExportIntegrity {
  /** Positive safe-integer byte length of the complete export. */
  readonly size: number;
  /** Lowercase hexadecimal SHA-256 digest of the complete export. */
  readonly sha256: string;
}

/**
 * Immutable identity of one durable database-export receipt.
 *
 * `authority` identifies the configured storage root and must remain unchanged
 * for every retry. Reusing the same complete identity converges only when the
 * already-committed export has exact byte integrity; a collision is preserved
 * and refused.
 */
export interface DatabaseExportReceiptIdentity {
  /** Receipt derivation version. Version 1 is permanently stable. */
  readonly version: 1;
  /** Canonical identifier of the immutable receipt storage authority. */
  readonly authority: string;
  /** Immutable provider database identifier. */
  readonly databaseId: string;
  /** Durable UUIDv4 operation identifier reused by every retry. */
  readonly operationId: string;
}

export interface DatabaseExport extends DatabaseExportIntegrity {
  readonly databaseId: string;
  readonly location: string;
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
      /** Optional provider script selector retained without reinterpretation. */
      scriptName?: string;
      /** Optional provider dispatch selector retained without reinterpretation. */
      dispatchNamespace?: string;
    }>
  | Readonly<{
      type: 'service';
      name: string | undefined;
      service: string | undefined;
      /** Optional service entrypoint retained for exact-version comparison. */
      entrypoint?: string;
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
      /** Provider-observable jurisdiction when Fleet can represent it. */
      jurisdiction?: 'eu' | 'fedramp';
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
    }>
  | Readonly<{
      type: 'unsupported';
      name: string | undefined;
      /** Supported raw provider type whose decision fields were malformed. */
      providerType:
        | 'd1'
        | 'durable_object_namespace'
        | 'service'
        | 'queue'
        | 'r2_bucket'
        | 'plain_text'
        | 'secret_text';
      /** Prevents malformed supported input from being normalized lossily. */
      issue: 'malformed-supported-binding';
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
export interface PlainWorkerDatabaseExportResult
  extends DatabaseExportIntegrity {
  /** Durable location returned by the export store. */
  readonly location: string;
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
  /**
   * Advances one bounded, read-only attachment scan chunk.
   *
   * The implementation must use the same Cloudflare account and credential
   * authority as this port's teardown mutations, including when a route API is
   * paired with a Wrangler runner. It never performs an unbounded fallback and
   * returns no durable absence or deletion authority.
   */
  advanceDecommissionAttachmentScan?(
    input: DecommissionAttachmentScanInput,
  ): Promise<DecommissionAttachmentScanResult>;
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
 * assertion. The direct-API adapter does not pre-assert either, so both
 * adapters have identical assertion counts.
 *
 * A method that resolves a `PlainWorkerMutationOutcome` over a transport that
 * asserts per request must ALSO assert explicitly before dispatch, so a lost
 * lease rejects instead of resolving `failed` and triggering readback.
 *
 * `PlainWorkerRouteApi.withMutationFence` entry is not itself an ownership
 * assertion; assertion belongs to each mutating request. Nested scopes retain
 * that request-level contract. Inherited `PlainWorkerRouteApi` members retain
 * their own contract.
 *
 * `undefined` and `'absent'` mean provider absence only. Adapters propagate
 * provider status and classification, may strip transport bodies and redact
 * secret material from messages, and never classify a fence failure as
 * absence. Methods without an absence-typed result do not classify absence.
 *
 * `createDatabase`, `uploadCandidate`, and `createDeployment` resolve a failed
 * outcome only after a provider mutation request was dispatched and failed or
 * its result became unknown; a preceding provider read does not count as
 * dispatch. They reject failures that provably predate that dispatch, including
 * fence assertion and local preparation failures. Other mutations reject on
 * failure except for their documented absence result.
 */
export interface PlainWorkerProvisioningApi extends PlainWorkerRouteApi {
  /** Maximum duration of any one provider mutation request after assertion. */
  readonly maxMutationDurationMs: number;
  /** Whether immutable-ID D1 reads and deletion are both available. */
  readonly supportsExactDatabaseDeletion: boolean;
  /**
   * Lists D1 database inventory facts visible to the adapter. A name filter
   * narrows the listing toward that name; an adapter forwards it where the
   * provider accepts one and filters locally otherwise, so a caller that needs
   * an exact match still compares the returned names.
   */
  listDatabases(
    filter?: Readonly<{ name?: string }>,
  ): Promise<readonly PlainWorkerDatabaseInventoryEntry[]>;
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
   * absence. The listing is bounded by item count and rejects rather than
   * truncating.
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
    versions: readonly OrdinaryWorkerDeploymentVersion[],
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
  /**
   * Canonical receipt storage authority. Present together with
   * `exportDatabaseReceipt`, absent together when unsupported, and immutable
   * for every retry of one receipt identity.
   */
  readonly databaseExportReceiptAuthority?: string;
  /**
   * Streams one operation-scoped export whose eager source-integrity promise is
   * independently verified by the configured store. An exact retry converges;
   * an identity or byte collision is preserved and refused.
   */
  exportDatabaseReceipt?(
    identity: DatabaseExportReceiptIdentity,
    fence: ExternalMutationFence,
  ): Promise<PlainWorkerDatabaseExportResult>;
}

export interface FleetStateLease extends ExternalMutationFence {
  readonly tenantTag: string;
  readonly environment: string;
  renew(): Promise<void>;
  put(record: FleetRecord): Promise<void>;
  delete(): Promise<void>;
  /**
   * Atomically persists the immutable terminal cleanup receipt, releases this
   * deployment's ownership claims, and deletes the Fleet row in one guarded
   * batch. Optional so external lease implementations do not break; callers
   * detect it with `Reflect.has`.
   */
  completeCleanup?(
    input: Readonly<{
      /** The terminal receipt, without `completedAtMs`. */
      receipt: CleanupTerminalReceipt;
      /** The intent revision the engine acted on. */
      expectedRevision: number;
    }>,
  ): Promise<CleanupTerminalReceipt>;
  /**
   * Force path: deletes the Fleet row AND releases this deployment's current
   * claims, with no receipt. Optional; legacy lease implementations keep
   * tombstone claims through `delete()`.
   */
  deleteReleasingClaims?(): Promise<void>;
}

export interface FleetStateStore {
  withDeploymentLease<T>(
    tenantTag: string,
    environment: string,
    operation: (lease: FleetStateLease) => Promise<T>,
  ): Promise<T>;
  get(tenantTag: string, environment: string): Promise<FleetRecord | undefined>;
  list(): Promise<readonly FleetRecord[]>;
  /** Reads one immutable terminal cleanup receipt by operation id. */
  readCleanupReceipt?(
    operationId: string,
  ): Promise<CleanupTerminalReceipt | undefined>;
  /**
   * Bounded explicit receipt GC: deletes at most `limit` receipts whose
   * D1-assigned `completedAtMs` is before the cutoff, in stable
   * completed-time-then-operation order. `limit` is an integer from 1 to
   * 1,000; anything else fails closed.
   */
  pruneCleanupReceipts?(
    input: Readonly<{ completedBeforeMs: number; limit: number }>,
  ): Promise<Readonly<{ deleted: number }>>;
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
  /**
   * Reads one database by immutable ID. Only `undefined` means absence.
   *
   * Present results are descriptor-safe plain data with bounded `id` and
   * `name`, `created: false`, and optional safe plain-data fields. Destructive
   * consumers reconstruct the required fields and discard extras.
   */
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
  /**
   * Advances one bounded, read-only attachment scan chunk.
   *
   * It must use the same provider authority as this backend's teardown
   * mutations, never perform an unbounded fallback, and return no durable
   * absence or deletion authority.
   */
  advanceDecommissionAttachmentScan?(
    input: DecommissionAttachmentScanInput,
  ): Promise<DecommissionAttachmentScanResult>;
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
  /**
   * Checks deployment-owned D1 deletion residuals without enumerating the
   * account-wide Worker attachment inventory.
   *
   * This retains deployment identity, route, release, inventory, control
   * Worker, Durable Object, and initial/final lease checks. It is read-only and
   * is not durable absence or deletion authority.
   */
  assertDatabaseDeletionResidualsRemoved?(
    spec: DeploymentSpec,
    record: FleetRecord,
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void>;
  exportDatabase(
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<DatabaseExport>;
  /**
   * Canonical receipt storage authority. Present together with
   * `exportDatabaseReceipt`, absent together when unsupported, and immutable
   * for every retry of one receipt identity.
   */
  readonly databaseExportReceiptAuthority?: string;
  /**
   * Exports one operation-scoped receipt. The lower store consumes the body
   * while its eager source-integrity promise settles, exact retries converge,
   * and identity or byte collisions are preserved and refused. The result must
   * be descriptor-safe plain data; bounded destructive consumers reconstruct
   * the required export fields and discard safe extras.
   */
  exportDatabaseReceipt?(
    identity: DatabaseExportReceiptIdentity,
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
