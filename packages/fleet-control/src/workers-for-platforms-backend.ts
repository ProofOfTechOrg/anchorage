// SPDX-License-Identifier: Apache-2.0

import { createHash, createHmac } from 'node:crypto';
import {
  provisionDeploymentIdentityProtocol,
  readDeploymentIdentityProtocol,
} from '@proofoftech/flowsafe/deployment-identity-protocol';
import type { MaintenanceCapabilityJwk } from '@proofoftech/flowsafe/host-kit';
import {
  MAINTENANCE_RECEIPT_HEADER,
  mintAsymmetricMaintenanceCapability,
  verifyMaintenanceReceipt,
} from '@proofoftech/flowsafe/host-kit';
import {
  applicationSecretNames,
  applicationSecretValues,
} from './application-bindings.js';
import { WorkerDeploymentError } from './deployment-error.js';
import { readMaintenanceHealth } from './maintenance-health.js';
import {
  applyMigrationsWithLedger,
  d1MigrationHistoryDigest,
} from './migration-ledger.js';
import {
  assertExternalPlatformTargetCompatibility,
  canonicalDeploymentEgressPolicy,
  canonicalMaintenanceCapabilityPublicKey,
  durableObjectMigrationHistoryDigest,
  externalEgressProxyScriptName,
  externalPlatformResourceGroupId,
  externalStateDeploymentSpec,
  externalStateScriptName,
  FLEET_AUDIT_PROXY_BINDING,
  FLEET_AUDIT_PROXY_CLASS_NAME,
  FLEET_AUDIT_PROXY_STATE_BINDING,
  trustedArtifactDigest,
  validateExternalPlatformProfile,
} from './platform-resources.js';
import { assertProviderBindingIdentitiesMatchInspection } from './provider-binding-inventory.js';
import { deploymentSpecDigest } from './spec-digest.js';
import type {
  D1Migration,
  DatabaseExport,
  DatabaseReference,
  DeploymentEgressPolicy,
  DeploymentSecrets,
  DeploymentSpec,
  DurableObjectBindingInventory,
  ExternalMutationFence,
  ExternalPlatformProfile,
  ExternalPlatformResources,
  ExternalPlatformTargetDescription,
  ExternalReleaseSnapshot,
  FleetRecord,
  LiveDeployment,
  MaintenanceHealth,
  PromotionGuard,
  ProviderBindingIdentity,
  ProvisioningBackend,
  ScriptInventoryTarget,
  SeedDeploymentIdentityOptions,
} from './types.js';

const RELEASE_DIGEST_LENGTH = 48;
const DEFAULT_MAINTENANCE_REQUEST_TIMEOUT_MS = 15_000;
const MAINTENANCE_CAPABILITY_MAX_TTL_SECONDS = 60;
const MAINTENANCE_CAPABILITY_SKEW_SECONDS = 5;

export function externalReleaseScriptName(spec: DeploymentSpec): string {
  const digest = deploymentSpecDigest(spec);
  const suffix = digest.slice(0, RELEASE_DIGEST_LENGTH);
  const logicalPrefix = spec.scriptName.slice(0, 63 - suffix.length - 1);
  return `${logicalPrefix}-${suffix}`;
}

function candidateMaintenanceUrl(
  spec: DeploymentSpec,
  physicalScriptName: string,
  operation: 'ensure-maintenance' | 'maintenance-status',
): URL {
  return new URL(
    `/.well-known/anchorage/maintenance/${encodeURIComponent(spec.tenantTag)}/${encodeURIComponent(spec.environment)}/${encodeURIComponent(physicalScriptName)}/${deploymentSpecDigest(spec)}/${operation}`,
    spec.maintenanceBaseUrl,
  );
}

function bindingKeys(
  bindings: readonly Readonly<{
    name: string;
    className: string;
    scriptName?: string;
    dispatchNamespace?: string;
  }>[],
): string {
  return JSON.stringify(
    bindings
      .map(
        (binding) =>
          `${binding.name}:${binding.className}:${binding.scriptName ?? ''}:${binding.dispatchNamespace ?? ''}`,
      )
      .sort(),
  );
}

function localBindingKeys(
  bindings: readonly Readonly<{ name: string; className: string }>[],
): string {
  return JSON.stringify(
    bindings.map((binding) => `${binding.name}:${binding.className}`).sort(),
  );
}

export interface WorkersForPlatformsApi {
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
    jurisdiction: import('./types.js').R2Jurisdiction,
  ): Promise<import('./types.js').ApplicationR2BucketSnapshot | undefined>;
  createR2Bucket?(
    resource: import('./types.js').ApplicationR2Binding,
    fence: ExternalMutationFence,
  ): Promise<void>;
  assertR2BucketEmpty?(
    resource: import('./types.js').ApplicationR2Binding,
  ): Promise<void>;
  deleteR2Bucket?(
    resource: import('./types.js').ApplicationR2Binding,
    fence: ExternalMutationFence,
  ): Promise<void>;
  withMutationFence<T>(
    fence: ExternalMutationFence,
    operation: () => Promise<T>,
  ): Promise<T>;
  findDatabase(name: string): Promise<DatabaseReference | undefined>;
  getDatabase(databaseId: string): Promise<DatabaseReference | undefined>;
  createDatabase(name: string): Promise<DatabaseReference>;
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
  uploadDispatchWorker(
    spec: DeploymentSpec,
    database: DatabaseReference,
    physicalScriptName?: string,
    platformResources?: ExternalPlatformResources,
    application?: import('./types.js').ApplicationBindingTopology,
  ): Promise<{ artifactVersion: string }>;
  uploadNamespacedStateWorker?(options: {
    readonly spec: DeploymentSpec;
    readonly database: DatabaseReference;
    readonly artifact: import('./types.js').TrustedWorkerArtifact;
    readonly artifactDigest: string;
    readonly maintenanceCapabilityPublicKey: string;
    readonly auditQueueName?: string;
    readonly sharedOutboundWorkerName: string;
    readonly stateEgressCredentialDigest: string;
  }): Promise<{ artifactVersion: string }>;
  uploadControlWorker(spec: {
    readonly scriptName: string;
    readonly mainModule: string;
    readonly modules: DeploymentSpec['modules'];
    readonly compatibilityDate: string;
    readonly compatibilityFlags?: readonly string[];
    readonly bindings: readonly Readonly<Record<string, unknown>>[];
    readonly migrations?: Readonly<Record<string, unknown>>;
    readonly tags?: readonly string[];
  }): Promise<string>;
  putControlSecrets(
    scriptName: string,
    secrets: Readonly<Record<string, string>>,
  ): Promise<void>;
  deleteControlSecrets?(
    scriptName: string,
    secretNames: readonly string[],
    fence: ExternalMutationFence,
  ): Promise<void>;
  inspectControlWorker(scriptName: string): Promise<
    | {
        artifactVersion: string;
        databaseIds: readonly string[];
        durableObjectBindings: readonly DurableObjectBindingInventory[];
        serviceBindings: readonly Readonly<{
          name: string;
          service: string;
          entrypoint?: string;
        }>[];
        queueProducerBindings?: readonly Readonly<{
          name: string;
          queueName: string;
        }>[];
        r2BucketBindings?: readonly import('./types.js').ApplicationR2Binding[];
        kvNamespaceBindings: readonly Readonly<{
          name: string;
          namespaceId: string;
        }>[];
        secretNames: readonly string[];
        plainTextBindings: Readonly<Record<string, string>>;
        providerBindingIdentities: readonly ProviderBindingIdentity[];
        workersDevEnabled: boolean;
        previewUrlsEnabled: boolean;
        routeHostnames: readonly string[];
        zoneRoutes: readonly import('./types.js').WorkerZoneRoute[];
      }
    | undefined
  >;
  revokeControlSecrets(scriptName: string): Promise<void>;
  disableControlWorkerPublicAccess(scriptName: string): Promise<void>;
  deleteControlWorker(scriptName: string): Promise<void>;
  hasDurableObjectNamespace(namespaceId: string): Promise<boolean>;
  listDurableObjectNamespaces(scriptName: string): Promise<readonly string[]>;
  putDispatchSecrets(
    scriptName: string,
    secrets: DeploymentSecrets,
    options?: Readonly<{
      includeMaintenanceAdmin?: boolean;
      additionalSecrets?: Readonly<Record<string, string>>;
    }>,
  ): Promise<void>;
  inspectDispatchWorker(scriptName: string): Promise<
    | {
        artifactVersion: string;
        databaseIds: readonly string[];
        durableObjectBindings: readonly DurableObjectBindingInventory[];
        serviceBindings?: readonly Readonly<{
          name: string;
          service: string;
          entrypoint?: string;
        }>[];
        queueProducerBindings?: readonly Readonly<{
          name: string;
          queueName: string;
        }>[];
        r2BucketBindings?: readonly import('./types.js').ApplicationR2Binding[];
        secretNames: readonly string[];
        tenantTag: string;
        environment: string;
        schemaVersion: number;
        desiredSpecDigest: string;
        durableObjectTag?: string;
        plainTextBindings: Readonly<Record<string, string>>;
        providerBindingIdentities: readonly ProviderBindingIdentity[];
      }
    | undefined
  >;
  revokeDispatchSecrets(scriptName: string): Promise<void>;
  deleteDispatchWorker(scriptName: string): Promise<void>;
  exportDatabase(databaseId: string): Promise<DatabaseExport>;
  deleteDatabase(databaseId: string): Promise<void>;
  putHostRouting(
    namespaceId: string,
    hostname: string,
    target: {
      readonly scriptName: string;
      readonly tenantTag: string;
      readonly environment: string;
      readonly policyId: string;
      readonly policyDigest: string;
      readonly policyHosts: readonly string[];
      readonly stateEgress?: Readonly<{
        resourceGroupId: string;
        stateScriptName: string;
        credentialDigest: string;
      }>;
    },
    guard: PromotionGuard,
  ): Promise<void>;
  deleteHostRouting(
    namespaceId: string,
    hostname: string,
    allowedTargets: readonly Readonly<{
      readonly scriptName: string;
      readonly tenantTag: string;
      readonly environment: string;
    }>[],
  ): Promise<void>;
  getHostRouting(
    namespaceId: string,
    hostname: string,
  ): Promise<string | undefined>;
  putScriptInventory(
    namespaceId: string,
    target: ScriptInventoryTarget,
  ): Promise<void>;
  deleteScriptInventory(
    namespaceId: string,
    expected: ScriptInventoryTarget,
  ): Promise<void>;
  getScriptInventory(
    namespaceId: string,
    scriptName: string,
  ): Promise<ScriptInventoryTarget | undefined>;
}

export function deriveStateEgressCredential(
  rootSecret: string,
  spec: DeploymentSpec,
  stateScriptName = externalStateScriptName(spec),
): string {
  if (rootSecret.length < 32) {
    throw new Error(
      'state egress root secret must contain at least 32 characters',
    );
  }
  return createHmac('sha256', rootSecret)
    .update(
      JSON.stringify({
        tenantTag: spec.tenantTag,
        environment: spec.environment,
        resourceGroupId: externalPlatformResourceGroupId(spec),
        stateScriptName,
        routeHostname: spec.routeHostname.toLowerCase(),
        policyId: externalPlatformResourceGroupId(spec),
      }),
    )
    .digest('base64url');
}

export class WorkersForPlatformsBackend implements ProvisioningBackend {
  readonly kind = 'workers-for-platforms' as const;
  readonly immutableExternalArtifacts = true as const;
  readonly #client: WorkersForPlatformsApi;
  readonly #fetch: typeof fetch;
  readonly #hostRoutingKvId: string;
  readonly #auditQueueName?: string;
  readonly #maintenanceRequestTimeoutMs: number;
  readonly #platformProfileFor?: (
    spec: DeploymentSpec,
  ) => ExternalPlatformProfile;
  readonly #namespacedState: Readonly<{
    dispatchNamespace: string;
    sharedOutboundWorkerName: string;
    stateEgressRootSecret: string;
  }>;

  constructor(options: {
    readonly client: WorkersForPlatformsApi;
    readonly fetch?: typeof fetch;
    readonly hostRoutingKvId: string;
    readonly auditQueueName?: string;
    readonly maintenanceRequestTimeoutMs?: number;
    readonly platformProfileFor?: (
      spec: DeploymentSpec,
    ) => ExternalPlatformProfile;
    readonly namespacedState: Readonly<{
      dispatchNamespace: string;
      sharedOutboundWorkerName: string;
      stateEgressRootSecret: string;
    }>;
  }) {
    if (!options.hostRoutingKvId) {
      throw new Error('hostRoutingKvId is required');
    }
    this.#client = options.client;
    this.#fetch = options.fetch ?? fetch;
    this.#hostRoutingKvId = options.hostRoutingKvId;
    this.#auditQueueName = options.auditQueueName;
    this.#maintenanceRequestTimeoutMs =
      options.maintenanceRequestTimeoutMs ??
      DEFAULT_MAINTENANCE_REQUEST_TIMEOUT_MS;
    if (
      !Number.isSafeInteger(this.#maintenanceRequestTimeoutMs) ||
      this.#maintenanceRequestTimeoutMs < 1 ||
      Math.ceil(this.#maintenanceRequestTimeoutMs / 1_000) +
        MAINTENANCE_CAPABILITY_SKEW_SECONDS >
        MAINTENANCE_CAPABILITY_MAX_TTL_SECONDS
    ) {
      throw new Error(
        'maintenanceRequestTimeoutMs must be a positive integer that fits the one-minute capability lifetime',
      );
    }
    this.#platformProfileFor = options.platformProfileFor;
    if (
      !options.namespacedState?.dispatchNamespace ||
      !options.namespacedState.sharedOutboundWorkerName ||
      options.namespacedState.stateEgressRootSecret.length < 32
    ) {
      throw new Error(
        'Workers for Platforms requires a dispatch namespace, shared outbound Worker, and 32-byte state-egress root secret',
      );
    }
    this.#namespacedState = options.namespacedState;
  }

  #stateEgressCredential(spec: DeploymentSpec): string {
    return deriveStateEgressCredential(
      this.#namespacedState.stateEgressRootSecret,
      spec,
    );
  }

  #withMutationFence<T>(
    fence: ExternalMutationFence,
    operation: () => Promise<T>,
  ): Promise<T> {
    return this.#client.withMutationFence(fence, operation);
  }

  async #inspectDispatchWorker(scriptName: string) {
    const inspection = await this.#client.inspectDispatchWorker(scriptName);
    if (inspection) {
      assertProviderBindingIdentitiesMatchInspection(
        inspection,
        `dispatch Worker '${scriptName}'`,
      );
    }
    return inspection;
  }

  async #inspectControlWorker(scriptName: string) {
    const inspection = await this.#client.inspectControlWorker(scriptName);
    if (inspection) {
      assertProviderBindingIdentitiesMatchInspection(
        inspection,
        `control Worker '${scriptName}'`,
      );
    }
    return inspection;
  }

  #deploymentAuditQueueName(spec: DeploymentSpec): string | undefined {
    if (!spec.queueProducer) return undefined;
    if (
      spec.queueProducer.binding !== 'AUDIT_QUEUE' ||
      !this.#auditQueueName ||
      spec.queueProducer.queueName !== this.#auditQueueName
    ) {
      throw new Error(
        'external audit producer must target the backend-owned AUDIT_QUEUE',
      );
    }
    return this.#auditQueueName;
  }

  findDatabase(spec: DeploymentSpec): Promise<DatabaseReference | undefined> {
    return this.#client.findDatabase(spec.databaseName);
  }

  getDatabase(databaseId: string): Promise<DatabaseReference | undefined> {
    return this.#client.getDatabase(databaseId);
  }

  ensureDatabase(
    spec: DeploymentSpec,
    fence: ExternalMutationFence,
  ): Promise<DatabaseReference> {
    return this.#withMutationFence(fence, async () => {
      try {
        return await this.#client.createDatabase(spec.databaseName);
      } catch (cause) {
        const recovered = await this.#client.findDatabase(spec.databaseName);
        if (recovered) {
          const owner = await this.readDeploymentIdentity(recovered, fence);
          if (owner !== undefined) {
            throw new Error(
              `refusing authorized database reconciliation for '${recovered.id}' owned by '${owner}'`,
              { cause },
            );
          }
          return { ...recovered, created: true };
        }
        throw cause;
      }
    });
  }

  async seedDeploymentIdentity(
    database: DatabaseReference,
    tenantTag: string,
    fence: ExternalMutationFence,
    options: SeedDeploymentIdentityOptions,
  ): Promise<void> {
    await this.#withMutationFence(fence, () =>
      provisionDeploymentIdentityProtocol(
        async (statement) =>
          this.#client.queryDatabase(
            database.id,
            statement.sql,
            statement.bindings,
          ),
        tenantTag,
        {
          caller: 'WorkersForPlatformsBackend.seedDeploymentIdentity',
          initialExecutionFenceState: options.initialExecutionFenceState,
        },
      ),
    );
  }

  readDeploymentIdentity(
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<string | undefined> {
    return this.#withMutationFence(fence, () =>
      readDeploymentIdentityProtocol((statement) =>
        this.#client.queryDatabase(
          database.id,
          statement.sql,
          statement.bindings,
        ),
      ),
    );
  }

  releaseScriptName(spec: DeploymentSpec): string {
    return spec.authoredBy === 'external'
      ? externalReleaseScriptName(spec)
      : spec.scriptName;
  }

  async applyMigrations(
    database: DatabaseReference,
    migrations: readonly D1Migration[],
    fence: ExternalMutationFence,
  ): Promise<void> {
    await this.#withMutationFence(fence, () =>
      applyMigrationsWithLedger(
        {
          query: (sql, bindings) =>
            this.#client.queryDatabase(database.id, sql, bindings),
          batch: (statements) =>
            this.#client.batchDatabase(database.id, statements),
        },
        migrations,
      ),
    );
  }

  async findApplicationR2Bucket(
    resource: import('./types.js').ApplicationR2Binding,
  ): Promise<import('./types.js').ApplicationR2BucketSnapshot | undefined> {
    if (!this.#client.getR2Bucket) {
      throw new Error(
        'Workers for Platforms client does not support application R2',
      );
    }
    const found = await this.#client.getR2Bucket(
      resource.bucketName,
      resource.jurisdiction,
    );
    return found
      ? { ...resource, creationDate: found.creationDate }
      : undefined;
  }

  async ensureApplicationR2Bucket(
    resource: import('./types.js').ApplicationR2Binding,
    fence: ExternalMutationFence,
  ): Promise<import('./types.js').ApplicationR2BucketSnapshot> {
    if (!this.#client.createR2Bucket) {
      throw new Error(
        'Workers for Platforms client does not support application R2',
      );
    }
    try {
      await this.#client.createR2Bucket(resource, fence);
    } catch (error) {
      const reconciled = await this.findApplicationR2Bucket(resource);
      if (reconciled) return reconciled;
      if (
        error &&
        typeof error === 'object' &&
        'status' in error &&
        error.status === 409
      ) {
        throw new Error(
          `R2 bucket '${resource.bucketName}' conflicts with a foreign resource`,
        );
      }
      throw error;
    }
    const confirmed = await this.findApplicationR2Bucket(resource);
    if (!confirmed)
      throw new Error(
        `R2 bucket '${resource.bucketName}' is absent after create`,
      );
    return confirmed;
  }

  async assertApplicationR2Detached(
    resource: import('./types.js').ApplicationR2Binding,
    _fence: ExternalMutationFence,
  ): Promise<void> {
    if (!this.#client.listWorkerR2Attachments) {
      throw new Error(
        'Workers for Platforms client cannot scan R2 attachments',
      );
    }
    const attachments = await this.#client.listWorkerR2Attachments(
      resource.bucketName,
    );
    if (attachments.length > 0) {
      throw new Error(
        `R2 bucket '${resource.bucketName}' remains attached to a Worker`,
      );
    }
  }

  async assertApplicationR2Empty(
    resource: import('./types.js').ApplicationR2Binding,
    _fence: ExternalMutationFence,
  ): Promise<void> {
    if (!this.#client.assertR2BucketEmpty) {
      throw new Error(
        'Workers for Platforms client cannot inspect R2 contents',
      );
    }
    await this.#client.assertR2BucketEmpty(resource);
  }

  async deleteApplicationR2Bucket(
    resource: import('./types.js').ApplicationR2Binding,
    fence: ExternalMutationFence,
  ): Promise<void> {
    if (!this.#client.deleteR2Bucket) {
      throw new Error(
        'Workers for Platforms client cannot delete application R2',
      );
    }
    const current = await this.findApplicationR2Bucket(resource);
    if (!current || current.creationDate !== resource.creationDate) {
      throw new Error(`R2 bucket '${resource.bucketName}' ownership changed`);
    }
    await this.#client.deleteR2Bucket(resource, fence);
    if (await this.findApplicationR2Bucket(resource)) {
      throw new Error(
        `R2 bucket '${resource.bucketName}' remains after delete`,
      );
    }
  }

  #platformProfile(spec: DeploymentSpec): ExternalPlatformProfile {
    const profile = this.#platformProfileFor?.(spec);
    if (!profile) {
      throw new Error(
        'external Workers for Platforms deployment requires a trusted platformProfileFor provider',
      );
    }
    validateExternalPlatformProfile(spec, profile);
    return profile;
  }

  #capabilityPrivateKey(spec: DeploymentSpec): MaintenanceCapabilityJwk {
    const profile = this.#platformProfile(spec);
    const privateKey = profile.maintenanceCapabilityPrivateKey;
    if (
      !privateKey ||
      typeof privateKey.d !== 'string' ||
      typeof privateKey.x !== 'string' ||
      typeof privateKey.kid !== 'string'
    ) {
      throw new Error(
        'external maintenance requires a fleet-private Ed25519 capability signer',
      );
    }
    const publicKey = canonicalMaintenanceCapabilityPublicKey(
      JSON.stringify({
        kty: privateKey.kty,
        crv: privateKey.crv,
        alg: privateKey.alg,
        kid: privateKey.kid,
        x: privateKey.x,
      }),
    );
    if (publicKey !== profile.maintenanceCapabilityPublicKey) {
      throw new Error(
        'maintenance capability signer does not match the immutable platform verifier',
      );
    }
    return privateKey;
  }

  #describeExternalPlatformTarget(
    spec: DeploymentSpec,
    profile: ExternalPlatformProfile,
  ): ExternalPlatformTargetDescription {
    const stateDurableObjectTag =
      profile.stateDurableObjectMigrations.at(-1)?.tag;
    const stateEgressCredential = this.#stateEgressCredential(spec);
    return {
      ...(this.#deploymentAuditQueueName(spec)
        ? { auditQueueName: this.#deploymentAuditQueueName(spec) }
        : {}),
      maintenanceCapabilityPublicKey: canonicalMaintenanceCapabilityPublicKey(
        profile.maintenanceCapabilityPublicKey ?? '',
      ),
      stateArtifactDigest: trustedArtifactDigest(profile.stateWorker),
      stateDurableObjectHistoryDigest: durableObjectMigrationHistoryDigest(
        profile.stateDurableObjectMigrations,
      ),
      ...(stateDurableObjectTag ? { stateDurableObjectTag } : {}),
      stateEgressCredentialDigest: createHash('sha256')
        .update(stateEgressCredential)
        .digest('hex'),
      sharedOutboundWorkerName: this.#namespacedState.sharedOutboundWorkerName,
      d1SchemaVersion: spec.schemaVersion,
      d1SchemaHistoryDigest: d1MigrationHistoryDigest(spec.migrations),
      outboundPolicy: canonicalDeploymentEgressPolicy({
        policyId: externalPlatformResourceGroupId(spec),
        tenantTag: spec.tenantTag,
        environment: spec.environment,
        allowedHosts: profile.organizationEgressHosts,
      }),
    };
  }

  describeExternalPlatformTarget(
    spec: DeploymentSpec,
  ): ExternalPlatformTargetDescription {
    return this.#describeExternalPlatformTarget(
      spec,
      this.#platformProfile(spec),
    );
  }

  #assertNamespacedStateTransition(
    profile: ExternalPlatformProfile,
    currentTarget: ExternalPlatformTargetDescription | undefined,
    target: ExternalPlatformTargetDescription,
  ): void {
    if (!currentTarget) return;
    if (
      currentTarget.stateDurableObjectHistoryDigest ===
        target.stateDurableObjectHistoryDigest &&
      currentTarget.stateDurableObjectTag === target.stateDurableObjectTag
    ) {
      return;
    }
    const priorPrefixLength =
      profile.stateDurableObjectMigrations.findIndex(
        (_migration, index) =>
          durableObjectMigrationHistoryDigest(
            profile.stateDurableObjectMigrations.slice(0, index + 1),
          ) === currentTarget.stateDurableObjectHistoryDigest &&
          profile.stateDurableObjectMigrations[index]?.tag ===
            currentTarget.stateDurableObjectTag,
      ) + 1;
    const emptyPriorMatches =
      currentTarget.stateDurableObjectTag === undefined &&
      currentTarget.stateDurableObjectHistoryDigest ===
        durableObjectMigrationHistoryDigest([]);
    if (priorPrefixLength === 0 && !emptyPriorMatches) {
      throw new Error(
        'trusted state Durable Object migration history is not an exact append-only extension',
      );
    }
    if (
      profile.stateDurableObjectMigrations
        .slice(priorPrefixLength)
        .some(
          (migration) =>
            (migration.deletedClasses?.length ?? 0) > 0 ||
            (migration.renamedClasses?.length ?? 0) > 0,
        )
    ) {
      throw new Error(
        'trusted state profile cannot delete or rename Durable Object classes retained by routed releases',
      );
    }
  }

  async #inspectNamespacedState(
    spec: DeploymentSpec,
    database: DatabaseReference,
    profile: ExternalPlatformProfile,
    target: ExternalPlatformTargetDescription,
  ): Promise<ExternalPlatformResources['stateWorker'] | undefined> {
    const config = this.#namespacedState;
    const stateSpec = externalStateDeploymentSpec(spec, profile);
    const scriptName = externalStateScriptName(spec);
    const live = await this.#inspectDispatchWorker(scriptName);
    if (!live) return undefined;
    const auditQueueName = target.auditQueueName;
    const expectedBindings = [
      ...spec.durableObjectBindings.map((binding) => ({
        name: binding.name,
        className: binding.className,
      })),
      ...(auditQueueName
        ? [
            {
              name: FLEET_AUDIT_PROXY_STATE_BINDING,
              className: FLEET_AUDIT_PROXY_CLASS_NAME,
            },
          ]
        : []),
    ];
    const plainText = live.plainTextBindings ?? {};
    const expectedPlainText: Readonly<Record<string, string>> = {
      DEPLOYMENT_TENANT: spec.tenantTag,
      FLEET_ENVIRONMENT: spec.environment,
      FLEET_SCHEMA_VERSION: String(target.d1SchemaVersion),
      FLEET_SPEC_DIGEST: deploymentSpecDigest(stateSpec),
      FLEET_RESOURCE_GROUP: externalPlatformResourceGroupId(spec),
      FLEET_RESOURCE_ROLE: 'platform-state',
      FLEET_DEPLOYMENT_SCRIPT: spec.scriptName,
      FLEET_MAINTENANCE_CAPABILITIES: 'required',
      FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY:
        target.maintenanceCapabilityPublicKey,
      FLEET_ARTIFACT_DIGEST: target.stateArtifactDigest,
      FLEET_RUNTIME_CONTRACT: String(profile.runtimeContractVersion),
      OUTBOUND_TENANT_ID: spec.tenantTag,
      OUTBOUND_ENVIRONMENT: spec.environment,
      OUTBOUND_RESOURCE_GROUP_ID: externalPlatformResourceGroupId(spec),
      OUTBOUND_STATE_SCRIPT_NAME: scriptName,
      OUTBOUND_ROUTE_HOSTNAME: spec.routeHostname.toLowerCase(),
      OUTBOUND_POLICY_ID: target.outboundPolicy.policyId,
      ...(auditQueueName ? { FLEET_AUDIT_PROXY_INGRESS: 'required' } : {}),
    };
    if (
      live.tenantTag !== spec.tenantTag ||
      live.environment !== spec.environment ||
      live.schemaVersion !== target.d1SchemaVersion ||
      live.desiredSpecDigest !== deploymentSpecDigest(stateSpec) ||
      live.databaseIds.length !== 1 ||
      live.databaseIds[0] !== database.id ||
      localBindingKeys(live.durableObjectBindings) !==
        localBindingKeys(expectedBindings) ||
      JSON.stringify(live.serviceBindings ?? []) !==
        JSON.stringify([
          {
            name: 'OUTBOUND_PROXY',
            service: config.sharedOutboundWorkerName,
            entrypoint: 'StateEgress',
          },
        ]) ||
      JSON.stringify(live.queueProducerBindings ?? []) !==
        JSON.stringify(
          auditQueueName
            ? [{ name: 'AUDIT_QUEUE', queueName: auditQueueName }]
            : [],
        ) ||
      (live.r2BucketBindings ?? []).length !== 0 ||
      JSON.stringify([...live.secretNames].sort()) !==
        JSON.stringify(
          [
            'DEPLOYMENT_IDENTITY_SECRET',
            'MAINTENANCE_ADMIN_SECRET',
            'OUTBOUND_PROXY_CREDENTIAL',
          ].sort(),
        ) ||
      JSON.stringify(Object.entries(plainText).sort()) !==
        JSON.stringify(Object.entries(expectedPlainText).sort())
    ) {
      throw new Error(
        `trusted namespaced state Worker '${scriptName}' has drifted exact bindings`,
      );
    }
    const namespaceIds = [
      ...new Set([
        ...live.durableObjectBindings.map((binding) => binding.namespaceId),
        ...(await this.#client.listDurableObjectNamespaces(scriptName)),
      ]),
    ].sort();
    return {
      scriptName,
      artifactVersion: live.artifactVersion,
      artifactDigest: target.stateArtifactDigest,
      plane: 'dispatch',
      dispatchNamespace: config.dispatchNamespace,
      durableObjectBindings: live.durableObjectBindings,
      namespaceIds,
      ...(target.stateDurableObjectTag
        ? { durableObjectTag: target.stateDurableObjectTag }
        : {}),
    };
  }

  async #ensureNamespacedPlatformResources(
    spec: DeploymentSpec,
    database: DatabaseReference,
    secrets: DeploymentSecrets,
    profile: ExternalPlatformProfile,
    target: ExternalPlatformTargetDescription,
  ): Promise<import('./types.js').PlatformResourceProvisioningResult> {
    const config = this.#namespacedState;
    const upload = this.#client.uploadNamespacedStateWorker;
    const credential = this.#stateEgressCredential(spec);
    if (!upload) {
      throw new Error('namespaced state API is unavailable');
    }
    if (
      spec.queueProducer &&
      !profile.stateDurableObjectMigrations.some((migration) =>
        [
          ...(migration.newClasses ?? []),
          ...(migration.newSqliteClasses ?? []),
        ].includes(FLEET_AUDIT_PROXY_CLASS_NAME),
      )
    ) {
      throw new Error(
        `state profile does not provision reserved Durable Object class '${FLEET_AUDIT_PROXY_CLASS_NAME}'`,
      );
    }
    const scriptName = externalStateScriptName(spec);
    const existing = await this.#inspectDispatchWorker(scriptName);
    if (existing) {
      if (
        existing.tenantTag !== spec.tenantTag ||
        existing.environment !== spec.environment ||
        existing.databaseIds.length !== 1 ||
        existing.databaseIds[0] !== database.id ||
        existing.plainTextBindings?.FLEET_RESOURCE_ROLE !== 'platform-state' ||
        existing.plainTextBindings.FLEET_RESOURCE_GROUP !==
          externalPlatformResourceGroupId(spec)
      ) {
        throw new Error(
          `refusing to repair namespaced state Worker '${scriptName}' with another owner`,
        );
      }
      await upload.call(this.#client, {
        spec: externalStateDeploymentSpec(spec, profile),
        database,
        artifact: profile.stateWorker,
        artifactDigest: target.stateArtifactDigest,
        maintenanceCapabilityPublicKey: target.maintenanceCapabilityPublicKey,
        ...(target.auditQueueName
          ? { auditQueueName: target.auditQueueName }
          : {}),
        sharedOutboundWorkerName: config.sharedOutboundWorkerName,
        stateEgressCredentialDigest: target.stateEgressCredentialDigest ?? '',
      });
      await this.#client.putDispatchSecrets(scriptName, secrets, {
        additionalSecrets: { OUTBOUND_PROXY_CREDENTIAL: credential },
      });
      const inspected = await this.#inspectNamespacedState(
        spec,
        database,
        profile,
        target,
      );
      if (!inspected) throw new Error(`state Worker '${scriptName}' vanished`);
      return {
        resources: {
          ...(target.auditQueueName
            ? { auditQueueName: target.auditQueueName }
            : {}),
          maintenanceCapabilityPublicKey: target.maintenanceCapabilityPublicKey,
          stateWorker: inspected,
          outboundPolicy: target.outboundPolicy,
          sharedOutboundWorkerName: config.sharedOutboundWorkerName,
        },
        created: { stateWorker: false, egressProxy: false },
      };
    }
    const inventory = {
      scriptName,
      tenantTag: spec.tenantTag,
      environment: spec.environment,
      databaseId: database.id,
      routeHostname: spec.routeHostname,
    };
    await this.#client.putScriptInventory(this.#hostRoutingKvId, inventory);
    try {
      await upload.call(this.#client, {
        spec: externalStateDeploymentSpec(spec, profile),
        database,
        artifact: profile.stateWorker,
        artifactDigest: target.stateArtifactDigest,
        maintenanceCapabilityPublicKey: target.maintenanceCapabilityPublicKey,
        ...(target.auditQueueName
          ? { auditQueueName: target.auditQueueName }
          : {}),
        sharedOutboundWorkerName: config.sharedOutboundWorkerName,
        stateEgressCredentialDigest: target.stateEgressCredentialDigest ?? '',
      });
      await this.#client.putDispatchSecrets(scriptName, secrets, {
        additionalSecrets: { OUTBOUND_PROXY_CREDENTIAL: credential },
      });
      const stateWorker = await this.#inspectNamespacedState(
        spec,
        database,
        profile,
        target,
      );
      if (!stateWorker)
        throw new Error(`state Worker '${scriptName}' vanished`);
      return {
        resources: {
          ...(target.auditQueueName
            ? { auditQueueName: target.auditQueueName }
            : {}),
          maintenanceCapabilityPublicKey: target.maintenanceCapabilityPublicKey,
          stateWorker,
          outboundPolicy: target.outboundPolicy,
          sharedOutboundWorkerName: config.sharedOutboundWorkerName,
        },
        created: { stateWorker: true, egressProxy: false },
      };
    } catch (cause) {
      const cleanupErrors: unknown[] = [];
      try {
        await this.#client.revokeDispatchSecrets(scriptName);
        await this.#client.deleteDispatchWorker(scriptName);
        await this.#client.deleteScriptInventory(
          this.#hostRoutingKvId,
          inventory,
        );
      } catch (error) {
        cleanupErrors.push(error);
      }
      throw new AggregateError(
        [cause, ...cleanupErrors],
        `failed to provision namespaced state Worker '${scriptName}'`,
      );
    }
  }

  async inspectPlatformResources(
    spec: DeploymentSpec,
    database: DatabaseReference,
  ): Promise<ExternalPlatformResources | undefined> {
    const profile = this.#platformProfile(spec);
    const target = this.#describeExternalPlatformTarget(spec, profile);
    const stateWorker = await this.#inspectNamespacedState(
      spec,
      database,
      profile,
      target,
    );
    if (!stateWorker) return undefined;
    return {
      ...(target.auditQueueName
        ? { auditQueueName: target.auditQueueName }
        : {}),
      maintenanceCapabilityPublicKey: target.maintenanceCapabilityPublicKey,
      stateWorker,
      outboundPolicy: target.outboundPolicy,
      sharedOutboundWorkerName: this.#namespacedState.sharedOutboundWorkerName,
    };
  }

  async ensurePlatformResources(
    spec: DeploymentSpec,
    database: DatabaseReference,
    secrets: DeploymentSecrets,
    expectedTarget: ExternalPlatformTargetDescription | undefined,
    currentRecord: FleetRecord,
    fence: ExternalMutationFence,
  ) {
    return this.#withMutationFence(fence, async () => {
      const profile = this.#platformProfile(spec);
      if (
        currentRecord.platformResources &&
        currentRecord.platformResources.stateWorker.plane !== 'dispatch'
      ) {
        throw new Error(
          'ordinary state resources require the dedicated backend-switch lifecycle',
        );
      }
      const describedTarget = this.#describeExternalPlatformTarget(
        spec,
        profile,
      );
      const target = expectedTarget ?? describedTarget;
      if (!target.maintenanceCapabilityPublicKey) {
        throw new Error('persisted platform target has no capability verifier');
      }
      if (currentRecord.platformTarget) {
        assertExternalPlatformTargetCompatibility(
          currentRecord.platformTarget,
          target,
        );
      }
      this.#assertNamespacedStateTransition(
        profile,
        currentRecord.platformTarget,
        target,
      );
      if (
        expectedTarget &&
        JSON.stringify(expectedTarget) !==
          JSON.stringify({
            ...describedTarget,
            d1SchemaVersion: expectedTarget.d1SchemaVersion,
            d1SchemaHistoryDigest: expectedTarget.d1SchemaHistoryDigest,
          })
      ) {
        throw new Error(
          'trusted platform profile does not match the persisted migration target',
        );
      }
      if (target.d1SchemaVersion < spec.schemaVersion) {
        throw new Error(
          'persisted trusted platform target cannot lower the release schema',
        );
      }
      const preserveAppliedState =
        currentRecord.platformTarget !== undefined &&
        currentRecord.platformTarget.d1SchemaVersion > spec.schemaVersion;
      const platformSpec: DeploymentSpec = preserveAppliedState
        ? {
            ...spec,
            schemaVersion: target.d1SchemaVersion,
            durableObjectBindings:
              currentRecord.platformResources?.stateWorker.durableObjectBindings.map(
                ({ name, className }) => ({ name, className }),
              ) ?? spec.durableObjectBindings,
            previousDurableObjectTag:
              currentRecord.platformTarget?.stateDurableObjectTag,
          }
        : {
            ...spec,
            previousDurableObjectTag:
              currentRecord.platformTarget?.stateDurableObjectTag,
          };
      return this.#ensureNamespacedPlatformResources(
        platformSpec,
        database,
        secrets,
        profile,
        target,
      );
    });
  }

  async revokePlatformResourceCredentials(
    spec: DeploymentSpec,
    record: FleetRecord,
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void> {
    await this.#withMutationFence(fence, async () => {
      const resources = record.platformResources;
      const stateName = externalStateScriptName(spec);
      if (resources && resources.stateWorker.scriptName !== stateName) {
        throw new Error(
          'refusing to revoke credentials for an unexpected state Worker',
        );
      }
      if (resources && resources.stateWorker.plane !== 'dispatch') {
        throw new Error(
          'ordinary state credentials require the dedicated backend-switch lifecycle',
        );
      }
      const namespacedLive = await this.#inspectDispatchWorker(stateName);
      if (!namespacedLive) return;
      const persistedState = resources?.stateWorker;
      const persistedTarget = record.platformTarget;
      const persistedVariantMatches =
        persistedState?.plane === 'dispatch' &&
        persistedState.dispatchNamespace ===
          this.#namespacedState.dispatchNamespace &&
        namespacedLive.artifactVersion === persistedState.artifactVersion &&
        namespacedLive.tenantTag === spec.tenantTag &&
        namespacedLive.environment === spec.environment &&
        namespacedLive.databaseIds.length === 1 &&
        namespacedLive.databaseIds[0] === database.id &&
        localBindingKeys(namespacedLive.durableObjectBindings) ===
          localBindingKeys(persistedState.durableObjectBindings) &&
        JSON.stringify(namespacedLive.serviceBindings ?? []) ===
          JSON.stringify([
            {
              name: 'OUTBOUND_PROXY',
              service:
                resources?.sharedOutboundWorkerName ??
                persistedTarget?.sharedOutboundWorkerName,
              entrypoint: 'StateEgress',
            },
          ]) &&
        JSON.stringify(namespacedLive.queueProducerBindings ?? []) ===
          JSON.stringify(
            resources?.auditQueueName
              ? [
                  {
                    name: 'AUDIT_QUEUE',
                    queueName: resources.auditQueueName,
                  },
                ]
              : [],
          ) &&
        JSON.stringify([...namespacedLive.secretNames].sort()) ===
          JSON.stringify(
            [
              'DEPLOYMENT_IDENTITY_SECRET',
              'MAINTENANCE_ADMIN_SECRET',
              'OUTBOUND_PROXY_CREDENTIAL',
            ].sort(),
          ) &&
        namespacedLive.plainTextBindings?.FLEET_RESOURCE_ROLE ===
          'platform-state' &&
        namespacedLive.plainTextBindings.FLEET_RESOURCE_GROUP ===
          externalPlatformResourceGroupId(spec) &&
        namespacedLive.plainTextBindings.DEPLOYMENT_TENANT === spec.tenantTag &&
        namespacedLive.plainTextBindings.FLEET_ENVIRONMENT === spec.environment;
      if (persistedVariantMatches) {
        await this.#client.revokeDispatchSecrets(stateName);
        return;
      }
      const namespacedProfile = this.#platformProfile(spec);
      const namespacedTarget = this.#describeExternalPlatformTarget(
        spec,
        namespacedProfile,
      );
      const inspectionSpec = {
        ...spec,
        previousDurableObjectTag: record.platformTarget?.stateDurableObjectTag,
      };
      const inspected = await this.#inspectNamespacedState(
        inspectionSpec,
        database,
        namespacedProfile,
        namespacedTarget,
      );
      if (
        !inspected ||
        !record.migrationIntent ||
        JSON.stringify(record.migrationIntent.target) !==
          JSON.stringify(namespacedTarget)
      ) {
        throw new Error(
          `refusing to revoke credentials for drifted state Worker '${stateName}'`,
        );
      }
      await this.#client.revokeDispatchSecrets(stateName);
      return;
    });
  }

  async deletePlatformResources(
    spec: DeploymentSpec,
    record: FleetRecord,
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void> {
    await this.#withMutationFence(fence, async () => {
      const resources = record.platformResources;
      const stateName = externalStateScriptName(spec);
      if (
        resources &&
        (resources.stateWorker.plane !== 'dispatch' ||
          resources.stateWorker.scriptName !== stateName ||
          resources.stateWorker.dispatchNamespace !==
            this.#namespacedState.dispatchNamespace)
      ) {
        throw new Error(
          'ordinary or unexpected state resources require the dedicated backend-switch lifecycle',
        );
      }
      const namespaceIds = [
        ...new Set([
          ...(resources?.stateWorker.namespaceIds ?? []),
          ...(await this.#client.listDurableObjectNamespaces(stateName)),
        ]),
      ];
      await this.#client.revokeDispatchSecrets(stateName);
      await this.#client.deleteDispatchWorker(stateName);
      await this.#client.deleteScriptInventory(this.#hostRoutingKvId, {
        scriptName: stateName,
        tenantTag: spec.tenantTag,
        environment: spec.environment,
        databaseId: database.id,
        routeHostname: spec.routeHostname,
      });
      if (await this.#inspectDispatchWorker(stateName)) {
        throw new Error(
          `namespaced state Worker '${stateName}' remains after deletion`,
        );
      }
      for (const namespaceId of namespaceIds) {
        if (await this.#client.hasDurableObjectNamespace(namespaceId)) {
          throw new Error(
            `Durable Object namespace '${namespaceId}' remains after namespaced state deletion`,
          );
        }
      }
      return;
    });
  }

  async deployWorker(
    spec: DeploymentSpec,
    database: DatabaseReference,
    secrets: DeploymentSecrets,
    platformResources: ExternalPlatformResources | undefined,
    fence: ExternalMutationFence,
    expectedArtifactVersion?: string,
    application?: import('./types.js').ApplicationBindingTopology,
  ): Promise<{
    artifactVersion: string;
    created: boolean;
    physicalScriptName?: string;
  }> {
    return this.#withMutationFence(fence, async () => {
      const physicalScriptName = this.releaseScriptName(spec);
      this.#deploymentAuditQueueName(spec);
      const existing = await this.#inspectDispatchWorker(physicalScriptName);
      const targetDigest = deploymentSpecDigest(spec);
      if (
        spec.authoredBy === 'external' &&
        expectedArtifactVersion === undefined
      ) {
        throw new Error(
          'immutable external deployment requires a persisted artifact expectation',
        );
      }
      if (
        spec.authoredBy === 'external' &&
        expectedArtifactVersion !== 'pending' &&
        (!existing || existing.artifactVersion !== expectedArtifactVersion)
      ) {
        throw new Error(
          `immutable release '${physicalScriptName}' does not match persisted artifact version '${expectedArtifactVersion}'`,
        );
      }
      if (spec.authoredBy === 'external' && !platformResources) {
        throw new Error(
          'external candidate requires persisted platform resources',
        );
      }
      const expectedBindings = [
        ...spec.durableObjectBindings.map((binding) => ({
          ...binding,
          ...(platformResources
            ? {
                scriptName: platformResources.stateWorker.scriptName,
                ...(platformResources.stateWorker.dispatchNamespace
                  ? {
                      dispatchNamespace:
                        platformResources.stateWorker.dispatchNamespace,
                    }
                  : {}),
              }
            : {}),
        })),
        ...(spec.authoredBy === 'external' && spec.queueProducer
          ? [
              {
                name: FLEET_AUDIT_PROXY_BINDING,
                className: FLEET_AUDIT_PROXY_CLASS_NAME,
                scriptName: platformResources?.stateWorker.scriptName,
                ...(platformResources?.stateWorker.dispatchNamespace
                  ? {
                      dispatchNamespace:
                        platformResources.stateWorker.dispatchNamespace,
                    }
                  : {}),
              },
            ]
          : []),
      ];
      const expectedServiceBindings =
        spec.authoredBy === 'external'
          ? []
          : spec.egressProxyService
            ? [
                {
                  name: 'EGRESS_PROXY',
                  service: spec.egressProxyService,
                },
              ]
            : [];
      const expectedQueueBindings =
        spec.authoredBy === 'external'
          ? []
          : spec.queueProducer
            ? [
                {
                  name: spec.queueProducer.binding,
                  queueName: spec.queueProducer.queueName,
                },
              ]
            : [];
      const expectedSecretNames =
        spec.authoredBy === 'external'
          ? [
              'DEPLOYMENT_IDENTITY_SECRET',
              ...applicationSecretNames(spec),
            ].sort()
          : [
              'DEPLOYMENT_IDENTITY_SECRET',
              'MAINTENANCE_ADMIN_SECRET',
              ...applicationSecretNames(spec),
            ].sort();
      if (existing) {
        if (
          existing.tenantTag !== spec.tenantTag ||
          existing.environment !== spec.environment ||
          (spec.authoredBy === 'external' &&
            (existing.desiredSpecDigest !== targetDigest ||
              existing.schemaVersion !== spec.schemaVersion)) ||
          existing.databaseIds.length !== 1 ||
          existing.databaseIds[0] !== database.id ||
          bindingKeys(existing.durableObjectBindings) !==
            bindingKeys(expectedBindings) ||
          JSON.stringify(existing.serviceBindings ?? []) !==
            JSON.stringify(expectedServiceBindings) ||
          JSON.stringify(existing.queueProducerBindings ?? []) !==
            JSON.stringify(expectedQueueBindings)
        ) {
          throw new Error(
            spec.authoredBy === 'external'
              ? `immutable release '${physicalScriptName}' already contains a different build or binding topology`
              : `existing release '${physicalScriptName}' does not match its owned binding topology`,
          );
        }
      }
      let deployed: { artifactVersion: string };
      const inventory = {
        scriptName: physicalScriptName,
        tenantTag: spec.tenantTag,
        environment: spec.environment,
        databaseId: database.id,
        routeHostname: spec.routeHostname,
      };
      await this.#client.putScriptInventory(this.#hostRoutingKvId, inventory);
      try {
        if (
          existing !== undefined &&
          spec.authoredBy === 'external' &&
          expectedArtifactVersion !== 'pending'
        ) {
          await this.#client.putDispatchSecrets(physicalScriptName, secrets, {
            includeMaintenanceAdmin: false,
            ...(applicationSecretNames(spec).length > 0
              ? { additionalSecrets: applicationSecretValues(spec, secrets) }
              : {}),
          });
          deployed = { artifactVersion: existing.artifactVersion };
        } else if (existing !== undefined) {
          await this.#client.putDispatchSecrets(physicalScriptName, secrets, {
            includeMaintenanceAdmin: spec.authoredBy !== 'external',
            ...(applicationSecretNames(spec).length > 0
              ? { additionalSecrets: applicationSecretValues(spec, secrets) }
              : {}),
          });
          deployed = await this.#client.uploadDispatchWorker(
            spec,
            database,
            physicalScriptName,
            platformResources,
            application,
          );
        } else {
          deployed = await this.#client.uploadDispatchWorker(
            spec,
            database,
            physicalScriptName,
            platformResources,
            application,
          );
          await this.#client.putDispatchSecrets(physicalScriptName, secrets, {
            includeMaintenanceAdmin: spec.authoredBy !== 'external',
            ...(applicationSecretNames(spec).length > 0
              ? { additionalSecrets: applicationSecretValues(spec, secrets) }
              : {}),
          });
        }
        const attested = await this.#inspectDispatchWorker(physicalScriptName);
        if (
          !attested ||
          attested.artifactVersion !== deployed.artifactVersion ||
          attested.tenantTag !== spec.tenantTag ||
          attested.environment !== spec.environment ||
          attested.desiredSpecDigest !== targetDigest ||
          attested.schemaVersion !== spec.schemaVersion ||
          attested.databaseIds.length !== 1 ||
          attested.databaseIds[0] !== database.id ||
          bindingKeys(attested.durableObjectBindings) !==
            bindingKeys(expectedBindings) ||
          JSON.stringify(attested.serviceBindings ?? []) !==
            JSON.stringify(expectedServiceBindings) ||
          JSON.stringify(attested.queueProducerBindings ?? []) !==
            JSON.stringify(expectedQueueBindings) ||
          JSON.stringify(
            (attested.r2BucketBindings ?? []).map(({ name, bucketName }) => ({
              name,
              bucketName,
            })),
          ) !==
            JSON.stringify(
              (application?.r2Buckets ?? []).map(({ name, bucketName }) => ({
                name,
                bucketName,
              })),
            ) ||
          JSON.stringify([...attested.secretNames].sort()) !==
            JSON.stringify(expectedSecretNames)
        ) {
          throw new Error(
            `release '${physicalScriptName}' did not converge its exact artifact, binding, and secret topology`,
          );
        }
        return {
          ...deployed,
          created: existing === undefined,
          ...(spec.authoredBy === 'external' ? { physicalScriptName } : {}),
        };
      } catch (cause) {
        if (existing !== undefined) {
          throw new WorkerDeploymentError({
            message: `failed to update existing Worker '${physicalScriptName}'`,
            cause,
            createdByAttempt: false,
            resourceState: 'present',
          });
        }
        const cleanupErrors: unknown[] = [];
        try {
          await this.#client.revokeDispatchSecrets(physicalScriptName);
        } catch (cleanupError) {
          cleanupErrors.push(cleanupError);
        }
        try {
          await this.#client.deleteHostRouting(
            this.#hostRoutingKvId,
            spec.routeHostname,
            [
              {
                scriptName: physicalScriptName,
                tenantTag: spec.tenantTag,
                environment: spec.environment,
              },
            ],
          );
          if (
            (await this.#client.getHostRouting(
              this.#hostRoutingKvId,
              spec.routeHostname,
            )) !== undefined
          ) {
            throw new Error(
              `host route '${spec.routeHostname}' remains after cleanup`,
            );
          }
        } catch (cleanupError) {
          cleanupErrors.push(cleanupError);
        }
        let scriptAbsent = false;
        try {
          await this.#client.deleteDispatchWorker(physicalScriptName);
          if (await this.#inspectDispatchWorker(physicalScriptName)) {
            throw new Error(
              `dispatch release '${physicalScriptName}' remains after cleanup`,
            );
          }
          scriptAbsent = true;
        } catch (cleanupError) {
          cleanupErrors.push(cleanupError);
        }
        if (scriptAbsent) {
          try {
            await this.#client.deleteScriptInventory(
              this.#hostRoutingKvId,
              inventory,
            );
            if (
              await this.#client.getScriptInventory(
                this.#hostRoutingKvId,
                physicalScriptName,
              )
            ) {
              throw new Error(
                `script inventory '${physicalScriptName}' remains after cleanup`,
              );
            }
          } catch (cleanupError) {
            cleanupErrors.push(cleanupError);
          }
        }
        if (cleanupErrors.length > 0) {
          throw new WorkerDeploymentError({
            message: `failed to install credentials and clean up '${physicalScriptName}'`,
            cause: new AggregateError([cause, ...cleanupErrors]),
            createdByAttempt: true,
            resourceState: 'unknown',
          });
        }
        throw new WorkerDeploymentError({
          message: `failed to install Worker '${physicalScriptName}'`,
          cause,
          createdByAttempt: true,
          resourceState: 'absent',
        });
      }
    });
  }

  async promoteWorker(
    spec: DeploymentSpec,
    guard: PromotionGuard,
    outboundPolicy: DeploymentEgressPolicy | undefined,
    fence: ExternalMutationFence,
    expectedArtifactVersion?: string,
  ): Promise<void> {
    if (!outboundPolicy) {
      throw new Error('WfP promotion requires persisted outbound policy');
    }
    const policy = canonicalDeploymentEgressPolicy({
      policyId: outboundPolicy.policyId,
      tenantTag: spec.tenantTag,
      environment: spec.environment,
      allowedHosts: outboundPolicy.policyHosts,
    });
    if (
      policy.policyId !== externalPlatformResourceGroupId(spec) ||
      policy.policyDigest !== outboundPolicy.policyDigest ||
      JSON.stringify(policy.policyHosts) !==
        JSON.stringify(outboundPolicy.policyHosts)
    ) {
      throw new Error('persisted outbound policy is not canonical');
    }
    if (!expectedArtifactVersion) {
      throw new Error('WfP promotion requires a persisted artifact version');
    }
    const physicalScriptName = this.releaseScriptName(spec);
    const live = await this.#inspectDispatchWorker(physicalScriptName);
    if (!live || live.artifactVersion !== expectedArtifactVersion) {
      throw new Error(
        `immutable release '${physicalScriptName}' does not match persisted artifact version '${expectedArtifactVersion}'`,
      );
    }
    await this.#withMutationFence(fence, () =>
      this.#client.putHostRouting(
        this.#hostRoutingKvId,
        spec.routeHostname,
        {
          scriptName: this.releaseScriptName(spec),
          tenantTag: spec.tenantTag,
          environment: spec.environment,
          policyId: policy.policyId,
          policyDigest: policy.policyDigest,
          policyHosts: policy.policyHosts,
          stateEgress: {
            resourceGroupId: externalPlatformResourceGroupId(spec),
            stateScriptName: externalStateScriptName(spec),
            credentialDigest: createHash('sha256')
              .update(this.#stateEgressCredential(spec))
              .digest('hex'),
          },
        },
        guard,
      ),
    );
  }

  async ensureMaintenance(
    spec: DeploymentSpec,
    maintenanceAdminSecret: string,
    fence: ExternalMutationFence,
    expectedArtifactVersion?: string,
  ): Promise<MaintenanceHealth> {
    return (
      await this.ensureMaintenanceAttestation(
        spec,
        maintenanceAdminSecret,
        fence,
        expectedArtifactVersion,
      )
    ).health;
  }

  async ensureMaintenanceAttestation(
    spec: DeploymentSpec,
    maintenanceAdminSecret: string,
    fence: ExternalMutationFence,
    expectedArtifactVersion?: string,
  ): Promise<Readonly<{ health: MaintenanceHealth; receipt: string }>> {
    if (this.#maintenanceRequestTimeoutMs >= fence.mutationLeaseTtlMs) {
      throw new Error(
        'maintenance request timeout must be shorter than the mutation lease lifetime',
      );
    }
    const physicalScriptName = this.releaseScriptName(spec);
    if (!expectedArtifactVersion) {
      throw new Error('WfP maintenance requires a persisted artifact version');
    }
    const live = await this.#inspectDispatchWorker(physicalScriptName);
    if (!live || live.artifactVersion !== expectedArtifactVersion) {
      throw new Error(
        `immutable release '${physicalScriptName}' does not match persisted artifact version '${expectedArtifactVersion}'`,
      );
    }
    const capability = await mintAsymmetricMaintenanceCapability({
      privateKey: this.#capabilityPrivateKey(spec),
      operation: 'ensure-maintenance',
      tenantTag: spec.tenantTag,
      environment: spec.environment,
      scriptName: physicalScriptName,
      specDigest: deploymentSpecDigest(spec),
      ttlSeconds:
        Math.ceil(this.#maintenanceRequestTimeoutMs / 1_000) +
        MAINTENANCE_CAPABILITY_SKEW_SECONDS,
    });
    await fence.assertOwned();
    const response = await this.#fetch(
      candidateMaintenanceUrl(spec, physicalScriptName, 'ensure-maintenance'),
      {
        method: 'POST',
        headers: { authorization: `Bearer ${capability.token}` },
        signal: AbortSignal.timeout(this.#maintenanceRequestTimeoutMs),
      },
    );
    const receipt = response.headers.get(MAINTENANCE_RECEIPT_HEADER);
    const result = receipt
      ? await verifyMaintenanceReceipt({
          secret: maintenanceAdminSecret,
          token: receipt,
          capability: capability.claims,
        })
      : undefined;
    if (result === undefined) {
      throw new Error(
        `maintenance response did not attest fleet specification digest '${capability.claims.specDigest}'`,
      );
    }
    return {
      health: await readMaintenanceHealth(
        Response.json({
          ...(result as Record<string, unknown>),
          deploymentSpecDigest: capability.claims.specDigest,
        }),
      ),
      receipt: receipt as string,
    };
  }

  async inspect(
    spec: DeploymentSpec,
    maintenanceAdminSecret: string,
    expectedArtifactVersion?: string,
  ): Promise<LiveDeployment | undefined> {
    const physicalScriptName = this.releaseScriptName(spec);
    const live = await this.#inspectDispatchWorker(physicalScriptName);
    if (!live) return undefined;
    if (
      expectedArtifactVersion &&
      live.artifactVersion !== expectedArtifactVersion
    ) {
      throw new Error(
        `immutable release '${physicalScriptName}' does not match persisted artifact version '${expectedArtifactVersion}'`,
      );
    }
    if (
      live.tenantTag !== spec.tenantTag ||
      live.environment !== spec.environment
    ) {
      throw new Error(
        `script '${physicalScriptName}' has a different tenant mapping`,
      );
    }
    if (live.databaseIds.length !== 1) {
      throw new Error(
        `script '${physicalScriptName}' must have exactly one D1 binding`,
      );
    }
    const databaseId = live.databaseIds[0];
    if (!databaseId) throw new Error('D1 binding has no database id');
    const capability = await mintAsymmetricMaintenanceCapability({
      privateKey: this.#capabilityPrivateKey(spec),
      operation: 'maintenance-status',
      tenantTag: spec.tenantTag,
      environment: spec.environment,
      scriptName: physicalScriptName,
      specDigest: deploymentSpecDigest(spec),
      ttlSeconds:
        Math.ceil(this.#maintenanceRequestTimeoutMs / 1_000) +
        MAINTENANCE_CAPABILITY_SKEW_SECONDS,
    });
    const response = await this.#fetch(
      candidateMaintenanceUrl(spec, physicalScriptName, 'maintenance-status'),
      {
        headers: { authorization: `Bearer ${capability.token}` },
        signal: AbortSignal.timeout(this.#maintenanceRequestTimeoutMs),
      },
    );
    const receipt = response.headers.get(MAINTENANCE_RECEIPT_HEADER);
    const result = receipt
      ? await verifyMaintenanceReceipt({
          secret: maintenanceAdminSecret,
          token: receipt,
          capability: capability.claims,
        })
      : undefined;
    if (result === undefined) {
      throw new Error(
        `maintenance response does not match dispatch Worker '${physicalScriptName}'`,
      );
    }
    const maintenance = await readMaintenanceHealth(
      Response.json({
        ...(result as Record<string, unknown>),
        deploymentSpecDigest: capability.claims.specDigest,
      }),
    );
    if (maintenance.deploymentSpecDigest !== live.desiredSpecDigest) {
      throw new Error(
        `maintenance response does not match dispatch Worker '${physicalScriptName}'`,
      );
    }
    return {
      tenantTag: live.tenantTag,
      environment: live.environment,
      scriptName: physicalScriptName,
      databaseId,
      durableObjectBindings: live.durableObjectBindings,
      serviceBindings: live.serviceBindings ?? [],
      queueProducerBindings: live.queueProducerBindings ?? [],
      plainTextBindings: live.plainTextBindings,
      r2BucketBindings: live.r2BucketBindings ?? [],
      secretNames: live.secretNames,
      providerBindingIdentities: live.providerBindingIdentities,
      artifactVersion: live.artifactVersion,
      desiredSpecDigest: live.desiredSpecDigest,
      schemaVersion: live.schemaVersion,
      maintenance,
    };
  }

  async revokeCredentials(
    spec: DeploymentSpec,
    retainedReleases: readonly ExternalReleaseSnapshot[] | undefined,
    activeRelease: ExternalReleaseSnapshot | undefined,
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void> {
    await this.#withMutationFence(fence, async () => {
      const releases = this.#releaseTargets(
        spec,
        retainedReleases ?? [],
        activeRelease,
      );
      for (const release of releases) {
        await this.#assertReleaseOwner(spec, release, database);
      }
      for (const release of releases) {
        await this.#client.revokeDispatchSecrets(release.physicalScriptName);
      }
    });
  }

  async removeTraffic(
    spec: DeploymentSpec,
    retainedReleases: readonly ExternalReleaseSnapshot[] | undefined,
    activeRelease: ExternalReleaseSnapshot | undefined,
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void> {
    await this.#withMutationFence(fence, async () => {
      const releases = this.#releaseTargets(
        spec,
        retainedReleases ?? [],
        activeRelease,
      );
      for (const release of releases) {
        await this.#assertReleaseOwner(spec, release, database);
      }
      await this.#client.deleteHostRouting(
        this.#hostRoutingKvId,
        spec.routeHostname,
        releases.map((release) => ({
          scriptName: release.physicalScriptName,
          tenantTag: spec.tenantTag,
          environment: spec.environment,
        })),
      );
    });
  }

  async assertTrafficRemoved(spec: DeploymentSpec): Promise<void> {
    if (
      (await this.#client.getHostRouting(
        this.#hostRoutingKvId,
        spec.routeHostname,
      )) !== undefined
    ) {
      throw new Error(
        `host route '${spec.routeHostname}' remains after traffic removal`,
      );
    }
  }

  deleteWorker(
    spec: DeploymentSpec,
    retainedReleases: readonly ExternalReleaseSnapshot[] | undefined,
    database: DatabaseReference,
    activeRelease: ExternalReleaseSnapshot | undefined,
    fence: ExternalMutationFence,
  ): Promise<void> {
    return this.#withMutationFence(fence, () =>
      this.#deleteWorker(spec, retainedReleases ?? [], database, activeRelease),
    );
  }

  #releaseTargets(
    spec: DeploymentSpec,
    retainedReleases: readonly ExternalReleaseSnapshot[],
    activeRelease?: ExternalReleaseSnapshot,
  ): readonly ExternalReleaseSnapshot[] {
    const active: ExternalReleaseSnapshot = {
      physicalScriptName: this.releaseScriptName(spec),
      specDigest: deploymentSpecDigest(spec),
      artifactVersion: '',
      releaseSchemaVersion: spec.schemaVersion,
    };
    const routed = activeRelease ?? active;
    return spec.authoredBy === 'external'
      ? [
          routed,
          ...[active, ...retainedReleases].filter(
            (release, index, releases) =>
              release.physicalScriptName !== routed.physicalScriptName &&
              releases.findIndex(
                (candidate) =>
                  candidate.physicalScriptName === release.physicalScriptName,
              ) === index,
          ),
        ]
      : [active];
  }

  async #assertReleaseOwner(
    spec: DeploymentSpec,
    release: ExternalReleaseSnapshot,
    database: DatabaseReference,
  ): Promise<void> {
    const live = await this.#inspectDispatchWorker(release.physicalScriptName);
    if (!live) return;
    if (
      live.tenantTag !== spec.tenantTag ||
      live.environment !== spec.environment ||
      live.desiredSpecDigest !== release.specDigest ||
      live.schemaVersion !== release.releaseSchemaVersion ||
      live.databaseIds.length !== 1 ||
      live.databaseIds[0] !== database.id ||
      (release.artifactVersion !== '' &&
        release.artifactVersion !== 'pending' &&
        live.artifactVersion !== release.artifactVersion) ||
      (spec.authoredBy === 'external' && live.durableObjectTag !== undefined)
    ) {
      throw new Error(
        `refusing to modify release '${release.physicalScriptName}' owned by another build or deployment`,
      );
    }
  }

  async #deleteWorker(
    spec: DeploymentSpec,
    retainedReleases: readonly ExternalReleaseSnapshot[],
    database: DatabaseReference,
    activeRelease?: ExternalReleaseSnapshot,
  ): Promise<void> {
    const releases = this.#releaseTargets(
      spec,
      retainedReleases,
      activeRelease,
    );
    for (const release of releases) {
      await this.#assertReleaseOwner(spec, release, database);
    }
    await this.assertTrafficRemoved(spec);
    const errors: unknown[] = [];
    for (const release of releases) {
      try {
        await this.#client.deleteDispatchWorker(release.physicalScriptName);
        if (await this.#inspectDispatchWorker(release.physicalScriptName)) {
          throw new Error(
            `dispatch release '${release.physicalScriptName}' remains after deletion`,
          );
        }
        await this.#client.deleteScriptInventory(this.#hostRoutingKvId, {
          scriptName: release.physicalScriptName,
          tenantTag: spec.tenantTag,
          environment: spec.environment,
          databaseId: database.id,
          routeHostname: spec.routeHostname,
        });
        if (
          await this.#client.getScriptInventory(
            this.#hostRoutingKvId,
            release.physicalScriptName,
          )
        ) {
          throw new Error(
            `script inventory '${release.physicalScriptName}' remains after deletion`,
          );
        }
      } catch (error) {
        errors.push(error);
      }
    }
    if (errors.length > 0) {
      throw new AggregateError(
        errors,
        `failed to delete ${errors.length} Worker artifact(s) for '${spec.scriptName}'`,
      );
    }
  }

  async deleteRetainedRelease(
    spec: DeploymentSpec,
    release: ExternalReleaseSnapshot,
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void> {
    await this.#withMutationFence(fence, async () => {
      await this.#assertReleaseOwner(spec, release, database);
      await this.#client.revokeDispatchSecrets(release.physicalScriptName);
      await this.#client.deleteDispatchWorker(release.physicalScriptName);
      if (await this.#inspectDispatchWorker(release.physicalScriptName)) {
        throw new Error(
          `retained release '${release.physicalScriptName}' remains after deletion`,
        );
      }
      await this.#client.deleteScriptInventory(this.#hostRoutingKvId, {
        scriptName: release.physicalScriptName,
        tenantTag: spec.tenantTag,
        environment: spec.environment,
        databaseId: database.id,
        routeHostname: spec.routeHostname,
      });
      if (
        await this.#client.getScriptInventory(
          this.#hostRoutingKvId,
          release.physicalScriptName,
        )
      ) {
        throw new Error(
          `retained release inventory '${release.physicalScriptName}' remains after deletion`,
        );
      }
    });
  }

  async assertDatabaseDetached(
    spec: DeploymentSpec,
    record: FleetRecord,
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void> {
    await fence.assertOwned();
    if (
      record.tenantTag !== spec.tenantTag ||
      record.environment !== spec.environment ||
      record.routeHostname !== spec.routeHostname ||
      record.databaseId !== database.id ||
      record.databaseName !== database.name
    ) {
      throw new Error(
        'refusing to attest database detachment for a different deployment',
      );
    }
    if (
      (await this.#client.getHostRouting(
        this.#hostRoutingKvId,
        record.routeHostname,
      )) !== undefined
    ) {
      throw new Error(
        `host route '${record.routeHostname}' remains before D1 deletion`,
      );
    }
    const databaseAttachments =
      await this.#client.listWorkerDatabaseAttachments(database.id);
    if (databaseAttachments.length > 0) {
      throw new Error(
        `database '${database.id}' remains bound to Worker scripts before D1 deletion: ${databaseAttachments
          .map((attachment) => `${attachment.plane}:${attachment.scriptName}`)
          .join(', ')}`,
      );
    }
    const fallbackRelease: ExternalReleaseSnapshot = {
      physicalScriptName: this.releaseScriptName(spec),
      specDigest: deploymentSpecDigest(spec),
      artifactVersion: record.artifactVersion,
      releaseSchemaVersion: spec.schemaVersion,
    };
    const releases = [
      record.activeRelease,
      record.pendingRelease,
      record.migrationPriorRelease,
      record.rollbackRelease,
      record.retiringRelease,
      fallbackRelease,
    ].filter(
      (release, index, candidates): release is ExternalReleaseSnapshot =>
        release !== undefined &&
        candidates.findIndex(
          (candidate) =>
            candidate?.physicalScriptName === release.physicalScriptName,
        ) === index,
    );
    for (const release of releases) {
      if (await this.#inspectDispatchWorker(release.physicalScriptName)) {
        throw new Error(
          `dispatch release '${release.physicalScriptName}' remains before D1 deletion`,
        );
      }
      if (
        await this.#client.getScriptInventory(
          this.#hostRoutingKvId,
          release.physicalScriptName,
        )
      ) {
        throw new Error(
          `script inventory '${release.physicalScriptName}' remains before D1 deletion`,
        );
      }
    }
    const stateName = externalStateScriptName(spec);
    const proxyName = externalEgressProxyScriptName(spec);
    for (const scriptName of [stateName, proxyName]) {
      if (await this.#inspectControlWorker(scriptName)) {
        throw new Error(
          `trusted platform Worker '${scriptName}' remains before D1 deletion`,
        );
      }
    }
    const authoritativeNamespaceIds =
      await this.#client.listDurableObjectNamespaces(stateName);
    const namespaceIds = [
      ...new Set([
        ...record.durableObjectBindings.map((binding) => binding.namespaceId),
        ...(record.platformResources?.stateWorker.namespaceIds ?? []),
        ...authoritativeNamespaceIds,
      ]),
    ].sort();
    for (const namespaceId of namespaceIds) {
      if (await this.#client.hasDurableObjectNamespace(namespaceId)) {
        throw new Error(
          `Durable Object namespace '${namespaceId}' remains before D1 deletion`,
        );
      }
    }
    await fence.assertOwned();
  }

  exportDatabase(
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<DatabaseExport> {
    return this.#withMutationFence(fence, () =>
      this.#client.exportDatabase(database.id),
    );
  }

  deleteDatabase(
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void> {
    return this.#withMutationFence(fence, () =>
      this.#client.deleteDatabase(database.id),
    );
  }
}
