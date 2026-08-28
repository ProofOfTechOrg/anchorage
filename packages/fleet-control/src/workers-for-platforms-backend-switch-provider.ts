// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { posix } from 'node:path';
import { AUDIT_PROXY_INSTANCE_NAME } from '@proofoftech/flowsafe/audit-export';
import {
  applicationBindingTopology,
  applicationSecretValues,
  DEPLOYMENT_PLATFORM_VARIABLE_NAMES,
  LEGACY_BRIDGE_PLATFORM_VARIABLE_NAMES,
  liveApplicationTopologyMatches,
} from './application-bindings.js';
import type {
  BackendSwitchCandidateSnapshot,
  BackendSwitchMutationFence,
  BackendSwitchProvider,
  BridgeMutationPlan,
  BridgeSnapshot,
  PlainBackendSnapshot,
} from './backend-switch.js';
import { finalizedBridgeForRecord } from './backend-switch.js';
import { workerMigrations } from './cloudflare-ordinary-worker-operations.js';
import type { HostRoutingTarget } from './host-routing.js';
import { parseHostRoutingTarget } from './host-routing.js';
import { d1MigrationHistoryDigest } from './migration-ledger.js';
import {
  canonicalDeploymentEgressPolicy,
  canonicalDurableObjectMigrationHistory,
  durableObjectMigrationHistoryDigest,
  externalPlatformResourceGroupId,
  externalReleaseTopology,
  externalStateDeploymentSpec,
  FLEET_AUDIT_PROXY_CLASS_NAME,
  FLEET_AUDIT_PROXY_STATE_BINDING,
  trustedArtifactDigest,
  validateExternalPlatformProfile,
} from './platform-resources.js';
import { assertProviderBindingIdentitiesMatchInspection } from './provider-binding-inventory.js';
import { deploymentSpecDigest } from './spec-digest.js';
import type {
  ApplicationBindingTopology,
  DeploymentSecrets,
  DeploymentSpec,
  DurableObjectBindingInventory,
  DurableObjectMigration,
  ExternalPlatformProfile,
  ExternalPlatformResources,
  ExternalPlatformTargetDescription,
  ExternalReleaseSnapshot,
  FleetRecord,
  TrustedWorkerArtifact,
  WorkerZoneRoute,
} from './types.js';
import {
  deriveStateEgressCredential,
  type WorkersForPlatformsApi,
  type WorkersForPlatformsBackend,
} from './workers-for-platforms-backend.js';

export const LEGACY_APPLICATION_MODULE_PLACEHOLDER =
  '__ANCHORAGE_LEGACY_APPLICATION_MODULE__';

function moduleBytes(content: string | Uint8Array): Uint8Array {
  return typeof content === 'string'
    ? new TextEncoder().encode(content)
    : content;
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  return (
    left.length === right.length &&
    left.every((byte, index) => byte === right[index])
  );
}

/**
 * Inserts the exact prior application module graph into a trusted bridge
 * template. The template owns control routes and delegates ordinary fetches to
 * the placeholder import; the prior graph stays byte-identical.
 */
export function composeLegacyBridgeArtifact(
  priorSpec: DeploymentSpec,
  template: TrustedWorkerArtifact,
): TrustedWorkerArtifact {
  const validModuleName = (name: string): boolean =>
    name.length > 0 &&
    !name.startsWith('/') &&
    posix.normalize(name) === name &&
    !name.split('/').includes('..');
  if (
    !validModuleName(priorSpec.mainModule) ||
    !validModuleName(template.mainModule) ||
    priorSpec.modules.some((module) => !validModuleName(module.name)) ||
    template.modules.some((module) => !validModuleName(module.name)) ||
    !template.modules.some((module) => module.name === template.mainModule)
  ) {
    throw new Error(
      'legacy bridge contains an invalid module path or main module',
    );
  }
  const templateNames = new Set(template.modules.map((module) => module.name));
  for (const module of priorSpec.modules) {
    if (templateNames.has(module.name)) {
      throw new Error(
        `legacy bridge template collides with application module '${module.name}'`,
      );
    }
  }
  let occurrences = 0;
  let placeholderModule: string | undefined;
  for (const module of template.modules) {
    if (typeof module.content !== 'string') continue;
    const count =
      module.content.split(LEGACY_APPLICATION_MODULE_PLACEHOLDER).length - 1;
    if (count > 0) placeholderModule = module.name;
    occurrences += count;
  }
  if (occurrences !== 1 || placeholderModule !== template.mainModule) {
    throw new Error(
      'legacy bridge template main module must contain exactly one application module placeholder',
    );
  }
  const relativeApplicationModule = posix.relative(
    posix.dirname(template.mainModule),
    priorSpec.mainModule,
  );
  const applicationSpecifier = relativeApplicationModule.startsWith('.')
    ? relativeApplicationModule
    : `./${relativeApplicationModule}`;
  const bridgeModules = template.modules.map((module) => {
    if (typeof module.content !== 'string') return module;
    const content = module.content.replaceAll(
      LEGACY_APPLICATION_MODULE_PLACEHOLDER,
      applicationSpecifier,
    );
    return { ...module, content };
  });
  const artifact: TrustedWorkerArtifact = {
    ...template,
    modules: [...priorSpec.modules, ...bridgeModules],
  };
  for (const applicationModule of priorSpec.modules) {
    const included = artifact.modules.find(
      (module) => module.name === applicationModule.name,
    );
    if (
      !included ||
      included.contentType !== applicationModule.contentType ||
      !equalBytes(
        moduleBytes(included.content),
        moduleBytes(applicationModule.content),
      )
    ) {
      throw new Error(
        'legacy bridge changed the prior application module graph',
      );
    }
  }
  return artifact;
}

function migrationHistory(
  priorSpec: DeploymentSpec,
  profile: ExternalPlatformProfile,
  requiresAuditProxy: boolean,
): readonly DurableObjectMigration[] {
  const prior = canonicalDurableObjectMigrationHistory(
    priorSpec.durableObjectMigrations,
  );
  const platform = canonicalDurableObjectMigrationHistory(
    profile.stateDurableObjectMigrations,
  );
  const prefix =
    JSON.stringify(platform.slice(0, prior.length)) === JSON.stringify(prior);
  const append = prefix ? platform.slice(prior.length) : platform;
  if (!prefix) {
    const priorTags = new Set(prior.map((migration) => migration.tag));
    if (append.some((migration) => priorTags.has(migration.tag))) {
      throw new Error(
        'bridge Durable Object migration tags collide with prior history',
      );
    }
  }
  if (
    append.some(
      (migration) =>
        (migration.deletedClasses?.length ?? 0) > 0 ||
        (migration.renamedClasses?.length ?? 0) > 0,
    )
  ) {
    throw new Error('bridge may only append Durable Object classes');
  }
  if (
    requiresAuditProxy &&
    ![...prior, ...append].some(
      (migration) =>
        migration.newClasses?.includes(FLEET_AUDIT_PROXY_CLASS_NAME) ||
        migration.newSqliteClasses?.includes(FLEET_AUDIT_PROXY_CLASS_NAME),
    )
  ) {
    throw new Error(
      `bridge migration history must append '${FLEET_AUDIT_PROXY_CLASS_NAME}'`,
    );
  }
  return [...prior, ...append];
}

function bindingKey(binding: {
  readonly name: string;
  readonly className: string;
  readonly scriptName?: string;
  readonly dispatchNamespace?: string;
}): string {
  return [
    binding.name,
    binding.className,
    binding.scriptName ?? '',
    binding.dispatchNamespace ?? '',
  ].join(':');
}

function sortedBindingKeys(
  bindings: readonly {
    readonly name: string;
    readonly className: string;
    readonly scriptName?: string;
    readonly dispatchNamespace?: string;
  }[],
): readonly string[] {
  return bindings.map(bindingKey).sort();
}

function expectedSecretNames(
  includeStateChannels: boolean,
  application: ApplicationBindingTopology | undefined = undefined,
): readonly string[] {
  return [
    'DEPLOYMENT_IDENTITY_SECRET',
    'MAINTENANCE_ADMIN_SECRET',
    ...(includeStateChannels ? ['OUTBOUND_PROXY_CREDENTIAL'] : []),
    ...(application?.secrets.map(({ name }) => name) ?? []),
  ].sort();
}

function sameJson(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

type ProviderControlWorkerInspection = NonNullable<
  Awaited<ReturnType<WorkersForPlatformsApi['inspectControlWorker']>>
>;

interface BridgeTopologyExpectation {
  readonly databaseIds: readonly string[];
  readonly durableObjectBindings: readonly Readonly<{
    name: string;
    className: string;
    namespaceId?: string;
    scriptName?: string;
    dispatchNamespace?: string;
  }>[];
  readonly namespaceIds?: readonly string[];
  readonly serviceBindings: ProviderControlWorkerInspection['serviceBindings'];
  readonly queueProducerBindings: NonNullable<
    ProviderControlWorkerInspection['queueProducerBindings']
  >;
  readonly secretNames: readonly string[];
  readonly fixedVariables: Readonly<Record<string, string>>;
  readonly application?: ApplicationBindingTopology;
  readonly exactNamespaceIdentity: boolean;
}

function sortedJson(values: readonly unknown[]): readonly unknown[] {
  return [...values].sort((left, right) =>
    JSON.stringify(left).localeCompare(JSON.stringify(right)),
  );
}

function normalizedDurableObjectBindings(
  bindings: BridgeTopologyExpectation['durableObjectBindings'],
  exactNamespaceIdentity: boolean,
): readonly unknown[] {
  return sortedJson(
    bindings.map((binding) => ({
      name: binding.name,
      className: binding.className,
      ...(exactNamespaceIdentity
        ? { namespaceId: binding.namespaceId ?? '' }
        : {}),
      ...(binding.scriptName ? { scriptName: binding.scriptName } : {}),
      ...(binding.dispatchNamespace
        ? { dispatchNamespace: binding.dispatchNamespace }
        : {}),
    })),
  );
}

function legacyBridgeFixedVariables(
  plainTextBindings: Readonly<Record<string, string>>,
): Readonly<Record<string, string>> {
  return Object.fromEntries(
    Object.entries(plainTextBindings)
      .filter(([name]) => LEGACY_BRIDGE_PLATFORM_VARIABLE_NAMES.includes(name))
      .sort(([left], [right]) => left.localeCompare(right)),
  );
}

function bridgeTopologyMatches(
  live: ProviderControlWorkerInspection,
  liveNamespaceIds: readonly string[],
  expected: BridgeTopologyExpectation,
): boolean {
  const uniqueLiveNamespaceIds = new Set(liveNamespaceIds);
  if (
    uniqueLiveNamespaceIds.size !== liveNamespaceIds.length ||
    live.durableObjectBindings.some(
      (binding) =>
        binding.namespaceId.length > 0 &&
        !uniqueLiveNamespaceIds.has(binding.namespaceId),
    )
  ) {
    return false;
  }
  return (
    sameJson([...live.databaseIds].sort(), [...expected.databaseIds].sort()) &&
    sameJson(
      normalizedDurableObjectBindings(
        live.durableObjectBindings,
        expected.exactNamespaceIdentity,
      ),
      normalizedDurableObjectBindings(
        expected.durableObjectBindings,
        expected.exactNamespaceIdentity,
      ),
    ) &&
    (expected.namespaceIds === undefined ||
      sameJson(
        [...liveNamespaceIds].sort(),
        [...expected.namespaceIds].sort(),
      )) &&
    sameJson(
      sortedJson(live.serviceBindings),
      sortedJson(expected.serviceBindings),
    ) &&
    sameJson(
      sortedJson(live.queueProducerBindings ?? []),
      sortedJson(expected.queueProducerBindings),
    ) &&
    live.kvNamespaceBindings.length === 0 &&
    sameJson([...live.secretNames].sort(), [...expected.secretNames].sort()) &&
    sameJson(
      legacyBridgeFixedVariables(live.plainTextBindings),
      expected.fixedVariables,
    ) &&
    liveApplicationTopologyMatches(
      expected.application,
      live,
      LEGACY_BRIDGE_PLATFORM_VARIABLE_NAMES,
    )
  );
}

function bridgeTopologyExpectationFromBindings(input: {
  readonly bindings: readonly Readonly<Record<string, unknown>>[];
  readonly secretNames: readonly string[];
  readonly application?: ApplicationBindingTopology;
  readonly durableObjectBindings?: readonly DurableObjectBindingInventory[];
  readonly namespaceIds?: readonly string[];
}): BridgeTopologyExpectation {
  const expectedDurableObjectBindings = input.bindings.flatMap((binding) =>
    binding.type === 'durable_object_namespace'
      ? [
          {
            name: String(binding.name),
            className: String(binding.class_name),
            ...(binding.script_name
              ? { scriptName: String(binding.script_name) }
              : {}),
            ...(binding.dispatch_namespace
              ? { dispatchNamespace: String(binding.dispatch_namespace) }
              : {}),
          },
        ]
      : [],
  );
  const durableObjectBindings =
    input.durableObjectBindings ?? expectedDurableObjectBindings;
  return {
    databaseIds: input.bindings.flatMap((binding) =>
      binding.type === 'd1' ? [String(binding.database_id)] : [],
    ),
    durableObjectBindings,
    ...(input.namespaceIds ? { namespaceIds: input.namespaceIds } : {}),
    serviceBindings: input.bindings.flatMap((binding) =>
      binding.type === 'service'
        ? [
            {
              name: String(binding.name),
              service: String(binding.service),
              ...(binding.entrypoint
                ? { entrypoint: String(binding.entrypoint) }
                : {}),
            },
          ]
        : [],
    ),
    queueProducerBindings: input.bindings.flatMap((binding) =>
      binding.type === 'queue'
        ? [
            {
              name: String(binding.name),
              queueName: String(binding.queue_name),
            },
          ]
        : [],
    ),
    secretNames: input.secretNames,
    fixedVariables: legacyBridgeFixedVariables(
      Object.fromEntries(
        input.bindings.flatMap((binding) =>
          binding.type === 'plain_text'
            ? [[String(binding.name), String(binding.text)] as const]
            : [],
        ),
      ),
    ),
    ...(input.application ? { application: input.application } : {}),
    exactNamespaceIdentity: input.durableObjectBindings !== undefined,
  };
}

function stripPlatformOwnership(record: FleetRecord): FleetRecord {
  const {
    activeRelease: _activeRelease,
    pendingRelease: _pendingRelease,
    migrationPriorRelease: _migrationPriorRelease,
    rollbackRelease: _rollbackRelease,
    retiringRelease: _retiringRelease,
    outboundPolicy: _outboundPolicy,
    platformResources: _platformResources,
    platformTarget: _platformTarget,
    migrationIntent: _migrationIntent,
    ...plain
  } = record;
  return plain;
}

export interface WorkersForPlatformsBackendSwitchProviderOptions {
  readonly client: BackendSwitchApi;
  readonly backend: WorkersForPlatformsBackend;
  readonly hostRoutingKvId: string;
  readonly sharedOutboundWorkerName: string;
  readonly stateEgressRootSecret: string;
  readonly platformProfileFor: (
    spec: DeploymentSpec,
  ) => ExternalPlatformProfile;
  readonly assertServing: (
    hostname: string,
    expectedScriptName: string,
  ) => Promise<void>;
  readonly drainCandidate: (
    candidate: ExternalReleaseSnapshot,
  ) => Promise<void>;
}

export interface BackendSwitchApi extends WorkersForPlatformsApi {
  listCustomDomains(): Promise<
    readonly Readonly<{ id: string; hostname: string; service: string }>[]
  >;
  attachCustomDomain(
    target: { readonly hostname: string; readonly service: string },
    fence: BackendSwitchMutationFence,
  ): Promise<void>;
  detachCustomDomain(
    domainId: string,
    fence: BackendSwitchMutationFence,
  ): Promise<void>;
  disableOrdinaryWorkerPublicAccess(
    scriptName: string,
    fence: BackendSwitchMutationFence,
  ): Promise<void>;
  inspectOrdinaryWorkerFootprint(scriptName: string): Promise<{
    readonly scriptPresent: boolean;
    readonly workersDevEnabled?: boolean;
    readonly previewUrlsEnabled?: boolean;
    readonly customDomains: readonly Readonly<{
      id: string;
      hostname: string;
      service: string;
    }>[];
    readonly zoneRoutes: readonly WorkerZoneRoute[];
  }>;
}

export interface SwitchBridgeRemovalAuthority {
  readonly prior: PlainBackendSnapshot;
  readonly priorSpec: DeploymentSpec;
  readonly bridge?: BridgeSnapshot;
  readonly plan?: BridgeMutationPlan;
  readonly targetSpec: DeploymentSpec;
  readonly allowedArtifactVersions: readonly string[];
}

export class WorkersForPlatformsBackendSwitchProvider
  implements BackendSwitchProvider
{
  readonly #client: BackendSwitchApi;
  readonly #backend: WorkersForPlatformsBackend;
  readonly #hostRoutingKvId: string;
  readonly #sharedOutboundWorkerName: string;
  readonly #stateEgressRootSecret: string;
  readonly #platformProfileFor: (
    spec: DeploymentSpec,
  ) => ExternalPlatformProfile;
  readonly #assertServing: WorkersForPlatformsBackendSwitchProviderOptions['assertServing'];
  readonly #drainCandidate: WorkersForPlatformsBackendSwitchProviderOptions['drainCandidate'];

  constructor(options: WorkersForPlatformsBackendSwitchProviderOptions) {
    if (!options.hostRoutingKvId || !options.sharedOutboundWorkerName) {
      throw new Error(
        'backend switch provider requires shared platform resources',
      );
    }
    if (options.stateEgressRootSecret.length < 32) {
      throw new Error(
        'backend switch provider requires a 32-byte egress root secret',
      );
    }
    this.#client = options.client;
    this.#backend = options.backend;
    this.#hostRoutingKvId = options.hostRoutingKvId;
    this.#sharedOutboundWorkerName = options.sharedOutboundWorkerName;
    this.#stateEgressRootSecret = options.stateEgressRootSecret;
    this.#platformProfileFor = options.platformProfileFor;
    this.#assertServing = options.assertServing;
    this.#drainCandidate = options.drainCandidate;
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

  #profile(spec: DeploymentSpec): ExternalPlatformProfile {
    const profile = this.#platformProfileFor(spec);
    validateExternalPlatformProfile(spec, profile);
    if (!profile.legacyBridgeWorker) {
      throw new Error('backend switch profile has no legacy bridge artifact');
    }
    return profile;
  }

  #credential(spec: DeploymentSpec, scriptName: string): string {
    return deriveStateEgressCredential(
      this.#stateEgressRootSecret,
      spec,
      scriptName,
    );
  }

  #credentialDigest(spec: DeploymentSpec, scriptName: string): string {
    return createHash('sha256')
      .update(this.#credential(spec, scriptName))
      .digest('hex');
  }

  #finalizedMigrations(
    targetSpec: DeploymentSpec,
    currentRecord: FleetRecord,
    profile: ExternalPlatformProfile,
  ): readonly DurableObjectMigration[] {
    const prior = canonicalDurableObjectMigrationHistory(
      currentRecord.durableObjectMigrationHistory ?? [],
    );
    if (
      durableObjectMigrationHistoryDigest(prior) !==
        currentRecord.durableObjectMigrationHistoryDigest ||
      prior.at(-1)?.tag !== currentRecord.durableObjectTag
    ) {
      throw new Error(
        'finalized ordinary state has inconsistent persisted migration history',
      );
    }
    const platform = canonicalDurableObjectMigrationHistory(
      profile.stateDurableObjectMigrations,
    );
    const appended: DurableObjectMigration[] = [];
    let lastMatchedIndex = -1;
    let sawNewTag = false;
    for (const migration of platform) {
      const matchedIndex = prior.findIndex(
        (persisted) => persisted.tag === migration.tag,
      );
      if (matchedIndex >= 0) {
        if (
          sawNewTag ||
          matchedIndex <= lastMatchedIndex ||
          JSON.stringify(prior[matchedIndex]) !== JSON.stringify(migration)
        ) {
          throw new Error(
            'finalized ordinary state profile rewrites persisted migration history',
          );
        }
        lastMatchedIndex = matchedIndex;
        continue;
      }
      sawNewTag = true;
      if (
        (migration.deletedClasses?.length ?? 0) > 0 ||
        (migration.renamedClasses?.length ?? 0) > 0
      ) {
        throw new Error(
          'finalized ordinary state profile may only append Durable Object classes',
        );
      }
      appended.push(migration);
    }
    const combined = [...prior, ...appended];
    if (
      targetSpec.queueProducer &&
      !combined.some(
        (migration) =>
          migration.newClasses?.includes(FLEET_AUDIT_PROXY_CLASS_NAME) ||
          migration.newSqliteClasses?.includes(FLEET_AUDIT_PROXY_CLASS_NAME),
      )
    ) {
      throw new Error(
        `bridge migration history must append '${FLEET_AUDIT_PROXY_CLASS_NAME}'`,
      );
    }
    return combined;
  }

  describeFinalizedBridgeTarget(
    targetSpec: DeploymentSpec,
    currentRecord: FleetRecord,
  ): ExternalPlatformTargetDescription {
    const bridge = finalizedBridgeForRecord(currentRecord);
    const profile = this.#profile(targetSpec);
    const migrations = this.#finalizedMigrations(
      targetSpec,
      currentRecord,
      profile,
    );
    const stateDurableObjectTag = migrations.at(-1)?.tag;
    return {
      ...(targetSpec.queueProducer
        ? { auditQueueName: targetSpec.queueProducer.queueName }
        : {}),
      maintenanceCapabilityPublicKey: profile.maintenanceCapabilityPublicKey,
      stateArtifactDigest: trustedArtifactDigest(profile.stateWorker),
      stateDurableObjectHistoryDigest:
        durableObjectMigrationHistoryDigest(migrations),
      ...(stateDurableObjectTag ? { stateDurableObjectTag } : {}),
      stateEgressCredentialDigest: this.#credentialDigest(
        targetSpec,
        bridge.scriptName,
      ),
      sharedOutboundWorkerName: this.#sharedOutboundWorkerName,
      d1SchemaVersion: targetSpec.schemaVersion,
      d1SchemaHistoryDigest: d1MigrationHistoryDigest(targetSpec.migrations),
      outboundPolicy: canonicalDeploymentEgressPolicy({
        policyId: externalPlatformResourceGroupId(targetSpec),
        tenantTag: targetSpec.tenantTag,
        environment: targetSpec.environment,
        allowedHosts: profile.organizationEgressHosts,
      }),
    };
  }

  describeFinalizedState(input: {
    readonly targetSpec: DeploymentSpec;
    readonly currentRecord: FleetRecord;
    readonly target: ExternalPlatformTargetDescription;
  }): BridgeMutationPlan {
    const bridge = finalizedBridgeForRecord(input.currentRecord);
    const expectedTarget = this.describeFinalizedBridgeTarget(
      input.targetSpec,
      input.currentRecord,
    );
    const appliedTarget = input.currentRecord.platformTarget;
    const d1MatchesExpected =
      input.target.d1SchemaVersion === expectedTarget.d1SchemaVersion &&
      input.target.d1SchemaHistoryDigest ===
        expectedTarget.d1SchemaHistoryDigest;
    const d1MatchesApplied =
      input.target.d1SchemaVersion === appliedTarget?.d1SchemaVersion &&
      input.target.d1SchemaHistoryDigest ===
        appliedTarget.d1SchemaHistoryDigest;
    if (
      (!d1MatchesExpected && !d1MatchesApplied) ||
      JSON.stringify(input.target) !==
        JSON.stringify({
          ...expectedTarget,
          d1SchemaVersion: input.target.d1SchemaVersion,
          d1SchemaHistoryDigest: input.target.d1SchemaHistoryDigest,
        })
    ) {
      throw new Error(
        'finalized state target does not match its trusted bridge profile',
      );
    }
    const profile = this.#profile(input.targetSpec);
    const artifactDigest = trustedArtifactDigest(profile.stateWorker);
    const durableObjectMigrations = this.#finalizedMigrations(
      input.targetSpec,
      input.currentRecord,
      profile,
    );
    const priorDurableObjectTag =
      input.currentRecord.platformTarget?.stateDurableObjectTag;
    const targetDurableObjectTag = durableObjectMigrations.at(-1)?.tag;
    const secretNames = expectedSecretNames(true);
    const stateSpec = {
      ...input.targetSpec,
      schemaVersion: input.target.d1SchemaVersion,
    };
    const stateSpecDigest = deploymentSpecDigest(
      externalStateDeploymentSpec(stateSpec, profile, durableObjectMigrations),
    );
    const bindings = this.#bridgeBindings(
      stateSpec,
      bridge.databaseId,
      profile,
      artifactDigest,
      targetDurableObjectTag,
      undefined,
      stateSpecDigest,
    );
    const mutationDigest = createHash('sha256')
      .update(
        JSON.stringify({
          artifactDigest,
          bindings,
          durableObjectMigrations,
          priorDurableObjectTag: priorDurableObjectTag ?? null,
          targetDurableObjectTag: targetDurableObjectTag ?? null,
          secretNames,
          tags: [
            'fleet:anchorage',
            'role:platform-state',
            `group:${externalPlatformResourceGroupId(input.targetSpec)}`,
            'backend-switch:finalized',
          ],
        }),
      )
      .digest('hex');
    return {
      artifactDigest,
      durableObjectMigrations,
      ...(priorDurableObjectTag ? { priorDurableObjectTag } : {}),
      ...(targetDurableObjectTag ? { targetDurableObjectTag } : {}),
      secretNames,
      mutationDigest,
    };
  }

  async #assertPersistedFinalizedBridge(
    input: {
      readonly targetSpec: DeploymentSpec;
      readonly currentRecord: FleetRecord;
      readonly target: ExternalPlatformTargetDescription;
      readonly plan: BridgeMutationPlan;
    },
    bridge: BridgeSnapshot,
  ): Promise<void> {
    const live = await this.#inspectControlWorker(bridge.scriptName);
    const resources = input.currentRecord.platformResources;
    const namespaces = await this.#client.listDurableObjectNamespaces(
      bridge.scriptName,
    );
    const profile = this.#profile(input.targetSpec);
    const persistedMigrations =
      input.currentRecord.durableObjectMigrationHistory ??
      input.plan.durableObjectMigrations;
    const stateSpec = {
      ...input.targetSpec,
      schemaVersion:
        input.currentRecord.platformTarget?.d1SchemaVersion ??
        input.currentRecord.schemaVersion,
    };
    const stateSpecDigest = deploymentSpecDigest(
      externalStateDeploymentSpec(stateSpec, profile, persistedMigrations),
    );
    const bindings = this.#bridgeBindings(
      stateSpec,
      bridge.databaseId,
      profile,
      bridge.artifactDigest,
      input.currentRecord.platformResources?.stateWorker.durableObjectTag ??
        input.currentRecord.platformTarget?.stateDurableObjectTag ??
        persistedMigrations.at(-1)?.tag,
      undefined,
      stateSpecDigest,
    );
    const expectedTopology = bridgeTopologyExpectationFromBindings({
      bindings,
      secretNames: bridge.secretNames,
      durableObjectBindings: bridge.durableObjectBindings,
      namespaceIds: bridge.namespaceIds,
    });
    const persistedSpecDigest = live?.plainTextBindings.FLEET_SPEC_DIGEST;
    if (
      !live ||
      !resources ||
      live.artifactVersion !== bridge.artifactVersion ||
      !bridgeTopologyMatches(live, namespaces, {
        ...expectedTopology,
        fixedVariables: {
          ...expectedTopology.fixedVariables,
          ...(persistedSpecDigest
            ? { FLEET_SPEC_DIGEST: persistedSpecDigest }
            : {}),
        },
      }) ||
      live.workersDevEnabled ||
      live.previewUrlsEnabled ||
      live.routeHostnames.length !== 0 ||
      live.zoneRoutes.length !== 0
    ) {
      throw new Error(
        'finalized ordinary state bridge changed from its persisted snapshot',
      );
    }
  }

  async ensureFinalizedState(input: {
    readonly targetSpec: DeploymentSpec;
    readonly currentRecord: FleetRecord;
    readonly target: ExternalPlatformTargetDescription;
    readonly plan: BridgeMutationPlan;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<BridgeSnapshot> {
    const bridge = finalizedBridgeForRecord(input.currentRecord);
    const expectedPlan = this.describeFinalizedState(input);
    if (JSON.stringify(expectedPlan) !== JSON.stringify(input.plan)) {
      throw new Error(
        'finalized state mutation differs from its durable authorization',
      );
    }
    const inspectDesired = async (): Promise<BridgeSnapshot | undefined> => {
      try {
        const stateSpec = {
          ...input.targetSpec,
          schemaVersion: input.target.d1SchemaVersion,
        };
        const stateSpecDigest = deploymentSpecDigest(
          externalStateDeploymentSpec(
            stateSpec,
            this.#profile(input.targetSpec),
            input.plan.durableObjectMigrations,
          ),
        );
        const desired = await this.#inspectBridge(
          stateSpec,
          bridge.databaseId,
          input.plan.artifactDigest,
          true,
          undefined,
          input.plan.durableObjectMigrations,
          stateSpecDigest,
        );
        if (
          bridge.namespaceIds.some(
            (namespaceId) => !desired.namespaceIds.includes(namespaceId),
          )
        ) {
          throw new Error(
            'finalized state mutation lost a persisted namespace',
          );
        }
        return desired;
      } catch (error) {
        if (
          error instanceof Error &&
          error.message.includes('did not converge exactly')
        ) {
          return undefined;
        }
        throw error;
      }
    };
    const alreadyDesired = await inspectDesired();
    if (alreadyDesired) return alreadyDesired;
    await this.#assertPersistedFinalizedBridge(input, bridge);
    const profile = this.#profile(input.targetSpec);
    const stateSpec = {
      ...input.targetSpec,
      schemaVersion: input.target.d1SchemaVersion,
    };
    const stateSpecDigest = deploymentSpecDigest(
      externalStateDeploymentSpec(
        stateSpec,
        profile,
        input.plan.durableObjectMigrations,
      ),
    );
    try {
      await this.#client.withMutationFence(input.fence, () =>
        this.#client.uploadControlWorker({
          scriptName: bridge.scriptName,
          ...profile.stateWorker,
          bindings: this.#bridgeBindings(
            stateSpec,
            bridge.databaseId,
            profile,
            input.plan.artifactDigest,
            input.plan.targetDurableObjectTag,
            undefined,
            stateSpecDigest,
          ),
          migrations: workerMigrations(
            input.plan.durableObjectMigrations,
            input.plan.priorDurableObjectTag,
          ),
          tags: [
            'fleet:anchorage',
            'role:platform-state',
            `group:${externalPlatformResourceGroupId(input.targetSpec)}`,
            'backend-switch:finalized',
          ],
        }),
      );
    } catch (cause) {
      const committed = await inspectDesired();
      if (committed) return committed;
      throw cause;
    }
    const converged = await inspectDesired();
    if (!converged) {
      throw new Error('finalized ordinary state bridge did not converge');
    }
    return converged;
  }

  async assertFinalizedState(input: {
    readonly targetSpec: DeploymentSpec;
    readonly currentRecord: FleetRecord;
    readonly target: ExternalPlatformTargetDescription;
    readonly plan: BridgeMutationPlan;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<void> {
    await input.fence.assertOwned();
    const bridge = finalizedBridgeForRecord(input.currentRecord);
    if (
      JSON.stringify(this.describeFinalizedState(input)) !==
      JSON.stringify(input.plan)
    ) {
      throw new Error(
        'finalized state inspection differs from its intended mutation',
      );
    }
    try {
      const stateSpec = {
        ...input.targetSpec,
        schemaVersion: input.target.d1SchemaVersion,
      };
      const stateSpecDigest = deploymentSpecDigest(
        externalStateDeploymentSpec(
          stateSpec,
          this.#profile(input.targetSpec),
          input.plan.durableObjectMigrations,
        ),
      );
      const desired = await this.#inspectBridge(
        stateSpec,
        bridge.databaseId,
        input.plan.artifactDigest,
        true,
        undefined,
        input.plan.durableObjectMigrations,
        stateSpecDigest,
      );
      if (
        bridge.namespaceIds.every((namespaceId) =>
          desired.namespaceIds.includes(namespaceId),
        )
      ) {
        return;
      }
    } catch (error) {
      if (
        !(error instanceof Error) ||
        !error.message.includes('did not converge exactly')
      ) {
        throw error;
      }
    }
    await this.#assertPersistedFinalizedBridge(input, bridge);
  }

  #bridgeBindings(
    spec: DeploymentSpec,
    databaseId: string,
    profile: ExternalPlatformProfile,
    artifactDigest: string,
    durableObjectTag: string | undefined,
    application?: ApplicationBindingTopology,
    specDigest = deploymentSpecDigest(spec),
  ): readonly Readonly<Record<string, unknown>>[] {
    const groupId = externalPlatformResourceGroupId(spec);
    return [
      { name: 'DB', type: 'd1', database_id: databaseId },
      { name: 'DEPLOYMENT_TENANT', type: 'plain_text', text: spec.tenantTag },
      { name: 'FLEET_ENVIRONMENT', type: 'plain_text', text: spec.environment },
      {
        name: 'FLEET_SCHEMA_VERSION',
        type: 'plain_text',
        text: String(spec.schemaVersion),
      },
      ...(application?.vars.map(({ name, value }) => ({
        name,
        type: 'plain_text',
        text: value,
      })) ?? []),
      ...(application?.r2Buckets.map(({ name, bucketName }) => ({
        name,
        type: 'r2_bucket',
        bucket_name: bucketName,
      })) ?? []),
      {
        name: 'FLEET_SPEC_DIGEST',
        type: 'plain_text',
        text: specDigest,
      },
      { name: 'FLEET_RESOURCE_GROUP', type: 'plain_text', text: groupId },
      {
        name: 'FLEET_RESOURCE_ROLE',
        type: 'plain_text',
        text: 'platform-state',
      },
      {
        name: 'FLEET_DEPLOYMENT_SCRIPT',
        type: 'plain_text',
        text: spec.scriptName,
      },
      {
        name: 'FLEET_MAINTENANCE_CAPABILITIES',
        type: 'plain_text',
        text: 'required',
      },
      {
        name: 'FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY',
        type: 'plain_text',
        text: profile.maintenanceCapabilityPublicKey,
      },
      {
        name: 'FLEET_ARTIFACT_DIGEST',
        type: 'plain_text',
        text: artifactDigest,
      },
      {
        name: 'FLEET_RUNTIME_CONTRACT',
        type: 'plain_text',
        text: String(profile.runtimeContractVersion),
      },
      ...(durableObjectTag
        ? [{ name: 'FLEET_DO_TAG', type: 'plain_text', text: durableObjectTag }]
        : []),
      { name: 'OUTBOUND_TENANT_ID', type: 'plain_text', text: spec.tenantTag },
      {
        name: 'OUTBOUND_ENVIRONMENT',
        type: 'plain_text',
        text: spec.environment,
      },
      { name: 'OUTBOUND_RESOURCE_GROUP_ID', type: 'plain_text', text: groupId },
      {
        name: 'OUTBOUND_STATE_SCRIPT_NAME',
        type: 'plain_text',
        text: spec.scriptName,
      },
      {
        name: 'OUTBOUND_ROUTE_HOSTNAME',
        type: 'plain_text',
        text: spec.routeHostname.toLowerCase(),
      },
      { name: 'OUTBOUND_POLICY_ID', type: 'plain_text', text: groupId },
      ...spec.durableObjectBindings.map((binding) => ({
        name: binding.name,
        type: 'durable_object_namespace',
        class_name: binding.className,
      })),
      ...(spec.queueProducer
        ? [
            {
              name: FLEET_AUDIT_PROXY_STATE_BINDING,
              type: 'durable_object_namespace',
              class_name: FLEET_AUDIT_PROXY_CLASS_NAME,
            },
            {
              name: 'FLEET_AUDIT_PROXY_INGRESS',
              type: 'plain_text',
              text: 'required',
            },
            {
              name: 'AUDIT_QUEUE',
              type: 'queue',
              queue_name: spec.queueProducer.queueName,
            },
          ]
        : []),
      {
        name: 'OUTBOUND_PROXY',
        type: 'service',
        service: this.#sharedOutboundWorkerName,
        entrypoint: 'StateEgress',
      },
      {
        name: 'FLEET_AUDIT_PROXY_OBJECT_NAME',
        type: 'plain_text',
        text: AUDIT_PROXY_INSTANCE_NAME,
      },
    ];
  }

  #describeBridgeMutation(input: {
    readonly priorSpec: DeploymentSpec;
    readonly targetSpec: DeploymentSpec;
    readonly prior: PlainBackendSnapshot;
  }): BridgeMutationPlan {
    const profile = this.#profile(input.targetSpec);
    const template = profile.legacyBridgeWorker;
    if (!template) throw new Error('backend switch profile has no bridge');
    const artifact = composeLegacyBridgeArtifact(input.priorSpec, template);
    const artifactDigest = trustedArtifactDigest(artifact);
    const durableObjectMigrations = migrationHistory(
      input.priorSpec,
      profile,
      input.targetSpec.queueProducer !== undefined,
    );
    const priorDurableObjectTag =
      input.priorSpec.durableObjectMigrations.at(-1)?.tag;
    const targetDurableObjectTag = durableObjectMigrations.at(-1)?.tag;
    const secretNames = expectedSecretNames(true, input.prior.application);
    const bindings = this.#bridgeBindings(
      input.targetSpec,
      input.prior.databaseId,
      profile,
      artifactDigest,
      targetDurableObjectTag,
      input.prior.application,
    );
    const mutationDigest = createHash('sha256')
      .update(
        JSON.stringify({
          artifactDigest,
          bindings,
          durableObjectMigrations,
          priorDurableObjectTag: priorDurableObjectTag ?? null,
          secretNames,
          tags: [
            'fleet:anchorage',
            'role:platform-state',
            `group:${externalPlatformResourceGroupId(input.targetSpec)}`,
            'backend-switch:bridge',
          ],
        }),
      )
      .digest('hex');
    return {
      artifactDigest,
      durableObjectMigrations,
      ...(priorDurableObjectTag ? { priorDurableObjectTag } : {}),
      ...(targetDurableObjectTag ? { targetDurableObjectTag } : {}),
      secretNames,
      mutationDigest,
    };
  }

  describeBridge(input: {
    readonly priorSpec: DeploymentSpec;
    readonly targetSpec: DeploymentSpec;
    readonly prior: PlainBackendSnapshot;
  }): BridgeMutationPlan {
    return this.#describeBridgeMutation(input);
  }

  async #inspectBridge(
    spec: DeploymentSpec,
    databaseId: string,
    artifactDigest: string,
    stateOnly: boolean,
    application?: ApplicationBindingTopology,
    durableObjectMigrations?: readonly DurableObjectMigration[],
    specDigest = deploymentSpecDigest(spec),
  ): Promise<BridgeSnapshot> {
    const live = await this.#inspectControlWorker(spec.scriptName);
    if (!live) throw new Error(`bridge '${spec.scriptName}' is absent`);
    const profile = this.#profile(spec);
    const durableObjectTag = (
      durableObjectMigrations ??
      migrationHistory(spec, profile, spec.queueProducer !== undefined)
    ).at(-1)?.tag;
    const bindings = this.#bridgeBindings(
      spec,
      databaseId,
      profile,
      artifactDigest,
      durableObjectTag,
      application,
      specDigest,
    );
    const namespaceIds = await this.#client.listDurableObjectNamespaces(
      spec.scriptName,
    );
    if (
      !bridgeTopologyMatches(
        live,
        namespaceIds,
        bridgeTopologyExpectationFromBindings({
          bindings,
          secretNames: expectedSecretNames(true, application),
          ...(application ? { application } : {}),
        }),
      ) ||
      live.workersDevEnabled ||
      live.previewUrlsEnabled ||
      live.zoneRoutes.length !== 0
    ) {
      throw new Error(`bridge '${spec.scriptName}' did not converge exactly`);
    }
    const domain = (await this.#client.listCustomDomains()).find(
      (candidate) =>
        candidate.hostname.toLowerCase() === spec.routeHostname.toLowerCase() &&
        candidate.service === spec.scriptName,
    );
    return {
      scriptName: spec.scriptName,
      artifactVersion: live.artifactVersion,
      artifactDigest,
      databaseId,
      durableObjectBindings: live.durableObjectBindings,
      namespaceIds,
      secretNames: [...live.secretNames].sort(),
      ...(application ? { application } : {}),
      publicRouteAttached: domain !== undefined,
      stateOnly,
    };
  }

  async snapshotPlainDeployment(
    priorSpec: DeploymentSpec,
    currentRecord: FleetRecord,
    fence: BackendSwitchMutationFence,
  ): Promise<PlainBackendSnapshot> {
    await fence.assertOwned();
    const [database, live, domains] = await Promise.all([
      this.#client.findDatabase(priorSpec.databaseName),
      this.#inspectControlWorker(priorSpec.scriptName),
      this.#client.listCustomDomains(),
    ]);
    if (
      currentRecord.backend !== 'plain-worker' ||
      currentRecord.phase !== 'ready' ||
      currentRecord.scriptName !== priorSpec.scriptName ||
      currentRecord.databaseName !== priorSpec.databaseName ||
      currentRecord.desiredSpecDigest !== deploymentSpecDigest(priorSpec)
    ) {
      throw new Error(
        'fleet record does not own the requested plain deployment',
      );
    }
    if (!database || !live) throw new Error('plain deployment is absent');
    const matchingDomains = domains.filter(
      (domain) =>
        domain.hostname.toLowerCase() ===
          priorSpec.routeHostname.toLowerCase() &&
        domain.service === priorSpec.scriptName,
    );
    if (
      live.databaseIds.length !== 1 ||
      live.databaseIds[0] !== database.id ||
      database.id !== currentRecord.databaseId ||
      live.artifactVersion !== currentRecord.artifactVersion ||
      JSON.stringify(sortedBindingKeys(live.durableObjectBindings)) !==
        JSON.stringify(
          sortedBindingKeys(currentRecord.durableObjectBindings),
        ) ||
      JSON.stringify(sortedBindingKeys(live.durableObjectBindings)) !==
        JSON.stringify(sortedBindingKeys(priorSpec.durableObjectBindings)) ||
      JSON.stringify([...live.secretNames].sort()) !==
        JSON.stringify(
          expectedSecretNames(false, currentRecord.applicationBindings),
        ) ||
      !liveApplicationTopologyMatches(
        currentRecord.applicationBindings,
        live,
        DEPLOYMENT_PLATFORM_VARIABLE_NAMES,
      ) ||
      matchingDomains.length !== 1 ||
      live.workersDevEnabled ||
      live.previewUrlsEnabled ||
      live.zoneRoutes.length !== 0
    ) {
      throw new Error(
        'plain deployment snapshot is not exact or private-by-route',
      );
    }
    const customDomain = matchingDomains[0];
    if (!customDomain) throw new Error('plain custom domain disappeared');
    if (
      (currentRecord.applicationResources ?? []).some(
        (resource) => resource.state !== 'created' || !resource.creationDate,
      )
    ) {
      throw new Error(
        'plain deployment has incomplete application R2 creation identity',
      );
    }
    return {
      scriptName: priorSpec.scriptName,
      artifactVersion: live.artifactVersion,
      specDigest: deploymentSpecDigest(priorSpec),
      databaseId: database.id,
      databaseName: database.name,
      durableObjectBindings: live.durableObjectBindings,
      namespaceIds: await this.#client.listDurableObjectNamespaces(
        priorSpec.scriptName,
      ),
      secretNames: [...live.secretNames].sort(),
      ...(currentRecord.applicationBindings
        ? { application: currentRecord.applicationBindings }
        : {}),
      applicationResources: currentRecord.applicationResources ?? [],
      customDomain: {
        id: customDomain.id,
        hostname: customDomain.hostname,
      },
    };
  }

  async ensureBridge(input: {
    readonly priorSpec: DeploymentSpec;
    readonly targetSpec: DeploymentSpec;
    readonly secrets: DeploymentSecrets;
    readonly prior: PlainBackendSnapshot;
    readonly plan: BridgeMutationPlan;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<BridgeSnapshot> {
    const profile = this.#profile(input.targetSpec);
    const bridgeTemplate = profile.legacyBridgeWorker;
    if (!bridgeTemplate)
      throw new Error('backend switch profile has no bridge');
    const artifact = composeLegacyBridgeArtifact(
      input.priorSpec,
      bridgeTemplate,
    );
    const artifactDigest = trustedArtifactDigest(artifact);
    const expectedPlan = this.#describeBridgeMutation(input);
    if (!sameJson(expectedPlan, input.plan)) {
      throw new Error('backend switch bridge plan differs from durable intent');
    }
    const migrations = input.plan.durableObjectMigrations;
    const durableObjectTag = input.plan.targetDurableObjectTag;
    const liveBefore = await this.#inspectControlWorker(input.prior.scriptName);
    if (!liveBefore) throw new Error('backend switch prior Worker disappeared');
    let liveDurableObjectTag: string | undefined;
    if (liveBefore.artifactVersion === input.prior.artifactVersion) {
      liveDurableObjectTag = input.plan.priorDurableObjectTag;
    } else if (
      liveBefore.plainTextBindings.FLEET_ARTIFACT_DIGEST === artifactDigest &&
      liveBefore.plainTextBindings.FLEET_DO_TAG === durableObjectTag &&
      liveBefore.databaseIds.length === 1 &&
      liveBefore.databaseIds[0] === input.prior.databaseId &&
      liveBefore.plainTextBindings.DEPLOYMENT_TENANT ===
        input.targetSpec.tenantTag &&
      liveBefore.plainTextBindings.FLEET_ENVIRONMENT ===
        input.targetSpec.environment
    ) {
      liveDurableObjectTag = durableObjectTag;
    } else {
      throw new Error(
        'backend switch bridge live state is neither the snapshotted prior nor the intended bridge',
      );
    }
    await this.#client.withMutationFence(input.fence, async () => {
      await this.#client.uploadControlWorker({
        scriptName: input.prior.scriptName,
        ...artifact,
        bindings: this.#bridgeBindings(
          input.targetSpec,
          input.prior.databaseId,
          profile,
          artifactDigest,
          durableObjectTag,
          input.prior.application,
        ),
        migrations: workerMigrations(migrations, liveDurableObjectTag),
        tags: [
          'fleet:anchorage',
          'role:platform-state',
          `group:${externalPlatformResourceGroupId(input.targetSpec)}`,
          'backend-switch:bridge',
        ],
      });
      await this.#client.putControlSecrets(input.prior.scriptName, {
        DEPLOYMENT_IDENTITY_SECRET: input.secrets.deploymentIdentity,
        MAINTENANCE_ADMIN_SECRET: input.secrets.maintenanceAdmin,
        OUTBOUND_PROXY_CREDENTIAL: this.#credential(
          input.targetSpec,
          input.prior.scriptName,
        ),
        ...applicationSecretValues(input.priorSpec, input.secrets),
      });
    });
    const bridge = await this.#inspectBridge(
      input.targetSpec,
      input.prior.databaseId,
      artifactDigest,
      false,
      input.prior.application,
    );
    const retained = new Set(input.prior.namespaceIds);
    if (
      input.prior.namespaceIds.some(
        (namespaceId) => !bridge.namespaceIds.includes(namespaceId),
      )
    ) {
      throw new Error('bridge changed a prior Durable Object namespace');
    }
    if (retained.size !== input.prior.namespaceIds.length) {
      throw new Error('plain snapshot contains duplicate namespaces');
    }
    return bridge;
  }

  async recoverBridge(input: {
    readonly priorSpec: DeploymentSpec;
    readonly targetSpec: DeploymentSpec;
    readonly prior: PlainBackendSnapshot;
    readonly plan: BridgeMutationPlan;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<BridgeSnapshot | undefined> {
    await input.fence.assertOwned();
    const expectedPlan = this.#describeBridgeMutation(input);
    if (!sameJson(expectedPlan, input.plan)) {
      throw new Error('backend switch bridge plan differs from durable intent');
    }
    const live = await this.#inspectControlWorker(input.prior.scriptName);
    if (!live || live.artifactVersion === input.prior.artifactVersion) {
      return undefined;
    }
    if (
      live.plainTextBindings.FLEET_ARTIFACT_DIGEST !==
        input.plan.artifactDigest ||
      live.plainTextBindings.FLEET_DO_TAG !==
        input.plan.targetDurableObjectTag ||
      live.databaseIds.length !== 1 ||
      live.databaseIds[0] !== input.prior.databaseId ||
      live.plainTextBindings.DEPLOYMENT_TENANT !== input.targetSpec.tenantTag ||
      live.plainTextBindings.FLEET_ENVIRONMENT !== input.targetSpec.environment
    ) {
      throw new Error('backend switch cannot reconstruct a foreign bridge');
    }
    try {
      return await this.#inspectBridge(
        input.targetSpec,
        input.prior.databaseId,
        input.plan.artifactDigest,
        false,
        input.prior.application,
      );
    } catch (error) {
      if (
        error instanceof Error &&
        error.message.includes('did not converge exactly')
      ) {
        return undefined;
      }
      throw error;
    }
  }

  async ensureCandidate(input: {
    readonly targetSpec: DeploymentSpec;
    readonly secrets: DeploymentSecrets;
    readonly bridge: BridgeSnapshot;
    readonly target: ExternalPlatformTargetDescription;
    readonly currentRecord: FleetRecord;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<BackendSwitchCandidateSnapshot> {
    const profile = this.#profile(input.targetSpec);
    const migrations = migrationHistory(
      input.targetSpec,
      profile,
      input.targetSpec.queueProducer !== undefined,
    );
    if (
      input.target.sharedOutboundWorkerName !==
        this.#sharedOutboundWorkerName ||
      input.target.stateEgressCredentialDigest !==
        this.#credentialDigest(input.targetSpec, input.bridge.scriptName) ||
      input.target.stateDurableObjectHistoryDigest !==
        durableObjectMigrationHistoryDigest(migrations)
    ) {
      throw new Error(
        'backend switch target does not match bridge state channels',
      );
    }
    const resources: ExternalPlatformResources = {
      ...(input.target.auditQueueName
        ? { auditQueueName: input.target.auditQueueName }
        : {}),
      maintenanceCapabilityPublicKey:
        input.target.maintenanceCapabilityPublicKey,
      stateWorker: {
        scriptName: input.bridge.scriptName,
        artifactVersion: input.bridge.artifactVersion,
        artifactDigest: input.bridge.artifactDigest,
        plane: 'ordinary',
        durableObjectBindings: input.bridge.durableObjectBindings,
        namespaceIds: input.bridge.namespaceIds,
      },
      outboundPolicy: input.target.outboundPolicy,
      sharedOutboundWorkerName: this.#sharedOutboundWorkerName,
    };
    const database = await this.#client.getDatabase(input.bridge.databaseId);
    if (!database) throw new Error('backend switch database disappeared');
    const deployed = await this.#backend.deployWorker(
      input.targetSpec,
      database,
      input.secrets,
      resources,
      input.fence,
      'pending',
      applicationBindingTopology(
        input.targetSpec,
        input.currentRecord.applicationResources ?? [],
      ),
    );
    const physicalScriptName =
      deployed.physicalScriptName ??
      this.#backend.releaseScriptName(input.targetSpec);
    const topology = externalReleaseTopology(
      input.targetSpec,
      resources,
      input.currentRecord.applicationResources,
    );
    const application = topology.application;
    if (!application) {
      throw new Error('external bridge candidate has no application topology');
    }
    if (
      topology.serviceBindings.length !== 0 ||
      topology.queueProducerBindings.length !== 0 ||
      JSON.stringify(topology.secretNames) !==
        JSON.stringify(
          [
            'DEPLOYMENT_IDENTITY_SECRET',
            ...application.secrets.map(({ name }) => name),
          ].sort(),
        )
    ) {
      throw new Error(
        'external bridge candidate received a privileged channel',
      );
    }
    const attestation = await this.#backend.ensureMaintenanceAttestation(
      input.targetSpec,
      input.secrets.maintenanceAdmin,
      input.fence,
      deployed.artifactVersion,
    );
    if (!attestation.health.armed) {
      throw new Error('backend switch candidate maintenance is unarmed');
    }
    return {
      physicalScriptName,
      specDigest: deploymentSpecDigest(input.targetSpec),
      artifactVersion: deployed.artifactVersion,
      releaseSchemaVersion: input.targetSpec.schemaVersion,
      application,
      topology,
      maintenance: {
        receipt: attestation.receipt,
        specDigest: deploymentSpecDigest(input.targetSpec),
      },
    };
  }

  async publishCandidateHost(input: {
    readonly targetSpec: DeploymentSpec;
    readonly candidate: ExternalReleaseSnapshot;
    readonly bridge: BridgeSnapshot;
    readonly target: ExternalPlatformTargetDescription;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<void> {
    const desired = {
      scriptName: input.candidate.physicalScriptName,
      tenantTag: input.targetSpec.tenantTag,
      environment: input.targetSpec.environment,
      policyId: input.target.outboundPolicy.policyId,
      policyDigest: input.target.outboundPolicy.policyDigest,
      policyHosts: input.target.outboundPolicy.policyHosts,
      stateEgress: {
        resourceGroupId: externalPlatformResourceGroupId(input.targetSpec),
        stateScriptName: input.bridge.scriptName,
        credentialDigest: this.#credentialDigest(
          input.targetSpec,
          input.bridge.scriptName,
        ),
      },
    };
    const existing = await this.#client.getHostRouting(
      this.#hostRoutingKvId,
      input.targetSpec.routeHostname,
    );
    if (existing !== undefined) {
      await parseHostRoutingTarget(existing);
      if (existing !== JSON.stringify(desired)) {
        throw new Error(
          'candidate host route differs from the complete durable target',
        );
      }
      return;
    }
    await this.#client.withMutationFence(input.fence, () =>
      this.#client.putHostRouting(
        this.#hostRoutingKvId,
        input.targetSpec.routeHostname,
        desired,
        {
          allowedCurrentScriptNames: [input.candidate.physicalScriptName],
          allowUnrouted: true,
        },
      ),
    );
  }

  async assertCandidateHostPublished(input: {
    readonly targetSpec: DeploymentSpec;
    readonly candidate: ExternalReleaseSnapshot;
    readonly target: ExternalPlatformTargetDescription;
  }): Promise<void> {
    const serialized = await this.#client.getHostRouting(
      this.#hostRoutingKvId,
      input.targetSpec.routeHostname,
    );
    if (!serialized) throw new Error('candidate host route is absent');
    await parseHostRoutingTarget(serialized);
    const expected = {
      scriptName: input.candidate.physicalScriptName,
      tenantTag: input.targetSpec.tenantTag,
      environment: input.targetSpec.environment,
      policyId: input.target.outboundPolicy.policyId,
      policyDigest: input.target.outboundPolicy.policyDigest,
      policyHosts: input.target.outboundPolicy.policyHosts,
      stateEgress: {
        resourceGroupId: externalPlatformResourceGroupId(input.targetSpec),
        stateScriptName: input.targetSpec.scriptName,
        credentialDigest: this.#credentialDigest(
          input.targetSpec,
          input.targetSpec.scriptName,
        ),
      },
    };
    if (serialized !== JSON.stringify(expected)) {
      throw new Error('candidate host route does not match the durable switch');
    }
  }

  async detachPlainCustomDomain(input: {
    readonly prior: PlainBackendSnapshot;
    readonly bridge: BridgeSnapshot;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<void> {
    const current = (await this.#client.listCustomDomains()).find(
      (domain) => domain.id === input.prior.customDomain.id,
    );
    if (
      current &&
      (current.service !== input.bridge.scriptName ||
        current.hostname.toLowerCase() !==
          input.prior.customDomain.hostname.toLowerCase())
    ) {
      throw new Error('plain custom domain ownership changed before detach');
    }
    if (current) await this.#client.detachCustomDomain(current.id, input.fence);
  }

  assertCandidateServing(input: {
    readonly targetSpec: DeploymentSpec;
    readonly candidate: ExternalReleaseSnapshot;
  }): Promise<void> {
    return this.#assertServing(
      input.targetSpec.routeHostname,
      input.candidate.physicalScriptName,
    );
  }

  async privatizeBridge(input: {
    readonly bridge: BridgeSnapshot;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<BridgeSnapshot> {
    await this.#client.withMutationFence(input.fence, () =>
      this.#client.disableControlWorkerPublicAccess(input.bridge.scriptName),
    );
    const live = await this.#inspectControlWorker(input.bridge.scriptName);
    if (
      !live ||
      live.workersDevEnabled ||
      live.previewUrlsEnabled ||
      live.routeHostnames.length !== 0 ||
      live.zoneRoutes.length !== 0
    ) {
      throw new Error('bridge remains publicly reachable after privatization');
    }
    return { ...input.bridge, publicRouteAttached: false };
  }

  async commitWorkersForPlatformsOwnership(input: {
    readonly prior: PlainBackendSnapshot;
    readonly bridge: BridgeSnapshot;
    readonly candidate: ExternalReleaseSnapshot;
    readonly target: ExternalPlatformTargetDescription;
    readonly targetSpec: DeploymentSpec;
    readonly currentRecord: FleetRecord;
  }): Promise<FleetRecord> {
    const bridgeHistory = migrationHistory(
      input.targetSpec,
      this.#profile(input.targetSpec),
      input.targetSpec.queueProducer !== undefined,
    );
    return {
      ...input.currentRecord,
      backend: 'workers-for-platforms',
      scriptName: input.prior.scriptName,
      databaseId: input.prior.databaseId,
      artifactVersion: input.candidate.artifactVersion,
      desiredSpecDigest: input.candidate.specDigest,
      schemaVersion: input.target.d1SchemaVersion,
      durableObjectTag: input.target.stateDurableObjectTag,
      durableObjectMigrationHistory: bridgeHistory,
      durableObjectMigrationHistoryDigest:
        input.target.stateDurableObjectHistoryDigest,
      durableObjectBindings:
        input.candidate.topology?.durableObjectBindings ?? [],
      applicationBindings: input.candidate.application,
      activeRelease: {
        physicalScriptName: input.candidate.physicalScriptName,
        specDigest: input.candidate.specDigest,
        artifactVersion: input.candidate.artifactVersion,
        releaseSchemaVersion: input.candidate.releaseSchemaVersion,
        application: input.candidate.application,
        ...(input.candidate.topology
          ? { topology: input.candidate.topology }
          : {}),
      },
      outboundPolicy: input.target.outboundPolicy,
      platformTarget: input.target,
      platformResources: {
        ...(input.target.auditQueueName
          ? { auditQueueName: input.target.auditQueueName }
          : {}),
        maintenanceCapabilityPublicKey:
          input.target.maintenanceCapabilityPublicKey,
        stateWorker: {
          scriptName: input.bridge.scriptName,
          artifactVersion: input.bridge.artifactVersion,
          artifactDigest: input.bridge.artifactDigest,
          plane: 'ordinary',
          ...(input.target.stateDurableObjectTag
            ? { durableObjectTag: input.target.stateDurableObjectTag }
            : {}),
          durableObjectBindings: input.bridge.durableObjectBindings,
          namespaceIds: input.bridge.namespaceIds,
        },
        outboundPolicy: input.target.outboundPolicy,
        sharedOutboundWorkerName: this.#sharedOutboundWorkerName,
      },
      phase: 'ready',
      pendingSpecDigest: undefined,
      pendingArtifactVersion: undefined,
      pendingRelease: undefined,
      migrationIntent: undefined,
    };
  }

  async routePlainDomainToBridge(input: {
    readonly prior: PlainBackendSnapshot;
    readonly bridge: BridgeSnapshot;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<void> {
    await this.#client.attachCustomDomain(
      {
        hostname: input.prior.customDomain.hostname,
        service: input.bridge.scriptName,
      },
      input.fence,
    );
  }

  assertPlainBridgeServing(input: {
    readonly prior: PlainBackendSnapshot;
    readonly bridge: BridgeSnapshot;
  }): Promise<void> {
    return this.#assertServing(
      input.prior.customDomain.hostname,
      input.bridge.scriptName,
    );
  }

  async removeCandidateHostAndDrain(input: {
    readonly targetSpec: DeploymentSpec;
    readonly candidate: ExternalReleaseSnapshot;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<void> {
    await this.#client.withMutationFence(input.fence, () =>
      this.#client.deleteHostRouting(
        this.#hostRoutingKvId,
        input.targetSpec.routeHostname,
        [
          {
            scriptName: input.candidate.physicalScriptName,
            tenantTag: input.targetSpec.tenantTag,
            environment: input.targetSpec.environment,
          },
        ],
      ),
    );
    if (
      (await this.#client.getHostRouting(
        this.#hostRoutingKvId,
        input.targetSpec.routeHostname,
      )) !== undefined
    ) {
      throw new Error('candidate host route remains before rollback drain');
    }
    await this.#drainCandidate(input.candidate);
  }

  async restorePlainDeployment(input: {
    readonly priorSpec: DeploymentSpec;
    readonly targetSpec: DeploymentSpec;
    readonly secrets: DeploymentSecrets;
    readonly prior: PlainBackendSnapshot;
    readonly bridge: BridgeSnapshot;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<string> {
    const profile = this.#profile(input.targetSpec);
    const bridgeTemplate = profile.legacyBridgeWorker;
    if (!bridgeTemplate)
      throw new Error('backend switch profile has no bridge');
    const artifact = composeLegacyBridgeArtifact(
      input.priorSpec,
      bridgeTemplate,
    );
    const migrations = migrationHistory(
      input.priorSpec,
      profile,
      input.priorSpec.queueProducer !== undefined,
    );
    await this.#client.withMutationFence(input.fence, async () => {
      await this.#client.uploadControlWorker({
        scriptName: input.prior.scriptName,
        ...artifact,
        bindings: [
          { name: 'DB', type: 'd1', database_id: input.prior.databaseId },
          {
            name: 'DEPLOYMENT_TENANT',
            type: 'plain_text',
            text: input.priorSpec.tenantTag,
          },
          {
            name: 'FLEET_ENVIRONMENT',
            type: 'plain_text',
            text: input.priorSpec.environment,
          },
          {
            name: 'FLEET_SCHEMA_VERSION',
            type: 'plain_text',
            text: String(input.priorSpec.schemaVersion),
          },
          {
            name: 'FLEET_SPEC_DIGEST',
            type: 'plain_text',
            text: deploymentSpecDigest(input.priorSpec),
          },
          ...(input.prior.application?.vars.map(({ name, value }) => ({
            name,
            type: 'plain_text',
            text: value,
          })) ?? []),
          ...(input.prior.application?.r2Buckets.map(
            ({ name, bucketName }) => ({
              name,
              type: 'r2_bucket',
              bucket_name: bucketName,
            }),
          ) ?? []),
          ...input.priorSpec.durableObjectBindings.map((binding) => ({
            name: binding.name,
            type: 'durable_object_namespace',
            class_name: binding.className,
          })),
        ],
        migrations: workerMigrations(migrations, migrations.at(-1)?.tag),
        tags: ['fleet:anchorage', 'backend-switch:rolled-back'],
      });
      await this.#client.putControlSecrets(input.prior.scriptName, {
        DEPLOYMENT_IDENTITY_SECRET: input.secrets.deploymentIdentity,
        MAINTENANCE_ADMIN_SECRET: input.secrets.maintenanceAdmin,
        ...applicationSecretValues(input.priorSpec, input.secrets),
      });
    });
    const live = await this.#inspectControlWorker(input.prior.scriptName);
    const namespaces = await this.#client.listDurableObjectNamespaces(
      input.prior.scriptName,
    );
    if (
      !live ||
      live.databaseIds[0] !== input.prior.databaseId ||
      JSON.stringify(sortedBindingKeys(live.durableObjectBindings)) !==
        JSON.stringify(sortedBindingKeys(input.prior.durableObjectBindings)) ||
      JSON.stringify([...live.secretNames].sort()) !==
        JSON.stringify(expectedSecretNames(false, input.prior.application)) ||
      !liveApplicationTopologyMatches(
        input.prior.application,
        live,
        DEPLOYMENT_PLATFORM_VARIABLE_NAMES,
      ) ||
      input.bridge.namespaceIds.some(
        (namespaceId) => !namespaces.includes(namespaceId),
      )
    ) {
      throw new Error(
        'plain rollback did not retain bridge Durable Object history',
      );
    }
    return live.artifactVersion;
  }

  async commitPlainOwnership(input: {
    readonly prior: PlainBackendSnapshot;
    readonly restoredArtifactVersion: string;
    readonly currentRecord: FleetRecord;
  }): Promise<FleetRecord> {
    return {
      ...stripPlatformOwnership(input.currentRecord),
      backend: 'plain-worker',
      scriptName: input.prior.scriptName,
      databaseId: input.prior.databaseId,
      artifactVersion: input.restoredArtifactVersion,
      desiredSpecDigest: input.prior.specDigest,
      durableObjectBindings: input.prior.durableObjectBindings,
      applicationBindings: input.prior.application,
      phase: 'ready',
    };
  }

  async ensureStateOnlyBridge(input: {
    readonly targetSpec: DeploymentSpec;
    readonly bridge: BridgeSnapshot;
    readonly target: ExternalPlatformTargetDescription;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<BridgeSnapshot> {
    const profile = this.#profile(input.targetSpec);
    const artifact = profile.stateWorker;
    const artifactDigest = trustedArtifactDigest(artifact);
    if (artifactDigest !== input.target.stateArtifactDigest) {
      throw new Error('state-only artifact differs from durable target');
    }
    const migrations = migrationHistory(
      input.targetSpec,
      profile,
      input.targetSpec.queueProducer !== undefined,
    );
    const stateSpecDigest = deploymentSpecDigest(
      externalStateDeploymentSpec(input.targetSpec, profile, migrations),
    );
    const applicationSecretNames =
      input.bridge.application?.secrets.map(({ name }) => name) ?? [];
    if (
      applicationSecretNames.length > 0 &&
      !this.#client.deleteControlSecrets
    ) {
      throw new Error(
        'backend switch client cannot remove application secrets',
      );
    }
    await this.#client.withMutationFence(input.fence, () =>
      this.#client.uploadControlWorker({
        scriptName: input.bridge.scriptName,
        ...artifact,
        bindings: this.#bridgeBindings(
          input.targetSpec,
          input.bridge.databaseId,
          profile,
          artifactDigest,
          migrations.at(-1)?.tag,
          undefined,
          stateSpecDigest,
        ),
        migrations: workerMigrations(migrations, migrations.at(-1)?.tag),
        tags: [
          'fleet:anchorage',
          'role:platform-state',
          `group:${externalPlatformResourceGroupId(input.targetSpec)}`,
          'backend-switch:finalized',
        ],
      }),
    );
    await this.#client.deleteControlSecrets?.(
      input.bridge.scriptName,
      applicationSecretNames,
      input.fence,
    );
    const state = await this.#inspectBridge(
      input.targetSpec,
      input.bridge.databaseId,
      artifactDigest,
      true,
      undefined,
      migrations,
      stateSpecDigest,
    );
    if (
      input.bridge.namespaceIds.some(
        (namespaceId) => !state.namespaceIds.includes(namespaceId),
      )
    ) {
      throw new Error(
        'state-only finalization changed a Durable Object namespace',
      );
    }
    return state;
  }

  async commitFinalizedOwnership(input: {
    readonly currentRecord: FleetRecord;
    readonly bridge: BridgeSnapshot;
    readonly target: ExternalPlatformTargetDescription;
  }): Promise<FleetRecord> {
    if (
      !input.target.sharedOutboundWorkerName ||
      !input.target.stateEgressCredentialDigest
    ) {
      throw new Error(
        'finalized ownership target has no dispatch-native state channels',
      );
    }
    return {
      ...input.currentRecord,
      platformResources: {
        ...(input.target.auditQueueName
          ? { auditQueueName: input.target.auditQueueName }
          : {}),
        maintenanceCapabilityPublicKey:
          input.target.maintenanceCapabilityPublicKey,
        stateWorker: {
          scriptName: input.bridge.scriptName,
          artifactVersion: input.bridge.artifactVersion,
          artifactDigest: input.target.stateArtifactDigest,
          plane: 'ordinary',
          ...(input.target.stateDurableObjectTag
            ? { durableObjectTag: input.target.stateDurableObjectTag }
            : {}),
          durableObjectBindings: input.bridge.durableObjectBindings,
          namespaceIds: input.bridge.namespaceIds,
        },
        outboundPolicy: input.target.outboundPolicy,
        sharedOutboundWorkerName: input.target.sharedOutboundWorkerName,
      },
    };
  }

  async removeSwitchTraffic(
    input: SwitchBridgeRemovalAuthority & {
      readonly tenantTag: string;
      readonly environment: string;
      readonly routeHostname: string;
      readonly routeTargets: readonly HostRoutingTarget[];
      readonly fence: BackendSwitchMutationFence;
    },
  ): Promise<void> {
    const live = await this.#assertSwitchBridgeRemovalAuthority(input);
    const footprint = await this.#client.inspectOrdinaryWorkerFootprint(
      input.prior.scriptName,
    );
    if (live ? !footprint.scriptPresent : footprint.scriptPresent) {
      throw new Error(
        'refusing to mutate backend-switch traffic with an inconsistent ordinary Worker footprint',
      );
    }
    const routeTargets = input.routeTargets;
    const serialized = await this.#client.getHostRouting(
      this.#hostRoutingKvId,
      input.routeHostname,
    );
    if (serialized !== undefined) {
      await parseHostRoutingTarget(serialized);
      if (
        !routeTargets.some(
          (candidate) => JSON.stringify(candidate) === serialized,
        )
      ) {
        throw new Error(
          'refusing to delete a host route outside the decommission snapshot',
        );
      }
    }
    const domains = await this.#client.listCustomDomains();
    const hostnameDomains = domains.filter(
      (domain) =>
        domain.hostname.toLowerCase() ===
        input.prior.customDomain.hostname.toLowerCase(),
    );
    if (
      hostnameDomains.some(
        (domain) => domain.service !== input.prior.scriptName,
      )
    ) {
      throw new Error('refusing to detach a foreign same-host custom domain');
    }
    const attachedDomains = domains.filter(
      (domain) => domain.service === input.prior.scriptName,
    );
    if (
      attachedDomains.some(
        (domain) =>
          domain.id !== input.prior.customDomain.id ||
          domain.hostname.toLowerCase() !==
            input.prior.customDomain.hostname.toLowerCase(),
      ) ||
      footprint.customDomains.some(
        (domain) =>
          domain.id !== input.prior.customDomain.id ||
          domain.hostname.toLowerCase() !==
            input.prior.customDomain.hostname.toLowerCase() ||
          domain.service !== input.prior.scriptName,
      ) ||
      footprint.zoneRoutes.length > 0 ||
      (!live &&
        (attachedDomains.length > 0 ||
          footprint.customDomains.length > 0 ||
          footprint.workersDevEnabled === true ||
          footprint.previewUrlsEnabled === true))
    ) {
      throw new Error(
        'refusing to mutate an unexpected backend-switch ingress footprint',
      );
    }
    await this.#client.withMutationFence(input.fence, () =>
      this.#client.deleteHostRouting(
        this.#hostRoutingKvId,
        input.routeHostname,
        routeTargets,
      ),
    );
    for (const domain of hostnameDomains) {
      if (
        domain.hostname.toLowerCase() ===
          input.prior.customDomain.hostname.toLowerCase() &&
        domain.service === input.prior.scriptName
      ) {
        await this.#client.detachCustomDomain(domain.id, input.fence);
      }
    }
    if (live) {
      await this.#client.disableOrdinaryWorkerPublicAccess(
        input.prior.scriptName,
        input.fence,
      );
    }
    if (
      (await this.#client.getHostRouting(
        this.#hostRoutingKvId,
        input.routeHostname,
      )) !== undefined ||
      (await this.#client.listCustomDomains()).some(
        (domain) =>
          domain.hostname.toLowerCase() ===
            input.prior.customDomain.hostname.toLowerCase() &&
          domain.service === input.prior.scriptName,
      )
    ) {
      throw new Error('backend switch traffic remains during decommission');
    }
  }

  async assertSwitchTrafficRemoved(input: {
    readonly prior: PlainBackendSnapshot;
    readonly routeHostname: string;
  }): Promise<void> {
    const [route, domains, footprint] = await Promise.all([
      this.#client.getHostRouting(this.#hostRoutingKvId, input.routeHostname),
      this.#client.listCustomDomains(),
      this.#client.inspectOrdinaryWorkerFootprint(input.prior.scriptName),
    ]);
    if (
      route !== undefined ||
      domains.some((domain) => domain.service === input.prior.scriptName) ||
      footprint.customDomains.length > 0 ||
      footprint.zoneRoutes.length > 0 ||
      footprint.workersDevEnabled === true ||
      footprint.previewUrlsEnabled === true
    ) {
      throw new Error('backend switch traffic remains during decommission');
    }
  }

  async removeSwitchRelease(input: {
    readonly prior: PlainBackendSnapshot;
    readonly tenantTag: string;
    readonly environment: string;
    readonly routeHostname: string;
    readonly release: ExternalReleaseSnapshot;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<void> {
    const release = input.release;
    const candidateName = release.physicalScriptName;
    const live = await this.#inspectDispatchWorker(candidateName);
    const inventory = await this.#client.getScriptInventory(
      this.#hostRoutingKvId,
      candidateName,
    );
    if (
      inventory &&
      (inventory.scriptName !== candidateName ||
        inventory.tenantTag !== input.tenantTag ||
        inventory.environment !== input.environment ||
        inventory.databaseId !== input.prior.databaseId ||
        inventory.routeHostname.toLowerCase() !==
          input.routeHostname.toLowerCase())
    ) {
      throw new Error('refusing to delete a foreign release registry entry');
    }
    if (live) {
      const topology = release.topology;
      if (
        !inventory ||
        !topology ||
        live.tenantTag !== input.tenantTag ||
        live.environment !== input.environment ||
        live.desiredSpecDigest !== release.specDigest ||
        live.schemaVersion !== release.releaseSchemaVersion ||
        (release.artifactVersion !== 'pending' &&
          live.artifactVersion !== release.artifactVersion) ||
        live.databaseIds.length !== 1 ||
        live.databaseIds[0] !== input.prior.databaseId ||
        JSON.stringify(sortedBindingKeys(live.durableObjectBindings)) !==
          JSON.stringify(sortedBindingKeys(topology.durableObjectBindings)) ||
        JSON.stringify(live.serviceBindings ?? []) !==
          JSON.stringify(topology.serviceBindings) ||
        JSON.stringify(live.queueProducerBindings ?? []) !==
          JSON.stringify(topology.queueProducerBindings) ||
        JSON.stringify([...(live.secretNames ?? [])].sort()) !==
          JSON.stringify([...topology.secretNames].sort()) ||
        !liveApplicationTopologyMatches(
          release.application,
          live,
          DEPLOYMENT_PLATFORM_VARIABLE_NAMES,
        )
      ) {
        throw new Error('refusing to delete a foreign backend-switch release');
      }
      await this.#client.withMutationFence(input.fence, async () => {
        await this.#client.revokeDispatchSecrets(candidateName);
        await this.#client.deleteDispatchWorker(candidateName);
      });
    }
    await this.#client.withMutationFence(input.fence, () =>
      this.#client.deleteScriptInventory(this.#hostRoutingKvId, {
        scriptName: candidateName,
        tenantTag: input.tenantTag,
        environment: input.environment,
        databaseId: input.prior.databaseId,
        routeHostname: input.routeHostname,
      }),
    );
    if (
      (await this.#inspectDispatchWorker(candidateName)) ||
      (await this.#client.getScriptInventory(
        this.#hostRoutingKvId,
        candidateName,
      ))
    ) {
      throw new Error('backend-switch release remains after decommission');
    }
  }

  #canonicalRemovalPlanMatches(input: SwitchBridgeRemovalAuthority): boolean {
    if (!input.plan) return true;
    if (sameJson(this.#describeBridgeMutation(input), input.plan)) return true;
    if (!input.bridge?.stateOnly) return false;

    const profile = this.#profile(input.targetSpec);
    const durableObjectMigrations = migrationHistory(
      input.targetSpec,
      profile,
      input.targetSpec.queueProducer !== undefined,
    );
    const artifactDigest = trustedArtifactDigest(profile.stateWorker);
    const targetDurableObjectTag = durableObjectMigrations.at(-1)?.tag;
    const secretNames = expectedSecretNames(true);
    const stateSpecDigest = deploymentSpecDigest(
      externalStateDeploymentSpec(
        input.targetSpec,
        profile,
        durableObjectMigrations,
      ),
    );
    const bindings = this.#bridgeBindings(
      input.targetSpec,
      input.prior.databaseId,
      profile,
      artifactDigest,
      targetDurableObjectTag,
      undefined,
      stateSpecDigest,
    );
    const priorDurableObjectTag = input.plan.priorDurableObjectTag;
    const mutationDigest = createHash('sha256')
      .update(
        JSON.stringify({
          artifactDigest,
          bindings,
          durableObjectMigrations,
          priorDurableObjectTag: priorDurableObjectTag ?? null,
          targetDurableObjectTag: targetDurableObjectTag ?? null,
          secretNames,
          tags: [
            'fleet:anchorage',
            'role:platform-state',
            `group:${externalPlatformResourceGroupId(input.targetSpec)}`,
            'backend-switch:finalized',
          ],
        }),
      )
      .digest('hex');
    return sameJson(input.plan, {
      artifactDigest,
      durableObjectMigrations,
      ...(priorDurableObjectTag ? { priorDurableObjectTag } : {}),
      ...(targetDurableObjectTag ? { targetDurableObjectTag } : {}),
      secretNames,
      mutationDigest,
    });
  }

  #snapshotBridgeBindings(
    input: SwitchBridgeRemovalAuthority & { readonly bridge: BridgeSnapshot },
  ): readonly Readonly<Record<string, unknown>>[] {
    const profile = this.#profile(input.targetSpec);
    const application = input.bridge.stateOnly
      ? undefined
      : (input.bridge.application ?? input.prior.application);
    const statePlan =
      input.bridge.stateOnly &&
      input.plan?.artifactDigest === input.bridge.artifactDigest
        ? input.plan
        : undefined;
    const durableObjectMigrations =
      statePlan?.durableObjectMigrations ??
      migrationHistory(
        input.targetSpec,
        profile,
        input.targetSpec.queueProducer !== undefined,
      );
    const specDigest = input.bridge.stateOnly
      ? deploymentSpecDigest(
          externalStateDeploymentSpec(
            input.targetSpec,
            profile,
            durableObjectMigrations,
          ),
        )
      : deploymentSpecDigest(input.targetSpec);
    const bindings = this.#bridgeBindings(
      input.targetSpec,
      input.prior.databaseId,
      profile,
      input.bridge.artifactDigest,
      durableObjectMigrations.at(-1)?.tag,
      application,
      specDigest,
    );
    const expectedApplication = input.bridge.stateOnly
      ? undefined
      : input.prior.application;
    const expectedArtifactDigest = input.bridge.stateOnly
      ? input.bridge.artifactDigest
      : (input.plan?.artifactDigest ??
        this.#describeBridgeMutation(input).artifactDigest);
    const expectedDurableObjectBindings = bridgeTopologyExpectationFromBindings(
      {
        bindings,
        secretNames: [],
      },
    ).durableObjectBindings;
    if (
      input.bridge.scriptName !== input.prior.scriptName ||
      input.bridge.databaseId !== input.prior.databaseId ||
      input.bridge.artifactDigest !== expectedArtifactDigest ||
      !sameJson(input.bridge.application, expectedApplication) ||
      !sameJson(
        [...input.bridge.secretNames].sort(),
        expectedSecretNames(true, expectedApplication),
      ) ||
      !sameJson(
        normalizedDurableObjectBindings(
          input.bridge.durableObjectBindings,
          false,
        ),
        normalizedDurableObjectBindings(expectedDurableObjectBindings, false),
      ) ||
      input.bridge.durableObjectBindings.some(
        (binding) => !input.bridge.namespaceIds.includes(binding.namespaceId),
      )
    ) {
      throw new Error('refusing to delete a foreign backend-switch bridge');
    }
    return bindings;
  }

  async #assertSwitchBridgeRemovalAuthority(
    input: SwitchBridgeRemovalAuthority,
  ): Promise<Awaited<ReturnType<BackendSwitchApi['inspectControlWorker']>>> {
    if (!this.#canonicalRemovalPlanMatches(input)) {
      throw new Error('backend switch bridge plan differs from durable intent');
    }
    const live = await this.#inspectControlWorker(input.prior.scriptName);
    if (!live) return undefined;

    const liveNamespaceIds =
      input.plan || input.bridge
        ? await this.#client.listDurableObjectNamespaces(input.prior.scriptName)
        : [];

    const plannedBindings =
      input.plan && !input.bridge
        ? this.#bridgeBindings(
            input.targetSpec,
            input.prior.databaseId,
            this.#profile(input.targetSpec),
            input.plan.artifactDigest,
            input.plan.targetDurableObjectTag,
            input.prior.application,
          )
        : undefined;
    const isPlannedBridge =
      input.bridge === undefined &&
      input.plan !== undefined &&
      plannedBindings !== undefined &&
      bridgeTopologyMatches(
        live,
        liveNamespaceIds,
        bridgeTopologyExpectationFromBindings({
          bindings: plannedBindings,
          secretNames: input.plan.secretNames,
          application: input.prior.application,
        }),
      );
    const snapshotBindings = input.bridge
      ? this.#snapshotBridgeBindings({ ...input, bridge: input.bridge })
      : undefined;
    const isSnapshottedBridge =
      input.bridge !== undefined &&
      snapshotBindings !== undefined &&
      live.artifactVersion === input.bridge.artifactVersion &&
      bridgeTopologyMatches(
        live,
        liveNamespaceIds,
        bridgeTopologyExpectationFromBindings({
          bindings: snapshotBindings,
          secretNames: input.bridge.secretNames,
          durableObjectBindings: input.bridge.durableObjectBindings,
          namespaceIds: input.bridge.namespaceIds,
          ...(input.bridge.stateOnly
            ? input.bridge.application
              ? { application: input.bridge.application }
              : {}
            : {
                application:
                  input.bridge.application ?? input.prior.application,
              }),
        }),
      );
    const isSnapshottedVersion =
      input.bridge?.artifactVersion === live.artifactVersion;
    const isPriorWorker =
      live.artifactVersion === input.prior.artifactVersion &&
      live.plainTextBindings.FLEET_SPEC_DIGEST === input.prior.specDigest &&
      JSON.stringify(sortedBindingKeys(live.durableObjectBindings)) ===
        JSON.stringify(sortedBindingKeys(input.prior.durableObjectBindings)) &&
      JSON.stringify([...live.secretNames].sort()) ===
        JSON.stringify([...input.prior.secretNames].sort()) &&
      JSON.stringify(live.serviceBindings) ===
        JSON.stringify(
          input.priorSpec.egressProxyService
            ? [
                {
                  name: 'EGRESS_PROXY',
                  service: input.priorSpec.egressProxyService,
                },
              ]
            : [],
        ) &&
      JSON.stringify(live.queueProducerBindings ?? []) ===
        JSON.stringify(
          input.priorSpec.queueProducer
            ? [
                {
                  name: input.priorSpec.queueProducer.binding,
                  queueName: input.priorSpec.queueProducer.queueName,
                },
              ]
            : [],
        ) &&
      live.kvNamespaceBindings.length === 0 &&
      liveApplicationTopologyMatches(
        input.prior.application,
        live,
        DEPLOYMENT_PLATFORM_VARIABLE_NAMES,
      );
    const restoredPlainTextBindings = {
      DEPLOYMENT_TENANT: input.priorSpec.tenantTag,
      FLEET_ENVIRONMENT: input.priorSpec.environment,
      FLEET_SCHEMA_VERSION: String(input.priorSpec.schemaVersion),
      FLEET_SPEC_DIGEST: input.prior.specDigest,
      ...Object.fromEntries(
        input.prior.application?.vars.map(({ name, value }) => [name, value]) ??
          [],
      ),
    };
    const isAllowedRestoredWorker =
      input.bridge !== undefined &&
      input.allowedArtifactVersions.includes(live.artifactVersion) &&
      live.artifactVersion !== input.prior.artifactVersion &&
      !isSnapshottedVersion &&
      live.plainTextBindings.FLEET_SPEC_DIGEST === input.prior.specDigest &&
      JSON.stringify(sortedBindingKeys(live.durableObjectBindings)) ===
        JSON.stringify(sortedBindingKeys(input.prior.durableObjectBindings)) &&
      JSON.stringify([...live.secretNames].sort()) ===
        JSON.stringify([...input.prior.secretNames].sort()) &&
      JSON.stringify(live.serviceBindings) === JSON.stringify([]) &&
      JSON.stringify(live.queueProducerBindings ?? []) === JSON.stringify([]) &&
      live.kvNamespaceBindings.length === 0 &&
      sameJson(
        Object.entries(live.plainTextBindings).sort(),
        Object.entries(restoredPlainTextBindings).sort(),
      ) &&
      liveApplicationTopologyMatches(
        input.prior.application,
        live,
        DEPLOYMENT_PLATFORM_VARIABLE_NAMES,
      );
    if (
      (isSnapshottedVersion && !isSnapshottedBridge) ||
      (!isSnapshottedBridge &&
        !isPlannedBridge &&
        !isPriorWorker &&
        !isAllowedRestoredWorker) ||
      live.databaseIds.length !== 1 ||
      live.databaseIds[0] !== input.prior.databaseId ||
      live.plainTextBindings.DEPLOYMENT_TENANT !== input.targetSpec.tenantTag ||
      live.plainTextBindings.FLEET_ENVIRONMENT !==
        input.targetSpec.environment ||
      (live.plainTextBindings.FLEET_RESOURCE_ROLE !== undefined &&
        (live.plainTextBindings.FLEET_RESOURCE_ROLE !== 'platform-state' ||
          live.plainTextBindings.FLEET_RESOURCE_GROUP !==
            externalPlatformResourceGroupId(input.targetSpec))) ||
      input.prior.namespaceIds.some(
        (namespaceId) =>
          !live.durableObjectBindings.some(
            (binding) => binding.namespaceId === namespaceId,
          ),
      )
    ) {
      throw new Error('refusing to delete a foreign backend-switch bridge');
    }
    return live;
  }

  async removeSwitchBridge(
    input: SwitchBridgeRemovalAuthority & {
      readonly fence: BackendSwitchMutationFence;
    },
  ): Promise<void> {
    const live = await this.#assertSwitchBridgeRemovalAuthority(input);
    const authoritativeNamespaceIds = [
      ...new Set([
        ...input.prior.namespaceIds,
        ...input.prior.durableObjectBindings.flatMap(({ namespaceId }) =>
          namespaceId ? [namespaceId] : [],
        ),
        ...(input.bridge?.namespaceIds ?? []),
        ...(input.bridge?.durableObjectBindings.flatMap(({ namespaceId }) =>
          namespaceId ? [namespaceId] : [],
        ) ?? []),
        ...(await this.#client.listDurableObjectNamespaces(
          input.prior.scriptName,
        )),
        ...(live?.durableObjectBindings.flatMap(({ namespaceId }) =>
          namespaceId ? [namespaceId] : [],
        ) ?? []),
      ]),
    ];
    if (live) {
      await this.#client.withMutationFence(input.fence, async () => {
        await this.#client.revokeControlSecrets(input.prior.scriptName);
        await this.#client.deleteControlWorker(input.prior.scriptName);
      });
    }
    if (await this.#inspectControlWorker(input.prior.scriptName)) {
      throw new Error('backend-switch bridge remains after decommission');
    }
    for (const namespaceId of authoritativeNamespaceIds) {
      if (await this.#client.hasDurableObjectNamespace(namespaceId)) {
        throw new Error(
          `backend-switch namespace '${namespaceId}' remains after decommission`,
        );
      }
    }
  }

  async findSwitchApplicationR2(
    resource: import('./types.js').ApplicationR2Resource,
  ): Promise<import('./types.js').ApplicationR2BucketSnapshot | undefined> {
    if (!this.#backend.findApplicationR2Bucket) {
      throw new Error('backend switch cannot inspect application R2');
    }
    return this.#backend.findApplicationR2Bucket(resource);
  }

  async assertSwitchApplicationR2Detached(
    resource: import('./types.js').ApplicationR2Resource,
    fence: BackendSwitchMutationFence,
  ): Promise<void> {
    if (!this.#backend.assertApplicationR2Detached) {
      throw new Error('backend switch cannot scan application R2 attachments');
    }
    await this.#backend.assertApplicationR2Detached(resource, fence);
  }

  async assertSwitchApplicationR2Empty(
    resource: import('./types.js').ApplicationR2Resource,
    fence: BackendSwitchMutationFence,
  ): Promise<void> {
    if (!this.#backend.assertApplicationR2Empty) {
      throw new Error('backend switch cannot inspect application R2 contents');
    }
    await this.#backend.assertApplicationR2Empty(resource, fence);
  }

  async deleteSwitchApplicationR2(
    resource: import('./types.js').ApplicationR2Resource,
    fence: BackendSwitchMutationFence,
  ): Promise<void> {
    if (!this.#backend.deleteApplicationR2Bucket) {
      throw new Error('backend switch cannot delete application R2');
    }
    await this.#backend.deleteApplicationR2Bucket(resource, fence);
  }

  async exportSwitchDatabase(input: {
    readonly prior: PlainBackendSnapshot;
    readonly targetSpec: DeploymentSpec;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<import('./types.js').DatabaseExport> {
    await this.#assertSwitchDatabaseOwned(
      input.prior,
      input.targetSpec.tenantTag,
      input.fence,
    );
    return this.#client.withMutationFence(input.fence, () =>
      this.#client.exportDatabase(input.prior.databaseId),
    );
  }

  async deleteSwitchDatabase(input: {
    readonly prior: PlainBackendSnapshot;
    readonly targetSpec: DeploymentSpec;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<void> {
    if (!(await this.#client.getDatabase(input.prior.databaseId))) {
      return;
    }
    await this.#assertSwitchDatabaseOwned(
      input.prior,
      input.targetSpec.tenantTag,
      input.fence,
    );
    await this.#client.withMutationFence(input.fence, () =>
      this.#client.deleteDatabase(input.prior.databaseId),
    );
    if (await this.#client.getDatabase(input.prior.databaseId)) {
      throw new Error('backend-switch database remains after delete');
    }
  }

  async #assertSwitchDatabaseOwned(
    prior: PlainBackendSnapshot,
    expectedTenantTag: string,
    fence: BackendSwitchMutationFence,
  ): Promise<void> {
    const database = await this.#client.getDatabase(prior.databaseId);
    if (
      !database ||
      database.id !== prior.databaseId ||
      database.name !== prior.databaseName
    ) {
      throw new Error(
        'backend-switch database identity changed before teardown',
      );
    }
    const owner = await this.#backend.readDeploymentIdentity(
      { ...database, created: false },
      fence,
    );
    if (owner !== expectedTenantTag) {
      throw new Error('backend-switch database deployment sentinel changed');
    }
    const attachments = await this.#client.listWorkerDatabaseAttachments(
      prior.databaseId,
    );
    if (attachments.length > 0) {
      throw new Error(
        'backend-switch database remains attached before teardown',
      );
    }
  }
}
