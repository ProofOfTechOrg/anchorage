// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import {
  applicationBindingTopology,
  applicationSecretNames,
} from './application-bindings.js';
import { canonicalEgressHosts, isSha256 } from './deployment-context.js';
import type { HostRoutingTarget } from './host-routing.js';
import type {
  DeploymentEgressPolicy,
  DeploymentSpec,
  DurableObjectMigration,
  ExternalPlatformProfile,
  ExternalPlatformResources,
  ExternalPlatformTargetDescription,
  ExternalReleaseSnapshot,
  ExternalReleaseTopology,
  FleetRecord,
  ProvisioningBackend,
  TrustedWorkerArtifact,
} from './types.js';

export const FLEET_AUDIT_PROXY_BINDING = 'AUDIT_PROXY';
export const FLEET_AUDIT_PROXY_STATE_BINDING = 'FLEET_AUDIT_PROXY_OBJECT';
export const FLEET_AUDIT_PROXY_CLASS_NAME = 'FlowsafeFleetAuditProxy';

export interface ExternalRouteExpectation {
  readonly release: ExternalReleaseSnapshot;
  readonly target: ExternalPlatformTargetDescription;
  readonly routeTarget?: HostRoutingTarget;
}

function dedupeExternalRouteExpectations(
  expectations: readonly ExternalRouteExpectation[],
): readonly ExternalRouteExpectation[] {
  const byRoute = new Map<string, ExternalRouteExpectation>();
  for (const expectation of expectations) {
    const key = JSON.stringify({
      scriptName: expectation.release.physicalScriptName,
      outboundPolicy: expectation.target.outboundPolicy,
      stateEgressCredentialDigest:
        expectation.target.stateEgressCredentialDigest ?? null,
      persistedRouteTarget: expectation.routeTarget ?? null,
    });
    if (!byRoute.has(key)) byRoute.set(key, expectation);
  }
  return [...byRoute.values()];
}

function currentRouteExpectations(
  record: FleetRecord,
  releases: readonly (ExternalReleaseSnapshot | undefined)[],
): readonly ExternalRouteExpectation[] {
  const target = record.platformTarget;
  if (!target) {
    throw new Error(
      `external ${record.phase} route authority has no persisted platform target`,
    );
  }
  const expectations = dedupeExternalRouteExpectations(
    releases.flatMap((release) => (release ? [{ release, target }] : [])),
  );
  if (expectations.length === 0) {
    throw new Error(
      `external ${record.phase} route authority has no persisted release`,
    );
  }
  return expectations;
}

export function externalRouteExpectations(
  record: FleetRecord,
): readonly ExternalRouteExpectation[] {
  if (record.backend !== 'workers-for-platforms') return [];
  if (
    record.phase === 'traffic-removed' ||
    (record.backendSwitchIntent?.subphase.startsWith('decommission-') ===
      true &&
      record.backendSwitchIntent.subphase !== 'decommission-traffic-authorized')
  ) {
    return [];
  }
  const decommissionRoutes =
    record.backendSwitchIntent?.decommissionSnapshot?.routeTargets;
  if (decommissionRoutes) {
    return decommissionRoutes.map(({ release, target, routeTarget }) => ({
      release,
      target,
      routeTarget,
    }));
  }
  if (
    record.migrationIntent &&
    ['migrating', 'decommissioning', 'credentials-revoked'].includes(
      record.phase,
    )
  ) {
    const intent = record.migrationIntent;
    if (
      JSON.stringify(intent.priorTarget.outboundPolicy) !==
      JSON.stringify(intent.priorOutboundPolicy)
    ) {
      throw new Error(
        'external migration prior target and outbound policy differ',
      );
    }
    const prior = {
      release: intent.priorRelease,
      target: intent.priorTarget,
    };
    const target = {
      release: intent.targetRelease,
      target: intent.target,
    };
    if (intent.subphase === 'route-published') return [target];
    if (
      intent.subphase === 'candidate-armed' ||
      (intent.platformOnly === true && intent.subphase === 'platform-applied')
    ) {
      return [prior, target];
    }
    return [prior];
  }
  if (record.phase === 'rolling-back') {
    return currentRouteExpectations(record, [
      record.activeRelease,
      record.pendingRelease,
    ]);
  }
  if (record.phase === 'publishing') {
    return currentRouteExpectations(record, [record.pendingRelease]);
  }
  if (record.phase === 'ready') {
    return currentRouteExpectations(record, [record.activeRelease]);
  }
  if (
    record.phase === 'decommissioning' ||
    record.phase === 'credentials-revoked'
  ) {
    return currentRouteExpectations(record, [
      record.activeRelease,
      record.pendingRelease,
    ]);
  }
  return [];
}

export function externalHostRoutingTarget(
  record: Pick<FleetRecord, 'tenantTag' | 'environment' | 'platformResources'>,
  expectation: ExternalRouteExpectation,
  explicitStateScriptName?: string,
): HostRoutingTarget {
  const stateScriptName =
    explicitStateScriptName ??
    expectation.routeTarget?.stateEgress?.stateScriptName ??
    record.platformResources?.stateWorker.scriptName;
  if (expectation.target.stateEgressCredentialDigest && !stateScriptName) {
    throw new Error(
      'external route expectation has no trusted state script identity',
    );
  }
  const target: HostRoutingTarget = {
    scriptName: expectation.release.physicalScriptName,
    tenantTag: record.tenantTag,
    environment: record.environment,
    policyId: expectation.target.outboundPolicy.policyId,
    policyDigest: expectation.target.outboundPolicy.policyDigest,
    policyHosts: expectation.target.outboundPolicy.policyHosts,
    ...(expectation.target.stateEgressCredentialDigest && stateScriptName
      ? {
          stateEgress: {
            resourceGroupId: expectation.target.outboundPolicy.policyId,
            stateScriptName,
            credentialDigest: expectation.target.stateEgressCredentialDigest,
          },
        }
      : {}),
  };
  if (
    expectation.routeTarget &&
    JSON.stringify(expectation.routeTarget) !== JSON.stringify(target)
  ) {
    throw new Error(
      'persisted external route target differs from its release and platform target',
    );
  }
  return expectation.routeTarget ?? target;
}

export function externalStateDeploymentSpec(
  spec: DeploymentSpec,
  profile: ExternalPlatformProfile,
  durableObjectMigrations: readonly DurableObjectMigration[] = profile.stateDurableObjectMigrations,
): DeploymentSpec {
  return {
    ...spec,
    authoredBy: 'platform',
    mainModule: profile.stateWorker.mainModule,
    modules: profile.stateWorker.modules,
    compatibilityDate: profile.stateWorker.compatibilityDate,
    compatibilityFlags: profile.stateWorker.compatibilityFlags,
    durableObjectMigrations,
    previousDurableObjectTag: spec.previousDurableObjectTag,
    egressProxyService: undefined,
  };
}

export function externalReleaseTopology(
  spec: DeploymentSpec,
  resources: ExternalPlatformResources | undefined,
  applicationResources: readonly import('./types.js').ApplicationR2Resource[] = [],
): ExternalReleaseTopology {
  if (!resources) {
    throw new Error('external release topology requires platform resources');
  }
  const namespaces = new Map(
    resources.stateWorker.durableObjectBindings.map((binding) => [
      `${binding.name}:${binding.className}`,
      binding.namespaceId,
    ]),
  );
  const durableObjectBindings = spec.durableObjectBindings.map((binding) => {
    const namespaceId = namespaces.get(`${binding.name}:${binding.className}`);
    if (!namespaceId) {
      throw new Error(
        `trusted state resources have no namespace for release binding '${binding.name}:${binding.className}'`,
      );
    }
    return {
      name: binding.name,
      className: binding.className,
      namespaceId,
      scriptName: resources.stateWorker.scriptName,
      ...(resources.stateWorker.dispatchNamespace
        ? { dispatchNamespace: resources.stateWorker.dispatchNamespace }
        : {}),
    };
  });
  if (spec.queueProducer) {
    const auditProxy = resources.stateWorker.durableObjectBindings.find(
      (binding) =>
        binding.name === FLEET_AUDIT_PROXY_STATE_BINDING &&
        binding.className === FLEET_AUDIT_PROXY_CLASS_NAME,
    );
    if (!auditProxy) {
      throw new Error('trusted state resources have no audit proxy namespace');
    }
    durableObjectBindings.push({
      name: FLEET_AUDIT_PROXY_BINDING,
      className: FLEET_AUDIT_PROXY_CLASS_NAME,
      namespaceId: auditProxy.namespaceId,
      scriptName: resources.stateWorker.scriptName,
      ...(resources.stateWorker.dispatchNamespace
        ? { dispatchNamespace: resources.stateWorker.dispatchNamespace }
        : {}),
    });
  }
  return {
    durableObjectBindings,
    serviceBindings: [],
    queueProducerBindings: [],
    secretNames: [
      'DEPLOYMENT_IDENTITY_SECRET',
      ...applicationSecretNames(spec),
    ].sort(),
    application: applicationBindingTopology(spec, applicationResources),
  };
}

function sha256(value: string | Uint8Array): string {
  return createHash('sha256').update(value).digest('hex');
}

export function trustedArtifactDigest(artifact: TrustedWorkerArtifact): string {
  return sha256(
    JSON.stringify({
      mainModule: artifact.mainModule,
      compatibilityDate: artifact.compatibilityDate,
      compatibilityFlags: [...(artifact.compatibilityFlags ?? [])].sort(),
      modules: artifact.modules
        .map((module) => ({
          name: module.name,
          contentType: module.contentType ?? null,
          contentSha256: sha256(module.content),
        }))
        .sort((left, right) => left.name.localeCompare(right.name)),
    }),
  );
}

export function canonicalMaintenanceCapabilityPublicKey(value: string): string {
  let key: Readonly<Record<string, unknown>>;
  try {
    const parsed: unknown = JSON.parse(value);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error('maintenance capability public key must be an object');
    }
    key = parsed as Readonly<Record<string, unknown>>;
  } catch {
    throw new Error('maintenance capability public key must be valid JSON');
  }
  if (
    key.kty !== 'OKP' ||
    key.crv !== 'Ed25519' ||
    key.alg !== 'EdDSA' ||
    typeof key.kid !== 'string' ||
    !/^[A-Za-z0-9._-]{1,64}$/u.test(key.kid) ||
    typeof key.x !== 'string' ||
    !/^[A-Za-z0-9_-]{43}$/u.test(key.x) ||
    key.d !== undefined
  ) {
    throw new Error(
      'maintenance capability public key must be a public Ed25519 signing JWK with kid',
    );
  }
  return JSON.stringify({
    kty: 'OKP',
    crv: 'Ed25519',
    alg: 'EdDSA',
    kid: key.kid,
    x: key.x,
  });
}

export function durableObjectMigrationHistoryDigest(
  migrations: readonly DurableObjectMigration[],
): string {
  return sha256(
    JSON.stringify(
      canonicalDurableObjectMigrationHistory(migrations).map((migration) => ({
        tag: migration.tag,
        newSqliteClasses: migration.newSqliteClasses ?? [],
        newClasses: migration.newClasses ?? [],
        deletedClasses: migration.deletedClasses ?? [],
        renamedClasses: migration.renamedClasses ?? [],
      })),
    ),
  );
}

export function canonicalDurableObjectMigrationHistory(
  migrations: readonly DurableObjectMigration[],
): readonly DurableObjectMigration[] {
  return migrations.map((migration) => {
    const newSqliteClasses = [...(migration.newSqliteClasses ?? [])].sort();
    const newClasses = [...(migration.newClasses ?? [])].sort();
    const deletedClasses = [...(migration.deletedClasses ?? [])].sort();
    const renamedClasses = [...(migration.renamedClasses ?? [])]
      .map((rename) => ({ from: rename.from, to: rename.to }))
      .sort((left, right) =>
        `${left.from}:${left.to}`.localeCompare(`${right.from}:${right.to}`),
      );
    return {
      tag: migration.tag,
      ...(newSqliteClasses.length > 0 ? { newSqliteClasses } : {}),
      ...(newClasses.length > 0 ? { newClasses } : {}),
      ...(deletedClasses.length > 0 ? { deletedClasses } : {}),
      ...(renamedClasses.length > 0 ? { renamedClasses } : {}),
    };
  });
}

export type ExternalResourceIdentity = Pick<
  DeploymentSpec,
  'tenantTag' | 'environment' | 'scriptName' | 'databaseName'
>;

export function externalPlatformResourceGroupId(
  spec: ExternalResourceIdentity,
): string {
  return sha256(
    JSON.stringify({
      tenantTag: spec.tenantTag,
      environment: spec.environment,
      scriptName: spec.scriptName,
      databaseName: spec.databaseName,
    }),
  ).slice(0, 20);
}

function resourceScriptName(
  spec: ExternalResourceIdentity,
  role: 'state' | 'egress',
): string {
  const suffix = `${role}-${externalPlatformResourceGroupId(spec)}`;
  const prefix = spec.scriptName
    .slice(0, 63 - suffix.length - 1)
    .replace(/-+$/u, '');
  return `${prefix}-${suffix}`;
}

export function externalStateScriptName(
  spec: ExternalResourceIdentity,
): string {
  return resourceScriptName(spec, 'state');
}

export function externalEgressProxyScriptName(
  spec: ExternalResourceIdentity,
): string {
  return resourceScriptName(spec, 'egress');
}

export function deploymentEgressPolicyDigest(policy: {
  readonly policyId: string;
  readonly tenantTag: string;
  readonly environment: string;
  readonly allowedHosts: readonly string[];
}): string {
  return sha256(
    JSON.stringify({
      policyId: policy.policyId,
      tenantTag: policy.tenantTag,
      environment: policy.environment,
      allowedHosts: [...new Set(policy.allowedHosts)].sort(),
    }),
  );
}

export function canonicalDeploymentEgressPolicy(policy: {
  readonly policyId: string;
  readonly tenantTag: string;
  readonly environment: string;
  readonly allowedHosts: readonly string[];
}): Readonly<DeploymentEgressPolicy> {
  if (!policy.policyId) throw new Error('egress policyId is required');
  const normalizedPolicyHosts = canonicalEgressHosts(policy.allowedHosts);
  return {
    policyId: policy.policyId,
    policyHosts: normalizedPolicyHosts,
    policyDigest: deploymentEgressPolicyDigest({
      ...policy,
      allowedHosts: normalizedPolicyHosts,
    }),
  };
}

export function defaultDeploymentEgressPolicy(
  spec: DeploymentSpec,
): Readonly<DeploymentEgressPolicy> {
  return canonicalDeploymentEgressPolicy({
    policyId: externalPlatformResourceGroupId(spec),
    tenantTag: spec.tenantTag,
    environment: spec.environment,
    allowedHosts: [],
  });
}

export function assertExternalPlatformTarget(
  actual: ExternalPlatformTargetDescription | undefined,
  expected: ExternalPlatformTargetDescription,
  context: string,
): void {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${context} does not match the persisted platform target`);
  }
}

export function assertExternalPlatformTargetCompatibility(
  current: ExternalPlatformTargetDescription,
  target: ExternalPlatformTargetDescription,
): void {
  if (
    current.maintenanceCapabilityPublicKey !==
    target.maintenanceCapabilityPublicKey
  ) {
    throw new Error(
      'maintenance capability verifier is immutable; rotate it only in a coordinated fleet maintenance window',
    );
  }
  if (
    (current.auditQueueName ?? undefined) !==
    (target.auditQueueName ?? undefined)
  ) {
    throw new Error('trusted platform audit queue is immutable');
  }
  if (
    current.stateEgressCredentialDigest !== target.stateEgressCredentialDigest
  ) {
    throw new Error(
      'state-egress credential digest is immutable; root-secret rotation requires a coordinated credential migration',
    );
  }
}

export function effectiveAppliedPlatformTarget(
  record: FleetRecord,
  target: ExternalPlatformTargetDescription,
): ExternalPlatformTargetDescription {
  return record.platformTarget &&
    record.activeRelease &&
    record.activeRelease.releaseSchemaVersion < record.schemaVersion
    ? {
        ...target,
        d1SchemaVersion: record.platformTarget.d1SchemaVersion,
        d1SchemaHistoryDigest: record.platformTarget.d1SchemaHistoryDigest,
      }
    : target;
}

export function describeExternalPlatformTarget(
  backend: ProvisioningBackend,
  spec: DeploymentSpec,
): ExternalPlatformTargetDescription {
  const target = backend.describeExternalPlatformTarget?.(spec);
  if (!target) {
    throw new Error(
      'external backend cannot describe its trusted platform target',
    );
  }
  if (
    !isSha256(target.stateArtifactDigest) ||
    !isSha256(target.stateDurableObjectHistoryDigest) ||
    (target.stateDurableObjectTag !== undefined &&
      target.stateDurableObjectTag.length === 0) ||
    (target.auditQueueName !== undefined &&
      target.auditQueueName.length === 0) ||
    (target.sharedOutboundWorkerName === undefined
      ? !target.egressArtifactDigest || !isSha256(target.egressArtifactDigest)
      : !target.sharedOutboundWorkerName) ||
    target.d1SchemaVersion !== spec.schemaVersion ||
    !isSha256(target.d1SchemaHistoryDigest)
  ) {
    throw new Error('external backend returned an invalid platform target');
  }
  const policy = canonicalDeploymentEgressPolicy({
    policyId: target.outboundPolicy.policyId,
    tenantTag: spec.tenantTag,
    environment: spec.environment,
    allowedHosts: target.outboundPolicy.policyHosts,
  });
  if (
    policy.policyId !== externalPlatformResourceGroupId(spec) ||
    policy.policyDigest !== target.outboundPolicy.policyDigest ||
    JSON.stringify(policy.policyHosts) !==
      JSON.stringify(target.outboundPolicy.policyHosts)
  ) {
    throw new Error('external backend returned an invalid platform policy');
  }
  return target;
}

export function assertPlatformResourcesMatchTarget(
  resources: ExternalPlatformResources,
  target: ExternalPlatformTargetDescription,
): void {
  const outboundPolicy = resources.outboundPolicy ?? resources.egressProxy;
  if (
    resources.stateWorker.artifactDigest !== target.stateArtifactDigest ||
    resources.maintenanceCapabilityPublicKey !==
      target.maintenanceCapabilityPublicKey ||
    (resources.auditQueueName ?? undefined) !==
      (target.auditQueueName ?? undefined) ||
    (resources.stateWorker.durableObjectTag ?? undefined) !==
      (target.stateDurableObjectTag ?? undefined) ||
    outboundPolicy?.policyId !== target.outboundPolicy.policyId ||
    JSON.stringify(outboundPolicy?.policyHosts) !==
      JSON.stringify(target.outboundPolicy.policyHosts) ||
    outboundPolicy?.policyDigest !== target.outboundPolicy.policyDigest ||
    (target.sharedOutboundWorkerName !== undefined
      ? resources.sharedOutboundWorkerName !==
          target.sharedOutboundWorkerName || resources.egressProxy !== undefined
      : resources.egressProxy?.artifactDigest !== target.egressArtifactDigest)
  ) {
    throw new Error(
      'trusted platform resources do not match the persisted platform target',
    );
  }
}

function validateArtifact(
  artifact: TrustedWorkerArtifact,
  label: string,
): void {
  if (!/^\d{4}-\d{2}-\d{2}$/u.test(artifact.compatibilityDate)) {
    throw new Error(`${label} compatibilityDate must use YYYY-MM-DD`);
  }
  if (artifact.modules.length === 0) {
    throw new Error(`${label} must contain at least one module`);
  }
  const names = new Set<string>();
  for (const module of artifact.modules) {
    if (
      !module.name ||
      module.name.startsWith('/') ||
      module.name.split('/').includes('..')
    ) {
      throw new Error(`${label} contains an invalid module name`);
    }
    if (names.has(module.name)) {
      throw new Error(`${label} contains duplicate module '${module.name}'`);
    }
    names.add(module.name);
  }
  if (!names.has(artifact.mainModule)) {
    throw new Error(`${label} mainModule is absent from its modules`);
  }
}

export function validateExternalPlatformProfile(
  spec: DeploymentSpec,
  profile: ExternalPlatformProfile,
): void {
  if (spec.authoredBy !== 'external') {
    throw new Error('external platform resources require an external release');
  }
  if (profile.runtimeContractVersion !== 1) {
    throw new Error('unsupported trusted platform runtime contract');
  }
  if (profile.backwardCompatibleWithRetainedReleases !== true) {
    throw new Error(
      'trusted platform profile must attest compatibility with retained releases',
    );
  }
  if (
    typeof profile.maintenanceCapabilityPublicKey !== 'string' ||
    canonicalMaintenanceCapabilityPublicKey(
      profile.maintenanceCapabilityPublicKey,
    ) !== profile.maintenanceCapabilityPublicKey
  ) {
    throw new Error('maintenance capability public key must be canonical');
  }
  const privateKey = profile.maintenanceCapabilityPrivateKey;
  if (
    privateKey.kty !== 'OKP' ||
    privateKey.crv !== 'Ed25519' ||
    privateKey.alg !== 'EdDSA' ||
    typeof privateKey.kid !== 'string' ||
    typeof privateKey.x !== 'string' ||
    typeof privateKey.d !== 'string' ||
    !/^[A-Za-z0-9_-]{43}$/u.test(privateKey.d)
  ) {
    throw new Error(
      'maintenance capability private key must contain Ed25519 signing material',
    );
  }
  if (
    canonicalMaintenanceCapabilityPublicKey(
      JSON.stringify({
        kty: privateKey.kty,
        crv: privateKey.crv,
        alg: privateKey.alg,
        kid: privateKey.kid,
        x: privateKey.x,
      }),
    ) !== profile.maintenanceCapabilityPublicKey
  ) {
    throw new Error(
      'maintenance capability private signer does not match its public verifier',
    );
  }
  validateArtifact(profile.stateWorker, 'state Worker artifact');
  if (profile.legacyBridgeWorker) {
    validateArtifact(profile.legacyBridgeWorker, 'legacy bridge artifact');
  }
  if (profile.egressProxyWorker) {
    validateArtifact(profile.egressProxyWorker, 'egress proxy artifact');
  }
  canonicalEgressHosts(profile.organizationEgressHosts);
  if (
    spec.queueProducer &&
    spec.durableObjectBindings.some(
      (binding) =>
        binding.name === FLEET_AUDIT_PROXY_BINDING ||
        binding.name === FLEET_AUDIT_PROXY_STATE_BINDING,
    )
  ) {
    throw new Error('release uses a reserved fleet audit proxy binding');
  }
  const migrationTags = new Set<string>();
  const provisionedClasses = new Set<string>();
  for (const migration of profile.stateDurableObjectMigrations) {
    if (migrationTags.has(migration.tag)) {
      throw new Error(`state migration tag '${migration.tag}' is duplicated`);
    }
    migrationTags.add(migration.tag);
    for (const className of [
      ...(migration.newSqliteClasses ?? []),
      ...(migration.newClasses ?? []),
    ]) {
      provisionedClasses.add(className);
    }
    for (const className of migration.deletedClasses ?? []) {
      provisionedClasses.delete(className);
    }
    for (const rename of migration.renamedClasses ?? []) {
      provisionedClasses.delete(rename.from);
      provisionedClasses.add(rename.to);
    }
  }
  for (const binding of spec.durableObjectBindings) {
    if (!provisionedClasses.has(binding.className)) {
      throw new Error(
        `state profile does not provision Durable Object class '${binding.className}'`,
      );
    }
  }
}
