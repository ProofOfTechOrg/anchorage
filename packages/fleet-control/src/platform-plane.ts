// SPDX-License-Identifier: Apache-2.0

import { isDeploymentScriptName } from './deployment-context.js';
import {
  canonicalMaintenanceCapabilityPublicKey,
  trustedArtifactDigest,
} from './platform-resources.js';
import { assertProviderBindingIdentitiesMatchInspection } from './provider-binding-inventory.js';
import type {
  ExternalMutationFence,
  PlatformPlaneLease,
  PlatformPlaneStateStore,
  ProviderBindingIdentity,
  WorkerModule,
  WorkerZoneRoute,
} from './types.js';

const PLATFORM_PLANE_IDENTITY = /^[a-z0-9][a-z0-9:_-]{0,127}$/u;
const PRIVATE_BOOTSTRAP_MARKER = 'deny-all-v1';
const PRIVATE_BOOTSTRAP_MODULE = '__anchorage_private_bootstrap__.js';

type PlatformWorkerRole =
  | 'shared-dispatch'
  | 'shared-outbound'
  | 'shared-audit';

interface PlatformWorkerInspection {
  readonly artifactVersion: string;
  readonly databaseIds: readonly string[];
  readonly durableObjectBindings: readonly Readonly<{ name: string }>[];
  readonly serviceBindings: readonly Readonly<{
    name: string;
    service: string;
  }>[];
  readonly queueProducerBindings: readonly Readonly<{
    name: string;
    queueName: string;
  }>[];
  readonly kvNamespaceBindings: readonly Readonly<{
    name: string;
    namespaceId: string;
  }>[];
  readonly dispatchNamespaceBindings: readonly Readonly<{
    name: string;
    namespace: string;
    outbound: unknown;
  }>[];
  readonly r2BucketBindings?: readonly Readonly<{ name: string }>[];
  readonly secretNames?: readonly string[];
  readonly plainTextBindings: Readonly<Record<string, string>>;
  readonly providerBindingIdentities: readonly ProviderBindingIdentity[];
  readonly workersDevEnabled: boolean;
  readonly previewUrlsEnabled: boolean;
  readonly routeHostnames: readonly string[];
  readonly zoneRoutes: readonly WorkerZoneRoute[];
}

export interface PlatformPlaneClient {
  platformPlaneScope(): Readonly<{
    accountId: string;
    dispatchNamespace: string;
  }>;
  withMutationFence<T>(
    fence: ExternalMutationFence,
    operation: () => Promise<T>,
  ): Promise<T>;
  ensureDispatchNamespace(): Promise<void>;
  assertUntrustedDispatchNamespace(): Promise<void>;
  uploadControlWorker(spec: {
    readonly scriptName: string;
    readonly mainModule: string;
    readonly modules: readonly WorkerModule[];
    readonly compatibilityDate: string;
    readonly bindings: readonly Readonly<Record<string, unknown>>[];
  }): Promise<string>;
  inspectControlWorker(
    scriptName: string,
  ): Promise<PlatformWorkerInspection | undefined>;
  disableControlWorkerPublicAccess(scriptName: string): Promise<void>;
  putControlSecrets(
    scriptName: string,
    secrets: Readonly<Record<string, string>>,
  ): Promise<void>;
  ensureQueueConsumer(options: {
    readonly queueName: string;
    readonly scriptName: string;
    readonly deadLetterQueue?: string;
  }): Promise<void>;
}

export interface PlatformPlaneSpec {
  readonly platformPlaneIdentity: string;
  readonly dispatchNamespace: string;
  readonly compatibilityDate: string;
  readonly hostRoutingKvId: string;
  readonly tenantCpuLimitMs: number;
  readonly tenantSubrequestLimit: number;
  readonly auditQueueName: string;
  readonly maintenanceCapabilityPublicKey: string;
  readonly auditDeadLetterQueue?: string;
  readonly siemEndpoint: string;
  readonly siemAuthHeader?: string;
  readonly dispatchWorker: Readonly<{
    scriptName: string;
    mainModule: string;
    modules: readonly WorkerModule[];
  }>;
  readonly outboundWorker: Readonly<{
    scriptName: string;
    mainModule: string;
    modules: readonly WorkerModule[];
  }>;
  readonly auditWorker: Readonly<{
    scriptName: string;
    mainModule: string;
    modules: readonly WorkerModule[];
  }>;
}

export interface PlatformPlaneResult {
  readonly platformPlaneIdentity: string;
  readonly dispatchArtifactVersion: string;
  readonly outboundArtifactVersion: string;
  readonly auditArtifactVersion: string;
}

function positiveInteger(value: number, name: string): void {
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
}

function workerArtifactDigest(
  worker: PlatformPlaneSpec['dispatchWorker'],
  compatibilityDate: string,
): string {
  return trustedArtifactDigest({ ...worker, compatibilityDate });
}

function workerIdentityBindings(options: {
  readonly artifactDigest: string;
  readonly platformPlaneIdentity: string;
  readonly role: PlatformWorkerRole;
}): readonly Readonly<Record<string, unknown>>[] {
  return [
    {
      name: 'FLEET_PLATFORM_PLANE_ID',
      type: 'plain_text',
      text: options.platformPlaneIdentity,
    },
    {
      name: 'FLEET_RESOURCE_ROLE',
      type: 'plain_text',
      text: options.role,
    },
    {
      name: 'FLEET_ARTIFACT_DIGEST',
      type: 'plain_text',
      text: options.artifactDigest,
    },
  ];
}

function assertWorkerOwner(options: {
  readonly inspection: PlatformWorkerInspection | undefined;
  readonly platformPlaneIdentity: string;
  readonly role: PlatformWorkerRole;
  readonly scriptName: string;
}): void {
  if (!options.inspection) return;
  assertProviderBindingIdentitiesMatchInspection(
    options.inspection,
    `control Worker '${options.scriptName}'`,
  );
  const bindings = options.inspection.plainTextBindings;
  if (
    bindings.FLEET_PLATFORM_PLANE_ID !== options.platformPlaneIdentity ||
    bindings.FLEET_RESOURCE_ROLE !== options.role
  ) {
    throw new Error(
      `control Worker '${options.scriptName}' is not owned by platform plane '${options.platformPlaneIdentity}' as role '${options.role}'`,
    );
  }
}

function canonicalValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value
      .map(canonicalValue)
      .sort((left, right) =>
        JSON.stringify(left).localeCompare(JSON.stringify(right)),
      );
  }
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value as Readonly<Record<string, unknown>>)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, item]) => [key, canonicalValue(item)]),
    );
  }
  return value;
}

function canonicalBindingSet(value: readonly unknown[]): string {
  return JSON.stringify(canonicalValue(value));
}

function canonicalPlainTextBindings(
  value: Readonly<Record<string, string>>,
): string {
  return JSON.stringify(
    Object.entries(value).sort(([left], [right]) => left.localeCompare(right)),
  );
}

function privateBootstrapBindings(options: {
  readonly platformPlaneIdentity: string;
  readonly role: PlatformWorkerRole;
}): readonly Readonly<Record<string, unknown>>[] {
  return [
    {
      name: 'FLEET_PLATFORM_PLANE_ID',
      type: 'plain_text',
      text: options.platformPlaneIdentity,
    },
    {
      name: 'FLEET_RESOURCE_ROLE',
      type: 'plain_text',
      text: options.role,
    },
    {
      name: 'FLEET_PRIVATE_BOOTSTRAP',
      type: 'plain_text',
      text: PRIVATE_BOOTSTRAP_MARKER,
    },
  ];
}

function isExactPrivateBootstrap(
  inspection: PlatformWorkerInspection | undefined,
  options: {
    readonly platformPlaneIdentity: string;
    readonly role: PlatformWorkerRole;
    readonly scriptName: string;
  },
): inspection is PlatformWorkerInspection {
  if (!inspection) return false;
  assertProviderBindingIdentitiesMatchInspection(
    inspection,
    `control Worker '${options.scriptName}'`,
  );
  const expectedPlainText = Object.fromEntries(
    privateBootstrapBindings(options).map((binding) => [
      String(binding.name),
      String(binding.text),
    ]),
  );
  return (
    inspection.databaseIds.length === 0 &&
    inspection.durableObjectBindings.length === 0 &&
    inspection.serviceBindings.length === 0 &&
    inspection.queueProducerBindings.length === 0 &&
    inspection.kvNamespaceBindings.length === 0 &&
    inspection.dispatchNamespaceBindings.length === 0 &&
    canonicalPlainTextBindings(inspection.plainTextBindings) ===
      canonicalPlainTextBindings(expectedPlainText)
  );
}

async function preparePrivateWorker(options: {
  readonly client: PlatformPlaneClient;
  readonly compatibilityDate: string;
  readonly lease: PlatformPlaneLease;
  readonly platformPlaneIdentity: string;
  readonly role: PlatformWorkerRole;
  readonly scriptName: string;
}): Promise<void> {
  let inspection = await options.client.inspectControlWorker(
    options.scriptName,
  );
  if (!inspection) {
    try {
      await options.lease.assertOwned();
      await options.client.uploadControlWorker({
        scriptName: options.scriptName,
        mainModule: PRIVATE_BOOTSTRAP_MODULE,
        modules: [
          {
            name: PRIVATE_BOOTSTRAP_MODULE,
            content:
              'export default {fetch(){return new Response(null,{status:503})}}',
          },
        ],
        compatibilityDate: options.compatibilityDate,
        bindings: privateBootstrapBindings(options),
      });
    } catch (cause) {
      inspection = await options.client.inspectControlWorker(
        options.scriptName,
      );
      if (!isExactPrivateBootstrap(inspection, options)) throw cause;
    }
    inspection = await options.client.inspectControlWorker(options.scriptName);
    if (!isExactPrivateBootstrap(inspection, options)) {
      throw new Error(
        `private control Worker '${options.scriptName}' bootstrap did not converge exactly`,
      );
    }
  } else if (!isExactPrivateBootstrap(inspection, options)) {
    assertWorkerOwner({ ...options, inspection });
  }
  await options.lease.assertOwned();
  await options.client.disableControlWorkerPublicAccess(options.scriptName);
  inspection = await options.client.inspectControlWorker(options.scriptName);
  if (!inspection) {
    throw new Error(
      `private control Worker '${options.scriptName}' disappeared during privatization`,
    );
  }
  if (inspection.plainTextBindings.FLEET_PRIVATE_BOOTSTRAP) {
    if (!isExactPrivateBootstrap(inspection, options)) {
      throw new Error(
        `private control Worker '${options.scriptName}' bootstrap drifted before real upload`,
      );
    }
  } else {
    assertWorkerOwner({ ...options, inspection });
  }
  if (
    inspection.workersDevEnabled ||
    inspection.previewUrlsEnabled ||
    inspection.routeHostnames.length > 0 ||
    inspection.zoneRoutes.length > 0
  ) {
    throw new Error(
      `private control Worker '${options.scriptName}' remains publicly routable before real upload`,
    );
  }
}

function platformWorkerBindings(
  spec: PlatformPlaneSpec,
  role: PlatformWorkerRole,
): readonly Readonly<Record<string, unknown>>[] {
  if (role === 'shared-outbound') {
    return [
      {
        name: 'HOSTS',
        type: 'kv_namespace',
        namespace_id: spec.hostRoutingKvId,
      },
    ];
  }
  if (role === 'shared-audit') {
    return [
      {
        name: 'SIEM_ENDPOINT',
        type: 'plain_text',
        text: spec.siemEndpoint,
      },
    ];
  }
  return [
    {
      name: 'DISPATCH',
      type: 'dispatch_namespace',
      namespace: spec.dispatchNamespace,
      outbound: {
        worker: { service: spec.outboundWorker.scriptName },
        params: [
          { name: 'scriptName' },
          { name: 'tenantTag' },
          { name: 'environment' },
          { name: 'policyId' },
          { name: 'policyDigest' },
          { name: 'policyHosts' },
        ],
      },
    },
    {
      name: 'HOSTS',
      type: 'kv_namespace',
      namespace_id: spec.hostRoutingKvId,
    },
    {
      name: 'TENANT_CPU_LIMIT_MS',
      type: 'plain_text',
      text: String(spec.tenantCpuLimitMs),
    },
    {
      name: 'TENANT_SUBREQUEST_LIMIT',
      type: 'plain_text',
      text: String(spec.tenantSubrequestLimit),
    },
    {
      name: 'FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY',
      type: 'plain_text',
      text: spec.maintenanceCapabilityPublicKey,
    },
  ];
}

function assertWorkerAttestation(options: {
  readonly artifactDigest: string;
  readonly bindings: readonly Readonly<Record<string, unknown>>[];
  readonly inspection: PlatformWorkerInspection | undefined;
  readonly platformPlaneIdentity: string;
  readonly privateWorker: boolean;
  readonly role: PlatformWorkerRole;
  readonly scriptName: string;
}): void {
  if (!options.inspection) {
    throw new Error(
      `control Worker '${options.scriptName}' disappeared after upload`,
    );
  }
  assertWorkerOwner(options);
  if (
    options.inspection.plainTextBindings.FLEET_ARTIFACT_DIGEST !==
    options.artifactDigest
  ) {
    throw new Error(
      `control Worker '${options.scriptName}' does not attest the uploaded artifact`,
    );
  }
  if (
    options.privateWorker &&
    (options.inspection.workersDevEnabled ||
      options.inspection.previewUrlsEnabled ||
      options.inspection.routeHostnames.length > 0 ||
      options.inspection.zoneRoutes.length > 0)
  ) {
    throw new Error(
      `control Worker '${options.scriptName}' remains publicly routable`,
    );
  }
  const expectedBindings = [
    ...workerIdentityBindings(options),
    ...options.bindings,
  ];
  const expectedPlainTextBindings = Object.fromEntries(
    expectedBindings.flatMap((binding) =>
      binding.type === 'plain_text'
        ? [[String(binding.name), String(binding.text)] as const]
        : [],
    ),
  );
  const expectedServiceBindings = expectedBindings.flatMap((binding) =>
    binding.type === 'service'
      ? [{ name: String(binding.name), service: String(binding.service) }]
      : [],
  );
  const expectedQueueBindings = expectedBindings.flatMap((binding) =>
    binding.type === 'queue'
      ? [{ name: String(binding.name), queueName: String(binding.queue_name) }]
      : [],
  );
  const expectedKvBindings = expectedBindings.flatMap((binding) =>
    binding.type === 'kv_namespace'
      ? [
          {
            name: String(binding.name),
            namespaceId: String(binding.namespace_id),
          },
        ]
      : [],
  );
  const expectedDispatchBindings = expectedBindings.flatMap((binding) =>
    binding.type === 'dispatch_namespace'
      ? [
          {
            name: String(binding.name),
            namespace: String(binding.namespace),
            outbound: binding.outbound,
          },
        ]
      : [],
  );
  if (
    options.inspection.databaseIds.length !== 0 ||
    options.inspection.durableObjectBindings.length !== 0 ||
    canonicalPlainTextBindings(options.inspection.plainTextBindings) !==
      canonicalPlainTextBindings(expectedPlainTextBindings) ||
    canonicalBindingSet(options.inspection.serviceBindings) !==
      canonicalBindingSet(expectedServiceBindings) ||
    canonicalBindingSet(options.inspection.queueProducerBindings) !==
      canonicalBindingSet(expectedQueueBindings) ||
    canonicalBindingSet(options.inspection.kvNamespaceBindings) !==
      canonicalBindingSet(expectedKvBindings) ||
    canonicalBindingSet(options.inspection.dispatchNamespaceBindings) !==
      canonicalBindingSet(expectedDispatchBindings)
  ) {
    throw new Error(
      `control Worker '${options.scriptName}' has drifted role bindings`,
    );
  }
}

async function uploadAndAttestWorker(options: {
  readonly artifactDigest: string;
  readonly bindings: readonly Readonly<Record<string, unknown>>[];
  readonly client: PlatformPlaneClient;
  readonly compatibilityDate: string;
  readonly platformPlaneIdentity: string;
  readonly lease: PlatformPlaneLease;
  readonly privateWorker: boolean;
  readonly role: PlatformWorkerRole;
  readonly worker: PlatformPlaneSpec['dispatchWorker'];
}): Promise<string> {
  if (options.privateWorker) {
    await preparePrivateWorker({
      ...options,
      scriptName: options.worker.scriptName,
    });
  }
  await options.lease.assertOwned();
  const artifactVersion = await options.client.uploadControlWorker({
    ...options.worker,
    compatibilityDate: options.compatibilityDate,
    bindings: [...workerIdentityBindings(options), ...options.bindings],
  });
  if (options.privateWorker) {
    await options.lease.assertOwned();
    await options.client.disableControlWorkerPublicAccess(
      options.worker.scriptName,
    );
  }
  const inspection = await options.client.inspectControlWorker(
    options.worker.scriptName,
  );
  assertWorkerAttestation({
    ...options,
    inspection,
    scriptName: options.worker.scriptName,
  });
  return artifactVersion;
}

export async function provisionPlatformPlane(options: {
  readonly client: PlatformPlaneClient;
  readonly spec: PlatformPlaneSpec;
  readonly store: PlatformPlaneStateStore;
}): Promise<PlatformPlaneResult> {
  const { client, spec, store } = options;
  positiveInteger(spec.tenantCpuLimitMs, 'tenantCpuLimitMs');
  positiveInteger(spec.tenantSubrequestLimit, 'tenantSubrequestLimit');
  if (!spec.hostRoutingKvId || !spec.auditQueueName) {
    throw new Error('hostRoutingKvId and auditQueueName are required');
  }
  if (spec.auditDeadLetterQueue === spec.auditQueueName) {
    throw new Error('auditQueueName and auditDeadLetterQueue must be distinct');
  }
  if (!PLATFORM_PLANE_IDENTITY.test(spec.platformPlaneIdentity)) {
    throw new Error('platformPlaneIdentity is invalid');
  }
  if (
    canonicalMaintenanceCapabilityPublicKey(
      spec.maintenanceCapabilityPublicKey,
    ) !== spec.maintenanceCapabilityPublicKey
  ) {
    throw new Error('maintenance capability public key must be canonical');
  }
  const workers = [spec.dispatchWorker, spec.outboundWorker, spec.auditWorker];
  for (const worker of workers) {
    if (!isDeploymentScriptName(worker.scriptName)) {
      throw new Error(`control Worker name '${worker.scriptName}' is invalid`);
    }
  }
  if (new Set(workers.map((worker) => worker.scriptName)).size !== 3) {
    throw new Error('dispatch, outbound, and audit Workers must be distinct');
  }
  const endpoint = new URL(spec.siemEndpoint);
  if (endpoint.protocol !== 'https:') {
    throw new Error('siemEndpoint must use HTTPS');
  }

  const scope = client.platformPlaneScope();
  if (scope.dispatchNamespace !== spec.dispatchNamespace) {
    throw new Error(
      `platform plane dispatch namespace '${spec.dispatchNamespace}' does not match client namespace '${scope.dispatchNamespace}'`,
    );
  }

  const outboundArtifactDigest = workerArtifactDigest(
    spec.outboundWorker,
    spec.compatibilityDate,
  );
  const auditArtifactDigest = workerArtifactDigest(
    spec.auditWorker,
    spec.compatibilityDate,
  );
  const dispatchArtifactDigest = workerArtifactDigest(
    spec.dispatchWorker,
    spec.compatibilityDate,
  );
  const dispatchBindings = platformWorkerBindings(spec, 'shared-dispatch');
  const outboundBindings = platformWorkerBindings(spec, 'shared-outbound');
  const auditBindings = platformWorkerBindings(spec, 'shared-audit');
  return store.withPlatformPlaneLease(
    {
      accountId: scope.accountId,
      dispatchNamespace: scope.dispatchNamespace,
      dispatchScriptName: spec.dispatchWorker.scriptName,
      outboundScriptName: spec.outboundWorker.scriptName,
      auditScriptName: spec.auditWorker.scriptName,
      hostRoutingKvId: spec.hostRoutingKvId,
      auditQueueName: spec.auditQueueName,
      maintenanceCapabilityPublicKey: spec.maintenanceCapabilityPublicKey,
      ...(spec.auditDeadLetterQueue
        ? { auditDeadLetterQueue: spec.auditDeadLetterQueue }
        : {}),
    },
    spec.platformPlaneIdentity,
    async (lease) =>
      client.withMutationFence(lease, async () => {
        const existingWorkers = await Promise.all(
          workers.map((worker) =>
            client.inspectControlWorker(worker.scriptName),
          ),
        );
        assertWorkerOwner({
          inspection: existingWorkers[0],
          platformPlaneIdentity: spec.platformPlaneIdentity,
          role: 'shared-dispatch',
          scriptName: spec.dispatchWorker.scriptName,
        });
        if (
          existingWorkers[0] &&
          existingWorkers[0].plainTextBindings
            .FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY !==
            spec.maintenanceCapabilityPublicKey
        ) {
          throw new Error(
            'dispatcher maintenance capability verifier is immutable; use a coordinated fleet maintenance window for key rotation',
          );
        }
        assertWorkerOwner({
          inspection: existingWorkers[1],
          platformPlaneIdentity: spec.platformPlaneIdentity,
          role: 'shared-outbound',
          scriptName: spec.outboundWorker.scriptName,
        });
        assertWorkerOwner({
          inspection: existingWorkers[2],
          platformPlaneIdentity: spec.platformPlaneIdentity,
          role: 'shared-audit',
          scriptName: spec.auditWorker.scriptName,
        });

        await lease.assertOwned();
        await client.ensureDispatchNamespace();
        await uploadAndAttestWorker({
          artifactDigest: outboundArtifactDigest,
          bindings: outboundBindings,
          client,
          compatibilityDate: spec.compatibilityDate,
          lease,
          platformPlaneIdentity: spec.platformPlaneIdentity,
          privateWorker: true,
          role: 'shared-outbound',
          worker: spec.outboundWorker,
        });
        await lease.assertOwned();
        await client.putControlSecrets(spec.outboundWorker.scriptName, {});
        await uploadAndAttestWorker({
          artifactDigest: auditArtifactDigest,
          bindings: auditBindings,
          client,
          compatibilityDate: spec.compatibilityDate,
          lease,
          platformPlaneIdentity: spec.platformPlaneIdentity,
          privateWorker: true,
          role: 'shared-audit',
          worker: spec.auditWorker,
        });
        await lease.assertOwned();
        await client.putControlSecrets(
          spec.auditWorker.scriptName,
          spec.siemAuthHeader ? { SIEM_AUTH_HEADER: spec.siemAuthHeader } : {},
        );
        await lease.assertOwned();
        await client.ensureQueueConsumer({
          queueName: spec.auditQueueName,
          scriptName: spec.auditWorker.scriptName,
          deadLetterQueue: spec.auditDeadLetterQueue,
        });
        await uploadAndAttestWorker({
          artifactDigest: dispatchArtifactDigest,
          bindings: dispatchBindings,
          client,
          compatibilityDate: spec.compatibilityDate,
          lease,
          platformPlaneIdentity: spec.platformPlaneIdentity,
          privateWorker: false,
          role: 'shared-dispatch',
          worker: spec.dispatchWorker,
        });
        await lease.assertOwned();
        await client.putControlSecrets(spec.dispatchWorker.scriptName, {});

        await client.assertUntrustedDispatchNamespace();
        const finalWorkers = await Promise.all(
          workers.map((worker) =>
            client.inspectControlWorker(worker.scriptName),
          ),
        );
        const [finalDispatch, finalOutbound, finalAudit] = finalWorkers;
        if (!finalDispatch || !finalOutbound || !finalAudit) {
          throw new Error('platform plane resource group is incomplete');
        }
        const finalAttestations = [
          {
            artifactDigest: dispatchArtifactDigest,
            bindings: dispatchBindings,
            inspection: finalDispatch,
            privateWorker: false,
            role: 'shared-dispatch' as const,
            scriptName: spec.dispatchWorker.scriptName,
          },
          {
            artifactDigest: outboundArtifactDigest,
            bindings: outboundBindings,
            inspection: finalOutbound,
            privateWorker: true,
            role: 'shared-outbound' as const,
            scriptName: spec.outboundWorker.scriptName,
          },
          {
            artifactDigest: auditArtifactDigest,
            bindings: auditBindings,
            inspection: finalAudit,
            privateWorker: true,
            role: 'shared-audit' as const,
            scriptName: spec.auditWorker.scriptName,
          },
        ];
        for (const attestation of finalAttestations) {
          assertWorkerAttestation({
            ...attestation,
            platformPlaneIdentity: spec.platformPlaneIdentity,
          });
        }
        await lease.assertOwned();
        await client.ensureQueueConsumer({
          queueName: spec.auditQueueName,
          scriptName: spec.auditWorker.scriptName,
          deadLetterQueue: spec.auditDeadLetterQueue,
        });
        await lease.assertOwned();
        return {
          platformPlaneIdentity: spec.platformPlaneIdentity,
          dispatchArtifactVersion: finalDispatch.artifactVersion,
          outboundArtifactVersion: finalOutbound.artifactVersion,
          auditArtifactVersion: finalAudit.artifactVersion,
        };
      }),
  );
}
