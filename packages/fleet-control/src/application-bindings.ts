// SPDX-License-Identifier: Apache-2.0

import { createHash, randomBytes } from 'node:crypto';
import type {
  ApplicationBindingTopology,
  ApplicationR2Binding,
  ApplicationR2Resource,
  DeploymentApplicationBindings,
  DeploymentSecrets,
  DeploymentSpec,
} from './types.js';

const EMPTY_APPLICATION: DeploymentApplicationBindings = Object.freeze({
  vars: [],
  secrets: [],
  r2Buckets: [],
});

export const DEPLOYMENT_PLATFORM_VARIABLE_NAMES = Object.freeze([
  'DEPLOYMENT_TENANT',
  'FLEET_AUDIT_PROXY',
  'FLEET_ENVIRONMENT',
  'FLEET_INGRESS_CONTRACT',
  'FLEET_MAINTENANCE_CAPABILITIES',
  'FLEET_SCHEMA_VERSION',
  'FLEET_SPEC_DIGEST',
]);

export const LEGACY_BRIDGE_PLATFORM_VARIABLE_NAMES = Object.freeze([
  'DEPLOYMENT_TENANT',
  'FLEET_ARTIFACT_DIGEST',
  'FLEET_AUDIT_PROXY_INGRESS',
  'FLEET_AUDIT_PROXY_OBJECT_NAME',
  'FLEET_DEPLOYMENT_SCRIPT',
  'FLEET_DO_TAG',
  'FLEET_ENVIRONMENT',
  'FLEET_MAINTENANCE_CAPABILITIES',
  'FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY',
  'FLEET_RESOURCE_GROUP',
  'FLEET_RESOURCE_ROLE',
  'FLEET_RUNTIME_CONTRACT',
  'FLEET_SCHEMA_VERSION',
  'FLEET_SPEC_DIGEST',
  'OUTBOUND_ENVIRONMENT',
  'OUTBOUND_POLICY_ID',
  'OUTBOUND_RESOURCE_GROUP_ID',
  'OUTBOUND_ROUTE_HOSTNAME',
  'OUTBOUND_STATE_SCRIPT_NAME',
  'OUTBOUND_TENANT_ID',
]);

function byName<T extends { readonly name: string }>(
  values: readonly T[],
): readonly T[] {
  if (
    values.some(
      (value) =>
        !value ||
        typeof value !== 'object' ||
        Array.isArray(value) ||
        typeof value.name !== 'string',
    )
  ) {
    throw new Error('application binding entries must be named objects');
  }
  return [...values]
    .map((value) => ({ ...value }))
    .sort((left, right) =>
      left.name < right.name ? -1 : left.name > right.name ? 1 : 0,
    );
}

export function canonicalApplicationBindings(
  spec: Pick<DeploymentSpec, 'application'>,
): DeploymentApplicationBindings {
  const application: unknown = spec.application ?? EMPTY_APPLICATION;
  if (
    !application ||
    typeof application !== 'object' ||
    Array.isArray(application)
  ) {
    throw new Error('application bindings must be an object');
  }
  if ('kvNamespaces' in application) {
    throw new Error(
      'application KV is unsupported; use an isolated R2 bucket binding',
    );
  }
  const candidate = application as Partial<DeploymentApplicationBindings>;
  for (const [name, value] of Object.entries({
    vars: candidate.vars,
    secrets: candidate.secrets,
    r2Buckets: candidate.r2Buckets,
  })) {
    if (value !== undefined && !Array.isArray(value)) {
      throw new Error(`application ${name} must be an array`);
    }
  }
  return {
    vars: byName(candidate.vars ?? []),
    secrets: byName(candidate.secrets ?? []),
    r2Buckets: byName(candidate.r2Buckets ?? []).map((binding) => ({
      ...binding,
      jurisdiction: binding.jurisdiction ?? 'default',
    })),
  };
}

export function applicationSecretValues(
  spec: Pick<DeploymentSpec, 'application'>,
  secrets: DeploymentSecrets,
): Readonly<Record<string, string>> {
  const descriptors = canonicalApplicationBindings(spec).secrets;
  const values = secrets.application ?? {};
  const actualNames = Object.keys(values).sort();
  const expectedNames = descriptors.map(({ name }) => name);
  if (JSON.stringify(actualNames) !== JSON.stringify(expectedNames)) {
    throw new Error(
      'application secret values must exactly match the declared secret bindings',
    );
  }
  for (const descriptor of descriptors) {
    const value = values[descriptor.name];
    if (typeof value !== 'string' || value.length === 0) {
      throw new Error(`application secret '${descriptor.name}' is empty`);
    }
    if (new TextEncoder().encode(value).byteLength > 5 * 1024) {
      throw new Error(`application secret '${descriptor.name}' exceeds 5 KB`);
    }
    const digest = createHash('sha256').update(value, 'utf8').digest('hex');
    if (digest !== descriptor.valueSha256) {
      throw new Error(
        `application secret '${descriptor.name}' does not match its SHA-256 descriptor`,
      );
    }
  }
  return values;
}

export function applicationR2Bindings(
  spec: Pick<
    DeploymentSpec,
    'tenantTag' | 'environment' | 'scriptName' | 'databaseName' | 'application'
  >,
  resources?: readonly ApplicationR2Resource[],
): readonly ApplicationR2Binding[] {
  const descriptors = canonicalApplicationBindings(spec).r2Buckets;
  if (resources !== undefined) {
    const resolved = [...resources]
      .sort((left, right) =>
        left.name < right.name ? -1 : left.name > right.name ? 1 : 0,
      )
      .map(({ name, bucketName, jurisdiction }) => ({
        name,
        bucketName,
        jurisdiction,
      }));
    if (
      JSON.stringify(
        resolved.map(({ name, jurisdiction }) => ({ name, jurisdiction })),
      ) !==
      JSON.stringify(
        descriptors.map(({ name, jurisdiction = 'default' }) => ({
          name,
          jurisdiction,
        })),
      )
    ) {
      throw new Error(
        'application R2 resources do not match the specification',
      );
    }
    return resolved;
  }
  if (descriptors.length > 0) {
    throw new Error('application R2 resources have not been reserved');
  }
  return [];
}

function reservedBucketName(
  spec: Pick<
    DeploymentSpec,
    'tenantTag' | 'environment' | 'scriptName' | 'databaseName'
  >,
  bindingName: string,
  jurisdiction: string,
  reservationNonce: string,
): string {
  const suffix = createHash('sha256')
    .update(
      JSON.stringify({
        tenantTag: spec.tenantTag,
        environment: spec.environment,
        scriptName: spec.scriptName,
        databaseName: spec.databaseName,
        binding: bindingName,
        jurisdiction,
        reservationNonce,
      }),
    )
    .digest('hex')
    .slice(0, 20);
  const prefix = spec.scriptName
    .slice(0, 63 - suffix.length - 1)
    .replace(/-+$/u, '');
  return `${prefix}-${suffix}`;
}

export function reserveApplicationR2Resources(
  spec: DeploymentSpec,
): readonly ApplicationR2Resource[] {
  return canonicalApplicationBindings(spec).r2Buckets.map((binding) => {
    const jurisdiction = binding.jurisdiction ?? 'default';
    const reservationNonce = randomBytes(24).toString('base64url');
    return {
      name: binding.name,
      bucketName: reservedBucketName(
        spec,
        binding.name,
        jurisdiction,
        reservationNonce,
      ),
      jurisdiction,
      state: 'reserved',
      reservationNonce,
    };
  });
}

export function applicationBindingTopology(
  spec: DeploymentSpec,
  r2Buckets: readonly ApplicationR2Resource[],
): ApplicationBindingTopology {
  const application = canonicalApplicationBindings(spec);
  const expected = applicationR2Bindings(spec, r2Buckets);
  return {
    vars: application.vars,
    secrets: application.secrets,
    r2Buckets: expected,
  };
}

export function applicationSecretNames(
  spec: Pick<DeploymentSpec, 'application'>,
): readonly string[] {
  return canonicalApplicationBindings(spec).secrets.map(({ name }) => name);
}

type ApplicationR2LifecycleBackend = Readonly<{
  findApplicationR2Bucket?: (
    resource: ApplicationR2Resource,
  ) => Promise<import('./types.js').ApplicationR2BucketSnapshot | undefined>;
  ensureApplicationR2Bucket?: (
    resource: ApplicationR2Resource,
    fence: import('./types.js').ExternalMutationFence,
  ) => Promise<import('./types.js').ApplicationR2BucketSnapshot>;
  assertApplicationR2Detached?: (
    resource: ApplicationR2Resource,
    fence: import('./types.js').ExternalMutationFence,
  ) => Promise<void>;
  assertApplicationR2Empty?: (
    resource: ApplicationR2Resource,
    fence: import('./types.js').ExternalMutationFence,
  ) => Promise<void>;
  deleteApplicationR2Bucket?: (
    resource: ApplicationR2Resource,
    fence: import('./types.js').ExternalMutationFence,
  ) => Promise<void>;
}>;

function assertLiveR2Identity(
  resource: ApplicationR2Resource,
  live: import('./types.js').ApplicationR2BucketSnapshot,
): void {
  if (
    live.name !== resource.name ||
    live.bucketName !== resource.bucketName ||
    live.jurisdiction !== resource.jurisdiction ||
    (resource.creationDate !== undefined &&
      live.creationDate !== resource.creationDate)
  ) {
    throw new Error(
      `R2 bucket '${resource.bucketName}' changed its persisted creation identity`,
    );
  }
}

export function assertApplicationR2ReservationIdentity(
  spec: Pick<
    DeploymentSpec,
    'tenantTag' | 'environment' | 'scriptName' | 'databaseName'
  >,
  resource: ApplicationR2Resource,
): void {
  if (
    resource.bucketName !==
    reservedBucketName(
      spec,
      resource.name,
      resource.jurisdiction,
      resource.reservationNonce,
    )
  ) {
    throw new Error(
      `R2 bucket '${resource.bucketName}' does not match its persisted reservation nonce`,
    );
  }
}

export async function convergeApplicationR2Creation(options: {
  readonly spec: Pick<
    DeploymentSpec,
    'tenantTag' | 'environment' | 'scriptName' | 'databaseName'
  >;
  readonly resources: readonly ApplicationR2Resource[];
  readonly backend: ApplicationR2LifecycleBackend;
  readonly fence: import('./types.js').ExternalMutationFence;
  readonly persist: (
    resources: readonly ApplicationR2Resource[],
  ) => Promise<void>;
}): Promise<readonly ApplicationR2Resource[]> {
  if (options.resources.length === 0) return [];
  if (!options.backend.findApplicationR2Bucket) {
    throw new Error('backend cannot inspect application R2 resources');
  }
  if (
    options.resources.some(
      (resource) =>
        resource.state !== 'reserved' &&
        resource.state !== 'create-authorized' &&
        resource.state !== 'created',
    )
  ) {
    throw new Error('application R2 creation has invalid persisted progress');
  }
  let resources = [...options.resources];
  const persistState = async (
    index: number,
    resource: ApplicationR2Resource,
  ): Promise<void> => {
    resources = resources.map((current, currentIndex) =>
      currentIndex === index ? resource : current,
    );
    await options.persist(resources);
  };
  for (let index = 0; index < resources.length; index += 1) {
    let resource = resources[index] as ApplicationR2Resource;
    assertApplicationR2ReservationIdentity(options.spec, resource);
    if (resource.state === 'reserved') {
      if (await options.backend.findApplicationR2Bucket(resource)) {
        throw new Error(
          `refusing to claim pre-existing R2 bucket '${resource.bucketName}'`,
        );
      }
      resource = { ...resource, state: 'create-authorized' };
      await persistState(index, resource);
      await options.fence.assertOwned();
    }
    if (resource.state === 'create-authorized') {
      let live = await options.backend.findApplicationR2Bucket(resource);
      if (!live) {
        if (!options.backend.ensureApplicationR2Bucket) {
          throw new Error('backend cannot create application R2 resources');
        }
        await options.fence.assertOwned();
        try {
          live = await options.backend.ensureApplicationR2Bucket(
            resource,
            options.fence,
          );
        } catch (error) {
          live = await options.backend.findApplicationR2Bucket(resource);
          if (!live) throw error;
        }
      }
      assertLiveR2Identity(resource, live);
      resource = {
        ...resource,
        state: 'created',
        creationDate: live.creationDate,
      };
      await persistState(index, resource);
    }
    const live = await options.backend.findApplicationR2Bucket(resource);
    if (!live) {
      throw new Error(
        `R2 bucket '${resource.bucketName}' is absent after create`,
      );
    }
    assertLiveR2Identity(resource, live);
  }
  return resources;
}

export async function assertApplicationR2EmptyBeforeDecommission(options: {
  readonly resources: readonly ApplicationR2Resource[];
  readonly backend: ApplicationR2LifecycleBackend;
  readonly fence: import('./types.js').ExternalMutationFence;
}): Promise<void> {
  if (options.resources.length === 0) return;
  const findApplicationR2BucketCandidate =
    options.backend.findApplicationR2Bucket;
  if (typeof findApplicationR2BucketCandidate !== 'function') {
    throw new Error('backend cannot preflight application R2 evacuation');
  }
  const findApplicationR2Bucket = findApplicationR2BucketCandidate.bind(
    options.backend,
  );
  const needsEmptyAttestation = options.resources.some(
    (resource) => resource.state !== 'reserved' && resource.state !== 'deleted',
  );
  const assertApplicationR2EmptyCandidate = needsEmptyAttestation
    ? options.backend.assertApplicationR2Empty
    : undefined;
  if (
    needsEmptyAttestation &&
    typeof assertApplicationR2EmptyCandidate !== 'function'
  ) {
    throw new Error('backend cannot preflight application R2 evacuation');
  }
  const assertApplicationR2Empty = assertApplicationR2EmptyCandidate?.bind(
    options.backend,
  );
  for (const resource of options.resources) {
    const live = await findApplicationR2Bucket(resource);
    if (resource.state === 'reserved') {
      if (live) {
        throw new Error(
          `refusing to decommission unauthorized R2 bucket '${resource.bucketName}'`,
        );
      }
      continue;
    }
    if (resource.state === 'deleted') {
      if (live) {
        throw new Error(
          `deleted R2 bucket '${resource.bucketName}' reappeared before decommission`,
        );
      }
      continue;
    }
    if (!live) {
      throw new Error(
        `R2 bucket '${resource.bucketName}' is absent before decommission`,
      );
    }
    assertLiveR2Identity(resource, live);
    await options.fence.assertOwned();
    await assertApplicationR2Empty?.(resource, options.fence);
  }
}

/** @internal Package-private result for one application-R2 deletion step. */
export type ApplicationR2DeletionAdvance =
  | Readonly<{
      status: 'complete';
      resources: readonly ApplicationR2Resource[];
    }>
  | Readonly<{
      status: 'detachment-required';
      resourceIndex: number;
      resource: ApplicationR2Resource & {
        readonly state: 'detach-authorized';
        readonly creationDate: string;
      };
      resources: readonly ApplicationR2Resource[];
    }>
  | Readonly<{
      status: 'resource-advanced';
      resourceIndex: number;
      resources: readonly ApplicationR2Resource[];
    }>;

function replaceApplicationR2Resource(
  resources: readonly ApplicationR2Resource[],
  index: number,
  resource: ApplicationR2Resource,
): readonly ApplicationR2Resource[] {
  return resources.map((current, currentIndex) =>
    currentIndex === index ? resource : current,
  );
}

function invalidApplicationR2DeletionStart(): Error {
  return new Error('application R2 deletion start index is invalid');
}

function invalidApplicationR2DetachmentProof(): Error {
  return new Error('application R2 detachment proof is invalid');
}

/** @internal Advances at most one application-R2 resource state or mutation. */
export async function advanceApplicationR2Deletion(options: {
  readonly spec: Pick<
    DeploymentSpec,
    'tenantTag' | 'environment' | 'scriptName' | 'databaseName'
  >;
  readonly resources: readonly ApplicationR2Resource[];
  readonly backend: ApplicationR2LifecycleBackend;
  readonly fence: import('./types.js').ExternalMutationFence;
  readonly startResourceIndex?: number;
  readonly verifiedDetachmentResourceIndex?: number;
}): Promise<ApplicationR2DeletionAdvance> {
  const resources = [...options.resources];
  const startResourceIndex = options.startResourceIndex ?? 0;
  if (
    !Number.isSafeInteger(startResourceIndex) ||
    startResourceIndex < 0 ||
    startResourceIndex > resources.length
  ) {
    throw invalidApplicationR2DeletionStart();
  }
  const verifiedDetachmentResourceIndex =
    options.verifiedDetachmentResourceIndex;
  if (
    verifiedDetachmentResourceIndex !== undefined &&
    (!Number.isSafeInteger(verifiedDetachmentResourceIndex) ||
      verifiedDetachmentResourceIndex < startResourceIndex ||
      verifiedDetachmentResourceIndex >= resources.length)
  ) {
    throw invalidApplicationR2DetachmentProof();
  }
  let find:
    | NonNullable<ApplicationR2LifecycleBackend['findApplicationR2Bucket']>
    | undefined;
  let findRead = false;
  const requireFind = () => {
    if (!findRead) {
      findRead = true;
      const candidate = options.backend.findApplicationR2Bucket;
      if (typeof candidate === 'function') {
        find = candidate.bind(options.backend);
      }
    }
    if (!find) {
      throw new Error('backend cannot inspect application R2 resources');
    }
    return find;
  };
  let assertEmpty:
    | NonNullable<ApplicationR2LifecycleBackend['assertApplicationR2Empty']>
    | undefined;
  let assertEmptyRead = false;
  const requireAssertEmpty = () => {
    if (!assertEmptyRead) {
      assertEmptyRead = true;
      const candidate = options.backend.assertApplicationR2Empty;
      if (typeof candidate === 'function') {
        assertEmpty = candidate.bind(options.backend);
      }
    }
    if (!assertEmpty) {
      throw new Error('backend cannot attest application R2 emptiness');
    }
    return assertEmpty;
  };
  let deleteBucket:
    | NonNullable<ApplicationR2LifecycleBackend['deleteApplicationR2Bucket']>
    | undefined;
  let deleteBucketRead = false;
  const requireDeleteBucket = () => {
    if (!deleteBucketRead) {
      deleteBucketRead = true;
      const candidate = options.backend.deleteApplicationR2Bucket;
      if (typeof candidate === 'function') {
        deleteBucket = candidate.bind(options.backend);
      }
    }
    if (!deleteBucket) {
      throw new Error('backend cannot delete application R2 resources');
    }
    return deleteBucket;
  };
  if (startResourceIndex === resources.length) {
    return { status: 'complete', resources };
  }

  let index = startResourceIndex;
  for (; index < resources.length; index += 1) {
    const resource = resources[index] as ApplicationR2Resource;
    assertApplicationR2ReservationIdentity(options.spec, resource);
    if (resource.state === 'reserved') {
      if (await requireFind()(resource)) {
        throw new Error(
          `refusing to delete pre-existing R2 bucket '${resource.bucketName}' from an unauthorized reservation`,
        );
      }
      continue;
    }
    if (resource.state === 'deleted') {
      if (await requireFind()(resource)) {
        throw new Error(
          `R2 bucket '${resource.bucketName}' reappeared after deletion`,
        );
      }
      continue;
    }
    break;
  }

  if (index === resources.length) {
    if (verifiedDetachmentResourceIndex !== undefined) {
      throw invalidApplicationR2DetachmentProof();
    }
    return { status: 'complete', resources };
  }

  let resource = resources[index] as ApplicationR2Resource;
  if (
    verifiedDetachmentResourceIndex !== undefined &&
    (verifiedDetachmentResourceIndex !== index ||
      resource.state !== 'detach-authorized')
  ) {
    throw invalidApplicationR2DetachmentProof();
  }

  if (resource.state === 'create-authorized') {
    const live = await requireFind()(resource);
    if (live) assertLiveR2Identity(resource, live);
    resource = live
      ? {
          ...resource,
          state: 'created' as const,
          creationDate: live.creationDate,
        }
      : { ...resource, state: 'delete-authorized' as const };
    return {
      status: 'resource-advanced',
      resourceIndex: index,
      resources: replaceApplicationR2Resource(resources, index, resource),
    };
  }

  if (resource.state === 'created') {
    const live = await requireFind()(resource);
    if (!live) {
      throw new Error(
        `R2 bucket '${resource.bucketName}' disappeared before delete authorization`,
      );
    }
    assertLiveR2Identity(resource, live);
    const detachAuthorized = {
      ...resource,
      state: 'detach-authorized' as const,
      creationDate: resource.creationDate ?? live.creationDate,
    };
    return {
      status: 'detachment-required',
      resourceIndex: index,
      resource: detachAuthorized,
      resources: replaceApplicationR2Resource(
        resources,
        index,
        detachAuthorized,
      ),
    };
  }

  if (resource.state === 'detach-authorized') {
    if (verifiedDetachmentResourceIndex === index) {
      if (resource.creationDate === undefined) {
        throw invalidApplicationR2DetachmentProof();
      }
      return {
        status: 'resource-advanced',
        resourceIndex: index,
        resources: replaceApplicationR2Resource(resources, index, {
          ...resource,
          state: 'detached',
        }),
      };
    }
    const live = await requireFind()(resource);
    if (!live) {
      throw new Error(
        `R2 bucket '${resource.bucketName}' disappeared before delete authorization`,
      );
    }
    assertLiveR2Identity(resource, live);
    const detachAuthorized = {
      ...resource,
      state: 'detach-authorized' as const,
      creationDate: resource.creationDate ?? live.creationDate,
    };
    return {
      status: 'detachment-required',
      resourceIndex: index,
      resource: detachAuthorized,
      resources: replaceApplicationR2Resource(
        resources,
        index,
        detachAuthorized,
      ),
    };
  }

  if (resource.state === 'detached') {
    const live = await requireFind()(resource);
    if (!live) {
      throw new Error(
        `R2 bucket '${resource.bucketName}' disappeared before delete authorization`,
      );
    }
    assertLiveR2Identity(resource, live);
    return {
      status: 'resource-advanced',
      resourceIndex: index,
      resources: replaceApplicationR2Resource(resources, index, {
        ...resource,
        state: 'empty-authorized',
        creationDate: resource.creationDate ?? live.creationDate,
      }),
    };
  }

  if (resource.state === 'empty-authorized') {
    const live = await requireFind()(resource);
    if (!live) {
      throw new Error(
        `R2 bucket '${resource.bucketName}' disappeared before delete authorization`,
      );
    }
    assertLiveR2Identity(resource, live);
    await options.fence.assertOwned();
    await requireAssertEmpty()(resource, options.fence);
    return {
      status: 'resource-advanced',
      resourceIndex: index,
      resources: replaceApplicationR2Resource(resources, index, {
        ...resource,
        state: 'empty',
        creationDate: resource.creationDate ?? live.creationDate,
      }),
    };
  }

  if (resource.state === 'empty') {
    return {
      status: 'resource-advanced',
      resourceIndex: index,
      resources: replaceApplicationR2Resource(resources, index, {
        ...resource,
        state: 'delete-authorized',
      }),
    };
  }

  if (resource.state !== 'delete-authorized') {
    throw new Error('application R2 deletion has invalid persisted progress');
  }

  const findBucket = requireFind();
  const live = await findBucket(resource);
  if (!live) {
    return {
      status: 'resource-advanced',
      resourceIndex: index,
      resources: replaceApplicationR2Resource(resources, index, {
        ...resource,
        state: 'deleted',
      }),
    };
  }
  assertLiveR2Identity(resource, live);
  if (resource.creationDate === undefined) {
    return {
      status: 'resource-advanced',
      resourceIndex: index,
      resources: replaceApplicationR2Resource(resources, index, {
        ...resource,
        creationDate: live.creationDate,
      }),
    };
  }
  await options.fence.assertOwned();
  try {
    await requireDeleteBucket()(resource, options.fence);
  } catch (error) {
    if (await findBucket(resource)) throw error;
    return {
      status: 'resource-advanced',
      resourceIndex: index,
      resources: replaceApplicationR2Resource(resources, index, {
        ...resource,
        state: 'deleted',
      }),
    };
  }
  if (await findBucket(resource)) {
    throw new Error(
      `R2 bucket '${resource.bucketName}' remains after deletion`,
    );
  }
  return {
    status: 'resource-advanced',
    resourceIndex: index,
    resources: replaceApplicationR2Resource(resources, index, {
      ...resource,
      state: 'deleted',
    }),
  };
}

export async function convergeApplicationR2Deletion(options: {
  readonly spec: Pick<
    DeploymentSpec,
    'tenantTag' | 'environment' | 'scriptName' | 'databaseName'
  >;
  readonly resources: readonly ApplicationR2Resource[];
  readonly backend: ApplicationR2LifecycleBackend;
  readonly fence: import('./types.js').ExternalMutationFence;
  readonly persist: (
    resources: readonly ApplicationR2Resource[],
  ) => Promise<void>;
}): Promise<readonly ApplicationR2Resource[]> {
  if (options.resources.length === 0) return [];
  const findApplicationR2BucketCandidate =
    options.backend.findApplicationR2Bucket;
  const assertApplicationR2DetachedCandidate =
    options.backend.assertApplicationR2Detached;
  const assertApplicationR2EmptyCandidate =
    options.backend.assertApplicationR2Empty;
  const deleteApplicationR2BucketCandidate =
    options.backend.deleteApplicationR2Bucket;
  if (
    typeof findApplicationR2BucketCandidate !== 'function' ||
    typeof assertApplicationR2DetachedCandidate !== 'function' ||
    typeof assertApplicationR2EmptyCandidate !== 'function' ||
    typeof deleteApplicationR2BucketCandidate !== 'function'
  ) {
    throw new Error('backend cannot safely delete application R2 resources');
  }
  const backend = {
    findApplicationR2Bucket: findApplicationR2BucketCandidate.bind(
      options.backend,
    ),
    assertApplicationR2Detached: assertApplicationR2DetachedCandidate.bind(
      options.backend,
    ),
    assertApplicationR2Empty: assertApplicationR2EmptyCandidate.bind(
      options.backend,
    ),
    deleteApplicationR2Bucket: deleteApplicationR2BucketCandidate.bind(
      options.backend,
    ),
  };
  let resources = [...options.resources];
  let startResourceIndex = 0;
  for (;;) {
    const result = await advanceApplicationR2Deletion({
      spec: options.spec,
      resources,
      backend,
      fence: options.fence,
      startResourceIndex,
    });
    if (result.status === 'complete') return result.resources;

    if (result.status === 'detachment-required') {
      if (
        result.resources[result.resourceIndex] !==
        resources[result.resourceIndex]
      ) {
        resources = [...result.resources];
        await options.persist(resources);
      } else {
        resources = [...result.resources];
      }
      await options.fence.assertOwned();
      await backend.assertApplicationR2Detached(result.resource, options.fence);
      startResourceIndex = result.resourceIndex;
      const verified = await advanceApplicationR2Deletion({
        spec: options.spec,
        resources,
        backend,
        fence: options.fence,
        startResourceIndex,
        verifiedDetachmentResourceIndex: result.resourceIndex,
      });
      if (
        verified.status !== 'resource-advanced' ||
        verified.resourceIndex !== result.resourceIndex ||
        verified.resources[result.resourceIndex]?.state !== 'detached'
      ) {
        throw invalidApplicationR2DetachmentProof();
      }
      resources = [...verified.resources];
      await options.persist(resources);
      startResourceIndex = verified.resourceIndex;
      continue;
    }

    resources = [...result.resources];
    await options.persist(resources);
    if (resources[result.resourceIndex]?.state === 'deleted') {
      startResourceIndex = result.resourceIndex + 1;
    } else {
      startResourceIndex = result.resourceIndex;
    }
  }
}

function comparableR2Bindings(
  bindings: readonly ApplicationR2Binding[],
): readonly Readonly<{ name: string; bucketName: string }>[] {
  return bindings
    .map(({ name, bucketName }) => ({ name, bucketName }))
    .sort((left, right) => left.name.localeCompare(right.name));
}

export function liveApplicationTopologyMatches(
  expected: ApplicationBindingTopology | undefined,
  live: Readonly<{
    plainTextBindings: Readonly<Record<string, string>>;
    r2BucketBindings?: readonly ApplicationR2Binding[];
  }>,
  fixedPlatformVariableNames: readonly string[],
): boolean {
  const fixedNames = new Set(fixedPlatformVariableNames);
  const expectedVariables = [...(expected?.vars ?? [])].sort((left, right) =>
    left.name.localeCompare(right.name),
  );
  const liveVariables = Object.entries(live.plainTextBindings)
    .filter(([name]) => !fixedNames.has(name))
    .map(([name, value]) => ({ name, value }))
    .sort((left, right) => left.name.localeCompare(right.name));
  return (
    JSON.stringify(liveVariables) === JSON.stringify(expectedVariables) &&
    JSON.stringify(comparableR2Bindings(live.r2BucketBindings ?? [])) ===
      JSON.stringify(comparableR2Bindings(expected?.r2Buckets ?? []))
  );
}
