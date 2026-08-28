// SPDX-License-Identifier: Apache-2.0

import type { VersionCreateParams } from 'cloudflare/resources/workers/scripts/versions';
import { readField, readStringField } from './json-field-reads.js';
import type {
  OrdinaryWorkerDeploymentVersion,
  PlainWorkerUploadIntent,
  PlainWorkerVersionBinding,
  ProviderBindingIdentity,
} from './types.js';

export function providerBindingsToPlainWorkerShape(
  bindings: readonly unknown[],
): readonly PlainWorkerVersionBinding[] {
  return bindings.map((binding) => {
    if (!binding || typeof binding !== 'object' || Array.isArray(binding)) {
      return { type: 'unsupported', name: undefined, issue: 'not-object' };
    }
    const name = readStringField(binding, 'name');
    const rawType = readField(binding, 'type');
    if (
      typeof rawType !== 'string' ||
      rawType.length === 0 ||
      rawType !== rawType.trim()
    ) {
      return {
        type: 'unsupported',
        name,
        providerType: typeof rawType === 'string' ? rawType : undefined,
        issue: 'invalid-type',
      };
    }
    switch (rawType) {
      case 'd1': {
        const id =
          readField(binding, 'id') ?? readField(binding, 'database_id');
        return {
          type: 'd1',
          name,
          databaseId: typeof id === 'string' ? id : undefined,
        };
      }
      case 'durable_object_namespace':
        return {
          type: 'durable-object',
          name,
          className: readStringField(binding, 'class_name'),
          namespaceId: readStringField(binding, 'namespace_id'),
        };
      case 'service':
        return {
          type: 'service',
          name,
          service: readStringField(binding, 'service'),
        };
      case 'queue':
        return {
          type: 'queue-producer',
          name,
          queueName: readStringField(binding, 'queue_name'),
        };
      case 'r2_bucket':
        return {
          type: 'r2-bucket',
          name,
          bucketName: readStringField(binding, 'bucket_name'),
        };
      case 'plain_text':
        return {
          type: 'plain-text',
          name,
          value: readStringField(binding, 'text'),
        };
      case 'secret_text':
        return { type: 'secret-text', name };
      default:
        return {
          type: 'unsupported',
          name,
          providerType: rawType,
          issue: 'unsupported-type',
        };
    }
  });
}

export function assertOrdinaryWorkerDeploymentVersions(
  versions: readonly OrdinaryWorkerDeploymentVersion[],
): void {
  if (versions.length === 0) {
    throw new Error('ordinary Worker deployment requires at least one version');
  }
  const ids = new Set<string>();
  for (const version of versions) {
    if (typeof version.versionId !== 'string') {
      throw new Error('ordinary Worker deployment version ids must be strings');
    }
    if (
      !Number.isFinite(version.percentage) ||
      version.percentage < 0 ||
      version.percentage > 100
    ) {
      throw new Error(
        'ordinary Worker deployment percentages must be finite values from 0 to 100',
      );
    }
    if (ids.has(version.versionId)) {
      throw new Error('ordinary Worker deployment has duplicate version ids');
    }
    ids.add(version.versionId);
  }
}

export function uploadIntentToProviderBindings(
  intent: PlainWorkerUploadIntent,
): NonNullable<VersionCreateParams.Metadata['bindings']> {
  const bindings: NonNullable<VersionCreateParams.Metadata['bindings']> = [];
  for (const { name, value } of intent.bindings.plainText) {
    bindings.push({ name, type: 'plain_text', text: value });
  }
  for (const { name, value } of intent.bindings.secrets) {
    bindings.push({ name, type: 'secret_text', text: value });
  }
  for (const { name, databaseId } of intent.bindings.d1) {
    bindings.push({ name, type: 'd1', database_id: databaseId });
  }
  for (const { name, className } of intent.bindings.durableObjects) {
    bindings.push({
      name,
      type: 'durable_object_namespace',
      class_name: className,
    });
  }
  for (const { name, service } of intent.bindings.services) {
    bindings.push({ name, type: 'service', service });
  }
  for (const { name, queueName } of intent.bindings.queueProducers) {
    bindings.push({ name, type: 'queue', queue_name: queueName });
  }
  for (const { name, bucketName } of intent.bindings.r2Buckets) {
    bindings.push({ name, type: 'r2_bucket', bucket_name: bucketName });
  }
  return bindings;
}

function bindingKey(binding: ProviderBindingIdentity): string {
  return `${binding.type}\u0000${binding.name}`;
}

function hasNonEmptyString(
  candidate: Readonly<Record<string, unknown>>,
  field: string,
): boolean {
  const value = candidate[field];
  return (
    typeof value === 'string' && value.length > 0 && value === value.trim()
  );
}

export function providerBindingIdentity(
  type: string,
  name: string,
): ProviderBindingIdentity {
  return { type, name };
}

export function assertEveryProviderBindingConsumed(
  rawBindings: readonly unknown[],
  consumedBindings: readonly ProviderBindingIdentity[],
  context: string,
): readonly ProviderBindingIdentity[] {
  const identities = rawBindings.map((binding, index) => {
    if (!binding || typeof binding !== 'object' || Array.isArray(binding)) {
      throw new Error(`${context} binding ${index} is not an object`);
    }
    const candidate = binding as Readonly<Record<string, unknown>>;
    if (
      typeof candidate.type !== 'string' ||
      candidate.type.length === 0 ||
      candidate.type !== candidate.type.trim()
    ) {
      throw new Error(`${context} binding ${index} has no valid type`);
    }
    if (
      typeof candidate.name !== 'string' ||
      candidate.name.length === 0 ||
      candidate.name !== candidate.name.trim()
    ) {
      throw new Error(`${context} binding ${index} has no valid name`);
    }
    return providerBindingIdentity(candidate.type, candidate.name);
  });
  const names = new Set<string>();
  for (const identity of identities) {
    if (names.has(identity.name)) {
      throw new Error(`${context} has duplicate provider binding names`);
    }
    names.add(identity.name);
  }
  const actual = identities.map(bindingKey).sort();
  const consumed = consumedBindings.map(bindingKey).sort();
  if (JSON.stringify(actual) !== JSON.stringify(consumed)) {
    throw new Error(
      `${context} has an unsupported or malformed provider binding`,
    );
  }
  return identities.sort((left, right) =>
    bindingKey(left).localeCompare(bindingKey(right)),
  );
}

export function assertSupportedProviderBindings(
  rawBindings: readonly unknown[],
  allowedTypes: ReadonlySet<string>,
  context: string,
): readonly ProviderBindingIdentity[] {
  const consumed = rawBindings.flatMap((binding) => {
    if (!binding || typeof binding !== 'object' || Array.isArray(binding)) {
      return [];
    }
    const candidate = binding as Readonly<Record<string, unknown>>;
    const { name, type } = candidate;
    if (
      typeof name !== 'string' ||
      typeof type !== 'string' ||
      !allowedTypes.has(type)
    ) {
      return [];
    }
    const valid = (() => {
      switch (type) {
        case 'd1':
          return (
            hasNonEmptyString(candidate, 'database_id') ||
            hasNonEmptyString(candidate, 'id')
          );
        case 'durable_object_namespace':
          return (
            hasNonEmptyString(candidate, 'namespace_id') &&
            hasNonEmptyString(candidate, 'class_name')
          );
        case 'service':
          return hasNonEmptyString(candidate, 'service');
        case 'queue':
          return hasNonEmptyString(candidate, 'queue_name');
        case 'kv_namespace':
          return (
            hasNonEmptyString(candidate, 'namespace_id') ||
            hasNonEmptyString(candidate, 'id')
          );
        case 'dispatch_namespace':
          return hasNonEmptyString(candidate, 'namespace');
        case 'r2_bucket':
          return hasNonEmptyString(candidate, 'bucket_name');
        case 'plain_text':
          return typeof candidate.text === 'string';
        case 'secret_text':
          return true;
        default:
          return false;
      }
    })();
    return valid ? [providerBindingIdentity(type, name)] : [];
  });
  return assertEveryProviderBindingConsumed(rawBindings, consumed, context);
}

export function plainWorkerBindingsToProviderShape(
  bindings: readonly PlainWorkerVersionBinding[],
): readonly unknown[] {
  return bindings.map((binding) => {
    switch (binding.type) {
      case 'd1':
        return {
          type: 'd1',
          name: binding.name,
          id: binding.databaseId,
        };
      case 'durable-object':
        return {
          type: 'durable_object_namespace',
          name: binding.name,
          namespace_id: binding.namespaceId,
          class_name: binding.className,
        };
      case 'service':
        return {
          type: 'service',
          name: binding.name,
          service: binding.service,
        };
      case 'queue-producer':
        return {
          type: 'queue',
          name: binding.name,
          queue_name: binding.queueName,
        };
      case 'r2-bucket':
        return {
          type: 'r2_bucket',
          name: binding.name,
          bucket_name: binding.bucketName,
        };
      case 'plain-text':
        return {
          type: 'plain_text',
          name: binding.name,
          text: binding.value,
        };
      case 'secret-text':
        return { type: 'secret_text', name: binding.name };
      case 'unsupported':
        if (binding.issue === 'not-object') return undefined;
        // For `invalid-type` the reconstructed type changes no message (either spelling
        // fails the type check); for `unsupported-type` it preserves the pre-port
        // `unsupported or malformed` refusal instead of an index-based `no valid type`.
        // Carried as a provider fact for adapter diagnostics.
        return { type: binding.providerType, name: binding.name };
    }
    // Exhaustiveness tripwire: a new normalized binding must define wire reconstruction.
    binding satisfies never;
    return undefined;
  });
}

export function assertSupportedPlainWorkerBindings(
  bindings: readonly PlainWorkerVersionBinding[],
  context: string,
): readonly ProviderBindingIdentity[] {
  return assertSupportedProviderBindings(
    plainWorkerBindingsToProviderShape(bindings),
    new Set([
      'd1',
      'durable_object_namespace',
      'service',
      'queue',
      'r2_bucket',
      'plain_text',
      'secret_text',
    ]),
    context,
  );
}

export function assertProviderBindingIdentitiesMatchInspection(
  inspection: Readonly<{
    databaseIds: readonly string[];
    durableObjectBindings: readonly Readonly<{ name: string }>[];
    serviceBindings?: readonly Readonly<{ name: string }>[];
    queueProducerBindings?: readonly Readonly<{ name: string }>[];
    kvNamespaceBindings?: readonly Readonly<{ name: string }>[];
    dispatchNamespaceBindings?: readonly Readonly<{ name: string }>[];
    r2BucketBindings?: readonly Readonly<{ name: string }>[];
    plainTextBindings: Readonly<Record<string, string>>;
    secretNames?: readonly string[];
    providerBindingIdentities?: readonly ProviderBindingIdentity[];
  }>,
  context: string,
): void {
  if (!inspection.providerBindingIdentities) {
    throw new Error(`${context} binding inventory is incomplete`);
  }
  const expected = providerBindingIdentitiesForInspection(inspection);
  assertEveryProviderBindingConsumed(
    inspection.providerBindingIdentities,
    expected,
    context,
  );
}

export function providerBindingIdentitiesForInspection(
  inspection: Readonly<{
    databaseIds: readonly string[];
    durableObjectBindings: readonly Readonly<{ name: string }>[];
    serviceBindings?: readonly Readonly<{ name: string }>[];
    queueProducerBindings?: readonly Readonly<{ name: string }>[];
    kvNamespaceBindings?: readonly Readonly<{ name: string }>[];
    dispatchNamespaceBindings?: readonly Readonly<{ name: string }>[];
    r2BucketBindings?: readonly Readonly<{ name: string }>[];
    plainTextBindings: Readonly<Record<string, string>>;
    secretNames?: readonly string[];
  }>,
): readonly ProviderBindingIdentity[] {
  return [
    ...inspection.databaseIds.map(() => providerBindingIdentity('d1', 'DB')),
    ...inspection.durableObjectBindings.map(({ name }) =>
      providerBindingIdentity('durable_object_namespace', name),
    ),
    ...(inspection.serviceBindings ?? []).map(({ name }) =>
      providerBindingIdentity('service', name),
    ),
    ...(inspection.queueProducerBindings ?? []).map(({ name }) =>
      providerBindingIdentity('queue', name),
    ),
    ...(inspection.kvNamespaceBindings ?? []).map(({ name }) =>
      providerBindingIdentity('kv_namespace', name),
    ),
    ...(inspection.dispatchNamespaceBindings ?? []).map(({ name }) =>
      providerBindingIdentity('dispatch_namespace', name),
    ),
    ...(inspection.r2BucketBindings ?? []).map(({ name }) =>
      providerBindingIdentity('r2_bucket', name),
    ),
    ...Object.keys(inspection.plainTextBindings).map((name) =>
      providerBindingIdentity('plain_text', name),
    ),
    ...(inspection.secretNames ?? []).map((name) =>
      providerBindingIdentity('secret_text', name),
    ),
  ];
}
