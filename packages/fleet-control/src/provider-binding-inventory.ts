// SPDX-License-Identifier: Apache-2.0

import type { ProviderBindingIdentity } from './types.js';

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
