// SPDX-License-Identifier: Apache-2.0

import { isDeploymentScriptName, isSha256 } from './deployment-context.js';
import type {
  ApplicationBindingTopology,
  DurableObjectBindingInventory,
  ExternalReleaseTopology,
  R2Jurisdiction,
} from './types.js';

const BINDING_NAME = /^[A-Z][A-Z0-9_]{0,63}$/u;
const BUCKET_NAME = /^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/u;
const RESERVED_BINDINGS = new Set([
  'AUDIT_PROXY',
  'AUDIT_QUEUE',
  'DB',
  'DEPLOYMENT_IDENTITY_SECRET',
  'EGRESS_PROXY',
  'HOSTS',
  'MAINTENANCE_ADMIN_SECRET',
]);
const JURISDICTIONS = new Set<R2Jurisdiction>(['default', 'eu', 'fedramp']);

function object(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`${label} must be an object`);
  }
  return value as Record<string, unknown>;
}

function exactKeys(
  value: Record<string, unknown>,
  required: readonly string[],
  optional: readonly string[],
  label: string,
): void {
  const keys = Object.keys(value).sort();
  const allowed = new Set([...required, ...optional]);
  if (
    required.some((key) => !(key in value)) ||
    keys.some((key) => !allowed.has(key))
  ) {
    throw new Error(`${label} has invalid fields`);
  }
}

function namedEntries<T>(
  value: unknown,
  label: string,
  decode: (
    entry: Record<string, unknown>,
    label: string,
  ) => T & {
    readonly name: string;
  },
): readonly (T & { readonly name: string })[] {
  if (!Array.isArray(value)) throw new Error(`${label} must be an array`);
  const decoded = value.map((entry, index) =>
    decode(object(entry, `${label}[${index}]`), `${label}[${index}]`),
  );
  const names = decoded.map(({ name }) => name);
  if (new Set(names).size !== names.length) {
    throw new Error(`${label} contains duplicate binding names`);
  }
  return decoded.sort((left, right) => left.name.localeCompare(right.name));
}

function bindingName(value: unknown, label: string): string {
  if (typeof value !== 'string' || !BINDING_NAME.test(value)) {
    throw new Error(`${label} is not a valid binding name`);
  }
  return value;
}

export function applicationBindingTopologyFromUnknown(
  value: unknown,
  label: string,
): ApplicationBindingTopology {
  const topology = object(value, label);
  exactKeys(topology, ['vars', 'secrets', 'r2Buckets'], [], label);
  const vars = namedEntries(topology.vars, `${label}.vars`, (entry, item) => {
    exactKeys(entry, ['name', 'value'], [], item);
    if (typeof entry.value !== 'string') {
      throw new Error(`${item}.value must be a string`);
    }
    if (new TextEncoder().encode(entry.value).byteLength > 5 * 1024) {
      throw new Error(`${item}.value exceeds 5 KB`);
    }
    return {
      name: bindingName(entry.name, `${item}.name`),
      value: entry.value,
    };
  });
  const secrets = namedEntries(
    topology.secrets,
    `${label}.secrets`,
    (entry, item) => {
      exactKeys(entry, ['name', 'valueSha256'], [], item);
      if (
        typeof entry.valueSha256 !== 'string' ||
        !isSha256(entry.valueSha256)
      ) {
        throw new Error(`${item}.valueSha256 must be a SHA-256 digest`);
      }
      return {
        name: bindingName(entry.name, `${item}.name`),
        valueSha256: entry.valueSha256,
      };
    },
  );
  const r2Buckets = namedEntries(
    topology.r2Buckets,
    `${label}.r2Buckets`,
    (entry, item) => {
      exactKeys(
        entry,
        ['name', 'bucketName', 'jurisdiction'],
        ['creationDate'],
        item,
      );
      if (
        typeof entry.bucketName !== 'string' ||
        !BUCKET_NAME.test(entry.bucketName) ||
        typeof entry.jurisdiction !== 'string' ||
        !JURISDICTIONS.has(entry.jurisdiction as R2Jurisdiction) ||
        (entry.creationDate !== undefined &&
          (typeof entry.creationDate !== 'string' ||
            !Number.isFinite(Date.parse(entry.creationDate))))
      ) {
        throw new Error(`${item} has invalid R2 identity`);
      }
      return {
        name: bindingName(entry.name, `${item}.name`),
        bucketName: entry.bucketName,
        jurisdiction: entry.jurisdiction as R2Jurisdiction,
        ...(entry.creationDate !== undefined
          ? { creationDate: entry.creationDate }
          : {}),
      };
    },
  );
  const allNames = [...vars, ...secrets, ...r2Buckets].map(({ name }) => name);
  if (
    new Set(allNames).size !== allNames.length ||
    allNames.some(
      (name) =>
        RESERVED_BINDINGS.has(name) ||
        name.startsWith('FLEET_') ||
        name.startsWith('DEPLOYMENT_'),
    )
  ) {
    throw new Error(`${label} reuses an application binding name`);
  }
  return { vars, secrets, r2Buckets };
}

export function externalReleaseTopologyFromUnknown(
  value: unknown,
  label: string,
): ExternalReleaseTopology {
  const topology = object(value, label);
  exactKeys(
    topology,
    [
      'durableObjectBindings',
      'serviceBindings',
      'queueProducerBindings',
      'secretNames',
    ],
    ['application'],
    label,
  );
  const durableObjectBindings = namedEntries(
    topology.durableObjectBindings,
    `${label}.durableObjectBindings`,
    (entry, item): DurableObjectBindingInventory => {
      exactKeys(
        entry,
        ['name', 'className', 'namespaceId'],
        ['scriptName', 'dispatchNamespace'],
        item,
      );
      if (
        typeof entry.className !== 'string' ||
        entry.className.length === 0 ||
        typeof entry.namespaceId !== 'string' ||
        entry.namespaceId.length === 0 ||
        (entry.scriptName !== undefined &&
          (typeof entry.scriptName !== 'string' ||
            !isDeploymentScriptName(entry.scriptName))) ||
        (entry.dispatchNamespace !== undefined &&
          (typeof entry.dispatchNamespace !== 'string' ||
            !isDeploymentScriptName(entry.dispatchNamespace))) ||
        (entry.dispatchNamespace !== undefined &&
          entry.scriptName === undefined)
      ) {
        throw new Error(`${item} has invalid Durable Object identity`);
      }
      return {
        name: bindingName(entry.name, `${item}.name`),
        className: entry.className,
        namespaceId: entry.namespaceId,
        ...(entry.scriptName !== undefined
          ? { scriptName: entry.scriptName as string }
          : {}),
        ...(entry.dispatchNamespace !== undefined
          ? { dispatchNamespace: entry.dispatchNamespace as string }
          : {}),
      };
    },
  );
  const serviceBindings = namedEntries(
    topology.serviceBindings,
    `${label}.serviceBindings`,
    (entry, item) => {
      exactKeys(entry, ['name', 'service'], [], item);
      if (
        typeof entry.service !== 'string' ||
        !isDeploymentScriptName(entry.service)
      ) {
        throw new Error(`${item}.service is invalid`);
      }
      return {
        name: bindingName(entry.name, `${item}.name`),
        service: entry.service,
      };
    },
  );
  const queueProducerBindings = namedEntries(
    topology.queueProducerBindings,
    `${label}.queueProducerBindings`,
    (entry, item) => {
      exactKeys(entry, ['name', 'queueName'], [], item);
      if (typeof entry.queueName !== 'string' || entry.queueName.length === 0) {
        throw new Error(`${item}.queueName is invalid`);
      }
      return {
        name: bindingName(entry.name, `${item}.name`),
        queueName: entry.queueName,
      };
    },
  );
  if (
    !Array.isArray(topology.secretNames) ||
    topology.secretNames.some(
      (name) => typeof name !== 'string' || !BINDING_NAME.test(name),
    ) ||
    new Set(topology.secretNames).size !== topology.secretNames.length
  ) {
    throw new Error(`${label}.secretNames is invalid`);
  }
  const secretNames = [...(topology.secretNames as string[])].sort();
  return {
    durableObjectBindings,
    serviceBindings,
    queueProducerBindings,
    secretNames,
    ...(topology.application !== undefined
      ? {
          application: applicationBindingTopologyFromUnknown(
            topology.application,
            `${label}.application`,
          ),
        }
      : {}),
  };
}
