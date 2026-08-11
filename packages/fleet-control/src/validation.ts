// SPDX-License-Identifier: Apache-2.0

import { assertValidDeploymentTag } from '@proofoftech/flowsafe/deployment-identity-protocol';
import {
  applicationSecretValues,
  canonicalApplicationBindings,
} from './application-bindings.js';
import {
  isDeploymentEnvironment,
  isDeploymentScriptName,
  isDeploymentTenantTag,
} from './deployment-context.js';

import type {
  DeploymentSecrets,
  DeploymentSpec,
  WorkerModule,
} from './types.js';

const BINDING_PATTERN = /^[A-Z][A-Z0-9_]{0,63}$/;
const CLASS_PATTERN = /^[A-Za-z_$][A-Za-z0-9_$]*$/;
const MIGRATION_TAG_PATTERN = /^[A-Za-z0-9._-]{1,64}$/;
const SECRET_PATTERN = /^[\x21-\x7e]{32,256}$/;
const SHA256_PATTERN = /^[a-f0-9]{64}$/u;
const RESERVED_BINDING_NAMES = new Set([
  'AUDIT_PROXY',
  'AUDIT_QUEUE',
  'DB',
  'DEPLOYMENT_IDENTITY_SECRET',
  'EGRESS_PROXY',
  'HOSTS',
  'MAINTENANCE_ADMIN_SECRET',
]);
const MAX_BINDINGS = 128;
const MAX_TEXT_BINDING_BYTES = 5 * 1024;

function assertApplicationBindings(spec: DeploymentSpec): void {
  const application = canonicalApplicationBindings(spec);
  const applicationBindings = [
    ...application.vars.map(({ name }) => ({ name, kind: 'variable' })),
    ...application.secrets.map(({ name }) => ({ name, kind: 'secret' })),
    ...application.r2Buckets.map(({ name }) => ({ name, kind: 'R2' })),
  ];
  const allBindings = [
    ...applicationBindings,
    ...spec.durableObjectBindings.map(({ name }) => ({
      name,
      kind: 'Durable Object',
    })),
    ...(spec.queueProducer
      ? [{ name: spec.queueProducer.binding, kind: 'queue' }]
      : []),
  ];
  const seen = new Set<string>();
  for (const binding of allBindings) {
    if (!BINDING_PATTERN.test(binding.name)) {
      throw new Error(`${binding.kind} binding '${binding.name}' is invalid`);
    }
    if (seen.has(binding.name)) {
      throw new Error(
        `binding name '${binding.name}' is declared more than once`,
      );
    }
    seen.add(binding.name);
  }
  for (const binding of applicationBindings) {
    if (
      RESERVED_BINDING_NAMES.has(binding.name) ||
      binding.name.startsWith('FLEET_') ||
      binding.name.startsWith('DEPLOYMENT_')
    ) {
      throw new Error(`binding '${binding.name}' is reserved by fleet control`);
    }
  }
  for (const secret of application.secrets) {
    if (
      !secret ||
      typeof secret.name !== 'string' ||
      typeof secret.valueSha256 !== 'string'
    ) {
      throw new Error(
        'application secret descriptors must contain name and valueSha256 strings',
      );
    }
    if (!SHA256_PATTERN.test(secret.valueSha256)) {
      throw new Error(
        `application secret '${secret.name}' has an invalid SHA-256 descriptor`,
      );
    }
  }
  for (const variable of application.vars) {
    if (typeof variable.value !== 'string') {
      throw new Error(
        `application variable '${variable.name}' must be a string`,
      );
    }
    if (
      new TextEncoder().encode(variable.value).byteLength >
      MAX_TEXT_BINDING_BYTES
    ) {
      throw new Error(`application variable '${variable.name}' exceeds 5 KB`);
    }
  }
  for (const bucket of application.r2Buckets) {
    if (!bucket || typeof bucket.name !== 'string') {
      throw new Error('application R2 descriptors must contain a name string');
    }
    if (
      bucket.jurisdiction !== undefined &&
      !['default', 'eu', 'fedramp'].includes(bucket.jurisdiction)
    ) {
      throw new Error(
        `R2 binding '${bucket.name}' has an invalid jurisdiction`,
      );
    }
  }
  const bindingCount =
    10 +
    spec.durableObjectBindings.length +
    (spec.queueProducer ? 1 : 0) +
    (spec.egressProxyService ? 1 : 0) +
    applicationBindings.length;
  if (bindingCount > MAX_BINDINGS) {
    throw new Error(
      `Worker binding count ${bindingCount} exceeds ${MAX_BINDINGS}`,
    );
  }
}

function unique(values: readonly string[], label: string): void {
  const seen = new Set<string>();
  for (const value of values) {
    if (seen.has(value))
      throw new Error(`${label} contains duplicate '${value}'`);
    seen.add(value);
  }
}

function assertModule(module: WorkerModule): void {
  if (
    !module.name ||
    module.name.includes('..') ||
    module.name.startsWith('/')
  ) {
    throw new Error(`invalid Worker module name '${module.name}'`);
  }
  if (
    typeof module.content !== 'string' &&
    !(module.content instanceof Uint8Array)
  ) {
    throw new Error(`Worker module '${module.name}' has invalid content`);
  }
}

export function targetDurableObjectTag(
  spec: Pick<
    DeploymentSpec,
    'durableObjectMigrations' | 'previousDurableObjectTag'
  >,
): string | undefined {
  return (
    spec.durableObjectMigrations.at(-1)?.tag ?? spec.previousDurableObjectTag
  );
}

export function validateDeploymentSpec(spec: DeploymentSpec): void {
  assertValidDeploymentTag(spec.tenantTag, 'fleet-control');
  if (!isDeploymentEnvironment(spec.environment)) {
    throw new Error(`environment '${spec.environment}' is invalid`);
  }
  if (!isDeploymentScriptName(spec.scriptName)) {
    throw new Error(`scriptName '${spec.scriptName}' is invalid`);
  }
  if (!isDeploymentScriptName(spec.databaseName)) {
    throw new Error(`databaseName '${spec.databaseName}' is invalid`);
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(spec.compatibilityDate)) {
    throw new Error('compatibilityDate must use YYYY-MM-DD');
  }
  if (!Number.isSafeInteger(spec.schemaVersion) || spec.schemaVersion < 0) {
    throw new Error('schemaVersion must be a non-negative safe integer');
  }
  assertApplicationBindings(spec);
  if (spec.modules.length === 0)
    throw new Error('at least one module is required');
  for (const module of spec.modules) assertModule(module);
  unique(
    spec.modules.map((module) => module.name),
    'modules',
  );
  if (!spec.modules.some((module) => module.name === spec.mainModule)) {
    throw new Error(
      `mainModule '${spec.mainModule}' is not present in modules`,
    );
  }
  unique(
    spec.durableObjectBindings.map((binding) => binding.name),
    'Durable Object bindings',
  );
  for (const binding of spec.durableObjectBindings) {
    if (!BINDING_PATTERN.test(binding.name)) {
      throw new Error(`Durable Object binding '${binding.name}' is invalid`);
    }
    if (!CLASS_PATTERN.test(binding.className)) {
      throw new Error(`Durable Object class '${binding.className}' is invalid`);
    }
    if (
      binding.scriptName !== undefined &&
      !isDeploymentScriptName(binding.scriptName)
    ) {
      throw new Error(
        `Durable Object binding scriptName '${binding.scriptName}' is invalid`,
      );
    }
    if (
      binding.dispatchNamespace !== undefined &&
      !isDeploymentScriptName(binding.dispatchNamespace)
    ) {
      throw new Error(
        `Durable Object binding dispatchNamespace '${binding.dispatchNamespace}' is invalid`,
      );
    }
    if (binding.dispatchNamespace !== undefined && !binding.scriptName) {
      throw new Error(
        'a Durable Object dispatchNamespace requires a platform scriptName',
      );
    }
  }
  if (spec.authoredBy === 'external') {
    if (spec.durableObjectMigrations.length > 0) {
      throw new Error(
        'externally authored artifacts cannot define Durable Object migrations',
      );
    }
    for (const binding of spec.durableObjectBindings) {
      if (
        binding.scriptName !== undefined ||
        binding.dispatchNamespace !== undefined
      ) {
        throw new Error(
          `external Durable Object binding '${binding.name}' cannot select its platform state target`,
        );
      }
    }
    if (spec.egressProxyService !== undefined) {
      throw new Error(
        'external artifacts cannot select their platform egress service',
      );
    }
  }
  unique(
    spec.durableObjectMigrations.map((migration) => migration.tag),
    'Durable Object migration tags',
  );
  for (const migration of spec.durableObjectMigrations) {
    if (!MIGRATION_TAG_PATTERN.test(migration.tag)) {
      throw new Error(
        `Durable Object migration tag '${migration.tag}' is invalid`,
      );
    }
    const classes = [
      ...(migration.newSqliteClasses ?? []),
      ...(migration.newClasses ?? []),
      ...(migration.deletedClasses ?? []),
      ...(migration.renamedClasses?.flatMap((item) => [item.from, item.to]) ??
        []),
    ];
    for (const className of classes) {
      if (!CLASS_PATTERN.test(className)) {
        throw new Error(
          `Durable Object migration class '${className}' is invalid`,
        );
      }
    }
  }
  if (
    spec.previousDurableObjectTag !== undefined &&
    !MIGRATION_TAG_PATTERN.test(spec.previousDurableObjectTag)
  ) {
    throw new Error('previousDurableObjectTag is invalid');
  }
  if (
    spec.previousDurableObjectTag !== undefined &&
    !spec.durableObjectMigrations.some(
      (migration) => migration.tag === spec.previousDurableObjectTag,
    )
  ) {
    throw new Error(
      'previousDurableObjectTag must appear in the ordered migration history',
    );
  }
  for (const [index, migration] of spec.migrations.entries()) {
    if (!Number.isSafeInteger(migration.version) || migration.version < 1) {
      throw new Error('D1 migration versions must be positive safe integers');
    }
    const expectedVersion = index + 1;
    if (migration.version !== expectedVersion) {
      throw new Error(
        `D1 migrations must be contiguous from version 1; expected version ${expectedVersion}, found ${migration.version}`,
      );
    }
    if (!migration.sql.trim()) throw new Error('D1 migrations cannot be empty');
  }
  if (spec.migrations.length !== spec.schemaVersion) {
    throw new Error(
      'D1 migration history must contain every version through schemaVersion',
    );
  }
  if (spec.queueProducer && !BINDING_PATTERN.test(spec.queueProducer.binding)) {
    throw new Error(`queue binding '${spec.queueProducer.binding}' is invalid`);
  }
  if (
    spec.egressProxyService !== undefined &&
    !isDeploymentScriptName(spec.egressProxyService)
  ) {
    throw new Error(
      `egressProxyService '${spec.egressProxyService}' is invalid`,
    );
  }
  let maintenanceUrl: URL;
  try {
    maintenanceUrl = new URL(spec.maintenanceBaseUrl);
  } catch {
    throw new Error('maintenanceBaseUrl must be an absolute URL');
  }
  if (
    maintenanceUrl.protocol !== 'https:' &&
    maintenanceUrl.hostname !== 'localhost'
  ) {
    throw new Error('maintenanceBaseUrl must use HTTPS outside localhost');
  }
  if (
    maintenanceUrl.pathname !== '/' ||
    maintenanceUrl.search !== '' ||
    maintenanceUrl.hash !== ''
  ) {
    throw new Error(
      'maintenanceBaseUrl must not contain a path, query, or hash',
    );
  }
  let routeUrl: URL;
  try {
    routeUrl = new URL(`https://${spec.routeHostname}`);
  } catch {
    throw new Error('routeHostname must be a valid hostname');
  }
  if (
    routeUrl.hostname !== spec.routeHostname.toLowerCase() ||
    routeUrl.pathname !== '/' ||
    routeUrl.port !== '' ||
    !spec.routeHostname.includes('.')
  ) {
    throw new Error(
      'routeHostname must be a lowercase hostname without a port',
    );
  }
  if (
    spec.cpuLimitMs !== undefined &&
    (!Number.isSafeInteger(spec.cpuLimitMs) || spec.cpuLimitMs < 1)
  ) {
    throw new Error('cpuLimitMs must be a positive safe integer');
  }
  if (
    spec.subrequestLimit !== undefined &&
    (!Number.isSafeInteger(spec.subrequestLimit) || spec.subrequestLimit < 1)
  ) {
    throw new Error('subrequestLimit must be a positive safe integer');
  }
}

export function validateDeploymentSecrets(
  spec: DeploymentSpec,
  secrets: DeploymentSecrets,
): void {
  if (!SECRET_PATTERN.test(secrets.deploymentIdentity)) {
    throw new Error(
      'deploymentIdentity must contain 32-256 visible ASCII characters',
    );
  }
  if (!SECRET_PATTERN.test(secrets.maintenanceAdmin)) {
    throw new Error(
      'maintenanceAdmin must contain 32-256 visible ASCII characters',
    );
  }
  if (secrets.deploymentIdentity === secrets.maintenanceAdmin) {
    throw new Error('deployment credentials must be distinct');
  }
  applicationSecretValues(spec, secrets);
  for (const [name, value] of Object.entries(secrets.application ?? {})) {
    if (new TextEncoder().encode(value).byteLength > MAX_TEXT_BINDING_BYTES) {
      throw new Error(`application secret '${name}' exceeds 5 KB`);
    }
  }
}

export function deploymentKey(tenantTag: string, environment: string): string {
  if (!isDeploymentTenantTag(tenantTag)) {
    throw new Error(`tenantTag '${tenantTag}' is invalid`);
  }
  if (!isDeploymentEnvironment(environment)) {
    throw new Error(`environment '${environment}' is invalid`);
  }
  return `${tenantTag}:${environment}`;
}
