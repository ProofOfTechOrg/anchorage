// SPDX-License-Identifier: Apache-2.0

export const CONFORMANCE_CONTRACT_VERSION = 1;

function requireString(value, label) {
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`conformance config requires ${label}`);
  }
  return value;
}

function requireObject(value, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`conformance config requires ${label}`);
  }
  return value;
}

function requireArray(value, label, { nonEmpty = false } = {}) {
  if (!Array.isArray(value) || (nonEmpty && value.length === 0)) {
    throw new Error(`conformance config requires ${label}`);
  }
  return value;
}

function requirePositiveInteger(value, label) {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error(`conformance config requires positive ${label}`);
  }
  return value;
}

function requireUrl(value, label) {
  const text = requireString(value, label);
  let url;
  try {
    url = new URL(text);
  } catch {
    throw new Error(`conformance config requires absolute ${label}`);
  }
  if (url.protocol !== 'https:') {
    throw new Error(`conformance config requires HTTPS ${label}`);
  }
  return text;
}

function requirePath(value, label) {
  const path = requireString(value, label);
  if (!path.startsWith('/') || path.startsWith('//')) {
    throw new Error(`conformance config requires absolute-path ${label}`);
  }
  return path;
}

function validateArtifact(artifact, label) {
  const value = requireObject(artifact, label);
  requireString(value.bundle, `${label}.bundle`);
  requireString(value.mainModule, `${label}.mainModule`);
  requireString(value.compatibilityDate, `${label}.compatibilityDate`);
}

function validateStateProfiles(profile, conformance) {
  const stateProfiles = requireArray(
    profile.stateProfiles,
    'platformProfile.stateProfiles',
  );
  if (stateProfiles.length !== 2) {
    throw new Error(
      'conformance config requires exactly two platformProfile.stateProfiles',
    );
  }
  for (const [index, stateProfile] of stateProfiles.entries()) {
    const label = `platformProfile.stateProfiles[${index}]`;
    const value = requireObject(stateProfile, label);
    if (value.name !== `v${index + 1}`) {
      throw new Error(`${label}.name must be 'v${index + 1}'`);
    }
    validateArtifact(value.stateWorker, `${label}.stateWorker`);
    requireArray(
      value.stateDurableObjectMigrations,
      `${label}.stateDurableObjectMigrations`,
      { nonEmpty: true },
    );
  }
  const v1Migrations = stateProfiles[0].stateDurableObjectMigrations;
  const v2Migrations = stateProfiles[1].stateDurableObjectMigrations;
  if (
    v2Migrations.length <= v1Migrations.length ||
    JSON.stringify(v2Migrations.slice(0, v1Migrations.length)) !==
      JSON.stringify(v1Migrations)
  ) {
    throw new Error(
      'platformProfile.stateProfiles[1] migrations must append to the complete v1 history',
    );
  }
  const newClassName = conformance.newDurableObjectBinding.className;
  const appendedClasses = v2Migrations
    .slice(v1Migrations.length)
    .flatMap((migration) => [
      ...(migration.newClasses ?? []),
      ...(migration.newSqliteClasses ?? []),
    ]);
  if (!appendedClasses.includes(newClassName)) {
    throw new Error(
      'platformProfile.stateProfiles[1] migrations must append the conformance new Durable Object class',
    );
  }
  if (
    v2Migrations
      .slice(v1Migrations.length)
      .some(
        (migration) =>
          (migration.deletedClasses?.length ?? 0) > 0 ||
          (migration.renamedClasses?.length ?? 0) > 0,
      )
  ) {
    throw new Error(
      'platformProfile.stateProfiles[1] migrations must be append-only',
    );
  }
}

export function validateConformanceConfig(value) {
  requireObject(value, 'root object');
  if (value.contractVersion !== CONFORMANCE_CONTRACT_VERSION) {
    throw new Error('conformance config contractVersion must be 1');
  }
  const tenantTags = requireArray(value.tenantTags, 'tenantTags');
  if (tenantTags.length !== 2) {
    throw new Error('conformance config requires exactly two tenantTags');
  }
  for (const tenantTag of tenantTags) {
    requireString(tenantTag, 'non-empty tenantTags');
  }
  if (new Set(tenantTags).size !== tenantTags.length) {
    throw new Error('conformance config tenantTags must be distinct');
  }
  requireString(value.environment, 'environment');
  requireString(value.dispatchNamespace, 'dispatchNamespace');
  requireString(value.hostRoutingKvId, 'hostRoutingKvId');
  requireString(value.sharedOutboundWorkerName, 'sharedOutboundWorkerName');
  requireString(value.auditQueueName, 'auditQueueName');
  requireString(value.workerBundle, 'workerBundle');
  requireString(value.mainModule, 'mainModule');
  requireString(value.exportDirectory, 'exportDirectory');
  requireString(value.compatibilityDate, 'compatibilityDate');
  if (!Number.isSafeInteger(value.schemaVersion) || value.schemaVersion < 0) {
    throw new Error('conformance config requires a non-negative schemaVersion');
  }
  requireArray(value.migrations, 'migrations');
  requirePositiveInteger(value.cpuLimitMs, 'cpuLimitMs');
  requirePositiveInteger(value.subrequestLimit, 'subrequestLimit');

  const maintenanceBaseUrls = requireObject(
    value.maintenanceBaseUrls,
    'maintenanceBaseUrls',
  );
  const routeHostnames = requireObject(value.routeHostnames, 'routeHostnames');
  for (const tenantTag of tenantTags) {
    requireUrl(
      maintenanceBaseUrls[tenantTag],
      `maintenanceBaseUrls['${tenantTag}']`,
    );
    requireString(routeHostnames[tenantTag], `routeHostnames['${tenantTag}']`);
  }

  const durableObjectBindings = requireArray(
    value.durableObjectBindings,
    'durableObjectBindings',
    { nonEmpty: true },
  );
  for (const [index, binding] of durableObjectBindings.entries()) {
    const candidate = requireObject(binding, `durableObjectBindings[${index}]`);
    requireString(candidate.name, `durableObjectBindings[${index}].name`);
    requireString(
      candidate.className,
      `durableObjectBindings[${index}].className`,
    );
    if (
      candidate.scriptName !== undefined ||
      candidate.dispatchNamespace !== undefined
    ) {
      throw new Error(
        'conformance durableObjectBindings must resolve to local trusted state',
      );
    }
  }

  const application = requireObject(value.application, 'application');
  const variables = requireArray(application.vars, 'application.vars', {
    nonEmpty: true,
  });
  requireArray(application.secrets, 'application.secrets');
  const r2Buckets = requireArray(
    application.r2Buckets,
    'application.r2Buckets',
    {
      nonEmpty: true,
    },
  );
  for (const [index, variable] of variables.entries()) {
    const candidate = requireObject(variable, `application.vars[${index}]`);
    requireString(candidate.name, `application.vars[${index}].name`);
    requireString(candidate.value, `application.vars[${index}].value`);
  }
  for (const [index, bucket] of r2Buckets.entries()) {
    const candidate = requireObject(bucket, `application.r2Buckets[${index}]`);
    requireString(candidate.name, `application.r2Buckets[${index}].name`);
  }
  if (application.secrets.length !== 0) {
    throw new Error(
      'conformance config application.secrets must be empty; the gate derives its application secret descriptor from environment-only plaintext',
    );
  }
  requireString(value.applicationSecretBinding, 'applicationSecretBinding');

  const conformance = requireObject(value.conformance, 'conformance');
  requirePath(conformance.httpPath, 'conformance.httpPath');
  requirePath(conformance.webSocketPath, 'conformance.webSocketPath');
  requireUrl(conformance.allowedUpstreamUrl, 'conformance.allowedUpstreamUrl');
  requireUrl(conformance.deniedUpstreamUrl, 'conformance.deniedUpstreamUrl');
  requirePositiveInteger(
    conformance.deniedUpstreamStatus,
    'conformance.deniedUpstreamStatus',
  );
  requirePositiveInteger(
    conformance.cpuOverLimitStatus,
    'conformance.cpuOverLimitStatus',
  );
  requireString(
    conformance.applicationVariableName,
    'conformance.applicationVariableName',
  );
  requireString(
    conformance.applicationVariableValue,
    'conformance.applicationVariableValue',
  );
  const configuredVariable = application.vars.find(
    (variable) => variable?.name === conformance.applicationVariableName,
  );
  if (configuredVariable?.value !== conformance.applicationVariableValue) {
    throw new Error(
      'conformance application variable must match application.vars exactly',
    );
  }
  const newDurableObjectBinding = requireObject(
    conformance.newDurableObjectBinding,
    'conformance.newDurableObjectBinding',
  );
  requireString(
    newDurableObjectBinding.name,
    'conformance.newDurableObjectBinding.name',
  );
  requireString(
    newDurableObjectBinding.className,
    'conformance.newDurableObjectBinding.className',
  );
  if (
    durableObjectBindings.some(
      (binding) =>
        binding.name === newDurableObjectBinding.name ||
        binding.className === newDurableObjectBinding.className,
    )
  ) {
    throw new Error(
      'conformance.newDurableObjectBinding must add a new binding and class',
    );
  }

  const profile = requireObject(value.platformProfile, 'platformProfile');
  requireString(
    profile.maintenanceCapabilityPublicKey,
    'platformProfile.maintenanceCapabilityPublicKey',
  );
  if (profile.runtimeContractVersion !== CONFORMANCE_CONTRACT_VERSION) {
    throw new Error('platformProfile.runtimeContractVersion must be 1');
  }
  if (profile.backwardCompatibleWithRetainedReleases !== true) {
    throw new Error(
      'platformProfile.backwardCompatibleWithRetainedReleases must be true',
    );
  }
  const egressHosts = requireArray(
    profile.organizationEgressHosts,
    'platformProfile.organizationEgressHosts',
    { nonEmpty: true },
  );
  const allowedHostname = new URL(conformance.allowedUpstreamUrl).hostname;
  if (!egressHosts.includes(allowedHostname)) {
    throw new Error(
      'platformProfile.organizationEgressHosts must include the allowed upstream hostname',
    );
  }
  if (egressHosts.includes(new URL(conformance.deniedUpstreamUrl).hostname)) {
    throw new Error(
      'platformProfile.organizationEgressHosts must exclude the denied upstream hostname',
    );
  }
  validateStateProfiles(profile, conformance);
  return value;
}
