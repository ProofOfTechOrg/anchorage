// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { exactActiveVersionId } from '../src/active-route.ts';
import {
  databaseExportReceiptKey,
  isPortablePathSegment,
} from '../src/export-file-name.ts';
import { providerBindingsToPlainWorkerShape } from '../src/provider-binding-inventory.ts';
import { cancelBodyWithoutAwait } from './direct-credentialed-body-cancel.mjs';
import {
  deriveDirectConformanceNames,
  validateDirectConformanceConfig,
} from './direct-credentialed-conformance-config.mjs';
import {
  DirectProviderError,
  openDirectProviderSession,
  validateProviderAuth,
} from './direct-credentialed-provider.mjs';
import { mutationPending } from './direct-credentialed-run-state.mjs';

const CODES = new Set([
  'invalid-input',
  'outcome-unknown',
  'observation-mismatch',
  'provider-unavailable',
  'budget-exhausted',
]);
const SETTLEMENT_SQL =
  "SELECT run_key,observation_kind,observation_key,identity_json,identity_sha256,provenance_json,provenance_sha256 FROM direct_reference_observations WHERE run_key=? AND observation_kind='settlement'";
const READY_SQL =
  'SELECT tenant_tag,environment,backend,script_name,database_id,schema_version,artifact_version,desired_spec_digest,phase,settled_settlement_key FROM anchorage_fleet_deployments WHERE tenant_tag=? AND environment=?';

export class DirectObservationError extends Error {
  constructor(code = 'invalid-input') {
    const accepted = CODES.has(code) ? code : 'invalid-input';
    super(accepted);
    this.name = 'DirectObservationError';
    this.code = accepted;
  }
}

function refuse(code = 'observation-mismatch') {
  throw new DirectObservationError(code);
}
// Releases the response body the refusal leaves unread, handing the source
// that same refusal.
function cancelAndRefuse(response) {
  const refusal = new DirectObservationError('observation-mismatch');
  cancelBodyWithoutAwait(response?.body, refusal);
  throw refusal;
}
function object(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) refuse();
  return value;
}
function id(value) {
  if (
    typeof value !== 'string' ||
    value.length > 128 ||
    !isPortablePathSegment(value)
  )
    refuse();
  return value;
}
function hash(value) {
  if (typeof value !== 'string' || !/^[a-f0-9]{64}$/u.test(value)) refuse();
  return value;
}
function role(value) {
  if (!['a', 'b', 'recovery'].includes(value)) refuse();
  return value;
}
function integer(value, minimum = 0) {
  if (!Number.isSafeInteger(value) || value < minimum) refuse();
  return value;
}
function equal(value, expected) {
  if (!isDeepStrictEqual(value, expected)) refuse();
}
function freeze(value) {
  for (const item of Object.values(value))
    if (item && typeof item === 'object') freeze(item);
  return Object.freeze(value);
}

function context(input) {
  try {
    const prepared = structuredClone(input.prepared);
    const config = validateDirectConformanceConfig(prepared.config);
    const names = deriveDirectConformanceNames(config);
    equal(prepared.config, config);
    equal(prepared.names, names);
    const snapshot = structuredClone(input.journal.snapshot());
    const apiToken = input.apiToken;
    const fetchRequest = input.fetch ?? globalThis.fetch;
    validateProviderAuth(apiToken);
    if (typeof fetchRequest !== 'function') refuse();
    const binding = snapshot.binding;
    id(binding.accountId);
    equal(binding, {
      accountId: binding.accountId,
      configSha256: hash(prepared.configSha256),
      referenceModuleSetSha256: hash(prepared.referenceModuleSetSha256),
      resourcePrefix: config.resourcePrefix,
      maxInvocations: config.referenceWorker.maxInvocations,
    });
    const bootstrap = snapshot.bootstrap;
    if (mutationPending(snapshot)) refuse('outcome-unknown');
    if (
      snapshot.version !== 2 ||
      !bootstrap ||
      bootstrap.pending !== null ||
      snapshot.lastInvocation?.state !== 'settled'
    )
      refuse();
    integer(snapshot.invocationCount, 1);
    if (
      snapshot.invocationCount > config.referenceWorker.maxInvocations ||
      snapshot.lastInvocation.ordinal !== snapshot.invocationCount
    )
      refuse();
    hash(snapshot.lastInvocation.requestSha256);
    integer(bootstrap.controlReadOrdinal, 1);
    if (bootstrap.controlReadOrdinal > snapshot.invocationCount) refuse();
    equal(bootstrap.context.names, names);
    id(bootstrap.context.zoneId);
    if (
      typeof bootstrap.context.zoneName !== 'string' ||
      !(
        config.ownedHostname === bootstrap.context.zoneName ||
        config.ownedHostname.endsWith(`.${bootstrap.context.zoneName}`)
      )
    )
      refuse();
    if (
      !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/u.test(
        bootstrap.context.accountWorkersDevSubdomain,
      )
    )
      refuse();
    for (const [field, name] of [
      ['fleet', names.fleetDatabase],
      ['quota', names.quotaDatabase],
    ]) {
      id(bootstrap[field]?.uuid);
      if (bootstrap[field].name !== name) refuse();
    }
    if (
      bootstrap.fleet.uuid === bootstrap.quota.uuid ||
      bootstrap.exports?.name !== names.exportBucket ||
      bootstrap.exports.jurisdiction !== 'default' ||
      !Number.isFinite(Date.parse(bootstrap.exports.creationDate))
    )
      refuse();
    if (
      bootstrap.upload?.scriptName !== names.referenceWorker ||
      bootstrap.ingress?.enabled !== true ||
      bootstrap.ingress.previewsEnabled !== false
    )
      refuse();
    id(bootstrap.active?.deploymentId);
    id(bootstrap.active?.versionId);
    return {
      prepared,
      config,
      names,
      snapshot,
      bootstrap,
      apiToken,
      fetchRequest,
      accountId: binding.accountId,
    };
  } catch (error) {
    if (
      error instanceof DirectObservationError &&
      error.code === 'outcome-unknown'
    )
      throw error;
    refuse('invalid-input');
  }
}

function target(value, ctx) {
  const selectedRole = role(value.role);
  const release = value.applicationRelease;
  if (release !== '1' && release !== '2') refuse();
  const versionId = id(value.versionId);
  if (versionId === 'pending') refuse();
  return {
    role: selectedRole,
    ...ctx.names.roles[selectedRole],
    environment: ctx.config.environment,
    versionId,
    databaseId: id(value.databaseId),
    specDigest: hash(value.specDigest),
    applicationRelease: release,
    schemaVersion: Number(release),
  };
}

async function session(ctx, observe) {
  let provider;
  try {
    provider = await openDirectProviderSession({
      apiToken: ctx.apiToken,
      fetchRequest: ctx.fetchRequest,
      timeoutMs: ctx.config.referenceWorker.requestTimeoutMs,
    });
    return freeze(await observe(provider));
  } catch (error) {
    const transportFailure = provider?.transport.failure();
    if (transportFailure) refuse(transportFailure);
    if (error instanceof DirectObservationError) throw error;
    if (error instanceof DirectProviderError) refuse(error.code);
    refuse('provider-unavailable');
  } finally {
    provider?.transport.close();
  }
}

function traffic(value, bound) {
  object(value);
  id(value.id);
  if (
    value.strategy !== 'percentage' ||
    !Array.isArray(value.versions) ||
    value.versions.length < 1 ||
    value.versions.length > bound
  )
    refuse();
  const seen = new Set();
  let total = 0;
  const versions = value.versions.map((entry) => {
    const versionId = id(entry.version_id);
    if (
      seen.has(versionId) ||
      typeof entry.percentage !== 'number' ||
      !Number.isFinite(entry.percentage) ||
      entry.percentage < 0 ||
      entry.percentage > 100
    )
      refuse();
    seen.add(versionId);
    total += entry.percentage;
    return { versionId, percentage: entry.percentage };
  });
  if (total !== 100) refuse();
  // Zero-weight entries do not participate in the active route.
  const active = exactActiveVersionId(
    { versions: value.versions.filter((entry) => entry.percentage > 0) },
    'direct',
  );
  return { deploymentId: value.id, activeVersionId: active, versions };
}

function bindings(value, expected, checkRelease) {
  if (!Array.isArray(value)) refuse();
  const normalized = providerBindingsToPlainWorkerShape(value);
  const indexed = new Map();
  for (const binding of normalized) {
    if (
      binding.type === 'unsupported' ||
      typeof binding.name !== 'string' ||
      indexed.has(binding.name)
    )
      refuse();
    indexed.set(binding.name, binding);
  }
  const expectedTypes = {
    DB: 'd1',
    MAINTENANCE: 'durable-object',
    RUNNER: 'durable-object',
    PROBE_BUCKET: 'r2-bucket',
    DEPLOYMENT_IDENTITY_SECRET: 'secret-text',
    MAINTENANCE_ADMIN_SECRET: 'secret-text',
    APP_PROBE_TOKEN: 'secret-text',
    DEPLOYMENT_TENANT: 'plain-text',
    FLEET_ENVIRONMENT: 'plain-text',
    FLEET_SCHEMA_VERSION: 'plain-text',
    FLEET_SPEC_DIGEST: 'plain-text',
    FLEET_INGRESS_CONTRACT: 'plain-text',
    APPLICATION_RELEASE: 'plain-text',
  };
  if (
    indexed.size !== Object.keys(expectedTypes).length ||
    Object.entries(expectedTypes).some(
      ([name, type]) => indexed.get(name)?.type !== type,
    )
  )
    refuse();
  if (indexed.get('DB').databaseId !== expected.databaseId) refuse();
  const namespaces = ['MAINTENANCE', 'RUNNER'].map((name) => {
    const binding = indexed.get(name);
    if (
      binding.className !==
        (name === 'MAINTENANCE' ? 'Maintenance' : 'Runner') ||
      (binding.scriptName !== undefined &&
        binding.scriptName !== expected.scriptName) ||
      binding.dispatchNamespace !== undefined
    )
      refuse();
    return {
      binding: name,
      className: binding.className,
      namespaceId: id(binding.namespaceId),
    };
  });
  if (namespaces[0].namespaceId === namespaces[1].namespaceId) refuse();
  const bucket = indexed.get('PROBE_BUCKET');
  if (
    bucket.jurisdiction !== undefined ||
    typeof bucket.bucketName !== 'string' ||
    !/^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/u.test(bucket.bucketName)
  )
    refuse();
  const vars = {
    DEPLOYMENT_TENANT: expected.tenantTag,
    FLEET_ENVIRONMENT: expected.environment,
    FLEET_INGRESS_CONTRACT: 'guarded-object-v1',
    ...(checkRelease
      ? {
          FLEET_SPEC_DIGEST: expected.specDigest,
          FLEET_SCHEMA_VERSION: String(expected.schemaVersion),
          APPLICATION_RELEASE: expected.applicationRelease,
        }
      : {}),
  };
  for (const [name, bindingValue] of Object.entries(vars))
    if (indexed.get(name).value !== bindingValue) refuse();
  if (
    !['1', '2'].includes(indexed.get('APPLICATION_RELEASE').value) ||
    indexed.get('FLEET_SCHEMA_VERSION').value !==
      indexed.get('APPLICATION_RELEASE').value ||
    typeof indexed.get('FLEET_SPEC_DIGEST').value !== 'string' ||
    !/^[a-f0-9]{64}$/u.test(indexed.get('FLEET_SPEC_DIGEST').value)
  )
    refuse();
  return {
    databaseId: expected.databaseId,
    namespaces,
    bucketName: bucket.bucketName,
  };
}

export async function observeDirectWorkerVersion(input) {
  const ctx = context(input);
  let expected;
  try {
    expected = target(input, ctx);
  } catch {
    refuse('invalid-input');
  }
  return session(ctx, async ({ sdk, bound }) => {
    const selectors = { account_id: ctx.accountId };
    const list = await sdk.workers.scripts.deployments.list(
      expected.scriptName,
      selectors,
    );
    if (
      !Array.isArray(list.deployments) ||
      list.deployments.length < 1 ||
      list.deployments.length > bound
    )
      refuse();
    const current = traffic(list.deployments[0], bound);
    const exact = await sdk.workers.scripts.deployments.get(
      current.deploymentId,
      { ...selectors, script_name: expected.scriptName },
    );
    equal(traffic(exact, bound), current);
    const version = await sdk.workers.scripts.versions.get(expected.versionId, {
      ...selectors,
      script_name: expected.scriptName,
    });
    if (version.id !== expected.versionId) refuse();
    const runtime = version.resources?.script_runtime;
    const intent = ctx.config.deployment;
    if (
      runtime?.limits?.cpu_ms !== intent.cpuLimitMs ||
      runtime.compatibility_date !== intent.compatibilityDate
    )
      refuse();
    // The version resource omits compatibility_flags when the list is empty.
    equal(
      runtime.compatibility_flags === undefined
        ? []
        : runtime.compatibility_flags,
      intent.compatibilityFlags,
    );
    const resources = bindings(version.resources?.bindings, expected, true);
    const settings = await sdk.workers.scripts.scriptAndVersionSettings.get(
      expected.scriptName,
      selectors,
    );
    if (settings.limits?.subrequests !== intent.subrequestLimit) refuse();
    equal(
      bindings(
        settings.bindings,
        expected,
        current.activeVersionId === expected.versionId,
      ),
      resources,
    );
    const bucket = await sdk.r2.buckets.get(resources.bucketName, {
      ...selectors,
      jurisdiction: 'default',
    });
    if (
      bucket.name !== resources.bucketName ||
      (bucket.jurisdiction !== undefined &&
        bucket.jurisdiction !== 'default') ||
      typeof bucket.creation_date !== 'string' ||
      bucket.creation_date.length > 64 ||
      !Number.isFinite(Date.parse(bucket.creation_date))
    )
      refuse();
    return {
      role: expected.role,
      accountId: ctx.accountId,
      tenantTag: expected.tenantTag,
      environment: expected.environment,
      scriptName: expected.scriptName,
      versionId: expected.versionId,
      currentDeployment: current,
      trafficPercentage:
        current.versions.find((entry) => entry.versionId === expected.versionId)
          ?.percentage ?? 0,
      cpuLimitMs: runtime.limits.cpu_ms,
      subrequestLimit: settings.limits.subrequests,
      specDigest: expected.specDigest,
      schemaVersion: expected.schemaVersion,
      applicationRelease: expected.applicationRelease,
      databaseId: resources.databaseId,
      namespaces: resources.namespaces,
      bucket: {
        name: bucket.name,
        jurisdiction: 'default',
        creationDate: new Date(bucket.creation_date).toISOString(),
      },
    };
  });
}

async function queryRows(sdk, ctx, sql, params, limit) {
  const page = await sdk.d1.database.query(ctx.bootstrap.fleet.uuid, {
    account_id: ctx.accountId,
    sql,
    params,
  });
  if (!Array.isArray(page.result) || page.result.length !== 1) refuse();
  const result = page.result[0];
  // Cloudflare returns errors: null on successful pages; accept it on statements too.
  if (
    result.success !== true ||
    (result.errors !== undefined &&
      result.errors !== null &&
      (!Array.isArray(result.errors) || result.errors.length !== 0)) ||
    (result.error !== undefined && result.error !== null) ||
    !Array.isArray(result.results) ||
    result.results.length > limit
  )
    refuse();
  return result.results.map(object);
}

function storedJson(row, field, prefix) {
  const value = row[`${field}_json`];
  const digest = hash(row[`${field}_sha256`]);
  if (
    typeof value !== 'string' ||
    Buffer.byteLength(value) > 256 * 1024 ||
    createHash('sha256')
      .update(
        JSON.stringify([
          'observation',
          prefix,
          'settlement',
          row.observation_key,
          field,
          value,
        ]),
      )
      .digest('hex') !== digest
  )
    refuse();
  return object(JSON.parse(value));
}

export async function readDirectSettlementEffects(input) {
  const ctx = context(input);
  let expected;
  try {
    if (
      !Array.isArray(input.expected) ||
      input.expected.length < 1 ||
      input.expected.length > 3
    )
      refuse();
    expected = input.expected.map((value) => target(value, ctx));
    if (new Set(expected.map((value) => value.role)).size !== expected.length)
      refuse();
  } catch {
    refuse('invalid-input');
  }
  return session(ctx, async ({ single }) => {
    const { fleetSettlementKey } = await import('@proofoftech/fleet-control');
    const rows = await queryRows(
      single,
      ctx,
      SETTLEMENT_SQL,
      [ctx.config.resourcePrefix],
      expected.length,
    );
    if (rows.length !== expected.length) refuse();
    const effects = [];
    const seen = new Set();
    for (const row of rows) {
      if (
        row.run_key !== ctx.config.resourcePrefix ||
        row.observation_kind !== 'settlement'
      )
        refuse();
      const key = hash(row.observation_key);
      if (seen.has(key)) refuse();
      seen.add(key);
      const identity = storedJson(row, 'identity', ctx.config.resourcePrefix);
      const provenance = storedJson(
        row,
        'provenance',
        ctx.config.resourcePrefix,
      );
      const match = expected.find((value) => value.role === identity.role);
      if (!match) refuse();
      equal(identity, {
        version: 1,
        role: match.role,
        tenantTag: match.tenantTag,
        environment: match.environment,
        target: {
          physicalScriptName: match.scriptName,
          specDigest: match.specDigest,
          artifactVersion: match.versionId,
        },
      });
      if (
        key !==
          fleetSettlementKey({
            tenantTag: match.tenantTag,
            environment: match.environment,
            specDigest: match.specDigest,
            artifactVersion: match.versionId,
          }) ||
        typeof provenance.alreadySettled !== 'boolean' ||
        typeof provenance.observedAt !== 'string' ||
        !Number.isFinite(Date.parse(provenance.observedAt))
      )
        refuse();
      const ready = await queryRows(
        single,
        ctx,
        READY_SQL,
        [match.tenantTag, match.environment],
        1,
      );
      equal(ready, [
        {
          tenant_tag: match.tenantTag,
          environment: match.environment,
          backend: 'plain-worker',
          script_name: match.scriptName,
          database_id: match.databaseId,
          schema_version: match.schemaVersion,
          artifact_version: match.versionId,
          desired_spec_digest: match.specDigest,
          phase: 'ready',
          settled_settlement_key: key,
        },
      ]);
      effects.push({
        role: match.role,
        tenantTag: match.tenantTag,
        environment: match.environment,
        scriptName: match.scriptName,
        databaseId: match.databaseId,
        versionId: match.versionId,
        specDigest: match.specDigest,
        schemaVersion: match.schemaVersion,
        settlementKey: key,
        identitySha256: row.identity_sha256,
        provenanceSha256: row.provenance_sha256,
      });
    }
    return effects.sort((a, b) => a.role.localeCompare(b.role));
  });
}

export async function verifyDirectDecommissionExport(input) {
  const ctx = context(input);
  let metadata;
  let sourceInvocationOrdinal;
  let selectedRole;
  let key;
  try {
    selectedRole = role(input.role);
    sourceInvocationOrdinal = integer(input.sourceInvocationOrdinal, 1);
    metadata = structuredClone(input.metadata);
    const last = ctx.snapshot.lastInvocation;
    if (last.ordinal !== sourceInvocationOrdinal || last.state !== 'settled')
      refuse();
    equal(last.action, { kind: 'decommission-export', role: selectedRole });
    const receipt = object(metadata.receipt);
    const authority = `r2://${ctx.bootstrap.exports.name}/${ctx.config.resourcePrefix}/receipts/v1`;
    if (
      metadata.available !== true ||
      metadata.role !== selectedRole ||
      receipt.version !== 1 ||
      receipt.authority !== authority
    )
      refuse();
    id(receipt.databaseId);
    id(receipt.operationId);
    equal(receipt, {
      version: 1,
      authority,
      databaseId: receipt.databaseId,
      operationId: receipt.operationId,
    });
    if (
      metadata.location !==
        `${authority}/${receipt.databaseId}/${receipt.operationId}.sql` ||
      !['database-exported', 'database-deleting', 'decommissioned'].includes(
        metadata.lifecyclePhase,
      ) ||
      !['transitioning', 'discover', 'verify', 'blocked', 'complete'].includes(
        metadata.intentState,
      )
    )
      refuse();
    integer(metadata.revision);
    integer(metadata.generation);
    integer(metadata.size, 1);
    hash(metadata.sha256);
    key = databaseExportReceiptKey(ctx.config.resourcePrefix, receipt);
  } catch {
    refuse('invalid-input');
  }
  return session(ctx, async ({ exportReader }) => {
    const response = await exportReader(metadata.size).r2.buckets.objects.get(
      key,
      {
        account_id: ctx.accountId,
        bucket_name: ctx.bootstrap.exports.name,
        jurisdiction: 'default',
      },
    );
    if (
      response.status !== 200 ||
      response.redirected ||
      response.headers.has('content-range') ||
      (response.headers.has('content-encoding') &&
        response.headers.get('content-encoding') !== 'identity')
    )
      cancelAndRefuse(response);
    const length = response.headers.get('content-length');
    if (
      length !== null &&
      (!/^[1-9][0-9]*$/u.test(length) || Number(length) !== metadata.size)
    )
      cancelAndRefuse(response);
    if (!response.body) refuse();
    const reader = response.body.getReader();
    const digest = createHash('sha256');
    let size = 0;
    try {
      while (true) {
        const chunk = await reader.read();
        if (chunk.done) break;
        size += chunk.value.byteLength;
        if (size > metadata.size) refuse();
        digest.update(chunk.value);
      }
    } finally {
      void reader.cancel().catch(() => {});
    }
    const sha256 = digest.digest('hex');
    if (size !== metadata.size || sha256 !== metadata.sha256) refuse();
    return {
      verified: true,
      role: selectedRole,
      receipt: metadata.receipt,
      location: metadata.location,
      size,
      sha256,
      sourceInvocationOrdinal,
    };
  });
}
