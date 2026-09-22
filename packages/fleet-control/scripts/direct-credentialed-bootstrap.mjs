// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { cancelBodyWithoutAwait } from './direct-credentialed-body-cancel.mjs';
import { DIRECT_INVOCATION_FAILURE_DETAILS } from './direct-credentialed-invocation.mjs';
import {
  validateProviderAuth as auth,
  classifyDispatchNamespaces,
  DirectProviderError,
  identifier,
  inventory,
  openDirectProviderSession,
  resolveDirectZone,
} from './direct-credentialed-provider.mjs';
import { REFERENCE_SECRET_NAMES } from './direct-credentialed-reference-vocabulary.mjs';
import {
  DirectRunStateError,
  mutationPending,
} from './direct-credentialed-run-state.mjs';

const ERROR_CODES = new Set([
  'invalid-input',
  'provider-unavailable',
  'observation-mismatch',
  'name-collision',
  'outcome-unknown',
  'budget-exhausted',
  'invocation-budget-exhausted',
  'reference-refused',
]);

export class DirectBootstrapError extends Error {
  constructor(code = 'invalid-input', detail) {
    const accepted = ERROR_CODES.has(code) ? code : 'invalid-input';
    super(accepted);
    this.name = 'DirectBootstrapError';
    this.code = accepted;
    this.detail = DIRECT_INVOCATION_FAILURE_DETAILS.includes(detail)
      ? detail
      : undefined;
  }
}

function refuse(code = 'observation-mismatch', detail) {
  throw new DirectBootstrapError(code, detail);
}

function object(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) refuse();
  return value;
}

function equal(value, expected) {
  if (!isDeepStrictEqual(value, expected)) refuse();
}

async function checkedInput(input) {
  const { validateDirectConformanceConfig, deriveDirectConformanceNames } =
    await import('./direct-credentialed-conformance-config.mjs');
  const { DIRECT_MANIFEST_MODULE, DIRECT_MAX_UPLOAD_BYTES } = await import(
    './direct-credentialed-conformance-preflight.mjs'
  );
  try {
    const prepared = structuredClone(input.prepared);
    const config = validateDirectConformanceConfig(prepared.config);
    equal(config, prepared.config);
    equal(prepared.names, deriveDirectConformanceNames(config));
    const journal = input.journal;
    for (const method of [
      'snapshot',
      'bindBootstrapContext',
      'beginBootstrapMutation',
      'confirmBootstrapMutation',
      'recordBootstrapObservation',
      'reserveInvocation',
      'settleInvocation',
    ]) {
      if (typeof journal[method] !== 'function') refuse('invalid-input');
    }
    const snapshot = journal.snapshot();
    if (
      snapshot.version !== 2 ||
      !Number.isSafeInteger(snapshot.invocationCount) ||
      snapshot.invocationCount < 0 ||
      snapshot.invocationCount > config.referenceWorker.maxInvocations ||
      !Object.hasOwn(snapshot, 'bootstrap') ||
      !Object.hasOwn(snapshot, 'lastInvocation')
    )
      refuse('invalid-input');
    const binding = snapshot.binding;
    identifier(binding.accountId);
    equal(binding, {
      accountId: binding.accountId,
      configSha256: prepared.configSha256,
      referenceModuleSetSha256: prepared.referenceModuleSetSha256,
      resourcePrefix: config.resourcePrefix,
      maxInvocations: config.referenceWorker.maxInvocations,
    });
    if (
      ![prepared.configSha256, prepared.referenceModuleSetSha256].every(
        (hash) => typeof hash === 'string' && /^[a-f0-9]{64}$/u.test(hash),
      )
    )
      refuse('invalid-input');
    if (mutationPending(snapshot)) refuse('outcome-unknown');
    if (
      snapshot.invocationCount > 0 &&
      (!snapshot.bootstrap?.upload || !snapshot.bootstrap?.ingress)
    )
      refuse('invalid-input');
    if (snapshot.bootstrap) {
      equal(snapshot.bootstrap.context.names, prepared.names);
      await journal.bindBootstrapContext(snapshot.bootstrap.context);
    }
    if (
      !Array.isArray(prepared.referenceModules) ||
      prepared.referenceModules.length < 2
    )
      refuse('invalid-input');
    const seen = new Set();
    const table = prepared.referenceModules.map((module) => {
      identifier(module.name, 255);
      if (seen.has(module.name) || module.name === 'metadata')
        refuse('invalid-input');
      seen.add(module.name);
      let bytes;
      if (
        module.contentType === 'application/javascript+module' &&
        typeof module.source === 'string'
      )
        bytes = Buffer.from(module.source);
      else if (
        module.contentType === 'application/wasm' &&
        typeof module.base64 === 'string'
      ) {
        bytes = Buffer.from(module.base64, 'base64');
        if (bytes.toString('base64') !== module.base64) refuse('invalid-input');
      } else refuse('invalid-input');
      if (
        bytes.length !== module.byteLength ||
        createHash('sha256').update(bytes).digest('hex') !== module.sha256
      )
        refuse('invalid-input');
      return {
        name: module.name,
        contentType: module.contentType,
        byteLength: module.byteLength,
        sha256: module.sha256,
      };
    });
    const [main, manifest] = prepared.referenceModules;
    if (
      main.name !== config.referenceWorker.artifact.mainModule ||
      main.sha256 !== config.referenceWorker.artifact.sha256 ||
      manifest.name !== DIRECT_MANIFEST_MODULE ||
      manifest.source !==
        `export default ${JSON.stringify(prepared.manifest)};\n` ||
      prepared.manifest.configSha256 !== prepared.configSha256
    )
      refuse('invalid-input');
    equal(prepared.manifest.names, prepared.names);
    const { artifact: referenceArtifact, ...referenceRuntime } =
      config.referenceWorker;
    const {
      artifact: tenantArtifact,
      spec,
      ...deploymentRuntime
    } = config.deployment;
    equal(prepared.manifest.referenceRuntime, referenceRuntime);
    equal(prepared.manifest.deploymentRuntime, deploymentRuntime);
    if (
      prepared.manifest.resourcePrefix !== config.resourcePrefix ||
      prepared.manifest.environment !== config.environment ||
      prepared.manifest.contractVersion !== config.contractVersion ||
      prepared.manifest.fixtureVersion !== spec.fixtureVersion ||
      prepared.manifest.interruption !== config.interruption ||
      prepared.manifest.tenantModule.name !== tenantArtifact.mainModule ||
      prepared.manifest.tenantModule.sha256 !== tenantArtifact.sha256
    )
      refuse('invalid-input');
    equal(
      prepared.referenceModules
        .slice(2)
        .map(({ name, sha256 }) => ({ name, sha256 })),
      (referenceArtifact.auxiliaryWasm ?? []).map(({ name, sha256 }) => ({
        name,
        sha256,
      })),
    );
    if (
      createHash('sha256').update(JSON.stringify(table)).digest('hex') !==
        prepared.referenceModuleSetSha256 ||
      table.reduce((sum, module) => sum + module.byteLength, 0) !==
        prepared.referenceUploadBytes ||
      prepared.referenceUploadBytes > DIRECT_MAX_UPLOAD_BYTES
    )
      refuse('invalid-input');
    auth(input.apiToken);
    auth(input.invokeSecret);
    const fetchRequest = input.fetch ?? globalThis.fetch;
    if (typeof fetchRequest !== 'function') refuse('invalid-input');
    return {
      prepared,
      journal,
      accountId: binding.accountId,
      apiToken: input.apiToken,
      invokeSecret: input.invokeSecret,
      fetchRequest,
    };
  } catch (error) {
    if (
      error instanceof DirectBootstrapError &&
      error.code === 'outcome-unknown'
    )
      throw error;
    refuse('invalid-input');
  }
}

function assertZonePolicy(token, verifiedId, accountId) {
  if (
    token.id !== verifiedId ||
    token.status !== 'active' ||
    !Array.isArray(token.policies) ||
    token.policies.length === 0
  )
    refuse();
  const required = [
    ['Zone Read'],
    ['Workers Routes Read'],
    ['Workers Routes Edit', 'Workers Routes Write'],
  ];
  const allowed = new Set();
  for (const policy of token.policies) {
    object(policy);
    identifier(policy.id);
    if (
      !['allow', 'deny'].includes(policy.effect) ||
      !Array.isArray(policy.permission_groups) ||
      policy.permission_groups.length === 0
    )
      refuse();
    const names = new Set(
      policy.permission_groups.map((group) => {
        object(group);
        identifier(group.id);
        if (group.name !== undefined) identifier(group.name);
        return group.name;
      }),
    );
    const resources = object(policy.resources);
    if (Object.keys(resources).length === 0) refuse();
    for (const [key, access] of Object.entries(resources)) {
      identifier(key, 256);
      if (typeof access === 'string') identifier(access, 256);
      else {
        object(access);
        if (Object.keys(access).length === 0) refuse();
        for (const [nested, value] of Object.entries(access)) {
          identifier(nested, 256);
          identifier(value, 256);
        }
      }
    }
    const account = resources[`com.cloudflare.api.account.${accountId}`];
    const coversAll =
      resources['com.cloudflare.api.account.zone.*'] === '*' ||
      (typeof account === 'object' &&
        account?.['com.cloudflare.api.account.zone.*'] === '*');
    const restricts = Object.entries(resources).some(
      ([key, access]) =>
        (key.startsWith('com.cloudflare.api.account.zone.') &&
          access === '*') ||
        (key === `com.cloudflare.api.account.${accountId}` &&
          typeof access === 'object' &&
          Object.keys(access).some((nested) =>
            nested.startsWith('com.cloudflare.api.account.zone.'),
          )),
    );
    for (const [index, alternatives] of required.entries()) {
      if (!alternatives.some((name) => names.has(name))) continue;
      if (policy.effect === 'deny' && restricts) refuse();
      if (policy.effect === 'allow' && coversAll) allowed.add(index);
    }
  }
  if (allowed.size !== required.length) refuse();
}

async function attestToken(sdk, accountId, APIError) {
  for (const family of ['account', 'user']) {
    let verified;
    let token;
    try {
      verified =
        family === 'account'
          ? await sdk.accounts.tokens.verify({ account_id: accountId })
          : await sdk.user.tokens.verify();
      identifier(verified.id);
      if (verified.status !== 'active') refuse();
      token =
        family === 'account'
          ? await sdk.accounts.tokens.get(verified.id, {
              account_id: accountId,
            })
          : await sdk.user.tokens.get(verified.id);
    } catch (error) {
      if (
        family === 'account' &&
        error instanceof APIError &&
        ([401, 403, 404, 405, 429].includes(error.status) ||
          (error.status >= 500 && error.status <= 599))
      )
        continue;
      throw error;
    }
    assertZonePolicy(token, verified.id, accountId);
    return;
  }
}

function d1Receipt(value, name, uuid) {
  object(value);
  identifier(value.uuid);
  identifier(value.name);
  if (value.name !== name || (uuid !== undefined && value.uuid !== uuid))
    refuse();
  return { uuid: value.uuid, name };
}

function r2Receipt(value, name) {
  object(value);
  if (
    value.name !== name ||
    (value.jurisdiction !== undefined && value.jurisdiction !== 'default') ||
    typeof value.creation_date !== 'string' ||
    value.creation_date.length > 64 ||
    !Number.isFinite(Date.parse(value.creation_date))
  )
    refuse();
  return {
    name,
    jurisdiction: 'default',
    creationDate: new Date(value.creation_date).toISOString(),
  };
}

async function assertAbsent(request, APIError) {
  let response;
  try {
    response = await request.asResponse();
    if (response.status === 200) refuse('name-collision');
    refuse('provider-unavailable');
  } catch (error) {
    if (error instanceof APIError && error.status === 404) return;
    throw error;
  } finally {
    cancelBodyWithoutAwait(response?.body);
  }
}

export async function bootstrapDirectConformance(input) {
  const { prepared, journal, accountId, apiToken, invokeSecret, fetchRequest } =
    await checkedInput(input);
  let transport;
  try {
    const session = await openDirectProviderSession({
      apiToken,
      fetchRequest,
      timeoutMs: prepared.config.referenceWorker.requestTimeoutMs,
    });
    transport = session.transport;
    const { sdk, numbered, single, status, APIError, bound } = session;
    const selectors = { account_id: accountId };
    const names = prepared.names;
    if ((await sdk.accounts.get(selectors)).id !== accountId) refuse();
    const subdomain = (await sdk.workers.subdomains.get(selectors)).subdomain;
    if (
      typeof subdomain !== 'string' ||
      !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/u.test(subdomain)
    )
      refuse();
    await attestToken(sdk, accountId, APIError);
    const selectedZone = await resolveDirectZone({
      sdk,
      numbered,
      accountId,
      ownedHostname: prepared.config.ownedHostname,
      bound,
    });
    const namespaces = await classifyDispatchNamespaces(
      single,
      selectors,
      bound,
    );
    const dispatch = { kind: namespaces.kind, count: namespaces.count };
    await journal.bindBootstrapContext({
      names,
      zoneId: selectedZone.id,
      zoneName: selectedZone.name,
      accountWorkersDevSubdomain: subdomain,
      dispatch,
    });
    const workerAbsent = () =>
      assertAbsent(
        status.workers.scripts.get(names.referenceWorker, selectors),
        APIError,
      );
    const bucketAbsent = () =>
      assertAbsent(
        status.r2.buckets.get(names.exportBucket, {
          ...selectors,
          jurisdiction: 'default',
        }),
        APIError,
      );
    const databaseAbsent = async (name) => {
      const rows = await inventory(
        numbered.d1.database.list({ ...selectors, name, per_page: 100 }),
        (row) => {
          identifier(row.name);
          return [identifier(row.uuid)];
        },
        bound,
      );
      if (rows.some((row) => row.name === name)) refuse('name-collision');
    };
    let state = journal.snapshot().bootstrap;
    if (!state.upload) await workerAbsent();
    if (!state.exports) await bucketAbsent();
    for (const [field, name] of [
      ['fleet', names.fleetDatabase],
      ['quota', names.quotaDatabase],
    ]) {
      const receipt = state[field];
      if (receipt)
        d1Receipt(
          await sdk.d1.database.get(receipt.uuid, selectors),
          name,
          receipt.uuid,
        );
      else await databaseAbsent(name);
    }
    if (state.exports)
      equal(
        r2Receipt(
          await sdk.r2.buckets.get(names.exportBucket, {
            ...selectors,
            jurisdiction: 'default',
          }),
          names.exportBucket,
        ),
        state.exports,
      );
    const mutate = async (kind, dispatchMutation, receipt) => {
      transport.assertBudget();
      await journal.beginBootstrapMutation(kind);
      let detail = 'transport-failure';
      try {
        const value = await dispatchMutation();
        detail = 'non-contract-answer';
        const confirmed = receipt(value);
        detail = undefined;
        await journal.confirmBootstrapMutation({ kind, receipt: confirmed });
      } catch (error) {
        if (
          detail === 'transport-failure' &&
          error instanceof APIError &&
          error.status !== undefined
        ) {
          const headers = error.headers;
          const mediaType = headers
            ?.get('content-type')
            ?.split(';')[0]
            ?.trim()
            .toLowerCase();
          detail =
            (mediaType === 'text/plain' || mediaType === 'text/html') &&
            headers.get('cache-control') !== 'no-store' &&
            !headers.has('www-authenticate')
              ? 'platform-page'
              : 'non-contract-answer';
        }
        refuse('outcome-unknown', detail);
      }
    };
    for (const [field, name, kind] of [
      ['fleet', names.fleetDatabase, 'create-fleet-d1'],
      ['quota', names.quotaDatabase, 'create-quota-d1'],
    ]) {
      if (journal.snapshot().bootstrap[field]) continue;
      await databaseAbsent(name);
      await mutate(
        kind,
        () => sdk.d1.database.create({ ...selectors, name }, { maxRetries: 0 }),
        (value) => d1Receipt(value, name),
      );
    }
    if (!state.exports) {
      await bucketAbsent();
      await mutate(
        'create-export-r2',
        () =>
          sdk.r2.buckets.create(
            { ...selectors, name: names.exportBucket, jurisdiction: 'default' },
            { maxRetries: 0 },
          ),
        (value) => r2Receipt(value, names.exportBucket),
      );
    }
    state = journal.snapshot().bootstrap;
    const runBinding = {
      version: 1,
      accountId,
      fleetDatabaseId: state.fleet.uuid,
      quotaDatabaseId: state.quota.uuid,
      exportBucketName: state.exports.name,
      referenceModuleSetSha256: prepared.referenceModuleSetSha256,
      accountWorkersDevSubdomain: subdomain,
    };
    const bindings = [
      { name: 'FLEET_DB', type: 'd1', database_id: state.fleet.uuid },
      { name: 'QUOTA_DB', type: 'd1', database_id: state.quota.uuid },
      { name: 'EXPORTS', type: 'r2_bucket', bucket_name: state.exports.name },
      {
        name: 'DIRECT_RUN_BINDING',
        type: 'plain_text',
        text: JSON.stringify(runBinding),
      },
    ];
    if (!state.upload) {
      await workerAbsent();
      const { generateDirectDeploymentSecrets } = await import(
        './direct-credentialed-spec.ts'
      );
      const { namedWorkerUploadBody } = await import(
        '../src/cloudflare-worker-upload.ts'
      );
      const { toFile } = await import('cloudflare/uploads');
      const secrets = {
        a: generateDirectDeploymentSecrets(),
        b: generateDirectDeploymentSecrets(),
        recovery: generateDirectDeploymentSecrets(),
      };
      const values = [apiToken, invokeSecret, JSON.stringify(secrets)];
      const runtime = prepared.config.referenceWorker;
      const metadata = JSON.stringify({
        main_module: runtime.artifact.mainModule,
        compatibility_date: runtime.compatibilityDate,
        compatibility_flags: runtime.compatibilityFlags,
        limits: {
          cpu_ms: runtime.cpuLimitMs,
          subrequests: runtime.subrequestLimit,
        },
        bindings: [
          ...bindings,
          ...REFERENCE_SECRET_NAMES.map((name, index) => ({
            name,
            type: 'secret_text',
            text: values[index],
          })),
        ],
      });
      const files = await Promise.all(
        prepared.referenceModules.map((module) =>
          toFile(
            module.contentType === 'application/wasm'
              ? Buffer.from(module.base64, 'base64')
              : Buffer.from(module.source),
            module.name,
            { type: module.contentType },
          ),
        ),
      );
      await mutate(
        'upload-reference',
        () =>
          sdk.workers.scripts.update(
            names.referenceWorker,
            { ...selectors, metadata },
            {
              maxRetries: 0,
              body: namedWorkerUploadBody(files, metadata),
              headers: { 'Content-Type': null },
            },
          ),
        (value) => {
          if (value.id !== undefined && value.id !== names.referenceWorker)
            refuse();
          return {
            scriptName: names.referenceWorker,
            tag: value.tag === undefined ? null : identifier(value.tag),
            etag: value.etag === undefined ? null : identifier(value.etag),
          };
        },
      );
    }
    state = journal.snapshot().bootstrap;
    if (state.upload.tag !== null) {
      const scripts = await inventory(
        single.workers.scripts.list(selectors),
        (row) => {
          if (row.tag !== undefined) identifier(row.tag);
          return [identifier(row.id)];
        },
        bound,
      );
      if (
        scripts.find((row) => row.id === names.referenceWorker)?.tag !==
        state.upload.tag
      )
        refuse();
    }
    const { exactActiveVersionId } = await import('../src/active-route.ts');
    const deployments = (
      await sdk.workers.scripts.deployments.list(
        names.referenceWorker,
        selectors,
      )
    ).deployments;
    if (
      !Array.isArray(deployments) ||
      deployments.length === 0 ||
      deployments.length > bound
    )
      refuse();
    const current = deployments[0];
    identifier(current.id);
    const activeVersion = (value) => {
      try {
        return identifier(exactActiveVersionId(value, 'reference'));
      } catch {
        refuse();
      }
    };
    const versionId = activeVersion(current);
    const active = { deploymentId: current.id, versionId };
    if (state.active) equal(active, state.active);
    const deployment = await sdk.workers.scripts.deployments.get(
      active.deploymentId,
      { ...selectors, script_name: names.referenceWorker },
    );
    if (
      deployment.id !== active.deploymentId ||
      activeVersion(deployment) !== versionId
    )
      refuse();
    const version = await sdk.workers.scripts.versions.get(versionId, {
      ...selectors,
      script_name: names.referenceWorker,
    });
    if (version.id !== versionId) refuse();
    const runtime = prepared.config.referenceWorker;
    const checkRuntime = (value) => {
      if (
        value?.compatibility_date !== runtime.compatibilityDate ||
        value?.limits?.cpu_ms !== runtime.cpuLimitMs
      )
        refuse();
      // The version resource omits compatibility_flags when the list is empty.
      equal(
        value.compatibility_flags === undefined
          ? []
          : value.compatibility_flags,
        runtime.compatibilityFlags,
      );
    };
    checkRuntime(version.resources?.script_runtime);
    const { providerBindingsToPlainWorkerShape } = await import(
      '../src/provider-binding-inventory.ts'
    );
    const expectedBindings = providerBindingsToPlainWorkerShape([
      ...bindings,
      ...REFERENCE_SECRET_NAMES.map((name) => ({ name, type: 'secret_text' })),
    ]).sort((a, b) => a.name.localeCompare(b.name));
    const checkBindings = (value) => {
      if (!Array.isArray(value)) refuse();
      const observed = providerBindingsToPlainWorkerShape(value);
      if (
        observed.some(
          (binding) =>
            binding.type === 'unsupported' || typeof binding.name !== 'string',
        )
      )
        refuse();
      equal(
        observed.sort((a, b) => a.name.localeCompare(b.name)),
        expectedBindings,
      );
    };
    checkBindings(version.resources?.bindings);
    const settings = await sdk.workers.scripts.scriptAndVersionSettings.get(
      names.referenceWorker,
      selectors,
    );
    checkRuntime(settings);
    checkBindings(settings.bindings);
    if (settings.limits.subrequests !== runtime.subrequestLimit) refuse();
    await journal.recordBootstrapObservation({ kind: 'active', ...active });
    const checkIngress = (value) => {
      if (value.enabled !== true || value.previews_enabled !== false) refuse();
      return { enabled: true, previewsEnabled: false };
    };
    if (!journal.snapshot().bootstrap.ingress)
      await mutate(
        'enable-reference-ingress',
        () =>
          sdk.workers.scripts.subdomain.create(
            names.referenceWorker,
            { ...selectors, enabled: true, previews_enabled: false },
            { maxRetries: 0 },
          ),
        checkIngress,
      );
    checkIngress(
      await sdk.workers.scripts.subdomain.get(names.referenceWorker, selectors),
    );
    const { awaitReferenceIngress, createDirectInvocationClient } =
      await import('./direct-credentialed-invocation.mjs');
    if (
      !(await awaitReferenceIngress({
        prepared,
        accountWorkersDevSubdomain: subdomain,
        fetch: fetchRequest,
      }))
    )
      refuse('provider-unavailable');
    const client = createDirectInvocationClient({
      prepared,
      journal,
      accountWorkersDevSubdomain: subdomain,
      invokeSecret,
      fetch: input.fetch,
    });
    const observed = await client.invoke({ kind: 'control-read' });
    equal(observed.result?.binding, runBinding);
    await journal.recordBootstrapObservation({
      kind: 'control-read',
      ordinal: journal.snapshot().lastInvocation.ordinal,
    });
    return client;
  } catch (error) {
    if (error instanceof DirectBootstrapError) throw error;
    if (error instanceof DirectProviderError) refuse(error.code);
    if (error instanceof DirectRunStateError)
      refuse(error.code === 'outcome-unknown' ? error.code : 'invalid-input');
    if (transport?.failure()) refuse(transport.failure());
    if (error?.name === 'DirectInvocationError' && ERROR_CODES.has(error.code))
      refuse(error.code, error.detail);
    refuse('provider-unavailable');
  } finally {
    transport?.close();
  }
}
