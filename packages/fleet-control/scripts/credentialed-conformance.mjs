// SPDX-License-Identifier: Apache-2.0

import {
  createHash,
  createHmac,
  randomBytes,
  randomUUID,
  timingSafeEqual,
} from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  CONFORMANCE_CONTRACT_VERSION,
  validateConformanceConfig,
} from './credentialed-conformance-config.mjs';
import {
  cleanupCredentialedDeployment,
  credentialedPlainWorkerDurableObjectBindings,
  credentialedWranglerVersionIds,
  loadCredentialedConformanceArtifacts,
  runCredentialedConformance,
  validateOperationalConformance,
} from './credentialed-conformance-runtime.mjs';

const REQUIRED_ENVIRONMENT_VARIABLES = Object.freeze([
  'FLEET_CONFORMANCE_CONFIG',
  'CLOUDFLARE_API_TOKEN',
  'CLOUDFLARE_ACCOUNT_ID',
  'FLEET_MAINTENANCE_CAPABILITY_PRIVATE_JWK',
  'FLEET_STATE_EGRESS_ROOT_SECRET',
  'FLEET_CONFORMANCE_APPLICATION_SECRET',
]);
const requiredEnvironment = Object.fromEntries(
  REQUIRED_ENVIRONMENT_VARIABLES.map((name) => [name, process.env[name]]),
);
if (REQUIRED_ENVIRONMENT_VARIABLES.some((name) => !requiredEnvironment[name])) {
  const finalEnvironmentVariable = REQUIRED_ENVIRONMENT_VARIABLES.at(-1);
  throw new Error(
    `${REQUIRED_ENVIRONMENT_VARIABLES.slice(0, -1).join(', ')}, and ${finalEnvironmentVariable} are required`,
  );
}
const configPath = requiredEnvironment.FLEET_CONFORMANCE_CONFIG;
const apiToken = requiredEnvironment.CLOUDFLARE_API_TOKEN;
const accountId = requiredEnvironment.CLOUDFLARE_ACCOUNT_ID;
const maintenanceCapabilityPrivateJwk =
  requiredEnvironment.FLEET_MAINTENANCE_CAPABILITY_PRIVATE_JWK;
const stateEgressRootSecret =
  requiredEnvironment.FLEET_STATE_EGRESS_ROOT_SECRET;
const applicationSecret =
  requiredEnvironment.FLEET_CONFORMANCE_APPLICATION_SECRET;

const config = JSON.parse(await readFile(resolve(configPath), 'utf8'));
validateConformanceConfig(config);
const { canonicalMaintenanceCapabilityPublicKey } = await import(
  '../dist/platform-resources.js'
);
const { maintenanceCapabilityPrivateKey, workerContent, stateWorkerContents } =
  await loadCredentialedConformanceArtifacts({
    privateJwk: maintenanceCapabilityPrivateJwk,
    publicJwk: config.platformProfile.maintenanceCapabilityPublicKey,
    canonicalizePublicKey: canonicalMaintenanceCapabilityPublicKey,
    workerBundle: config.workerBundle,
    stateWorkerBundles: config.platformProfile.stateProfiles.map(
      (profile) => profile.stateWorker.bundle,
    ),
    readArtifact: (path) => readFile(resolve(path)),
  });
if (stateEgressRootSecret.length < 32) {
  throw new Error('FLEET_STATE_EGRESS_ROOT_SECRET must contain 32 characters');
}

const applicationSecretDescriptor = Object.freeze({
  name: config.applicationSecretBinding,
  valueSha256: createHash('sha256').update(applicationSecret).digest('hex'),
});

function trustedArtifact(configuration, content) {
  return {
    mainModule: configuration.mainModule,
    modules: [
      {
        name: configuration.mainModule,
        content,
        contentType: 'application/javascript+module',
      },
    ],
    compatibilityDate: configuration.compatibilityDate,
    compatibilityFlags: configuration.compatibilityFlags,
  };
}

const stateProfiles = config.platformProfile.stateProfiles.map(
  (profile, index) => ({
    runtimeContractVersion: CONFORMANCE_CONTRACT_VERSION,
    backwardCompatibleWithRetainedReleases: true,
    maintenanceCapabilityPublicKey:
      config.platformProfile.maintenanceCapabilityPublicKey,
    maintenanceCapabilityPrivateKey,
    stateWorker: trustedArtifact(
      profile.stateWorker,
      stateWorkerContents[index],
    ),
    stateDurableObjectMigrations: profile.stateDurableObjectMigrations,
    organizationEgressHosts: config.platformProfile.organizationEgressHosts,
  }),
);
let activeStateProfileIndex = 0;
const platformProfile = () => stateProfiles[activeStateProfileIndex];

const {
  CloudflareProvisioningClient,
  cleanupDeploymentArtifacts,
  decommissionDeployment,
  deploymentSpecDigest,
  externalPlatformResourceGroupId,
  externalReleaseScriptName,
  externalStateScriptName,
  FileSystemDatabaseExportStore,
  generateDeploymentSecrets,
  migrateFleet,
  provisionDeployment,
  ProcessLocalCloudflareApiRateCoordinator,
  rollbackExternalRelease,
  validateDeploymentSecrets,
  validateDeploymentSpec,
  validateExternalPlatformProfile,
  WorkersForPlatformsBackend,
  WranglerCommandRunner,
  WranglerLoopBackend,
} = await import('../dist/index.js');
const { default: Cloudflare } = await import('cloudflare');
const { DEPLOYMENT_PLATFORM_VARIABLE_NAMES, liveApplicationTopologyMatches } =
  await import('../dist/application-bindings.js');
const {
  externalStateDeploymentSpec,
  FLEET_AUDIT_PROXY_CLASS_NAME,
  FLEET_AUDIT_PROXY_STATE_BINDING,
} = await import('../dist/platform-resources.js');
const suffix = Date.now().toString(36);
const mutationLeaseTtlMs = 15 * 60_000;

class MemoryStore {
  records = new Map();
  leases = new Map();

  key(tenantTag, environment) {
    return `${tenantTag}:${environment}`;
  }

  async withDeploymentLease(tenantTag, environment, operation) {
    const key = this.key(tenantTag, environment);
    if (this.leases.has(key)) throw new Error(`${key} is already leased`);
    const token = Symbol(key);
    const lease = { token, expiresAt: Date.now() + mutationLeaseTtlMs };
    this.leases.set(key, lease);
    const assertOwned = async () => {
      const current = this.leases.get(key);
      if (current?.token !== token || current.expiresAt <= Date.now()) {
        throw new Error(`${key} mutation lease was lost`);
      }
      current.expiresAt = Date.now() + mutationLeaseTtlMs;
    };
    try {
      return await operation({
        tenantTag,
        environment,
        mutationLeaseTtlMs,
        assertOwned,
        renew: assertOwned,
        put: (record) => this.put(record),
        delete: () => this.delete(tenantTag, environment),
      });
    } finally {
      if (this.leases.get(key)?.token === token) {
        this.leases.delete(key);
      }
    }
  }

  async get(tenantTag, environment) {
    return this.records.get(this.key(tenantTag, environment));
  }

  async put(record) {
    this.records.set(this.key(record.tenantTag, record.environment), record);
  }

  async delete(tenantTag, environment) {
    this.records.delete(this.key(tenantTag, environment));
  }

  async list() {
    return [...this.records.values()];
  }
}

function deploymentSpec(tenantTag, release) {
  const scriptName = `conformance-${tenantTag}-${suffix}`;
  return {
    tenantTag,
    environment: config.environment ?? 'conformance',
    scriptName,
    databaseName: scriptName,
    compatibilityDate: config.compatibilityDate,
    compatibilityFlags: config.compatibilityFlags,
    mainModule: config.mainModule ?? 'worker.js',
    modules: [
      {
        name: config.mainModule ?? 'worker.js',
        content:
          release === 1
            ? workerContent
            : Buffer.concat([
                workerContent,
                Buffer.from(`\n// conformance-release:${release}\n`),
              ]),
        contentType: 'application/javascript+module',
      },
    ],
    authoredBy: 'external',
    schemaVersion: config.schemaVersion,
    migrations: config.migrations,
    durableObjectMigrations: [],
    durableObjectBindings:
      release === 1
        ? config.durableObjectBindings
        : [
            ...config.durableObjectBindings,
            config.conformance.newDurableObjectBinding,
          ],
    application: {
      vars: config.application.vars,
      secrets: [applicationSecretDescriptor],
      r2Buckets: config.application.r2Buckets,
    },
    queueProducer: {
      binding: 'AUDIT_QUEUE',
      queueName: config.auditQueueName,
    },
    maintenanceBaseUrl: config.maintenanceBaseUrls[tenantTag],
    routeHostname: config.routeHostnames[tenantTag],
    cpuLimitMs: config.cpuLimitMs,
    subrequestLimit: config.subrequestLimit,
  };
}

const deployments = config.tenantTags.map((tenantTag) => {
  const initialSpec = deploymentSpec(tenantTag, 1);
  return {
    initialSpec,
    currentSpec: initialSpec,
    nextSpec: deploymentSpec(tenantTag, 2),
    secrets: {
      ...generateDeploymentSecrets(),
      application: {
        [applicationSecretDescriptor.name]: applicationSecret,
      },
    },
    store: new MemoryStore(),
  };
});

validateOperationalConformance({
  plans: deployments.map((deployment) => ({
    initialSpec: deployment.initialSpec,
    nextSpec: deployment.nextSpec,
    initialProfile: stateProfiles[0],
    nextProfile: stateProfiles[1],
    secrets: deployment.secrets,
  })),
  maintenanceCapabilityPublicKey:
    config.platformProfile.maintenanceCapabilityPublicKey,
  validateDeploymentSpec,
  validateDeploymentSecrets,
  validateExternalPlatformProfile,
  canonicalMaintenanceCapabilityPublicKey,
});

const rateCoordinator = new ProcessLocalCloudflareApiRateCoordinator();
const exportStore = new FileSystemDatabaseExportStore(config.exportDirectory);
const client = new CloudflareProvisioningClient({
  accountId,
  apiToken,
  dispatchNamespace: config.dispatchNamespace,
  rateCoordinator,
  exportStore,
});
// This narrow read client sits outside the client's coordinated fetch path.
// One explicit acquire therefore covers exactly one SDK request, with no retry.
const cloudflare = new Cloudflare({
  apiToken,
  logLevel: 'off',
  maxRetries: 0,
});
const backend = new WorkersForPlatformsBackend({
  client,
  hostRoutingKvId: config.hostRoutingKvId,
  auditQueueName: config.auditQueueName,
  platformProfileFor: platformProfile,
  namespacedState: {
    dispatchNamespace: config.dispatchNamespace,
    sharedOutboundWorkerName: config.sharedOutboundWorkerName,
    stateEgressRootSecret,
  },
});

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

async function listPlainWorkerVersionIds(runner, scriptName) {
  const result = await runner.run([
    'versions',
    'list',
    '--name',
    scriptName,
    '--json',
  ]);
  return credentialedWranglerVersionIds(result.stdout);
}

function trackedPlainWorkerRouteApi(routeApi, tracking) {
  return new Proxy(routeApi, {
    get(target, property) {
      if (property === 'deleteControlSecrets') {
        return async (...arguments_) => {
          tracking.controlSecretDeletionStarted = true;
          return target.deleteControlSecrets(...arguments_);
        };
      }
      const value = Reflect.get(target, property, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}

function trackedPlainWorkerBackend(backend, runner, tracking) {
  return new Proxy(backend, {
    get(target, property) {
      if (property === 'revokeCredentials') {
        return async (...arguments_) => {
          const [spec] = arguments_;
          await listPlainWorkerVersionIds(runner, spec.scriptName);
          await target.revokeCredentials(...arguments_);
          tracking.revocationCompleted = true;
          await listPlainWorkerVersionIds(runner, spec.scriptName);
        };
      }
      if (property === 'deleteWorker') {
        return async (...arguments_) => {
          const [spec] = arguments_;
          assert(
            tracking.revocationCompleted,
            'plain Worker deletion began without a completed tracked credential revocation',
          );
          await listPlainWorkerVersionIds(runner, spec.scriptName);
          tracking.workerDeletionStarted = true;
          const result = await target.deleteWorker(...arguments_);
          tracking.workerDeletionCompleted = true;
          return result;
        };
      }
      const value = Reflect.get(target, property, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}

function assertPlainWorkerVersionTracking(tracking) {
  assert(
    tracking.controlSecretDeletionStarted &&
      tracking.revocationCompleted &&
      tracking.workerDeletionStarted &&
      tracking.workerDeletionCompleted,
    'plain Worker version-churn proof did not observe revocation and deletion with a nonempty version set',
  );
}

async function eventually(operation, attempts = 20) {
  let lastError;
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      if (attempt + 1 < attempts) {
        await new Promise((resolveDelay) => setTimeout(resolveDelay, 1_000));
      }
    }
  }
  throw lastError;
}

function withDeploymentMutationFence(deployment, operation) {
  return deployment.store.withDeploymentLease(
    deployment.initialSpec.tenantTag,
    deployment.initialSpec.environment,
    operation,
  );
}

function queryDatabaseWithDeploymentFence(
  deployment,
  databaseId,
  sql,
  bindings = [],
) {
  return withDeploymentMutationFence(deployment, (fence) =>
    client.withMutationFence(fence, () =>
      client.queryDatabase(databaseId, sql, bindings),
    ),
  );
}

async function assertD1Marker(deployment, databaseId, marker) {
  const rows = await queryDatabaseWithDeploymentFence(
    deployment,
    databaseId,
    'SELECT value FROM fleet_conformance_state WHERE marker = ?',
    [marker],
  );
  assert(
    rows.length === 1 && rows[0]?.value === marker,
    `D1 state marker '${marker}' did not survive the release transition`,
  );
}

async function assertPlatformGroup(deployment, record) {
  const { currentSpec: spec } = deployment;
  const resources = record.platformResources;
  assert(resources, `${spec.tenantTag} has no persisted platform resources`);
  assert(
    record.activeRelease,
    `${spec.tenantTag} has no persisted active release`,
  );
  assert(
    record.activeRelease.application,
    `${spec.tenantTag} active release has no application topology`,
  );
  const expectedCandidateSecretNames = [
    'DEPLOYMENT_IDENTITY_SECRET',
    ...record.activeRelease.application.secrets.map(({ name }) => name),
  ].sort();
  const stateName = externalStateScriptName(spec);
  const groupId = externalPlatformResourceGroupId(spec);
  assert(
    resources.stateWorker.scriptName === stateName,
    `${spec.tenantTag} state Worker name is not stable`,
  );
  assert(
    resources.stateWorker.dispatchNamespace === config.dispatchNamespace &&
      resources.sharedOutboundWorkerName === config.sharedOutboundWorkerName &&
      resources.egressProxy === undefined,
    `${spec.tenantTag} state topology consumes a legacy ordinary Worker`,
  );
  const [state, candidate] = await Promise.all([
    client.inspectDispatchWorker(stateName),
    client.inspectDispatchWorker(record.activeRelease.physicalScriptName),
  ]);
  assert(state, `${stateName} is missing`);
  assert(candidate, `${record.activeRelease.physicalScriptName} is missing`);
  assert(
    state.databaseIds.length === 1 &&
      state.databaseIds[0] === record.databaseId &&
      candidate.databaseIds.length === 1 &&
      candidate.databaseIds[0] === record.databaseId,
    `${spec.tenantTag} candidate and state Worker do not share the exact D1 database`,
  );
  assert(
    candidate.durableObjectBindings.every(
      (binding) =>
        binding.scriptName === stateName &&
        binding.dispatchNamespace === config.dispatchNamespace,
    ),
    `${spec.tenantTag} candidate has a Durable Object outside its state Worker`,
  );
  if (spec.queueProducer) {
    assert(
      candidate.durableObjectBindings.some(
        (binding) =>
          binding.name === 'AUDIT_PROXY' &&
          binding.className === 'FlowsafeFleetAuditProxy' &&
          binding.scriptName === stateName &&
          binding.dispatchNamespace === config.dispatchNamespace,
      ),
      `${spec.tenantTag} candidate has no exact remote audit Durable Object`,
    );
  }
  assert(
    (candidate.serviceBindings ?? []).length === 0 &&
      (candidate.queueProducerBindings ?? []).length === 0 &&
      JSON.stringify(candidate.secretNames) ===
        JSON.stringify(expectedCandidateSecretNames),
    `${spec.tenantTag} candidate received a privileged service, queue, or secret`,
  );
  assert(
    liveApplicationTopologyMatches(
      record.activeRelease.application,
      candidate,
      DEPLOYMENT_PLATFORM_VARIABLE_NAMES,
    ),
    `${spec.tenantTag} candidate has incorrect application variables or R2 bindings`,
  );
  assert(
    state.durableObjectBindings.every(
      (binding) =>
        binding.scriptName === undefined &&
        binding.dispatchNamespace === undefined,
    ),
    `${spec.tenantTag} state Worker Durable Objects are not local`,
  );
  assert(
    state.serviceBindings.some(
      (binding) =>
        binding.name === 'OUTBOUND_PROXY' &&
        binding.service === config.sharedOutboundWorkerName &&
        binding.entrypoint === 'StateEgress',
    ),
    `${spec.tenantTag} state Worker is not bound to named shared StateEgress`,
  );
  const expectedStateBindings = {
    DEPLOYMENT_TENANT: spec.tenantTag,
    FLEET_ENVIRONMENT: spec.environment,
    FLEET_RESOURCE_GROUP: groupId,
    FLEET_RESOURCE_ROLE: 'platform-state',
    OUTBOUND_TENANT_ID: spec.tenantTag,
    OUTBOUND_ENVIRONMENT: spec.environment,
    OUTBOUND_RESOURCE_GROUP_ID: groupId,
    OUTBOUND_STATE_SCRIPT_NAME: stateName,
    OUTBOUND_ROUTE_HOSTNAME: spec.routeHostname.toLowerCase(),
    OUTBOUND_POLICY_ID: record.outboundPolicy.policyId,
  };
  for (const [name, value] of Object.entries(expectedStateBindings)) {
    assert(
      state.plainTextBindings[name] === value,
      `${stateName} has incorrect static attribution '${name}'`,
    );
  }
  assert(
    JSON.stringify([...state.secretNames].sort()) ===
      JSON.stringify([
        'DEPLOYMENT_IDENTITY_SECRET',
        'MAINTENANCE_ADMIN_SECRET',
        'OUTBOUND_PROXY_CREDENTIAL',
      ]),
    `${stateName} has incorrect state-channel secrets`,
  );
  return { stateName, groupId };
}

async function assertLiveInventory(deployment, record, expectedNames) {
  const inventory = await client.collectFleetInventory({
    hostRoutingKvId: config.hostRoutingKvId,
    databaseNamePrefix: 'conformance-',
    scriptNamePrefix: 'conformance-',
    includeR2Buckets: true,
  });
  const tenantDeployments = inventory.deployments.filter(
    (item) =>
      item.tenantTag === deployment.currentSpec.tenantTag &&
      item.environment === deployment.currentSpec.environment,
  );
  const tenantFindings = inventory.findings.filter(
    (item) =>
      item.tenantTag === deployment.currentSpec.tenantTag &&
      item.environment === deployment.currentSpec.environment,
  );
  assert(
    tenantFindings.length === 0,
    `${deployment.currentSpec.tenantTag} inventory has findings: ${tenantFindings
      .map((item) => item.detail)
      .join('; ')}`,
  );
  for (const resource of record.applicationResources ?? []) {
    const bucket = inventory.r2Buckets?.find(
      (candidate) => candidate.bucketName === resource.bucketName,
    );
    assert(
      bucket?.creationDate === resource.creationDate &&
        bucket.jurisdiction === resource.jurisdiction,
      `${deployment.currentSpec.tenantTag} R2 creation identity is absent or mismatched`,
    );
  }
  for (const expectedName of expectedNames) {
    const item = tenantDeployments.find(
      (candidate) => candidate.scriptName === expectedName,
    );
    assert(
      item,
      `${deployment.currentSpec.tenantTag} inventory omits '${expectedName}'`,
    );
    if (item.resourceRole !== 'platform-state') {
      const release = [
        record.activeRelease,
        record.rollbackRelease,
        record.pendingRelease,
        record.migrationPriorRelease,
      ].find((candidate) => candidate?.physicalScriptName === expectedName);
      assert(
        release?.application,
        `${deployment.currentSpec.tenantTag} has no persisted application topology for '${expectedName}'`,
      );
      const expectedSecretNames = [
        'DEPLOYMENT_IDENTITY_SECRET',
        ...release.application.secrets.map(({ name }) => name),
      ].sort();
      assert(
        JSON.stringify(item.secretNames) ===
          JSON.stringify(expectedSecretNames),
        `${deployment.currentSpec.tenantTag} inventory has incorrect candidate secret names for '${expectedName}'`,
      );
      assert(
        liveApplicationTopologyMatches(
          release.application,
          item,
          DEPLOYMENT_PLATFORM_VARIABLE_NAMES,
        ),
        `${deployment.currentSpec.tenantTag} inventory has incorrect candidate application topology for '${expectedName}'`,
      );
    }
  }
  const state = tenantDeployments.find(
    (item) => item.resourceRole === 'platform-state',
  );
  assert(
    state &&
      !tenantDeployments.some(
        (item) => item.resourceRole === 'deployment-egress',
      ),
    `${deployment.currentSpec.tenantTag} dispatch-native state role is absent or has a legacy proxy`,
  );
  assert(
    state.resourceGroupId ===
      externalPlatformResourceGroupId(deployment.currentSpec),
    `${deployment.currentSpec.tenantTag} inventory resource group is inconsistent`,
  );
  assert(
    state.databaseIds.length === 1 &&
      state.databaseIds[0] === record.databaseId,
    `${deployment.currentSpec.tenantTag} inventory lost the state Worker D1 edge`,
  );
  const hostRoute = inventory.routes.find(
    (item) =>
      item.surface === 'host-registry' &&
      item.hostname === deployment.currentSpec.routeHostname.toLowerCase() &&
      item.tenantTag === deployment.currentSpec.tenantTag &&
      item.environment === deployment.currentSpec.environment,
  );
  assert(
    hostRoute,
    `${deployment.currentSpec.tenantTag} canonical host route is absent`,
  );
  assert(
    hostRoute.scriptName === record.activeRelease?.physicalScriptName &&
      hostRoute.policyId === record.outboundPolicy.policyId &&
      hostRoute.policyDigest === record.outboundPolicy.policyDigest &&
      JSON.stringify(hostRoute.policyHosts) ===
        JSON.stringify(record.outboundPolicy.policyHosts),
    `${deployment.currentSpec.tenantTag} canonical host route has incorrect policy context`,
  );
}

function candidateUrl(deployment, path, spec = deployment.currentSpec) {
  return new URL(path, `https://${spec.routeHostname}`);
}

function assertExactKeys(value, expected, label) {
  assert(value && typeof value === 'object', `${label} is not an object`);
  const actual = Object.keys(value).sort();
  const wanted = [...expected].sort();
  assert(
    JSON.stringify(actual) === JSON.stringify(wanted),
    `${label} fields are not the v1 contract`,
  );
}

async function contractRequest(
  deployment,
  action,
  input = {},
  expectedStatus = 200,
  spec = deployment.currentSpec,
) {
  const response = await fetch(
    candidateUrl(deployment, config.conformance.httpPath, spec),
    {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        contractVersion: CONFORMANCE_CONTRACT_VERSION,
        action,
        ...input,
      }),
      redirect: 'manual',
    },
  );
  assert(
    response.status === expectedStatus,
    `${action} returned ${response.status}, expected ${expectedStatus}`,
  );
  const body = await response.json();
  assert(
    body.contractVersion === CONFORMANCE_CONTRACT_VERSION &&
      body.action === action,
    `${action} returned another contract version or action`,
  );
  return body;
}

async function contractWebSocketFrame(deployment, action, input = {}) {
  const url = candidateUrl(deployment, config.conformance.webSocketPath);
  url.protocol = 'wss:';
  const nonce = input.nonce ?? randomUUID();
  return new Promise((resolveFrame, rejectFrame) => {
    const socket = new WebSocket(url);
    const timeout = setTimeout(() => {
      socket.close();
      rejectFrame(new Error(`${action} WebSocket timed out`));
    }, 15_000);
    const fail = (error) => {
      clearTimeout(timeout);
      socket.close();
      rejectFrame(error);
    };
    socket.addEventListener('open', () => {
      socket.send(
        JSON.stringify({
          contractVersion: CONFORMANCE_CONTRACT_VERSION,
          action,
          ...input,
          nonce,
        }),
      );
    });
    socket.addEventListener('error', () => {
      fail(new Error(`${action} WebSocket failed`));
    });
    socket.addEventListener('message', (event) => {
      try {
        const frame = JSON.parse(String(event.data));
        assert(
          frame.contractVersion === CONFORMANCE_CONTRACT_VERSION &&
            frame.action === action &&
            frame.nonce === nonce,
          `${action} WebSocket returned another contract, action, or nonce`,
        );
        clearTimeout(timeout);
        socket.close();
        resolveFrame(frame);
      } catch (error) {
        fail(error);
      }
    });
  });
}

async function assertApplicationProbe(deployment) {
  const nonce = randomBytes(32).toString('hex');
  const result = await contractRequest(deployment, 'application-bindings', {
    nonce,
  });
  assertExactKeys(
    result,
    [
      'action',
      'contractVersion',
      'secretHmacSha256',
      'secretName',
      'secretPlaintextExposed',
      'variableName',
      'variableValue',
    ],
    'application-bindings response',
  );
  assert(
    result.variableName === config.conformance.applicationVariableName &&
      result.variableValue === config.conformance.applicationVariableValue &&
      result.secretName === applicationSecretDescriptor.name &&
      result.secretPlaintextExposed === false,
    'application binding probe did not return the exact variable and secret name',
  );
  const expected = createHmac('sha256', applicationSecret)
    .update(nonce)
    .digest();
  const actual = Buffer.from(result.secretHmacSha256, 'hex');
  assert(
    actual.byteLength === expected.byteLength &&
      timingSafeEqual(actual, expected),
    'application secret HMAC challenge failed',
  );
}

async function assertEgressAndLimitProbes(deployment) {
  for (const [action, url, field] of [
    [
      'connector-egress-allowed',
      config.conformance.allowedUpstreamUrl,
      'allowed',
    ],
    ['connector-egress-denied', config.conformance.deniedUpstreamUrl, 'denied'],
    ['state-egress-allowed', config.conformance.allowedUpstreamUrl, 'allowed'],
    ['state-egress-denied', config.conformance.deniedUpstreamUrl, 'denied'],
  ]) {
    const result = await contractRequest(deployment, action, { url });
    assertExactKeys(
      result,
      ['action', 'contractVersion', field, 'upstreamStatus'],
      `${action} response`,
    );
    assert(result[field] === true, `${action} was not asserted`);
    if (field === 'denied') {
      assert(
        result.upstreamStatus === config.conformance.deniedUpstreamStatus,
        `${action} returned the wrong denial status`,
      );
    } else {
      assert(
        Number.isSafeInteger(result.upstreamStatus) &&
          result.upstreamStatus >= 200 &&
          result.upstreamStatus < 400,
        `${action} did not reach the allowed upstream`,
      );
    }
  }

  const socket = await contractWebSocketFrame(deployment, 'nonce-echo');
  assertExactKeys(
    socket,
    ['action', 'contractVersion', 'nonce'],
    'nonce-echo frame',
  );

  const control = await contractRequest(deployment, 'cpu-control');
  assertExactKeys(
    control,
    ['action', 'completed', 'contractVersion'],
    'cpu-control response',
  );
  assert(control.completed === true, 'CPU control request did not complete');
  const overLimit = await fetch(
    candidateUrl(deployment, config.conformance.httpPath),
    {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        contractVersion: CONFORMANCE_CONTRACT_VERSION,
        action: 'cpu-over-limit',
      }),
      redirect: 'manual',
    },
  );
  assert(
    overLimit.status === config.conformance.cpuOverLimitStatus,
    `CPU over-limit request returned ${overLimit.status}`,
  );
  const recovery = await contractRequest(deployment, 'cpu-control');
  assert(
    recovery.completed === true,
    'candidate did not recover after the CPU over-limit request',
  );
}

async function assertAuditProbe(deployment) {
  const nonce = randomUUID();
  const result = await contractRequest(deployment, 'audit-proxy', { nonce });
  assertExactKeys(
    result,
    ['accepted', 'action', 'contractVersion', 'nonce'],
    'audit-proxy response',
  );
  assert(
    result.accepted === true && result.nonce === nonce,
    'audit proxy did not accept the attributed event',
  );
}

async function assertStateMarker(deployment, action, marker) {
  const result = await contractRequest(deployment, action, { marker });
  assertExactKeys(
    result,
    ['action', 'contractVersion', 'marker'],
    `${action} response`,
  );
  assert(result.marker === marker, `${action} returned another marker`);
}

async function assertNewStateClass(deployment) {
  const nonce = randomUUID();
  const result = await contractRequest(deployment, 'state-new-class', {
    nonce,
  });
  assertExactKeys(
    result,
    ['action', 'contractVersion', 'nonce', 'stored'],
    'state-new-class response',
  );
  assert(
    result.nonce === nonce && result.stored === true,
    'new Durable Object class did not persist its probe',
  );
}

async function exerciseR2(deployment, { leaveNonempty = false } = {}) {
  const key = `conformance/${randomUUID()}`;
  const value = randomBytes(32).toString('hex');
  const written = await contractRequest(deployment, 'r2-write', { key, value });
  assertExactKeys(
    written,
    ['action', 'contractVersion', 'key', 'written'],
    'r2-write response',
  );
  assert(written.key === key && written.written === true, 'R2 write failed');
  const read = await contractRequest(deployment, 'r2-read', { key });
  assertExactKeys(
    read,
    ['action', 'contractVersion', 'key', 'value'],
    'r2-read response',
  );
  assert(read.key === key && read.value === value, 'R2 read was not exact');
  if (leaveNonempty) return { key, value };
  await deleteR2Fixture(deployment, { key });
  return { key, value };
}

async function deleteR2Fixture(
  deployment,
  fixture,
  spec = deployment.currentSpec,
) {
  const deleted = await contractRequest(
    deployment,
    'r2-delete',
    { key: fixture.key },
    200,
    spec,
  );
  assertExactKeys(
    deleted,
    ['action', 'contractVersion', 'deleted', 'key'],
    'r2-delete response',
  );
  assert(
    deleted.key === fixture.key && deleted.deleted === true,
    'R2 delete failed',
  );
  const absent = await contractRequest(
    deployment,
    'r2-absent',
    { key: fixture.key },
    200,
    spec,
  );
  assertExactKeys(
    absent,
    ['absent', 'action', 'contractVersion', 'key'],
    'r2-absent response',
  );
  assert(
    absent.key === fixture.key && absent.absent === true,
    'R2 object remains after candidate deletion',
  );
}

async function startFlowSafeProof(deployment) {
  const effectNonce = randomUUID();
  const started = await contractRequest(deployment, 'flowsafe-start', {
    effectNonce,
  });
  assertExactKeys(
    started,
    [
      'action',
      'approvalId',
      'contractVersion',
      'effectCount',
      'revision',
      'runId',
      'status',
    ],
    'flowsafe-start response',
  );
  assert(
    started.status === 'pending' &&
      started.effectCount === 0 &&
      Number.isSafeInteger(started.revision),
    'FlowSafe run did not suspend before its effect',
  );
  const update = await contractWebSocketFrame(
    deployment,
    'flowsafe-approval-update',
    {
      runId: started.runId,
      approvalId: started.approvalId,
      revision: started.revision,
    },
  );
  assertExactKeys(
    update,
    [
      'action',
      'approvalId',
      'contractVersion',
      'nonce',
      'revision',
      'runId',
      'status',
    ],
    'flowsafe-approval-update frame',
  );
  assert(
    update.runId === started.runId &&
      update.approvalId === started.approvalId &&
      update.revision === started.revision &&
      update.status === 'pending',
    'FlowSafe WebSocket did not deliver the suspended approval update',
  );
  return started;
}

async function completeFlowSafeProof(deployment, started) {
  const approved = await contractRequest(deployment, 'flowsafe-approve', {
    runId: started.runId,
    approvalId: started.approvalId,
    revision: started.revision,
  });
  assertExactKeys(
    approved,
    [
      'action',
      'approvalId',
      'contractVersion',
      'effectCount',
      'resumed',
      'runId',
      'status',
    ],
    'flowsafe-approve response',
  );
  assert(
    approved.runId === started.runId &&
      approved.approvalId === started.approvalId &&
      approved.status === 'approved' &&
      approved.resumed === true &&
      approved.effectCount === 1,
    'FlowSafe approval did not resume exactly one effect',
  );
  const terminal = await contractRequest(deployment, 'flowsafe-status', {
    runId: started.runId,
  });
  assertExactKeys(
    terminal,
    ['action', 'contractVersion', 'effectCount', 'runId', 'terminalD1'],
    'flowsafe-status response',
  );
  assert(
    terminal.runId === started.runId &&
      terminal.terminalD1 === true &&
      terminal.effectCount === 1,
    'FlowSafe terminal D1 state or exactly-once effect is absent',
  );
  for (const action of ['flowsafe-replay-decision', 'flowsafe-replay-resume']) {
    const replay = await contractRequest(
      deployment,
      action,
      {
        runId: started.runId,
        approvalId: started.approvalId,
        revision: started.revision,
      },
      409,
    );
    assertExactKeys(
      replay,
      ['action', 'contractVersion', 'effectCount', 'rejected', 'runId'],
      `${action} response`,
    );
    assert(
      replay.runId === started.runId &&
        replay.rejected === true &&
        replay.effectCount === 1,
      `${action} was not rejected after exactly one effect`,
    );
  }
}

async function assertSecretPreservingStateUpload(deployment, record) {
  const profile = platformProfile();
  const target = backend.describeExternalPlatformTarget(deployment.currentSpec);
  const database = {
    id: record.databaseId,
    name: record.databaseName,
    created: false,
  };
  let uploadError;
  try {
    await withDeploymentMutationFence(deployment, (fence) =>
      client.withMutationFence(fence, async () => {
        await client.uploadNamespacedStateWorker({
          spec: externalStateDeploymentSpec(deployment.currentSpec, profile),
          database,
          artifact: profile.stateWorker,
          artifactDigest: target.stateArtifactDigest,
          maintenanceCapabilityPublicKey: target.maintenanceCapabilityPublicKey,
          auditQueueName: config.auditQueueName,
          sharedOutboundWorkerName: config.sharedOutboundWorkerName,
          stateEgressCredentialDigest: target.stateEgressCredentialDigest,
        });
      }),
    );
  } catch (error) {
    uploadError = error;
  }
  let attestation;
  let attestationError;
  try {
    attestation = await withDeploymentMutationFence(deployment, (fence) =>
      backend.ensureMaintenanceAttestation(
        deployment.currentSpec,
        deployment.secrets.maintenanceAdmin,
        fence,
        record.activeRelease?.artifactVersion,
      ),
    );
  } catch (error) {
    attestationError = error;
  }
  const repaired = await provisionDeployment({
    initialExecutionFenceState: 'open',
    backend,
    store: deployment.store,
    spec: deployment.currentSpec,
    secrets: deployment.secrets,
  });
  const live = await client.inspectDispatchWorker(
    externalStateScriptName(deployment.currentSpec),
  );
  assert(
    live &&
      repaired.record.platformResources?.stateWorker.artifactVersion ===
        live.artifactVersion,
    'normal convergence did not persist the repaired state artifact version',
  );
  if (uploadError !== undefined) throw uploadError;
  if (attestationError !== undefined) throw attestationError;
  assert(
    attestation?.health.armed === true && attestation.receipt.length > 0,
    'same-name state upload did not preserve the maintenance receipt secret',
  );
}

async function accountWorkersDevSubdomain() {
  await rateCoordinator.acquire();
  const result = await cloudflare.workers.subdomains.get({
    account_id: accountId,
  });
  assert(
    typeof result.subdomain === 'string' &&
      /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/u.test(result.subdomain),
    'Cloudflare account returned an invalid Workers subdomain',
  );
  return result.subdomain;
}

function plainWorkerDeploymentSpec(workersDevSubdomain) {
  const tenantTag = config.tenantTags[0];
  const scriptName = `conformance-plain-${tenantTag}-${suffix}`;
  const stateProfile = stateProfiles[0];
  return {
    tenantTag,
    environment: config.environment ?? 'conformance',
    scriptName,
    databaseName: scriptName,
    compatibilityDate: stateProfile.stateWorker.compatibilityDate,
    compatibilityFlags: stateProfile.stateWorker.compatibilityFlags,
    mainModule: stateProfile.stateWorker.mainModule,
    modules: stateProfile.stateWorker.modules,
    authoredBy: 'platform',
    schemaVersion: config.schemaVersion,
    migrations: config.migrations,
    durableObjectMigrations: stateProfile.stateDurableObjectMigrations,
    durableObjectBindings: credentialedPlainWorkerDurableObjectBindings(
      config.durableObjectBindings,
      stateProfile.stateDurableObjectMigrations,
      {
        name: FLEET_AUDIT_PROXY_STATE_BINDING,
        className: FLEET_AUDIT_PROXY_CLASS_NAME,
      },
    ),
    maintenanceBaseUrl: `https://${scriptName}.${workersDevSubdomain}.workers.dev`,
    routeHostname: config.routeHostnames[tenantTag],
    cpuLimitMs: config.cpuLimitMs,
    subrequestLimit: config.subrequestLimit,
  };
}

async function deleteAttestedPlainWorkerAfterCredentialMutation(
  deployment,
  spec,
  plainBackend,
) {
  await deployment.store.withDeploymentLease(
    spec.tenantTag,
    spec.environment,
    async (fence) => {
      const record = await deployment.store.get(
        spec.tenantTag,
        spec.environment,
      );
      assert(
        record &&
          record.backend === 'plain-worker' &&
          record.tenantTag === spec.tenantTag &&
          record.environment === spec.environment &&
          record.scriptName === spec.scriptName &&
          record.databaseName === spec.databaseName &&
          typeof record.databaseId === 'string' &&
          record.databaseId.length > 0 &&
          record.desiredSpecDigest === deploymentSpecDigest(spec),
        `credentialed cleanup refuses to delete unowned plain Worker '${spec.scriptName}'`,
      );
      await plainBackend.deleteWorker(
        spec,
        undefined,
        {
          id: record.databaseId,
          name: record.databaseName,
          created: false,
        },
        {
          physicalScriptName: record.scriptName,
          specDigest: record.desiredSpecDigest,
          artifactVersion: record.artifactVersion,
          releaseSchemaVersion: record.schemaVersion,
        },
        fence,
      );
    },
  );
}

async function provePlainWorkerSecretVersionChurnTeardown() {
  const spec = plainWorkerDeploymentSpec(await accountWorkersDevSubdomain());
  const secrets = generateDeploymentSecrets();
  validateDeploymentSpec(spec);
  validateDeploymentSecrets(spec, secrets);

  const deployment = {
    initialSpec: spec,
    nextSpec: spec,
    currentSpec: spec,
    secrets,
    store: new MemoryStore(),
  };
  const tracking = {
    controlSecretDeletionStarted: false,
    revocationCompleted: false,
    workerDeletionStarted: false,
    workerDeletionCompleted: false,
  };
  const runner = new WranglerCommandRunner({ apiToken, accountId });
  const plainBackend = new WranglerLoopBackend({
    runner,
    routeApi: trackedPlainWorkerRouteApi(client, tracking),
    exportDirectory: config.exportDirectory,
    exportStore,
  });
  const trackedBackend = trackedPlainWorkerBackend(
    plainBackend,
    runner,
    tracking,
  );

  let conformanceError;
  try {
    const provisioned = await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend: trackedBackend,
      store: deployment.store,
      spec,
      secrets,
    });
    assert(
      provisioned.record.phase === 'ready',
      'plain Worker did not reach ready before decommission',
    );
    const decommissioned = await decommissionDeployment({
      backend: trackedBackend,
      store: deployment.store,
      spec,
    });
    assert(
      decommissioned.record.phase === 'decommissioned',
      'plain Worker did not reach its terminal decommissioned phase',
    );
    assert(
      !(await client.getDatabase(decommissioned.databaseExport.databaseId)),
      `plain Worker database '${decommissioned.databaseExport.databaseId}' remains after terminal decommission`,
    );
    assertPlainWorkerVersionTracking(tracking);
  } catch (error) {
    conformanceError = error;
  }

  const cleanupErrors = [];
  if (
    conformanceError !== undefined &&
    tracking.controlSecretDeletionStarted &&
    !tracking.workerDeletionCompleted
  ) {
    try {
      await deleteAttestedPlainWorkerAfterCredentialMutation(
        deployment,
        spec,
        plainBackend,
      );
    } catch (error) {
      cleanupErrors.push(error);
    }
  }
  try {
    await cleanupCredentialedDeployment(deployment, {
      backend: plainBackend,
      deploymentSpecDigest,
      decommissionDeployment,
      cleanupDeploymentArtifacts,
    });
  } catch (error) {
    cleanupErrors.push(error);
  }
  if (conformanceError !== undefined && cleanupErrors.length > 0) {
    throw new AggregateError(
      [conformanceError, ...cleanupErrors],
      'plain Worker version-churn conformance and cleanup failed',
    );
  }
  if (conformanceError !== undefined) throw conformanceError;
  if (cleanupErrors.length > 0) {
    throw new AggregateError(
      cleanupErrors,
      'plain Worker version-churn cleanup failed',
    );
  }
}

async function provisionV1(deployment) {
  deployment.v1Result = await provisionDeployment({
    initialExecutionFenceState: 'open',
    backend,
    store: deployment.store,
    spec: deployment.initialSpec,
    secrets: deployment.secrets,
  });
  assert(
    deployment.v1Result.maintenance.armed,
    `${deployment.initialSpec.tenantTag} maintenance is not armed`,
  );
  const marker = `${deployment.initialSpec.tenantTag}:${suffix}`;
  await queryDatabaseWithDeploymentFence(
    deployment,
    deployment.v1Result.record.databaseId,
    'CREATE TABLE IF NOT EXISTS fleet_conformance_state (marker TEXT PRIMARY KEY, value TEXT NOT NULL)',
  );
  await queryDatabaseWithDeploymentFence(
    deployment,
    deployment.v1Result.record.databaseId,
    'INSERT INTO fleet_conformance_state (marker, value) VALUES (?, ?)',
    [marker, marker],
  );
  deployment.marker = marker;
}

async function probeV1(deployment) {
  const result = deployment.v1Result;
  const { stateName } = await assertPlatformGroup(deployment, result.record);
  await eventually(() =>
    assertLiveInventory(deployment, result.record, [
      externalReleaseScriptName(deployment.initialSpec),
      stateName,
    ]),
  );
  deployment.initialStateNamespaces = new Map(
    result.record.platformResources.stateWorker.durableObjectBindings.map(
      (binding) => [
        `${binding.name}:${binding.className}`,
        binding.namespaceId,
      ],
    ),
  );
  await assertStateMarker(deployment, 'state-marker-put', deployment.marker);
  await assertStateMarker(deployment, 'state-marker-get', deployment.marker);
  await assertApplicationProbe(deployment);
  await assertAuditProbe(deployment);
  await assertEgressAndLimitProbes(deployment);
  await exerciseR2(deployment);
  deployment.flowSafeProof = await startFlowSafeProof(deployment);
  await assertSecretPreservingStateUpload(deployment, result.record);
}

async function assertTenantIsolation() {
  const firstDatabase = await client.findDatabase(
    deployments[0].initialSpec.databaseName,
  );
  if (!firstDatabase) throw new Error('first conformance database is missing');
  let mismatchRejected = false;
  let sentinelOwner;
  await withDeploymentMutationFence(deployments[0], async (fence) => {
    try {
      // The fence state is irrelevant to what this probe asserts (the sentinel
      // must refuse a re-stamp), but the protocol requires one; 'open' is the
      // state this already-provisioned database is in, so a refusal that
      // somehow did not happen could not also silently close its fence.
      await backend.seedDeploymentIdentity(
        firstDatabase,
        deployments[1].initialSpec.tenantTag,
        fence,
        { initialExecutionFenceState: 'open' },
      );
    } catch (error) {
      mismatchRejected = /already belongs|refusing to re-stamp/.test(
        String(error),
      );
    }
    sentinelOwner = await backend.readDeploymentIdentity(firstDatabase, fence);
  });
  assert(mismatchRejected, 'deployment sentinel mismatch was not rejected');
  assert(
    sentinelOwner === deployments[0].initialSpec.tenantTag,
    'deployment sentinel mismatch changed the original owner',
  );
}

async function migrateV2(deployment) {
  const before = await deployment.store.get(
    deployment.initialSpec.tenantTag,
    deployment.initialSpec.environment,
  );
  assert(before, `${deployment.initialSpec.tenantTag} fleet state is missing`);
  deployment.currentSpec = deployment.nextSpec;
  [deployment.migrated] = await migrateFleet({
    store: deployment.store,
    records: [before],
    canaryTenantTags: [deployment.initialSpec.tenantTag],
    backendFor: () => backend,
    specFor: () => deployment.nextSpec,
    secretsFor: () => deployment.secrets,
  });
  assert(
    deployment.migrated?.activeRelease && deployment.migrated.rollbackRelease,
    `${deployment.initialSpec.tenantTag} migration release snapshots are incomplete`,
  );
}

async function probeV2(deployment) {
  const migrated = deployment.migrated;
  assert(
    migrated.activeRelease.physicalScriptName ===
      externalReleaseScriptName(deployment.nextSpec) &&
      migrated.rollbackRelease.physicalScriptName ===
        externalReleaseScriptName(deployment.initialSpec),
    `${deployment.initialSpec.tenantTag} release flip did not retain its rollback release`,
  );
  await assertPlatformGroup(deployment, migrated);
  await assertD1Marker(deployment, migrated.databaseId, deployment.marker);
  for (const [binding, namespaceId] of deployment.initialStateNamespaces) {
    const retained =
      migrated.platformResources.stateWorker.durableObjectBindings.find(
        (candidate) => `${candidate.name}:${candidate.className}` === binding,
      );
    assert(
      retained?.namespaceId === namespaceId,
      `${deployment.initialSpec.tenantTag} changed original Durable Object namespace '${binding}'`,
    );
  }
  const newBinding = config.conformance.newDurableObjectBinding;
  assert(
    migrated.platformResources.stateWorker.durableObjectBindings.some(
      (binding) =>
        binding.name === newBinding.name &&
        binding.className === newBinding.className &&
        ![...deployment.initialStateNamespaces.values()].includes(
          binding.namespaceId,
        ),
    ),
    `${deployment.initialSpec.tenantTag} did not create a new Durable Object namespace`,
  );
  await assertStateMarker(deployment, 'state-marker-get', deployment.marker);
  await assertNewStateClass(deployment);
  await assertApplicationProbe(deployment);
  await eventually(() =>
    assertLiveInventory(deployment, migrated, [
      migrated.activeRelease.physicalScriptName,
      migrated.rollbackRelease.physicalScriptName,
      externalStateScriptName(deployment.currentSpec),
    ]),
  );
}

async function rollback(deployment) {
  const rolledBack = await rollbackExternalRelease({
    store: deployment.store,
    backend,
    currentSpec: deployment.nextSpec,
    rollbackSpec: deployment.initialSpec,
    secrets: deployment.secrets,
  });
  deployment.currentSpec = deployment.initialSpec;
  assert(
    rolledBack.activeRelease?.physicalScriptName ===
      externalReleaseScriptName(deployment.initialSpec) &&
      rolledBack.rollbackRelease,
    `${deployment.initialSpec.tenantTag} rollback snapshots are incomplete`,
  );
  await assertPlatformGroup(deployment, rolledBack);
  await assertD1Marker(deployment, rolledBack.databaseId, deployment.marker);
}

async function proveNonemptyDecommission(deployment) {
  deployment.nonemptyFixture = await exerciseR2(deployment, {
    leaveNonempty: true,
  });
  let refused = false;
  try {
    await decommissionDeployment({
      backend,
      store: deployment.store,
      spec: deployment.currentSpec,
      secrets: deployment.secrets,
    });
  } catch (error) {
    refused = /R2 bucket .* is not empty/u.test(String(error));
  }
  assert(refused, 'decommission did not refuse a nonempty R2 bucket');
  await deleteR2Fixture(deployment, deployment.nonemptyFixture);
  deployment.nonemptyFixture = undefined;
}

async function decommission(deployment) {
  const result = await decommissionDeployment({
    backend,
    store: deployment.store,
    spec: deployment.currentSpec,
    secrets: deployment.secrets,
  });
  const exported = await readFile(
    fileURLToPath(result.databaseExport.location),
  );
  assert(
    exported.byteLength === result.databaseExport.size &&
      createHash('sha256').update(exported).digest('hex') ===
        result.databaseExport.sha256,
    `${deployment.currentSpec.databaseName} export integrity is not durable`,
  );
  assert(
    !(await client.getDatabase(result.databaseExport.databaseId)),
    `${result.databaseExport.databaseId} remains after decommission`,
  );
}

async function assertZeroResiduals() {
  await eventually(async () => {
    const residual = await client.collectFleetInventory({
      hostRoutingKvId: config.hostRoutingKvId,
      databaseNamePrefix: 'conformance-',
      scriptNamePrefix: 'conformance-',
      includeR2Buckets: true,
    });
    const residualCount =
      residual.findings.length +
      (residual.dispatchScriptCount ?? 0) +
      residual.scriptRegistrations.filter((item) =>
        item.scriptName.startsWith('conformance-'),
      ).length +
      residual.deployments.filter((item) =>
        item.scriptName.startsWith('conformance-'),
      ).length +
      (residual.r2Buckets?.filter((item) =>
        item.bucketName.startsWith('conformance-'),
      ).length ?? 0) +
      residual.databaseIds.length +
      residual.namespaceIds.length +
      residual.routes.filter((item) =>
        item.scriptName.startsWith('conformance-'),
      ).length;
    assert(
      residualCount === 0,
      `credentialed cleanup left ${residualCount} fleet resources`,
    );
  });
}

async function cleanup(deployment) {
  await cleanupCredentialedDeployment(deployment, {
    backend,
    deploymentSpecDigest,
    beforeCleanup: async (spec) => {
      if (deployment.nonemptyFixture) {
        await deleteR2Fixture(deployment, deployment.nonemptyFixture, spec);
        deployment.nonemptyFixture = undefined;
      }
    },
    decommissionDeployment,
    cleanupDeploymentArtifacts,
  });
}

const result = await runCredentialedConformance(
  { deployments },
  {
    provisionV1,
    probeV1,
    assertTenantIsolation,
    activateV2: async () => {
      activeStateProfileIndex = 1;
    },
    migrateV2,
    probeV2,
    completeFlowSafe: (deployment) =>
      completeFlowSafeProof(deployment, deployment.flowSafeProof),
    rollback,
    proveNonemptyDecommission,
    decommission,
    provePlainWorkerSecretVersionChurnTeardown,
    assertZeroResiduals,
    cleanup,
  },
);
console.log(JSON.stringify(result));
