// SPDX-License-Identifier: Apache-2.0

import {
  createPrivateKey,
  createPublicKey,
  randomBytes,
  sign,
  verify,
} from 'node:crypto';

const REQUIRED_OPERATIONS = Object.freeze([
  'provisionV1',
  'probeV1',
  'assertTenantIsolation',
  'activateV2',
  'migrateV2',
  'probeV2',
  'completeFlowSafe',
  'rollback',
  'proveNonemptyDecommission',
  'decommission',
  'assertZeroResiduals',
  'cleanup',
]);

const PRIVATE_ED25519_JWK_FIELDS = Object.freeze([
  'alg',
  'crv',
  'd',
  'kid',
  'kty',
  'x',
]);

const DECOMMISSIONABLE_PHASES = new Set([
  'publishing',
  'ready',
  'migrating',
  'rolling-back',
  'decommissioning',
  'traffic-removed',
  'credentials-revoked',
  'worker-deleted',
  'platform-credentials-revoked',
  'platform-resources-deleted',
  'application-resources-deleting',
  'application-resources-deleted',
  'database-exported',
  'database-deleting',
]);

export function preflightMaintenanceCapabilityKeyPair(options) {
  let privateKey;
  try {
    privateKey = JSON.parse(options.privateJwk);
  } catch {
    throw new Error(
      'FLEET_MAINTENANCE_CAPABILITY_PRIVATE_JWK must contain JSON',
    );
  }
  if (
    !privateKey ||
    typeof privateKey !== 'object' ||
    Array.isArray(privateKey) ||
    JSON.stringify(Object.keys(privateKey).sort()) !==
      JSON.stringify(PRIVATE_ED25519_JWK_FIELDS) ||
    privateKey.kty !== 'OKP' ||
    privateKey.crv !== 'Ed25519' ||
    privateKey.alg !== 'EdDSA' ||
    typeof privateKey.kid !== 'string' ||
    !/^[A-Za-z0-9._-]{1,64}$/u.test(privateKey.kid) ||
    typeof privateKey.x !== 'string' ||
    !/^[A-Za-z0-9_-]{43}$/u.test(privateKey.x) ||
    typeof privateKey.d !== 'string' ||
    !/^[A-Za-z0-9_-]{43}$/u.test(privateKey.d)
  ) {
    throw new Error(
      'FLEET_MAINTENANCE_CAPABILITY_PRIVATE_JWK must be a canonical Ed25519 private signing JWK with kid',
    );
  }

  const publicJwk = options.canonicalizePublicKey(options.publicJwk);
  if (publicJwk !== options.publicJwk) {
    throw new Error(
      'platformProfile.maintenanceCapabilityPublicKey must be canonical',
    );
  }
  const privatePublicJwk = options.canonicalizePublicKey(
    JSON.stringify({
      kty: privateKey.kty,
      crv: privateKey.crv,
      alg: privateKey.alg,
      kid: privateKey.kid,
      x: privateKey.x,
    }),
  );
  if (privatePublicJwk !== publicJwk) {
    throw new Error(
      'maintenance capability private signer does not match its configured public verifier',
    );
  }

  try {
    const nonce = randomBytes(32);
    const signature = sign(
      null,
      nonce,
      createPrivateKey({ key: privateKey, format: 'jwk' }),
    );
    const verifier = createPublicKey({
      key: JSON.parse(publicJwk),
      format: 'jwk',
    });
    if (!verify(null, nonce, verifier, signature)) {
      throw new Error('signature verification failed');
    }
  } catch (error) {
    throw new Error(
      'maintenance capability private signer failed Ed25519 key-pair verification',
      { cause: error },
    );
  }

  return Object.freeze({ ...privateKey });
}

export async function loadCredentialedConformanceArtifacts(options) {
  const maintenanceCapabilityPrivateKey = preflightMaintenanceCapabilityKeyPair(
    {
      privateJwk: options.privateJwk,
      publicJwk: options.publicJwk,
      canonicalizePublicKey: options.canonicalizePublicKey,
    },
  );
  const workerContent = await options.readArtifact(options.workerBundle);
  const stateWorkerContents = await Promise.all(
    options.stateWorkerBundles.map((bundle) => options.readArtifact(bundle)),
  );
  return Object.freeze({
    maintenanceCapabilityPrivateKey,
    workerContent,
    stateWorkerContents: Object.freeze(stateWorkerContents),
  });
}

export function selectCredentialedCleanupSpec(options) {
  if (!options.record) return undefined;
  const initialDigest = options.deploymentSpecDigest(options.initialSpec);
  const nextDigest = options.deploymentSpecDigest(options.nextSpec);
  if (options.record.desiredSpecDigest === initialDigest) {
    return options.initialSpec;
  }
  if (options.record.desiredSpecDigest === nextDigest) {
    return options.nextSpec;
  }
  throw new Error(
    `credentialed cleanup refuses unknown desired specification digest '${options.record.desiredSpecDigest}'`,
  );
}

export async function cleanupCredentialedDeployment(deployment, dependencies) {
  const record = await deployment.store.get(
    deployment.initialSpec.tenantTag,
    deployment.initialSpec.environment,
  );
  const spec = selectCredentialedCleanupSpec({
    record,
    initialSpec: deployment.initialSpec,
    nextSpec: deployment.nextSpec,
    deploymentSpecDigest: dependencies.deploymentSpecDigest,
  });
  if (!record || !spec || record.phase === 'decommissioned') return;
  await dependencies.beforeCleanup?.(spec, record);
  if (DECOMMISSIONABLE_PHASES.has(record.phase)) {
    await dependencies.decommissionDeployment({
      backend: dependencies.backend,
      store: deployment.store,
      spec,
      secrets: deployment.secrets,
    });
    return;
  }
  await dependencies.cleanupDeploymentArtifacts({
    backend: dependencies.backend,
    store: deployment.store,
    spec,
  });
}

export function validateOperationalConformance(options) {
  const {
    plans,
    maintenanceCapabilityPublicKey,
    validateDeploymentSpec,
    validateDeploymentSecrets,
    validateExternalPlatformProfile,
    canonicalMaintenanceCapabilityPublicKey,
  } = options;
  if (!Array.isArray(plans) || plans.length !== 2) {
    throw new Error('operational conformance requires two deployment plans');
  }
  for (const plan of plans) {
    validateDeploymentSpec(plan.initialSpec);
    validateDeploymentSpec(plan.nextSpec);
    validateDeploymentSecrets(plan.initialSpec, plan.secrets);
    validateDeploymentSecrets(plan.nextSpec, plan.secrets);
    validateExternalPlatformProfile(plan.initialSpec, plan.initialProfile);
    validateExternalPlatformProfile(plan.nextSpec, plan.nextProfile);
  }
  for (const plan of plans) {
    if (
      canonicalMaintenanceCapabilityPublicKey(
        plan.initialProfile.maintenanceCapabilityPublicKey,
      ) !== maintenanceCapabilityPublicKey ||
      canonicalMaintenanceCapabilityPublicKey(
        plan.nextProfile.maintenanceCapabilityPublicKey,
      ) !== maintenanceCapabilityPublicKey
    ) {
      throw new Error(
        'maintenance capability signer does not match the operational profiles',
      );
    }
  }
}

export async function runCredentialedConformance(config, dependencies) {
  if (!Array.isArray(config.deployments) || config.deployments.length !== 2) {
    throw new Error('credentialed conformance requires two deployments');
  }
  for (const operation of REQUIRED_OPERATIONS) {
    if (typeof dependencies[operation] !== 'function') {
      throw new Error(`credentialed conformance requires ${operation}`);
    }
  }

  let conformanceError;
  try {
    for (const deployment of config.deployments) {
      await dependencies.provisionV1(deployment);
      await dependencies.probeV1(deployment);
    }
    await dependencies.assertTenantIsolation(config.deployments);
    await dependencies.activateV2();
    for (const deployment of config.deployments) {
      await dependencies.migrateV2(deployment);
      await dependencies.probeV2(deployment);
      await dependencies.completeFlowSafe(deployment);
      await dependencies.rollback(deployment);
    }
    await dependencies.proveNonemptyDecommission(config.deployments[0]);
    for (const deployment of config.deployments) {
      await dependencies.decommission(deployment);
    }
    await dependencies.assertZeroResiduals();
  } catch (error) {
    conformanceError = error;
  }

  const cleanupErrors = [];
  for (const deployment of config.deployments) {
    try {
      await dependencies.cleanup(deployment);
    } catch (error) {
      cleanupErrors.push(error);
    }
  }
  if (conformanceError !== undefined && cleanupErrors.length > 0) {
    throw new AggregateError(
      [conformanceError, ...cleanupErrors],
      'credentialed conformance and cleanup failed',
    );
  }
  if (conformanceError !== undefined) throw conformanceError;
  if (cleanupErrors.length > 0) {
    throw new AggregateError(cleanupErrors, 'credentialed cleanup failed');
  }
  return Object.freeze({
    ok: true,
    contractVersion1: true,
    sentinelMismatchRejected: true,
    maintenanceArmed: true,
    sameNameSecretsPreserved: true,
    externalReleaseFlipAndRollback: true,
    platformResourceGroups: true,
    d1StateRetained: true,
    durableObjectMigrationRetainedState: true,
    staticEgressAttribution: true,
    applicationBindingsProbed: true,
    applicationR2LifecycleProbed: true,
    nonemptyR2DecommissionRejected: true,
    auditProxyProbed: true,
    connectorAndStateEgressProbed: true,
    webSocketNonceEchoed: true,
    cpuLimitAndRecoveryProbed: true,
    flowSafeApprovalRoundTripProbed: true,
    zeroOrphans: true,
  });
}
