// SPDX-License-Identifier: Apache-2.0

/**
 * Render the operator configuration for `pnpm fleet-control:credentialed` from
 * `src/conformance/contract.json`, the same file the artifacts compile against.
 * Every value the artifacts and the gate must agree on is derived here; only
 * account-specific values are placeholders an operator replaces.
 *
 * Writing is this script's only job. The staleness check lives in
 * `scripts/conformance-config-check.test.mjs`, which calls
 * `conformanceConfigIsCurrent` alongside fleet control's own validators — one
 * command that fails for every reason the configuration can be wrong.
 */

import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const contractPath = fileURLToPath(
  new URL('../src/conformance/contract.json', import.meta.url),
);
export const CONFORMANCE_CONFIG_PATH = fileURLToPath(
  new URL('../conformance/anchorage-starter.conformance.json', import.meta.url),
);

/**
 * Bundle paths are relative to `packages/fleet-control`: the runner resolves
 * them against its own working directory, not against this file.
 * (`packages/fleet-control/package.json` `test:credentialed`.)
 */
const ARTIFACT_DIRECTORY = '../agent-starter/dist/conformance';

/** Lowercase on purpose: `new URL(...).hostname` lowercases, and the validator
 * compares that hostname against `organizationEgressHosts` by equality. */
const ALLOWED_UPSTREAM_URL =
  'https://allowed-upstream.replace-me.example/probe';
const DENIED_UPSTREAM_URL = 'https://denied-upstream.replace-me.example/probe';

const TENANT_TAGS = ['tenanta', 'tenantb'];

export function buildConformanceConfig() {
  const contract = JSON.parse(readFileSync(contractPath, 'utf8'));
  const stateMigrationV1 = {
    tag: contract.stateMigrationTags.v1,
    newSqliteClasses: [
      ...contract.durableObjectBindings.map((binding) => binding.className),
      contract.auditProxyClassName,
    ],
  };
  const stateMigrationV2 = {
    tag: contract.stateMigrationTags.v2,
    newSqliteClasses: [contract.newDurableObjectBinding.className],
  };
  const stateWorker = (version) => ({
    bundle: `${ARTIFACT_DIRECTORY}/trusted-state-${version}.mjs`,
    mainModule: contract.stateMainModule,
    compatibilityDate: contract.compatibilityDate,
    compatibilityFlags: contract.compatibilityFlags,
  });

  return {
    contractVersion: contract.contractVersion,
    tenantTags: TENANT_TAGS,
    environment: 'conformance',
    dispatchNamespace: 'anchorage-conformance',
    hostRoutingKvId: 'REPLACE_WITH_SCRATCH_KV_NAMESPACE_ID',
    sharedOutboundWorkerName: 'anchorage-conformance-outbound',
    auditQueueName: 'anchorage-conformance-audit',
    maintenanceBaseUrls: Object.fromEntries(
      TENANT_TAGS.map((tenantTag) => [
        tenantTag,
        `https://control-${tenantTag}.replace-me.example`,
      ]),
    ),
    routeHostnames: Object.fromEntries(
      TENANT_TAGS.map((tenantTag) => [
        tenantTag,
        `${tenantTag}.replace-me.example`,
      ]),
    ),
    workerBundle: `${ARTIFACT_DIRECTORY}/candidate.mjs`,
    mainModule: contract.candidateMainModule,
    compatibilityDate: contract.compatibilityDate,
    compatibilityFlags: contract.compatibilityFlags,
    schemaVersion: contract.schemaVersion,
    migrations: [],
    cpuLimitMs: contract.cpuLimitMs,
    subrequestLimit: contract.subrequestLimit,
    durableObjectBindings: contract.durableObjectBindings,
    application: {
      vars: [
        {
          name: contract.applicationVariableName,
          value: contract.applicationVariableValue,
        },
      ],
      // Empty by contract: the gate derives the one application secret
      // descriptor from FLEET_CONFORMANCE_APPLICATION_SECRET at runtime.
      secrets: [],
      r2Buckets: [
        { name: contract.applicationR2Binding, jurisdiction: 'default' },
      ],
    },
    applicationSecretBinding: contract.applicationSecretBinding,
    conformance: {
      httpPath: contract.httpPath,
      webSocketPath: contract.webSocketPath,
      allowedUpstreamUrl: ALLOWED_UPSTREAM_URL,
      deniedUpstreamUrl: DENIED_UPSTREAM_URL,
      deniedUpstreamStatus: 403,
      cpuOverLimitStatus: 500,
      applicationVariableName: contract.applicationVariableName,
      applicationVariableValue: contract.applicationVariableValue,
      newDurableObjectBinding: contract.newDurableObjectBinding,
    },
    platformProfile: {
      runtimeContractVersion: contract.contractVersion,
      backwardCompatibleWithRetainedReleases: true,
      maintenanceCapabilityPublicKey:
        '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"replace-with-active-kid","x":"replace-with-43-character-base64url-public-key"}',
      stateProfiles: [
        {
          name: 'v1',
          stateWorker: stateWorker('v1'),
          stateDurableObjectMigrations: [stateMigrationV1],
        },
        {
          name: 'v2',
          stateWorker: stateWorker('v2'),
          // The complete v1 history repeated, then exactly one appended
          // migration. The validator compares the prefix with JSON.stringify,
          // so both entries must be the same object shape.
          stateDurableObjectMigrations: [stateMigrationV1, stateMigrationV2],
        },
      ],
      // Derived, never written by hand: the validator compares this against
      // new URL(allowedUpstreamUrl).hostname, which is lowercased.
      organizationEgressHosts: [new URL(ALLOWED_UPSTREAM_URL).hostname],
    },
    exportDirectory: '/tmp/anchorage-conformance-exports',
  };
}

export function renderConformanceConfig() {
  return `${JSON.stringify(buildConformanceConfig(), undefined, 2)}\n`;
}

/**
 * Compare CONTENT, not bytes. Biome owns the committed file's layout and
 * collapses short arrays that JSON.stringify expands, so a byte comparison
 * would report drift after every `pnpm lint`. Every meaningful change still
 * fails this.
 */
export function conformanceConfigIsCurrent(current) {
  try {
    return (
      JSON.stringify(JSON.parse(current)) ===
      JSON.stringify(buildConformanceConfig())
    );
  } catch {
    return false;
  }
}

function main() {
  writeFileSync(CONFORMANCE_CONFIG_PATH, renderConformanceConfig());
  console.log(`wrote ${CONFORMANCE_CONFIG_PATH}`);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) main();
