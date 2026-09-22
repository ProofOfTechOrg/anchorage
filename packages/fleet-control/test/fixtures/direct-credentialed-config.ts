// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import {
  deriveDirectConformanceNames,
  validateDirectConformanceConfig,
} from '../../scripts/direct-credentialed-conformance-config.mjs';
import type { DirectRunManifest } from '../../scripts/direct-credentialed-conformance-preflight.mjs';

export const DIRECT_FIXTURE_PROVIDER = Object.freeze({
  accountWorkersDevSubdomain: 'direct-fixture',
});

export function directFixtureManifest(
  overrides: Readonly<
    Partial<
      Pick<
        DirectRunManifest['referenceRuntime'],
        'maxProviderRequests' | 'invocationTimeoutMs'
      >
    >
  > = {},
): DirectRunManifest {
  const raw = readFileSync(
    new URL(
      '../../scripts/direct-credentialed-conformance.example.json',
      import.meta.url,
    ),
  );
  const parsed = JSON.parse(raw.toString('utf8'));
  Object.assign(parsed.referenceWorker, overrides);
  const config = validateDirectConformanceConfig(parsed, {
    now: Date.parse('2026-09-09T12:00:00Z'),
  });
  const configBytes =
    Object.keys(overrides).length === 0
      ? raw
      : Buffer.from(JSON.stringify(parsed));
  const { artifact: _referenceArtifact, ...referenceRuntime } =
    config.referenceWorker;
  const {
    artifact: _tenantArtifact,
    spec,
    ...deploymentRuntime
  } = config.deployment;
  const source =
    'export class Maintenance {} export class Runner {} export default {};';
  return {
    contractVersion: config.contractVersion,
    configSha256: createHash('sha256').update(configBytes).digest('hex'),
    resourcePrefix: config.resourcePrefix,
    environment: config.environment,
    names: deriveDirectConformanceNames(config),
    referenceRuntime,
    deploymentRuntime,
    tenantModule: {
      name: 'worker.js',
      source,
      contentType: 'application/javascript+module',
      byteLength: Buffer.byteLength(source),
      sha256: createHash('sha256').update(source).digest('hex'),
    },
    tenantWasm: [],
    fixtureVersion: spec.fixtureVersion,
    interruption: config.interruption,
  };
}
