// SPDX-License-Identifier: Apache-2.0

import { Buffer } from 'node:buffer';
import { createHash, randomBytes } from 'node:crypto';
import {
  plainWorkerIngressModule,
  validateDeploymentSecrets,
  validateDeploymentSpec,
} from '@proofoftech/fleet-control';
import {
  type CloudflareDeploymentSpec,
  type DeploymentSecrets,
  generateDeploymentSecrets,
} from '@proofoftech/fleet-control/cloudflare-control-plane';
import type { DirectConformanceNames } from './direct-credentialed-conformance-config.mjs';
import { DIRECT_MAX_UPLOAD_BYTES } from './direct-credentialed-conformance-limits.mjs';
import type { DirectRunManifest } from './direct-credentialed-conformance-preflight.mjs';

export type DirectFixtureRole = keyof DirectConformanceNames['roles'];
export type DirectFixtureRelease = 'initial' | 'next' | 'failed-recovery';

export interface DirectProviderContext {
  readonly accountWorkersDevSubdomain: string;
}

export function generateDirectDeploymentSecrets(): DeploymentSecrets {
  return Object.freeze({
    ...generateDeploymentSecrets(),
    application: Object.freeze({
      APP_PROBE_TOKEN: Buffer.from(randomBytes(32)).toString('base64url'),
    }),
  });
}

export function directDeploymentSpec(
  manifest: DirectRunManifest,
  role: DirectFixtureRole,
  release: DirectFixtureRelease,
  secrets: DeploymentSecrets,
  provider: DirectProviderContext,
): CloudflareDeploymentSpec {
  if (
    !['a', 'b', 'recovery'].includes(role) ||
    !['initial', 'next', 'failed-recovery'].includes(release) ||
    (release === 'failed-recovery' && role !== 'recovery')
  )
    throw new Error('invalid direct fixture selection');
  if (manifest.contractVersion !== 1 || manifest.fixtureVersion !== 1)
    throw new Error('invalid direct fixture version');
  const subdomain = provider?.accountWorkersDevSubdomain;
  if (
    typeof subdomain !== 'string' ||
    !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/u.test(subdomain)
  )
    throw new Error('invalid direct fixture account Workers.dev subdomain');
  const artifact = manifest.tenantModule;
  if (
    new TextEncoder().encode(artifact.source).byteLength !==
      artifact.byteLength ||
    createHash('sha256').update(artifact.source).digest('hex') !==
      artifact.sha256
  )
    throw new Error('invalid direct fixture artifact');
  const wasm = manifest.tenantWasm.map((module) => {
    const content = new Uint8Array(Buffer.from(module.base64, 'base64'));
    if (
      content.byteLength !== module.byteLength ||
      createHash('sha256').update(content).digest('hex') !== module.sha256
    )
      throw new Error('invalid direct fixture Wasm artifact');
    return { name: module.name, content, contentType: module.contentType };
  });
  const token = secrets.application?.APP_PROBE_TOKEN;
  if (typeof token !== 'string' || token.length === 0)
    throw new Error('missing direct fixture application secret');
  const names = manifest.names.roles[role];
  const maintenanceHostname = `${names.scriptName}.${subdomain}.workers.dev`;
  if (maintenanceHostname === names.routeHostname)
    throw new Error(
      'direct fixture maintenance and application hosts must differ',
    );
  const next = release === 'next';
  const failed = release === 'failed-recovery';
  const spec: CloudflareDeploymentSpec = {
    tenantTag: names.tenantTag,
    environment: manifest.environment,
    scriptName: names.scriptName,
    databaseName: names.databaseName,
    ...manifest.deploymentRuntime,
    compatibilityFlags: [...manifest.deploymentRuntime.compatibilityFlags],
    mainModule: artifact.name,
    modules: [
      {
        name: artifact.name,
        content: artifact.source,
        contentType: artifact.contentType,
      },
      ...wasm,
    ],
    authoredBy: 'platform',
    schemaVersion: next || failed ? 2 : 1,
    migrations: [
      {
        version: 1,
        sql: "CREATE TABLE direct_conformance_fixture (id INTEGER PRIMARY KEY, marker TEXT NOT NULL); INSERT INTO direct_conformance_fixture (id, marker) VALUES (1, 'initial');",
      },
      ...(next
        ? [
            {
              version: 2,
              sql: "ALTER TABLE direct_conformance_fixture ADD COLUMN release TEXT NOT NULL DEFAULT 'next'; UPDATE direct_conformance_fixture SET marker = 'next' WHERE id = 1;",
              rollbackCompatible: true as const,
            },
          ]
        : failed
          ? [
              {
                version: 2,
                sql: 'INSERT INTO direct_conformance_missing_table (value) VALUES (1);',
              },
            ]
          : []),
    ],
    durableObjectMigrations: [
      { tag: 'v1', newSqliteClasses: ['Maintenance', 'Runner'] },
    ],
    ...(next ? { previousDurableObjectTag: 'v1' } : {}),
    durableObjectBindings: [
      { name: 'MAINTENANCE', className: 'Maintenance' },
      { name: 'RUNNER', className: 'Runner' },
    ],
    maintenanceBaseUrl: `https://${maintenanceHostname}`,
    routeHostname: names.routeHostname,
    application: {
      vars: [{ name: 'APPLICATION_RELEASE', value: next ? '2' : '1' }],
      secrets: [
        {
          name: 'APP_PROBE_TOKEN',
          valueSha256: createHash('sha256').update(token).digest('hex'),
        },
      ],
      r2Buckets: [{ name: 'PROBE_BUCKET' }],
    },
  };
  validateDeploymentSpec(spec);
  validateDeploymentSecrets(spec, secrets);
  const modules = [...spec.modules, plainWorkerIngressModule(spec)];
  const uploadBytes = modules.reduce(
    (sum, module) => sum + Buffer.byteLength(module.content),
    0,
  );
  if (uploadBytes > DIRECT_MAX_UPLOAD_BYTES)
    throw new Error('direct fixture upload size exceeds the Workers limit');
  return spec;
}
