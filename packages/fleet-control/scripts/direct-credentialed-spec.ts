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
import { directDeploymentModules } from './direct-credentialed-spec-modules.mjs';

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
  if (manifest.contractVersion !== 1 || manifest.fixtureVersion !== 1)
    throw new Error('invalid direct fixture version');
  const subdomain = provider?.accountWorkersDevSubdomain;
  if (
    typeof subdomain !== 'string' ||
    !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/u.test(subdomain)
  )
    throw new Error('invalid direct fixture account Workers.dev subdomain');
  const tenantModules = directDeploymentModules(manifest, role, release);
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
    mainModule: manifest.tenantModule.name,
    modules: tenantModules,
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
      vars: [
        { name: 'APPLICATION_RELEASE', value: next ? '2' : '1' },
        { name: 'APPROVAL_ALLOW_SELF_DECISION', value: 'true' },
      ],
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
  const uploadModules = [...spec.modules, plainWorkerIngressModule(spec)];
  const uploadBytes = uploadModules.reduce(
    (sum, module) => sum + Buffer.byteLength(module.content),
    0,
  );
  if (uploadBytes > DIRECT_MAX_UPLOAD_BYTES)
    throw new Error('direct fixture upload size exceeds the Workers limit');
  return spec;
}
