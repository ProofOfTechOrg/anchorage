// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { canonicalApplicationBindings } from './application-bindings.js';
import type { DeploymentSpec } from './types.js';

function contentDigest(content: string | Uint8Array): string {
  return createHash('sha256').update(content).digest('hex');
}

export function deploymentSpecDigest(spec: DeploymentSpec): string {
  const application = canonicalApplicationBindings(spec);
  const canonical = {
    tenantTag: spec.tenantTag,
    environment: spec.environment,
    scriptName: spec.scriptName,
    databaseName: spec.databaseName,
    compatibilityDate: spec.compatibilityDate,
    compatibilityFlags: spec.compatibilityFlags ?? [],
    mainModule: spec.mainModule,
    modules: spec.modules.map((module) => ({
      name: module.name,
      contentType: module.contentType ?? null,
      contentSha256: contentDigest(module.content),
    })),
    authoredBy: spec.authoredBy,
    schemaVersion: spec.schemaVersion,
    migrations: spec.migrations,
    durableObjectMigrations: spec.durableObjectMigrations,
    previousDurableObjectTag: spec.previousDurableObjectTag ?? null,
    durableObjectBindings: spec.durableObjectBindings,
    queueProducer: spec.queueProducer ?? null,
    egressProxyService: spec.egressProxyService ?? null,
    maintenanceBaseUrl: spec.maintenanceBaseUrl,
    routeHostname: spec.routeHostname,
    cpuLimitMs: spec.cpuLimitMs ?? null,
    subrequestLimit: spec.subrequestLimit ?? null,
    application: {
      vars: application.vars,
      secrets: application.secrets,
      r2Buckets: application.r2Buckets,
    },
  };
  return createHash('sha256').update(JSON.stringify(canonical)).digest('hex');
}
