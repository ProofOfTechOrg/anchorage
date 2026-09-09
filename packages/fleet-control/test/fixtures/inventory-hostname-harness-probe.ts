// SPDX-License-Identifier: Apache-2.0

import {
  assertApplicationR2ReservationIdentity,
  reserveApplicationR2Resources,
} from '../../src/application-bindings.js';
import {
  advanceCloudflareFleetInventoryStage,
  type CloudflareFleetInventoryDeps,
} from '../../src/cloudflare-fleet-inventory.js';
import {
  emptyFleetInventoryRowCounts,
  FleetInventoryFindingValueError,
} from '../../src/fleet-inventory-state.js';
import { generateDeploymentSecrets } from '../../src/secrets.js';
import type { DeploymentSpec } from '../../src/types.js';

function unexpected(): never {
  throw new Error('unexpected provider operation');
}

export default {
  async fetch(request: Request): Promise<Response> {
    if (new URL(request.url).pathname === '/reservation') {
      const spec: DeploymentSpec = {
        tenantTag: 'tenanta',
        environment: 'prod',
        scriptName: 'fleet-tenanta-prod',
        databaseName: 'fleet-tenanta-prod',
        compatibilityDate: '2026-08-06',
        mainModule: 'worker.js',
        modules: [
          {
            name: 'worker.js',
            content: 'export default {}',
            contentType: 'application/javascript+module',
          },
        ],
        authoredBy: 'platform',
        schemaVersion: 0,
        migrations: [],
        durableObjectMigrations: [],
        durableObjectBindings: [],
        maintenanceBaseUrl: 'https://fleet.example.test',
        routeHostname: 'tenanta.example.test',
        application: { vars: [], secrets: [], r2Buckets: [{ name: 'FILES' }] },
      };
      const first = reserveApplicationR2Resources(spec);
      const second = reserveApplicationR2Resources(spec);
      for (const resource of [...first, ...second])
        assertApplicationR2ReservationIdentity(spec, resource);
      return Response.json({
        first,
        second,
        secrets: generateDeploymentSecrets(),
      });
    }
    const hostname = new URL(request.url).searchParams.get('hostname');
    if (hostname === null) return new Response(null, { status: 400 });
    const deps: CloudflareFleetInventoryDeps = {
      get attachmentScan() {
        return unexpected();
      },
      dispatchNamespace: unexpected,
      isDispatchCapabilityError: () => false,
      listHostRoutingKeys: unexpected,
      readHostRoutingValue: unexpected,
      inspectDispatchWorker: unexpected,
      getDispatchNamespace: unexpected,
      listCustomDomains: async () => ({
        domains: [{ hostname, service: 'anchorage-missing' }],
      }),
      listWorkerRouteZoneIds: async () => [],
      listZoneRoutes: unexpected,
      listOrdinaryScripts: async () => ({ scripts: [] }),
      readOrdinaryScriptDetail: unexpected,
      listDatabases: unexpected,
      listDurableObjectNamespaces: unexpected,
      listR2Buckets: unexpected,
    };
    const stage = { step: 'route-claims' } as const;
    try {
      const result = await advanceCloudflareFleetInventoryStage(deps, {
        stage,
        options: {
          databaseNamePrefix: 'anchorage-db-',
          scriptNamePrefix: 'anchorage-',
          includeDispatchNamespace: false,
          includeR2Buckets: false,
        },
        progress: {
          stage,
          generation: 1,
          revision: 0,
          stagedCounts: emptyFleetInventoryRowCounts(),
          factCount: 0,
          providerRequests: 0,
        },
        maxProviderRequests: 9,
      });
      return Response.json(result);
    } catch (error) {
      if (error instanceof FleetInventoryFindingValueError) {
        return Response.json({ error: error.name }, { status: 400 });
      }
      throw error;
    }
  },
};
