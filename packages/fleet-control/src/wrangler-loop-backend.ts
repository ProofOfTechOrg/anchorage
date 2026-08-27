// SPDX-License-Identifier: Apache-2.0

import type { DurableDatabaseExportStore } from './cloudflare-client.js';
import {
  PlainWorkerBackend,
  resolveMaintenanceRequestTimeoutMs,
} from './plain-worker-backend.js';
import type { PlainWorkerRouteApi } from './types.js';
import { WranglerPlainWorkerProvisioningApi } from './wrangler-plain-worker-provisioning-api.js';
import type { CommandRunner } from './wrangler-runner.js';

/**
 * Ordinary-Worker backend backed by Wrangler command execution.
 *
 * Active-route attestation reads through the provider API because Wrangler CLI
 * reads run outside the shared quota coordinator.
 */
export class WranglerLoopBackend extends PlainWorkerBackend {
  constructor(options: {
    readonly runner: CommandRunner;
    readonly routeApi: PlainWorkerRouteApi;
    readonly exportDirectory: string;
    readonly exportStore: DurableDatabaseExportStore;
    readonly fetch?: typeof fetch;
    readonly maintenanceRequestTimeoutMs?: number;
    /** Stamps `observedAt` on an attestation. Injected so it can be pinned. */
    readonly clock?: () => number;
  }) {
    const {
      runner,
      routeApi,
      exportDirectory,
      exportStore,
      fetch: fetchImplementation,
      maintenanceRequestTimeoutMs,
      clock,
    } = options;
    if (!exportDirectory) throw new Error('exportDirectory is required');
    if (!exportStore) throw new Error('exportStore is required');
    if (!routeApi) throw new Error('routeApi is required');
    const resolvedMaintenanceRequestTimeoutMs =
      resolveMaintenanceRequestTimeoutMs(maintenanceRequestTimeoutMs);
    super({
      api: new WranglerPlainWorkerProvisioningApi({
        runner,
        routeApi,
        exportDirectory,
        exportStore,
      }),
      identityCaller: 'WranglerLoopBackend.seedDeploymentIdentity',
      fetch: fetchImplementation,
      maintenanceRequestTimeoutMs: resolvedMaintenanceRequestTimeoutMs,
      clock,
    });
  }
}

export { plainWorkerIngressModule } from './plain-worker-backend.js';
