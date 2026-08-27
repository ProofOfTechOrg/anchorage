// SPDX-License-Identifier: Apache-2.0

import { CloudflareApiPlainWorkerProvisioningApi } from './cloudflare-api-plain-worker-provisioning-api.js';
import { CloudflareProvisioningClient } from './cloudflare-client.js';
import { PlainWorkerBackend } from './plain-worker-backend.js';

/** Options for the direct Cloudflare API ordinary-Worker backend. */
export interface CloudflareApiPlainWorkerBackendOptions {
  /**
   * Plain-Worker Cloudflare client. Database teardown also requires the client
   * to have been constructed with a durable database export store.
   */
  readonly client: CloudflareProvisioningClient;
  readonly fetch?: typeof fetch;
  readonly maintenanceRequestTimeoutMs?: number;
  /** Stamps `observedAt` on an attestation. Injected so it can be pinned. */
  readonly clock?: () => number;
}

/**
 * Ordinary-Worker backend that uses Cloudflare APIs without invoking Wrangler.
 *
 * It preserves the reconciliation, ingress, maintenance, promotion, and
 * teardown policy documented by {@link PlainWorkerBackend}. Construct the
 * client with `plane: 'plain-worker'`; a dispatch namespace is neither needed
 * nor accepted. D1 deletion remains fail closed when the account cannot
 * enumerate Workers for Platforms namespaces during attachment scans.
 *
 * A failed upload reconciled by tag rediscovery is accepted only when the
 * Worker footprint attests the intended workers.dev and preview-URL state.
 * Cloudflare error `10220` can also prevent a deployment when secrets changed
 * since the uploaded version.
 */
export class CloudflareApiPlainWorkerBackend extends PlainWorkerBackend {
  constructor(options: CloudflareApiPlainWorkerBackendOptions) {
    if (!(options.client instanceof CloudflareProvisioningClient)) {
      throw new TypeError(
        'client must be a CloudflareProvisioningClient instance',
      );
    }
    super({
      api: new CloudflareApiPlainWorkerProvisioningApi({
        client: options.client,
      }),
      identityCaller: 'CloudflareApiPlainWorkerBackend.seedDeploymentIdentity',
      fetch: options.fetch,
      maintenanceRequestTimeoutMs: options.maintenanceRequestTimeoutMs,
      clock: options.clock,
    });
  }
}
