// SPDX-License-Identifier: Apache-2.0

import { MAINTENANCE_INSTANCE_NAME } from '@proofoftech/flowsafe/host-kit';

import type { ConformanceCandidateEnv } from './env.js';
import { mountConformanceRoutes } from './routes.js';

/**
 * The external candidate artifact.
 *
 * It exports NO Durable Object class: fleet control resolves every Durable
 * Object binding to the deployment's stable trusted state script, and an
 * external specification that owned classes would be rejected before upload
 * (`docs/fleet-control.md`, "Promote and roll back external releases"). It also
 * exposes no `scheduled()` or `queue()` handler, which user Workers never do.
 *
 * This is the shape an external agent author submits: one fetch surface over
 * platform-owned durable state.
 */
export default {
  async fetch(
    request: Request,
    env: ConformanceCandidateEnv,
  ): Promise<Response> {
    const path = new URL(request.url).pathname;
    const operation =
      request.method === 'POST' && path === '/admin/ensure-maintenance'
        ? 'ensure'
        : request.method === 'GET' && path === '/admin/maintenance-status'
          ? 'status'
          : undefined;
    if (operation) {
      const id = env.MAINTENANCE.idFromName(MAINTENANCE_INSTANCE_NAME);
      return env.MAINTENANCE.get(id).fetch(
        new Request(`http://maintenance/${operation}`, request),
      );
    }
    const conformance = await mountConformanceRoutes(request, env);
    if (conformance) return conformance;
    return new Response('not found', { status: 404 });
  },
};
