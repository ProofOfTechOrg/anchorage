// SPDX-License-Identifier: Apache-2.0

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
    const conformance = await mountConformanceRoutes(request, env);
    if (conformance) return conformance;
    return new Response('not found', { status: 404 });
  },
};
