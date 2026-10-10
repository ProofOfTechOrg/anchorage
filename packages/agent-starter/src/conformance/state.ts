// SPDX-License-Identifier: Apache-2.0

import {
  createFlowsafeMaintenanceDurableObject,
  createFlowsafeWorker,
  type FlowsafeWorkerConfig,
} from '@proofoftech/flowsafe/host-kit';

import type { ConformanceStateEnv } from './env.js';
import { conformanceRunnerLifecycleConfig } from './state-durable-objects.js';
import { CONFORMANCE_WORKFLOWS } from './workflow.js';

export { FlowsafeFleetAuditProxy } from '@proofoftech/flowsafe/audit-export';
export {
  ConformanceRunner,
  ConformanceState,
} from './state-durable-objects.js';

/**
 * The trusted state artifact's shared body. `state-v1.ts` and `state-v2.ts`
 * both re-export this; v2 adds exactly one class, matching the one migration
 * its profile appends. Neither file edits the other, which is what keeps the
 * Durable Object history append-only.
 *
 * The ordinary Worker teardown proof also uploads this entry and uses its
 * fetch handler for maintenance.
 */
const stateWorkerConfig = {
  ...conformanceRunnerLifecycleConfig,
  workflows: CONFORMANCE_WORKFLOWS,
  // No token verifier: nothing authenticates to this Worker as a user. Every
  // request either carries the fleet maintenance capability, which the
  // maintenance route verifies on its own, or is denied.
  buildVerifier: () => ({ verify: async () => undefined }),
  maintenance: {
    sweepIntervalMs: 5 * 60 * 1_000,
    purgeIntervalMs: 60 * 60 * 1_000,
  },
} satisfies FlowsafeWorkerConfig<ConformanceStateEnv>;

export class Maintenance extends createFlowsafeMaintenanceDurableObject(
  stateWorkerConfig,
) {}

const worker = createFlowsafeWorker<ConformanceStateEnv>(stateWorkerConfig);

export default {
  fetch: (request: Request, env: ConformanceStateEnv, ctx: ExecutionContext) =>
    worker.fetch(request, env, ctx),
};
