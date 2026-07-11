// SPDX-License-Identifier: Apache-2.0
// The catalog/runtime consistency check.
//
// createRunRouter looks a workflow up by WorkflowMeta.id, but start/resume route
// by the id the workflow was COMMITTED under (createWorkflow's id). Nothing in
// the type system ties the two together, so a typo in a meta surfaces as a
// mysterious 404 on a workflow the runtime is genuinely hosting. Every host
// asserts the two agree at registration time instead — a startup error, not a
// runtime mystery. Shared so no host can forget it or skew its wording.

import type { RunnerRuntime } from '../do-runner/index.js';
import type { WorkflowMeta } from './workflow-meta.js';

/** Structural view of the runtime: only the registered-ids listing is needed. */
export type WorkflowIdSource = Pick<RunnerRuntime, 'workflowIds'>;

/**
 * Throw unless every advertised meta id was actually committed onto `runtime`.
 * Call once, after registering all workflows and before serving traffic.
 */
export function assertWorkflowsRegistered(
  runtime: WorkflowIdSource,
  workflows: ReadonlyArray<WorkflowMeta>,
): void {
  const registered = new Set(runtime.workflowIds());
  for (const meta of workflows) {
    if (!registered.has(meta.id)) {
      throw new Error(
        `workflow '${meta.id}' is advertised but was never registered on the runtime`,
      );
    }
  }
}
