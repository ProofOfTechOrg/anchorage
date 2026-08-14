// SPDX-License-Identifier: Apache-2.0
// WorkflowMeta — the metadata a host advertises for a workflow, and the only
// part of the module contract the run router needs.
//
// Deliberately its own leaf module, separate from module.ts: `WorkflowModule`
// carries a breakwater `AuditLogger` in its registration context, and
// breakwater is a devDependency (flowsafe has no runtime dependency on it). A
// barrel that re-exported the module contract would pull that type into the
// public `./host-kit` .d.ts graph, so a consumer importing merely
// `createRunRouter` would need breakwater installed to typecheck. Keeping meta
// here lets index.ts export a breakwater-free surface.

import type { ApprovalRole } from '../approval-api/index.js';

/** Static metadata every workflow advertises to a launcher/host. */
export interface WorkflowMeta {
  /** Workflow id — MUST equal the registered createWorkflow id (PATH_SAFE_ID_PATTERN). */
  id: string;
  title: string;
  description: string;
  /** A ready-to-run inputData example a launcher can prefill. */
  sampleInput: unknown;
  /**
   * Roles permitted to mutate this workflow at the HTTP routes — start,
   * resume, and terminate. A role that may not start a workflow may not drive
   * or terminate its runs either. Reads stay coarse:
   * reviewer/viewer inspect runs they cannot drive. Omitted => only the
   * host's coarse start-role check applies. This is a route-level gate; the
   * approval queue's own role policy governs decisions, and in-step RBAC (if
   * any) is enforced separately inside the workflow.
   *
   * Must be a SUBSET of the host's coarse start-role set (RUN_START_ROLES in
   * approval-api/contract.ts): the coarse gate runs first, so any role listed
   * here that cannot start a run at all is silently dead.
   */
  allowedRoles?: readonly ApprovalRole[];
}
