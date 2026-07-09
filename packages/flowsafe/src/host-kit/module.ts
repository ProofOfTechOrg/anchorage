// The WorkflowModule contract: a self-contained workflow (its metadata + a
// register() that builds its steps/connectors and commits it onto a shared
// runtime). One RunnerRuntime hosts many modules — register-before-first-run is
// satisfied by a single builder that calls every module's register() before the
// first start. Generic over Deps so this kit stays workflow-agnostic: a host
// (the showcase) supplies its own shared-infrastructure bag.

import type { AuditLogger } from '@proofoftech/breakwater';

import type { ApprovalRole } from '../approval-api/index.js';
import type { InitResult } from '../do-runner/index.js';

/** Static metadata every workflow module advertises to a launcher/host. */
export interface WorkflowMeta {
  /** Workflow id — MUST equal the registered createWorkflow id (PATH_SAFE_ID_PATTERN). */
  id: string;
  title: string;
  description: string;
  /** A ready-to-run inputData example a launcher can prefill. */
  sampleInput: unknown;
  /**
   * Roles permitted to START this workflow at the HTTP route. Omitted => the
   * host's coarse start-role check applies. This is a route-level gate only;
   * in-step RBAC (if any) is enforced separately inside the workflow.
   *
   * Must be a SUBSET of the host's coarse start-role set (RUN_START_ROLES in
   * approval-api/contract.ts): the coarse gate runs first, so any role listed
   * here that cannot start a run at all is silently dead.
   */
  allowedRoles?: readonly ApprovalRole[];
}

/**
 * What a module's register() receives: the init()-derived builder factories
 * (createWorkflow is the register-intercepting wrapper; createStep is raw
 * Mastra) plus the shared, binding-gated infrastructure its connectors close
 * over. `deps` is generic so the kit imposes no host-specific shape.
 */
export interface WorkflowModuleContext<Deps = unknown> {
  createWorkflow: InitResult['createWorkflow'];
  createStep: InitResult['createStep'];
  /** Connector audit sink shared across every module on the runtime. */
  audit: AuditLogger;
  deps: Deps;
}

/** A workflow packaged for host-agnostic registration. */
export interface WorkflowModule<Deps = unknown> {
  meta: WorkflowMeta;
  /** Build the steps + connectors and .commit() the workflow onto the runtime. */
  register(ctx: WorkflowModuleContext<Deps>): void;
}
