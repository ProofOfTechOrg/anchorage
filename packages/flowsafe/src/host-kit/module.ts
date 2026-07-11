// SPDX-License-Identifier: Apache-2.0
// The WorkflowModule contract: a self-contained workflow (its metadata + a
// register() that builds its steps/connectors and commits it onto a shared
// runtime). One RunnerRuntime hosts many modules — register-before-first-run is
// satisfied by a single builder that calls every module's register() before the
// first start. Generic over Deps so this kit stays workflow-agnostic: a host
// (the showcase) supplies its own shared-infrastructure bag.
//
// Ships under its OWN subpath — `@proofoftech/flowsafe/host-kit/module` — not
// through host-kit's barrel. `WorkflowModuleContext.audit` is breakwater's
// AuditLogger, a class with private fields (so it cannot be mirrored
// structurally), and breakwater is a devDependency of this package. Re-exporting
// this from `./host-kit` would force every consumer of `createRunRouter` to
// resolve a breakwater type they do not need. Module authors already depend on
// breakwater — their connectors do — so the split costs them one import and
// keeps the route-mounting surface dependency-free.

import type { AuditLogger } from '@proofoftech/breakwater';

import type { InitResult } from '../do-runner/index.js';
import type { WorkflowMeta } from './workflow-meta.js';

export type { WorkflowMeta };

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
  /**
   * Build the steps + connectors and .commit() the workflow onto the runtime.
   *
   * INVARIANT the type system cannot enforce: a gate step's suspend payload
   * `connectors` MUST be a server-authored static literal (a module-level
   * `const`, not a value derived from the run's inputData). The approval bridge
   * copies it verbatim into the approval record, and an APPROVED record's
   * connectors ARE the minted grant — so deriving the list from run input would
   * let client input choose its own capability, re-opening the hole that
   * TCB_ONLY_CREATE_FIELDS closes at the HTTP boundary.
   */
  register(ctx: WorkflowModuleContext<Deps>): void;
}
