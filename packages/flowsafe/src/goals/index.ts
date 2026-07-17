// SPDX-License-Identifier: Apache-2.0
// Track F (M-005) — goals. Subpath-only (`@proofoftech/flowsafe/goals`), like
// agent-runner / background-tasks / signals: host-side wiring a consumer opts
// into, never the root barrel.

// The role-gated + audited objective HTTP surface (CI-M-005-001).
export {
  createObjectiveRouter,
  GOAL_REQUEST_CONTEXT_KEY,
  type ObjectiveAuditEvent,
  type ObjectiveAuditSink,
  type ObjectiveOperation,
  type ObjectiveRouter,
  type ObjectiveRouterOptions,
  type ObjectiveStore,
} from './objective-routes.js';
