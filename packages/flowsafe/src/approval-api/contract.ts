// The breakwater <-> flowsafe wire contract, mirrored by value.
//
// flowsafe deliberately does NOT import @proofoftech/breakwater at runtime:
// the packages compose over documented requestContext keys
// (docs/security-threat-model.md, trust boundary 6), and a
// runtime dependency would force breakwater's dist to exist before flowsafe
// typechecks (the repo gate runs typecheck before build). Literal equality
// with breakwater's exported constants is enforced by the cross-package
// contract tests in end-to-end.test.ts — drift fails the suite, not
// production.

// The requestContext key literals live in do-runner/breakwater-keys.ts —
// the runtime mints the workflow-scope key itself, and homing the literals
// in a do-runner leaf keeps approval-api -> do-runner as the only
// cross-directory dependency direction. Re-exported here because this
// module is the approval-api's contract surface.
export {
  BREAKWATER_ACTOR_KEY,
  BREAKWATER_APPROVED_CONNECTORS_KEY,
  BREAKWATER_WORKFLOW_SCOPE_KEY,
} from '../do-runner/breakwater-keys.js';

/** breakwater's five roles (security-threat-model.md, RBAC model). */
export type ApprovalRole =
  | 'admin'
  | 'builder'
  | 'operator'
  | 'reviewer'
  | 'viewer';

export const APPROVAL_ROLES: readonly ApprovalRole[] = [
  'admin',
  'builder',
  'operator',
  'reviewer',
  'viewer',
];

/** The acting principal, same shape as breakwater's Actor. */
export interface ApprovalActor {
  id: string;
  role: ApprovalRole;
}

/**
 * Audit event the approval service emits — structurally assignable to
 * breakwater AuditLogger.record's argument, so wiring is one line:
 * `audit: (event) => auditLogger.record(event)`.
 */
export interface ApprovalAuditEvent {
  actor: ApprovalActor | null;
  /** Dotted verb, e.g. 'approval.decide'. */
  action: string;
  /** What was acted on, e.g. 'approval:<id>'. */
  resource: string;
  decision: 'allowed' | 'denied' | 'error';
  reason?: string;
  detail?: Record<string, unknown>;
}

export type ApprovalAuditSink = (event: ApprovalAuditEvent) => void;
