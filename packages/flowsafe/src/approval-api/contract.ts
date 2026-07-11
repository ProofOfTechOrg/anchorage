// SPDX-License-Identifier: Apache-2.0
// The breakwater <-> flowsafe wire contract (mirrored by value), plus the
// flowsafe host role policy derived from it (RUN_START_ROLES).
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
import type { ApprovalRecord } from './types.js';

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

/**
 * Roles permitted to START a run at the HTTP route — the coarse start-role gate
 * every host applies to POST /runs, before any per-workflow allowedRoles check.
 * A strict subset of APPROVAL_ROLES; reviewer/viewer are review-only. A
 * host-level concept (breakwater has no equivalent), so there is no cross-package
 * mirror to keep.
 */
export const RUN_START_ROLES: readonly ApprovalRole[] = [
  'admin',
  'operator',
  'builder',
];

/**
 * The acting principal — breakwater's Actor shape plus the platform's tenant
 * dimension. breakwater stays tenant-agnostic (it is a standalone library);
 * flowsafe is the multi-tenant host, so ITS actor carries the tenant. The
 * e2e mirror tripwire pins "breakwater's Actor fields + tenantId".
 *
 * `tenantId` crosses an authentication boundary (bearer map or JWT claims):
 * every verifier must validate it against TENANT_ID_PATTERN (INV-3,
 * do-runner/path-safe-id.ts) before constructing an ApprovalActor — the type
 * says `string`, but the type system has no authority over a decoded token.
 */
export interface ApprovalActor {
  id: string;
  role: ApprovalRole;
  tenantId: string;
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

/**
 * The return value is IGNORED — unless it is a promise, whose rejection every
 * flowsafe caller contains (the same availability-over-export-reliability
 * policy as breakwater's AuditLogger.record, and what makes a composed
 * `combineAuditSinks(...)` with an async member safe to wire here directly).
 * Typed `unknown`, not `void | Promise<void>`: the union would forfeit TS's
 * void-return leniency and break every `(event) => events.push(event)`-shaped
 * sink, while `unknown` admits sync sinks, async sinks, and expression-bodied
 * shorthand alike.
 */
export type ApprovalAuditSink = (event: ApprovalAuditEvent) => unknown;

/**
 * A queue moment worth pushing at reviewers: a NEW request entered the queue
 * ('created' — fired only when a record is actually inserted, never on the
 * idempotent re-observation of an already-open step), or an open request
 * breached its SLA ('escalated', fired per record by the cron sweep).
 * Decisions are deliberately not notification events — the decider is looking
 * at the dashboard when they happen.
 */
export interface ApprovalNotificationEvent {
  type: 'created' | 'escalated';
  record: ApprovalRecord;
}

/**
 * The notification transport seam (email, Slack, pager — flowsafe ships NO
 * transport). Fire-and-forget with the same availability-over-delivery policy
 * as ApprovalAuditSink: a sink that throws or rejects never fails the
 * approval action that fired it — the failure is recorded to the AUDIT sink
 * as `approval.notify`/'error' and the action proceeds. Workers hosts whose
 * transport must outlive the response wrap it in ctx.waitUntil themselves
 * (the hostAuditSink keepAlive pattern): the service does not await the sink.
 *
 * EXPOSURE: the event carries the FULL ApprovalRecord — including the
 * workflow-authored `payload` (typically the suspend payload) and `summary`.
 * That is deliberately MORE than the audit trail ever emits (audit `detail`s
 * are scoped to ids), because a useful notification needs reviewer context.
 * A transport addressing a lower-trust channel (email, chat) must project
 * or redact the record itself — title/ids/priority/SLA usually suffice.
 */
export type ApprovalNotificationSink = (
  event: ApprovalNotificationEvent,
) => void | Promise<void>;
