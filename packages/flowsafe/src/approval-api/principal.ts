// SPDX-License-Identifier: Apache-2.0
// ExecutionPrincipal — WHO is executing, as opposed to who approved.
//
// Before this existed the platform had one identity, ApprovalActor, whose only
// authority vocabulary is a human ApprovalRole. Every automated path therefore
// fabricated a human: the schedule tick, cron maintenance, signal-provider
// delivery, and the suspension-reconcile bridge all minted `role: 'operator'`.
// That loses provenance (nothing records WHY the call exists) and, worse, hands
// autonomous execution whatever an operator may do.
//
// ApprovalActor is deliberately NOT replaced. It stays exactly what it always
// was: the identity of an authenticated human at the HTTP boundary and of a
// reviewer deciding an approval. A human approving an agent's request is still
// attributed to that human — `decidedBy` is unaffected by anything here.
//
// This lives in approval-api rather than contract.ts because contract.ts is the
// breakwater wire contract, mirrored by value and pinned by the cross-package
// tests. A principal is a host concept: breakwater learns only the KIND (its
// `PrincipalKind`), never the tenant or the purpose.

import {
  type ApprovalActor,
  APPROVAL_ROLES,
  type ApprovalRole,
} from './contract.js';

/**
 * Mirrors breakwater's `PrincipalKind` by value, for the same reason
 * contract.ts mirrors the request-context keys: flowsafe does not import
 * breakwater at runtime. The cross-package contract test pins the equality.
 */
export type ExecutionPrincipalKind = 'human' | 'service' | 'agent' | 'system';

export const EXECUTION_PRINCIPAL_KINDS: readonly ExecutionPrincipalKind[] = [
  'human',
  'service',
  'agent',
  'system',
];

/** Automated kinds — everything that is not a logged-in person. */
export const AUTOMATED_PRINCIPAL_KINDS: readonly ExecutionPrincipalKind[] = [
  'service',
  'agent',
  'system',
];

export type AutomatedPrincipalKind = Exclude<ExecutionPrincipalKind, 'human'>;

/** Upper bound on the free-text provenance fields, so audit rows stay bounded. */
const MAX_PURPOSE_LENGTH = 200;
const MAX_PRINCIPAL_ID_LENGTH = 200;

/**
 * `purpose` is REQUIRED on every automated kind, not optional as the roadmap
 * sketch had it. The failure being fixed is that fabricated operators "lose
 * provenance"; an optional field would let each new automated path skip the one
 * thing that restores it. A human needs no purpose — the person is the reason.
 */
export type ExecutionPrincipal =
  | {
      kind: 'human';
      id: string;
      tenantId: string;
      role: ApprovalRole;
    }
  | {
      kind: 'service';
      id: string;
      tenantId: string;
      purpose: string;
    }
  | {
      kind: 'agent';
      id: string;
      tenantId: string;
      purpose: string;
      /** The principal that delegated this run, for agent-to-agent work. */
      delegatedBy?: string;
    }
  | {
      kind: 'system';
      id: string;
      tenantId: string;
      purpose: string;
    };

export class ExecutionPrincipalError extends Error {
  constructor(message: string) {
    super(`execution principal: ${message}`);
    this.name = 'ExecutionPrincipalError';
  }
}

function boundedText(value: unknown, max: number): value is string {
  return (
    typeof value === 'string' && value.trim() !== '' && value.length <= max
  );
}

/**
 * Structural validation only. The TENANT binding is checked separately by
 * `assertExecutionPrincipal`, because "is this shaped like a principal" and "is
 * this principal allowed here" are different questions and conflating them
 * produces call sites that answer neither.
 */
export function isExecutionPrincipal(
  value: unknown,
): value is ExecutionPrincipal {
  if (value === null || typeof value !== 'object') return false;
  const candidate = value as Partial<ExecutionPrincipal>;
  if (
    !boundedText(candidate.id, MAX_PRINCIPAL_ID_LENGTH) ||
    typeof candidate.tenantId !== 'string' ||
    candidate.tenantId === ''
  ) {
    return false;
  }
  if (candidate.kind === 'human') {
    return (
      typeof candidate.role === 'string' &&
      (APPROVAL_ROLES as readonly string[]).includes(candidate.role)
    );
  }
  if (
    candidate.kind !== 'service' &&
    candidate.kind !== 'agent' &&
    candidate.kind !== 'system'
  ) {
    return false;
  }
  if (!boundedText(candidate.purpose, MAX_PURPOSE_LENGTH)) return false;
  const delegatedBy = (candidate as { delegatedBy?: unknown }).delegatedBy;
  if (
    delegatedBy !== undefined &&
    (candidate.kind !== 'agent' ||
      !boundedText(delegatedBy, MAX_PRINCIPAL_ID_LENGTH))
  ) {
    return false;
  }
  return true;
}

/**
 * Validate a principal AND bind it to the tenant that is about to act on it.
 *
 * `tenantId` on a decoded principal crosses an authentication boundary exactly
 * as `ApprovalActor.tenantId` does — the type says `string`, and the type system
 * has no authority over a value read back out of D1 or a DO's storage.
 */
export function assertExecutionPrincipal(
  value: unknown,
  expectedTenantId: string,
  label: string,
): ExecutionPrincipal {
  if (!isExecutionPrincipal(value)) {
    throw new ExecutionPrincipalError(`${label} is malformed`);
  }
  if (value.tenantId !== expectedTenantId) {
    throw new ExecutionPrincipalError(
      `${label} belongs to tenant '${value.tenantId}', not '${expectedTenantId}'`,
    );
  }
  return value;
}

export function isAutomatedPrincipal(
  principal: ExecutionPrincipal,
): principal is Extract<ExecutionPrincipal, { kind: AutomatedPrincipalKind }> {
  return principal.kind !== 'human';
}

/** Structural equality across every kind-specific field. */
export function samePrincipal(
  left: ExecutionPrincipal,
  right: ExecutionPrincipal,
): boolean {
  if (
    left.kind !== right.kind ||
    left.id !== right.id ||
    left.tenantId !== right.tenantId
  ) {
    return false;
  }
  if (left.kind === 'human') {
    return left.role === (right as typeof left).role;
  }
  const a = left as Extract<ExecutionPrincipal, { purpose: string }> & {
    delegatedBy?: string;
  };
  const b = right as typeof a;
  return a.purpose === b.purpose && a.delegatedBy === b.delegatedBy;
}

/**
 * The role an automated principal projects into breakwater's `Actor`.
 *
 * `Actor.role` is required, so an automated principal must carry SOME label.
 * breakwater's gate does not consult the role allowlist for a non-human kind,
 * making this value inert there — it is the least-privileged role precisely so
 * that any consumer which reads `actor.role` WITHOUT understanding `kind` gets
 * the minimum rather than the `operator` these paths used to fabricate.
 *
 * `viewer` also holds no decider role, so an automated principal projected onto
 * `ApprovalService` can never satisfy DECIDER_ROLES and approve anything.
 */
export const AUTOMATED_PROJECTED_ROLE: ApprovalRole = 'viewer';

/**
 * Project a principal onto the approval-service identity.
 *
 * Automated principals keep their own id — attribution stays truthful — while
 * borrowing the least-privileged role so that the approval service's own role
 * gates (CAN_CREATE, DECIDER_ROLES) treat them as read-only.
 */
export function principalActor(principal: ExecutionPrincipal): ApprovalActor {
  return {
    id: principal.id,
    role:
      principal.kind === 'human' ? principal.role : AUTOMATED_PROJECTED_ROLE,
    tenantId: principal.tenantId,
  };
}

/** An authenticated human at the HTTP boundary, as an execution principal. */
export function humanPrincipal(actor: ApprovalActor): ExecutionPrincipal {
  return {
    kind: 'human',
    id: actor.id,
    tenantId: actor.tenantId,
    role: actor.role,
  };
}

/** Correlation fields carried into every audit event for this principal. */
export function principalAuditFields(principal: ExecutionPrincipal): {
  principalKind: ExecutionPrincipalKind;
  principalId: string;
  purpose?: string;
  delegatedBy?: string;
} {
  if (principal.kind === 'human') {
    return { principalKind: 'human', principalId: principal.id };
  }
  return {
    principalKind: principal.kind,
    principalId: principal.id,
    purpose: principal.purpose,
    ...(principal.kind === 'agent' && principal.delegatedBy !== undefined
      ? { delegatedBy: principal.delegatedBy }
      : {}),
  };
}
