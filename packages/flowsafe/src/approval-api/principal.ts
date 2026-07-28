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
  APPROVAL_ROLES,
  type ApprovalActor,
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
      /** Only an agent delegates; `never` makes a wrong shape a type error. */
      delegatedBy?: never;
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
      delegatedBy?: never;
    };

/**
 * The trusted-automation brand, following the TENANT_BOUND idiom for the same
 * reason: TypeScript is structural, so a parameter typed plain
 * `ExecutionPrincipal` is satisfied by any object literal. Without this, the
 * service's automated entries authorize on the CALLER'S OWN ASSERTION that it
 * is a robot — which would give automation strictly more create authority than
 * a human `viewer`.
 *
 * The symbol is exported because declaration emit cannot reference a
 * module-private name from an exported type. The residual is the same one
 * TENANT_BOUND accepts and is grep-visible: forging requires
 * `import { TRUSTED_AUTOMATION }` plus stamping it, a deliberate TCB bypass on
 * par with an `as` cast. What it eliminates is accidental satisfaction and the
 * rushed fix that hands a request-derived principal to a trusted entry.
 */
export const TRUSTED_AUTOMATION: unique symbol = Symbol(
  'flowsafe.trustedAutomation',
);

/**
 * An automated principal that trusted platform code vouched for. The only kind
 * accepted by `ApprovalService.createAsPrincipal` and
 * `supersedeStaleAsPrincipal`.
 */
export type TrustedAutomationPrincipal = Extract<
  ExecutionPrincipal,
  { kind: AutomatedPrincipalKind }
> & { readonly [TRUSTED_AUTOMATION]: true };

/**
 * Vouch for an automated principal. Calling this IS the trust assertion, which
 * is why it is a named function rather than a cast: it should be greppable, and
 * it should never appear on a path that took the principal from a request.
 *
 * Throws on a human or a malformed principal — those must use the
 * role-authorized entries.
 */
export function trustAutomationPrincipal(
  principal: ExecutionPrincipal,
): TrustedAutomationPrincipal {
  if (!isExecutionPrincipal(principal) || principal.kind === 'human') {
    throw new Error(
      'execution principal: only a valid automated principal can be trusted for platform work',
    );
  }
  return principal as TrustedAutomationPrincipal;
}

/**
 * Bounded, non-empty, and free of control characters.
 *
 * Not an injection barrier — the principal reaches the wire through
 * `JSON.stringify`, which escapes U+0000–U+001F. It matters because `id` ALSO
 * travels raw in `x-flowsafe-actor`, and because refusing here produces a clean
 * validation error instead of a `Headers.set` TypeError deep in the topology.
 * Mirrors the `containsHeaderControl` check `createTenantResolver` already
 * applies to an actor id.
 */
function boundedText(value: unknown, max: number): value is string {
  if (typeof value !== 'string' || value.trim() === '' || value.length > max) {
    return false;
  }
  for (const character of value) {
    const code = character.charCodeAt(0);
    if (code <= 0x1f || code === 0x7f) return false;
  }
  return true;
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
    throw new Error(`execution principal: ${label} is malformed`);
  }
  if (value.tenantId !== expectedTenantId) {
    throw new Error(
      `execution principal: ${label} belongs to tenant '${value.tenantId}', not '${expectedTenantId}'`,
    );
  }
  return value;
}

/**
 * Structural equality across every kind-specific field.
 *
 * Cast-free: the union narrows on `kind`, so a future variant with a new
 * discriminating field becomes a compile error here rather than silently
 * comparing equal. Three rebinding guards depend on that.
 */
export function samePrincipal(
  left: ExecutionPrincipal,
  right: ExecutionPrincipal,
): boolean {
  if (left.id !== right.id || left.tenantId !== right.tenantId) return false;
  if (left.kind === 'human' || right.kind === 'human') {
    return (
      left.kind === 'human' &&
      right.kind === 'human' &&
      left.role === right.role
    );
  }
  return (
    left.kind === right.kind &&
    left.purpose === right.purpose &&
    left.delegatedBy === right.delegatedBy
  );
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
  const { id, role } = breakwaterActorFor(principal);
  return { id, role, tenantId: principal.tenantId };
}

/**
 * The principal as breakwater's `Actor`. The ONE place the projection rule is
 * written; `principalActor` and the trusted request context both go through it
 * so the breakwater-facing and approval-facing identities cannot disagree.
 */
export function breakwaterActorFor(principal: ExecutionPrincipal): {
  id: string;
  role: ApprovalRole;
  kind: ExecutionPrincipalKind;
} {
  return {
    id: principal.id,
    role:
      principal.kind === 'human' ? principal.role : AUTOMATED_PROJECTED_ROLE,
    kind: principal.kind,
  };
}

/**
 * Serialize a principal for the trusted thread header. Every field is already
 * bounded and header-control-free (`boundedText`), so plain JSON is safe and
 * stays readable in a trace.
 */
export function encodeExecutionPrincipal(
  principal: ExecutionPrincipal,
): string {
  return JSON.stringify(
    principal.kind === 'human'
      ? { kind: 'human', id: principal.id, role: principal.role }
      : {
          kind: principal.kind,
          id: principal.id,
          purpose: principal.purpose,
          ...(principal.delegatedBy !== undefined
            ? { delegatedBy: principal.delegatedBy }
            : {}),
        },
  );
}

/**
 * Rebuild a principal from the trusted header, binding it to the tenant the DO
 * already authenticated. Fields are picked EXPLICITLY rather than spread, so an
 * attacker-supplied extra property cannot ride into DO storage or D1.
 *
 * Returns undefined on anything malformed; the caller fails closed.
 */
export function decodeExecutionPrincipal(
  header: string,
  tenantId: string,
): ExecutionPrincipal | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(header);
  } catch {
    return undefined;
  }
  if (parsed === null || typeof parsed !== 'object') return undefined;
  const fields = parsed as Record<string, unknown>;
  const candidate =
    fields.kind === 'human'
      ? { kind: 'human', id: fields.id, tenantId, role: fields.role }
      : {
          kind: fields.kind,
          id: fields.id,
          tenantId,
          purpose: fields.purpose,
          ...(fields.delegatedBy !== undefined
            ? { delegatedBy: fields.delegatedBy }
            : {}),
        };
  return isExecutionPrincipal(candidate) ? candidate : undefined;
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
