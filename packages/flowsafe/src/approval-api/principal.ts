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
 *
 * Fields are `readonly` because a principal is an authorization snapshot, not a
 * mutable record: every consumer re-reads `kind` to decide what the holder may
 * do, so a principal that can change between two reads has no meaning. The
 * modifier is the compile-time half; `trustAutomationPrincipal` freezes the
 * runtime half. `readonly` is not checked in assignability, so producers may
 * still build one from an ordinary object literal.
 */
export type ExecutionPrincipal =
  | {
      readonly kind: 'human';
      readonly id: string;
      readonly tenantId: string;
      readonly role: ApprovalRole;
    }
  | {
      readonly kind: 'service';
      readonly id: string;
      readonly tenantId: string;
      readonly purpose: string;
      /** Only an agent delegates; `never` makes a wrong shape a type error. */
      readonly delegatedBy?: never;
    }
  | {
      readonly kind: 'agent';
      readonly id: string;
      readonly tenantId: string;
      readonly purpose: string;
      /** The principal that delegated this run, for agent-to-agent work. */
      readonly delegatedBy?: string;
    }
  | {
      readonly kind: 'system';
      readonly id: string;
      readonly tenantId: string;
      readonly purpose: string;
      readonly delegatedBy?: never;
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
 * module-private name from an exported type. It is deliberately absent from the
 * package barrel, but that is API-surface hygiene, NOT a capability boundary:
 * the brand is recoverable by reflection from any vouched principal
 * (`Object.getOwnPropertySymbols(maintenancePrincipal('x'))`), so in-process
 * code that means to forge one still can. That residual is the same one
 * TENANT_BOUND accepts, and it is deliberate — a TCB bypass on par with an `as`
 * cast. What the brand eliminates is accidental satisfaction and the rushed fix
 * that hands a request-derived principal to a trusted entry.
 */
export const TRUSTED_AUTOMATION: unique symbol = Symbol(
  'flowsafe.trustedAutomation',
);

/**
 * Any non-human principal — the shape a duty needs when it wants provenance but
 * derives no authority from the principal (the cron SLA sweep is the case).
 * Separate from `TrustedAutomationPrincipal` so the brand is demanded only
 * where it is actually read, which is `ApprovalService`'s two trusted entries.
 */
export type AutomatedExecutionPrincipal = Extract<
  ExecutionPrincipal,
  { kind: AutomatedPrincipalKind }
>;

/**
 * An automated principal that trusted platform code vouched for. The only kind
 * accepted by `ApprovalService.createAsPrincipal` and
 * `supersedeStaleAsPrincipal`.
 */
export type TrustedAutomationPrincipal = AutomatedExecutionPrincipal & {
  readonly [TRUSTED_AUTOMATION]: true;
};

/**
 * Vouch for an automated principal. Calling this IS the trust assertion, which
 * is why it is a named function rather than a cast: it should be greppable, and
 * it should never appear on a path that took the principal from a request.
 *
 * Throws on a human or a malformed principal — those must use the
 * role-authorized entries.
 *
 * Returns a CANONICAL CLONE, branded and frozen. Validating the caller's object
 * and handing the same reference back made the vouch time-of-check/time-of-use:
 * the caller kept a mutable alias, so a validated `system` principal could be
 * rewritten into `{kind:'human', role:'admin'}` after the check and before the
 * service read it — and `#authorizeAutomated` re-reads `kind`. The clone comes
 * from `canonicalPrincipal`, so it holds the exact values that were validated
 * and no extra property rides into the trusted computing base; the brand lets
 * consumers prove the value came from here, and the freeze keeps both true for
 * the object's whole life.
 */
export function trustAutomationPrincipal(
  principal: ExecutionPrincipal,
): TrustedAutomationPrincipal {
  // The brand is stamped onto the CANONICAL SNAPSHOT, never onto the argument.
  // Validating one object and minting from another is how a value that passed
  // the automated check came back out as a human admin; taking both from the
  // same read makes that impossible rather than merely difficult.
  const canonical = canonicalPrincipal(principal);
  if (canonical === undefined || canonical.kind === 'human') {
    throw new Error(
      'execution principal: only a valid automated principal can be trusted for platform work',
    );
  }
  return Object.freeze({
    ...canonical,
    [TRUSTED_AUTOMATION]: true as const,
  });
}

/**
 * Read one field as an OWN DATA property, or `undefined`.
 *
 * Every principal field is read through this rather than by `value.kind`,
 * because a plain read is not a stable observation of a value someone else
 * built: it walks the prototype chain, so an inherited getter answers it, and
 * an own accessor's getter can answer differently each time. `Object.freeze`
 * is no substitute — it constrains a value's own data properties and says
 * nothing about accessors, inherited fields, or virtual ones.
 *
 * This does NOT by itself prove the field cannot lie: over a Proxy with an
 * extensible target, `Object.getOwnPropertyDescriptor` is a trap as
 * unconstrained as `get`. What closes the class is that the single read is
 * captured — see `canonicalPrincipal`, which returns a snapshot built from
 * these reads, so nothing downstream re-reads the caller's object at all.
 *
 * The minter emits plain own data properties, so this refuses nothing real.
 */
function ownField(value: object, key: PropertyKey): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  return descriptor !== undefined && 'value' in descriptor
    ? descriptor.value
    : undefined;
}

/**
 * Does this value carry the brand AND still hold a valid automated shape?
 *
 * The parameter type alone proves nothing at runtime — TypeScript is erased,
 * and the ways in that remain (an `as` cast, a value rebuilt from storage or a
 * structured clone across a boundary) all produce something the type accepts.
 * This is the check that makes the brand load-bearing rather than decorative,
 * so the trusted service entries call it instead of trusting their signature.
 *
 * Deliberately NOT on the package barrel: it is the enforcement half of an
 * internal invariant, and exporting it would put a runtime shape check into the
 * public API that consumers would have to keep working across minor versions.
 */
export function isTrustedAutomationPrincipal(
  value: unknown,
): value is TrustedAutomationPrincipal {
  if (value === null || typeof value !== 'object') return false;
  return (
    canonicalAutomatedPrincipal(value) !== undefined &&
    // The brand is read as an OWN DATA property for the same reason every other
    // field is: a plain read would accept one inherited from a prototype, which
    // nobody stamped.
    ownField(value, TRUSTED_AUTOMATION) === true &&
    // A branded-but-UNFROZEN principal is by definition one somebody stamped
    // onto a live mutable object rather than minting. It is also what forces a
    // Proxy's descriptor and `get` channels to agree, since the spec invariants
    // only bind over a non-extensible target.
    Object.isFrozen(value)
  );
}

/**
 * Validate and return a canonical principal that is not a person.
 *
 * The returned snapshot contains the exact values that passed validation, so
 * callers never need to re-read a mutable or adversarial input. Kept off the
 * package barrel: this is an internal enforcement helper, not public API.
 */
export function canonicalAutomatedPrincipal(
  value: unknown,
): AutomatedExecutionPrincipal | undefined {
  const principal = canonicalPrincipal(value);
  return principal !== undefined && principal.kind !== 'human'
    ? principal
    : undefined;
}

/**
 * Bounded, non-empty, and free of control characters.
 *
 * Not an injection barrier for the wire — the principal travels through
 * `JSON.stringify`, which escapes U+0000–U+001F. It matters because these
 * fields do not stop at the wire: `id` becomes `requestedBy`/`decidedBy` in D1
 * and the actor on every audit row, and `purpose` rides into the SIEM export.
 * The bounds keep an audit row bounded; the control-character refusal keeps
 * those strings clean at the boundary rather than downstream. Mirrors the
 * `containsHeaderControl` check `createTenantResolver` applies to an actor id.
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
 * Validate AND canonicalize in one pass: every field is read exactly once, and
 * the result is a fresh plain object built from THOSE reads.
 *
 * Validating a caller's object and then reading it again is the mistake this
 * module keeps re-learning. The first version handed back the caller's own
 * mutable reference; the second cloned but re-read plainly, which an accessor
 * or a prototype getter could answer differently; reading through descriptors
 * narrows the channel but does not close it, because
 * `Object.getOwnPropertyDescriptor` is itself a Proxy trap and, over an
 * extensible target, is as unconstrained as `get`.
 *
 * Returning the snapshot ends the class rather than narrowing it: "validated"
 * and "used" become the same values, so no consumer can be handed something
 * other than what was checked, whatever the input was built from.
 *
 * Fields are picked EXPLICITLY. tsc catches a new kind (the returns stop being
 * exhaustive) and a new REQUIRED field (the literal stops matching), but NOT a
 * new optional one — that compiles clean and is silently dropped, so add it to
 * the pick by hand (`delegatedBy` is the existing example).
 */
function canonicalPrincipal(value: unknown): ExecutionPrincipal | undefined {
  if (value === null || typeof value !== 'object') return undefined;
  const id = ownField(value, 'id');
  const tenantId = ownField(value, 'tenantId');
  const kind = ownField(value, 'kind');
  if (
    !boundedText(id, MAX_PRINCIPAL_ID_LENGTH) ||
    typeof tenantId !== 'string' ||
    tenantId === ''
  ) {
    return undefined;
  }
  if (kind === 'human') {
    const role = ownField(value, 'role');
    return typeof role === 'string' &&
      (APPROVAL_ROLES as readonly string[]).includes(role)
      ? { kind: 'human', id, tenantId, role: role as ApprovalRole }
      : undefined;
  }
  if (kind !== 'service' && kind !== 'agent' && kind !== 'system') {
    return undefined;
  }
  const purpose = ownField(value, 'purpose');
  if (!boundedText(purpose, MAX_PURPOSE_LENGTH)) return undefined;
  const delegatedBy = ownField(value, 'delegatedBy');
  if (delegatedBy !== undefined) {
    return kind === 'agent' && boundedText(delegatedBy, MAX_PRINCIPAL_ID_LENGTH)
      ? { kind: 'agent', id, tenantId, purpose, delegatedBy }
      : undefined;
  }
  if (kind === 'agent') return { kind: 'agent', id, tenantId, purpose };
  if (kind === 'service') return { kind: 'service', id, tenantId, purpose };
  return { kind: 'system', id, tenantId, purpose };
}

/**
 * Structural validation only. The TENANT binding is checked separately by
 * `assertExecutionPrincipal`, because "is this shaped like a principal" and "is
 * this principal allowed here" are different questions and conflating them
 * produces call sites that answer neither.
 *
 * This answers only "was a valid principal readable from this value". It does
 * NOT promise a later plain read returns what was validated — nothing can, for
 * an object built to lie. Anything that goes on to USE the fields must take
 * them from `canonicalPrincipal`'s snapshot (as `trustAutomationPrincipal` and
 * `assertExecutionPrincipal` do) rather than from the argument.
 */
export function isExecutionPrincipal(
  value: unknown,
): value is ExecutionPrincipal {
  return canonicalPrincipal(value) !== undefined;
}

/**
 * Validate a principal AND bind it to the tenant that is about to act on it.
 *
 * `tenantId` on a decoded principal crosses an authentication boundary exactly
 * as `ApprovalActor.tenantId` does — the type says `string`, and the type system
 * has no authority over a value read back out of D1 or a DO's storage.
 *
 * Returns the canonical snapshot, not the argument: the tenant that was
 * compared and the tenant the caller goes on to use are then the same string.
 */
export function assertExecutionPrincipal(
  value: unknown,
  expectedTenantId: string,
  label: string,
): ExecutionPrincipal {
  const principal = canonicalPrincipal(value);
  if (principal === undefined) {
    throw new Error(`execution principal: ${label} is malformed`);
  }
  if (principal.tenantId !== expectedTenantId) {
    throw new Error(
      `execution principal: ${label} belongs to tenant '${principal.tenantId}', not '${expectedTenantId}'`,
    );
  }
  return principal;
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
