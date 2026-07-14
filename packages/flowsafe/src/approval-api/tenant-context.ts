// SPDX-License-Identifier: Apache-2.0
// The request-scoped tenant context — the seam that makes INV-2
// CONSTRUCTIBLE. The stores must be bound to the authenticated tenant, but a
// host's fetch() historically built its ApprovalService from `env` before
// anyone was authenticated, so "bind at construction from actor.tenantId" had
// no valid call site — the exact ordering hole a rushed fix plugs with an
// unauthenticated header. TenantResolver inverts the order: routers resolve
// the request FIRST (authenticate -> validate INV-3 -> bind), and everything
// downstream reads tenant.service() / tenant.newRunId() / tenant.ownsRun().

import { mintResourceId, mintThreadId } from '../do-runner/memory-id.js';
import {
  mintSaltedId,
  RESERVED_TENANT_IDS,
  TENANT_ID_PATTERN,
  tenantOwnsSaltedId,
} from '../do-runner/path-safe-id.js';
import {
  type ApprovalActor,
  type ApprovalRole,
  DECIDER_ROLES,
} from './contract.js';
import {
  type ApprovalService,
  type SelfDecisionPolicy,
  selfDecisionExempts,
} from './service.js';
import type { TenantBoundApprovalStore } from './tenant-brand.js';

export interface TenantContext {
  /** Already authenticated; tenantId === actor.tenantId, INV-3-validated. */
  readonly actor: ApprovalActor;
  readonly tenantId: string;
  /** The approval service over a store bound to THIS tenant. */
  service(): ApprovalService;
  /** Mint a tenant-salted runId: `${tenantId}_${uuid}` (INV-1). */
  newRunId(): string;
  /** Exact ownership: `runId.startsWith(`${tenantId}_`)` — see INV-3. */
  ownsRun(runId: string): boolean;
  /**
   * Mint a tenant-salted agent-memory threadId: `${tenantId}_${uuid}` —
   * the INV-1 carrier extended to Mastra memory
   * (docs/agent-memory-tenancy.md). Hosts never accept a client-supplied
   * threadId; this is the constructor.
   */
  newThreadId(): string;
  /**
   * Mint a tenant-salted agent-memory resourceId over the host's business
   * key (a user id, a lead id): `${tenantId}_${resourceKey}`. Same key, two
   * tenants, disjoint ids — the memory-leak class the salting closes.
   */
  newResourceId(resourceKey: string): string;
  /**
   * Exact ownership over memory ids (threadId or resourceId) — assert on
   * every memory read/write path; answer 404 on a foreign id.
   */
  ownsMemoryId(id: string): boolean;
  /**
   * Display hint only: whether THIS actor may decide its OWN request — true
   * iff the actor holds a decider role AND the deployment's self-decision
   * policy exempts that role. Enforcement stays in ApprovalService.decide();
   * the GET /workflows echo reads this so the SPA can drop its "the server
   * will refuse your decision" hint for an exempt role.
   */
  canSelfDecide(role: ApprovalRole): boolean;
}

/**
 * undefined => no identity => 401. Throws TenantResolutionError when an
 * AUTHENTICATED actor carries a tenant that fails INV-3 or names a reserved
 * identity ('system') — either is a verifier or claim-mapping bug, surfaced
 * as 403 by the routers, never concatenated into a runId ("undefined" is
 * itself an INV-3-valid string, so an unvalidated `String(undefined)` would
 * silently authorize a tenant literally named 'undefined').
 */
export type TenantResolver = (
  request: Request,
) => Promise<TenantContext | undefined>;

export class TenantResolutionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TenantResolutionError';
  }
}

export interface CreateTenantResolverOptions {
  /** The host's authenticate seam (bearerActorAuthenticator over a verifier). */
  authenticate: (
    request: Request,
  ) => Promise<ApprovalActor | undefined> | ApprovalActor | undefined;
  /** Store factory (D1 or in-memory) — binds per request, DDL memoized once. */
  storeFactory: { forTenant(tenantId: string): TenantBoundApprovalStore };
  /**
   * Host-specific service assembly (resumeRun topology, audit sink, SLA
   * defaults) over the request's bound store. Called lazily, at most once per
   * request.
   */
  buildService: (
    store: TenantBoundApprovalStore,
    actor: ApprovalActor,
  ) => ApprovalService;
  /** The uuid half of minted runIds. Default: crypto.randomUUID. */
  newRunId?: () => string;
  /**
   * The SoD exemption policy the resolver builds `canSelfDecide` from — pass
   * the SAME value the host feeds ApprovalService so the display hint can
   * never contradict the server's decide() verdict. Absent => SoD on
   * (`canSelfDecide` false for every role).
   */
  allowSelfDecision?: SelfDecisionPolicy;
}

export function createTenantResolver(
  options: CreateTenantResolverOptions,
): TenantResolver {
  const mintUuid = options.newRunId ?? (() => crypto.randomUUID());
  return async (request) => {
    const actor = await options.authenticate(request);
    if (!actor) return undefined;
    if (
      typeof actor.tenantId !== 'string' ||
      !TENANT_ID_PATTERN.test(actor.tenantId)
    ) {
      throw new TenantResolutionError(
        `authenticated actor '${actor.id}' carries a non-INV-3 tenantId — fix the verifier; refusing to scope the request`,
      );
    }
    // Belt over the verifier seam: the built-in toApprovalActor already drops
    // reserved identities, but a custom TokenVerifier or a hand-built actor
    // map handed to staticTokenVerifier never crosses it. This resolver is
    // the one chokepoint every routed request passes before a store binds or
    // a runId mints, so the TCB's own audit identity is re-refused here
    // whatever the verifier admitted. ('system' is INV-3-valid — the pattern
    // check above cannot catch it.)
    if (RESERVED_TENANT_IDS.includes(actor.tenantId)) {
      throw new TenantResolutionError(
        `authenticated actor '${actor.id}' carries the reserved tenantId '${actor.tenantId}' (the TCB's own audit identity) — fix the verifier; refusing to scope the request`,
      );
    }
    const tenantId = actor.tenantId;
    let service: ApprovalService | undefined;
    return {
      actor,
      tenantId,
      service: () => {
        service ??= options.buildService(
          options.storeFactory.forTenant(tenantId),
          actor,
        );
        return service;
      },
      // mintUuid passed LAZILY: mintSaltedId validates the tenant before
      // evaluating it (a no-op guard here — the resolver already refused a bad
      // tenant above — but it keeps the safe validate-then-mint idiom uniform).
      newRunId: () => mintSaltedId(tenantId, mintUuid, 'newRunId'),
      ownsRun: (runId: string) => tenantOwnsSaltedId(tenantId, runId),
      // Memory ids ride the same uuid seam as runIds so tests inject one
      // generator for both; mint* re-refuses reserved/non-INV-3 tenants,
      // a no-op here (this resolver already refused them above).
      newThreadId: () => mintThreadId(tenantId, mintUuid),
      newResourceId: (resourceKey: string) =>
        mintResourceId(tenantId, resourceKey),
      ownsMemoryId: (id: string) => tenantOwnsSaltedId(tenantId, id),
      // Display hint, not enforcement: the DECIDER_ROLES guard means a
      // non-decider never echoes true (it cannot decide at all), so the hint
      // never affirms a role decide() would reject. Fed the SAME
      // allowSelfDecision the service enforces (ApprovalService.decide).
      canSelfDecide: (role: ApprovalRole) =>
        DECIDER_ROLES.includes(role) &&
        selfDecisionExempts(options.allowSelfDecision, role),
    };
  };
}
