// SPDX-License-Identifier: Apache-2.0
// The request-scoped tenant context — the seam that makes INV-2
// CONSTRUCTIBLE. The stores must be bound to the authenticated tenant, but a
// host's fetch() historically built its ApprovalService from `env` before
// anyone was authenticated, so "bind at construction from actor.tenantId" had
// no valid call site — the exact ordering hole a rushed fix plugs with an
// unauthenticated header. TenantResolver inverts the order: routers resolve
// the request FIRST (authenticate -> validate INV-3 -> bind), and everything
// downstream reads tenant.service() / tenant.newRunId() / tenant.ownsRun().

import {
  RESERVED_TENANT_IDS,
  TENANT_ID_PATTERN,
} from '../do-runner/path-safe-id.js';
import type { ApprovalActor } from './contract.js';
import type { ApprovalService } from './service.js';
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
}

export function createTenantResolver(
  options: CreateTenantResolverOptions,
): TenantResolver {
  const mintUuid = options.newRunId ?? (() => crypto.randomUUID());
  return async (request) => {
    const actor = await options.authenticate(request);
    if (!actor) return undefined;
    if (!TENANT_ID_PATTERN.test(actor.tenantId)) {
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
      newRunId: () => `${tenantId}_${mintUuid()}`,
      ownsRun: (runId: string) => runId.startsWith(`${tenantId}_`),
    };
  };
}
