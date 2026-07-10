// Self-service sandbox reset: POST /demo/reset wipes the AUTHENTICATED demo
// tenant's runs + approvals with the same purge primitive the reaper uses,
// so a visitor can start over without waiting for tenant expiry.
//
// Trust posture:
// - The kill switch needs no handling here: DEMO_DISABLED makes the host's
//   buildVerifier drop the JWT verifier, so a demo caller's token stops
//   verifying -> resolve() returns undefined -> 401. Fail-closed for free.
// - The run budget is deliberately NOT refilled: demo_tenants.run_count and
//   demo_daily survive the purge, so reset can never farm runs past the
//   per-tenant cap. (A future budget-refill would extend the host's
//   purgeTenantData seam, not this router.)
// - This deliberately relaxes purgeTenant's "only purge tenants whose tokens
//   already expired" precondition: the caller holds a live token, so it is
//   the only purge that can race its OWN tenant. Exactly what each race does,
//   pinned by test rather than assumed:
//     * a straggler RESUME against a purged row fails without re-executing
//       the gated step or re-creating the snapshot (mastra-schema-guard.test.ts);
//     * a run STARTED inside the purge is reaped by the snapshot DELETE (it
//       re-reads the range), though its artifacts are never enumerated —
//       d1-storage.test.ts pins both halves. The purge is three un-transacted
//       statements and the artifact deletes are R2 calls no SQL transaction
//       could span, so a 5xx can leave it partially applied; the client is
//       told to resync from the server rather than trust its local view.
//   All of this is scoped to the requester's own tenant by the range
//   predicate — never another tenant's data.
// - Not cleared, by design: DO resume-ledger storage (orphaned exactly like
//   the retention purge's rows; runIds are never reused), in-memory
//   idempotency/artifact state (evaporates with its DO/runtime), and D1
//   rate-limit windows (self-expiring).

import type { TenantResolver } from '@proofoftech/flowsafe/approval-api';
import { TenantResolutionError } from '@proofoftech/flowsafe/approval-api';
import type { PurgeTenantResult } from '@proofoftech/flowsafe/do-runner';

export interface DemoResetRouterOptions {
  /** The SAME resolver the host's other routers use (authenticate -> bind). */
  resolve: TenantResolver;
  /** The authoritative demo discriminator (tenants.kind, never a slug guess). */
  isDemoTenant: (tenantId: string) => Promise<boolean>;
  /** The purge seam — D1 purgeTenant in the worker, in-memory in the dev server. */
  purgeTenantData: (tenantId: string) => Promise<PurgeTenantResult>;
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'content-type': 'application/json',
      'cache-control': 'no-store',
    },
  });
}

/**
 * Routes (null for every path other than /demo/reset, so hosts compose it
 * like the other routers):
 *   POST /demo/reset -> 200 { ok: true, tenantId, purged: PurgeTenantResult }
 *
 * Gate order (each pinned by demo-reset.test.ts): exact path -> method (405)
 * -> authenticate (401) -> admin role (403 — every demo visitor holds an
 * admin token, so this is an RBAC demonstration, not a lockout) -> demo
 * tenant (403 — commercial/static-token tenants can never self-wipe) ->
 * purge (200/500). The purge seam is never called on a refusal.
 */
export function createDemoResetRouter(
  options: DemoResetRouterOptions,
): (request: Request) => Promise<Response | null> {
  return async (request: Request): Promise<Response | null> => {
    const url = new URL(request.url);
    if (url.pathname !== '/demo/reset') return null;
    if (request.method !== 'POST') {
      return json({ error: 'method not allowed' }, 405);
    }
    try {
      const tenant = await options.resolve(request);
      if (!tenant) {
        return json({ error: 'authentication required' }, 401);
      }
      if (tenant.actor.role !== 'admin') {
        return json(
          {
            error: `forbidden: resetting the sandbox requires the admin role (you are '${tenant.actor.role}')`,
          },
          403,
        );
      }
      if (!(await options.isDemoTenant(tenant.tenantId))) {
        return json(
          { error: 'forbidden: reset only exists for demo sandboxes' },
          403,
        );
      }
      const purged = await options.purgeTenantData(tenant.tenantId);
      console.log(
        JSON.stringify({
          type: 'demo-reset',
          tenantId: tenant.tenantId,
          actorId: tenant.actor.id,
          ...purged,
        }),
      );
      return json({ ok: true, tenantId: tenant.tenantId, purged });
    } catch (error) {
      if (error instanceof TenantResolutionError) {
        return json({ error: error.message }, 403);
      }
      return json(
        { error: error instanceof Error ? error.message : String(error) },
        500,
      );
    }
  };
}
