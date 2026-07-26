// SPDX-License-Identifier: Apache-2.0
// The bearer auth seam every host shares: extract the bearer token, hand it
// to a TokenVerifier (static map or HS256 JWT — verifier.ts), get back an
// ApprovalActor or undefined.
//
// This is trusted-computing-base code: it ASSERTS identity (the service
// enforces roles from it; the tenant predicates key on actor.tenantId), so a
// malformed map must fail closed — an unparseable or absent secret yields an
// empty map and every authenticated route 401s, and an entry without an
// INV-3-valid tenantId is dropped (its token 401s) rather than admitted
// tenant-less.

import type { ApprovalActor } from '../approval-api/index.js';
import { type TokenVerifier, toApprovalActor } from './verifier.js';

/**
 * Parse the `APPROVAL_ACTOR_TOKENS` secret:
 * `{"<token>": {"id","role","tenantId"}}`. Every entry passes the real
 * validator (toApprovalActor) — unknown roles, empty ids, and missing,
 * invalid, or reserved-identity ('system') tenantIds are dropped rather
 * than trusted; there is no `as`-cast at this JSON boundary.
 */
export function parseActorTokens(
  raw: string | undefined,
): Map<string, ApprovalActor> {
  const actors = new Map<string, ApprovalActor>();
  if (!raw) return actors;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    console.error(
      JSON.stringify({
        type: 'config-error',
        var: 'APPROVAL_ACTOR_TOKENS',
        reason: 'not valid JSON — all authenticated routes will 401',
      }),
    );
    return actors;
  }
  if (parsed === null || typeof parsed !== 'object') return actors;
  for (const [token, candidate] of Object.entries(parsed)) {
    const actor = toApprovalActor(candidate);
    if (actor) {
      actors.set(token, actor);
    } else {
      console.error(
        JSON.stringify({
          type: 'config-error',
          var: 'APPROVAL_ACTOR_TOKENS',
          reason:
            "entry dropped: needs non-empty id, a known role, and an INV-3 tenantId (^[a-z0-9]{3,32}$, not the reserved identity 'system') — its token will 401",
        }),
      );
    }
  }
  return actors;
}

/**
 * Build the `authenticate` seam over a TokenVerifier. Callers with a parsed
 * token map wrap it: `bearerActorAuthenticator(staticTokenVerifier(map))`.
 * Parsing the map stays outside so the JSON is parsed once and shared across
 * both routers (a Worker `fetch` authenticates twice — approval router, then
 * run router; the secret arrives on `env`, so once per request is the floor).
 */
export function bearerActorAuthenticator(
  verifier: TokenVerifier,
): (request: Request) => Promise<ApprovalActor | undefined> {
  return async (request) => {
    const token = request.headers
      .get('authorization')
      ?.match(/^Bearer\s+(.+)$/i)?.[1];
    return token ? verifier.verify(token) : undefined;
  };
}
