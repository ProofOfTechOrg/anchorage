// The bearer-token auth seam every host shares: a JSON `token -> actor` map in
// a Worker secret, parsed once per request (the secret arrives on `env`, so a
// Worker cannot do better; an in-process host hoists it and parses once per
// process) and then consulted by both routers.
//
// This is trusted-computing-base code: it ASSERTS identity (the service
// enforces roles from it), so a malformed map must fail closed — an unparseable
// or absent secret yields an empty map and every authenticated route 401s.
//
// Production SSO/JWT verification replaces `bearerActorAuthenticator` wholesale;
// the `(request) => ApprovalActor | undefined` shape is the seam both
// createApprovalRouter and createRunRouter accept.

import { APPROVAL_ROLES, type ApprovalActor } from '../approval-api/index.js';

/**
 * Parse the `APPROVAL_ACTOR_TOKENS` secret: `{"<token>": {"id","role"}}`.
 * Unknown roles and malformed entries are dropped rather than trusted — a
 * half-valid map must not smuggle an actor with an unrecognized role past the
 * service's role check.
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
  for (const [token, actor] of Object.entries(parsed)) {
    const candidate = actor as { id?: unknown; role?: unknown };
    if (
      typeof candidate?.id === 'string' &&
      candidate.id.length > 0 &&
      typeof candidate.role === 'string' &&
      (APPROVAL_ROLES as readonly string[]).includes(candidate.role)
    ) {
      actors.set(token, candidate as ApprovalActor);
    }
  }
  return actors;
}

/**
 * Build the `authenticate` seam over an already-parsed token map. Take the map
 * as an argument rather than the raw secret so the JSON is parsed once and
 * shared across both routers, rather than once per `authenticate` call — a
 * Worker `fetch` authenticates twice (approval router, then run router). In a
 * Worker that is once per request (the secret arrives on `env`); an in-process
 * host can hoist it to module scope and parse once per process.
 */
export function bearerActorAuthenticator(
  actors: Map<string, ApprovalActor>,
): (request: Request) => ApprovalActor | undefined {
  return (request) => {
    const token = request.headers
      .get('authorization')
      ?.match(/^Bearer\s+(.+)$/i)?.[1];
    return token ? actors.get(token) : undefined;
  };
}
