// SPDX-License-Identifier: Apache-2.0
// Shared vocabulary between the DO-response reader and the run router: an error
// that already knows the HTTP status it should surface as.
//
// Its own leaf because both sides need it and neither should depend on the
// other — `doSummary` (do-response.ts) throws it, `createRunRouter`
// (run-router.ts) catches it. Homing it in the router would make the primitive
// depend on the composite.

/**
 * A transport failure from a host's run-lifecycle thunk, carrying the
 * status to surface. Hosts that reach their runs through a Durable Object stub
 * get this from `doSummary`: the DO already mapped the runtime's typed errors to
 * 404/409/400, so that status must survive rather than collapse into a 500.
 */
export class RunRouteError extends Error {
  readonly status: number;
  readonly reason?: unknown;

  constructor(status: number, message: string, reason?: unknown) {
    super(message);
    this.name = 'RunRouteError';
    this.status = status;
    this.reason = reason;
  }
}

/**
 * The Durable Object's own structured refusal, if this error carries one.
 *
 * The taxonomy's rule is that a `reason` is a SCREAMING_SNAKE code the DO
 * deliberately published (do-error-response.ts), and every router that fronts a
 * DO must pass one through with its status intact. Without this, a router that
 * collapses 5xx to a bare 500 — the shape agent-host/router.ts and
 * stream-router.ts both had — turns "this deployment is fenced, retry after the
 * migration" into "I am broken", and the caller cannot tell a retryable
 * operational state from a code fault.
 *
 * Narrow on purpose: only a plain object with a string `code` qualifies, so an
 * upstream body that happened to carry a `reason` field of some other shape
 * cannot widen what a 5xx surfaces.
 */
export function runRouteReason(
  error: RunRouteError,
): { code: string } | undefined {
  const { reason } = error;
  if (typeof reason !== 'object' || reason === null || Array.isArray(reason)) {
    return undefined;
  }
  const { code } = reason as { code?: unknown };
  return typeof code === 'string' ? (reason as { code: string }) : undefined;
}
