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
