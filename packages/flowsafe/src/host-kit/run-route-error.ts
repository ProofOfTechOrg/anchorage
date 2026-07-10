// Shared vocabulary between the DO-response reader and the run router: an error
// that already knows the HTTP status it should surface as.
//
// Its own leaf because both sides need it and neither should depend on the
// other — `doSummary` (do-response.ts) throws it, `createRunRouter`
// (run-router.ts) catches it. Homing it in the router would make the primitive
// depend on the composite.

/**
 * A transport failure from a host's start/status/resume thunk, carrying the
 * status to surface. Hosts that reach their runs through a Durable Object stub
 * get this from `doSummary`: the DO already mapped the runtime's typed errors to
 * 404/409/400, so that status must survive rather than collapse into a 500.
 */
export class RunRouteError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = 'RunRouteError';
    this.status = status;
  }
}
