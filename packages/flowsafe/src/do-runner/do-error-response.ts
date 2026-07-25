// SPDX-License-Identifier: Apache-2.0
// The ONE error->HTTP mapping the DO bases answer with.
//
// Its own leaf because both DO shells need it and neither should depend on the
// other (same reasoning as host-kit's RunRouteError, which is the Worker-side
// half of this contract: doSummary reads these statuses back off a stub, so a
// status that collapses here collapses all the way to the caller).
//
// The taxonomy IS the contract — a 404 for an unknown run, a 409 for a run that
// is not suspended or already exists, a 400 for a malformed request — so a DO
// that maps its runtime errors to a blanket 500 does not merely log worse: it
// turns "you asked for a run that does not exist" into "I am broken", and the
// router's RunRouteError passthrough has nothing to pass through.

import {
  InvalidRunRequestError,
  RunAlreadyExistsError,
  RunNotSuspendedError,
  UnknownRunError,
  UnknownWorkflowError,
} from './runtime.js';

/**
 * The base for a DO's OWN refusal — the taxonomy's extension point, for the
 * statuses the runtime errors above do not cover. A shell declares one
 * (ThreadIdentityError's 403) and so does a host route:
 *
 * ```ts
 * class UnknownSignalError extends DoStatusError {
 *   readonly status = 404;
 * }
 * ```
 *
 * A CLASS, so opting in is `instanceof` — deliberate and nominal, the posture
 * TENANT_BOUND and AuditLogger take. The structural alternative (any thrown
 * value with a numeric `status`) cannot tell a refusal this DO authored from the
 * arbitrary values its routes throw: an upstream client's `{status: 429}` would
 * become this API's 429, and `{status: 0}` — routine on HTTP-client error
 * objects — would make `new Response` raise inside the very catch whose job is
 * to never throw. Everything unrecognized stays a 500.
 */
export abstract class DoStatusError extends Error {
  /** The response status. 4xx/5xx only — see doErrorResponse. */
  abstract readonly status: number;
}

function statusOf(error: unknown): number | undefined {
  if (
    error instanceof UnknownWorkflowError ||
    error instanceof UnknownRunError
  ) {
    return 404;
  }
  if (
    error instanceof RunNotSuspendedError ||
    error instanceof RunAlreadyExistsError
  ) {
    return 409;
  }
  if (error instanceof InvalidRunRequestError) return 400;
  if (error instanceof DoStatusError) {
    // Range-checked even though the base states the contract, because the base
    // cannot enforce it: `new Response(body, { status })` raises RangeError
    // outside [200, 599] — from inside the catch block whose entire job is to
    // never throw, which would lose the 500 and the message with it. The 4xx/5xx
    // floor also keeps the message body legal (204/205/304 are null-body
    // statuses, all below 400). A shell that names anything else has a bug, and
    // falls through to the 500 that is the honest answer for one.
    const { status } = error;
    return Number.isInteger(status) && status >= 400 && status <= 599
      ? status
      : undefined;
  }
  return undefined;
}

/**
 * Map a thrown error to the DO's HTTP response. Anything unrecognized is a 500
 * with its message — the honest answer for a fault this layer did not classify.
 */
export function doErrorResponse(error: unknown): Response {
  const message = error instanceof Error ? error.message : String(error);
  return new Response(JSON.stringify({ error: message }), {
    status: statusOf(error) ?? 500,
    headers: { 'content-type': 'application/json' },
  });
}
