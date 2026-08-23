// SPDX-License-Identifier: Apache-2.0
// The ONE error->HTTP mapping the DO bases answer with.
//
// Its own leaf because both DO shells need it and neither should depend on the
// other (same reasoning as host-kit's RunRouteError, which is the Worker-side
// half of this contract: doSummary reads these statuses back off a stub, so a
// status that collapses here collapses all the way to the caller).
//
// The taxonomy IS the contract: a 404 for an unknown run, a 409 for a state or
// lifecycle conflict, and a 400 for a malformed request. A DO that maps its
// runtime errors to a blanket 500 does not merely log worse: it turns "you
// asked for a run that does not exist" into "I am broken", and the router's
// RunRouteError passthrough has nothing to pass through.

import { DeploymentIdentityError } from './deployment-identity.js';
import type { DoRefusalReason } from './do-status-error.js';
import { DoStatusError } from './do-status-error.js';
import {
  InvalidRunRequestError,
  RunAlreadyExistsError,
  RunLifecycleBlockedError,
  RunNotSuspendedError,
  RunStateUnreadableError,
  RunTerminalConflictError,
  UnknownRunError,
  UnknownWorkflowError,
} from './runtime.js';

// Re-exported under the name every shell already imports; the class itself
// lives on a leaf so a refusal module can extend it without closing an import
// cycle through this mapper (see do-status-error.ts).
export type { DoRefusalReason };
export { DoStatusError };

function statusOf(error: unknown): number | undefined {
  // Mis-provisioned deployment (env tag vs D1 sentinel): the operator's
  // problem, not the caller's — 503 so monitors separate a wiring fault from
  // a code fault. Fail closed: nothing below this line runs for one.
  if (error instanceof DeploymentIdentityError) return 503;
  // An authoritative read that did not reach storage: the same shape of answer
  // as the misprovisioning above — the caller asked for nothing wrong, the
  // condition is the operator's, and it clears on its own — so it is retryable
  // rather than a 500 that reads as a code fault. A 404 or a 200 with a
  // fabricated summary would be worse than either: both invite the caller to
  // conclude something from a read that never happened.
  if (error instanceof RunStateUnreadableError) return 503;
  if (
    error instanceof UnknownWorkflowError ||
    error instanceof UnknownRunError
  ) {
    return 404;
  }
  if (
    error instanceof RunNotSuspendedError ||
    error instanceof RunAlreadyExistsError ||
    error instanceof RunTerminalConflictError ||
    error instanceof RunLifecycleBlockedError
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
 * The structured reason a refusal publishes, or undefined. Two channels, one
 * renderer: RunLifecycleBlockedError predates DoStatusError and is a plain
 * Error, while every refusal authored since carries its reason on the
 * taxonomy's own base (ExecutionFencedError's EXECUTION_FENCED among them).
 * Anything else has no reason to publish — an unclassified fault must not
 * grow a machine-readable code it never defined.
 */
function reasonOf(error: unknown): unknown {
  if (error instanceof RunLifecycleBlockedError) return error.reason;
  if (error instanceof DoStatusError) return error.reason;
  return undefined;
}

/**
 * Map a thrown error to the DO's HTTP response. Anything unrecognized is a 500
 * with its message — the honest answer for a fault this layer did not classify.
 */
export function doErrorResponse(error: unknown): Response {
  const message = error instanceof Error ? error.message : String(error);
  const reason = reasonOf(error);
  return new Response(
    JSON.stringify({
      error: message,
      ...(reason === undefined ? {} : { reason }),
    }),
    {
      status: statusOf(error) ?? 500,
      headers: { 'content-type': 'application/json' },
    },
  );
}
