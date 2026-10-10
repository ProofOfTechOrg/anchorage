// SPDX-License-Identifier: Apache-2.0
// The ONE error->HTTP classification, shared by the run object and the run
// router.
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
import {
  DoStatusError,
  isRefusalStatus,
  refusalBody,
} from './do-status-error.js';
import {
  InvalidRunRequestError,
  RunAlreadyExistsError,
  RunLifecycleBlockedError,
  RunSettledConflictError,
  RunTerminalConflictError,
  UnknownRunError,
  UnknownWorkflowError,
} from './runtime.js';

// Re-exported under the name every shell already imports; the class itself
// lives on a leaf so a refusal module can extend it without closing an import
// cycle through this mapper (see do-status-error.ts).
export type { DoRefusalReason };
export { DoStatusError };

/**
 * @internal The status the run taxonomy answers `error` with, or undefined for
 * a fault it does not classify. One function, so the run router in process and
 * the run object answer an error with one status.
 */
export function refusalStatus(error: unknown): number | undefined {
  // Mis-provisioned deployment (env tag vs D1 sentinel): the operator's
  // problem, not the caller's — 503 so monitors separate a wiring fault from
  // a code fault. Fail closed: nothing below this line runs for one.
  if (error instanceof DeploymentIdentityError) return 503;
  if (
    error instanceof UnknownWorkflowError ||
    error instanceof UnknownRunError
  ) {
    return 404;
  }
  if (
    error instanceof RunAlreadyExistsError ||
    error instanceof RunTerminalConflictError ||
    error instanceof RunSettledConflictError ||
    error instanceof RunLifecycleBlockedError
  ) {
    return 409;
  }
  if (error instanceof InvalidRunRequestError) return 400;
  // Range-checked even though the base states the contract, because the base
  // cannot enforce it: see isRefusalStatus.
  if (error instanceof DoStatusError && isRefusalStatus(error.status))
    return error.status;
  return undefined;
}

/**
 * @internal The structured reason a refusal publishes, or undefined. Two
 * channels, one renderer: RunLifecycleBlockedError is a plain Error with its
 * own `reason`; a DoStatusError subclass carries its reason on the base.
 * Anything else has no reason to publish — an unclassified fault must not grow
 * a machine-readable code it never defined.
 */
export function refusalReason(error: unknown): unknown {
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
  const reason = refusalReason(error);
  return new Response(JSON.stringify(refusalBody(message, reason)), {
    status: refusalStatus(error) ?? 500,
    headers: { 'content-type': 'application/json' },
  });
}
