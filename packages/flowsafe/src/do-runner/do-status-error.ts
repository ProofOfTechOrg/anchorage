// SPDX-License-Identifier: Apache-2.0
// The taxonomy's extension point, on its own leaf.
//
// It lives apart from doErrorResponse (which imports it, and re-exports it as
// the public name) for one structural reason: every module that declares a
// refusal must `extends` this class at module-evaluation time, and
// doErrorResponse's own imports reach the runtime and the deployment identity
// guard. A refusal module importing the mapper would therefore close an import
// cycle whose failure mode is a temporal-dead-zone ReferenceError at
// `class X extends DoStatusError` — order-dependent, and invisible until some
// unrelated import order changes. A leaf with no imports of its own cannot be
// half-evaluated when a subclass needs it.

/** A structured refusal code doErrorResponse renders into the body. */
export interface DoRefusalReason {
  /** SCREAMING_SNAKE, per the taxonomy — see do-error-response.ts. */
  readonly code: string;
}

/**
 * The base for a DO's OWN refusal — the taxonomy's extension point, for the
 * statuses the runtime errors do not cover. A shell declares one
 * (ThreadIdentityError's 403) and so does a host route:
 *
 * ```ts
 * class UnknownSignalError extends DoStatusError {
 *   readonly status = 404;
 * }
 * ```
 *
 * A CLASS, so opting in is `instanceof` — deliberate and nominal, the posture
 * AuditLogger takes. The structural alternative (any thrown
 * value with a numeric `status`) cannot tell a refusal this DO authored from the
 * arbitrary values its routes throw: an upstream client's `{status: 429}` would
 * become this API's 429, and `{status: 0}` — routine on HTTP-client error
 * objects — would make `new Response` raise inside the very catch whose job is
 * to never throw. Everything unrecognized stays a 500.
 */
export abstract class DoStatusError extends Error {
  /** The response status. 4xx/5xx only — see doErrorResponse. */
  abstract readonly status: number;

  /**
   * An optional machine-readable reason, rendered into the response body
   * alongside the message. DECLARED HERE rather than sniffed structurally at
   * the mapper so the channel is part of the contract a subclass opts into,
   * and so the mapper needs no import of any subclass to render it (see the
   * header for why that import must not exist).
   */
  readonly reason?: DoRefusalReason;
}
