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
 * The base for a refusal that carries its own HTTP status and an optional
 * `reason` — the taxonomy's extension point. A shell or a host route declares
 * one:
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
  /** The response status, 4xx or 5xx; any other status answers 500. */
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

/**
 * @internal Whether `status` is one a refusal can carry: an integer in
 * [400, 599]. `new Response(body, { status })` raises RangeError outside
 * [200, 599], from inside the catch block that renders the refusal, and a
 * status below 400 would answer a refusal as a success or a redirect. A
 * refusal with any other status falls through to the 500 that is the honest
 * answer for a bug.
 */
export function isRefusalStatus(status: unknown): status is number {
  return (
    typeof status === 'number' &&
    Number.isInteger(status) &&
    status >= 400 &&
    status <= 599
  );
}

/** @internal The reason when it carries a published code, or undefined. */
export function codedReason(reason: unknown): { code: string } | undefined {
  if (typeof reason !== 'object' || reason === null || Array.isArray(reason)) {
    return undefined;
  }
  const { code } = reason as { code?: unknown };
  return typeof code === 'string' ? (reason as { code: string }) : undefined;
}

/**
 * @internal Whether a refusal at `status` may carry its own message to a
 * caller: any status below 500, or a 5xx whose reason carries a code. A 5xx
 * without one is a fault nobody classified, and its message can name storage or
 * deployment detail.
 */
export function isPublishedRefusal(status: number, reason: unknown): boolean {
  return status < 500 || codedReason(reason) !== undefined;
}

/** @internal The JSON body a refusal answers with. */
export function refusalBody(
  message: string,
  reason: unknown,
): { error: string; reason?: unknown } {
  return { error: message, ...(reason === undefined ? {} : { reason }) };
}
