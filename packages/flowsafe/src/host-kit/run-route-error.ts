// SPDX-License-Identifier: Apache-2.0
// This leaf keeps the Durable Object response reader independent of the router.

import {
  codedReason,
  isRefusalStatus,
  refusalBody,
} from '../do-runner/do-status-error.js';

/**
 * A host-authored HTTP refusal. The message and reason are forwarded to callers;
 * hosts must supply caller-safe, JSON-compatible values.
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
 * Preserve a published operational refusal across HTTP transports.
 */
export function runRouteReason(
  error: RunRouteError,
): { code: string } | undefined {
  return codedReason(error.reason);
}

/**
 * @internal The JSON body a router answers `error` with, or undefined when its
 * status is not one a refusal carries; the router then answers its generic 500.
 * The message is forwarded at any status: a host authors it caller-safe, and
 * the readers of a run or thread object's answer already replaced the message
 * of a 5xx without a reason code.
 */
export function runRouteErrorBody(
  error: RunRouteError,
): { error: string; reason?: unknown } | undefined {
  if (!isRefusalStatus(error.status)) return undefined;
  return refusalBody(error.message, error.reason);
}
