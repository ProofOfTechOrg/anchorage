// SPDX-License-Identifier: Apache-2.0
// This leaf keeps the Durable Object response reader independent of the router.

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
  const { reason } = error;
  if (typeof reason !== 'object' || reason === null || Array.isArray(reason)) {
    return undefined;
  }
  const { code } = reason as { code?: unknown };
  return typeof code === 'string' ? (reason as { code: string }) : undefined;
}
