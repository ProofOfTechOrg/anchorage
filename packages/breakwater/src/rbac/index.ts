// SPDX-License-Identifier: Apache-2.0
// RBAC — actor identity + role authorization.
//
// RBACMiddleware runs as a Mastra input processor: it authorizes the actor
// before the model call and records every decision on the shared AuditLogger
// sink (moved to ../audit when audit grew consumers beyond RBAC; re-exported
// below so existing '@proofoftech/breakwater/rbac' imports keep working).

import type {
  ProcessInputArgs,
  ProcessInputResult,
  Processor,
} from '@mastra/core/processors';
import type { RequestContext } from '@mastra/core/request-context';

import type { AuditLogger } from '../audit/index.js';

/** Role labels accepted by the built-in actor contract. */
export type Role = 'admin' | 'builder' | 'operator' | 'reviewer' | 'viewer';

/** All role labels accepted by `RBACMiddleware`. */
export const ROLES: readonly Role[] = [
  'admin',
  'builder',
  'operator',
  'reviewer',
  'viewer',
];

/** Authenticated identity evaluated by RBAC and attached to audit events. */
export interface Actor {
  /** Stable actor identifier from the host authentication system. */
  id: string;
  /** Role used by the middleware's exact allowlist. */
  role: Role;
}

export type {
  AuditEvent,
  AuditLoggerOptions,
  AuditSink,
} from '../audit/index.js';
// Audit moved to its own module; keep the historical rbac export surface.
export { AuditLogger } from '../audit/index.js';

/** requestContext key the default actor lookup reads. */
export const ACTOR_CONTEXT_KEY = 'breakwater.actor';

/**
 * Validated actor lookup from a Mastra RequestContext. Shared by
 * RBACMiddleware's default getActor and PolicyEngine's audit attribution.
 */
export function actorFromRequestContext(
  requestContext: RequestContext | undefined,
): Actor | undefined {
  const value = requestContext?.get(ACTOR_CONTEXT_KEY);
  if (!value || typeof value !== 'object') return undefined;
  const candidate = value as Partial<Actor>;
  if (typeof candidate.id !== 'string' || typeof candidate.role !== 'string') {
    return undefined;
  }
  return (ROLES as readonly string[]).includes(candidate.role)
    ? { id: candidate.id, role: candidate.role }
    : undefined;
}

/** Configuration for `RBACMiddleware`. */
export interface RBACMiddlewareOptions {
  /** Exact roles authorized to call the agent. */
  allowedRoles: readonly Role[];
  /** Optional audit logger for authorization decisions and lookup failures. */
  audit?: AuditLogger;
  /** Override actor sourcing. Default reads ACTOR_CONTEXT_KEY from requestContext. */
  getActor?: (args: ProcessInputArgs) => Actor | undefined;
}

/** Mastra input processor that authorizes an actor before model execution. */
export class RBACMiddleware implements Processor<'breakwater-rbac'> {
  /** Stable Mastra processor identifier. */
  readonly id = 'breakwater-rbac' as const;
  readonly #allowedRoles: readonly Role[];
  readonly #audit?: AuditLogger;
  readonly #getActor: (args: ProcessInputArgs) => Actor | undefined;

  constructor(options: RBACMiddlewareOptions) {
    if (options.allowedRoles.length === 0) {
      throw new Error('RBACMiddleware: allowedRoles must not be empty');
    }
    this.#allowedRoles = options.allowedRoles;
    this.#audit = options.audit;
    this.#getActor =
      options.getActor ??
      ((args) => actorFromRequestContext(args.requestContext));
  }

  processInput(args: ProcessInputArgs): ProcessInputResult {
    let actor: Actor | undefined;
    try {
      actor = this.#getActor(args);
    } catch (error) {
      // A crashing actor lookup is worse than a denial; it must not leave
      // less audit evidence than one. Keep the sink free of opaque exception
      // text, then fail the request with the original error.
      this.#audit?.record({
        actor: null,
        action: 'agent.input.authorize',
        resource: this.id,
        decision: 'error',
        reason: 'actor lookup failed',
      });
      throw error;
    }
    if (!actor) {
      const reason = `no actor in request context (key '${ACTOR_CONTEXT_KEY}')`;
      this.#audit?.record({
        actor: null,
        action: 'agent.input.authorize',
        resource: this.id,
        decision: 'denied',
        reason,
      });
      args.abort(reason);
    }
    if (!this.#allowedRoles.includes(actor.role)) {
      const reason = `role '${actor.role}' is not in allowed roles [${this.#allowedRoles.join(', ')}]`;
      this.#audit?.record({
        actor,
        action: 'agent.input.authorize',
        resource: this.id,
        decision: 'denied',
        reason,
      });
      args.abort(reason);
    }
    this.#audit?.record({
      actor,
      action: 'agent.input.authorize',
      resource: this.id,
      decision: 'allowed',
    });
    return args.messages;
  }
}
