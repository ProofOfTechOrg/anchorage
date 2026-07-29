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
import { authorizeActor } from './authorize.js';
import {
  assertPrincipalKinds,
  PRINCIPAL_KINDS,
  type PrincipalKind,
} from './principal.js';

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

// Re-exported for hosts; `assertPrincipalKinds` stays internal to the package.
export type { PrincipalKind } from './principal.js';
export {
  DEFAULT_ALLOWED_PRINCIPAL_KINDS,
  PRINCIPAL_KINDS,
  principalKindOf,
} from './principal.js';

/** Authenticated identity evaluated by RBAC and attached to audit events. */
export interface Actor {
  /** Stable actor identifier from the host authentication system. */
  id: string;
  /**
   * Role used by the middleware's exact allowlist. Meaningful only for the
   * 'human' kind; for automated kinds the role allowlist is not consulted at
   * all and hosts should project the least-privileged label. See
   * `authorizeActor`.
   */
  role: Role;
  /** Absent means 'human', so an existing host keeps its exact behavior. */
  kind?: PrincipalKind;
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
  if (
    typeof candidate.id !== 'string' ||
    candidate.id.trim() === '' ||
    typeof candidate.role !== 'string'
  ) {
    return undefined;
  }
  // An unrecognized kind resolves to no actor rather than to a human: a value
  // this build does not understand must never fall through to the default that
  // every guarded agent already admits.
  if (
    candidate.kind !== undefined &&
    !(PRINCIPAL_KINDS as readonly unknown[]).includes(candidate.kind)
  ) {
    return undefined;
  }
  return (ROLES as readonly string[]).includes(candidate.role)
    ? {
        id: candidate.id,
        role: candidate.role,
        ...(candidate.kind !== undefined ? { kind: candidate.kind } : {}),
      }
    : undefined;
}

/** Configuration for `RBACMiddleware`. */
export interface RBACMiddlewareOptions {
  /** Exact roles authorized to call the agent. Consulted for humans only. */
  allowedRoles: readonly Role[];
  /**
   * Exact principal kinds authorized to call the agent. Defaults to
   * `['human']`, so an existing configuration denies every automated principal
   * without changing a line — the caller must name the automation it wants.
   */
  allowedPrincipalKinds?: readonly PrincipalKind[];
  /** Optional audit logger for authorization decisions and lookup failures. */
  audit?: AuditLogger;
  /** Audit resource. Defaults to the stable processor identifier. */
  resource?: string;
  /** Override actor sourcing. Default reads ACTOR_CONTEXT_KEY from requestContext. */
  getActor?: (args: ProcessInputArgs) => Actor | undefined;
}

/** Mastra input processor that authorizes an actor before model execution. */
export class RBACMiddleware implements Processor<'breakwater-rbac'> {
  /** Stable Mastra processor identifier. */
  readonly id = 'breakwater-rbac' as const;
  readonly #allowedRoles: readonly Role[];
  readonly #allowedPrincipalKinds: readonly PrincipalKind[];
  readonly #audit?: AuditLogger;
  readonly #getActor: (args: ProcessInputArgs) => Actor | undefined;
  readonly #resource: string;

  constructor(options: RBACMiddlewareOptions) {
    if (options.allowedRoles.length === 0) {
      throw new Error('RBACMiddleware: allowedRoles must not be empty');
    }
    this.#allowedRoles = options.allowedRoles;
    this.#allowedPrincipalKinds = assertPrincipalKinds(
      options.allowedPrincipalKinds,
      'RBACMiddleware',
    );
    this.#audit = options.audit;
    this.#resource = options.resource ?? this.id;
    this.#getActor =
      options.getActor ??
      ((args) => actorFromRequestContext(args.requestContext));
  }

  processInput(args: ProcessInputArgs): ProcessInputResult {
    authorizeActor({
      allowedRoles: this.#allowedRoles,
      allowedPrincipalKinds: this.#allowedPrincipalKinds,
      audit: this.#audit,
      resource: this.#resource,
      requestContext: args.requestContext,
      resolveActor: () => this.#getActor(args),
      deny: (reason) => args.abort(reason),
    });
    return args.messages;
  }
}
