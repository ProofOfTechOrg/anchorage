// SPDX-License-Identifier: Apache-2.0
// RBAC — actor identity + role authorization.
//
// RBACMiddleware runs as a Mastra input processor: it authorizes the actor
// before the model call. The audit exports below keep
// '@proofoftech/breakwater/rbac' imports of the audit surface working.

import type {
  ProcessInputArgs,
  ProcessInputResult,
  Processor,
} from '@mastra/core/processors';
import type { RequestContext } from '@mastra/core/request-context';

import type { AuditLogger } from '../audit/index.js';
import {
  assertKnownFields,
  describeEntry,
  unknownFieldOf,
} from '../host-input.js';
import { stopWithoutCallMessages } from '../input-refusal.js';
import type { Actor, Role } from './actor.js';
import { authorizeActor } from './authorize.js';
import {
  assertPrincipalKinds,
  PRINCIPAL_KINDS,
  type PrincipalKind,
} from './principal.js';
import { ROLES, readAllowedRoles } from './roles.js';

export type {
  AuditEvent,
  AuditLoggerOptions,
  AuditSink,
} from '../audit/index.js';
export { AuditLogger } from '../audit/index.js';
export type { Actor, Role } from './actor.js';
// Re-exported for hosts; `assertPrincipalKinds` stays internal to the package.
export type { Permission, PrincipalPermissions } from './permission.js';
export {
  isPermissionIdentifier,
  isPrincipalPermissions,
  PRINCIPAL_PERMISSIONS_CONTEXT_KEY,
} from './permission.js';
export type { PrincipalKind } from './principal.js';
export {
  DEFAULT_ALLOWED_PRINCIPAL_KINDS,
  PRINCIPAL_KINDS,
  principalKindOf,
} from './principal.js';
export { ROLES } from './roles.js';

/** requestContext key the default actor lookup reads. */
export const ACTOR_CONTEXT_KEY = 'breakwater.actor';

const ACTOR_KEYS = {
  id: true,
  role: true,
  kind: true,
} satisfies Record<keyof Actor, true>;

/**
 * Validated actor lookup from a Mastra RequestContext. A value with a field
 * {@link Actor} does not declare resolves to no actor: a misspelled `kind`
 * would otherwise read as the human default.
 */
export function actorFromRequestContext(
  requestContext: RequestContext | undefined,
): Actor | undefined {
  const value = requestContext?.get(ACTOR_CONTEXT_KEY);
  if (!value || typeof value !== 'object') return undefined;
  if (unknownFieldOf(value, ACTOR_KEYS) !== undefined) return undefined;
  const candidate = value as Partial<Actor>;
  if (
    typeof candidate.id !== 'string' ||
    candidate.id.trim() === '' ||
    typeof candidate.role !== 'string'
  ) {
    return undefined;
  }
  // An unrecognized kind resolves to no actor rather than to a human: a value
  // this build does not understand must never fall through to the 'human'
  // default.
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
  /**
   * Exact roles authorized to call the agent. Consulted for humans only. A
   * non-empty array of distinct {@link ROLES} members; construction copies it.
   */
  allowedRoles: readonly Role[];
  /**
   * Exact principal kinds authorized to call the agent. Defaults to
   * `['human']`, so an existing configuration denies every automated principal
   * without changing a line — the caller must name the automation it wants.
   */
  allowedPrincipalKinds?: readonly PrincipalKind[];
  /**
   * Optional audit logger for authorization decisions and lookup failures. A
   * present value must have a callable `record`.
   */
  audit?: AuditLogger;
  /** Audit resource. Defaults to the stable processor identifier. */
  resource?: string;
  /**
   * Override actor sourcing. Default reads ACTOR_CONTEXT_KEY from
   * requestContext. A lookup that throws, or returns an actor whose `kind` is
   * not a {@link PRINCIPAL_KINDS} member, is an audited denial.
   */
  getActor?: (args: ProcessInputArgs) => Actor | undefined;
}

const RBAC_MIDDLEWARE_OPTION_KEYS = {
  allowedRoles: true,
  allowedPrincipalKinds: true,
  audit: true,
  resource: true,
  getActor: true,
} satisfies Record<keyof RBACMiddlewareOptions, true>;

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
    assertKnownFields(
      'RBACMiddleware: options',
      options,
      RBAC_MIDDLEWARE_OPTION_KEYS,
    );
    this.#allowedRoles = readAllowedRoles(
      'RBACMiddleware',
      options.allowedRoles,
    );
    this.#allowedPrincipalKinds = assertPrincipalKinds(
      options.allowedPrincipalKinds,
      'RBACMiddleware',
    );
    // A logger that cannot record would throw at the first decision, before
    // the denial, and Mastra's durable preparation runs the model past that
    // error. A present value without a callable `record`, null included, is
    // therefore refused rather than read as omitted.
    const audit = options.audit;
    if (
      audit !== undefined &&
      (typeof audit !== 'object' ||
        audit === null ||
        typeof audit.record !== 'function')
    ) {
      throw new TypeError(
        `RBACMiddleware: audit must be an AuditLogger when provided (got ${describeEntry(audit)})`,
      );
    }
    this.#audit = audit;
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
      deny: (reason) =>
        stopWithoutCallMessages(args.messageList, {}, () => args.abort(reason)),
    });
    return args.messages;
  }
}
