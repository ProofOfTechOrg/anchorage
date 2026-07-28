// SPDX-License-Identifier: Apache-2.0

import type { RequestContext } from '@mastra/core/request-context';

import { type AuditLogger, agentAuditDetail } from '../audit/index.js';
import type { Actor, Role } from './index.js';
import {
  DEFAULT_ALLOWED_PRINCIPAL_KINDS,
  type PrincipalKind,
  principalKindOf,
} from './principal.js';

export interface ActorAuthorizationOptions {
  allowedRoles: readonly Role[];
  /** Defaults to `['human']` — automated principals are denied unless named. */
  allowedPrincipalKinds?: readonly PrincipalKind[];
  audit?: AuditLogger;
  resource: string;
  requestContext?: RequestContext;
  resolveActor: () => Actor | undefined;
  deny: (reason: string) => never;
}

/**
 * Resolve and authorize one actor, with identical audit behavior at processor
 * and direct-call boundaries.
 */
export function authorizeActor(options: ActorAuthorizationOptions): Actor {
  let actor: Actor | undefined;
  try {
    actor = options.resolveActor();
  } catch (error) {
    options.audit?.record({
      actor: null,
      action: 'agent.input.authorize',
      resource: options.resource,
      decision: 'error',
      reason: 'actor lookup failed',
      detail: agentAuditDetail(options.requestContext),
    });
    throw error;
  }
  if (
    !actor ||
    typeof actor !== 'object' ||
    typeof actor.id !== 'string' ||
    actor.id.trim() === '' ||
    typeof actor.role !== 'string'
  ) {
    const reason = "no actor in request context (key 'breakwater.actor')";
    options.audit?.record({
      actor: null,
      action: 'agent.input.authorize',
      resource: options.resource,
      decision: 'denied',
      reason,
      detail: agentAuditDetail(options.requestContext),
    });
    options.deny(reason);
  }
  // Kind before role, and fail closed on an unnamed kind: a host that has not
  // thought about automation must not have its human role allowlist quietly
  // answer a question about a scheduled job.
  const allowedKinds =
    options.allowedPrincipalKinds ?? DEFAULT_ALLOWED_PRINCIPAL_KINDS;
  const kind = principalKindOf(actor);
  if (!allowedKinds.includes(kind)) {
    const reason = `principal kind '${kind}' is not in allowed kinds [${allowedKinds.join(', ')}]`;
    options.audit?.record({
      actor,
      action: 'agent.input.authorize',
      resource: options.resource,
      decision: 'denied',
      reason,
      detail: agentAuditDetail(options.requestContext),
    });
    options.deny(reason);
  }
  // Roles describe human authority, so they are authoritative only for humans.
  // An automated principal carries a role solely because `Actor.role` is
  // required; checking it here would mean either admitting whichever human role
  // the host projected, or forcing hosts to add that role to `allowedRoles` and
  // thereby admitting real humans holding it. The kind allowlist above is the
  // whole gate for automation, and it is opt-in.
  if (kind === 'human' && !options.allowedRoles.includes(actor.role)) {
    const reason = `role '${actor.role}' is not in allowed roles [${options.allowedRoles.join(', ')}]`;
    options.audit?.record({
      actor,
      action: 'agent.input.authorize',
      resource: options.resource,
      decision: 'denied',
      reason,
      detail: agentAuditDetail(options.requestContext),
    });
    options.deny(reason);
  }
  options.audit?.record({
    actor,
    action: 'agent.input.authorize',
    resource: options.resource,
    decision: 'allowed',
    detail: agentAuditDetail(options.requestContext),
  });
  return actor;
}
