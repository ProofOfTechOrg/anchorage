// SPDX-License-Identifier: Apache-2.0

import type { RequestContext } from '@mastra/core/request-context';

import { type AuditLogger, agentAuditDetail } from '../audit/index.js';
import type { Actor, Role } from './index.js';

export interface ActorAuthorizationOptions {
  allowedRoles: readonly Role[];
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
  if (!options.allowedRoles.includes(actor.role)) {
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
