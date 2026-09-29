// SPDX-License-Identifier: Apache-2.0

import type { RequestContext } from '@mastra/core/request-context';

import {
  type AuditLogger,
  agentAuditDetail,
  malformedAgentAuditContextEvent,
} from '../audit/index.js';
import type { Actor, Role } from './actor.js';
import { PRINCIPAL_KINDS, type PrincipalKind } from './principal.js';

const ACTOR_LOOKUP_FAILED = 'actor lookup failed';

interface ActorFields {
  readonly id: unknown;
  readonly role: unknown;
  readonly kind: unknown;
}

// Each field is read once, so a later check cannot see a different value.
function actorFieldsOf(value: unknown): ActorFields | undefined {
  if (!value || typeof value !== 'object') return undefined;
  const { id, role, kind } = value as Partial<ActorFields>;
  return { id, role, kind };
}

function isDeclaredKind(kind: unknown): kind is PrincipalKind | undefined {
  return (
    kind === undefined || (PRINCIPAL_KINDS as readonly unknown[]).includes(kind)
  );
}

export interface ActorAuthorizationOptions {
  allowedRoles: readonly Role[];
  /**
   * Required, because both call sites normalize it through
   * `assertPrincipalKinds` first. A default here would be a second place the
   * human-only policy could drift from that one.
   */
  allowedPrincipalKinds: readonly PrincipalKind[];
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
  const malformedAuditContext = malformedAgentAuditContextEvent(
    options.requestContext,
    options.resource,
    null,
  );
  if (malformedAuditContext) options.audit?.record(malformedAuditContext);
  // A lookup that throws, a getter included, is denied rather than rethrown:
  // Mastra's durable preparation runs the model past an input processor's
  // error that is not a tripwire. The thrown value can carry the credential
  // being parsed, so the record and the reason stay static.
  let fields: ActorFields | undefined;
  try {
    fields = actorFieldsOf(options.resolveActor());
  } catch {
    options.audit?.record({
      actor: null,
      action: 'agent.input.authorize',
      resource: options.resource,
      decision: 'error',
      reason: ACTOR_LOOKUP_FAILED,
      detail: agentAuditDetail(options.requestContext),
    });
    options.deny(ACTOR_LOOKUP_FAILED);
  }
  const id = fields?.id;
  const role = fields?.role;
  const declaredKind = fields?.kind;
  if (typeof id !== 'string' || id.trim() === '' || typeof role !== 'string') {
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
  // Kind before role, and fail closed on an UNDECLARED kind: a host that has not
  // thought about automation must not have its human role allowlist quietly
  // answer a question about a scheduled job. An undeclared kind is not echoed,
  // since it can be any value a custom lookup returns.
  if (!isDeclaredKind(declaredKind)) {
    const reason = 'principal kind is not declared';
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
  const actor: Actor = {
    id,
    role: role as Role,
    ...(declaredKind !== undefined ? { kind: declaredKind } : {}),
  };
  const allowedKinds = options.allowedPrincipalKinds;
  const kind = declaredKind ?? 'human';
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
