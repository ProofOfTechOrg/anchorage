// SPDX-License-Identifier: Apache-2.0
import type { MastraDBMessage } from '@mastra/core/agent/message-list';
import { MessageList } from '@mastra/core/agent/message-list';
import type { ProcessInputArgs } from '@mastra/core/processors';
import { RequestContext } from '@mastra/core/request-context';
import { describe, expect, it } from 'vitest';

import { AGENT_AUDIT_CONTEXT_KEY, AuditLogger } from '../audit/index.js';
import {
  ACTOR_CONTEXT_KEY,
  type Actor,
  type PrincipalKind,
  RBACMiddleware,
} from './index.js';

class Tripwire extends Error {}

let messageSeq = 0;

function makeMessage(text: string): MastraDBMessage {
  return {
    id: `msg-${++messageSeq}`,
    role: 'user',
    createdAt: new Date(),
    content: { format: 2, parts: [{ type: 'text', text }] },
  };
}

function makeInputArgs(
  options: { text?: string; contextValue?: unknown } = {},
): ProcessInputArgs {
  const requestContext = new RequestContext();
  if (options.contextValue !== undefined) {
    requestContext.set(ACTOR_CONTEXT_KEY, options.contextValue);
  }
  return {
    messages: [makeMessage(options.text ?? 'hello')],
    messageList: new MessageList(),
    systemMessages: [],
    state: {},
    retryCount: 0,
    requestContext,
    abort: (reason?: string): never => {
      throw new Tripwire(reason ?? 'aborted');
    },
  };
}

const OPERATOR: Actor = { id: 'user-1', role: 'operator' };

describe('RBACMiddleware', () => {
  it('passes messages through for an allowed role and records an allowed audit event', () => {
    // #given
    const audit = new AuditLogger();
    const rbac = new RBACMiddleware({
      allowedRoles: ['operator', 'admin'],
      audit,
    });
    const args = makeInputArgs({ contextValue: OPERATOR });

    // #when
    const result = rbac.processInput(args);

    // #then
    expect(result).toBe(args.messages);
    expect(audit.events()).toHaveLength(1);
    expect(audit.events()[0]).toMatchObject({
      action: 'agent.input.authorize',
      decision: 'allowed',
      actor: OPERATOR,
      resource: 'breakwater-rbac',
    });
  });

  it('uses a configured resource and safe request correlation', () => {
    const audit = new AuditLogger();
    const rbac = new RBACMiddleware({
      allowedRoles: ['operator'],
      audit,
      resource: 'agent:writer',
    });
    const args = makeInputArgs({ contextValue: OPERATOR });
    args.requestContext?.set(AGENT_AUDIT_CONTEXT_KEY, {
      agentId: 'writer',
      entryPath: 'http-start',
      prompt: 'private prompt',
    });

    rbac.processInput(args);

    expect(audit.events()[0]).toMatchObject({
      resource: 'agent:writer',
      detail: { agentId: 'writer', entryPath: 'http-start' },
    });
    expect(JSON.stringify(audit.events())).not.toContain('private prompt');
  });

  it('allows the builder and reviewer roles when listed', () => {
    // #given
    const rbac = new RBACMiddleware({ allowedRoles: ['builder', 'reviewer'] });

    // #when / #then
    for (const role of ['builder', 'reviewer'] as const) {
      const args = makeInputArgs({ contextValue: { id: `u-${role}`, role } });
      expect(rbac.processInput(args)).toBe(args.messages);
    }
  });

  it('aborts for a role outside allowedRoles and records the denial', () => {
    // #given
    const audit = new AuditLogger();
    const rbac = new RBACMiddleware({ allowedRoles: ['admin'], audit });
    const args = makeInputArgs({
      contextValue: { id: 'user-2', role: 'viewer' },
    });

    // #when
    let caught: unknown;
    try {
      rbac.processInput(args);
    } catch (error) {
      caught = error;
    }

    // #then
    expect(caught).toBeInstanceOf(Tripwire);
    expect((caught as Error).message).toMatch(
      /role 'viewer' is not in allowed roles/,
    );
    expect(audit.events()[0]).toMatchObject({
      decision: 'denied',
      actor: { id: 'user-2', role: 'viewer' },
    });
  });

  it('aborts when no actor is present and records a null-actor denial', () => {
    // #given
    const audit = new AuditLogger();
    const rbac = new RBACMiddleware({ allowedRoles: ['admin'], audit });

    // #when / #then
    expect(() => rbac.processInput(makeInputArgs())).toThrowError(
      /no actor in request context/,
    );
    expect(audit.events()[0]).toMatchObject({
      decision: 'denied',
      actor: null,
    });
  });

  it('treats malformed context values as missing actors', () => {
    // #given
    const rbac = new RBACMiddleware({ allowedRoles: ['admin'] });
    const malformed = [
      42,
      'admin',
      { id: 'x' },
      { id: '', role: 'admin' },
      { id: '   ', role: 'admin' },
      { id: 'x', role: 'superuser' },
    ];

    // #when / #then
    for (const contextValue of malformed) {
      expect(() =>
        rbac.processInput(makeInputArgs({ contextValue })),
      ).toThrowError(/no actor in request context/);
    }
  });

  it('supports custom actor sourcing via getActor', () => {
    // #given
    const rbac = new RBACMiddleware({
      allowedRoles: ['reviewer'],
      getActor: () => ({ id: 'jwt-sub', role: 'reviewer' }),
    });
    const args = makeInputArgs();

    // #when / #then
    expect(rbac.processInput(args)).toBe(args.messages);
  });

  it('fails closed when a custom actor source returns a malformed actor', () => {
    const rbac = new RBACMiddleware({
      allowedRoles: ['reviewer'],
      getActor: () => ({ id: '   ', role: 'reviewer' }),
    });

    expect(() => rbac.processInput(makeInputArgs())).toThrowError(
      /no actor in request context/,
    );
  });

  it('records an error audit event and rethrows when getActor throws', () => {
    // #given
    const audit = new AuditLogger();
    const rbac = new RBACMiddleware({
      allowedRoles: ['admin'],
      audit,
      getActor: () => {
        throw new Error('jwt decode failed');
      },
    });

    // #when / #then — the original error propagates (fail closed) AND the
    // audit trail records the gate failure, unlike a silent crash.
    expect(() => rbac.processInput(makeInputArgs())).toThrowError(
      'jwt decode failed',
    );
    expect(audit.events()).toHaveLength(1);
    expect(audit.events()[0]).toMatchObject({
      decision: 'error',
      actor: null,
      reason: 'actor lookup failed',
    });
    expect(JSON.stringify(audit.events())).not.toContain('jwt decode failed');
  });

  it('rejects an empty allowedRoles list at construction', () => {
    // #when / #then
    expect(() => new RBACMiddleware({ allowedRoles: [] })).toThrowError(
      /must not be empty/,
    );
  });
});

describe('RBACMiddleware principal kinds', () => {
  const SCHEDULER: Actor = {
    id: 'flowsafe-system',
    role: 'operator',
    kind: 'system',
  };

  it('denies an automated principal when the caller never opted in', () => {
    // #given — the pre-existing configuration shape: roles only.
    const audit = new AuditLogger();
    const rbac = new RBACMiddleware({
      allowedRoles: ['operator', 'admin'],
      audit,
    });

    // #when / #then — 'operator' is an allowed ROLE, so before principal kinds
    // this scheduled principal executed with human authority.
    expect(() =>
      rbac.processInput(makeInputArgs({ contextValue: SCHEDULER })),
    ).toThrowError(Tripwire);
    expect(audit.events()[0]).toMatchObject({
      decision: 'denied',
      reason: "principal kind 'system' is not in allowed kinds [human]",
    });
  });

  it('admits an automated principal only when its kind is named', () => {
    // #given
    const audit = new AuditLogger();
    const rbac = new RBACMiddleware({
      allowedRoles: ['operator'],
      allowedPrincipalKinds: ['system'],
      audit,
    });
    const args = makeInputArgs({ contextValue: SCHEDULER });

    // #when
    const result = rbac.processInput(args);

    // #then
    expect(result).toBe(args.messages);
    expect(audit.events()[0]).toMatchObject({ decision: 'allowed' });
  });

  it('ignores the role allowlist for an automated principal', () => {
    // #given — an automated principal carries a role only because Actor.role is
    // required. Whatever the host projected must not decide the outcome.
    const audit = new AuditLogger();
    const rbac = new RBACMiddleware({
      allowedRoles: ['admin'],
      allowedPrincipalKinds: ['service'],
      audit,
    });
    const args = makeInputArgs({
      contextValue: { id: 'delivery', role: 'viewer', kind: 'service' },
    });

    // #when / #then — 'viewer' is not in allowedRoles, yet the kind is allowed.
    expect(rbac.processInput(args)).toBe(args.messages);
    expect(audit.events()[0]).toMatchObject({ decision: 'allowed' });
  });

  it('still enforces the role allowlist for humans when automation is enabled', () => {
    // #given — opting in to automation must not widen the human path.
    const audit = new AuditLogger();
    const rbac = new RBACMiddleware({
      allowedRoles: ['admin'],
      allowedPrincipalKinds: ['human', 'system'],
      audit,
    });

    // #when / #then
    expect(() =>
      rbac.processInput(makeInputArgs({ contextValue: OPERATOR })),
    ).toThrowError(Tripwire);
    expect(audit.events()[0]).toMatchObject({
      decision: 'denied',
      reason: "role 'operator' is not in allowed roles [admin]",
    });
  });

  it('denies an unrecognized kind rather than defaulting it to human', () => {
    // #given — a kind this build does not know must not fall through to the
    // default every guarded agent already admits.
    const audit = new AuditLogger();
    const rbac = new RBACMiddleware({
      allowedRoles: ['operator'],
      allowedPrincipalKinds: ['human', 'system'],
      audit,
    });

    // #when / #then — actorFromRequestContext resolves no actor at all.
    expect(() =>
      rbac.processInput(
        makeInputArgs({
          contextValue: { id: 'u', role: 'operator', kind: 'superuser' },
        }),
      ),
    ).toThrowError(Tripwire);
    expect(audit.events()[0]).toMatchObject({
      decision: 'denied',
      actor: null,
      reason: "no actor in request context (key 'breakwater.actor')",
    });
  });

  it('rejects an unknown or empty kind allowlist at construction', () => {
    // #when / #then
    expect(
      () =>
        new RBACMiddleware({
          allowedRoles: ['admin'],
          allowedPrincipalKinds: [],
        }),
    ).toThrowError(/must be a non-empty array/);
    expect(
      () =>
        new RBACMiddleware({
          allowedRoles: ['admin'],
          allowedPrincipalKinds: ['root' as PrincipalKind],
        }),
    ).toThrowError(/unknown principal kind 'root'/);
    expect(
      () =>
        new RBACMiddleware({
          allowedRoles: ['admin'],
          allowedPrincipalKinds: ['system', 'system'],
        }),
    ).toThrowError(/duplicate principal kind 'system'/);
  });
});

// AuditLogger's own tests live in ../audit/audit.test.ts.
