import type { MastraDBMessage } from '@mastra/core/agent/message-list';
import { MessageList } from '@mastra/core/agent/message-list';
import type { ProcessInputArgs } from '@mastra/core/processors';
import { RequestContext } from '@mastra/core/request-context';
import { describe, expect, it } from 'vitest';

import { AuditLogger } from '../audit/index.js';
import { ACTOR_CONTEXT_KEY, type Actor, RBACMiddleware } from './index.js';

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
      reason: 'getActor threw: jwt decode failed',
    });
  });

  it('rejects an empty allowedRoles list at construction', () => {
    // #when / #then
    expect(() => new RBACMiddleware({ allowedRoles: [] })).toThrowError(
      /must not be empty/,
    );
  });
});

// AuditLogger's own tests live in ../audit/audit.test.ts.
