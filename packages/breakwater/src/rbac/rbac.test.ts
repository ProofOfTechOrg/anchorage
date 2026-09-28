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
  actorFromRequestContext,
  type PrincipalKind,
  principalKindOf,
  RBACMiddleware,
  type RBACMiddlewareOptions,
  type Role,
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

    // An undeclared field records an error event before the decision, and
    // neither record copies it.
    expect(audit.events()).toMatchObject([
      {
        action: 'audit.context',
        resource: 'agent:writer',
        decision: 'error',
        detail: { agentId: 'writer', entryPath: 'http-start' },
      },
      {
        action: 'agent.input.authorize',
        resource: 'agent:writer',
        decision: 'allowed',
        detail: { agentId: 'writer', entryPath: 'http-start' },
      },
    ]);
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

  it('records one error audit event and denies when getActor throws', () => {
    // #given
    const audit = new AuditLogger();
    const rbac = new RBACMiddleware({
      allowedRoles: ['admin'],
      audit,
      getActor: () => {
        throw new Error('jwt decode failed');
      },
    });

    // #when / #then — the call is denied (fail closed) with a static reason,
    // and the audit trail records the gate failure, unlike a silent crash.
    expect(() => rbac.processInput(makeInputArgs())).toThrow(
      new Tripwire('actor lookup failed'),
    );
    expect(audit.events()).toHaveLength(1);
    expect(audit.events()[0]).toMatchObject({
      decision: 'error',
      actor: null,
      reason: 'actor lookup failed',
    });
    expect(JSON.stringify(audit.events())).not.toContain('jwt decode failed');
  });

  it('denies an actor whose field getter throws as a failed lookup', () => {
    // #given
    const audit = new AuditLogger();
    const rbac = new RBACMiddleware({
      allowedRoles: ['admin'],
      audit,
      getActor: () =>
        ({
          get id(): string {
            throw new Error('lazy id failed');
          },
          role: 'admin',
        }) as Actor,
    });

    // #when / #then
    expect(() => rbac.processInput(makeInputArgs())).toThrow(
      new Tripwire('actor lookup failed'),
    );
    expect(audit.events()).toMatchObject([
      { decision: 'error', actor: null, reason: 'actor lookup failed' },
    ]);
  });

  it('removes the call input from the message list when it denies', () => {
    // #given — the list holds the call's input, as Mastra's does
    const rbac = new RBACMiddleware({ allowedRoles: ['admin'] });
    const args = makeInputArgs({ contextValue: OPERATOR });
    args.messageList.add(args.messages, 'input');

    // #when / #then
    expect(() => rbac.processInput(args)).toThrowError(Tripwire);
    expect(args.messageList.get.input.db()).toEqual([]);
  });

  it.each<[string, unknown, string]>([
    ['null', null, 'null'],
    ['an object without record', {}, 'object'],
    ['an object whose record is not a function', { record: 'yes' }, 'object'],
    ['a function', () => undefined, 'function'],
  ])('refuses %s as audit at construction', (_label, audit, got) => {
    // #when / #then
    expect(
      () =>
        new RBACMiddleware({
          allowedRoles: ['admin'],
          audit: audit as AuditLogger,
        }),
    ).toThrow(
      new TypeError(
        `RBACMiddleware: audit must be an AuditLogger when provided (got ${got})`,
      ),
    );
  });

  it.each<[string, unknown, string]>([
    [
      "a string, which would authorize 'viewer' as a substring of 'reviewer'",
      'reviewer',
      'RBACMiddleware: allowedRoles must be an array',
    ],
    ['a number', 42, 'RBACMiddleware: allowedRoles must be an array'],
    ['null', null, 'RBACMiddleware: allowedRoles must be an array'],
    ['a plain object', {}, 'RBACMiddleware: allowedRoles must be an array'],
    [
      'an empty list',
      [],
      'RBACMiddleware: allowedRoles must be a non-empty array',
    ],
    [
      'an unknown member',
      ['admin', 'superuser'],
      'RBACMiddleware: allowedRoles entry 1 is an unknown allowed role (got "superuser")',
    ],
    [
      'a duplicate',
      ['admin', 'admin'],
      'RBACMiddleware: allowedRoles entry 1 is a duplicate allowed role (got "admin")',
    ],
  ])('refuses %s as allowedRoles at construction', (_label, allowedRoles, message) => {
    // #when / #then
    expect(
      () =>
        new RBACMiddleware({
          allowedRoles: allowedRoles as readonly Role[],
        }),
    ).toThrow(new TypeError(message));
  });

  it('refuses a misspelled option, which would leave decisions unaudited', () => {
    // #when / #then
    expect(
      () =>
        new RBACMiddleware({
          allowedRoles: ['admin'],
          auditLogger: new AuditLogger(),
        } as RBACMiddlewareOptions),
    ).toThrow(
      new TypeError(
        'RBACMiddleware: options has unknown field "auditLogger" (valid fields: allowedRoles, allowedPrincipalKinds, audit, resource, getActor)',
      ),
    );
  });

  it('constructs with every declared option', () => {
    // #given — the Required type fails to compile while a declared option is
    // missing here
    const options: Required<RBACMiddlewareOptions> = {
      allowedRoles: ['admin'],
      allowedPrincipalKinds: ['human'],
      audit: new AuditLogger(),
      resource: 'agent:writer',
      getActor: () => undefined,
    };
    // #when / #then
    expect(new RBACMiddleware(options).id).toBe('breakwater-rbac');
  });
});

describe('RBACMiddleware principal kinds', () => {
  const SCHEDULER: Actor = {
    id: 'flowsafe-system',
    role: 'operator',
    kind: 'system',
  };

  it('denies an automated principal when the caller never opted in', () => {
    // #given — a configuration that names roles only.
    const audit = new AuditLogger();
    const rbac = new RBACMiddleware({
      allowedRoles: ['operator', 'admin'],
      audit,
    });

    // #when / #then — 'operator' is an allowed ROLE, so without the kind check
    // this scheduled principal would execute with human authority.
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
    // 'human' default.
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

  it.each<[string, Record<string, unknown>]>([
    ['principalKind', { principalKind: 'service' }],
    ['Kind', { Kind: 'service' }],
    ['type', { type: 'service' }],
    ['an extra field beside a valid kind', { kind: 'human', tenant: 'acme' }],
  ])('resolves no actor for a %s field the actor does not declare', (_label, extra) => {
    // #given — only humans are allowed, and 'operator' is an allowed role
    const audit = new AuditLogger();
    const rbac = new RBACMiddleware({ allowedRoles: ['operator'], audit });

    // #when / #then — a misspelled kind must not read as the human default
    expect(() =>
      rbac.processInput(
        makeInputArgs({
          contextValue: { id: 'svc-1', role: 'operator', ...extra },
        }),
      ),
    ).toThrow(
      new Tripwire("no actor in request context (key 'breakwater.actor')"),
    );
    expect(audit.events()).toMatchObject([{ decision: 'denied', actor: null }]);
    expect(
      actorFromRequestContext(
        makeInputArgs({
          contextValue: { id: 'svc-1', role: 'operator', ...extra },
        }).requestContext,
      ),
    ).toBeUndefined();
  });

  it('still denies the correctly spelled automated kind by its kind', () => {
    // #given
    const rbac = new RBACMiddleware({ allowedRoles: ['operator'] });

    // #when / #then
    expect(() =>
      rbac.processInput(
        makeInputArgs({
          contextValue: { id: 'svc-1', role: 'operator', kind: 'service' },
        }),
      ),
    ).toThrow(
      new Tripwire("principal kind 'service' is not in allowed kinds [human]"),
    );
  });

  it.each<[string, unknown, string]>([
    ['null', null, 'null'],
    ["the unknown 'robot'", 'robot', '"robot"'],
    ['a number', 42, 'number'],
    ["the wrong-case 'Service'", 'Service', '"Service"'],
  ])('principalKindOf refuses %s as a kind rather than reading it as human', (_label, kind, got) => {
    // #when / #then
    expect(() => principalKindOf({ kind: kind as PrincipalKind })).toThrow(
      new TypeError(
        `principalKindOf: kind must be one of human, service, agent, system (got ${got})`,
      ),
    );
  });

  it('principalKindOf reads an absent kind as human and keeps a member', () => {
    // #when / #then
    expect(principalKindOf({})).toBe('human');
    expect(principalKindOf({ kind: undefined })).toBe('human');
    expect(principalKindOf({ kind: 'service' })).toBe('service');
  });

  it.each<[string, unknown]>([
    ['a null kind', null],
    ["the unknown 'robot'", 'robot'],
    ['a symbol', Symbol('kind')],
  ])('records an audited denial when a custom getActor returns %s', (_label, kind) => {
    // #given
    const audit = new AuditLogger();
    const rbac = new RBACMiddleware({
      allowedRoles: ['operator'],
      audit,
      getActor: () => ({ id: 'u', role: 'operator', kind }) as unknown as Actor,
    });

    // #when / #then — the kind never reads as a human, and the reason does not
    // carry it
    expect(() => rbac.processInput(makeInputArgs())).toThrow(
      new Tripwire('principal kind is not declared'),
    );
    expect(audit.events()).toMatchObject([
      {
        action: 'agent.input.authorize',
        decision: 'denied',
        actor: null,
        reason: 'principal kind is not declared',
      },
    ]);
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
