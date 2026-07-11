// SPDX-License-Identifier: Apache-2.0
// Roadmap Phase 1 acceptance: "Processor chain evaluates without error".
// Mastra runs inputProcessors sequentially in array order, threading messages;
// this harness replicates that contract for [RBACMiddleware, PolicyEngine].

import type { MastraDBMessage } from '@mastra/core/agent/message-list';
import { MessageList } from '@mastra/core/agent/message-list';
import type { InputProcessor, ProcessInputArgs } from '@mastra/core/processors';
import { RequestContext } from '@mastra/core/request-context';
import { describe, expect, it } from 'vitest';

import { AuditLogger } from './audit/index.js';
import { denyPatterns, PolicyEngine } from './policy-engine/index.js';
import { ACTOR_CONTEXT_KEY, type Actor, RBACMiddleware } from './rbac/index.js';

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

function makeArgs(text: string, actor?: Actor): ProcessInputArgs {
  const requestContext = new RequestContext();
  if (actor) requestContext.set(ACTOR_CONTEXT_KEY, actor);
  return {
    messages: [makeMessage(text)],
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

async function runInputChain(
  chain: InputProcessor[],
  initialArgs: ProcessInputArgs,
): Promise<void> {
  let args = initialArgs;
  for (const processor of chain) {
    const result = await processor.processInput?.(args);
    if (Array.isArray(result)) {
      args = { ...args, messages: result };
    }
  }
}

describe('breakwater processor chain', () => {
  function buildChain(audit: AuditLogger): InputProcessor[] {
    const rbac = new RBACMiddleware({
      allowedRoles: ['operator', 'admin'],
      audit,
    });
    const policy = new PolicyEngine({
      policies: [denyPatterns(['rm -rf'])],
      audit,
    });
    return [rbac, policy];
  }

  it('evaluates without error for an authorized actor and clean input', async () => {
    // #given
    const audit = new AuditLogger();
    const args = makeArgs('summarize the quarterly report', {
      id: 'u1',
      role: 'operator',
    });

    // #when / #then
    await expect(
      runInputChain(buildChain(audit), args),
    ).resolves.toBeUndefined();
    expect(audit.events().map((e) => [e.action, e.decision])).toEqual([
      ['agent.input.authorize', 'allowed'],
      ['agent.input.policy', 'allowed'],
    ]);
  });

  it('stops at the RBAC gate before policy evaluation for unauthorized actors', async () => {
    // #given
    const audit = new AuditLogger();
    const args = makeArgs('summarize the quarterly report', {
      id: 'u2',
      role: 'viewer',
    });

    // #when / #then
    await expect(runInputChain(buildChain(audit), args)).rejects.toThrowError(
      Tripwire,
    );
    expect(audit.events()).toHaveLength(1);
    expect(audit.events()[0]).toMatchObject({
      action: 'agent.input.authorize',
      decision: 'denied',
    });
  });

  it('stops at the policy gate for blocked content from an authorized actor', async () => {
    // #given
    const audit = new AuditLogger();
    const args = makeArgs('please run rm -rf / now', {
      id: 'u1',
      role: 'admin',
    });

    // #when / #then
    await expect(runInputChain(buildChain(audit), args)).rejects.toThrowError(
      /deny-patterns/,
    );
    expect(audit.events().map((e) => [e.action, e.decision])).toEqual([
      ['agent.input.authorize', 'allowed'],
      ['agent.input.policy', 'denied'],
    ]);
  });
});
