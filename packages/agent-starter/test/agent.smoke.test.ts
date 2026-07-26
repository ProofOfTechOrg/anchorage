// SPDX-License-Identifier: Apache-2.0

import { RequestContext } from '@mastra/core/request-context';
import type { ToolExecutionContext } from '@mastra/core/tools';
import {
  ACTOR_CONTEXT_KEY,
  AuditLogger,
  ConnectorPolicyError,
  ISOLATION_SCOPE_CONTEXT_KEY,
} from '@proofoftech/breakwater';
import { describe, expect, it, vi } from 'vitest';

import {
  createRecordActionConnector,
  createStarterAgentModule,
  STARTER_AGENT_META,
} from '../src/agent.js';
import { deterministicModel } from './deterministic-model.js';

function sideEffectTrap(): {
  db: Env['DB'];
  prepare: ReturnType<typeof vi.fn>;
} {
  const prepare = vi.fn(() => {
    throw new Error('the smoke test must not reach D1');
  });
  return {
    db: { prepare } as unknown as Env['DB'],
    prepare,
  };
}

describe('advanced starter agent', () => {
  it('generates deterministically without a provider credential or side effect', async () => {
    const { db, prepare } = sideEffectTrap();
    const events: unknown[] = [];
    const module = createStarterAgentModule({
      model: deterministicModel,
      db,
      audit: new AuditLogger({
        sink: (event) => {
          events.push(event);
        },
      }),
    });
    const requestContext = new RequestContext();
    requestContext.set(ACTOR_CONTEXT_KEY, {
      id: 'starter-operator',
      role: 'operator',
    });

    const result = await module.agent.generate('smoke test', {
      requestContext,
    });

    expect(result.text).toBe('deterministic starter response');
    expect(module.meta).toEqual(STARTER_AGENT_META);
    expect(module.agent.allowedRoles).toEqual(STARTER_AGENT_META.allowedRoles);
    expect(events).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          action: 'agent.input.authorize',
          resource: `agent:${STARTER_AGENT_META.id}`,
          decision: 'allowed',
        }),
      ]),
    );
    expect(prepare).not.toHaveBeenCalled();
  });

  it('denies an ungranted write before the connector can touch D1', async () => {
    const { db, prepare } = sideEffectTrap();
    const connector = createRecordActionConnector(db);
    const requestContext = new RequestContext();
    requestContext.set(ISOLATION_SCOPE_CONTEXT_KEY, 'acme');
    const context = {
      requestContext,
      agent: {
        agentId: 'anchorage-agent',
        toolCallId: 'call-smoke',
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
        messages: [],
        suspend: async () => undefined,
      },
    } as unknown as ToolExecutionContext;
    if (!connector.execute) throw new Error('connector has no execute method');

    const failure = await connector
      .execute({ action: 'must remain unrecorded' }, context)
      .catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect((failure as ConnectorPolicyError).policy).toBe('write-permissions');
    expect(prepare).not.toHaveBeenCalled();
  });
});
