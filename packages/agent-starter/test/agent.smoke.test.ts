// SPDX-License-Identifier: Apache-2.0

import { RequestContext } from '@mastra/core/request-context';
import type { ToolExecutionContext } from '@mastra/core/tools';
import {
  ConnectorPolicyError,
  ISOLATION_SCOPE_CONTEXT_KEY,
} from '@proofoftech/breakwater';
import { describe, expect, it, vi } from 'vitest';

import {
  createRecordActionConnector,
  createStarterAgent,
  RECORD_ACTION_CONNECTOR_ID,
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
    const agent = createStarterAgent({
      model: deterministicModel,
      db,
    });

    const result = await agent.generate('smoke test');
    const tools = await agent.listTools();

    expect(result.text).toBe('deterministic starter response');
    expect(tools).toHaveProperty(RECORD_ACTION_CONNECTOR_ID);
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
