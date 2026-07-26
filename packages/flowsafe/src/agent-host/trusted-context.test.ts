// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';

import {
  createTrustedAgentRequestContext,
  deriveTrustedAgentContext,
  rejectReservedAgentContext,
  sanitizeStoredAgentContext,
} from './trusted-context.js';
import type { TrustedAgentExecution } from './types.js';

const execution: TrustedAgentExecution = {
  agentId: 'writer',
  actor: { id: 'operator-1', role: 'operator', tenantId: 'acme' },
  threadId: 'acme_thread',
  resourceId: 'acme_resource',
  runId: 'acme_run',
  entryPath: 'http.start',
  safeContext: {
    preservedFromExecution: 'yes',
    runId: 'forged-from-stored-state',
    'breakwater.actor': { id: 'forged-from-stored-state' },
  },
};

describe('trusted agent context boundary', () => {
  it('rejects reserved external keys, including the complete breakwater namespace', () => {
    expect(() =>
      rejectReservedAgentContext({ 'breakwater.futureCapability': true }),
    ).toThrow("reserved key 'breakwater.futureCapability'");
    expect(() => rejectReservedAgentContext({ runId: 'forged' })).toThrow(
      "reserved key 'runId'",
    );
  });

  it('strips reserved persisted values without mutating the source', () => {
    const source = {
      safe: 'preserved',
      runId: 'forged',
      'breakwater.actor': { id: 'attacker' },
      constructor: 'forged',
    };
    expect(sanitizeStoredAgentContext(source)).toEqual({
      safe: 'preserved',
    });
    expect(source.runId).toBe('forged');
  });

  it('merges sanitized context before exact trusted actor and correlation', () => {
    const values = deriveTrustedAgentContext(execution, {
      safe: 'preserved',
      runId: 'forged',
      'breakwater.auditContext': { agentId: 'forged' },
    });
    expect(values).toMatchObject({
      safe: 'preserved',
      preservedFromExecution: 'yes',
      runId: 'acme_run',
      threadId: 'acme_thread',
      resourceId: 'acme_resource',
      'breakwater.actor': { id: 'operator-1', role: 'operator' },
      'breakwater.auditContext': {
        agentId: 'writer',
        tenantId: 'acme',
        runId: 'acme_run',
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
        entryPath: 'http.start',
      },
    });
  });

  it('creates a Mastra RequestContext with the same trusted values', () => {
    const context = createTrustedAgentRequestContext(execution);
    expect(context.get('breakwater.actor')).toEqual({
      id: 'operator-1',
      role: 'operator',
    });
    expect(context.get('breakwater.auditContext')).toMatchObject({
      agentId: 'writer',
      entryPath: 'http.start',
    });
    expect(context.get('preservedFromExecution')).toBe('yes');
    expect(context.get('runId')).toBe('acme_run');
  });
});
