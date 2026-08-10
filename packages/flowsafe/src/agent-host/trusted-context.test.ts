// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';

import {
  createTrustedAgentRequestContext,
  deriveTrustedAgentContext,
  sanitizeStoredAgentContext,
} from './trusted-context.js';
import type { TrustedAgentExecution } from './types.js';

const execution: TrustedAgentExecution = {
  agentId: 'writer',
  principal: {
    kind: 'human',
    id: 'operator-1',
    role: 'operator',
  },
  threadId: 'acme_thread',
  resourceId: 'acme_resource',
  runId: 'acme_run',
  entryPath: 'http.start',
  principalPermissions: {
    permissions: ['reports.read'],
    policyVersion: 'permissions-v1',
  },
  safeContext: {
    preservedFromExecution: 'yes',
    runId: 'forged-from-stored-state',
    'breakwater.actor': { id: 'forged-from-stored-state' },
    'breakwater.principalPermissions': {
      permissions: ['forged.everything'],
      policyVersion: 'forged',
    },
  },
};

describe('trusted agent context boundary', () => {
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
      'breakwater.principalPermissions': {
        permissions: ['forged.everything'],
        policyVersion: 'forged',
      },
    });
    expect(values).toMatchObject({
      safe: 'preserved',
      preservedFromExecution: 'yes',
      runId: 'acme_run',
      threadId: 'acme_thread',
      resourceId: 'acme_resource',
      'breakwater.actor': { id: 'operator-1', role: 'operator' },
      'breakwater.principalPermissions': {
        permissions: ['reports.read'],
        policyVersion: 'permissions-v1',
      },
      'breakwater.auditContext': {
        agentId: 'writer',
        runId: 'acme_run',
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
        entryPath: 'http.start',
      },
    });
  });

  it('projects an explicit null when the execution carries no resolution, retiring any stale persisted value', () => {
    const values = deriveTrustedAgentContext({
      ...execution,
      principalPermissions: null,
    });
    // Present-with-null, not absent: the resume merge is provided-keys-win,
    // so only an explicit value can overwrite a stale persisted projection.
    expect(Object.hasOwn(values, 'breakwater.principalPermissions')).toBe(true);
    expect(values['breakwater.principalPermissions']).toBeNull();
  });

  it('creates a Mastra RequestContext with the same trusted values', () => {
    const context = createTrustedAgentRequestContext(execution);
    expect(context.get('breakwater.actor')).toEqual({
      id: 'operator-1',
      role: 'operator',
      kind: 'human',
    });
    expect(context.get('breakwater.principalPermissions')).toEqual({
      permissions: ['reports.read'],
      policyVersion: 'permissions-v1',
    });
    expect(context.get('breakwater.auditContext')).toMatchObject({
      agentId: 'writer',
      entryPath: 'http.start',
    });
    expect(context.get('preservedFromExecution')).toBe('yes');
    expect(context.get('runId')).toBe('acme_run');
  });
});
