// SPDX-License-Identifier: Apache-2.0

import type { GuardedAgentHandle } from '@proofoftech/breakwater/agent';
import { describe, expect, it, vi } from 'vitest';
import type { ApprovalRole } from '../approval-api/index.js';
import type { AgentMeta } from './types.js';

vi.mock('@proofoftech/breakwater/agent', () => ({
  isGuardedAgentHandle: (value: unknown) =>
    typeof value === 'object' &&
    value !== null &&
    (value as { guarded?: unknown }).guarded === true,
}));

import { createAgentCatalog, createAgentModuleCatalog } from './catalog.js';

function handle(
  id = 'writer',
  allowedRoles: readonly ApprovalRole[] = ['admin', 'operator'],
): GuardedAgentHandle {
  return {
    guarded: true,
    id,
    allowedRoles,
    maxSteps: 1,
  } as unknown as GuardedAgentHandle;
}

const meta = {
  id: 'writer',
  title: 'Writer',
  description: 'Writes an approved record',
  allowedRoles: ['operator', 'admin'] as const,
};

describe('agent catalog', () => {
  it('normalizes omitted roles to the global run-start set', () => {
    const catalog = createAgentCatalog([
      {
        id: 'reader',
        title: 'Reader',
        description: 'Reads a record',
      },
    ]);
    expect(catalog.allowedRoles('reader')).toEqual([
      'admin',
      'operator',
      'builder',
    ]);
    expect(catalog.agents[0]).not.toHaveProperty('allowedRoles');
  });

  it.each([
    [[{ ...meta, id: '../writer' }], 'id must be URL-path-safe'],
    [[{ ...meta, title: ' ' }], 'title must not be empty'],
    [[{ ...meta, description: '' }], 'description must not be empty'],
    [[{ ...meta, allowedRoles: [] }], 'allowedRoles must not be empty'],
    [[{ ...meta, allowedRoles: ['reviewer'] }], 'is not a run-start role'],
    [
      [{ ...meta, allowedRoles: ['admin', 'admin'] }],
      "contains duplicate 'admin'",
    ],
    [[meta, meta], "duplicate agent id 'writer'"],
  ])('rejects invalid metadata %#', (agents, message) => {
    expect(() => createAgentCatalog(agents as AgentMeta[])).toThrow(message);
  });

  it('accepts a branded module whose ids and effective role sets match', () => {
    const catalog = createAgentModuleCatalog([{ meta, agent: handle() }]);
    expect(catalog.get('writer')?.agent.id).toBe('writer');
  });

  it('rejects unbranded handles and metadata/handle mismatches', () => {
    expect(() =>
      createAgentModuleCatalog([
        {
          meta,
          agent: {
            ...handle(),
            guarded: false,
          } as unknown as GuardedAgentHandle,
        },
      ]),
    ).toThrow('must be created by createGuardedAgent');
    expect(() =>
      createAgentModuleCatalog([{ meta, agent: handle('another') }]),
    ).toThrow("does not match guarded agent id 'another'");
    expect(() =>
      createAgentModuleCatalog([
        {
          meta,
          agent: handle('writer', ['admin', 'builder']),
        },
      ]),
    ).toThrow('metadata roles must exactly match guarded agent roles');
  });
});
