// SPDX-License-Identifier: Apache-2.0

import type { GuardedAgentHandle } from '@proofoftech/breakwater/agent';
import { describe, expect, it, vi } from 'vitest';
import type { ApprovalRole } from '../approval-api/index.js';
import { type AgentMeta, isPermissionIdentifier } from './types.js';

const GUARDED_AGENT_HOST_PROTOCOL = Symbol.for(
  '@proofoftech/breakwater/guarded-agent-host/v1',
);

vi.mock('@proofoftech/breakwater/agent', () => ({
  isGuardedAgentHandle: (value: unknown) =>
    typeof value === 'object' &&
    value !== null &&
    (value as { guarded?: unknown }).guarded === true,
}));

import { AGENT_ENTRY_PATHS } from '../agent-runner/index.js';
import { createAgentCatalog, createAgentModuleCatalog } from './catalog.js';

function handle(
  id = 'writer',
  allowedRoles: readonly ApprovalRole[] = ['admin', 'operator'],
  allowedPrincipalKinds: readonly string[] = ['human'],
): GuardedAgentHandle {
  return {
    guarded: true,
    [GUARDED_AGENT_HOST_PROTOCOL]: {
      version: 1,
      supportsDurableStructuredOutput: false,
    },
    id,
    allowedRoles,
    allowedPrincipalKinds,
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
    expect(catalog.agents[0]).not.toHaveProperty('requiredPermissions');
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

  it('rejects a guarded handle without the durable-host protocol', () => {
    const legacy = { ...handle() } as Record<PropertyKey, unknown>;
    delete legacy[GUARDED_AGENT_HOST_PROTOCOL];

    expect(() =>
      createAgentModuleCatalog([
        { meta, agent: legacy as unknown as GuardedAgentHandle },
      ]),
    ).toThrow(/without the durable-host protocol.*>=0\.12\.0/);
  });

  it('rejects an incompatible durable-host protocol', () => {
    const incompatible = handle() as unknown as Record<PropertyKey, unknown>;
    incompatible[GUARDED_AGENT_HOST_PROTOCOL] = {
      version: 1,
      supportsDurableStructuredOutput: true,
    };

    expect(() =>
      createAgentModuleCatalog([
        { meta, agent: incompatible as unknown as GuardedAgentHandle },
      ]),
    ).toThrow(/malformed Breakwater guarded-agent host protocol/);
  });
});

describe('agent permission declaration', () => {
  it('preserves a canonical all-of declaration as an immutable copy', () => {
    const requiredPermissions = ['reports.read', 'agents.report.run'];
    const catalog = createAgentCatalog([{ ...meta, requiredPermissions }]);
    const normalized = catalog.get(meta.id)?.requiredPermissions;

    expect(normalized).toEqual(requiredPermissions);
    expect(Object.isFrozen(normalized)).toBe(true);
    requiredPermissions[0] = 'payments.release';
    expect(normalized).toEqual(['reports.read', 'agents.report.run']);
  });

  it.each([
    ['reports.read', true],
    ['agents.report.run', true],
    ['', false],
    ['reports', false],
    ['Reports.read', false],
    ['reports..read', false],
    ['reports.write-all', false],
    [`${'a'.repeat(198)}.b`, true],
    [`${'a'.repeat(199)}.b`, false],
  ])('validates the public permission grammar for %j', (value, valid) => {
    expect(isPermissionIdentifier(value)).toBe(valid);
  });

  it.each([
    ['reports.read', 'must be an array'],
    [[], 'must not be empty'],
    [[''], 'malformed permission identifier'],
    [['reports'], 'malformed permission identifier'],
    [['Reports.read'], 'malformed permission identifier'],
    [['reports..read'], 'malformed permission identifier'],
    [[42], 'malformed permission identifier'],
    [['reports.read', 'reports.read'], "contains duplicate 'reports.read'"],
  ])('rejects invalid requiredPermissions %#', (requiredPermissions, message) => {
    expect(() =>
      createAgentCatalog([
        { ...meta, requiredPermissions } as unknown as AgentMeta,
      ]),
    ).toThrow(message as string);
  });
});

describe('agent automation declaration', () => {
  const automated = {
    ...meta,
    allowedAutomation: [
      { kind: 'system' as const, entryPaths: ['schedule.fire' as const] },
    ],
  };

  it('denies every automated entry when nothing is declared', () => {
    // #given — the shape every agent written before principals had.
    const catalog = createAgentCatalog([meta]);

    // #when / #then
    for (const kind of ['system', 'service', 'agent'] as const) {
      for (const entryPath of AGENT_ENTRY_PATHS) {
        expect(
          catalog.automationAllowed(
            meta.id,
            { kind, id: 'robot', purpose: 'p' },
            entryPath,
          ),
        ).toBe(false);
      }
    }
  });

  it('admits only the declared kind on the declared entry path', () => {
    // #given
    const catalog = createAgentCatalog([automated]);
    const system = {
      kind: 'system' as const,
      id: 'sched',
      purpose: 'scheduled-agent-execution',
    };

    // #when / #then
    expect(catalog.automationAllowed(meta.id, system, 'schedule.fire')).toBe(
      true,
    );
    // Same principal, a path it was not declared for.
    expect(catalog.automationAllowed(meta.id, system, 'signal.wake')).toBe(
      false,
    );
    // Declared path, a kind that was not declared.
    expect(
      catalog.automationAllowed(
        meta.id,
        { kind: 'service', id: 'svc', purpose: 'p' },
        'schedule.fire',
      ),
    ).toBe(false);
    // An agent that does not exist.
    expect(catalog.automationAllowed('ghost', system, 'schedule.fire')).toBe(
      false,
    );
  });

  it('never answers for a human, who is authorized by role instead', () => {
    // #given
    const catalog = createAgentCatalog([automated]);

    // #when / #then
    expect(
      catalog.automationAllowed(
        meta.id,
        { kind: 'human', id: 'op', role: 'operator' },
        'schedule.fire',
      ),
    ).toBe(false);
  });

  it.each([
    [[{ kind: 'human', entryPaths: ['schedule.fire'] }], 'is not an automated'],
    [[{ kind: 'system', entryPaths: [] }], 'must name at least one entry path'],
    [[{ kind: 'system', entryPaths: ['nope'] }], 'unknown entry path'],
    [
      [
        { kind: 'system', entryPaths: ['schedule.fire'] },
        { kind: 'system', entryPaths: ['signal.wake'] },
      ],
      'repeats kind',
    ],
  ])('rejects a malformed declaration (%#)', (allowedAutomation, message) => {
    // #when / #then
    expect(() =>
      createAgentCatalog([
        { ...meta, allowedAutomation } as unknown as AgentMeta,
      ]),
    ).toThrow(message as string);
  });

  it('refuses a module whose declaration disagrees with its guarded agent', () => {
    // #given — the two halves of one decision must not drift: flowsafe routes
    // the entry, breakwater decides whether the kind may execute at all.
    expect(() =>
      createAgentModuleCatalog([{ meta: automated, agent: handle() }]),
    ).toThrow(/allowedAutomation kinds \[system\] must exactly match/);
    expect(() =>
      createAgentModuleCatalog([
        {
          meta,
          agent: handle('writer', ['admin', 'operator'], ['human', 'system']),
        },
      ]),
    ).toThrow(/allowedAutomation kinds \[\] must exactly match/);
  });
});
