// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import type { ResumeLedgerStorage } from '../do-runner/index.js';
import {
  AgentRunStateConflictError,
  AgentRunStateError,
  bindAgentThread,
  deleteAgentRunRecord,
  readAgentRunRecord,
  readAgentThreadBinding,
  writeAgentRunRecord,
} from './agent-run-state.js';

function memoryStorage(): ResumeLedgerStorage & {
  values: Map<string, unknown>;
} {
  const values = new Map<string, unknown>();
  return {
    values,
    get: async <T>(key: string) => values.get(key) as T | undefined,
    put: async (key, value) => {
      values.set(key, structuredClone(value));
    },
    delete: async (key) => values.delete(key),
  };
}

describe('durable agent thread/run metadata', () => {
  it('binds a thread once and rejects an agent/resource change', async () => {
    const storage = memoryStorage();
    const binding = {
      version: 1 as const,
      agentId: 'writer',
      resourceId: 'acme_resource',
    };

    await expect(bindAgentThread(storage, binding)).resolves.toEqual(binding);
    await expect(bindAgentThread(storage, binding)).resolves.toEqual(binding);
    await expect(
      bindAgentThread(storage, { ...binding, agentId: 'reviewer' }),
    ).rejects.toBeInstanceOf(AgentRunStateConflictError);
    await expect(readAgentThreadBinding(storage)).resolves.toEqual(binding);
  });

  it('preserves the original principal until terminal cleanup', async () => {
    const storage = memoryStorage();
    const record = {
      version: 2 as const,
      agentId: 'writer',
      principal: {
        kind: 'human' as const,
        id: 'starter',
        tenantId: 'acme',
        role: 'operator' as const,
      },
      originEntryPath: 'http.start' as const,
    };

    await writeAgentRunRecord(storage, 'acme_run-1', record);
    await expect(
      writeAgentRunRecord(storage, 'acme_run-1', {
        ...record,
        principal: { ...record.principal, id: 'reviewer' },
      }),
    ).rejects.toBeInstanceOf(AgentRunStateConflictError);
    await expect(readAgentRunRecord(storage, 'acme_run-1')).resolves.toEqual(
      record,
    );
    await deleteAgentRunRecord(storage, 'acme_run-1');
    await expect(
      readAgentRunRecord(storage, 'acme_run-1'),
    ).resolves.toBeUndefined();
  });

  it('fails closed on malformed persisted state and foreign run principals', async () => {
    const storage = memoryStorage();
    storage.values.set('flowsafe:agent-thread-binding:v1', {
      version: 2,
      agentId: '../writer',
      resourceId: 'acme_resource',
    });
    await expect(readAgentThreadBinding(storage)).rejects.toBeInstanceOf(
      AgentRunStateError,
    );
    await expect(
      writeAgentRunRecord(storage, 'acme_run-1', {
        version: 2,
        agentId: 'writer',
        principal: {
          kind: 'human',
          id: 'starter',
          tenantId: 'globex',
          role: 'operator',
        },
        originEntryPath: 'http.start',
      }),
    ).rejects.toBeInstanceOf(AgentRunStateError);
    await expect(
      writeAgentRunRecord(storage, 'acme_run-1', {
        version: 2,
        agentId: 'writer',
        principal: {
          kind: 'human',
          id: '   ',
          tenantId: 'acme',
          role: 'operator',
        },
        originEntryPath: 'http.start',
      }),
    ).rejects.toBeInstanceOf(AgentRunStateError);
  });
});

describe('agent run metadata migration', () => {
  it('rejects a version-1 record rather than upgrading it to a human', async () => {
    // #given — exactly what the previous release wrote for a schedule.fire run:
    // an ApprovalActor whose fabricated role was 'operator'.
    const storage = memoryStorage();
    await storage.put('flowsafe:agent-run:v1:acme_run-1', {
      version: 1,
      agentId: 'writer',
      principal: { id: 'flowsafe-system', role: 'operator', tenantId: 'acme' },
      originEntryPath: 'schedule.fire',
    });

    // #when / #then — reading it back as a human would hand a scheduled job
    // the authority of a human operator, so it fails closed instead.
    await expect(
      readAgentRunRecord(storage, 'acme_run-1'),
    ).rejects.toBeInstanceOf(AgentRunStateError);
  });

  it('rejects a version-2 record whose principal is still an ApprovalActor', async () => {
    // #given — the shape change, not just the version number.
    const storage = memoryStorage();
    await storage.put('flowsafe:agent-run:v1:acme_run-2', {
      version: 2,
      agentId: 'writer',
      principal: { id: 'starter', role: 'operator', tenantId: 'acme' },
      originEntryPath: 'http.start',
    });

    // #when / #then
    await expect(
      readAgentRunRecord(storage, 'acme_run-2'),
    ).rejects.toBeInstanceOf(AgentRunStateError);
  });

  it('rejects an automated principal that carries no purpose', async () => {
    // #given — purpose is the provenance the whole model restores.
    const storage = memoryStorage();
    await storage.put('flowsafe:agent-run:v1:acme_run-3', {
      version: 2,
      agentId: 'writer',
      principal: { kind: 'system', id: 'sched', tenantId: 'acme' },
      originEntryPath: 'schedule.fire',
    });

    // #when / #then
    await expect(
      readAgentRunRecord(storage, 'acme_run-3'),
    ).rejects.toBeInstanceOf(AgentRunStateError);
  });
});
