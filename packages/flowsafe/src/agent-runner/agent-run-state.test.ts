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
      version: 1 as const,
      agentId: 'writer',
      principal: {
        id: 'starter',
        role: 'operator' as const,
        tenantId: 'acme',
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
      version: 1,
      agentId: '../writer',
      resourceId: 'acme_resource',
    });
    await expect(readAgentThreadBinding(storage)).rejects.toBeInstanceOf(
      AgentRunStateError,
    );
    await expect(
      writeAgentRunRecord(storage, 'acme_run-1', {
        version: 1,
        agentId: 'writer',
        principal: {
          id: 'starter',
          role: 'operator',
          tenantId: 'globex',
        },
        originEntryPath: 'http.start',
      }),
    ).rejects.toBeInstanceOf(AgentRunStateError);
    await expect(
      writeAgentRunRecord(storage, 'acme_run-1', {
        version: 1,
        agentId: 'writer',
        principal: {
          id: '   ',
          role: 'operator',
          tenantId: 'acme',
        },
        originEntryPath: 'http.start',
      }),
    ).rejects.toBeInstanceOf(AgentRunStateError);
  });
});
