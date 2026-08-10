// SPDX-License-Identifier: Apache-2.0

import {
  assertExecutionPrincipal,
  type ExecutionPrincipal,
  samePrincipal,
} from '../approval-api/principal.js';
import type { DurableKeyValueStorage } from '../do-runner/cf-types.js';
import { isPathSafeId } from '../do-runner/path-safe-id.js';

export const AGENT_THREAD_BINDING_STORAGE_KEY =
  'flowsafe:agent-thread-binding:v1';
export const AGENT_RUN_STORAGE_KEY_PREFIX = 'flowsafe:agent-run:v1:';

export const AGENT_ENTRY_PATHS = [
  'http.start',
  'approval.resume',
  'signal.message',
  'signal.queue',
  'signal.reactive',
  'signal.state',
  'signal.notification',
  'signal.wake',
  'notification.dispatch',
  'schedule.fire',
] as const;

export type AgentEntryPath = (typeof AGENT_ENTRY_PATHS)[number];

export interface AgentThreadBinding {
  version: 1;
  agentId: string;
  resourceId: string;
}

/**
 * Version 2 carries an ExecutionPrincipal where version 1 carried an
 * ApprovalActor. There is deliberately no v1 upgrade path: a run started from
 * `schedule.fire` stored the fabricated `role: 'operator'` that this work
 * exists to remove, so reading a v1 record back as a human principal would
 * launder that authority through a migration. `canonicalRunRecord` rejects v1,
 * so a suspended pre-upgrade agent run cannot resume.
 */
export interface AgentRunRecord {
  version: 2;
  agentId: string;
  principal: ExecutionPrincipal;
  originEntryPath: AgentEntryPath;
}

export class AgentRunStateError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'AgentRunStateError';
  }
}

export class AgentRunStateConflictError extends AgentRunStateError {
  constructor(message: string) {
    super(message);
    this.name = 'AgentRunStateConflictError';
  }
}

function stateField(value: object, key: PropertyKey): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  return descriptor !== undefined && 'value' in descriptor
    ? descriptor.value
    : undefined;
}

function canonicalBinding(value: unknown): AgentThreadBinding | undefined {
  if (value === null || typeof value !== 'object') return undefined;
  const version = stateField(value, 'version');
  const agentId = stateField(value, 'agentId');
  const resourceId = stateField(value, 'resourceId');
  if (version !== 1 || !isPathSafeId(agentId) || !isPathSafeId(resourceId)) {
    return undefined;
  }
  return Object.freeze({ version: 1, agentId, resourceId });
}

function canonicalRunRecord(
  runId: string,
  value: unknown,
): AgentRunRecord | undefined {
  if (value === null || typeof value !== 'object' || !isPathSafeId(runId)) {
    return undefined;
  }
  const version = stateField(value, 'version');
  const agentId = stateField(value, 'agentId');
  const originEntryPath = stateField(value, 'originEntryPath');
  if (
    version !== 2 ||
    !isPathSafeId(agentId) ||
    typeof originEntryPath !== 'string' ||
    !(AGENT_ENTRY_PATHS as readonly string[]).includes(originEntryPath)
  ) {
    return undefined;
  }
  try {
    const principal = assertExecutionPrincipal(
      stateField(value, 'principal'),
      'stored agent run',
    );
    return Object.freeze({
      version: 2,
      agentId,
      principal,
      originEntryPath: originEntryPath as AgentEntryPath,
    });
  } catch {
    return undefined;
  }
}

export function agentRunStorageKey(runId: string): string {
  if (!isPathSafeId(runId)) {
    throw new AgentRunStateError('agent runId must be URL-path-safe');
  }
  return AGENT_RUN_STORAGE_KEY_PREFIX + runId;
}

export async function readAgentThreadBinding(
  storage: DurableKeyValueStorage,
): Promise<AgentThreadBinding | undefined> {
  const stored = await storage.get(AGENT_THREAD_BINDING_STORAGE_KEY);
  if (stored === undefined) return undefined;
  const canonical = canonicalBinding(stored);
  if (!canonical) {
    throw new AgentRunStateError('stored agent thread binding is malformed');
  }
  return canonical;
}

export async function bindAgentThread(
  storage: DurableKeyValueStorage,
  binding: AgentThreadBinding,
): Promise<AgentThreadBinding> {
  const canonical = canonicalBinding(binding);
  if (!canonical) {
    throw new AgentRunStateError('agent thread binding is malformed');
  }
  const current = await readAgentThreadBinding(storage);
  if (current) {
    if (
      current.agentId !== canonical.agentId ||
      current.resourceId !== canonical.resourceId
    ) {
      throw new AgentRunStateConflictError(
        'thread is already bound to a different agent or resource',
      );
    }
    return current;
  }
  const stored = structuredClone(canonical);
  await storage.put(AGENT_THREAD_BINDING_STORAGE_KEY, stored);
  return canonical;
}

export async function deleteAgentThreadBinding(
  storage: DurableKeyValueStorage,
  expected: Pick<AgentThreadBinding, 'agentId' | 'resourceId'>,
): Promise<boolean> {
  if (expected === null || typeof expected !== 'object') return false;
  const agentId = stateField(expected, 'agentId');
  const resourceId = stateField(expected, 'resourceId');
  if (!isPathSafeId(agentId) || !isPathSafeId(resourceId)) return false;
  const current = await readAgentThreadBinding(storage);
  if (
    !current ||
    current.agentId !== agentId ||
    current.resourceId !== resourceId
  ) {
    return false;
  }
  return storage.delete(AGENT_THREAD_BINDING_STORAGE_KEY);
}

export async function readAgentRunRecord(
  storage: DurableKeyValueStorage,
  runId: string,
): Promise<AgentRunRecord | undefined> {
  const stored = await storage.get(agentRunStorageKey(runId));
  if (stored === undefined) return undefined;
  const canonical = canonicalRunRecord(runId, stored);
  if (!canonical) {
    throw new AgentRunStateError(
      `stored metadata for run '${runId}' is malformed`,
    );
  }
  return canonical;
}

export async function writeAgentRunRecord(
  storage: DurableKeyValueStorage,
  runId: string,
  record: AgentRunRecord,
): Promise<AgentRunRecord> {
  const key = agentRunStorageKey(runId);
  const canonical = canonicalRunRecord(runId, record);
  if (!canonical) {
    throw new AgentRunStateError('agent run metadata is malformed');
  }
  const current = await readAgentRunRecord(storage, runId);
  if (current) {
    // Structural comparison across every kind-specific field: comparing only
    // id/role alone would let a run rebind from one automated purpose to
    // another, or from an agent's delegation chain to a different one.
    if (
      current.agentId !== canonical.agentId ||
      current.originEntryPath !== canonical.originEntryPath ||
      !samePrincipal(current.principal, canonical.principal)
    ) {
      throw new AgentRunStateConflictError(
        `run '${runId}' is already bound to a different agent principal`,
      );
    }
    return current;
  }
  const stored = structuredClone(canonical);
  await storage.put(key, stored);
  return canonical;
}

export async function deleteAgentRunRecord(
  storage: DurableKeyValueStorage,
  runId: string,
): Promise<void> {
  await storage.delete(agentRunStorageKey(runId));
}
