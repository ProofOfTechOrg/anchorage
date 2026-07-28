// SPDX-License-Identifier: Apache-2.0

import type { ApprovalActor, ApprovalRole } from '../approval-api/contract.js';
import {
  PATH_SAFE_ID_PATTERN,
  tenantOwnsSaltedId,
} from '../do-runner/path-safe-id.js';
import type { ResumeLedgerStorage } from '../do-runner/resume-ledger.js';

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

export interface AgentRunRecord {
  version: 1;
  agentId: string;
  principal: ApprovalActor;
  originEntryPath: AgentEntryPath;
}

const APPROVAL_ROLES: readonly ApprovalRole[] = [
  'admin',
  'builder',
  'operator',
  'reviewer',
  'viewer',
];

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

function validBinding(value: unknown): value is AgentThreadBinding {
  if (value === null || typeof value !== 'object') return false;
  const candidate = value as Partial<AgentThreadBinding>;
  return (
    candidate.version === 1 &&
    typeof candidate.agentId === 'string' &&
    PATH_SAFE_ID_PATTERN.test(candidate.agentId) &&
    typeof candidate.resourceId === 'string' &&
    PATH_SAFE_ID_PATTERN.test(candidate.resourceId)
  );
}

function validRunRecord(
  runId: string,
  value: unknown,
): value is AgentRunRecord {
  if (value === null || typeof value !== 'object') return false;
  const candidate = value as Partial<AgentRunRecord>;
  const principal = candidate.principal;
  return (
    candidate.version === 1 &&
    typeof candidate.agentId === 'string' &&
    PATH_SAFE_ID_PATTERN.test(candidate.agentId) &&
    principal !== null &&
    typeof principal === 'object' &&
    typeof principal.id === 'string' &&
    principal.id.trim() !== '' &&
    typeof principal.role === 'string' &&
    (APPROVAL_ROLES as readonly string[]).includes(principal.role) &&
    typeof principal.tenantId === 'string' &&
    tenantOwnsSaltedId(principal.tenantId, runId) &&
    typeof candidate.originEntryPath === 'string' &&
    (AGENT_ENTRY_PATHS as readonly string[]).includes(candidate.originEntryPath)
  );
}

export function agentRunStorageKey(runId: string): string {
  if (typeof runId !== 'string' || !PATH_SAFE_ID_PATTERN.test(runId)) {
    throw new AgentRunStateError('agent runId must be URL-path-safe');
  }
  return AGENT_RUN_STORAGE_KEY_PREFIX + runId;
}

export async function readAgentThreadBinding(
  storage: ResumeLedgerStorage,
): Promise<AgentThreadBinding | undefined> {
  const stored = await storage.get(AGENT_THREAD_BINDING_STORAGE_KEY);
  if (stored === undefined) return undefined;
  if (!validBinding(stored)) {
    throw new AgentRunStateError('stored agent thread binding is malformed');
  }
  return stored;
}

export async function bindAgentThread(
  storage: ResumeLedgerStorage,
  binding: AgentThreadBinding,
): Promise<AgentThreadBinding> {
  if (!validBinding(binding)) {
    throw new AgentRunStateError('agent thread binding is malformed');
  }
  const current = await readAgentThreadBinding(storage);
  if (current) {
    if (
      current.agentId !== binding.agentId ||
      current.resourceId !== binding.resourceId
    ) {
      throw new AgentRunStateConflictError(
        'thread is already bound to a different agent or resource',
      );
    }
    return current;
  }
  const stored = structuredClone(binding);
  await storage.put(AGENT_THREAD_BINDING_STORAGE_KEY, stored);
  return stored;
}

export async function readAgentRunRecord(
  storage: ResumeLedgerStorage,
  runId: string,
): Promise<AgentRunRecord | undefined> {
  const stored = await storage.get(agentRunStorageKey(runId));
  if (stored === undefined) return undefined;
  if (!validRunRecord(runId, stored)) {
    throw new AgentRunStateError(
      `stored metadata for run '${runId}' is malformed`,
    );
  }
  return stored;
}

export async function writeAgentRunRecord(
  storage: ResumeLedgerStorage,
  runId: string,
  record: AgentRunRecord,
): Promise<AgentRunRecord> {
  const key = agentRunStorageKey(runId);
  if (!validRunRecord(runId, record)) {
    throw new AgentRunStateError('agent run metadata is malformed');
  }
  const current = await readAgentRunRecord(storage, runId);
  if (current) {
    if (
      current.agentId !== record.agentId ||
      current.originEntryPath !== record.originEntryPath ||
      current.principal.id !== record.principal.id ||
      current.principal.role !== record.principal.role ||
      current.principal.tenantId !== record.principal.tenantId
    ) {
      throw new AgentRunStateConflictError(
        `run '${runId}' is already bound to a different agent principal`,
      );
    }
    return current;
  }
  const stored = structuredClone(record);
  await storage.put(key, stored);
  return stored;
}

export async function deleteAgentRunRecord(
  storage: ResumeLedgerStorage,
  runId: string,
): Promise<void> {
  await storage.delete(agentRunStorageKey(runId));
}
