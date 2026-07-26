// SPDX-License-Identifier: Apache-2.0

import type { GuardedAgentHandle } from '@proofoftech/breakwater/agent';
import type { AgentEntryPath } from '../agent-runner/index.js';
import type {
  ApprovalActor,
  ApprovalRecord,
  ApprovalRole,
} from '../approval-api/index.js';
import type { RunSummary } from '../do-runner/index.js';

export type { AgentEntryPath } from '../agent-runner/index.js';

export interface AgentMeta {
  id: string;
  title: string;
  description: string;
  allowedRoles?: readonly ApprovalRole[];
}

export interface AgentModule {
  meta: AgentMeta;
  agent: GuardedAgentHandle;
}

export interface AgentCatalog {
  readonly agents: readonly AgentMeta[];
  get(agentId: string): AgentMeta | undefined;
  allowedRoles(agentId: string): readonly ApprovalRole[] | undefined;
}

export interface AgentModuleCatalog {
  readonly agents: readonly AgentMeta[];
  readonly modules: readonly AgentModule[];
  get(agentId: string): AgentModule | undefined;
  allowedRoles(agentId: string): readonly ApprovalRole[] | undefined;
}

export interface AgentRunEnvelope {
  agentId: string;
  threadId: string;
  resourceId: string;
  runId: string;
  summary: RunSummary;
  approval?: ApprovalRecord;
  approvals?: ApprovalRecord[];
}

export interface TrustedAgentExecution {
  agentId: string;
  actor: ApprovalActor;
  threadId: string;
  resourceId: string;
  runId: string;
  entryPath: AgentEntryPath;
  /**
   * Non-reserved context accepted only from trusted internal entry paths.
   * Public HTTP agent starts never populate this field.
   */
  safeContext?: Readonly<Record<string, unknown>>;
}
