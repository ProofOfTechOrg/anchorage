// SPDX-License-Identifier: Apache-2.0
// Server-only guarded-agent catalog and Durable Object host. This subpath
// intentionally stays out of the Flowsafe root and host-kit barrels.

export {
  assertNoReservedExecutionContext,
  findReservedExecutionContextKey,
  isReservedExecutionContextKey,
  RESERVED_EXECUTION_CONTEXT_KEYS,
  ReservedExecutionContextError,
  stripReservedExecutionContext,
} from '../do-runner/index.js';
export {
  type AgentApprovalResumer,
  type AgentApprovalResumerOptions,
  createAgentApprovalResumer,
} from './approval-resumer.js';
export {
  createAgentCatalog,
  createAgentModuleCatalog,
  validateAgentMeta,
  validateAgentModule,
} from './catalog.js';
export {
  type AgentRouter,
  type AgentRouterOptions,
  createAgentRouter,
} from './router.js';
export {
  type AgentThreadInstanceScope,
  type AgentThreadStateStorage,
  type AutomatedEntryAuthorizer,
  type AutomatedEntryRequest,
  type BoundThreadAgent,
  createThreadAgentHost,
  type ThreadAgentHost,
  type ThreadAgentHostOptions,
  type ThreadAgentStartInput,
} from './thread-host.js';
export {
  AGENT_HOST_ROUTE_PREFIX,
  type AgentThreadObserveInput,
  type AgentThreadRunRef,
  type AgentThreadStartInput,
  type AgentThreadTopology,
  createAgentThreadTopology,
} from './thread-topology.js';
export {
  AGENT_AUDIT_CONTEXT_KEY,
  createTrustedAgentRequestContext,
  deriveTrustedAgentContext,
  sanitizeStoredAgentContext,
} from './trusted-context.js';
export type {
  AgentAutomationRule,
  AgentCatalog,
  AgentEntryPath,
  AgentMeta,
  AgentModule,
  AgentModuleCatalog,
  AgentRunEnvelope,
  AutomationCheck,
  TrustedAgentExecution,
} from './types.js';
