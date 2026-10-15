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
export type {
  RecordedRemoteStepResult,
  RemoteStepInvocation,
  RemoteStepNotificationOperation,
  RemoteStepPermit,
  WorkflowRemoteStepAddress,
  WorkflowRemoteStepInvocation,
} from '../do-runner/remote-step-journal.js';
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
  type BlockingAgentRun,
  type BoundThreadAgent,
  createThreadAgentHost,
  type PrincipalPermissionResolution,
  type PrincipalPermissionResolver,
  type ThreadAgentHost,
  type ThreadAgentHostOptions,
  type ThreadAgentStartInput,
} from './thread-host.js';
export {
  AGENT_HOST_ROUTE_PREFIX,
  type AgentThreadBoundTopology,
  type AgentThreadDispatchTopology,
  type AgentThreadObserveInput,
  type AgentThreadRunRef,
  type AgentThreadStartInput,
  type AgentThreadTopology,
  type AgentThreadTopologyOptions,
  createAgentThreadTopology,
  createRemoteStepNotificationRecoveryTick,
  type RemoteStepContinuationReader,
  type RemoteStepDispatchReader,
  type RemoteStepDispatchTicket,
  type WorkflowRemoteStepDispatchReader,
  type WorkflowRemoteStepDispatchTicket,
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
  Permission,
  TrustedAgentExecution,
} from './types.js';
export { isPermissionIdentifier } from './types.js';
export type { WorkflowDispatchPort } from './workflow-remote-step-route.js';
export {
  createWorkflowRemoteStepPreRoute,
  WORKFLOW_REMOTE_STEP_ROUTE,
} from './workflow-remote-step-route.js';
