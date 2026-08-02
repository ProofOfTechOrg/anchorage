// SPDX-License-Identifier: Apache-2.0
// @proofoftech/flowsafe/agent-runner — drive Mastra durable agents through the
// RunnerRuntime chokepoint (Track A, DL-001/DL-010).
//
// Subpath-only (like host-kit): it imports the durable Agent, which drags
// @mastra's Node built-ins, so it stays out of the root barrel and off the
// browser's import graph.

export type {
  AgentEntryPath,
  AgentRunRecord,
  AgentThreadBinding,
} from './agent-run-state.js';
export {
  AGENT_ENTRY_PATHS,
  AGENT_RUN_STORAGE_KEY_PREFIX,
  AGENT_THREAD_BINDING_STORAGE_KEY,
  AgentRunStateConflictError,
  AgentRunStateError,
  agentRunStorageKey,
  bindAgentThread,
  deleteAgentRunRecord,
  readAgentRunRecord,
  readAgentThreadBinding,
  writeAgentRunRecord,
} from './agent-run-state.js';
export {
  AGENT_APPROVAL_SUSPEND_TYPE,
  type AgentApprovalSuspend,
  type AgentGateGrantRequest,
  agentGateConnectors,
  agentGateGrantRequest,
  parseAgentApprovalSuspend,
} from './approval-shapes.js';
export {
  createFlowsafeDurableAgent,
  DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
  FlowsafeDurableAgent,
  type FlowsafeDurableAgentOptions,
  isRuntimeDrivenAgent,
  RUNTIME_DRIVEN_AGENT,
} from './durable-agent-runner.js';
