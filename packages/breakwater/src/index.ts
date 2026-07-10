// @proofoftech/breakwater — Safety middleware for Mastra
//
// breakwater plugs into Mastra as processors and tool/workflow wrappers:
// 1. Policy engine — pre/post gates around the agent's model call
// 2. RBAC + audit — 5 roles, actor authorization, structured audit log
// 3. Connector SDK — enforced permission manifests: side-effect
//    classification, network egress, write-approval gates, idempotent replay
// 4. Agent CLI adapters — Claude Code / Codex as approval-gated connectors
//    (Node-only at execution time)
//
// These are Mastra Processor implementations plus tool/workflow wrappers, not a custom runtime.

export {
  approvalRequired,
  crossWorkflowIsolation,
  denyPatterns,
  extractMessageText,
  ISOLATION_SCOPE_CONTEXT_KEY,
  maxTextLength,
  networkEgress,
  PolicyEngine,
  tenantIsolation,
  WORKFLOW_SCOPE_CONTEXT_KEY,
} from './policy-engine/index.js';
export type {
  CrossWorkflowIsolationOptions,
  NetworkEgressOptions,
  OutputChannel,
  PolicyContext,
  PolicyDecision,
  PolicyEngineOptions,
  PolicyEvaluator,
  PolicyPhase,
  SideEffect,
  ToolCallContext,
  ToolPolicyEvaluator,
  WritePermissionsPolicy,
} from './policy-engine/index.js';
export { AuditLogger } from './audit/index.js';
export type {
  AuditEvent,
  AuditLoggerOptions,
  AuditSink,
} from './audit/index.js';
export {
  ACTOR_CONTEXT_KEY,
  actorFromRequestContext,
  RBACMiddleware,
  ROLES,
} from './rbac/index.js';
export type {
  Actor,
  RBACMiddlewareOptions,
  Role,
} from './rbac/index.js';
export {
  APPROVED_CONNECTORS_CONTEXT_KEY,
  connectorManifest,
  ConnectorPolicyError,
  createConnector,
  D1IdempotencyStore,
  D1RateLimitStore,
  DRY_RUN_CONTEXT_KEY,
  IDEMPOTENCY_KEY_CONTEXT_KEY,
  InMemoryIdempotencyStore,
  InMemoryRateLimitStore,
} from './connector-sdk/index.js';
export type {
  AtomicIdempotencyStore,
  ConnectorConfig,
  ConnectorPolicies,
  D1IdempotencyStoreOptions,
  D1RateLimitStoreOptions,
  IdempotencyDatabase,
  IdempotencyRecord,
  IdempotencyReservation,
  IdempotencyStatement,
  IdempotencyStore,
  PermissionManifest,
  RateLimitDatabase,
  RateLimitStatement,
  RateLimitStore,
} from './connector-sdk/index.js';
export {
  AgentCliError,
  CLAUDE_CODE_CLI,
  CODEX_CLI,
  createAgentCliConnector,
  createClaudeCodeConnector,
  createCodexConnector,
} from './agent-cli/index.js';
export type {
  AgentCliConnectorOptions,
  AgentCliDefinition,
  AgentCliExec,
  AgentCliExecResult,
  AgentCliInput,
  AgentCliOutput,
} from './agent-cli/index.js';
