// SPDX-License-Identifier: Apache-2.0
// @proofoftech/breakwater — Safety middleware for Mastra
//
// breakwater plugs into Mastra as processors and tool/workflow wrappers:
// 1. Guarded agent — narrow execution with mandatory RBAC and policy ordering
// 2. Policy engine — pre/post gates around the agent's model call
// 3. RBAC + audit — 5 roles, actor authorization, structured audit log
// 4. Connector SDK — enforced permission manifests: side-effect
//    classification, network egress, write-approval gates, idempotent replay
// 5. Agent CLI adapters — Claude Code / Codex as approval-gated connectors
//    (Node-only at execution time)
//
// These are Mastra Processor implementations plus tool/workflow wrappers, not a custom runtime.

export type {
  GuardedAgentCallOptions,
  GuardedAgentConfig,
  GuardedAgentHandle,
  GuardedInputProcessor,
  GuardedOutputProcessor,
  GuardedToolChoice,
} from './agent/index.js';
export {
  createGuardedAgent,
  isGuardedAgentHandle,
} from './agent/index.js';
export type {
  AgentCliConnectorOptions,
  AgentCliDefinition,
  AgentCliErrorCode,
  AgentCliErrorMetadata,
  AgentCliExec,
  AgentCliExecResult,
  AgentCliInput,
  AgentCliOutput,
} from './agent-cli/index.js';
export {
  AgentCliError,
  CLAUDE_CODE_CLI,
  CODEX_CLI,
  createAgentCliConnector,
  createClaudeCodeConnector,
  createCodexConnector,
} from './agent-cli/index.js';
export type {
  AgentAuditContext,
  AuditEvent,
  AuditLoggerOptions,
  AuditSink,
  MetricsRecorder,
} from './audit/index.js';
export {
  AGENT_AUDIT_CONTEXT_KEY,
  AuditLogger,
  combineAuditSinks,
  metricsAuditSink,
} from './audit/index.js';
export type {
  AtomicIdempotencyStore,
  ConnectorApprovalGrant,
  ConnectorApprovalGrantBase,
  ConnectorApprovalSuspension,
  ConnectorConfig,
  ConnectorExecutionIdentity,
  ConnectorPolicies,
  ConnectorRuntime,
  D1IdempotencyStoreOptions,
  D1RateLimitStoreOptions,
  EgressDenial,
  EgressFetchBase,
  EgressFetchOptions,
  EgressGuardedFetch,
  EgressRequestInit,
  EgressResponse,
  EgressResponseHeaders,
  IdempotencyDatabase,
  IdempotencyRecord,
  IdempotencyReservation,
  IdempotencyStatement,
  IdempotencyStore,
  PermissionManifest,
  RateLimitDatabase,
  RateLimitStatement,
  RateLimitStore,
  SingleTenantAuditPosture,
  SingleTenantConnectorPolicies,
  SingleTenantConnectorPoliciesOptions,
  SingleTenantDurableStores,
  SingleTenantPermissionPosture,
} from './connector-sdk/index.js';
export {
  CONNECTOR_EXECUTION_CONTEXT_KEY,
  CONNECTOR_GRANTS_CONTEXT_KEY,
  ConnectorPolicyError,
  connectorManifest,
  createConnector,
  D1IdempotencyStore,
  D1RateLimitStore,
  DRY_RUN_CONTEXT_KEY,
  EgressDeniedError,
  egressFetch,
  IDEMPOTENCY_KEY_CONTEXT_KEY,
  InMemoryIdempotencyStore,
  InMemoryRateLimitStore,
  singleTenantConnectorPolicies,
} from './connector-sdk/index.js';
export type {
  BackgroundExecutionOptions,
  ClassifierPolicyOptions,
  CrossWorkflowIsolationOptions,
  NetworkEgressOptions,
  OutputChannel,
  PiiSecretsDetectorId,
  PiiSecretsOptions,
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
export {
  approvalRequired,
  backgroundExecution,
  classifierPolicy,
  crossWorkflowIsolation,
  denyPatterns,
  egressDomainAllowed,
  extractMessageText,
  ISOLATION_SCOPE_CONTEXT_KEY,
  LLM_BACKGROUND_OVERRIDE_KEY,
  maxTextLength,
  networkEgress,
  PII_SECRETS_DETECTOR_IDS,
  PolicyEngine,
  piiSecrets,
  tenantIsolation,
  WORKFLOW_SCOPE_CONTEXT_KEY,
} from './policy-engine/index.js';
export type {
  Actor,
  Permission,
  PrincipalKind,
  PrincipalPermissions,
  RBACMiddlewareOptions,
  Role,
} from './rbac/index.js';
export {
  ACTOR_CONTEXT_KEY,
  actorFromRequestContext,
  DEFAULT_ALLOWED_PRINCIPAL_KINDS,
  isPermissionIdentifier,
  isPrincipalPermissions,
  PRINCIPAL_KINDS,
  PRINCIPAL_PERMISSIONS_CONTEXT_KEY,
  principalKindOf,
  RBACMiddleware,
  ROLES,
} from './rbac/index.js';
