// SPDX-License-Identifier: Apache-2.0
// Cloudflare Durable Objects workflow runner — the Cloudflare-native
// counterpart of @mastra/inngest / @mastra/temporal. init() returns DO-bound
// createWorkflow/createStep (import-swap); DurableObjectRunner hosts
// execution; D1 holds run snapshots so suspend/resume survives restarts.

export type {
  D1DatabaseBinding,
  DurableObjectRunnerState,
} from './cf-types.js';
export type {
  D1StorageOptions,
  PurgeExpiredRunsOptions,
  PurgeTenantOptions,
  PurgeTenantResult,
  SnapshotDatabase,
  SnapshotStatement,
  TenantArtifactPurger,
} from './d1-storage.js';
export {
  createD1Storage,
  d1Changes,
  ensureSnapshotRunIdIndex,
  purgeExpiredWorkflowRuns,
  purgeTenant,
} from './d1-storage.js';
export { DurableObjectRunner } from './durable-object.js';
export type {
  DORunnerEnv,
  InitOptions,
  InitResult,
  InitSource,
} from './init.js';
export { init } from './init.js';
// Agent-memory tenancy chokepoint (docs/agent-memory-tenancy.md): the only
// constructors/decoders for salted threadId/resourceId values.
export {
  mintResourceId,
  mintThreadId,
  tenantOfMemoryId,
  tenantOwnsMemoryId,
} from './memory-id.js';
export {
  PATH_SAFE_ID_PATTERN,
  TENANT_ID_PATTERN,
  tenantOfRunId,
} from './path-safe-id.js';
export type { ResumeLedger, ResumeLedgerStorage } from './resume-ledger.js';
export {
  DurableStorageResumeLedger,
  InMemoryResumeLedger,
} from './resume-ledger.js';
export type {
  RequestContextProvider,
  ResumeRunOptions,
  RunLeg,
  RunnerRuntimeOptions,
  RunSummary,
  StartRunOptions,
} from './runtime.js';
export {
  InvalidRunRequestError,
  RunAlreadyExistsError,
  RunNotSuspendedError,
  RunnerRuntime,
  UnknownRunError,
  UnknownWorkflowError,
} from './runtime.js';
