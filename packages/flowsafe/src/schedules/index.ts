// SPDX-License-Identifier: Apache-2.0
// Track D (M-006) — schedules: the D1 domain, the CAS tick, and the authenticated
// facade. Subpath-only (`@proofoftech/flowsafe/schedules`), like agent-runner /
// background-tasks / signals / goals: host-side wiring a consumer opts into, not
// in the root barrel.

// The facade router (CI-M-006-003).
export {
  createScheduleRouter,
  type ScheduleFacadeStore,
  type ScheduleOperation,
  type ScheduleRouteAuditEvent,
  type ScheduleRouteAuditSink,
  type ScheduleRouter,
  type ScheduleRouterOptions,
} from './router.js';
// The D1 schedules storage domain (CI-M-006-001).
export {
  D1SchedulesStorage,
  parseScheduleAgentDispatchReceipt,
  type Schedule,
  type ScheduleAgentDispatchAction,
  type ScheduleAgentDispatchReceipt,
  type ScheduleAgentDispatchState,
  type ScheduleDatabase,
  type ScheduleFireClaim,
  type ScheduleStatement,
  type ScheduleTrigger,
} from './schedules-d1.js';
export { createScheduleStorageDomains } from './storage.js';
export {
  type AuthorizedSchedule,
  createScheduleTargetPolicy,
  type ScheduleTargetCatalogEntry,
  type ScheduleTargetDecision,
  type ScheduleTargetPolicy,
  type ScheduleTargetPolicyOptions,
  scheduleCreatorRole,
  scheduleWithCreatorRole,
} from './target-policy.js';
// The CAS tick (CI-M-006-002).
export {
  type AgentScheduleTarget,
  buildScheduledLegContext,
  canPersistScheduledAgentSignal,
  createScheduleStartSource,
  createScheduleTick,
  isReservedScheduleContextKey,
  RESERVED_SCHEDULE_CONTEXT_KEYS,
  type ScheduleFireOutcome,
  type ScheduleStartSource,
  type ScheduleStartSourceStore,
  type ScheduleStartSourceTarget,
  type ScheduleTickAuditEvent,
  type ScheduleTickAuditSink,
  type ScheduleTickDispatchRef,
  type ScheduleTickOptions,
  type ScheduleTickResult,
  type ScheduleTickRunCap,
  type ScheduleTickSignalAgent,
  type ScheduleTickSignalAgentInput,
  type ScheduleTickStart,
  type ScheduleTickStartAgent,
  type ScheduleTickStartAgentInput,
  type ScheduleTickStartInput,
  type ScheduleTickStatus,
  type ScheduleTickStatusResult,
  type ScheduleTickStore,
  stripReservedScheduleContext,
  type WorkflowScheduleTarget,
} from './tick.js';
