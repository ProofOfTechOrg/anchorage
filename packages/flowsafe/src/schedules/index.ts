// SPDX-License-Identifier: Apache-2.0
// Track D (M-006) — schedules: the D1 domain, the CAS tick, and the tenant
// facade. Subpath-only (`@proofoftech/flowsafe/schedules`), like agent-runner /
// background-tasks / signals / goals: host-side wiring a consumer opts into, not
// in the root barrel.

// The tenant facade router (CI-M-006-003).
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
  type Schedule,
  type ScheduleDatabase,
  type ScheduleStatement,
  type ScheduleTrigger,
} from './schedules-d1.js';
export { createScheduleStorageDomains } from './storage.js';
// The CAS tick (CI-M-006-002).
export {
  buildScheduledLegContext,
  createScheduleTick,
  isReservedScheduleContextKey,
  RESERVED_SCHEDULE_CONTEXT_KEYS,
  type ScheduleFireOutcome,
  type ScheduleTickAuditEvent,
  type ScheduleTickAuditSink,
  type ScheduleTickOptions,
  type ScheduleTickResult,
  type ScheduleTickRunCap,
  type ScheduleTickStart,
  type ScheduleTickStore,
  stripReservedScheduleContext,
} from './tick.js';
