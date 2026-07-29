// SPDX-License-Identifier: Apache-2.0
// The host-side ApprovalService assembly and cron SLA sweep that the
// showcase Worker and the deploy template previously carried as byte-copies:
// the structured-log + optional-Queues audit sink, the system actor derived
// from the store's own tenant binding, and the SoD-guarded multi-gate
// re-queue over the host's injected resume topology. The only genuine host
// difference — HOW a run resumes — stays injected as `resumeRun`
// (createDoRunTopology(...).resumeRecord for DO hosts, resumeViaRuntime for
// in-process ones). Also home to the isolate-scoped D1ApprovalStoreFactory
// memo, which every host needs for the same reason (the DDL promise must
// span the isolate, not one request).

import type {
  ApprovalAuditEvent,
  ApprovalAuditSink,
  ApprovalDatabase,
  ApprovalNotificationSink,
  ApprovalStreamSink,
  SelfDecisionPolicy,
  SystemApprovalStore,
  TenantBoundApprovalStore,
} from '../approval-api/index.js';
import {
  ApprovalService,
  type AutomatedExecutionPrincipal,
  D1ApprovalStoreFactory,
  purgeExpiredApprovals,
  sweepSLA,
  type TrustedAutomationPrincipal,
  trustAutomationPrincipal,
} from '../approval-api/index.js';
import { type AuditQueue, queueAuditSink } from '../audit-export/index.js';
import {
  type ResumeRunFn,
  reconcileApprovalsOnStatus,
  resumeRunWithRequeue,
} from './approval-bridge.js';
import { numberVar } from './env-vars.js';

/**
 * Attribution identity for cron maintenance — audit only. The sweep is TCB
 * code over the system store, so 'system' here is the reserved audit
 * identity (RESERVED_TENANT_IDS), which no verifier admits and no store
 * binds to; per-record tenants ride in the audit detail.
 */
export function maintenancePrincipal(
  systemActorId: string,
): TrustedAutomationPrincipal {
  return trustAutomationPrincipal({
    kind: 'system',
    id: systemActorId,
    tenantId: 'system',
    purpose: 'approval-sla-maintenance',
  });
}

// One factory per isolate, not per request: it owns the memoized schema-init
// promise, so rebuilding it inside fetch() would re-run the whole DDL pass
// (CREATE TABLE + DROP/CREATE indexes + PRAGMA + ALTERs) on every request.
// Keyed by the D1 binding, which is stable for an isolate's lifetime.
const approvalFactories = new WeakMap<
  ApprovalDatabase,
  D1ApprovalStoreFactory
>();

export function approvalStoreFactoryFor(
  db: ApprovalDatabase,
): D1ApprovalStoreFactory {
  let factory = approvalFactories.get(db);
  if (!factory) {
    factory = new D1ApprovalStoreFactory(db);
    approvalFactories.set(db, factory);
  }
  return factory;
}

export interface HostAuditSinkOptions {
  /** Optional Cloudflare queue producer for SIEM export. */
  queue?: AuditQueue<ApprovalAuditEvent>;
  /**
   * Keeps each queue send alive past the handler — ctx.waitUntil in fetch
   * scope; a cron runner collects and awaits instead (it already executes
   * under the handler's waitUntil).
   */
  keepAlive?: (send: Promise<unknown>) => void;
}

/**
 * The audit sink every host wires: structured Workers Logs always, plus the
 * SIEM queue when bound. Never throws and never blocks the approval path —
 * a failed send is logged, not propagated.
 */
export function hostAuditSink(
  options: HostAuditSinkOptions = {},
): ApprovalAuditSink {
  const queueSink = options.queue ? queueAuditSink(options.queue) : undefined;
  return (event) => {
    console.log(JSON.stringify({ type: 'audit', ...event }));
    if (queueSink) {
      const send = queueSink(event).catch((error: unknown) =>
        console.error(
          JSON.stringify({
            type: 'audit-queue-error',
            reason: String(error),
          }),
        ),
      );
      options.keepAlive?.(send);
    }
  };
}

export interface HostApprovalServiceOptions {
  /**
   * Id for system-created records (the bridge's record creator). Must differ
   * from human actor ids or the separation-of-duties check can never fire.
   */
  systemActorId: string;
  /** Applied when CreateApprovalInput.slaSeconds is absent. */
  defaultSlaSeconds?: number;
  /**
   * The host's resume topology — createDoRunTopology(...).resumeRecord for a
   * DO host, resumeViaRuntime(runtime) for an in-process one. Wrapped in
   * resumeRunWithRequeue here, so a run that re-suspends at a later gate
   * auto-queues its next approval(s) with SoD intact (the deciding reviewer
   * becomes the next gate's requester).
   */
  resumeRun: ResumeRunFn;
  /** Optional audit export queue (wrangler `queues` producer binding). */
  queue?: AuditQueue<ApprovalAuditEvent>;
  /** ctx.waitUntil — keeps audit queue sends alive past the response. */
  waitUntil?: (send: Promise<unknown>) => void;
  /**
   * Notification transport (email/Slack/pager adapter) for newly-created
   * approval requests — threaded to ApprovalServiceOptions.notify. Transports
   * needing to outlive the response wrap themselves in the host's waitUntil.
   */
  notify?: ApprovalNotificationSink;
  /**
   * Separation-of-duties exemption, forwarded to
   * ApprovalServiceOptions.allowSelfDecision (ENFORCEMENT). Default OFF (SoD
   * on): the requester can never decide their own request. `{ roles: ['admin']
   * }` lets a single-operator deployment self-approve as admin. Pass the
   * IDENTICAL value to createTenantResolver's `allowSelfDecision` so the
   * tenant's `canSelfDecide` display hint matches what this actually enforces.
   */
  allowSelfDecision?: SelfDecisionPolicy;
  /**
   * Live-stream fan-out sink (ApprovalServiceOptions.stream) — fired once per
   * successful approval mutation for the tenant's hub Durable Object. The host
   * supplies a sink that forwards each event to env.HUB.idFromName(record.tenantId)
   * (`createHubTopology`), wrapping the transport keepalive in `ctx.waitUntil`
   * at fetch scope. Undefined means no live fan-out (a poll-only host).
   */
  stream?: ApprovalStreamSink;
}

/**
 * Worker-level approval service sharing the DO's D1 database. Decisions
 * resume the run through the injected topology (grants come from the store
 * via the DO-side provider, never from this request); if the resumed run
 * suspends again at a later gate, the next approval is queued right here, so
 * multi-gate workflows keep flowing through the queue.
 *
 * One audit sink instance backs both the service's own trail and the
 * bridge's re-queue-failure signal: a re-queue that fails after a durable
 * resume is reported through it rather than silently absorbed.
 */
export function buildHostApprovalService(
  store: TenantBoundApprovalStore,
  options: HostApprovalServiceOptions,
): ApprovalService {
  const audit = hostAuditSink({
    queue: options.queue,
    keepAlive: options.waitUntil,
  });
  const service: ApprovalService = new ApprovalService({
    store,
    defaultSlaSeconds: options.defaultSlaSeconds,
    audit,
    notify: options.notify,
    stream: options.stream,
    allowSelfDecision: options.allowSelfDecision,
    resumeRun: resumeRunWithRequeue(
      options.resumeRun,
      () => service,
      options.systemActorId,
      audit,
    ),
  });
  return service;
}

export interface SlaSweepMaintenanceOptions {
  /** factory.system() — the cron-only cross-tenant view. */
  store: SystemApprovalStore;
  /**
   * `maintenancePrincipal(systemActorId)`. Typed as merely automated, not
   * vouched: the sweep derives no authority from the principal (the
   * `SystemApprovalStore` type is its authorization), so demanding the trust
   * brand here would ask for a token nothing on this path reads.
   */
  systemPrincipal: AutomatedExecutionPrincipal;
  /** Optional audit export queue. */
  queue?: AuditQueue<ApprovalAuditEvent>;
  /** The firing cron expression — log correlation only. */
  cron: string;
  /**
   * Notification transport for SLA escalations — threaded to
   * SweepSLAOptions.notify (the cron runner already executes under the
   * handler's waitUntil, so transports need no extra keep-alive here).
   */
  notify?: ApprovalNotificationSink;
  /**
   * Live-stream fan-out sink for SLA escalations — the BARE hub-publish thunk
   * `(event) => createHubTopology(env.HUB).publish(event)`, NOT wrapped in a
   * request-scoped waitUntil. A scheduled() handler runs under its OWN
   * waitUntil where a nested one is unavailable, so runSlaSweepMaintenance
   * COLLECTS each publish promise into its pendingSends and awaits it via the
   * terminal Promise.all — never fire-and-forget (which would be cancelled when
   * ctx.waitUntil(sweep()) settles) and never a nested waitUntil (which would
   * throw cross-request I/O). Undefined means no live escalation fan-out.
   */
  stream?: ApprovalStreamSink;
}

/**
 * The cron-owned SLA sweep, verbatim-shared by the hosts: sweepSLA over the
 * SYSTEM store (the only legitimate cross-tenant read+write, unreachable
 * over HTTP), audit to logs + optional queue, a structured line per
 * escalation, and containment — a sweep failure logs a maintenance-error and
 * resolves rather than throwing, so a caller running other duties is never
 * starved by this one (the two-cron split covers the uncatchable CPU-limit
 * case). Queue sends are collected and awaited HERE: the whole runner
 * already executes under the handler's ctx.waitUntil, where a nested
 * waitUntil is unavailable.
 */
export async function runSlaSweepMaintenance(
  options: SlaSweepMaintenanceOptions,
): Promise<void> {
  let escalated: number | undefined;
  const pendingSends: Promise<unknown>[] = [];
  try {
    escalated = (
      await sweepSLA(options.store, {
        systemPrincipal: options.systemPrincipal,
        audit: hostAuditSink({
          queue: options.queue,
          keepAlive: (send) => pendingSends.push(send),
        }),
        notify: options.notify,
        // Mirror the audit sink's keepAlive idiom: COLLECT each escalation's
        // publish promise into pendingSends so the terminal Promise.all keeps it
        // alive under the scheduled handler's own waitUntil (DL-020). The inner
        // .catch keeps a failed fan-out from rejecting the whole Promise.all.
        stream: (event) => {
          pendingSends.push(
            Promise.resolve(options.stream?.(event)).catch((error: unknown) =>
              // Log a wedged cron fan-out (matching the fetch path's
              // stream-publish-error) instead of swallowing it silently, while
              // still containing it so it can't reject the whole Promise.all.
              console.error(
                JSON.stringify({
                  type: 'stream-publish-error',
                  reason:
                    error instanceof Error ? error.message : String(error),
                }),
              ),
            ),
          );
        },
        onEscalation: (record) =>
          console.log(
            JSON.stringify({
              type: 'sla-escalation',
              id: record.id,
              tenantId: record.tenantId,
              workflowId: record.workflowId,
              runId: record.runId,
              slaDeadlineAt: record.slaDeadlineAt,
            }),
          ),
      })
    ).length;
  } catch (error) {
    console.error(
      JSON.stringify({
        type: 'maintenance-error',
        surface: 'sla-sweep',
        cron: options.cron,
        error: String(error),
      }),
    );
  }
  await Promise.all(pendingSends);
  console.log(
    JSON.stringify({ type: 'maintenance', cron: options.cron, escalated }),
  );
}

/**
 * waitUntil-detached wrapper over reconcileApprovalsOnStatus for
 * ExecutionContext-capable hosts. The heal only matters to a later poll,
 * never the response being built, so its D1 reads and writes (the supersede
 * CAS plus the fresh create) leave the status-response path. A rejection is
 * logged as `reconcile-error`, never left unhandled inside waitUntil. Hosts
 * without an ExecutionContext keep passing reconcileApprovalsOnStatus(...)
 * directly (awaited in-path), as documented by the host-agnostic
 * createRunRouter default.
 */
export function reconcileApprovalsOnStatusDetached(
  systemActorId: string,
  waitUntil: (promise: Promise<unknown>) => void,
): ReturnType<typeof reconcileApprovalsOnStatus> {
  const reconcile = reconcileApprovalsOnStatus(systemActorId);
  return (tenant, workflowId, summary) => {
    waitUntil(
      reconcile(tenant, workflowId, summary).catch((error: unknown) =>
        console.error(
          JSON.stringify({
            type: 'reconcile-error',
            workflowId,
            runId: summary.runId,
            error: error instanceof Error ? error.message : String(error),
          }),
        ),
      ),
    );
    return Promise.resolve();
  };
}

/** Options for the cron-owned approval-retention purge. */
export interface ApprovalRetentionPurgeOptions {
  /** factory.system() — the cron-only cross-tenant view. */
  store: SystemApprovalStore;
  /**
   * Raw APPROVAL_RETENTION_DAYS env value; parsed via numberVar(…, 30,
   * 'APPROVAL_RETENTION_DAYS', { allowZero: true }) and converted to ms —
   * 0 purges decided approvals immediately, the same convention
   * RUN_RETENTION_DAYS uses.
   */
  retentionDays: string | undefined;
  /** The firing cron expression — carried into the maintenance-error log line for correlation. */
  cron: string;
}

/**
 * The cron-owned approval-retention purge, previously hand-copied verbatim
 * by the hosts (deploy/worker.ts and the showcase worker): purgeExpiredApprovals
 * over the SYSTEM store (cross-tenant, cron-only — never a service method or
 * HTTP route, see retention.ts), the APPROVAL_RETENTION_DAYS var parsing
 * (allowZero: a 0-day retention purges decided approvals immediately, the
 * same convention RUN_RETENTION_DAYS uses), and containment — a purge
 * failure logs a maintenance-error and resolves `undefined` instead of
 * throwing, so it never aborts a caller's other maintenance duties. Unlike
 * runSlaSweepMaintenance, this does NOT log its own "maintenance" summary
 * line: both hosts fold the returned count into ONE combined purge log
 * alongside their other maintenance duties (the snapshot purge, and — for
 * the showcase — the demo-tenant reaper), so logging it here too would
 * double-log.
 */
export async function runApprovalRetentionPurge(
  options: ApprovalRetentionPurgeOptions,
): Promise<number | undefined> {
  try {
    return await purgeExpiredApprovals(options.store, {
      // allowZero: APPROVAL_RETENTION_DAYS=0 means "purge decided approvals
      // now" — same convention as RUN_RETENTION_DAYS.
      ttlMs:
        numberVar(options.retentionDays, 30, 'APPROVAL_RETENTION_DAYS', {
          allowZero: true,
        }) *
        24 *
        60 *
        60 *
        1000,
    });
  } catch (error) {
    console.error(
      JSON.stringify({
        type: 'maintenance-error',
        surface: 'approval-retention-purge',
        cron: options.cron,
        error: String(error),
      }),
    );
    return undefined;
  }
}
