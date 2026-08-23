// SPDX-License-Identifier: Apache-2.0
// The host-side ApprovalService assembly and alarm-owned SLA sweep that the
// showcase Worker and the deploy template previously carried as byte-copies:
// the structured-log + optional-Queues audit sink, the system principal, and the
// SoD-guarded multi-gate
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
  ApprovalStore,
  ApprovalStreamSink,
  SelfDecisionPolicy,
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
import type { ExecutionFenceWiring } from '../do-runner/execution-fence.js';
import { validateTablePrefix } from '../do-runner/table-prefix.js';
import {
  type ResumeRunFn,
  reconcileApprovalsOnStatus,
  resumeRunWithRequeue,
} from './approval-bridge.js';
import { numberVar } from './env-vars.js';

/**
 * Attribution identity for alarm-owned maintenance — audit only. The sweep is TCB
 * code over the deployment store and never enters through a verifier.
 */
export function maintenancePrincipal(
  systemPrincipalId: string,
): TrustedAutomationPrincipal {
  return trustAutomationPrincipal({
    kind: 'system',
    id: systemPrincipalId,
    purpose: 'approval-sla-maintenance',
  });
}

// One factory per isolate, not per request: it owns the memoized schema-init
// promise, so rebuilding it inside fetch() would re-run the whole DDL pass
// (CREATE TABLE + DROP/CREATE indexes + PRAGMA + ALTERs) on every request.
// Keyed by the D1 binding, which is stable for an isolate's lifetime.
const approvalFactories = new WeakMap<
  ApprovalDatabase,
  Map<string, D1ApprovalStoreFactory>
>();

export function approvalStoreFactoryFor(
  db: ApprovalDatabase,
  storageTablePrefix?: string,
): D1ApprovalStoreFactory {
  const prefix =
    validateTablePrefix(storageTablePrefix, 'storageTablePrefix') ?? '';
  let factories = approvalFactories.get(db);
  if (!factories) {
    factories = new Map();
    approvalFactories.set(db, factories);
  }
  let factory = factories.get(prefix);
  if (!factory) {
    factory = new D1ApprovalStoreFactory(db, {
      workflowSnapshotTable: `${prefix}mastra_workflow_snapshot`,
    });
    factories.set(prefix, factory);
  }
  return factory;
}

export interface HostAuditSinkOptions {
  /** Infrastructure-verified deployment tag for audit attribution. */
  deploymentTag?: string;
  /** Optional Cloudflare queue producer for SIEM export. */
  queue?: AuditQueue<ApprovalAuditEvent>;
  /**
   * Keeps each queue send alive past the handler — ctx.waitUntil in fetch
   * scope; an alarm runner instead collects the sends into its directly
   * awaited duty.
   */
  keepAlive?: (send: Promise<unknown>) => void;
  /** Observes a contained queue failure when the caller tracks duty health. */
  onError?: (error: unknown) => void;
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
    const attributed =
      options.deploymentTag === undefined
        ? event
        : {
            ...event,
            detail: { ...event.detail, deploymentTag: options.deploymentTag },
          };
    console.log(JSON.stringify({ type: 'audit', ...attributed }));
    if (queueSink) {
      const send = queueSink(attributed).catch((error: unknown) => {
        options.onError?.(error);
        console.error(
          JSON.stringify({
            type: 'audit-queue-error',
            reason: String(error),
          }),
        );
      });
      options.keepAlive?.(send);
    }
  };
}

export interface HostApprovalServiceOptions {
  /** Infrastructure-verified deployment tag for audit attribution. */
  deploymentTag?: string;
  /**
   * System-principal id used to authorize bridge bookkeeping. Requester kind
   * is persisted separately, so ids may overlap across principal kinds.
   */
  systemPrincipalId: string;
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
   * IDENTICAL value to createActorResolver's `allowSelfDecision` so the
   * context's `canSelfDecide` display hint matches what this actually enforces.
   */
  allowSelfDecision?: SelfDecisionPolicy;
  /**
   * Live-stream fan-out sink (ApprovalServiceOptions.stream) — fired once per
   * successful approval mutation for the deployment hub Durable Object. The host
   * supplies a sink through `createHubTopology`, wrapping the transport keepalive in `ctx.waitUntil`
   * at fetch scope. Undefined means no live fan-out (a poll-only host).
   */
  stream?: ApprovalStreamSink;
  /**
   * The deployment execution fence (do-runner/execution-fence.ts), forwarded to
   * ApprovalServiceOptions.executionFence, or `'none'` for a service with no
   * database behind it.
   *
   * REQUIRED, and the `'none'` branch is genuinely dangerous rather than merely
   * unusual: `'none'` makes decide() unfenced, and decide COMMITS the decision
   * and only then resumes. A migration-locked deployment would durably record a
   * decision — with its audit trail and its notification — whose resume then
   * 503s, and the deployment taking over inherits a decided approval with
   * nothing behind it. Write it only for a service that has no database to
   * fence against at all.
   *
   * This function receives an ApprovalStore rather than a database, so it
   * cannot build the store itself the way `init({ DB })` can — which is exactly
   * why the option is required rather than optional: the host is the only place
   * the wiring can happen, so the type has to make it name one.
   */
  executionFence: ExecutionFenceWiring;
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
  store: ApprovalStore,
  options: HostApprovalServiceOptions,
): ApprovalService {
  const audit = hostAuditSink({
    deploymentTag: options.deploymentTag,
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
    // Forwarded as written, opt-out included: ApprovalService now requires the
    // same wiring this composer does, so there is nothing left to resolve here
    // — and resolving `'none'` to `undefined` on the way down would have
    // erased, one layer above the gate, the distinction between a host that
    // named the opt-out and one that never held a fence at all.
    executionFence: options.executionFence,
    resumeRun: resumeRunWithRequeue(
      options.resumeRun,
      () => service,
      options.systemPrincipalId,
      audit,
    ),
  });
  return service;
}

export interface SlaSweepMaintenanceOptions {
  /** The deployment approval store. */
  store: ApprovalStore;
  /**
   * `maintenancePrincipal(systemPrincipalId)`. Typed as merely automated, not
   * vouched: the sweep derives no authority from the principal (the
   * deployment store is supplied only by the host), so demanding the trust
   * brand here would ask for a token nothing on this path reads.
   */
  systemPrincipal: AutomatedExecutionPrincipal;
  /** Infrastructure-verified deployment tag for audit attribution. */
  deploymentTag?: string;
  /** Optional audit export queue. */
  queue?: AuditQueue<ApprovalAuditEvent>;
  /** The maintenance duty name, used only for log correlation. */
  trigger: string;
  /**
   * Notification transport for SLA escalations — threaded to
   * SweepSLAOptions.notify. The alarm handler awaits the complete maintenance
   * duty, so transports need no request-scoped keep-alive here.
   */
  notify?: ApprovalNotificationSink;
  /**
   * Live-stream fan-out sink for SLA escalations — the BARE hub-publish thunk
   * `(event) => createHubTopology(env.HUB, env.DEPLOYMENT_IDENTITY_SECRET).publish(event)`, NOT wrapped in a
   * request-scoped waitUntil. A maintenance alarm directly awaits
   * runSlaSweepMaintenance, which collects each publish promise into its
   * pendingSends and awaits the terminal Promise.all. Undefined means no live
   * escalation fan-out.
   */
  stream?: ApprovalStreamSink;
}

/** Explicit result for failure-contained maintenance helpers. */
export type MaintenanceOutcome<T = undefined> =
  | { readonly ok: true; readonly value: T }
  | { readonly ok: false; readonly error: string };

/**
 * The maintenance-owned SLA sweep, verbatim-shared by the hosts: sweepSLA over the
 * deployment store, audit to logs + optional queue, a structured line per
 * escalation, and containment — a sweep failure logs a maintenance-error and
 * resolves rather than throwing, so a caller running other duties is never
 * starved by this one (separate alarm invocations cover the uncatchable CPU-limit
 * case). Queue sends are collected and awaited here as part of the alarm's
 * directly awaited maintenance duty.
 */
export async function runSlaSweepMaintenance(
  options: SlaSweepMaintenanceOptions,
): Promise<MaintenanceOutcome> {
  let escalated: number | undefined;
  const failures: string[] = [];
  const pendingSends: Promise<unknown>[] = [];
  try {
    escalated = (
      await sweepSLA(options.store, {
        systemPrincipal: options.systemPrincipal,
        audit: hostAuditSink({
          deploymentTag: options.deploymentTag,
          queue: options.queue,
          keepAlive: (send) => pendingSends.push(send),
          onError: (error) => failures.push(`audit-queue: ${String(error)}`),
        }),
        notify: options.notify,
        // Mirror the audit sink's keepAlive idiom: COLLECT each escalation's
        // publish promise into pendingSends so the terminal Promise.all keeps it
        // within the directly awaited alarm duty. The inner .catch keeps a
        // failed fan-out from rejecting the whole Promise.all.
        stream: (event) => {
          pendingSends.push(
            Promise.resolve(options.stream?.(event)).catch((error: unknown) => {
              failures.push(`stream-publish: ${String(error)}`);
              // Log a wedged maintenance fan-out (matching the fetch path's
              // stream-publish-error) instead of swallowing it silently, while
              // still containing it so it can't reject the whole Promise.all.
              console.error(
                JSON.stringify({
                  type: 'stream-publish-error',
                  reason:
                    error instanceof Error ? error.message : String(error),
                }),
              );
            }),
          );
        },
        onEscalation: (record) =>
          console.log(
            JSON.stringify({
              type: 'sla-escalation',
              ...(options.deploymentTag !== undefined
                ? { deploymentTag: options.deploymentTag }
                : {}),
              id: record.id,
              workflowId: record.workflowId,
              runId: record.runId,
              slaDeadlineAt: record.slaDeadlineAt,
            }),
          ),
      })
    ).length;
  } catch (error) {
    failures.push(String(error));
    console.error(
      JSON.stringify({
        type: 'maintenance-error',
        ...(options.deploymentTag !== undefined
          ? { deploymentTag: options.deploymentTag }
          : {}),
        surface: 'sla-sweep',
        trigger: options.trigger,
        error: String(error),
      }),
    );
  }
  await Promise.all(pendingSends);
  console.log(
    JSON.stringify({
      type: 'maintenance',
      ...(options.deploymentTag !== undefined
        ? { deploymentTag: options.deploymentTag }
        : {}),
      trigger: options.trigger,
      escalated,
    }),
  );
  return failures.length === 0
    ? { ok: true, value: undefined }
    : { ok: false, error: failures.join('; ') };
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
  systemPrincipalId: string,
  waitUntil: (promise: Promise<unknown>) => void,
): ReturnType<typeof reconcileApprovalsOnStatus> {
  const reconcile = reconcileApprovalsOnStatus(systemPrincipalId);
  return (context, workflowId, summary) => {
    waitUntil(
      reconcile(context, workflowId, summary).catch((error: unknown) =>
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

/** Options for the maintenance-owned approval-retention purge. */
export interface ApprovalRetentionPurgeOptions {
  /** The deployment approval store. */
  store: ApprovalStore;
  /**
   * Raw APPROVAL_RETENTION_DAYS env value; parsed via numberVar(…, 30,
   * 'APPROVAL_RETENTION_DAYS', { allowZero: true }) and converted to ms —
   * 0 purges decided approvals immediately, the same convention
   * RUN_RETENTION_DAYS uses.
   */
  retentionDays: string | undefined;
  /** The maintenance duty name, used only for log correlation. */
  trigger: string;
}

/**
 * The maintenance-owned approval-retention purge, previously hand-copied verbatim
 * by the hosts (deploy/worker.ts and the showcase worker): purgeExpiredApprovals
 * over the deployment store, the APPROVAL_RETENTION_DAYS var parsing
 * (allowZero: a 0-day retention purges decided approvals immediately, the
 * same convention RUN_RETENTION_DAYS uses), and containment — a purge
 * failure logs a maintenance-error and returns an explicit failed outcome
 * instead of throwing, so it never aborts a caller's other duties or gets
 * mistaken for a successful purge. Unlike
 * runSlaSweepMaintenance, this does NOT log its own "maintenance" summary
 * line: both hosts fold the returned count into ONE combined purge log
 * alongside their other maintenance duties, so logging it here too would
 * double-log.
 */
export async function runApprovalRetentionPurge(
  options: ApprovalRetentionPurgeOptions,
): Promise<MaintenanceOutcome<number>> {
  try {
    return {
      ok: true,
      value: await purgeExpiredApprovals(options.store, {
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
      }),
    };
  } catch (error) {
    const failure = String(error);
    console.error(
      JSON.stringify({
        type: 'maintenance-error',
        surface: 'approval-retention-purge',
        trigger: options.trigger,
        error: failure,
      }),
    );
    return { ok: false, error: failure };
  }
}
