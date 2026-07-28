// SPDX-License-Identifier: Apache-2.0
// Host-agnostic approval bridge: the glue that turns a workflow suspension into
// an approval request and re-queues the next gate on a multi-gate run. Promoted
// out of gtm-app/worker.ts so every host (the showcase Worker, the dev backend)
// shares one implementation instead of re-deriving the (suspendedAt, resumeCount)
// capture and the SoD-across-gates re-queue.

import { agentGateConnectors } from '../agent-runner/approval-shapes.js';
import {
  type ApprovalActor,
  type ApprovalAuditSink,
  type ApprovalDecision,
  type ApprovalRecord,
  type ApprovalResumeTarget,
  type ApprovalService,
  approvalCursor,
  MAX_APPROVAL_LIST_LIMIT,
  OPEN_STATUSES,
  principalActor,
  stepKeyOf,
  type TenantContext,
  type TrustedAutomationPrincipal,
  trustAutomationPrincipal,
} from '../approval-api/index.js';
import type { RunSummary } from '../do-runner/index.js';

/**
 * Resumes a run after a decision. The showcase Worker fetches the run's DO stub;
 * an in-process host uses resumeViaRuntime(runtime). Either way it returns the
 * post-resume RunSummary so the re-queue wrapper can inspect status.
 */
export type ResumeRunFn = (
  record: ApprovalRecord,
  decision: ApprovalDecision,
) => Promise<RunSummary>;

/**
 * The connector ids a suspended step's decision should mint. A workflow STEP
 * gate declares them explicitly in a `connectors` array. An agent gate
 * declares none — it names the tool the model called by `toolName` in both
 * durable suspend shapes). For automatic grant derivation, that provider-visible
 * name MUST already be the breakwater connector's provider-safe id
 * (`[A-Za-z0-9_-]+`); providers can rewrite punctuation-bearing ids and the
 * suspend payload carries no reversible original-id field. agentGateConnectors
 * derives [toolName] so an approved agent gate mints exactly that connector's
 * grant. The explicit array wins when present, so workflow gates are unaffected.
 *
 * ACCEPTED RISK (narrow): the agent fallback is a workflow-agnostic shape sniff —
 * a suspend payload has no workflowId to prove it came from the durable loop. A
 * workflow author who both omits `connectors` AND independently shapes a gate as
 * `{type:'approval', toolName, toolCallId}` (all three, the second discriminator
 * that agentGateConnectors demands) would derive [toolName] where earlier
 * behavior returned []. It is inert unless that toolName also names a real connector
 * the run calls; the shipped/showcase workflows all declare `connectors`, so none
 * collide today. The right convention for workflow gates remains the explicit
 * `connectors` array.
 */
export function requestedConnectors(stepPayload: unknown): string[] {
  if (stepPayload === null || typeof stepPayload !== 'object') return [];
  const connectors = (stepPayload as Record<string, unknown>).connectors;
  if (
    Array.isArray(connectors) &&
    connectors.every((c): c is string => typeof c === 'string')
  ) {
    return connectors;
  }
  return agentGateConnectors(stepPayload);
}

/**
 * A suspension IS an approval request: queue one record per suspended step
 * path (idempotently — the store's partial unique open-step index collapses
 * duplicates, so re-queuing an already-queued gate is a no-op). EVERY path
 * files: `.parallel()` branches can suspend together
 * (`summary.suspended = [['a'], ['b']]`), and a gate that never reaches the
 * queue can never be decided — its connector then denies on every resume
 * (fail closed) with nothing telling a reviewer why the run is stuck.
 * Capturing each step's (suspendedAt, resumeCount) pair binds its approval to
 * THAT suspension exactly (clock-free grant minting), and each suspend
 * payload's `connectors` declares what a decision should mint.
 *
 * `requestedBy` is the HUMAN who advanced the run to this suspension — the actor
 * who started it, or the reviewer whose decision caused a re-suspension at the
 * next gate. It must NOT be the system actor: the library's self-decision
 * separation-of-duties check compares `requestedBy` to the deciding actor, so
 * attributing every request to the system actor would make that check unfireable.
 * `systemPrincipal` is only the record's creator. It is an automated
 * ExecutionPrincipal filed through the service's trusted system entry, never a
 * fabricated human role.
 */
export async function queueApprovalForSuspension(
  service: ApprovalService,
  workflowId: string,
  summary: RunSummary,
  requestedBy: string,
  systemPrincipal: TrustedAutomationPrincipal,
  resumeTarget?: ApprovalResumeTarget,
): Promise<ApprovalRecord[]> {
  const suspended = summary.suspended ?? [];
  const records: ApprovalRecord[] = [];
  const failures: Array<{ stepKey: string; message: string }> = [];
  for (const stepPath of suspended) {
    const stepKey = stepPath.join('.');
    const stepPayload =
      summary.suspendPayload !== null &&
      typeof summary.suspendPayload === 'object'
        ? (summary.suspendPayload as Record<string, unknown>)[stepKey]
        : undefined;
    const connectors = requestedConnectors(stepPayload);
    try {
      const { record } = await service.createAsPrincipal(
        {
          workflowId,
          runId: summary.runId,
          stepPath,
          suspendedAt: summary.suspendedAt?.[stepKey],
          resumedAt: summary.resumedAt?.[stepKey],
          resumeCount: summary.resumeCount?.[stepKey],
          title: `Approve '${workflowId}' run`,
          payload: summary.suspendPayload,
          connectors: connectors.length > 0 ? connectors : undefined,
          requestedBy,
        },
        systemPrincipal,
        resumeTarget,
      );
      records.push(record);
    } catch (error) {
      // Isolate per path (the purge loops' convention): the run is ALREADY
      // suspended by the time this files, and a client retry of POST /runs
      // mints a fresh runId rather than re-filing this one — so a transient
      // store failure on one gate must not abandon its siblings' filings.
      // File every path that can file, then re-throw aggregated so the
      // caller still surfaces the failure. Filed records persist (create is
      // idempotent under the open-step unique index), and the next
      // decision's re-queue re-files any gate still missing.
      failures.push({
        stepKey,
        message: error instanceof Error ? error.message : String(error),
      });
    }
  }
  if (failures.length > 0) {
    throw new Error(
      `queueApprovalForSuspension: ${failures.length} of ${suspended.length} gate filing(s) failed, the rest were queued (${failures
        .map((failure) => `${failure.stepKey}: ${failure.message}`)
        .join('; ')})`,
    );
  }
  return records;
}

/**
 * Wrap a base resume fn so a run that re-suspends at a LATER gate auto-queues
 * its next approval(s) — the multi-gate flow (product-launch's two gates); every
 * suspended path re-queues, not just the first. The base
 * resume (resumeViaRuntime or a DO-stub fetch) deliberately omits re-queue; this
 * adds it without coupling to the host's resume topology.
 *
 * The reviewer whose decision advanced the run becomes the next gate's
 * `requestedBy`, so they cannot also decide it (SoD across gates). Guard
 * fail-CLOSED: refuse to re-queue without a requester rather than fall back to
 * the system id.
 *
 * `getService` is a thunk because the service references this closure at its own
 * construction (`service = new ApprovalService({ resumeRun: resumeRunWithRequeue(base, () => service, sys) })`);
 * the closure only runs on a later decision, so the reference is resolved by then.
 *
 * `audit` is optional and should be the SAME sink the service itself uses
 * (buildHostApprovalService wires it that way): the base resume above has
 * already durably advanced the run by the time the re-queue below can fail,
 * so a throw here leaves a gate suspended with no approval record and no
 * other signal that happened. reconcileApprovalsForSummary is the recovery;
 * this event is what tells an operator it was needed.
 */
export function resumeRunWithRequeue(
  base: ResumeRunFn,
  getService: () => ApprovalService,
  systemPrincipal: TrustedAutomationPrincipal,
  audit?: ApprovalAuditSink,
): ResumeRunFn {
  return async (record, decision) => {
    const summary = await base(record, decision);
    if (summary.status === 'suspended') {
      if (!record.decidedBy) {
        throw new Error(
          'resumeRunWithRequeue: decidedBy unset — refusing to re-queue an approval without a requester',
        );
      }
      const requestedBy =
        record.resumeTarget?.kind === 'agent-thread'
          ? record.resumeTarget.principal.id
          : record.decidedBy;
      try {
        await queueApprovalForSuspension(
          getService(),
          record.workflowId,
          summary,
          requestedBy,
          systemPrincipal,
          record.resumeTarget,
        );
      } catch (error) {
        // Best-effort: a crashing sink must not mask the resume failure it is
        // reporting (same "availability over export reliability" posture as
        // ApprovalService's own #record).
        try {
          const outcome = audit?.({
            actor: principalActor(systemPrincipal),
            action: 'approval.requeue',
            resource: `approval:${record.id}`,
            decision: 'error',
            reason: error instanceof Error ? error.message : String(error),
            detail: {
              tenantId: record.tenantId,
              workflowId: record.workflowId,
              runId: record.runId,
              suspended: summary.suspended,
            },
          });
          if (outcome instanceof Promise) {
            outcome.catch(() => {
              // ignore — see comment above
            });
          }
        } catch {
          // ignore — see comment above
        }
        throw error;
      }
    }
    return summary;
  };
}

/**
 * Pages service.list() to completion via an explicit cursor instead of one
 * unbounded call (`filter.limit` unset otherwise means "no limit" —
 * types.ts's clampApprovalLimit). reconcileApprovalsForSummary needs a
 * suspended run's FULL approval history — every status, every past
 * suspension — to tell a stale fingerprint from a current one; a truncated
 * read could hide a stale open record (so it never gets superseded) or a
 * decided-current one (so a fresh record double-files over it).
 */
async function listAllApprovals(
  service: ApprovalService,
  filter: { workflowId: string; runId: string },
  actor: ApprovalActor,
): Promise<ApprovalRecord[]> {
  const all: ApprovalRecord[] = [];
  let after: string | undefined;
  for (;;) {
    const page = await service.list(
      { ...filter, limit: MAX_APPROVAL_LIST_LIMIT, after },
      actor,
    );
    all.push(...page);
    const last = page.at(-1);
    if (page.length < MAX_APPROVAL_LIST_LIMIT || !last) break;
    after = approvalCursor(last);
  }
  return all;
}

/**
 * Recovery primitive: given a run's suspended RunSummary, heals every
 * CURRENTLY suspended step whose exact (suspendedAt, resumeCount)
 * fingerprint has no approval record of ANY status, then files it fresh.
 * Safe to call on every status() read of a suspended run, rather than only
 * after a confirmed bridge failure, because per step:
 *
 *  - any record (any status) already bound to the CURRENT fingerprint means
 *    nothing to do — a matching pending/claimed/escalated record makes
 *    queueApprovalForSuspension's own idempotent create() a no-op anyway
 *    (this skips the round-trip), and a matching DECIDED record means the
 *    decision landed but its resume has not (yet, or ever, on the wedge path
 *    this exists for) re-queued the next gate — re-filing would double-file
 *    a gate correctly waiting on its own resume, the decide -> resume
 *    in-flight window;
 *  - otherwise, every STALE OPEN record for that step (pending/claimed/
 *    escalated, bound to a suspension the step has since moved past —
 *    produced e.g. by the raw grant-free resume route re-suspending the step
 *    while an earlier request still sits open) is SUPERSEDED first
 *    (ApprovalService.supersedeStale: a CAS transition straight to
 *    'rejected', system-attributed, audited as approval.supersede, never
 *    through decide() — so it never touches the run) before the fresh file,
 *    closing the loop where a stale-but-open record otherwise never heals
 *    (every poll re-lists, finds the same open record via the open-step
 *    uniqueness index, and re-files nothing). A stale DECIDED record needs
 *    no supersede — it is already terminal and excluded from grant
 *    derivation by its own (non-'approved', or fingerprint-mismatched)
 *    status.
 *  - if a supersede loses its CAS (a real decision won the race between the
 *    list() above and the supersede), this step is left alone for this
 *    round entirely — no fresh file — rather than clobbering or
 *    double-filing over a decision that just landed; the next status() read
 *    re-evaluates against a fresh summary and a fresh list().
 *
 * Delegates the actual filing to queueApprovalForSuspension against a copy of
 * `summary` narrowed to only the healed paths, so a step with a live or
 * in-flight record is never touched. `requestedBy` defaults to the SYSTEM
 * principal because generic workflow reconciliation cannot reliably recover the
 * initiating principal. Agent hosts persist the original requester and pass
 * that id explicitly so separation of duties survives eviction and filing
 * retries.
 */
export async function reconcileApprovalsForSummary(
  service: ApprovalService,
  workflowId: string,
  summary: RunSummary,
  systemPrincipal: TrustedAutomationPrincipal,
  resumeTarget?: ApprovalResumeTarget,
  requestedBy = systemPrincipal.id,
): Promise<ApprovalRecord[]> {
  const suspended = summary.suspended ?? [];
  if (suspended.length === 0) return [];
  const existing = await listAllApprovals(
    service,
    { workflowId, runId: summary.runId },
    principalActor(systemPrincipal),
  );
  const toFile: string[][] = [];
  for (const stepPath of suspended) {
    const stepKey = stepKeyOf(stepPath);
    const suspendedAt = summary.suspendedAt?.[stepKey];
    const resumeCount = summary.resumeCount?.[stepKey];
    const stepRecords = existing.filter(
      (record) => stepKeyOf(record.stepPath) === stepKey,
    );
    const boundToCurrent = stepRecords.some(
      (record) =>
        record.suspendedAt === suspendedAt &&
        record.resumeCount === resumeCount,
    );
    if (boundToCurrent) continue;

    let lostRace = false;
    for (const record of stepRecords) {
      if (!OPEN_STATUSES.includes(record.status)) continue;
      const superseded = await service.supersedeStaleAsPrincipal(
        record.id,
        systemPrincipal,
        'superseded: stale suspension fingerprint',
      );
      // null = a real decision won the CAS between the list() above and this
      // supersede — back off THIS step for THIS round rather than clobber or
      // double-file; the next status() read re-evaluates fresh.
      if (!superseded) lostRace = true;
    }
    if (lostRace) continue;
    toFile.push(stepPath);
  }
  if (toFile.length === 0) return [];
  return queueApprovalForSuspension(
    service,
    workflowId,
    { ...summary, suspended: toFile },
    requestedBy,
    systemPrincipal,
    resumeTarget,
  );
}

/**
 * Adapts reconcileApprovalsForSummary to RunRouterOptions.reconcileApprovals's
 * per-request shape: `tenant.service()`/`tenant.tenantId` only exist once a
 * request resolves, so only the systemActorId (a per-deployment constant) can
 * be bound ahead of time — everything else is read from the tenant the router
 * hands in on each call.
 */
export function reconcileApprovalsOnStatus(
  systemActorId: string,
): (
  tenant: TenantContext,
  workflowId: string,
  summary: RunSummary,
) => Promise<void> {
  return async (tenant, workflowId, summary) => {
    await reconcileApprovalsForSummary(
      tenant.service(),
      workflowId,
      summary,
      trustAutomationPrincipal({
        kind: 'system',
        id: systemActorId,
        tenantId: tenant.tenantId,
        purpose: 'approval-suspension-reconcile',
      }),
    );
  };
}
