// Host-agnostic approval bridge: the glue that turns a workflow suspension into
// an approval request and re-queues the next gate on a multi-gate run. Promoted
// out of gtm-app/worker.ts so every host (the showcase Worker, the dev backend)
// shares one implementation instead of re-deriving the (suspendedAt, resumeCount)
// capture and the SoD-across-gates re-queue.

import type {
  ApprovalActor,
  ApprovalDecision,
  ApprovalRecord,
  ApprovalService,
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

/** Steps declare the grants they need in their suspend payload. */
export function requestedConnectors(stepPayload: unknown): string[] {
  if (stepPayload === null || typeof stepPayload !== 'object') return [];
  const connectors = (stepPayload as Record<string, unknown>).connectors;
  return Array.isArray(connectors) &&
    connectors.every((c): c is string => typeof c === 'string')
    ? connectors
    : [];
}

/**
 * A suspension IS an approval request: queue it (idempotently — the store's
 * partial unique index collapses duplicates). Capturing the step's
 * (suspendedAt, resumeCount) pair binds the approval to THIS suspension exactly
 * (clock-free grant minting), and the suspend payload's `connectors` declares
 * what a decision should mint.
 *
 * `requestedBy` is the HUMAN who advanced the run to this suspension — the actor
 * who started it, or the reviewer whose decision caused a re-suspension at the
 * next gate. It must NOT be the system actor: the library's self-decision
 * separation-of-duties check compares `requestedBy` to the deciding actor, so
 * attributing every request to the system actor would make that check unfireable.
 * `systemActor` is only the record's creator (needs a create-capable role).
 */
export async function queueApprovalForSuspension(
  service: ApprovalService,
  workflowId: string,
  summary: RunSummary,
  requestedBy: string,
  systemActor: ApprovalActor,
): Promise<ApprovalRecord> {
  const stepPath = summary.suspended?.[0];
  const stepKey = stepPath?.join('.');
  const stepPayload =
    stepKey !== undefined &&
    summary.suspendPayload !== null &&
    typeof summary.suspendPayload === 'object'
      ? (summary.suspendPayload as Record<string, unknown>)[stepKey]
      : undefined;
  const connectors = requestedConnectors(stepPayload);
  const { record } = await service.create(
    {
      workflowId,
      runId: summary.runId,
      stepPath,
      suspendedAt:
        stepKey !== undefined ? summary.suspendedAt?.[stepKey] : undefined,
      resumedAt:
        stepKey !== undefined ? summary.resumedAt?.[stepKey] : undefined,
      resumeCount:
        stepKey !== undefined ? summary.resumeCount?.[stepKey] : undefined,
      title: `Approve '${workflowId}' run`,
      payload: summary.suspendPayload,
      connectors: connectors.length > 0 ? connectors : undefined,
      requestedBy,
    },
    systemActor,
  );
  return record;
}

/**
 * Wrap a base resume fn so a run that re-suspends at a LATER gate auto-queues
 * its next approval — the multi-gate flow (product-launch's two gates). The base
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
 */
export function resumeRunWithRequeue(
  base: ResumeRunFn,
  getService: () => ApprovalService,
  systemActor: ApprovalActor,
): ResumeRunFn {
  return async (record, decision) => {
    const summary = await base(record, decision);
    if (summary.status === 'suspended') {
      if (!record.decidedBy) {
        throw new Error(
          'resumeRunWithRequeue: decidedBy unset — refusing to re-queue an approval without a requester',
        );
      }
      await queueApprovalForSuspension(
        getService(),
        record.workflowId,
        summary,
        record.decidedBy,
        systemActor,
      );
    }
    return summary;
  };
}
