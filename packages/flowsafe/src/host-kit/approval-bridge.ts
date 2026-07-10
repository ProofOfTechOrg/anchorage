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
 * `systemActor` is only the record's creator (needs a create-capable role).
 */
export async function queueApprovalForSuspension(
  service: ApprovalService,
  workflowId: string,
  summary: RunSummary,
  requestedBy: string,
  systemActor: ApprovalActor,
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
      const { record } = await service.create(
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
        systemActor,
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
