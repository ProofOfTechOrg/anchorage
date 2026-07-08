// Grant derivation — how a decision becomes an in-run capability.
//
// Grants NEVER travel in HTTP bodies. The runner's public resume route stays
// grant-free (a proxying Worker cannot forward what does not exist), and the
// requestContext the resumed leg executes under is minted server-side by
// approvalGrantProvider on EVERY start/resume.
//
// Grants are SUSPENSION-SCOPED: a step-keyed approval unlocks its connectors
// only for the leg that resumes THAT step, and only when the decision binds
// to the step's CURRENT suspension. The preferred binding is clock-free and
// exact on the (suspendedAt, resumedAt) fingerprint the snapshot carries: the
// creating bridge captures both into the record (CreateApprovalInput), and
// minting requires record.suspendedAt === leg.suspendedAt AND
// record.resumedAt === leg.resumedAt — all four come from the core clock, so
// a decision bound to any other suspension of the step never mints. resumedAt
// is the categorical tie-breaker: a step's FIRST suspension has it undefined,
// a RE-suspension defined, so even two same-step suspensions whose suspendedAt
// collide within a millisecond (possible only on the synchronous in-process
// path — production round-trips make them seconds apart) stay distinguishable.
// That closes both leak shapes: approving connector X at one approval point
// never unlocks X at another point of the run, and when the SAME step suspends
// again later, the earlier approval is spent — the new suspension needs its
// own decision, and a rejected re-quest never falls back to the old approval.
// (Residual, deferred: a chain of three+ suspensions compares two defined
// resumedAt values, which collide only if BOTH resumes AND both re-suspends
// each land in the same ms — astronomically rare, in-process only; the
// bulletproof fix is a synthesized monotonic per-(run,step) counter.)
// Records created WITHOUT observing the suspension (pre-capture bridges)
// fall back to chronology — service-clock decidedAt strictly after the
// core-clock suspendedAt — correct only where the two clocks are one
// (single-Worker/Cloudflare deployments; see the threat model, boundary 6).
//
// Step-less approvals (stepPath omitted at create) are explicitly RUN-SCOPED
// standing grants and mint on every leg — the deliberate opt-out, not the
// default; suspend-observation bridges should always set stepPath.
//
// The provider always returns the grant key — an empty list when nothing
// applies — because Mastra merges provided context over the persisted
// snapshot: overwriting each leg retires the previous leg's grants instead
// of letting them leak forward. A resume that bypasses decide() (or targets
// a suspension nothing was decided for) therefore finds no grant — fail
// closed.

import type {
  RequestContextProvider,
  RunLeg,
  RunnerRuntime,
  RunSummary,
} from '../do-runner/index.js';
import { BREAKWATER_APPROVED_CONNECTORS_KEY } from './contract.js';
import { type ApprovalStore, stepKeyOf } from './store.js';
import type { ApprovalDecision, ApprovalRecord } from './types.js';

// Whether a step-keyed approval is bound to the resumed step's CURRENT
// suspension (exact (suspendedAt, resumedAt) pair match, or the legacy
// decidedAt-after fallback) — the predicate that decides whether a decision
// mints.
function boundToCurrentSuspension(
  record: ApprovalRecord,
  suspendedAt: number | undefined,
  resumedAt: number | undefined,
): boolean {
  // Unknown suspension time (snapshot without step timestamps, or an
  // unresolvable resume target) — fail closed for step-keyed grants.
  if (suspendedAt === undefined) return false;
  // Preferred, clock-free binding on the (suspendedAt, resumedAt) fingerprint.
  // The bridge captured both from the snapshot at create time; both come from
  // the core clock and must match EXACTLY. Mastra stamps both with Date.now(),
  // so two same-step suspensions CAN share suspendedAt within one millisecond
  // (only on the synchronous in-process path — production has HTTP+D1 round-
  // trips between suspensions) — but a step's FIRST suspension has resumedAt
  // undefined and a RE-suspension has it defined, a categorical difference no
  // ms collision erases. undefined === undefined holds (first-vs-first), so the
  // pair reduces to the suspendedAt-exact case whenever resumedAt is absent on
  // both sides, and only tightens the deny direction when resumedAt differs.
  // See the module header for the depth-3+ residual (deferred monotonic
  // counter).
  if (record.suspendedAt !== undefined) {
    return record.suspendedAt === suspendedAt && record.resumedAt === resumedAt;
  }
  // Legacy fallback for records created without observing the suspension
  // (pre-capture bridges): service-clock decidedAt strictly after the core-
  // clock suspendedAt — correct on same-clock (single-Worker/Cloudflare)
  // topologies only; see security-threat-model.md boundary 6.
  return (
    record.decidedAt !== undefined && Date.parse(record.decidedAt) > suspendedAt
  );
}

/**
 * Union of connector ids across APPROVED records that apply to this leg:
 * step-keyed records matching the resumed step AND bound to the step's
 * CURRENT suspension (exact `(suspendedAt, resumedAt)` pair match, or the
 * legacy decidedAt-after fallback for records created without the capture),
 * plus step-less (run-scoped) records. Start legs and unresolvable resume
 * targets mint run-scoped records only.
 */
export async function approvedConnectorsForLeg(
  store: ApprovalStore,
  workflowId: string,
  runId: string,
  leg: RunLeg,
): Promise<string[]> {
  const approved = await store.list({ workflowId, runId, status: 'approved' });
  const targetKey =
    leg.kind === 'resume' && leg.step !== undefined
      ? stepKeyOf(leg.step)
      : undefined;
  const suspendedAt = leg.kind === 'resume' ? leg.suspendedAt : undefined;
  const resumedAt = leg.kind === 'resume' ? leg.resumedAt : undefined;
  const connectors = new Set<string>();
  for (const record of approved) {
    const recordKey = stepKeyOf(record.stepPath);
    const applies =
      recordKey === ''
        ? true
        : targetKey !== undefined &&
          recordKey === targetKey &&
          boundToCurrentSuspension(record, suspendedAt, resumedAt);
    if (!applies) continue;
    for (const connector of record.connectors) {
      connectors.add(connector);
    }
  }
  return [...connectors];
}

/**
 * RequestContextProvider wiring the approval store into the DO runner:
 * pass as init()'s / RunnerRuntimeOptions' requestContextForRun. Part of the
 * trusted computing base — it writes the breakwater grant key.
 */
export function approvalGrantProvider(
  store: ApprovalStore,
): RequestContextProvider {
  return async (workflowId, runId, leg) => {
    const connectors = await approvedConnectorsForLeg(
      store,
      workflowId,
      runId,
      leg,
    );
    // Always return the key (even empty) — see the header: overwrite, don't
    // inherit, so stale grants from earlier legs cannot survive the merge.
    return { [BREAKWATER_APPROVED_CONNECTORS_KEY]: connectors };
  };
}

/**
 * The default resumeData contract a decided approval resumes with:
 * `{ approved: boolean, comment?, decidedBy? }`. Approval steps either accept
 * this shape in their resumeSchema or the deployment overrides the builder.
 */
export function defaultResumeData(
  record: ApprovalRecord,
  decision: ApprovalDecision,
): Record<string, unknown> {
  const data: Record<string, unknown> = { approved: decision === 'approve' };
  if (record.comment !== undefined) data.comment = record.comment;
  if (record.decidedBy !== undefined) data.decidedBy = record.decidedBy;
  return data;
}

/**
 * ApprovalServiceOptions.resumeRun for same-process deployments: resumes the
 * decided run on a RunnerRuntime (rejects are resumed too — the workflow
 * learns the outcome from resumeData.approved). Cross-Worker deployments
 * implement resumeRun against the run's DO stub instead.
 */
export function resumeViaRuntime(
  runtime: RunnerRuntime,
  options: {
    resumeData?: (
      record: ApprovalRecord,
      decision: ApprovalDecision,
    ) => unknown;
  } = {},
): (record: ApprovalRecord, decision: ApprovalDecision) => Promise<RunSummary> {
  const buildResumeData = options.resumeData ?? defaultResumeData;
  return (record, decision) =>
    runtime.resume(record.workflowId, record.runId, {
      step: record.stepPath,
      resumeData: buildResumeData(record, decision),
    });
}
