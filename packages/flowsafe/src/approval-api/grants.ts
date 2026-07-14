// SPDX-License-Identifier: Apache-2.0
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
// exact on the (suspendedAt, resumeCount) fingerprint: the creating bridge
// captures both into the record (CreateApprovalInput), and minting requires
// record.suspendedAt === leg.suspendedAt AND
// record.resumeCount === leg.resumeCount, so a decision bound to any other
// suspension of the step never mints. resumeCount is the categorical
// tie-breaker: it is the runtime-owned monotonic per-(run,step) resume
// ordinal — undefined on a step's FIRST suspension, 1,2,… on successive
// RE-suspensions — that the runtime increments on EVERY resume, so even two
// same-step suspensions whose suspendedAt collide within a millisecond
// (possible only on the synchronous in-process path — production round-trips
// make them seconds apart) stay distinguishable, and unlike Mastra's
// payload-conditional resumedAt no no-payload resume can erase it. That closes
// both leak shapes: approving connector X at one approval point never unlocks
// X at another point of the run, and when the SAME step suspends again later,
// the earlier approval is spent — the new suspension needs its own decision,
// and a rejected re-request never falls back to the old approval. Because the
// ordinal strictly increments, a chain of three+ re-suspensions compares two
// distinct counts and never collides — closing the depth-3+ residual the
// prior (suspendedAt, resumedAt) binding deferred. The ledger is in-memory,
// but a reset across a DO restart can never leak: a same-ms suspendedAt
// collision (the only case the ordinal guards) requires a synchronous
// in-memory store with no I/O between the stamps, so same-ms-collision and
// surviving-a-restart are mutually exclusive — a durable (D1) deployment gives
// the two suspensions distinct suspendedAt, and the suspendedAt half alone
// distinguishes them. A reset ledger only yields leg.resumeCount=undefined
// against a re-suspension record's defined count → deny (fail-closed re-deny),
// then a fresh decision mints.
// Records created WITHOUT observing the suspension (pre-capture bridges)
// fall back to chronology — service-clock decidedAt strictly after the
// core-clock suspendedAt — correct only where the two clocks are one
// (single-Worker/Cloudflare deployments; see the threat model, boundary 6).
//
// Run-scoped standing grants are EXPLICIT: a step-less record mints on every
// leg only when it also carries runScoped === true. A step-less record without
// the flag mints nothing. The opt-out must be named, never inferred from a
// missing field — "absent stepPath implies run-wide privilege" was an inverted
// default that let any record whose stepPath was merely omitted (including one
// authored over the create route) become a standing capability. Neither field
// is settable over HTTP (router.ts, TCB_ONLY_CREATE_FIELDS);
// suspend-observation bridges always set stepPath and never runScoped.
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
import { tenantOfRunId } from '../do-runner/path-safe-id.js';
import { BREAKWATER_APPROVED_CONNECTORS_KEY } from './contract.js';
import {
  type ApprovalStore,
  listAllApprovedForRun,
  stepKeyOf,
} from './store.js';
import type { TenantBoundApprovalStore } from './tenant-brand.js';
import type { ApprovalDecision, ApprovalRecord } from './types.js';

// Whether a step-keyed approval is bound to the resumed step's CURRENT
// suspension (exact (suspendedAt, resumeCount) pair match, or the legacy
// decidedAt-after fallback) — the predicate that decides whether a decision
// mints.
function boundToCurrentSuspension(
  record: ApprovalRecord,
  suspendedAt: number | undefined,
  resumeCount: number | undefined,
): boolean {
  // Unknown suspension time (snapshot without step timestamps, or an
  // unresolvable resume target) — fail closed for step-keyed grants.
  if (suspendedAt === undefined) return false;
  // Preferred, clock-free binding on the (suspendedAt, resumeCount)
  // fingerprint. suspendedAt comes from the core clock; resumeCount is the
  // runtime's monotonic per-(run,step) resume ordinal (undefined on a first
  // suspension, 1,2,… on re-suspensions), captured by the bridge at create
  // time. Both must match EXACTLY. resumeCount — NOT the payload-conditional
  // resumedAt (informational only) — is the tie-breaker: because the runtime
  // increments it on every resume regardless of payload, a first-suspension
  // approval (resumeCount undefined) can never mint into a re-suspension leg
  // (resumeCount defined) even when their suspendedAt collide within one ms,
  // and successive re-suspensions carry strictly increasing counts that never
  // collide. A pre-`resume_count` record (suspendedAt defined, resumeCount
  // undefined from the upgrade window) still mints on a first-suspension leg
  // (undefined === undefined) and fails closed on a re-suspension leg — the
  // documented re-deny, resolved by a fresh decision.
  if (record.suspendedAt !== undefined) {
    return (
      record.suspendedAt === suspendedAt && record.resumeCount === resumeCount
    );
  }
  // Legacy fallback for records created without observing the suspension
  // (pre-capture bridges): service-clock decidedAt strictly after the core-
  // clock suspendedAt — correct on same-clock (single-Worker/Cloudflare)
  // topologies only; see security-threat-model.md boundary 6.
  //
  // TRANSITIONAL and self-draining — it fires only while pre-capture rows
  // remain undecided, and no client can force it (`suspendedAt` is in
  // TCB_ONLY_CREATE_FIELDS). INVARIANT for writers: every NEW step-keyed
  // record MUST capture `suspendedAt` (host-kit's bridge does); creating one
  // without it would re-open the clock-dependence this exact-match binding
  // exists to close.
  return (
    record.decidedAt !== undefined && Date.parse(record.decidedAt) > suspendedAt
  );
}

/**
 * Union of connector ids across APPROVED records that apply to this leg:
 * step-keyed records matching the resumed step AND bound to the step's
 * CURRENT suspension (exact `(suspendedAt, resumeCount)` pair match, or the
 * legacy decidedAt-after fallback for records created without the capture),
 * plus step-less records that explicitly opted in with `runScoped: true`.
 * Start legs and unresolvable resume targets mint run-scoped records only. A
 * step-less record without `runScoped` is inert on every leg — fail closed.
 */
export async function approvedConnectorsForLeg(
  store: ApprovalStore,
  workflowId: string,
  runId: string,
  leg: RunLeg,
): Promise<string[]> {
  // Retrieve ALL approved records for the run via the shared complete-reader —
  // its docstring carries the load-bearing workflowId/runId-predicate and
  // fail-closed paging rationale grants.test.ts pins with a spy store. Grant
  // derivation MUST see the complete history: the record matching the current
  // leg may be the newest, which a single bounded FIFO page would drop past the
  // cap and fail the grant closed.
  const approved = await listAllApprovedForRun(store, workflowId, runId);
  const targetKey =
    leg.kind === 'resume' && leg.step !== undefined
      ? stepKeyOf(leg.step)
      : undefined;
  const suspendedAt = leg.kind === 'resume' ? leg.suspendedAt : undefined;
  const resumeCount = leg.kind === 'resume' ? leg.resumeCount : undefined;
  const connectors = new Set<string>();
  for (const record of approved) {
    const recordKey = stepKeyOf(record.stepPath);
    const applies =
      recordKey === ''
        ? record.runScoped === true
        : targetKey !== undefined &&
          recordKey === targetKey &&
          boundToCurrentSuspension(record, suspendedAt, resumeCount);
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
 *
 * Takes a TENANT-BOUND store (the brand): a DO builds it as
 * `factory.forTenant(this.tenantId)` from its own identity. A
 * SystemApprovalStore is not assignable here — the type-level assertion in
 * grants.test.ts pins that.
 */
export function approvalGrantProvider(
  store: TenantBoundApprovalStore,
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
 * Factory-backed provider for hosts whose ONE runtime serves every tenant
 * in-process (the dev plugin; DO hosts bind per-instance instead). The
 * tenant is recovered from the leg's runId prefix — the same INV-1 carrier
 * the DO name join uses — and the store is bound per leg. A runId without a
 * valid tenant prefix mints an EMPTY grant list (fail closed), never a
 * cross-tenant read.
 */
export function approvalGrantProviderFromFactory(factory: {
  forTenant(tenantId: string): TenantBoundApprovalStore;
}): RequestContextProvider {
  return async (workflowId, runId, leg) => {
    const tenantId = tenantOfRunId(runId);
    if (tenantId === undefined) {
      return { [BREAKWATER_APPROVED_CONNECTORS_KEY]: [] };
    }
    const connectors = await approvedConnectorsForLeg(
      factory.forTenant(tenantId),
      workflowId,
      runId,
      leg,
    );
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
