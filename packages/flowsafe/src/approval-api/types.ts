// Approval-queue domain types. All timestamps are ISO 8601 strings — records
// travel over HTTP and in/out of D1, so the canonical representation is
// JSON-safe end to end.

export type ApprovalStatus =
  | 'pending'
  | 'claimed'
  | 'approved'
  | 'rejected'
  | 'escalated';

export type ApprovalDecision = 'approve' | 'reject';

export type ApprovalPriority = 'low' | 'normal' | 'high' | 'critical';

/**
 * Statuses that still await a decision. 'escalated' stays decidable —
 * escalation raises visibility, it does not close the request.
 */
export const OPEN_STATUSES: readonly ApprovalStatus[] = [
  'pending',
  'claimed',
  'escalated',
];

export const APPROVAL_PRIORITIES: readonly ApprovalPriority[] = [
  'low',
  'normal',
  'high',
  'critical',
];

export const APPROVAL_STATUSES: readonly ApprovalStatus[] = [
  'pending',
  'claimed',
  'approved',
  'rejected',
  'escalated',
];

export interface ApprovalRecord {
  id: string;
  /**
   * The owning tenant. STAMPED by the bound store from its own constructor
   * field — never accepted from input (CreateApprovalInput deliberately has
   * no tenantId: a field that cannot be supplied cannot be spoofed). Every
   * read/write predicate carries it (INV-2).
   */
  tenantId: string;
  workflowId: string;
  runId: string;
  /** Suspended step path this approval unblocks, e.g. ['approval']. */
  stepPath?: string[];
  title: string;
  summary?: string;
  /** Workflow-provided reviewer context (typically the suspend payload). */
  payload?: unknown;
  /**
   * Connector ids an approval grants (breakwater write gate). Recorded at
   * creation by trusted server-side code — the grant provider derives
   * requestContext grants from these on approved records only.
   */
  connectors: string[];
  priority: ApprovalPriority;
  status: ApprovalStatus;
  requestedBy?: string;
  claimedBy?: string;
  decidedBy?: string;
  decision?: ApprovalDecision;
  comment?: string;
  /** Latest delegation target (also reflected in claimedBy). */
  delegatedTo?: string;
  createdAt: string;
  updatedAt: string;
  claimedAt?: string;
  decidedAt?: string;
  escalatedAt?: string;
  slaDeadlineAt?: string;
  /**
   * Epoch-ms suspendedAt of the suspension this approval binds to (core
   * clock) — see CreateApprovalInput.suspendedAt. Grant minting requires an
   * EXACT match with the resumed leg's suspension timestamp when present,
   * paired with `resumeCount`.
   */
  suspendedAt?: number;
  /**
   * Epoch-ms resumedAt of that suspension (core clock). INFORMATIONAL audit
   * metadata only — Mastra stamps it solely on a payload-bearing resume, so it
   * is NOT the grant tie-breaker (that is `resumeCount`).
   */
  resumedAt?: number;
  /**
   * Runtime-owned monotonic resume ordinal of that suspension — undefined for
   * a step's FIRST suspension, 1,2,… on successive re-suspensions. Pairs with
   * `suspendedAt` in the exact grant binding as the collision-free
   * tie-breaker: unlike `resumedAt` the runtime increments it on every resume
   * regardless of payload, so two same-step suspensions stay distinguishable
   * even when their `suspendedAt` collide within a millisecond.
   */
  resumeCount?: number;
  /**
   * Explicit run-scoped standing grant: a step-less record mints its
   * connectors on EVERY leg of the run. Opt-in only — a step-less record
   * WITHOUT this flag mints nothing, because "absent field => maximal
   * privilege" is an inverted default. Suspend-observation bridges must never
   * set this (they always carry a stepPath); only trusted code that
   * deliberately wants a run-wide capability does.
   */
  runScoped?: boolean;
}

export interface CreateApprovalInput {
  workflowId: string;
  runId: string;
  stepPath?: string[];
  title: string;
  summary?: string;
  payload?: unknown;
  connectors?: string[];
  priority?: ApprovalPriority;
  /** Seconds from creation to the SLA deadline; overrides the service default. */
  slaSeconds?: number;
  requestedBy?: string;
  /**
   * Epoch-ms suspendedAt of the suspension this approval binds to, observed
   * from RunSummary.suspendedAt by the creating bridge (core clock, so grant
   * minting is clock-free: mint requires record.suspendedAt to EXACTLY match
   * the resumed leg's suspension). Step-keyed approvals created without it
   * fall back to the same-clock decidedAt-after-suspension comparison.
   */
  suspendedAt?: number;
  /**
   * Epoch-ms resumedAt of that suspension, observed from RunSummary.resumedAt
   * by the same bridge. INFORMATIONAL only — not the grant tie-breaker.
   */
  resumedAt?: number;
  /**
   * Runtime resume ordinal of that suspension, observed from
   * RunSummary.resumeCount by the same bridge — undefined for a first
   * suspension, 1,2,… on re-suspensions. Pairs with `suspendedAt` as the
   * collision-free grant-binding tie-breaker.
   */
  resumeCount?: number;
  /**
   * Opt in to a run-scoped standing grant (mints on every leg). Create-time
   * only — see ApprovalRecord.runScoped. Never settable over HTTP.
   */
  runScoped?: boolean;
}

export interface ApprovalListFilter {
  status?: ApprovalStatus | ApprovalStatus[];
  workflowId?: string;
  runId?: string;
  claimedBy?: string;
}

export interface ApprovalMetrics {
  /** Requests still awaiting a decision (pending | claimed | escalated). */
  openCount: number;
  /** Open requests past their SLA deadline (swept into 'escalated' or not yet). */
  slaBreachedCount: number;
  /** Requests that were ever escalated, regardless of current status. */
  escalationCount: number;
  decidedCount: number;
  approvedCount: number;
  rejectedCount: number;
  /** Mean createdAt→decidedAt seconds across decided requests; null when none decided. */
  avgResolutionSeconds: number | null;
}

/** Outcome of the resume attempt a decision triggers. */
export interface ResumeOutcome {
  /** false when no resumeRun is wired — the caller resumes separately. */
  attempted: boolean;
  ok?: boolean;
  /** RunSummary (or whatever resumeRun returns) on success. */
  summary?: unknown;
  error?: string;
}

export interface DecideResult {
  record: ApprovalRecord;
  /**
   * The decision is durable regardless of this outcome — a failed resume is
   * retryable (the run stays suspended; grants derive from the store).
   */
  resume: ResumeOutcome;
}
