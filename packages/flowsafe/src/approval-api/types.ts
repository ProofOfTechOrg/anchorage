// SPDX-License-Identifier: Apache-2.0
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

/**
 * The approval lifecycle's closed states — the complement of OPEN_STATUSES.
 * Once decided, a record never reopens (a later re-suspension of the same
 * step files a FRESH record — see store.ts's open-uniqueness index). Drives
 * the retention purge (retention.ts): only these statuses are ever eligible.
 */
export const TERMINAL_APPROVAL_STATUSES: readonly ApprovalStatus[] = [
  'approved',
  'rejected',
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

/** Trusted host topology used to resume a durable agent after DO eviction. */
export interface ApprovalResumeTarget {
  kind: 'thread';
  threadId: string;
  resourceId?: string;
}

export interface ApprovalRecord {
  id: string;
  /**
   * The owning tenant. STAMPED by the bound store from its own constructor
   * field — never accepted from input (CreateApprovalInput deliberately has
   * no tenantId: a field that cannot be supplied cannot be spoofed). Every
   * read/write predicate carries it.
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
  /** Server-authored resume topology. Never accepted from the HTTP create body. */
  resumeTarget?: ApprovalResumeTarget;
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

/** List orderings accepted by ApprovalListFilter.orderBy. */
export const APPROVAL_LIST_ORDERS = ['created', 'reviewer'] as const;

export type ApprovalListOrder = (typeof APPROVAL_LIST_ORDERS)[number];

export interface ApprovalListFilter {
  status?: ApprovalStatus | ApprovalStatus[];
  workflowId?: string;
  runId?: string;
  claimedBy?: string;
  /** Exact match on ApprovalRecord.requestedBy (triage: "everything Ada's runs asked for"). */
  requestedBy?: string;
  /**
   * ISO-8601 bound: matches records whose createdAt is strictly BEFORE this
   * instant (triage: "older than 4 hours"). Compared chronologically — any
   * parseable ISO variant works; an unparseable value throws (router.ts maps
   * it to 400 before a store sees it). Distinct from the `after` CURSOR,
   * which pages queue position, not creation time.
   */
  createdBefore?: string;
  /** ISO-8601 bound: matches records whose createdAt is strictly AFTER this instant. */
  createdAfter?: string;
  /**
   * Max records to return (clamped to [1, MAX_APPROVAL_LIST_LIMIT] — see
   * clampApprovalLimit). undefined requests no explicit limit: a tenant-bound
   * store then defaults to MAX_APPROVAL_LIST_LIMIT so a bare list() never
   * becomes an unbounded scan, while the cron-only SystemApprovalStore view
   * stays complete. Page complete history with an explicit `after` cursor.
   */
  limit?: number;
  /**
   * Opaque pagination cursor from approvalCursor() — resume list() strictly
   * after that record in queue order (createdAt, then id). undefined starts
   * from the beginning.
   */
  after?: string;
  /**
   * Result ordering. 'created' (default) is FIFO — (createdAt, id), the
   * order `after` cursors page over. 'reviewer' is the dashboard queue
   * order (byReviewerOrder: priority, then nearest SLA deadline, then
   * FIFO), applied BEFORE `limit` so a bounded page is the TOP of the
   * reviewer queue — under FIFO-then-limit, a fresh critical request
   * beyond the oldest `limit` records was invisible to a bounded dashboard
   * poll (2026-07-11 review). Incompatible with `after`; see
   * approvalListOrder.
   */
  orderBy?: ApprovalListOrder;
}

/**
 * Resolves ApprovalListFilter.orderBy to its effective value — 'created'
 * unless set — and rejects the one incoherent combination: an `after` cursor
 * encodes a position in (createdAt, id) order, where new records only ever
 * land PAST the cursor; under 'reviewer' order they insert anywhere, so
 * cursor pages would silently skip or repeat records. Every list()
 * implementation (both backends, both tenant views) resolves through here;
 * router.ts maps the throw to a 400 before a store sees the filter.
 */
export function approvalListOrder(
  filter: Pick<ApprovalListFilter, 'after' | 'orderBy'>,
): ApprovalListOrder {
  const orderBy = filter.orderBy ?? 'created';
  if (orderBy === 'reviewer' && filter.after !== undefined) {
    throw new Error(
      "orderBy 'reviewer' cannot be combined with an 'after' cursor — cursors page FIFO (createdAt, id) order only",
    );
  }
  return orderBy;
}

const REVIEWER_PRIORITY_RANK: Record<ApprovalPriority, number> = {
  critical: 0,
  high: 1,
  normal: 2,
  low: 3,
};

// D1 casts the priority TEXT column straight into the union, so the lookup
// must tolerate an out-of-enum value at runtime: it ranks after 'low',
// exactly like the SQL CASE's ELSE arm in d1-store.ts.
function reviewerPriorityRank(priority: ApprovalPriority): number {
  return (
    (REVIEWER_PRIORITY_RANK as Partial<Record<string, number>>)[priority] ?? 4
  );
}

// Bytewise, locale-independent — equals SQLite TEXT comparison, and equals
// chronological order for the fixed-format ISO-8601 stamps. Exported (not
// barreled) so store.ts's byQueueOrder shares the SAME collation: one
// divergent localeCompare would let the in-memory and D1 backends disagree
// on an id tie-break (concrete on mixed-case ids — 'B' < 'a' bytewise but
// 'a' < 'B' in a locale collation).
export function compareStrings(a: string, b: string): number {
  if (a === b) return 0;
  return a < b ? -1 : 1;
}

/**
 * Reviewer-facing queue order: priority first, then nearest SLA deadline
 * (records without one last), then FIFO (createdAt, id). THE definition of
 * orderBy: 'reviewer' — the dashboard's sortQueue (approval-ui/view-model.ts)
 * delegates here and d1-store.ts mirrors it in SQL (store.test.ts pins the
 * cross-backend parity), so a bounded server page and the client-side sort
 * can never rank differently.
 */
export function byReviewerOrder(a: ApprovalRecord, b: ApprovalRecord): number {
  const byPriority =
    reviewerPriorityRank(a.priority) - reviewerPriorityRank(b.priority);
  if (byPriority !== 0) return byPriority;
  if (a.slaDeadlineAt !== b.slaDeadlineAt) {
    if (a.slaDeadlineAt === undefined) return 1;
    if (b.slaDeadlineAt === undefined) return -1;
    return compareStrings(a.slaDeadlineAt, b.slaDeadlineAt);
  }
  return compareStrings(a.createdAt, b.createdAt) || compareStrings(a.id, b.id);
}

/** Hard cap on ApprovalListFilter.limit — see clampApprovalLimit. */
export const MAX_APPROVAL_LIST_LIMIT = 500;

/**
 * Clamps a caller-supplied ApprovalListFilter.limit into
 * [1, MAX_APPROVAL_LIST_LIMIT]. undefined passes through unchanged (no
 * limit was requested at all). This is store-level defense in depth: the
 * HTTP boundary (router.ts) rejects a non-integer or out-of-range limit
 * outright (400) rather than silently clamping it, so a client typo is a
 * loud error, not a surprising page size — this clamp exists for
 * programmatic callers (in-process filters) so a pathological value can
 * NEVER produce an unbounded query. That guarantee has to hold for
 * non-finite input too: NaN and +/-Infinity clamp to MAX_APPROVAL_LIST_LIMIT
 * (not to "no limit") — a caller that derives a limit from arithmetic that
 * can divide by zero (e.g. a per-page budget) must still get a bounded
 * query, not silently reopen the unbounded scan this function exists to
 * prevent.
 */
export function clampApprovalLimit(
  limit: number | undefined,
): number | undefined {
  if (limit === undefined) return undefined;
  if (!Number.isFinite(limit)) return MAX_APPROVAL_LIST_LIMIT;
  return Math.min(MAX_APPROVAL_LIST_LIMIT, Math.max(1, Math.trunc(limit)));
}

export interface ApprovalCursor {
  createdAt: string;
  id: string;
}

/**
 * Opaque list() pagination cursor: base64 of `${createdAt}|${id}` — the
 * exact (createdAt, id) tuple both stores order list() by (FIFO queue
 * order — byQueueOrder in store.ts). A caller derives the next page's
 * cursor from the last record of the current page and passes it as
 * ApprovalListFilter.after; round-trips through parseApprovalCursor(). Both
 * fields are ISO-8601/UUID-shaped and never contain '|', so the encoding is
 * lossless. btoa/atob (not a bundled base64 lib) because they are ambient
 * on Node >=16, Workers, and browsers alike — the same DOM-free-but-
 * web-standard posture client.ts already relies on for fetch.
 */
export function approvalCursor(
  record: Pick<ApprovalRecord, 'createdAt' | 'id'>,
): string {
  return btoa(`${record.createdAt}|${record.id}`);
}

/**
 * Throws a plain Error (router.ts maps it to 400) when `cursor` is not a
 * validly-shaped approvalCursor().
 */
export function parseApprovalCursor(cursor: string): ApprovalCursor {
  let decoded: string;
  try {
    decoded = atob(cursor);
  } catch {
    throw new Error(`invalid approval cursor: '${cursor}'`);
  }
  const separator = decoded.indexOf('|');
  if (separator <= 0 || separator === decoded.length - 1) {
    throw new Error(`invalid approval cursor: '${cursor}'`);
  }
  return {
    createdAt: decoded.slice(0, separator),
    id: decoded.slice(separator + 1),
  };
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

/**
 * Parses an ISO-8601 time-bound filter value (createdBefore/createdAfter) to
 * epoch ms. Throws a plain Error on an unparseable stamp — router.ts maps it
 * to 400 eagerly (the cursor convention), and BOTH store backends resolve
 * bounds through here so they fail identically instead of one silently
 * matching nothing while the other errors.
 */
export function parseApprovalTimeBound(value: string, field: string): number {
  const ms = Date.parse(value);
  if (Number.isNaN(ms)) {
    throw new Error(
      `${field} is not a parseable ISO-8601 timestamp: '${value}'`,
    );
  }
  return ms;
}

/**
 * Eagerly validates BOTH time-bound filters (createdBefore/createdAfter) when
 * present, discarding the parsed instants — the call is purely for
 * parseApprovalTimeBound's throw-on-garbage side effect. Both in-memory list
 * paths (InMemoryApprovalStore.list and the cron-only system view) call this up
 * front, BEFORE filtering, so a zero-match view rejects an unparseable bound
 * identically to D1's unconditional appendListFilters instead of silently
 * returning [] — single-sourcing the "both backends fail identically" contract
 * across both stores. Fail-closed: a garbage bound errors, never a silently
 * empty page.
 */
export function assertApprovalTimeBounds(
  filter: Pick<ApprovalListFilter, 'createdBefore' | 'createdAfter'>,
): void {
  if (filter.createdBefore !== undefined)
    parseApprovalTimeBound(filter.createdBefore, 'createdBefore');
  if (filter.createdAfter !== undefined)
    parseApprovalTimeBound(filter.createdAfter, 'createdAfter');
}

/**
 * Hard cap on ids per decideBatch call — see ApprovalService.decideBatch.
 * Sized against the Workers request ceilings, not taste: each decided record
 * costs a store CAS plus (when resumeRun is wired, as in every DO host) a
 * cross-Worker resume fetch, sequentially — so the cap bounds subrequests
 * and request duration. Bump deliberately, with those ceilings in mind.
 */
export const MAX_APPROVAL_BATCH_DECIDE = 100;

/**
 * Per-record outcome of a batch decide. `ok: false` carries the reason —
 * `code` mirrors the HTTP status the same failure would produce on the
 * single-record decide route ('not-found' 404, 'conflict' 409, 'forbidden'
 * 403 — separation of duties, 'invalid' 400, 'error' 500).
 */
export interface BatchDecideItem {
  id: string;
  ok: boolean;
  record?: ApprovalRecord;
  resume?: ResumeOutcome;
  error?: string;
  code?: 'not-found' | 'conflict' | 'forbidden' | 'invalid' | 'error';
}

/**
 * Envelope of ApprovalService.decideBatch: per-record fan-out results plus
 * the tallies. Partial failure lives IN the envelope (HTTP 200), never as a
 * response status — each record's CAS decided independently.
 */
export interface BatchDecideResult {
  results: BatchDecideItem[];
  decided: number;
  failed: number;
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
