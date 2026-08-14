// SPDX-License-Identifier: Apache-2.0
// ApprovalStore — the persistence contract for the approval queue.
//
// Every state change goes through transition(), a compare-and-swap guarded by
// the record's CURRENT status: racing writers (two reviewers deciding, a
// sweep racing a claim) resolve to exactly one winner; losers get null and
// surface as HTTP 409. There is deliberately no unconditional update.

import {
  type ApprovalListFilter,
  type ApprovalMetrics,
  type ApprovalRecord,
  type ApprovalStatus,
  approvalCursor,
  approvalListOrder,
  assertApprovalTimeBounds,
  byReviewerOrder,
  clampApprovalLimit,
  compareStrings,
  MAX_APPROVAL_LIST_LIMIT,
  OPEN_STATUSES,
  parseApprovalCursor,
  parseApprovalTimeBound,
  TERMINAL_APPROVAL_STATUSES,
} from './types.js';

/** Uniqueness scope for open requests: one per (workflowId, runId, stepKey). */
export function stepKeyOf(stepPath: string[] | undefined): string {
  return stepPath?.join('.') ?? '';
}

export interface CreateResult {
  record: ApprovalRecord;
  /**
   * false when an open request for the same (workflowId, runId, stepKey), or
   * any record for the same captured suspension fingerprint, already existed
   * and was returned instead.
   */
  created: boolean;
}

/**
 * Fields transition() may change. updatedAt is mandatory — every transition
 * is a write and must say when.
 */
export interface ApprovalPatch {
  status?: ApprovalStatus;
  claimedBy?: string;
  decidedBy?: string;
  decision?: ApprovalRecord['decision'];
  comment?: string;
  delegatedTo?: string;
  updatedAt: string;
  claimedAt?: string;
  decidedAt?: string;
  escalatedAt?: string;
}

export interface ApprovalTransitionOptions {
  /** Human decisions must lose their CAS once the run has a terminal fence. */
  requireRunDecidable?: boolean;
}

export interface ApprovalStore {
  /**
   * Insert — unless an OPEN request (pending | claimed | escalated) already
   * exists for the same (workflowId, runId, stepKey), or any record already
   * carries the same captured (stepPath, suspendedAt, resumeCount)
   * fingerprint. The existing record is returned with created: false, so a
   * stale reconciler cannot file over a decision that landed after its history
   * read. A later re-suspension changes the fingerprint and opens fresh.
   */
  create(record: ApprovalRecord): Promise<CreateResult>;
  get(id: string): Promise<ApprovalRecord | null>;
  /**
   * Ordered per filter.orderBy: oldest-first (createdAt, then id — FIFO
   * queue order) by default, or the reviewer queue order (priority → SLA →
   * FIFO, byReviewerOrder) under 'reviewer' — applied BEFORE filter.limit,
   * so a bounded page is the top of the reviewer queue. Bounded by
   * filter.limit/filter.after; a bare list() with no limit defaults to
   * MAX_APPROVAL_LIST_LIMIT so repeated dashboard polls cannot trigger
   * unbounded full-table scans. Page complete history with an explicit
   * `after` cursor.
   */
  list(filter?: ApprovalListFilter): Promise<ApprovalRecord[]>;
  /**
   * Compare-and-swap: apply patch iff the current status is in `from`.
   * Returns the updated record, or null when the guard fails (the status
   * moved concurrently, or the id is unknown — callers disambiguate via
   * get()).
   */
  transition(
    id: string,
    from: readonly ApprovalStatus[],
    patch: ApprovalPatch,
    options?: ApprovalTransitionOptions,
  ): Promise<ApprovalRecord | null>;
  /**
   * Aggregate queue metrics — field semantics on ApprovalMetrics (types.ts),
   * computed here instead of requiring callers to load every record into
   * JavaScript. `nowMs` is the SLA-breach reference instant (the service's
   * injected clock), keeping the computation deterministic under tests.
   */
  metrics(nowMs: number): Promise<ApprovalMetrics>;
  /** Delete terminal records older than cutoffIso, bounded by limit. */
  purgeExpired(cutoffIso: string, limit: number): Promise<number>;
}

/**
 * Drain the COMPLETE approved history of one run by explicit after-cursor
 * paging — the shared complete-internal-reader that BOTH grant derivation
 * (connectorGrantsForLeg, grants.ts) and the cross-gate SoD bar
 * (ApprovalService.decide) depend on, so the two can never drift. A single
 * default-bounded page is insufficient for either: a bare list() is capped at
 * MAX_APPROVAL_LIST_LIMIT, so a many-gate run's newest approvals sit past the
 * first page under FIFO 'created' order. Dropping them fails the grant closed
 * and the separation-of-duties check open. Both callers are fail-closed
 * complete readers, so this pages to exhaustion. The `workflowId`+`runId`
 * predicates are load-bearing and must never be "optimized away": grant
 * derivation must read only the named run even though the deployment store
 * contains every run in the organization. grants.test.ts pins them on every
 * page with a spy store.
 * 'created' FIFO is the ONLY after-cursor-compatible order (approvalListOrder
 * rejects 'reviewer' + after); in practice one run's approved records sit far
 * under the cap, so this is a single page.
 */
export async function listAllApprovedForRun(
  store: ApprovalStore,
  workflowId: string,
  runId: string,
): Promise<ApprovalRecord[]> {
  const approved: ApprovalRecord[] = [];
  let after: string | undefined;
  for (;;) {
    const page = await store.list({
      workflowId,
      runId,
      status: 'approved',
      limit: MAX_APPROVAL_LIST_LIMIT,
      after,
    });
    approved.push(...page);
    const last = page.at(-1);
    if (page.length < MAX_APPROVAL_LIST_LIMIT || !last) break;
    after = approvalCursor(last);
  }
  return approved;
}

/**
 * The filter predicate shared by store implementations. Internal to
 * approval-api — deliberately not on the package barrel.
 */
export function matchesFilter(
  record: ApprovalRecord,
  filter: ApprovalListFilter,
): boolean {
  if (filter.status !== undefined) {
    const statuses = Array.isArray(filter.status)
      ? filter.status
      : [filter.status];
    if (!statuses.includes(record.status)) return false;
  }
  if (
    filter.workflowId !== undefined &&
    record.workflowId !== filter.workflowId
  )
    return false;
  if (filter.runId !== undefined && record.runId !== filter.runId) return false;
  if (filter.claimedBy !== undefined && record.claimedBy !== filter.claimedBy)
    return false;
  if (
    filter.requestedBy !== undefined &&
    record.requestedBy !== filter.requestedBy
  )
    return false;
  // Chronological (parsed), not bytewise: the filter value is caller-formatted
  // ISO and may not match the column's canonical toISOString() form. Strict
  // bounds; parseApprovalTimeBound throws on garbage (router 400s it first).
  if (
    filter.createdBefore !== undefined &&
    !(
      Date.parse(record.createdAt) <
      parseApprovalTimeBound(filter.createdBefore, 'createdBefore')
    )
  )
    return false;
  if (
    filter.createdAfter !== undefined &&
    !(
      Date.parse(record.createdAt) >
      parseApprovalTimeBound(filter.createdAfter, 'createdAfter')
    )
  )
    return false;
  return true;
}

/**
 * FIFO queue order (createdAt, then id) — shared by both store views.
 * Bytewise via the shared compareStrings (NOT localeCompare): D1 orders with
 * BINARY collation and the cursor row-value comparison in appendListFilters
 * compares bytewise too, so a locale-collated in-memory sort would disagree
 * with D1 on an id tie-break (mixed-case ids) and with its own cursor
 * filter.
 */
export function byQueueOrder(a: ApprovalRecord, b: ApprovalRecord): number {
  return compareStrings(a.createdAt, b.createdAt) || compareStrings(a.id, b.id);
}

/**
 * The comparator for a filter's effective orderBy — shared by both in-memory
 * list() implementations, same no-drift rationale as matchesFilter. Also the
 * chokepoint where the reviewer/after incoherence throws (approvalListOrder);
 * D1's list() resolves through the same function when building its ORDER BY.
 */
export function approvalListComparator(
  filter: ApprovalListFilter,
): (a: ApprovalRecord, b: ApprovalRecord) => number {
  return approvalListOrder(filter) === 'reviewer'
    ? byReviewerOrder
    : byQueueOrder;
}

/**
 * Cursor + limit paging over an already status/workflow/run/claimedBy-
 * filtered, orderBy-sorted record array. `after` only ever reaches this under
 * default FIFO order (approvalListOrder rejects it under 'reviewer' before
 * any sort happens). D1's list() applies the equivalent conditions in SQL
 * (appendListFilters' cursor clause + a LIMIT, d1-store.ts) instead of
 * calling this.
 */
export function paginateApprovalList(
  records: readonly ApprovalRecord[],
  filter: Pick<ApprovalListFilter, 'after' | 'limit'>,
): ApprovalRecord[] {
  let page: readonly ApprovalRecord[] = records;
  if (filter.after !== undefined) {
    const cursor = parseApprovalCursor(filter.after);
    page = page.filter(
      (record) =>
        record.createdAt > cursor.createdAt ||
        (record.createdAt === cursor.createdAt && record.id > cursor.id),
    );
  }
  const limit = clampApprovalLimit(filter.limit);
  return limit !== undefined ? page.slice(0, limit) : [...page];
}

/**
 * The in-memory metrics reduction — ALSO the reference "old JS computation"
 * the D1 aggregate SQL (d1-store.ts's METRICS_QUERY) must match; see
 * store.test.ts's cross-backend parity case. Field semantics: ApprovalMetrics
 * (types.ts).
 */
export function computeApprovalMetrics(
  records: readonly ApprovalRecord[],
  nowMs: number,
): ApprovalMetrics {
  const open = records.filter((record) =>
    OPEN_STATUSES.includes(record.status),
  );
  const decided = records.filter(
    (record) => record.status === 'approved' || record.status === 'rejected',
  );
  const resolutionsSeconds = decided
    .filter((record) => record.decidedAt !== undefined)
    .map(
      (record) =>
        (Date.parse(record.decidedAt as string) -
          Date.parse(record.createdAt)) /
        1000,
    );
  return {
    openCount: open.length,
    slaBreachedCount: open.filter(
      (record) =>
        record.slaDeadlineAt !== undefined &&
        Date.parse(record.slaDeadlineAt) <= nowMs,
    ).length,
    escalationCount: records.filter(
      (record) => record.escalatedAt !== undefined,
    ).length,
    decidedCount: decided.length,
    approvedCount: decided.filter((record) => record.status === 'approved')
      .length,
    rejectedCount: decided.filter((record) => record.status === 'rejected')
      .length,
    avgResolutionSeconds:
      resolutionsSeconds.length > 0
        ? resolutionsSeconds.reduce((sum, value) => sum + value, 0) /
          resolutionsSeconds.length
        : null,
  };
}

// Records carry caller-provided payloads; clone on every boundary so callers
// can never mutate stored state (or observe another caller's mutations).
function clone(record: ApprovalRecord): ApprovalRecord {
  return structuredClone(record);
}

/**
 * In-memory reference implementation. Mirrors D1ApprovalStore semantics and
 * keeps CAS atomic because each check-and-mutate section is synchronous (no
 * awaits inside).
 */
export class InMemoryApprovalStore implements ApprovalStore {
  readonly #records: Map<string, ApprovalRecord>;

  constructor(records?: Map<string, ApprovalRecord>) {
    this.#records = records ?? new Map();
  }

  async create(record: ApprovalRecord): Promise<CreateResult> {
    const snapshot = clone(record);
    const key = stepKeyOf(snapshot.stepPath);
    if (snapshot.stepPath !== undefined && snapshot.suspendedAt !== undefined) {
      const fingerprint = [...this.#records.values()].filter(
        (existing) =>
          existing.workflowId === snapshot.workflowId &&
          existing.runId === snapshot.runId &&
          stepKeyOf(existing.stepPath) === key &&
          existing.stepPath !== undefined &&
          existing.suspendedAt === snapshot.suspendedAt &&
          existing.resumeCount === snapshot.resumeCount,
      );
      if (fingerprint.length > 1) {
        throw new Error(
          `approval suspension fingerprint has ${fingerprint.length} existing records`,
        );
      }
      if (fingerprint[0]) {
        return { record: clone(fingerprint[0]), created: false };
      }
    }
    for (const existing of this.#records.values()) {
      if (
        existing.workflowId === snapshot.workflowId &&
        existing.runId === snapshot.runId &&
        stepKeyOf(existing.stepPath) === key &&
        OPEN_STATUSES.includes(existing.status)
      ) {
        return { record: clone(existing), created: false };
      }
    }
    if (this.#records.has(snapshot.id)) {
      throw new Error(`approval id '${snapshot.id}' already exists`);
    }
    this.#records.set(snapshot.id, snapshot);
    return { record: clone(snapshot), created: true };
  }

  async get(id: string): Promise<ApprovalRecord | null> {
    const record = this.#records.get(id);
    return record ? clone(record) : null;
  }

  async list(filter: ApprovalListFilter = {}): Promise<ApprovalRecord[]> {
    // Eagerly validate the time bounds so an unparseable createdBefore/
    // createdAfter throws even when ZERO records reach matchesFilter (a
    // zero-match store never enters the per-record predicate). D1's
    // appendListFilters validates unconditionally; this assertion keeps the
    // in-memory path failing identically to D1.
    // Fail-closed: a garbage bound errors, never a silently empty page.
    assertApprovalTimeBounds(filter);
    const matched = [...this.#records.values()]
      .filter((record) => matchesFilter(record, filter))
      .sort(approvalListComparator(filter));
    // D3: a bare list() defaults to MAX_APPROVAL_LIST_LIMIT so a repeated
    // dashboard poll can never fall back to an unbounded scan.
    return paginateApprovalList(matched, {
      after: filter.after,
      limit: filter.limit ?? MAX_APPROVAL_LIST_LIMIT,
    }).map(clone);
  }

  async metrics(nowMs: number): Promise<ApprovalMetrics> {
    return computeApprovalMetrics([...this.#records.values()], nowMs);
  }

  async transition(
    id: string,
    from: readonly ApprovalStatus[],
    patch: ApprovalPatch,
    _options?: ApprovalTransitionOptions,
  ): Promise<ApprovalRecord | null> {
    const current = this.#records.get(id);
    if (!current || !from.includes(current.status)) return null;
    const updated: ApprovalRecord = { ...current };
    for (const [field, value] of Object.entries(patch)) {
      if (value !== undefined) {
        (updated as unknown as Record<string, unknown>)[field] = value;
      }
    }
    this.#records.set(id, updated);
    return clone(updated);
  }

  async purgeExpired(cutoffIso: string, limit: number): Promise<number> {
    let purged = 0;
    for (const [id, record] of this.#records) {
      if (purged >= limit) break;
      if (!TERMINAL_APPROVAL_STATUSES.includes(record.status)) continue;
      if ((record.decidedAt ?? record.updatedAt) < cutoffIso) {
        this.#records.delete(id);
        purged += 1;
      }
    }
    return purged;
  }
}
