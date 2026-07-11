// SPDX-License-Identifier: Apache-2.0
// ApprovalStore — the persistence contract for the approval queue.
//
// Every state change goes through transition(), a compare-and-swap guarded by
// the record's CURRENT status: racing writers (two reviewers deciding, a
// sweep racing a claim) resolve to exactly one winner; losers get null and
// surface as HTTP 409. There is deliberately no unconditional update.

import { TENANT_ID_PATTERN } from '../do-runner/path-safe-id.js';
import { TENANT_BOUND } from './tenant-brand.js';
import {
  type ApprovalListFilter,
  type ApprovalMetrics,
  type ApprovalRecord,
  type ApprovalStatus,
  approvalListOrder,
  byReviewerOrder,
  clampApprovalLimit,
  compareStrings,
  MAX_APPROVAL_LIST_LIMIT,
  OPEN_STATUSES,
  parseApprovalCursor,
  parseApprovalTimeBound,
} from './types.js';

/** Uniqueness scope for open requests: one per (workflowId, runId, stepKey). */
export function stepKeyOf(stepPath: string[] | undefined): string {
  return stepPath?.join('.') ?? '';
}

export interface CreateResult {
  record: ApprovalRecord;
  /** false when an open request for the same (workflowId, runId, stepKey) already existed and was returned instead. */
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

export interface ApprovalStore {
  /**
   * Insert — unless an OPEN request (pending | claimed | escalated) already
   * exists for the same (workflowId, runId, stepKey); then that record is
   * returned with created: false (idempotent create, so the runner glue can
   * fire on every suspend observation without duplicating queue entries).
   * Decided requests never block a new one: a later re-suspension of the
   * same step opens a fresh approval.
   */
  create(record: ApprovalRecord): Promise<CreateResult>;
  get(id: string): Promise<ApprovalRecord | null>;
  /**
   * Ordered per filter.orderBy: oldest-first (createdAt, then id — FIFO
   * queue order) by default, or the reviewer queue order (priority → SLA →
   * FIFO, byReviewerOrder) under 'reviewer' — applied BEFORE filter.limit,
   * so a bounded page is the top of the reviewer queue. Bounded by
   * filter.limit/filter.after; a tenant-bound bare list() with no limit
   * defaults to MAX_APPROVAL_LIST_LIMIT (D3: an unbounded list() repeated on
   * every dashboard poll is a full-table scan), so page complete history with
   * an explicit `after` cursor. The cron-only SystemApprovalStore view stays
   * complete (no default) for reconciliation and the SLA sweep.
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
  ): Promise<ApprovalRecord | null>;
  /**
   * Aggregate queue metrics for this store's tenant — field semantics on
   * ApprovalMetrics (types.ts), computed here instead of by the caller
   * loading every record into JS (D3: metrics() used to do exactly that).
   * `nowMs` is the SLA-breach reference instant (the service's injected
   * clock), keeping the computation deterministic under tests.
   */
  metrics(nowMs: number): Promise<ApprovalMetrics>;
}

/**
 * The filter predicate, shared by the tenant-bound store and the cron-only
 * system view (tenant-store.ts). Internal to approval-api — deliberately not
 * on the package barrel; the two views must never drift.
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
 * filtered, orderBy-sorted record array — shared by both in-memory
 * list() implementations (the tenant-bound store below and the system view
 * in tenant-store.ts) so they can never drift, same rationale as
 * matchesFilter/byQueueOrder. `after` only ever reaches this under the
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
 * In-memory reference implementation, BOUND to one tenant at construction
 * (INV-2). Mirrors D1ApprovalStore semantics — both run the same contract
 * test suite, including the cross-tenant cases over ONE shared backend (pass
 * the same `records` Map to two instances; the InMemoryApprovalStoreFactory
 * does exactly that). CAS atomicity holds because the check-and-mutate
 * section is synchronous (no awaits inside).
 *
 * Every read/write carries the tenant predicate sourced from the constructor
 * field — never from a parameter — and create() STAMPS the tenant, so no
 * caller (and no spoofed input) can write another tenant's row.
 */
export class InMemoryApprovalStore implements ApprovalStore {
  readonly tenantId: string;
  readonly [TENANT_BOUND] = true as const;
  readonly #records: Map<string, ApprovalRecord>;

  constructor(tenantId: string, records?: Map<string, ApprovalRecord>) {
    if (!TENANT_ID_PATTERN.test(tenantId)) {
      throw new Error(
        `InMemoryApprovalStore: tenantId '${tenantId}' violates INV-3 (^[a-z0-9]{3,32}$)`,
      );
    }
    this.tenantId = tenantId;
    this.#records = records ?? new Map();
  }

  #owns(record: ApprovalRecord | undefined): record is ApprovalRecord {
    return record !== undefined && record.tenantId === this.tenantId;
  }

  async create(record: ApprovalRecord): Promise<CreateResult> {
    // Stamp, never trust: the bound store is the tenant authority.
    const stamped: ApprovalRecord = { ...record, tenantId: this.tenantId };
    const key = stepKeyOf(stamped.stepPath);
    for (const existing of this.#records.values()) {
      if (
        existing.tenantId === this.tenantId &&
        existing.workflowId === stamped.workflowId &&
        existing.runId === stamped.runId &&
        stepKeyOf(existing.stepPath) === key &&
        OPEN_STATUSES.includes(existing.status)
      ) {
        return { record: clone(existing), created: false };
      }
    }
    if (this.#records.has(stamped.id)) {
      throw new Error(`approval id '${stamped.id}' already exists`);
    }
    this.#records.set(stamped.id, clone(stamped));
    return { record: clone(stamped), created: true };
  }

  async get(id: string): Promise<ApprovalRecord | null> {
    const record = this.#records.get(id);
    return this.#owns(record) ? clone(record) : null;
  }

  async list(filter: ApprovalListFilter = {}): Promise<ApprovalRecord[]> {
    const matched = [...this.#records.values()]
      .filter(
        (record) =>
          record.tenantId === this.tenantId && matchesFilter(record, filter),
      )
      .sort(approvalListComparator(filter));
    // D3: a tenant-bound bare list() defaults to MAX_APPROVAL_LIST_LIMIT so a
    // repeated dashboard poll can never fall back to an unbounded scan. The
    // default lives HERE, not in paginateApprovalList — the cron-only system
    // view (tenant-store.ts) shares that helper and must stay complete.
    return paginateApprovalList(matched, {
      after: filter.after,
      limit: filter.limit ?? MAX_APPROVAL_LIST_LIMIT,
    }).map(clone);
  }

  async metrics(nowMs: number): Promise<ApprovalMetrics> {
    const owned = [...this.#records.values()].filter(
      (record) => record.tenantId === this.tenantId,
    );
    return computeApprovalMetrics(owned, nowMs);
  }

  async transition(
    id: string,
    from: readonly ApprovalStatus[],
    patch: ApprovalPatch,
  ): Promise<ApprovalRecord | null> {
    const current = this.#records.get(id);
    // A wrong-tenant id behaves exactly like an unknown id (null -> 404/409
    // disambiguation upstream) — no oracle for other tenants' record ids.
    if (!this.#owns(current) || !from.includes(current.status)) return null;
    const updated: ApprovalRecord = { ...current };
    for (const [field, value] of Object.entries(patch)) {
      if (value !== undefined) {
        (updated as unknown as Record<string, unknown>)[field] = value;
      }
    }
    this.#records.set(id, updated);
    return clone(updated);
  }
}
