// ApprovalStore — the persistence contract for the approval queue.
//
// Every state change goes through transition(), a compare-and-swap guarded by
// the record's CURRENT status: racing writers (two reviewers deciding, a
// sweep racing a claim) resolve to exactly one winner; losers get null and
// surface as HTTP 409. There is deliberately no unconditional update.

import {
  type ApprovalListFilter,
  type ApprovalRecord,
  type ApprovalStatus,
  OPEN_STATUSES,
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
  /** Ordered oldest-first (createdAt, then id) — FIFO queue order. */
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
}

function matchesFilter(
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
  return true;
}

function byQueueOrder(a: ApprovalRecord, b: ApprovalRecord): number {
  return a.createdAt.localeCompare(b.createdAt) || a.id.localeCompare(b.id);
}

// Records carry caller-provided payloads; clone on every boundary so callers
// can never mutate stored state (or observe another caller's mutations).
function clone(record: ApprovalRecord): ApprovalRecord {
  return structuredClone(record);
}

/**
 * In-memory reference implementation. Mirrors D1ApprovalStore semantics —
 * both run the same contract test suite. CAS atomicity holds because the
 * check-and-mutate section is synchronous (no awaits inside).
 */
export class InMemoryApprovalStore implements ApprovalStore {
  readonly #records = new Map<string, ApprovalRecord>();

  async create(record: ApprovalRecord): Promise<CreateResult> {
    const key = stepKeyOf(record.stepPath);
    for (const existing of this.#records.values()) {
      if (
        existing.workflowId === record.workflowId &&
        existing.runId === record.runId &&
        stepKeyOf(existing.stepPath) === key &&
        OPEN_STATUSES.includes(existing.status)
      ) {
        return { record: clone(existing), created: false };
      }
    }
    if (this.#records.has(record.id)) {
      throw new Error(`approval id '${record.id}' already exists`);
    }
    this.#records.set(record.id, clone(record));
    return { record: clone(record), created: true };
  }

  async get(id: string): Promise<ApprovalRecord | null> {
    const record = this.#records.get(id);
    return record ? clone(record) : null;
  }

  async list(filter: ApprovalListFilter = {}): Promise<ApprovalRecord[]> {
    return [...this.#records.values()]
      .filter((record) => matchesFilter(record, filter))
      .sort(byQueueOrder)
      .map(clone);
  }

  async transition(
    id: string,
    from: readonly ApprovalStatus[],
    patch: ApprovalPatch,
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
}
