// SPDX-License-Identifier: Apache-2.0
// Alarm-owned approval-queue retention purge (2026-07-11 audit, D3) — the
// approvals analog of do-runner's purgeExpiredWorkflowRuns
// (do-runner/d1-storage.ts), structured like sweepSLA (service.ts): a free
// function over the deployment store, never a service method or HTTP route.
//
// TERMINAL-TIMESTAMP CHOICE: a decided (approved/rejected) record's terminal
// instant is decidedAt — service.ts's decide() always stamps decidedAt and
// updatedAt together in the same patch, so for every record created through
// the normal path the two are identical at decision time. A record can only
// lack decidedAt while carrying a terminal status by being written straight
// into the store (a TCB bulk-import, a hand-built fixture) — for that edge
// case updatedAt is the fallback: it is never absent, and for a status that
// itself never transitions again it is the best available proxy for "when
// did this row last change".
//
// ACCEPTED, BY DESIGN: this purge has NO liveness check against the
// run/suspension a decided record's grant belongs to — unlike
// purgeExpiredWorkflowRuns, which is gated on the RUN's own terminal status.
// A liveness check would need either a join against do-runner's snapshot
// table (outside this module's structural ApprovalDatabase seam) or a
// host-supplied predicate, for a failure mode that is now self-healing on its
// own (see RECOVERY below) — not worth the coupling.
//
// RECOVERY (D4, 2026-07-11 audit): service.ts's decide() derives the grant
// and resumes the run synchronously in the common case; its #resume() catch
// documents the fallback path for a failed resume — "the run stays
// suspended; a later resume derives the same grants from the store". If this
// purge deletes the decided record before that retry lands, the retry fails
// CLOSED (no leak — see grants.ts) and the run is left suspended with no
// approval record: the same wedge shape host-kit/approval-bridge.ts's
// reconcileApprovalsForSummary exists to close for a live re-queue failure.
// The next status() read of that run re-files a FRESH approval bound to the
// CURRENT suspension fingerprint, requiring a NEW decision — deliberately:
// an approval that aged past retention should not silently re-arm the grant
// it already spent. One separation-of-duties relaxation applies only in this
// recovery path: the re-filed record's requestedBy is the system principal
// rather than a human, so the reviewer who decided the purged record MAY
// legally decide its replacement — there is no human "who advanced the run
// to this suspension" left to attribute it to. Operators should still set
// APPROVAL_RETENTION_DAYS well beyond any expected resume-retry window: a
// re-decision is real reviewer work, not a free retry.
//
// reconcileApprovalsForSummary's healing is not limited to this purge-then-
// retry trigger (2026-07-11 audit, follow-up): it also SUPERSEDES a
// stale-but-still-OPEN record -- a re-suspension of the same step while an
// earlier request for it still sits open, independent of any purge or TTL --
// before filing fresh. See its own doc comment (approval-bridge.ts) for the
// supersede mechanics; the terminal status it uses is 'rejected', so a
// superseded record is excluded from grant derivation by its STATUS alone,
// the same guarantee this purge relies on for a decided-then-purged record.

import type { ApprovalStore } from './store.js';

export interface PurgeExpiredApprovalsOptions {
  /**
   * Decided (approved/rejected) records whose terminal timestamp — decidedAt,
   * or updatedAt when a decided record was persisted without one — is older
   * than this are eligible. Open requests (pending/claimed/escalated) are
   * NEVER purged at any age: an approval still awaiting a decision is not
   * garbage (mirrors purgeExpiredWorkflowRuns' "live runs are never
   * purged").
   */
  ttlMs: number;
  /** Injectable clock (tests, deterministic TTL math). Default: Date.now. */
  now?: () => number;
  /**
   * Records deleted per call — one LIMIT-batched DELETE per firing; the
   * shrinking eligible set is the cursor across firings (same convention as
   * purgeExpiredWorkflowRuns' row-only path). Default 1000.
   */
  limit?: number;
}

/**
 * Deletes decided approval records past their retention TTL. Trusted
 * maintenance code — deliberately not a service method or HTTP route. Returns
 * the number of deleted records.
 */
export async function purgeExpiredApprovals(
  store: ApprovalStore,
  options: PurgeExpiredApprovalsOptions,
): Promise<number> {
  // Validated here so BOTH backends inherit it: a negative ttlMs turns the
  // cutoff into a FUTURE timestamp, purging just-decided records instead of
  // old ones, and a negative limit diverges by backend — the in-memory
  // purge's `purged >= limit` guard never fires (no-op), while D1's `LIMIT
  // -1` is unbounded (SQLite treats a negative LIMIT as "no limit"), turning
  // a batch cap into an unbounded DELETE. limit: 0 (no-op) and
  // ttlMs: 0 (purge decided approvals now) stay valid — both are real
  // operator intents (see numberVar's allowZero convention).
  if (!Number.isFinite(options.ttlMs) || options.ttlMs < 0) {
    throw new TypeError(
      `purgeExpiredApprovals: ttlMs must be a non-negative finite number (got ${options.ttlMs})`,
    );
  }
  if (
    options.limit !== undefined &&
    (!Number.isInteger(options.limit) || options.limit < 0)
  ) {
    throw new TypeError(
      `purgeExpiredApprovals: limit must be a non-negative integer (got ${options.limit})`,
    );
  }
  const now = options.now ?? Date.now;
  const cutoffIso = new Date(now() - options.ttlMs).toISOString();
  return store.purgeExpired(cutoffIso, options.limit ?? 1000);
}
