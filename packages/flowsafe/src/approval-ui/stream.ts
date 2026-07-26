// SPDX-License-Identifier: Apache-2.0
// The live-streaming seam and its pure reducers. Deliberately DOM-free: the
// transport is INJECTED behind a structural interface (like client.ts's
// FetchLike), so this file typechecks in the main (workers-typed) pass with no
// DOM lib — a browser-WebSocket reference here would break that build far from
// the cause. The only place a `WebSocket` global lives is the UI-pass-only
// use-web-socket-transport.ts, which supplies a StreamTransport by injection.
//
// The reducers are pure and node-testable (stream.test.ts): they take the
// current dashboard state plus one wire event and return the next state, so all
// live-merge / optimistic-decide / reconcile logic stays out of the React hook.
// Wire types come from ../approval-api — the frame payloads ARE those types
// serialized (ApprovalStreamEvent for the queue channel); RunSummary and the
// presence roster are mirrored as minimal local structural shapes so this
// module keeps approval-ui's "no Mastra, no do-runner" decoupling.

import type { ApprovalStreamEvent } from '../approval-api/contract.js';
import {
  type ApprovalDecision,
  type ApprovalMetrics,
  type ApprovalRecord,
  type ApprovalStatus,
  byReviewerOrder,
} from '../approval-api/types.js';

// ---- Transport seam (structural, mirrors FetchLike) -----------------------

/** A live connection the hook closes on unmount / transport change. */
export interface StreamConnection {
  close(): void;
  /**
   * Send a frame to the server (optional). The hook uses it for a liveness
   * heartbeat ('ping'); a transport that omits it simply gets no heartbeat and
   * relies on the socket's own onClose. Browsers cannot send on a half-open
   * socket, so the heartbeat + a pong deadline is what detects a silently dead
   * connection and resumes polling (a broadcast-only run channel never fires
   * onClose on its own).
   */
  send?(data: string): void;
}

/** The transport wires these back to the subscriber. */
export interface StreamHandlers {
  onMessage(data: string): void;
  onOpen?(): void;
  onClose?(): void;
  onError?(err: unknown): void;
}

/**
 * The injected transport. A browser-WebSocket implementation lives ONLY in the
 * UI pass (use-web-socket-transport.ts); the library never hard-depends on a
 * WebSocket global.
 */
export interface StreamTransport {
  open(url: string, handlers: StreamHandlers): StreamConnection;
}

// ---- Wire frames ----------------------------------------------------------

/** A reviewer attached to the tenant's hub, mirrored from do-runner/hub-do. */
export interface PresenceMember {
  actorId: string;
  role: string;
}

/**
 * Minimal structural mirror of do-runner's RunSummary — only the JSON-safe
 * fields a run frame carries. Defined locally (not imported) so approval-ui
 * stays free of a do-runner/Mastra dependency; a real RunSummary is
 * structurally assignable to it.
 */
export interface StreamRunSummary {
  runId: string;
  status: string;
  result?: unknown;
  error?: string;
  suspended?: string[][];
  suspendPayload?: unknown;
  suspendedAt?: Record<string, number>;
  resumedAt?: Record<string, number>;
  resumeCount?: Record<string, number>;
  createdAt?: string;
  updatedAt?: string;
}

/**
 * The wire union every Durable Object emits: the per-tenant hub fans out `queue`
 * (an ApprovalStreamEvent) and `presence` (the roster) frames; the per-run
 * runner DO emits `run` (the wholesale authoritative RunSummary) frames.
 */
export type StreamFrame =
  | { type: 'queue'; event: ApprovalStreamEvent }
  | { type: 'run'; summary: StreamRunSummary }
  | { type: 'presence'; roster: PresenceMember[] };

/**
 * Parse a raw text frame to a StreamFrame, or undefined for anything malformed
 * or of an unknown type. This is a same-trust intra-tenant feed (every
 * subscriber is an authenticated reviewer of this tenant), so it validates the
 * discriminant only, not the payload shape.
 */
export function parseStreamFrame(data: string): StreamFrame | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(data);
  } catch {
    return undefined;
  }
  if (typeof parsed !== 'object' || parsed === null) return undefined;
  const { type } = parsed as { type?: unknown };
  if (type === 'queue' || type === 'run' || type === 'presence') {
    return parsed as StreamFrame;
  }
  return undefined;
}

// ---- Optimistic-decide state ----------------------------------------------

/** One reviewer's in-flight optimistic decision, awaiting authoritative confirmation. */
export interface OptimisticDecision {
  id: string;
  decision: ApprovalDecision;
  /** The deciding reviewer's id — compared against the authoritative decidedBy to detect a conflict. */
  actorId: string;
}

/** Optimistic decisions keyed by record id. */
export type PendingDecisions = Readonly<Record<string, OptimisticDecision>>;

/** A live decision the stream attributed to a DIFFERENT reviewer than our optimistic decide. */
export interface DecisionConflict {
  id: string;
  actualDecider: string;
}

// ---- Pure reducers --------------------------------------------------------

export interface MergeApprovalEventOptions {
  /**
   * The queue's status filter. When set, an upserted record whose status is
   * NOT in the set is dropped from the list (a `decided` event removes a record
   * from the open queue; a `created` event never enters a decided-history
   * view). Undefined keeps every status.
   */
  statuses?: readonly ApprovalStatus[];
  /** Row comparator — defaults to the reviewer order the dashboard renders in. */
  comparator?: (a: ApprovalRecord, b: ApprovalRecord) => number;
}

/**
 * Live-merge one queue event: upsert the post-transition record by id, drop it
 * when it leaves the current status filter, and re-sort by the SAME comparator
 * the hook renders with (byReviewerOrder). Non-mutating.
 */
export function mergeApprovalEvent(
  records: readonly ApprovalRecord[],
  event: ApprovalStreamEvent,
  options: MergeApprovalEventOptions = {},
): ApprovalRecord[] {
  const { statuses, comparator = byReviewerOrder } = options;
  const incoming = event.record;
  const withoutIncoming = records.filter((record) => record.id !== incoming.id);
  const leavesFilter =
    statuses !== undefined && !statuses.includes(incoming.status);
  const next = leavesFilter ? withoutIncoming : [...withoutIncoming, incoming];
  return next.sort(comparator);
}

/**
 * Mark a record locally decided (it greys out immediately) and return the
 * pending descriptor to track until the authoritative event reconciles it.
 * Non-mutating.
 */
export function applyOptimisticDecide(
  records: readonly ApprovalRecord[],
  id: string,
  decision: ApprovalDecision,
  actorId: string,
): { records: ApprovalRecord[]; pending: OptimisticDecision } {
  const status: ApprovalStatus =
    decision === 'approve' ? 'approved' : 'rejected';
  const next = records.map((record) =>
    record.id === id
      ? { ...record, status, decision, decidedBy: actorId }
      : record,
  );
  return { records: next, pending: { id, decision, actorId } };
}

/**
 * Reconcile an authoritative decided event against the optimistic pending map:
 * clear the matching entry, and surface a conflict when the authoritative
 * decidedBy differs from the reviewer who optimistically decided (someone else
 * decided first). A pending entry with an empty actorId (no id was supplied to
 * the hook) clears WITHOUT raising a conflict — there is no identity to compare.
 * Non-mutating.
 */
export function reconcileDecided(
  pending: PendingDecisions,
  event: ApprovalStreamEvent,
): { pending: PendingDecisions; conflict?: DecisionConflict } {
  const { id, decidedBy } = event.record;
  const entry = pending[id];
  if (entry === undefined) return { pending };
  const rest: Record<string, OptimisticDecision> = {};
  for (const [key, value] of Object.entries(pending)) {
    if (key !== id) rest[key] = value;
  }
  const conflict =
    entry.actorId !== '' &&
    decidedBy !== undefined &&
    decidedBy !== entry.actorId
      ? { id, actualDecider: decidedBy }
      : undefined;
  return { pending: rest, conflict };
}

/**
 * Reduce a presence roster frame: distinct reviewers by actorId (a reviewer
 * with two tabs counts once), in a stable order. Non-mutating.
 */
export function presenceReducer(
  roster: readonly PresenceMember[],
): PresenceMember[] {
  const seen = new Set<string>();
  const distinct: PresenceMember[] = [];
  for (const member of roster) {
    if (!seen.has(member.actorId)) {
      seen.add(member.actorId);
      distinct.push({ actorId: member.actorId, role: member.role });
    }
  }
  return distinct.sort((a, b) =>
    a.actorId < b.actorId ? -1 : a.actorId > b.actorId ? 1 : 0,
  );
}

/**
 * Optimistic metrics counter deltas: apply the derivable movement on
 * each event for instant feedback; the interval poll's wholesale metrics()
 * refetch is the authoritative reconciler. Counts never go negative.
 * Returns the same reference when nothing derivable changed (no re-render), and
 * null when metrics have not loaded yet.
 */
export function applyMetricsDelta(
  metrics: ApprovalMetrics | null,
  event: ApprovalStreamEvent,
): ApprovalMetrics | null {
  if (metrics === null) return null;
  switch (event.type) {
    case 'created':
      return { ...metrics, openCount: metrics.openCount + 1 };
    case 'decided': {
      const approved = event.record.decision === 'approve';
      return {
        ...metrics,
        openCount: Math.max(0, metrics.openCount - 1),
        decidedCount: metrics.decidedCount + 1,
        approvedCount: metrics.approvedCount + (approved ? 1 : 0),
        rejectedCount: metrics.rejectedCount + (approved ? 0 : 1),
      };
    }
    case 'escalated':
      return { ...metrics, escalationCount: metrics.escalationCount + 1 };
    case 'superseded':
      return { ...metrics, openCount: Math.max(0, metrics.openCount - 1) };
    default:
      // claimed / delegated leave the counters untouched.
      return metrics;
  }
}
