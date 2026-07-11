// Pure presentation logic for the dashboard — SLA math, queue ordering,
// formatting. Kept out of the components so it runs under plain vitest with
// no DOM, and the .tsx files stay declarative shells.

import { type ApprovalRecord, byReviewerOrder } from '../approval-api/types.js';

export type SlaState = 'none' | 'ok' | 'warning' | 'breached';

/** Remaining time under this threshold renders as a warning. */
export const DEFAULT_SLA_WARNING_MS = 15 * 60 * 1000;

/** Milliseconds until the SLA deadline (negative = overdue); null without one. */
export function msRemaining(
  record: Pick<ApprovalRecord, 'slaDeadlineAt'>,
  nowMs: number,
): number | null {
  if (record.slaDeadlineAt === undefined) return null;
  return Date.parse(record.slaDeadlineAt) - nowMs;
}

export function slaStateOf(
  record: Pick<ApprovalRecord, 'slaDeadlineAt'>,
  nowMs: number,
  warningMs: number = DEFAULT_SLA_WARNING_MS,
): SlaState {
  const remaining = msRemaining(record, nowMs);
  if (remaining === null) return 'none';
  if (remaining <= 0) return 'breached';
  return remaining <= warningMs ? 'warning' : 'ok';
}

/** '2d 4h' | '2h 5m' | '3m' | '45s' for a non-negative duration. */
export function formatDuration(ms: number): string {
  const seconds = Math.floor(Math.max(0, ms) / 1000);
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m`;
  return `${seconds % 60}s`;
}

/** Human SLA cell: 'no SLA' | '<duration> left' | 'overdue by <duration>'. */
export function formatSlaCountdown(
  record: Pick<ApprovalRecord, 'slaDeadlineAt'>,
  nowMs: number,
): string {
  const remaining = msRemaining(record, nowMs);
  if (remaining === null) return 'no SLA';
  if (remaining <= 0) return `overdue by ${formatDuration(-remaining)}`;
  return `${formatDuration(remaining)} left`;
}

/**
 * Reviewer-facing queue order: priority first, then nearest SLA deadline
 * (records without one last), then FIFO. Delegates to the shared
 * byReviewerOrder (approval-api/types.ts) — the SAME rule the stores apply
 * server-side under ApprovalListFilter.orderBy: 'reviewer', so a bounded
 * page and this client-side sort can never rank differently (2026-07-11
 * review: a page cut in a different order hid high-priority records).
 * Non-mutating.
 */
export function sortQueue(
  records: readonly ApprovalRecord[],
): ApprovalRecord[] {
  return [...records].sort(byReviewerOrder);
}

/** Metrics cell for avgResolutionSeconds. */
export function formatResolution(seconds: number | null): string {
  if (seconds === null) return '—';
  return formatDuration(seconds * 1000);
}
