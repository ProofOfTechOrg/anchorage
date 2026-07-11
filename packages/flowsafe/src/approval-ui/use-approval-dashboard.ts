// Headless core of the approval dashboard: all data + interaction logic, no
// markup. A consumer can drive a fully custom UI from this hook alone, or use
// the slot-based views (App/QueueView/…) that render it through injected
// components. React-only (no DOM globals), but UI-pass-only — excluded from the
// main workers-typed tsc pass alongside the .tsx views.

import { useCallback, useEffect, useState } from 'react';

import type {
  ApprovalDecision,
  ApprovalListFilter,
  ApprovalMetrics,
  ApprovalRecord,
} from '../approval-api/types.js';
import { OPEN_STATUSES } from '../approval-api/types.js';
import type { ApprovalApiClient } from './client.js';
import { sortQueue } from './view-model.js';

/**
 * The dashboard's default queue slice: open (non-terminal) requests only,
 * page-bounded at 100. An unfiltered client.list() repeated every poll is a
 * full-table scan on every open dashboard (2026-07-11 audit, D3) — this is
 * the fallback whenever a caller does not override
 * UseApprovalDashboardOptions.filter.
 */
export const DEFAULT_QUEUE_FILTER: ApprovalListFilter = {
  status: [...OPEN_STATUSES],
  limit: 100,
};

/**
 * One poll's worth of dashboard data. DOM-free and hook-free — pulled out of
 * refresh() so the filter wiring (which ApprovalListFilter reaches
 * client.list) is testable without mounting the hook: approval-ui has no
 * renderer (see components.test.ts's "hooks need a renderer, the merge does
 * not" — the package's documented no-jsdom stance). metrics() is
 * deliberately called with no filter: it always summarizes the whole tenant
 * queue, independent of the queue view's slice.
 */
export async function fetchDashboardSnapshot(
  client: Pick<ApprovalApiClient, 'list' | 'metrics'>,
  filter: ApprovalListFilter,
): Promise<{ records: ApprovalRecord[]; metrics: ApprovalMetrics }> {
  const [records, metrics] = await Promise.all([
    client.list(filter),
    client.metrics(),
  ]);
  return { records, metrics };
}

export interface UseApprovalDashboardOptions {
  /** Queue/metrics refresh cadence; <= 0 disables polling. Default 10s. */
  pollIntervalMs?: number;
  /** Injectable clock (deterministic SLA countdowns in tests/stories). */
  now?: () => number;
  /**
   * The queue list filter, re-applied on every poll. Default:
   * DEFAULT_QUEUE_FILTER (open statuses, limit 100) — bounded so the
   * dashboard never issues an unfiltered full-table scan.
   */
  filter?: ApprovalListFilter;
}

export interface ApprovalDashboardState {
  /** Queue in reviewer order (priority → deadline → FIFO). */
  records: ApprovalRecord[];
  metrics: ApprovalMetrics | null;
  /** Derived from the fetched list — never stale after a refresh. */
  selected: ApprovalRecord | null;
  selectedId: string | null;
  error: string | null;
  /** True while a claim/decide/delegate mutation is in flight. */
  busy: boolean;
  nowMs: number;
  select: (id: string) => void;
  decide: (decision: ApprovalDecision, comment: string) => void;
  claim: () => void;
  delegate: (to: string) => void;
  refresh: () => Promise<void>;
}

export function useApprovalDashboard(
  client: ApprovalApiClient,
  {
    pollIntervalMs = 10_000,
    now = Date.now,
    filter = DEFAULT_QUEUE_FILTER,
  }: UseApprovalDashboardOptions = {},
): ApprovalDashboardState {
  const [records, setRecords] = useState<ApprovalRecord[]>([]);
  const [metrics, setMetrics] = useState<ApprovalMetrics | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [nowMs, setNowMs] = useState(now);

  const refresh = useCallback(async () => {
    try {
      const { records: nextRecords, metrics: nextMetrics } =
        await fetchDashboardSnapshot(client, filter);
      setRecords(nextRecords);
      setMetrics(nextMetrics);
      setNowMs(now());
      setError(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    }
  }, [client, now, filter]);

  useEffect(() => {
    void refresh();
    if (pollIntervalMs <= 0) return;
    const timer = setInterval(() => {
      void refresh();
    }, pollIntervalMs);
    return () => clearInterval(timer);
  }, [refresh, pollIntervalMs]);

  // Selection is derived — the queue refresh may change or remove the record.
  const selected = records.find((record) => record.id === selectedId) ?? null;

  const act = useCallback(
    async (action: () => Promise<unknown>): Promise<void> => {
      setBusy(true);
      try {
        await action();
        await refresh();
      } catch (cause) {
        setError(cause instanceof Error ? cause.message : String(cause));
      } finally {
        setBusy(false);
      }
    },
    [refresh],
  );

  const decide = useCallback(
    (decision: ApprovalDecision, comment: string): void => {
      if (!selected) return;
      void act(() =>
        client.decide(
          selected.id,
          decision,
          comment === '' ? undefined : comment,
        ),
      );
    },
    [act, client, selected],
  );

  const claim = useCallback((): void => {
    if (!selected) return;
    void act(() => client.claim(selected.id));
  }, [act, client, selected]);

  const delegate = useCallback(
    (to: string): void => {
      if (!selected) return;
      void act(() => client.delegate(selected.id, to));
    },
    [act, client, selected],
  );

  return {
    records: sortQueue(records),
    metrics,
    selected,
    selectedId,
    error,
    busy,
    nowMs,
    select: setSelectedId,
    decide,
    claim,
    delegate,
    refresh,
  };
}
