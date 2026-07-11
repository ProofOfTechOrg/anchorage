// SPDX-License-Identifier: Apache-2.0
// Headless core of the approval dashboard: all data + interaction logic, no
// markup. A consumer can drive a fully custom UI from this hook alone, or use
// the slot-based views (App/QueueView/…) that render it through injected
// components. React-only (no DOM globals), but UI-pass-only — excluded from the
// main workers-typed tsc pass alongside the .tsx views.

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import type {
  ApprovalDecision,
  ApprovalListFilter,
  ApprovalMetrics,
  ApprovalRecord,
  BatchDecideResult,
} from '../approval-api/types.js';
import { OPEN_STATUSES } from '../approval-api/types.js';
import type { ApprovalApiClient } from './client.js';
import { sortQueue } from './view-model.js';

/**
 * The dashboard's default queue slice: open (non-terminal) requests only,
 * page-bounded at 100 in REVIEWER order. The bound is D3 — an unfiltered
 * client.list() repeated every poll is a full-table scan on every open
 * dashboard (2026-07-11 audit). orderBy: 'reviewer' makes the server rank
 * priority → SLA → FIFO BEFORE cutting the page, so the 100 rows are the
 * top of the queue — under the default FIFO cut, a fresh critical request
 * beyond the oldest 100 never reached the dashboard at all (2026-07-11
 * review). This is the fallback whenever a caller does not override
 * UseApprovalDashboardOptions.filter.
 */
export const DEFAULT_QUEUE_FILTER: ApprovalListFilter = {
  status: [...OPEN_STATUSES],
  limit: 100,
  orderBy: 'reviewer',
};

/**
 * Render-stable identity for a filter: its JSON serialization. The hook keys
 * refresh() on this VALUE instead of the object reference, so the natural
 * `useApprovalDashboard(client, { filter: { status: 'pending' } })` — a new
 * object identity every render — polls on the configured interval instead of
 * looping list/metrics requests back-to-back (2026-07-11 review), while a
 * filter that CHANGES value still refetches immediately. Key order follows
 * the caller's literal, which is constant across that caller's renders — the
 * only stability this needs.
 */
export function approvalFilterKey(filter: ApprovalListFilter): string {
  return JSON.stringify(filter);
}

/**
 * One poll's worth of dashboard data. DOM-free and hook-free — pulled out of
 * refresh() so the filter wiring (which ApprovalListFilter reaches
 * client.list) is testable without mounting the hook (the package's default
 * no-renderer stance; the one exception is
 * use-approval-dashboard.render.test.ts, which mounts the hook on happy-dom
 * to pin the dependency-identity wiring — see README). metrics() is
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

/**
 * Queue display order: re-sort into reviewer order ONLY when the filter asked
 * for it; otherwise return the server's order untouched, so a 'created'/FIFO
 * or `after`-paged filter is never client-resorted against the server's paging
 * (a page-2 FIFO slice re-sorted into reviewer order would interleave with
 * page 1). Resolves orderBy like approvalListOrder but WITHOUT its
 * reviewer+after throw — this runs in the hook's render path, where a throw
 * would crash the dashboard (the fetch already surfaces that incoherent combo
 * as an error). Pulled out (like fetchDashboardSnapshot) for DOM-free testing.
 */
export function orderRecordsForDisplay(
  records: readonly ApprovalRecord[],
  filter: ApprovalListFilter,
): ApprovalRecord[] {
  return (filter.orderBy ?? 'created') === 'reviewer'
    ? sortQueue(records)
    : [...records];
}

/**
 * The batch-selection prune: only ids still present in the fetched page AND
 * still open (decidable) survive. Derived on every render, never an effect —
 * a refresh that decides, escalates-out, or pages a record away cannot leave
 * a stale id behind for decideSelected to re-submit. Pulled out (like
 * fetchDashboardSnapshot) for DOM-free testing.
 */
export function pruneSelection(
  selectedIds: readonly string[],
  records: readonly ApprovalRecord[],
): string[] {
  return selectedIds.filter((id) =>
    records.some(
      (record) => record.id === id && OPEN_STATUSES.includes(record.status),
    ),
  );
}

/** A setFilter override, valid only against the options.filter it was set under. */
export interface ApprovalFilterOverride {
  /** approvalFilterKey(options.filter) at the moment setFilter was called. */
  baseKey: string;
  filter: ApprovalListFilter;
}

/**
 * The controlled-filter derivation (deliberately NOT a reset effect): a
 * setFilter override is honoured only while the OPTIONS filter still has the
 * VALUE it was created against, so a caller-side filter change (tab switch,
 * new options literal) naturally retires the override on the same render —
 * no effect, no transient fetch against a stale filter. Pulled out for
 * DOM-free testing; the hook applies it every render.
 */
export function effectiveApprovalFilter(
  override: ApprovalFilterOverride | null,
  optionsFilter: ApprovalListFilter,
): ApprovalListFilter {
  return override && override.baseKey === approvalFilterKey(optionsFilter)
    ? override.filter
    : optionsFilter;
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
  /**
   * Queue records. In reviewer order (priority → deadline → FIFO) when the
   * filter requests it (the default) — otherwise the server's order, so a
   * FIFO/`after`-paged filter is not client-resorted. See orderRecordsForDisplay.
   */
  records: ApprovalRecord[];
  metrics: ApprovalMetrics | null;
  /** Derived from the fetched list — never stale after a refresh. */
  selected: ApprovalRecord | null;
  selectedId: string | null;
  error: string | null;
  /** True while a claim/decide/delegate/decideSelected mutation is in flight. */
  busy: boolean;
  nowMs: number;
  /** The EFFECTIVE queue filter: options.filter unless setFilter overrode it. */
  filter: ApprovalListFilter;
  /**
   * Override the queue filter from the UI (FilterBar). The override holds
   * until the OPTIONS filter value changes, which retires it on the same
   * render — controlled without a reset effect (effectiveApprovalFilter).
   */
  setFilter: (next: ApprovalListFilter) => void;
  /**
   * Batch selection, derived-pruned to ids still in the fetched page AND
   * still open (pruneSelection) — a decided or paged-out record can never
   * ride a stale checkbox into decideSelected.
   */
  selectedIds: readonly string[];
  toggleSelect: (id: string) => void;
  clearSelection: () => void;
  /** One decision fanned out over the current selection via decideBatch. No-op when empty. */
  decideSelected: (decision: ApprovalDecision, comment: string) => void;
  /** The most recent batch envelope; cleared when the next mutation starts. */
  lastBatch: BatchDecideResult | null;
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
  const [override, setOverride] = useState<ApprovalFilterOverride | null>(null);
  const [rawSelectedIds, setRawSelectedIds] = useState<readonly string[]>([]);
  const [lastBatch, setLastBatch] = useState<BatchDecideResult | null>(null);

  // Latest-ref for the injected clock: an inline `now` (a fixed test/story
  // clock) is a new function identity every render — the same request-loop
  // class as an inline filter, but a function cannot be value-keyed, so it
  // rides a ref and refresh() reads it at call time.
  const nowRef = useRef(now);
  useEffect(() => {
    nowRef.current = now;
  });

  // A setFilter override wins only while options.filter still carries the
  // value it was set against (effectiveApprovalFilter) — an options change
  // retires it with no reset effect. optionKey is that guard's identity.
  const optionKey = approvalFilterKey(filter);
  const effective = effectiveApprovalFilter(override, filter);

  const setFilter = useCallback(
    (next: ApprovalListFilter) => {
      setOverride({ baseKey: optionKey, filter: next });
    },
    [optionKey],
  );

  // Value identity for the filter (see approvalFilterKey): rebuilt from the
  // key alone so the memo depends on nothing identity-unstable. Without
  // this, an inline filter recreated refresh() every render, whose effect
  // refetched immediately, whose state updates rendered again — an unbounded
  // request loop that never reached the poll interval (2026-07-11 review).
  const filterKey = approvalFilterKey(effective);
  const stableFilter = useMemo(
    () => JSON.parse(filterKey) as ApprovalListFilter,
    [filterKey],
  );

  // `client` stays an identity dependency on purpose: a NEW client means a
  // new endpoint/authorization (the showcase actor switcher), which must
  // refetch immediately — unlike filter/now, its identity carries meaning.
  const refresh = useCallback(async () => {
    try {
      const { records: nextRecords, metrics: nextMetrics } =
        await fetchDashboardSnapshot(client, stableFilter);
      setRecords(nextRecords);
      setMetrics(nextMetrics);
      setNowMs(nowRef.current());
      setError(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    }
  }, [client, stableFilter]);

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

  // Batch selection is derived-pruned the same way (see pruneSelection).
  const selectedIds = useMemo(
    () => pruneSelection(rawSelectedIds, records),
    [rawSelectedIds, records],
  );

  const toggleSelect = useCallback((id: string): void => {
    setRawSelectedIds((current) =>
      current.includes(id)
        ? current.filter((existing) => existing !== id)
        : [...current, id],
    );
  }, []);

  const clearSelection = useCallback((): void => {
    setRawSelectedIds([]);
  }, []);

  const act = useCallback(
    async (action: () => Promise<unknown>): Promise<void> => {
      setBusy(true);
      // Any new mutation supersedes the previous batch summary.
      setLastBatch(null);
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

  const decideSelected = useCallback(
    (decision: ApprovalDecision, comment: string): void => {
      const ids = selectedIds;
      if (ids.length === 0) return;
      void act(async () => {
        const outcome = await client.decideBatch(
          ids,
          decision,
          comment === '' ? undefined : comment,
        );
        setLastBatch(outcome);
        setRawSelectedIds([]);
      });
    },
    [act, client, selectedIds],
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
    // Reviewer order only when the filter requested it (DEFAULT_QUEUE_FILTER
    // does, so this is idempotent there — the server already returns reviewer
    // order); a FIFO / `after`-paged filter is returned in the server's order,
    // never client-resorted against its own paging. See orderRecordsForDisplay.
    records: orderRecordsForDisplay(records, stableFilter),
    metrics,
    selected,
    selectedId,
    error,
    busy,
    nowMs,
    filter: stableFilter,
    setFilter,
    selectedIds,
    toggleSelect,
    clearSelection,
    decideSelected,
    lastBatch,
    select: setSelectedId,
    decide,
    claim,
    delegate,
    refresh,
  };
}
