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
  ApprovalStatus,
  BatchDecideResult,
} from '../approval-api/types.js';
import { OPEN_STATUSES } from '../approval-api/types.js';
import type { ApprovalApiClient } from './client.js';
import {
  applyMetricsDelta,
  applyOptimisticDecide,
  type DecisionConflict,
  mergeApprovalEvent,
  type PendingDecisions,
  type PresenceMember,
  parseStreamFrame,
  presenceReducer,
  reconcileDecided,
  type StreamConnection,
  type StreamFrame,
  type StreamHandlers,
  type StreamTransport,
} from './stream.js';
import { sortQueue } from './view-model.js';

/**
 * The dashboard's default queue slice: open (non-terminal) requests only,
 * page-bounded at 100 in reviewer order. The bound prevents an unfiltered
 * client.list() on every poll from scanning the full table. `orderBy:
 * 'reviewer'` makes the server rank priority → SLA → FIFO before cutting the
 * page, so the 100 rows are the top of the queue. This is the fallback
 * whenever a caller does not override UseApprovalDashboardOptions.filter.
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
 * deliberately called with no filter: it always summarizes the whole deployment
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

/** Normalize an ApprovalListFilter.status into a plain status array (or undefined = all). */
function statusFilterOf(
  filter: ApprovalListFilter,
): readonly ApprovalStatus[] | undefined {
  if (filter.status === undefined) return undefined;
  return Array.isArray(filter.status) ? filter.status : [filter.status];
}

/**
 * A cancelable timer seam — injected in tests, defaults to the host globals.
 * Kept structural (no ambient-timer types) so it typechecks in both the UI and
 * the transitively-included workers-typed test pass.
 */
export interface StreamScheduler {
  setTimeout(handler: () => void, ms: number): ReturnType<typeof setTimeout>;
  clearTimeout(handle: ReturnType<typeof setTimeout>): void;
}

const DEFAULT_STREAM_SCHEDULER: StreamScheduler = {
  setTimeout: (handler, ms) => setTimeout(handler, ms),
  clearTimeout: (handle) => {
    clearTimeout(handle);
  },
};

/** Reconnect backoff (ms); the last entry repeats for every further attempt. */
export const DEFAULT_STREAM_RECONNECT_MS: readonly number[] = [
  1_000, 2_000, 5_000, 10_000,
];

/** Heartbeat ping cadence (ms) — how often the client proves the socket is live. */
export const DEFAULT_STREAM_HEARTBEAT_MS = 20_000;
/** Pong deadline (ms) after a ping — a missed pong forces a reconnect. */
export const DEFAULT_STREAM_HEARTBEAT_TIMEOUT_MS = 10_000;

/**
 * A permanent (non-retryable) ticket failure: the host is not serving streaming
 * (no STREAM_TICKET_SECRET ⇒ the ticket route 404s), or the caller is not
 * permitted to stream. A 4xx means a retry cannot succeed, so the subscription
 * gives up and the client stays cleanly poll-only — never hammering the ticket
 * route with endless background 404s. A 5xx or a network error (no status)
 * stays transient and reconnects with backoff.
 */
function isPermanentStreamError(cause: unknown): boolean {
  const status = (cause as { status?: unknown } | null | undefined)?.status;
  return typeof status === 'number' && status >= 400 && status < 500;
}

/** The injected stream option: a transport plus a ticket() thunk (both from the host). */
export interface ApprovalStreamOption {
  transport: StreamTransport;
  /** Mints a fresh addressing ticket; the socket URL is `url + ticket`. */
  ticket: () => Promise<{ url: string; ticket: string }>;
}

export interface SubscribeApprovalStreamOptions extends ApprovalStreamOption {
  onFrame: (frame: StreamFrame) => void;
  /** Fires on each successful (re)open — the run channel resumes pausing its poll here. */
  onOpen?: () => void;
  /** Fires once per disconnect (before the backoff reconnect) — poll fallback resumes here. */
  onClose?: () => void;
  reconnectDelaysMs?: readonly number[];
  scheduler?: StreamScheduler;
  /** Heartbeat ping cadence (ms). Default DEFAULT_STREAM_HEARTBEAT_MS. */
  heartbeatMs?: number;
  /** Pong deadline (ms) after a ping. Default DEFAULT_STREAM_HEARTBEAT_TIMEOUT_MS. */
  heartbeatTimeoutMs?: number;
}

/**
 * Open a stream and keep it open across drops (fetch a ticket, open the injected
 * transport, parse each frame to onFrame, reconnect with backoff on
 * close/error). Pure and node-testable — the React hook wires onFrame to its
 * reducers. DOM-free: the transport and timers are injected. Returns a
 * StreamConnection whose close() stops reconnecting and closes the live socket.
 */
export function subscribeApprovalStream({
  transport,
  ticket,
  onFrame,
  onOpen,
  onClose,
  reconnectDelaysMs = DEFAULT_STREAM_RECONNECT_MS,
  scheduler = DEFAULT_STREAM_SCHEDULER,
  heartbeatMs = DEFAULT_STREAM_HEARTBEAT_MS,
  heartbeatTimeoutMs = DEFAULT_STREAM_HEARTBEAT_TIMEOUT_MS,
}: SubscribeApprovalStreamOptions): StreamConnection {
  let active = true;
  let connection: StreamConnection | undefined;
  let disconnected = false;
  let attempt = 0;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let heartbeatTimer: ReturnType<typeof setTimeout> | undefined;
  let livenessTimer: ReturnType<typeof setTimeout> | undefined;

  const clearHeartbeat = (): void => {
    if (heartbeatTimer !== undefined) {
      scheduler.clearTimeout(heartbeatTimer);
      heartbeatTimer = undefined;
    }
    if (livenessTimer !== undefined) {
      scheduler.clearTimeout(livenessTimer);
      livenessTimer = undefined;
    }
  };

  const scheduleReconnect = (): void => {
    if (!active) return;
    const index = Math.min(attempt, reconnectDelaysMs.length - 1);
    const delay = reconnectDelaysMs[index] ?? 1_000;
    attempt += 1;
    timer = scheduler.setTimeout(() => {
      if (active) void connect();
    }, delay);
  };

  // Single disconnect funnel — a socket that fires onerror THEN onclose, or a
  // missed-pong force-close, must schedule exactly one reconnect. Closes the
  // (possibly half-open) socket so the transport releases it.
  const handleDisconnect = (): void => {
    if (!active || disconnected) return;
    disconnected = true;
    clearHeartbeat();
    connection?.close();
    connection = undefined;
    onClose?.();
    scheduleReconnect();
  };

  // Liveness heartbeat (F1): a browser cannot detect a silently half-open socket
  // (sleep/wake, NAT drop) — it never fires onclose — so a broadcast-only run
  // channel would sit paused on stale state indefinitely. Ping on an interval;
  // ANY inbound frame (the DO answers 'ping' with 'pong', which parseStreamFrame
  // ignores) proves liveness and clears the deadline; a missed pong forces a
  // disconnect so the poll resumes and the socket reconnects. A transport with
  // no send() gets no heartbeat and relies on its own onClose.
  const armLiveness = (): void => {
    if (livenessTimer !== undefined) scheduler.clearTimeout(livenessTimer);
    livenessTimer = scheduler.setTimeout(() => {
      livenessTimer = undefined;
      handleDisconnect();
    }, heartbeatTimeoutMs);
  };

  const scheduleHeartbeat = (): void => {
    heartbeatTimer = scheduler.setTimeout(() => {
      heartbeatTimer = undefined;
      if (!active) return;
      const conn = connection;
      if (!conn?.send) return; // no heartbeat capability — rely on the socket's onClose
      try {
        conn.send('ping');
      } catch {
        handleDisconnect();
        return;
      }
      armLiveness();
      scheduleHeartbeat();
    }, heartbeatMs);
  };

  const handlers: StreamHandlers = {
    onMessage: (data) => {
      // Any inbound frame proves the socket is alive — clear the pong deadline.
      if (livenessTimer !== undefined) {
        scheduler.clearTimeout(livenessTimer);
        livenessTimer = undefined;
      }
      const frame = parseStreamFrame(data);
      if (frame) onFrame(frame);
    },
    onOpen: () => {
      attempt = 0;
      onOpen?.();
      if (connection?.send) scheduleHeartbeat();
    },
    onClose: handleDisconnect,
    onError: handleDisconnect,
  };

  async function connect(): Promise<void> {
    if (!active) return;
    disconnected = false;
    let address: { url: string; ticket: string };
    try {
      address = await ticket();
    } catch (cause) {
      if (isPermanentStreamError(cause)) {
        // Streaming is not mounted / not permitted here — stop retrying and stay
        // cleanly poll-only (F4), never hammering the ticket route forever.
        active = false;
        clearHeartbeat();
        return;
      }
      // A transient ticket() failure is a disconnect — back off and retry.
      handleDisconnect();
      return;
    }
    if (!active) return;
    try {
      connection = transport.open(`${address.url}${address.ticket}`, handlers);
    } catch {
      // A failed open() is a disconnect — back off and retry.
      handleDisconnect();
    }
  }

  void connect();

  return {
    close: () => {
      active = false;
      clearHeartbeat();
      if (timer !== undefined) scheduler.clearTimeout(timer);
      connection?.close();
    },
  };
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
  /**
   * Live streaming through an injected StreamTransport and ticket() thunk.
   * Absent means poll-only. When present the hook opens
   * the deployment approval stream and live-merges events on top of the interval
   * poll, which keeps running as the periodic reconciler.
   */
  stream?: ApprovalStreamOption;
  /**
   * The current reviewer's id — attributes an optimistic decide so a live
   * 'decided' event naming a DIFFERENT decider surfaces a conflict. Absent ⇒
   * optimistic decide still greys the row, but no conflict is ever raised (no
   * id to compare against); the host supplies it.
   */
  actorId?: string;
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
  /**
   * Reviewers currently connected to the deployment live stream (deduped by
   * actorId). Empty in poll-only mode.
   */
  presence: PresenceMember[];
  /**
   * Set when a live 'decided' event attributes a record this reviewer
   * optimistically decided to a DIFFERENT decider (they decided first). null
   * otherwise. Never set in poll-only mode or without an actorId.
   */
  conflict: DecisionConflict | null;
  /** Clears the current conflict surface (Toast dismissal). */
  dismissConflict: () => void;
}

export function useApprovalDashboard(
  client: ApprovalApiClient,
  {
    pollIntervalMs = 10_000,
    now = Date.now,
    filter = DEFAULT_QUEUE_FILTER,
    stream,
    actorId,
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
  const [presence, setPresence] = useState<PresenceMember[]>([]);
  const [conflict, setConflict] = useState<DecisionConflict | null>(null);
  // Optimistic decisions are a transient tracker, never rendered directly (the
  // greyed row lives in `records`), so a ref avoids a re-render per mutation
  // and gives the frame handler a synchronous read for conflict detection.
  const pendingRef = useRef<PendingDecisions>({});

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

  // The live-merge status filter, read by the (stable) frame handler via a ref
  // so a filter change never re-subscribes the socket. Kept current here.
  const statusesRef = useRef<readonly ApprovalStatus[] | undefined>(
    statusFilterOf(stableFilter),
  );
  useEffect(() => {
    statusesRef.current = statusFilterOf(stableFilter);
  }, [stableFilter]);

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

  const dismissConflict = useCallback((): void => {
    setConflict(null);
  }, []);

  // Route one live frame into state. Stable ([] deps) — it reads current values
  // through refs and the functional setState updaters, so the subscription
  // effect never re-subscribes when the filter or queue changes (guideline 8.3).
  const handleFrame = useCallback((frame: StreamFrame): void => {
    if (frame.type === 'queue') {
      const { event } = frame;
      setRecords((current) =>
        mergeApprovalEvent(current, event, { statuses: statusesRef.current }),
      );
      setMetrics((current) => applyMetricsDelta(current, event));
      if (event.type === 'decided') {
        const { pending: next, conflict: found } = reconcileDecided(
          pendingRef.current,
          event,
        );
        pendingRef.current = next;
        if (found) setConflict(found);
      }
    } else if (frame.type === 'presence') {
      setPresence(presenceReducer(frame.roster));
    }
    // 'run' frames carry the wholesale RunSummary for the per-run view (M-008);
    // this queue-focused dashboard holds no run state, so it ignores them.
  }, []);

  // Live subscription — additive to the poll, which keeps running as the queue
  // reconciler (DL-021). Narrow deps (guideline 5.7): re-subscribe only when the
  // injected transport/ticket identity changes (a new client/auth), never on a
  // state or filter change. Absent stream ⇒ no subscription (poll-only).
  const streamTransport = stream?.transport;
  const streamTicket = stream?.ticket;
  useEffect(() => {
    if (streamTransport === undefined || streamTicket === undefined) return;
    const connection = subscribeApprovalStream({
      transport: streamTransport,
      ticket: streamTicket,
      onFrame: handleFrame,
    });
    return () => connection.close();
  }, [streamTransport, streamTicket, handleFrame]);

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
      // Optimistic: grey every selected row and track each pending decision.
      const decidedBy = actorId ?? '';
      setRecords((current) =>
        ids.reduce(
          (recs, id) =>
            applyOptimisticDecide(recs, id, decision, decidedBy).records,
          current,
        ),
      );
      const nextPending: Record<string, PendingDecisions[string]> = {
        ...pendingRef.current,
      };
      for (const id of ids)
        nextPending[id] = { id, decision, actorId: decidedBy };
      pendingRef.current = nextPending;
      void act(async () => {
        try {
          const outcome = await client.decideBatch(
            ids,
            decision,
            comment === '' ? undefined : comment,
          );
          setLastBatch(outcome);
          setRawSelectedIds([]);
          // Reconcile each attempted id against the authoritative envelope; a
          // failed item (no record) just drops its pending — the post-action
          // refresh (in act) un-greys it from the server.
          let map = pendingRef.current;
          for (const item of outcome.results) {
            if (item.record) {
              map = reconcileDecided(map, {
                type: 'decided',
                record: item.record,
              }).pending;
            } else {
              const rest: Record<string, PendingDecisions[string]> = { ...map };
              delete rest[item.id];
              map = rest;
            }
          }
          pendingRef.current = map;
        } catch (cause) {
          // Wholesale batch failure (F3): clear every optimistic pending for
          // these ids so none can later raise a spurious conflict, and resync the
          // greyed rows from the server (covers pollIntervalMs <= 0).
          const rest = { ...pendingRef.current };
          for (const id of ids) delete rest[id];
          pendingRef.current = rest;
          await refresh();
          throw cause;
        }
      });
    },
    [act, actorId, client, refresh, selectedIds],
  );

  const decide = useCallback(
    (decision: ApprovalDecision, comment: string): void => {
      if (!selected) return;
      const id = selected.id;
      // Optimistic: grey the row immediately, then confirm against the server
      // response (poll-only path) or the authoritative 'decided' stream event.
      const decidedBy = actorId ?? '';
      setRecords(
        (current) =>
          applyOptimisticDecide(current, id, decision, decidedBy).records,
      );
      pendingRef.current = {
        ...pendingRef.current,
        [id]: { id, decision, actorId: decidedBy },
      };
      void act(async () => {
        try {
          const result = await client.decide(
            id,
            decision,
            comment === '' ? undefined : comment,
          );
          pendingRef.current = reconcileDecided(pendingRef.current, {
            type: 'decided',
            record: result.record,
          }).pending;
        } catch (cause) {
          // Roll back the optimistic decision on failure (F3): clear the pending
          // entry so a later authoritative 'decided' event for this record does
          // NOT read as a spurious conflict, and resync the greyed row from the
          // server (covers pollIntervalMs <= 0, where no interval poll un-greys it).
          const rest = { ...pendingRef.current };
          delete rest[id];
          pendingRef.current = rest;
          await refresh();
          throw cause;
        }
      });
    },
    [act, actorId, client, refresh, selected],
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
    presence,
    conflict,
    dismissConflict,
  };
}
