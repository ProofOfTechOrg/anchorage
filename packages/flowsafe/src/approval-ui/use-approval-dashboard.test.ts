// SPDX-License-Identifier: Apache-2.0
// DOM-free: exercises the default-filter contract and the poll wiring
// (fetchDashboardSnapshot) without mounting the hook — approval-ui has no
// renderer (see components.test.ts's "hooks need a renderer, the merge does
// not"; README.md: "No jsdom render tests"). fetchDashboardSnapshot is
// exported from use-approval-dashboard.ts precisely so this filter-forwarding
// behavior — the D3 fix — is testable in plain node.

import { describe, expect, it } from 'vitest';
import type { ApprovalStreamEvent } from '../approval-api/contract.js';
import type {
  ApprovalListFilter,
  ApprovalMetrics,
  ApprovalRecord,
} from '../approval-api/types.js';
import { OPEN_STATUSES } from '../approval-api/types.js';
import {
  mergeApprovalEvent,
  type StreamConnection,
  type StreamHandlers,
  type StreamTransport,
} from './stream.js';
import {
  approvalFilterKey,
  DEFAULT_QUEUE_FILTER,
  effectiveApprovalFilter,
  fetchDashboardSnapshot,
  orderRecordsForDisplay,
  pruneSelection,
  type StreamScheduler,
  subscribeApprovalStream,
} from './use-approval-dashboard.js';

const METRICS: ApprovalMetrics = {
  openCount: 0,
  slaBreachedCount: 0,
  escalationCount: 0,
  decidedCount: 0,
  approvedCount: 0,
  rejectedCount: 0,
  avgResolutionSeconds: null,
};

function makeRecord(overrides: Partial<ApprovalRecord> = {}): ApprovalRecord {
  return {
    id: 'apr-1',
    workflowId: 'wf',
    runId: 'acme_r1',
    title: 'publish',
    connectors: [],
    priority: 'normal',
    status: 'pending',
    createdAt: '2026-07-06T12:00:00.000Z',
    updatedAt: '2026-07-06T12:00:00.000Z',
    ...overrides,
  };
}

function fakeClient(records: ApprovalRecord[] = []): {
  list: (filter?: ApprovalListFilter) => Promise<ApprovalRecord[]>;
  metrics: () => Promise<ApprovalMetrics>;
  listCalls: Array<ApprovalListFilter | undefined>;
  metricsCallCount: () => number;
} {
  const listCalls: Array<ApprovalListFilter | undefined> = [];
  let metricsCalls = 0;
  return {
    listCalls,
    metricsCallCount: () => metricsCalls,
    list: async (filter) => {
      listCalls.push(filter);
      return records;
    },
    metrics: async () => {
      metricsCalls += 1;
      return METRICS;
    },
  };
}

describe('orderRecordsForDisplay', () => {
  it('re-sorts into reviewer order when the filter asks for reviewer order', () => {
    // #given — the server returned a normal ahead of a critical (out of order)
    const normal = makeRecord({ id: 'apr-normal', priority: 'normal' });
    const critical = makeRecord({ id: 'apr-critical', priority: 'critical' });

    // #then — byReviewerOrder surfaces the critical first
    expect(
      orderRecordsForDisplay([normal, critical], { orderBy: 'reviewer' }).map(
        (record) => record.id,
      ),
    ).toEqual(['apr-critical', 'apr-normal']);
  });

  it('preserves server order for a created/FIFO filter (no client re-sort)', () => {
    // #given — a FIFO page where a higher-priority record legitimately sits later
    const older = makeRecord({
      id: 'apr-older',
      priority: 'normal',
      createdAt: '2026-07-06T12:00:00.000Z',
    });
    const newerCritical = makeRecord({
      id: 'apr-newer',
      priority: 'critical',
      createdAt: '2026-07-06T13:00:00.000Z',
    });

    // #then — server FIFO order preserved verbatim (the critical stays second)
    expect(
      orderRecordsForDisplay([older, newerCritical], {
        orderBy: 'created',
      }).map((record) => record.id),
    ).toEqual(['apr-older', 'apr-newer']);
  });

  it('preserves server order when orderBy is omitted (defaults to FIFO)', () => {
    // #given
    const older = makeRecord({
      id: 'apr-older',
      priority: 'normal',
      createdAt: '2026-07-06T12:00:00.000Z',
    });
    const newerCritical = makeRecord({
      id: 'apr-newer',
      priority: 'critical',
      createdAt: '2026-07-06T13:00:00.000Z',
    });

    // #then — no orderBy => server FIFO, not reshuffled into reviewer order
    expect(
      orderRecordsForDisplay([older, newerCritical], {}).map(
        (record) => record.id,
      ),
    ).toEqual(['apr-older', 'apr-newer']);
  });

  it('does not throw on the reviewer+after combination (unlike approvalListOrder) and still re-sorts', () => {
    // #given — reviewer order WITH an after cursor: approvalListOrder rejects
    // this combination (the server 400s it), but orderRecordsForDisplay runs
    // in the render path where a throw would crash the dashboard, so it must
    // resolve the order WITHOUT that guard. Pins that a future refactor cannot
    // reintroduce the throw by delegating to approvalListOrder.
    const normal = makeRecord({ id: 'apr-normal', priority: 'normal' });
    const critical = makeRecord({ id: 'apr-critical', priority: 'critical' });

    // #then — no throw, and reviewer order still applied
    expect(
      orderRecordsForDisplay([normal, critical], {
        orderBy: 'reviewer',
        after: 'some-cursor',
      }).map((record) => record.id),
    ).toEqual(['apr-critical', 'apr-normal']);
  });
});

describe('DEFAULT_QUEUE_FILTER', () => {
  it('scopes the queue to open statuses, bounded at 100 in REVIEWER order', () => {
    // #then — orderBy makes the server rank priority → SLA → FIFO before
    // cutting the page; a FIFO cut hid a fresh critical request beyond the
    // oldest 100 (2026-07-11 review)
    expect(DEFAULT_QUEUE_FILTER).toEqual({
      status: [...OPEN_STATUSES],
      limit: 100,
      orderBy: 'reviewer',
    });
  });
});

describe('approvalFilterKey', () => {
  it('gives two structurally equal filters (fresh inline literals) ONE identity', () => {
    // #given — what an inline `filter: {...}` option produces on every
    // render: the same shape under a new object identity
    const key = approvalFilterKey({ status: ['pending'], limit: 25 });
    const rerenderKey = approvalFilterKey({ status: ['pending'], limit: 25 });

    // #then — identical keys keep the hook's refresh() stable, so the poll
    // interval governs request cadence instead of an every-render refetch
    // loop (2026-07-11 review)
    expect(rerenderKey).toBe(key);
  });

  it('changes when the filter value changes, and round-trips losslessly', () => {
    // #given
    const filter: ApprovalListFilter = {
      status: ['pending'],
      limit: 25,
      orderBy: 'reviewer',
    };

    // #when / #then — a semantic change still refetches immediately, and the
    // JSON round-trip the hook memoizes reconstructs an equivalent filter
    expect(approvalFilterKey({ ...filter, limit: 50 })).not.toBe(
      approvalFilterKey(filter),
    );
    expect(JSON.parse(approvalFilterKey(filter))).toEqual(filter);
  });
});

describe('fetchDashboardSnapshot', () => {
  it('forwards the given filter to client.list, and calls metrics() with no filter', async () => {
    // #given
    const client = fakeClient();
    const filter: ApprovalListFilter = { status: 'pending', limit: 25 };

    // #when
    await fetchDashboardSnapshot(client, filter);

    // #then — the exact filter object reached list(); metrics() took no args
    expect(client.listCalls).toEqual([filter]);
    expect(client.metricsCallCount()).toBe(1);
  });

  it('defaults to DEFAULT_QUEUE_FILTER when the hook applies no override', async () => {
    // #given
    const client = fakeClient();

    // #when
    await fetchDashboardSnapshot(client, DEFAULT_QUEUE_FILTER);

    // #then
    expect(client.listCalls).toEqual([DEFAULT_QUEUE_FILTER]);
  });

  it('returns the records and metrics from one poll', async () => {
    // #given
    const record = makeRecord();
    const client = fakeClient([record]);

    // #when
    const snapshot = await fetchDashboardSnapshot(client, DEFAULT_QUEUE_FILTER);

    // #then
    expect(snapshot.records).toEqual([record]);
    expect(snapshot.metrics).toEqual(METRICS);
  });

  it('lets a caller override the filter entirely (e.g. a decided-history view)', async () => {
    // #given
    const client = fakeClient();
    const historyFilter: ApprovalListFilter = {
      status: ['approved', 'rejected'],
      limit: 50,
    };

    // #when
    await fetchDashboardSnapshot(client, historyFilter);

    // #then — the override reached list() verbatim, not the open-statuses default
    expect(client.listCalls).toEqual([historyFilter]);
  });
});

describe('pruneSelection', () => {
  it('keeps only ids that are present in the page AND still open', () => {
    // #given
    const open = makeRecord({ id: 'apr-open', status: 'pending' });
    const claimed = makeRecord({ id: 'apr-claimed', status: 'claimed' });
    const escalated = makeRecord({ id: 'apr-escalated', status: 'escalated' });
    const decided = makeRecord({ id: 'apr-decided', status: 'approved' });
    const records = [open, claimed, escalated, decided];

    // #when — selection carries a decided record and a paged-out one
    const pruned = pruneSelection(
      ['apr-open', 'apr-decided', 'apr-gone', 'apr-claimed', 'apr-escalated'],
      records,
    );

    // #then — every OPEN status survives; decided and absent ids drop
    expect(pruned).toEqual(['apr-open', 'apr-claimed', 'apr-escalated']);
  });

  it('returns empty for an empty selection or an empty page', () => {
    // #when / #then
    expect(pruneSelection([], [makeRecord()])).toEqual([]);
    expect(pruneSelection(['apr-1'], [])).toEqual([]);
  });
});

describe('effectiveApprovalFilter', () => {
  const optionsFilter: ApprovalListFilter = { status: ['pending'], limit: 5 };

  it('returns the options filter when no override is set', () => {
    // #when / #then
    expect(effectiveApprovalFilter(null, optionsFilter)).toBe(optionsFilter);
  });

  it('honours an override created against the current options value', () => {
    // #given — override captured while optionsFilter had this exact value
    const override = {
      baseKey: approvalFilterKey(optionsFilter),
      filter: { requestedBy: 'ada' } satisfies ApprovalListFilter,
    };

    // #when / #then
    expect(effectiveApprovalFilter(override, optionsFilter)).toBe(
      override.filter,
    );
    // Value identity, not reference: an equal-value options literal still
    // honours the override (the inline-options caller shape).
    expect(
      effectiveApprovalFilter(override, { status: ['pending'], limit: 5 }),
    ).toBe(override.filter);
  });

  it('retires the override when the options filter VALUE changes', () => {
    // #given
    const override = {
      baseKey: approvalFilterKey(optionsFilter),
      filter: { requestedBy: 'ada' } satisfies ApprovalListFilter,
    };
    const changed: ApprovalListFilter = { status: ['claimed'], limit: 5 };

    // #when / #then — no reset effect needed; the derivation itself retires it
    expect(effectiveApprovalFilter(override, changed)).toBe(changed);
  });
});

// ---- Live stream subscription (subscribeApprovalStream) --------------------
// DOM-free: an injected FAKE transport drives live updates through the pure
// reducer, and a fake scheduler proves reconnect-with-backoff — no renderer, no
// browser WebSocket (that lives only in use-web-socket-transport.ts). This is
// the transport-wiring the hook's stream effect uses; the reducers themselves
// are covered in stream.test.ts.

interface FakeTransport {
  transport: StreamTransport;
  opens: Array<{ url: string; handlers: StreamHandlers }>;
  closes: () => number;
  sent: () => string[];
}

function makeFakeTransport(): FakeTransport {
  const opens: Array<{ url: string; handlers: StreamHandlers }> = [];
  let closed = 0;
  const sent: string[] = [];
  const transport: StreamTransport = {
    open(url, handlers): StreamConnection {
      opens.push({ url, handlers });
      return {
        send: (data) => {
          sent.push(data);
        },
        close: () => {
          closed += 1;
        },
      };
    },
  };
  return { transport, opens, closes: () => closed, sent: () => sent };
}

interface FakeScheduler {
  scheduler: StreamScheduler;
  runNext: () => void;
  pending: () => number;
}

function makeFakeScheduler(): FakeScheduler {
  const timers = new Map<number, () => void>();
  let nextId = 0;
  const scheduler: StreamScheduler = {
    setTimeout: (handler) => {
      nextId += 1;
      timers.set(nextId, handler);
      return nextId as unknown as ReturnType<typeof setTimeout>;
    },
    clearTimeout: (handle) => {
      timers.delete(handle as unknown as number);
    },
  };
  return {
    scheduler,
    runNext: () => {
      const entry = timers.entries().next();
      if (entry.done) return;
      const [id, handler] = entry.value;
      timers.delete(id);
      handler();
    },
    pending: () => timers.size,
  };
}

// Flush the ticket() microtask (subscribeApprovalStream opens after awaiting it).
async function flush(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

describe('subscribeApprovalStream', () => {
  it('opens the transport at url+ticket and live-merges a queue event through the reducer', async () => {
    // #given — a fake transport and a records array the onFrame handler merges into
    const { transport, opens } = makeFakeTransport();
    let records: ApprovalRecord[] = [makeRecord({ id: 'apr-existing' })];
    const connection = subscribeApprovalStream({
      transport,
      ticket: async () => ({
        url: 'wss://h/api/stream/hub?ticket=',
        ticket: 'TOK',
      }),
      onFrame: (frame) => {
        if (frame.type === 'queue') {
          records = mergeApprovalEvent(records, frame.event, {
            statuses: OPEN_STATUSES,
          });
        }
      },
    });
    await flush();

    // #then — opened once, the ticket concatenated onto the url
    expect(opens).toHaveLength(1);
    expect(opens[0]?.url).toBe('wss://h/api/stream/hub?ticket=TOK');

    // #when — the transport delivers a created event
    const created: ApprovalStreamEvent = {
      type: 'created',
      record: makeRecord({ id: 'apr-new', priority: 'critical' }),
    };
    opens[0]?.handlers.onMessage(
      JSON.stringify({ type: 'queue', event: created }),
    );

    // #then — the new record is live-merged (critical sorts ahead of the normal)
    expect(records.map((record) => record.id)).toEqual([
      'apr-new',
      'apr-existing',
    ]);
    connection.close();
  });

  it('ignores a malformed frame without throwing', async () => {
    // #given
    const { transport, opens } = makeFakeTransport();
    let frames = 0;
    const connection = subscribeApprovalStream({
      transport,
      ticket: async () => ({ url: 'u?t=', ticket: 'T' }),
      onFrame: () => {
        frames += 1;
      },
    });
    await flush();

    // #when — garbage arrives on the wire
    opens[0]?.handlers.onMessage('{not json');

    // #then — no frame routed
    expect(frames).toBe(0);
    connection.close();
  });

  it('reconnects with backoff after a disconnect', async () => {
    // #given
    const { transport, opens } = makeFakeTransport();
    const { scheduler, runNext, pending } = makeFakeScheduler();
    const connection = subscribeApprovalStream({
      transport,
      ticket: async () => ({ url: 'u?t=', ticket: 'T' }),
      onFrame: () => {},
      reconnectDelaysMs: [10],
      scheduler,
    });
    await flush();
    expect(opens).toHaveLength(1);

    // #when — the socket drops, then the backoff timer fires
    opens[0]?.handlers.onClose?.();
    expect(pending()).toBe(1);
    runNext();
    await flush();

    // #then — a fresh connection was opened
    expect(opens).toHaveLength(2);
    connection.close();
  });

  it('schedules exactly one reconnect when a socket fires onError then onClose', async () => {
    // #given
    const { transport, opens } = makeFakeTransport();
    const { scheduler, pending } = makeFakeScheduler();
    const connection = subscribeApprovalStream({
      transport,
      ticket: async () => ({ url: 'u?t=', ticket: 'T' }),
      onFrame: () => {},
      reconnectDelaysMs: [10],
      scheduler,
    });
    await flush();

    // #when — both fire for a single drop
    opens[0]?.handlers.onError?.(new Error('boom'));
    opens[0]?.handlers.onClose?.();

    // #then — one pending reconnect, not two
    expect(pending()).toBe(1);
    connection.close();
  });

  it('close() on a healthy connection closes the live socket', async () => {
    // #given
    const { transport, closes } = makeFakeTransport();
    const connection = subscribeApprovalStream({
      transport,
      ticket: async () => ({ url: 'u?t=', ticket: 'T' }),
      onFrame: () => {},
    });
    await flush();

    // #when
    connection.close();

    // #then
    expect(closes()).toBe(1);
  });

  it('pings after open and forces a reconnect when no pong arrives (F1 half-open liveness)', async () => {
    // #given — a socket that opens but then goes silently half-open (never fires
    // onClose): without a heartbeat the run poll would stay paused on stale state.
    const { transport, opens, sent } = makeFakeTransport();
    const { scheduler, runNext, pending } = makeFakeScheduler();
    const connection = subscribeApprovalStream({
      transport,
      ticket: async () => ({ url: 'u?t=', ticket: 'T' }),
      onFrame: () => {},
      reconnectDelaysMs: [10],
      scheduler,
      heartbeatMs: 100,
      heartbeatTimeoutMs: 50,
    });
    await flush();
    expect(opens).toHaveLength(1);

    // #when — the socket opens (schedules the heartbeat), then the heartbeat fires
    opens[0]?.handlers.onOpen?.();
    runNext(); // heartbeat: sends 'ping', arms the pong deadline

    // #then — a ping went out
    expect(sent()).toEqual(['ping']);

    // #when — no pong arrives; the pong deadline fires
    runNext(); // liveness deadline → force-disconnect → schedule reconnect
    await flush();

    // #then — the dead socket was closed and exactly one reconnect scheduled
    expect(pending()).toBe(1);
    runNext(); // fire the reconnect
    await flush();
    expect(opens).toHaveLength(2);
    connection.close();
  });

  it('an inbound frame clears the pong deadline so a live socket is not force-closed (F1)', async () => {
    // #given
    const { transport, opens, sent } = makeFakeTransport();
    const { scheduler, runNext } = makeFakeScheduler();
    const connection = subscribeApprovalStream({
      transport,
      ticket: async () => ({ url: 'u?t=', ticket: 'T' }),
      onFrame: () => {},
      reconnectDelaysMs: [10],
      scheduler,
      heartbeatMs: 100,
      heartbeatTimeoutMs: 50,
    });
    await flush();
    opens[0]?.handlers.onOpen?.();
    runNext(); // ping sent, pong deadline armed

    // #when — a pong (ignored by the frame parser) proves the socket is alive
    opens[0]?.handlers.onMessage('pong');
    runNext(); // the next heartbeat fires (the liveness timer was cleared, not fired)

    // #then — still one healthy socket (no reconnect), heartbeat kept pinging
    expect(opens).toHaveLength(1);
    expect(sent()).toEqual(['ping', 'ping']);
    connection.close();
  });

  it('stops retrying when the ticket route returns a permanent 4xx (F4 poll-only)', async () => {
    // #given — a host with no STREAM_TICKET_SECRET: the ticket route 404s. Retrying
    // cannot succeed, so the subscription must give up (not hammer 404s forever).
    const { transport, opens } = makeFakeTransport();
    const { scheduler, pending } = makeFakeScheduler();
    let ticketCalls = 0;
    const connection = subscribeApprovalStream({
      transport,
      ticket: async () => {
        ticketCalls += 1;
        throw Object.assign(new Error('not found'), { status: 404 });
      },
      onFrame: () => {},
      reconnectDelaysMs: [10],
      scheduler,
    });
    await flush();

    // #then — no socket opened, and NO reconnect scheduled (gave up after one try)
    expect(opens).toHaveLength(0);
    expect(pending()).toBe(0);
    expect(ticketCalls).toBe(1);
    connection.close();
  });

  it('keeps retrying on a transient ticket failure with no status (network error)', async () => {
    // #given — a transient failure (no HTTP status) must still back off and retry.
    const { transport } = makeFakeTransport();
    const { scheduler, pending } = makeFakeScheduler();
    const connection = subscribeApprovalStream({
      transport,
      ticket: async () => {
        throw new Error('network down');
      },
      onFrame: () => {},
      reconnectDelaysMs: [10],
      scheduler,
    });
    await flush();

    // #then — a reconnect IS scheduled (transient, unlike the permanent 4xx)
    expect(pending()).toBe(1);
    connection.close();
  });

  it('close() cancels a pending reconnect (a half-open socket never reopens)', async () => {
    // #given
    const { transport, opens } = makeFakeTransport();
    const { scheduler, runNext, pending } = makeFakeScheduler();
    const connection = subscribeApprovalStream({
      transport,
      ticket: async () => ({ url: 'u?t=', ticket: 'T' }),
      onFrame: () => {},
      reconnectDelaysMs: [10],
      scheduler,
    });
    await flush();

    // #when — a drop schedules a reconnect, then we close before it fires
    opens[0]?.handlers.onClose?.();
    connection.close();

    // #then — the reconnect is cancelled; firing a stale timer re-opens nothing
    expect(pending()).toBe(0);
    runNext();
    await flush();
    expect(opens).toHaveLength(1);
  });
});
