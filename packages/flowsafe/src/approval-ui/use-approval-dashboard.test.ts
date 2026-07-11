// DOM-free: exercises the default-filter contract and the poll wiring
// (fetchDashboardSnapshot) without mounting the hook — approval-ui has no
// renderer (see components.test.ts's "hooks need a renderer, the merge does
// not"; README.md: "No jsdom render tests"). fetchDashboardSnapshot is
// exported from use-approval-dashboard.ts precisely so this filter-forwarding
// behavior — the D3 fix — is testable in plain node.

import { describe, expect, it } from 'vitest';
import type {
  ApprovalListFilter,
  ApprovalMetrics,
  ApprovalRecord,
} from '../approval-api/types.js';
import { OPEN_STATUSES } from '../approval-api/types.js';
import {
  approvalFilterKey,
  DEFAULT_QUEUE_FILTER,
  fetchDashboardSnapshot,
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
    tenantId: 'acme',
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
