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
  it('scopes the queue to open (non-terminal) statuses, bounded at 100', () => {
    // #then
    expect(DEFAULT_QUEUE_FILTER).toEqual({
      status: [...OPEN_STATUSES],
      limit: 100,
    });
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
