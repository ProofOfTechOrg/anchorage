// SPDX-License-Identifier: Apache-2.0
// DOM-free: the live-stream reducers are pure, so they run under plain node
// (this file is in the workers-typed test pass — no DOM/JSX). They carry all
// the live-merge / optimistic-decide / reconcile logic, keeping the React hook
// thin (README "No jsdom render tests").

import { describe, expect, it } from 'vitest';

import type { ApprovalStreamEvent } from '../approval-api/contract.js';
import type {
  ApprovalMetrics,
  ApprovalRecord,
  ApprovalStatus,
} from '../approval-api/types.js';
import {
  OPEN_STATUSES,
  TERMINAL_APPROVAL_STATUSES,
} from '../approval-api/types.js';
import {
  applyMetricsDelta,
  applyOptimisticDecide,
  mergeApprovalEvent,
  type PendingDecisions,
  parseStreamFrame,
  presenceReducer,
  reconcileDecided,
} from './stream.js';

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

function streamEvent(
  type: ApprovalStreamEvent['type'],
  record: ApprovalRecord,
): ApprovalStreamEvent {
  return { type, record };
}

const METRICS: ApprovalMetrics = {
  openCount: 5,
  slaBreachedCount: 1,
  escalationCount: 2,
  decidedCount: 10,
  approvedCount: 7,
  rejectedCount: 3,
  avgResolutionSeconds: 42,
};

describe('mergeApprovalEvent', () => {
  it('upserts a newly created record and re-sorts into reviewer order', () => {
    // #given — an existing normal-priority record
    const normal = makeRecord({ id: 'apr-normal', priority: 'normal' });
    const critical = makeRecord({ id: 'apr-critical', priority: 'critical' });

    // #when — a critical request is created live
    const merged = mergeApprovalEvent(
      [normal],
      streamEvent('created', critical),
    );

    // #then — inserted AND surfaced first (byReviewerOrder, critical < normal)
    expect(merged.map((record) => record.id)).toEqual([
      'apr-critical',
      'apr-normal',
    ]);
  });

  it('replaces an existing record in place on a claim (upsert by id, no duplicate)', () => {
    // #given
    const pending = makeRecord({ id: 'apr-1', status: 'pending' });
    const claimed = makeRecord({
      id: 'apr-1',
      status: 'claimed',
      claimedBy: 'ray',
    });

    // #when
    const merged = mergeApprovalEvent(
      [pending],
      streamEvent('claimed', claimed),
    );

    // #then — one record, now claimed
    expect(merged).toHaveLength(1);
    expect(merged[0]?.status).toBe('claimed');
    expect(merged[0]?.claimedBy).toBe('ray');
  });

  it('drops a decided record from the OPEN queue (it left the status filter)', () => {
    // #given — an open queue holding one pending record
    const pending = makeRecord({ id: 'apr-1', status: 'pending' });
    const decided = makeRecord({
      id: 'apr-1',
      status: 'approved',
      decision: 'approve',
      decidedBy: 'ada',
    });

    // #when — it is decided live, under the open-statuses filter
    const merged = mergeApprovalEvent(
      [pending],
      streamEvent('decided', decided),
      { statuses: OPEN_STATUSES },
    );

    // #then — removed from the open queue
    expect(merged).toEqual([]);
  });

  it('does NOT add a freshly created record to a decided-history view', () => {
    // #given — a history view (terminal statuses only)
    const decided = makeRecord({ id: 'apr-old', status: 'approved' });
    const created = makeRecord({ id: 'apr-new', status: 'pending' });

    // #when — a new pending record is created
    const merged = mergeApprovalEvent(
      [decided],
      streamEvent('created', created),
      { statuses: TERMINAL_APPROVAL_STATUSES },
    );

    // #then — the pending record never enters the decided view
    expect(merged.map((record) => record.id)).toEqual(['apr-old']);
  });

  it('keeps every status when no status filter is supplied', () => {
    // #given
    const pending = makeRecord({ id: 'apr-1', status: 'pending' });
    const decided = makeRecord({ id: 'apr-1', status: 'approved' });

    // #when — no statuses option: an all-status view
    const merged = mergeApprovalEvent(
      [pending],
      streamEvent('decided', decided),
    );

    // #then — the decided record is retained (upserted), not dropped
    expect(merged).toHaveLength(1);
    expect(merged[0]?.status).toBe('approved');
  });
});

describe('applyOptimisticDecide', () => {
  it('marks the record decided and returns a pending descriptor', () => {
    // #given
    const record = makeRecord({ id: 'apr-1', status: 'pending' });

    // #when
    const { records, pending } = applyOptimisticDecide(
      [record],
      'apr-1',
      'approve',
      'ada',
    );

    // #then — greyed: status approved, decidedBy set; pending tracked
    expect(records[0]).toMatchObject({
      status: 'approved',
      decision: 'approve',
      decidedBy: 'ada',
    });
    expect(pending).toEqual({
      id: 'apr-1',
      decision: 'approve',
      actorId: 'ada',
    });
  });

  it('marks a reject as rejected', () => {
    // #when
    const { records } = applyOptimisticDecide(
      [makeRecord({ id: 'apr-1' })],
      'apr-1',
      'reject',
      'ada',
    );

    // #then
    expect(records[0]?.status).toBe('rejected');
  });

  it('leaves other records untouched', () => {
    // #given
    const target = makeRecord({ id: 'apr-1' });
    const other = makeRecord({ id: 'apr-2' });

    // #when
    const { records } = applyOptimisticDecide(
      [target, other],
      'apr-1',
      'approve',
      'ada',
    );

    // #then
    expect(records[1]).toBe(other);
  });
});

describe('reconcileDecided', () => {
  const pending: PendingDecisions = {
    'apr-1': { id: 'apr-1', decision: 'approve', actorId: 'ada' },
  };

  it('clears the pending entry when the same reviewer is the authoritative decider', () => {
    // #given — the hub echoes OUR own decision back
    const event = streamEvent(
      'decided',
      makeRecord({ id: 'apr-1', status: 'approved', decidedBy: 'ada' }),
    );

    // #when
    const result = reconcileDecided(pending, event);

    // #then — pending cleared, no conflict
    expect(result.pending).toEqual({});
    expect(result.conflict).toBeUndefined();
  });

  it('surfaces a conflict when a DIFFERENT reviewer decided first', () => {
    // #given — someone else decided the record we optimistically approved
    const event = streamEvent(
      'decided',
      makeRecord({ id: 'apr-1', status: 'rejected', decidedBy: 'ray' }),
    );

    // #when
    const result = reconcileDecided(pending, event);

    // #then — conflict names the actual decider; pending still cleared
    expect(result.conflict).toEqual({ id: 'apr-1', actualDecider: 'ray' });
    expect(result.pending).toEqual({});
  });

  it('clears without a conflict when the optimistic actorId is empty (no id to compare)', () => {
    // #given — the hook had no actorId, so it tracked an empty actor
    const anon: PendingDecisions = {
      'apr-1': { id: 'apr-1', decision: 'approve', actorId: '' },
    };
    const event = streamEvent(
      'decided',
      makeRecord({ id: 'apr-1', status: 'approved', decidedBy: 'ray' }),
    );

    // #when
    const result = reconcileDecided(anon, event);

    // #then — cleared, but no false conflict
    expect(result.pending).toEqual({});
    expect(result.conflict).toBeUndefined();
  });

  it('is a no-op for an event whose record we never optimistically decided', () => {
    // #when
    const event = streamEvent(
      'decided',
      makeRecord({ id: 'apr-other', decidedBy: 'ray' }),
    );
    const result = reconcileDecided(pending, event);

    // #then — pending untouched, no conflict
    expect(result.pending).toBe(pending);
    expect(result.conflict).toBeUndefined();
  });
});

describe('presenceReducer', () => {
  it('dedupes reviewers by actorId and sorts by actorId', () => {
    // #given — a roster where "ada" appears twice (two tabs)
    const roster = [
      { actorId: 'ray', role: 'reviewer' },
      { actorId: 'ada', role: 'admin' },
      { actorId: 'ada', role: 'admin' },
    ];

    // #when
    const distinct = presenceReducer(roster);

    // #then — one entry per reviewer, sorted
    expect(distinct).toEqual([
      { actorId: 'ada', role: 'admin' },
      { actorId: 'ray', role: 'reviewer' },
    ]);
  });

  it('returns an empty roster unchanged', () => {
    // #when / #then
    expect(presenceReducer([])).toEqual([]);
  });
});

describe('applyMetricsDelta', () => {
  it('increments openCount on a created event', () => {
    // #when
    const next = applyMetricsDelta(
      METRICS,
      streamEvent('created', makeRecord()),
    );

    // #then
    expect(next?.openCount).toBe(6);
  });

  it('moves an approval from open to decided/approved', () => {
    // #when
    const next = applyMetricsDelta(
      METRICS,
      streamEvent('decided', makeRecord({ decision: 'approve' })),
    );

    // #then
    expect(next).toMatchObject({
      openCount: 4,
      decidedCount: 11,
      approvedCount: 8,
      rejectedCount: 3,
    });
  });

  it('counts a rejection under rejectedCount', () => {
    // #when
    const next = applyMetricsDelta(
      METRICS,
      streamEvent('decided', makeRecord({ decision: 'reject' })),
    );

    // #then
    expect(next).toMatchObject({ decidedCount: 11, rejectedCount: 4 });
  });

  it('increments escalationCount on an escalated event', () => {
    // #when
    const next = applyMetricsDelta(
      METRICS,
      streamEvent('escalated', makeRecord({ status: 'escalated' })),
    );

    // #then
    expect(next?.escalationCount).toBe(3);
  });

  it('leaves the counters untouched (same reference) on a claim', () => {
    // #when
    const next = applyMetricsDelta(
      METRICS,
      streamEvent('claimed', makeRecord()),
    );

    // #then — no derivable movement, so no re-render churn
    expect(next).toBe(METRICS);
  });

  it('never lets openCount go negative', () => {
    // #given — metrics already at zero open
    const zeroOpen: ApprovalMetrics = { ...METRICS, openCount: 0 };

    // #when — a decided event arrives (optimistic drift)
    const next = applyMetricsDelta(
      zeroOpen,
      streamEvent('decided', makeRecord({ decision: 'approve' })),
    );

    // #then — clamped at zero, not -1
    expect(next?.openCount).toBe(0);
  });

  it('returns null when metrics have not loaded yet', () => {
    // #when / #then
    expect(
      applyMetricsDelta(null, streamEvent('created', makeRecord())),
    ).toBeNull();
  });
});

describe('parseStreamFrame', () => {
  it('parses a well-formed queue frame', () => {
    // #given
    const raw = JSON.stringify(streamEventFrame());

    // #when
    const frame = parseStreamFrame(raw);

    // #then
    expect(frame?.type).toBe('queue');
  });

  it('parses a presence frame', () => {
    // #when
    const frame = parseStreamFrame(
      JSON.stringify({ type: 'presence', roster: [] }),
    );

    // #then
    expect(frame).toEqual({ type: 'presence', roster: [] });
  });

  it('returns undefined for malformed JSON', () => {
    // #when / #then
    expect(parseStreamFrame('{not json')).toBeUndefined();
  });

  it('returns undefined for an unknown frame type', () => {
    // #when / #then
    expect(
      parseStreamFrame(JSON.stringify({ type: 'gossip' })),
    ).toBeUndefined();
  });
});

function streamEventFrame(): { type: 'queue'; event: ApprovalStreamEvent } {
  const status: ApprovalStatus = 'pending';
  return {
    type: 'queue',
    event: streamEvent('created', makeRecord({ status })),
  };
}
