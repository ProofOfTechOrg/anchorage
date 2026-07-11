// SPDX-License-Identifier: Apache-2.0
// DOM-free: FilterBar's mapping from drafts to an ApprovalListFilter is the
// pure buildTriageFilter (exported for exactly this), so the age math, status
// mapping, and field trimming are pinned without a renderer — the component
// itself is a thin slot shell (README "No jsdom render tests"). The age
// cutoff takes an injected nowMs; no Date.now in any render path.

import { describe, expect, it } from 'vitest';

import type { ApprovalListFilter } from '../approval-api/types.js';
import { OPEN_STATUSES } from '../approval-api/types.js';
import {
  AGE_PRESETS,
  buildTriageFilter,
  DEFAULT_TRIAGE_DRAFTS,
  statusDraftOf,
} from './FilterBar.js';
import { DEFAULT_QUEUE_FILTER } from './use-approval-dashboard.js';

const NOW_MS = Date.parse('2026-07-11T12:00:00.000Z');

describe('buildTriageFilter', () => {
  it('maps the default drafts to the open-statuses filter, keeping the base bound and order', () => {
    // #when
    const filter = buildTriageFilter(
      DEFAULT_TRIAGE_DRAFTS,
      DEFAULT_QUEUE_FILTER,
      NOW_MS,
    );

    // #then — D3 page bound + reviewer ranking carry over; no time bound
    expect(filter).toEqual({
      status: [...OPEN_STATUSES],
      limit: 100,
      orderBy: 'reviewer',
    });
  });

  it('maps a single status, trims text fields, and drops empty ones', () => {
    // #when
    const filter = buildTriageFilter(
      {
        ...DEFAULT_TRIAGE_DRAFTS,
        status: 'escalated',
        workflowId: '  lead-gen  ',
        runId: '',
        claimedBy: 'ray',
        requestedBy: '   ',
      },
      {},
      NOW_MS,
    );

    // #then
    expect(filter).toEqual({
      status: 'escalated',
      workflowId: 'lead-gen',
      claimedBy: 'ray',
    });
  });

  it("maps 'all' to no status filter at all", () => {
    // #when
    const filter = buildTriageFilter(
      { ...DEFAULT_TRIAGE_DRAFTS, status: 'all' },
      {},
      NOW_MS,
    );

    // #then
    expect(filter.status).toBeUndefined();
  });

  it.each([
    ['1h', '2026-07-11T11:00:00.000Z'],
    ['4h', '2026-07-11T08:00:00.000Z'],
    ['24h', '2026-07-10T12:00:00.000Z'],
  ] as const)("maps age '%s' to createdBefore = now minus the preset (injected clock)", (age, expected) => {
    // #when — evaluated at APPLY time with the injected nowMs
    const filter = buildTriageFilter(
      { ...DEFAULT_TRIAGE_DRAFTS, age },
      {},
      NOW_MS,
    );

    // #then
    expect(filter.createdBefore).toBe(expected);
    expect(NOW_MS - Date.parse(expected)).toBe(AGE_PRESETS[age]);
  });

  it("never carries an 'after' cursor from the base (new filter = page one)", () => {
    // #given — a base filter mid-pagination
    const base: ApprovalListFilter = {
      limit: 50,
      orderBy: 'created',
      after: 'CURSOR',
    } as ApprovalListFilter;

    // #when
    const filter = buildTriageFilter(DEFAULT_TRIAGE_DRAFTS, base, NOW_MS);

    // #then — limit/orderBy carried, cursor dropped
    expect(filter.limit).toBe(50);
    expect(filter.orderBy).toBe('created');
    expect(filter).not.toHaveProperty('after');
  });
});

describe('statusDraftOf', () => {
  it('round-trips the open set, a single status, and the absent filter', () => {
    // #then
    expect(statusDraftOf({ status: [...OPEN_STATUSES] })).toBe('open');
    expect(statusDraftOf({ status: 'rejected' })).toBe('rejected');
    expect(statusDraftOf({ status: ['claimed'] })).toBe('claimed');
    expect(statusDraftOf({})).toBe('all');
  });

  it('falls back to the open set for a multi-status list the bar cannot represent', () => {
    // #then
    expect(statusDraftOf({ status: ['approved', 'rejected'] })).toBe('open');
  });
});
