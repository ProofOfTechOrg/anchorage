// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import type { ApprovalRecord } from '../approval-api/types.js';
import {
  formatDuration,
  formatResolution,
  formatSlaCountdown,
  msRemaining,
  slaStateOf,
  sortQueue,
} from './view-model.js';

const NOW = Date.parse('2026-07-06T12:00:00.000Z');

let seq = 0;

function record(overrides: Partial<ApprovalRecord> = {}): ApprovalRecord {
  seq += 1;
  const at = new Date(1700000000000 + seq * 1000).toISOString();
  return {
    id: `apr-${seq}`,
    workflowId: 'wf',
    runId: `run-${seq}`,
    title: `approval ${seq}`,
    connectors: [],
    priority: 'normal',
    status: 'pending',
    createdAt: at,
    updatedAt: at,
    ...overrides,
  };
}

describe('SLA math', () => {
  it('computes remaining milliseconds against the deadline', () => {
    // #given
    const due = record({ slaDeadlineAt: '2026-07-06T12:30:00.000Z' });

    // #when / #then
    expect(msRemaining(due, NOW)).toBe(30 * 60 * 1000);
    expect(msRemaining(record(), NOW)).toBeNull();
  });

  it('classifies none/ok/warning/breached', () => {
    // #given
    const ok = record({ slaDeadlineAt: '2026-07-06T14:00:00.000Z' });
    const warning = record({ slaDeadlineAt: '2026-07-06T12:10:00.000Z' });
    const breached = record({ slaDeadlineAt: '2026-07-06T11:59:00.000Z' });

    // #when / #then
    expect(slaStateOf(record(), NOW)).toBe('none');
    expect(slaStateOf(ok, NOW)).toBe('ok');
    expect(slaStateOf(warning, NOW)).toBe('warning');
    expect(slaStateOf(breached, NOW)).toBe('breached');
  });

  it('formats countdowns in both directions', () => {
    // #given
    const soon = record({ slaDeadlineAt: '2026-07-06T14:05:00.000Z' });
    const overdue = record({ slaDeadlineAt: '2026-07-06T11:57:00.000Z' });

    // #when / #then
    expect(formatSlaCountdown(soon, NOW)).toBe('2h 5m left');
    expect(formatSlaCountdown(overdue, NOW)).toBe('overdue by 3m');
    expect(formatSlaCountdown(record(), NOW)).toBe('no SLA');
  });
});

describe('formatDuration', () => {
  it('picks the two most significant units', () => {
    // #when / #then
    expect(formatDuration(45_000)).toBe('45s');
    expect(formatDuration(3 * 60_000)).toBe('3m');
    expect(formatDuration(2 * 3_600_000 + 5 * 60_000)).toBe('2h 5m');
    expect(formatDuration(50 * 3_600_000)).toBe('2d 2h');
    expect(formatDuration(0)).toBe('0s');
  });
});

describe('formatResolution', () => {
  it('renders a duration or a dash for null', () => {
    // #when / #then
    expect(formatResolution(120)).toBe('2m');
    expect(formatResolution(null)).toBe('—');
  });
});

describe('sortQueue', () => {
  it('orders by priority, then nearest deadline, then FIFO', () => {
    // #given
    const lowFirst = record({ priority: 'low' });
    const criticalLate = record({
      priority: 'critical',
      slaDeadlineAt: '2026-07-06T15:00:00.000Z',
    });
    const criticalSoon = record({
      priority: 'critical',
      slaDeadlineAt: '2026-07-06T12:30:00.000Z',
    });
    const normalNoSla = record({ priority: 'normal' });
    const normalWithSla = record({
      priority: 'normal',
      slaDeadlineAt: '2026-07-06T18:00:00.000Z',
    });

    // #when
    const sorted = sortQueue([
      lowFirst,
      criticalLate,
      criticalSoon,
      normalNoSla,
      normalWithSla,
    ]).map((entry) => entry.id);

    // #then — critical by deadline, then normal (deadline before none), low last
    expect(sorted).toEqual([
      criticalSoon.id,
      criticalLate.id,
      normalWithSla.id,
      normalNoSla.id,
      lowFirst.id,
    ]);
  });

  it('does not mutate its input', () => {
    // #given
    const records = [record({ priority: 'low' }), record({ priority: 'high' })];
    const snapshot = [...records];

    // #when
    sortQueue(records);

    // #then
    expect(records).toEqual(snapshot);
  });
});
