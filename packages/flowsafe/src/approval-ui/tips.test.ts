import { describe, expect, it } from 'vitest';

import type { ApprovalStatus } from '../approval-api/types.js';
import { APPROVAL_TIPS } from './tips.js';

// Record<ApprovalStatus, string> already forces completeness at compile time;
// this pins non-emptiness (a key set to '' would render a silent no-op tip).
const ALL_STATUSES: readonly ApprovalStatus[] = [
  'pending',
  'claimed',
  'approved',
  'rejected',
  'escalated',
];

const METRIC_LABELS = [
  'Open',
  'SLA breached',
  'Escalations',
  'Decided',
  'Approved',
  'Rejected',
  'Avg resolution',
] as const;

const CONCEPT_KEYS = [
  'sla',
  'statusColumn',
  'priority',
  'claim',
  'delegate',
  'grantsOnApprove',
  'decision',
] as const;

describe('APPROVAL_TIPS completeness', () => {
  it('covers every approval status with a non-empty tip', () => {
    for (const status of ALL_STATUSES) {
      expect(APPROVAL_TIPS.status[status].length).toBeGreaterThan(0);
    }
  });

  it('covers every metrics label with a non-empty tip', () => {
    for (const label of METRIC_LABELS) {
      expect(APPROVAL_TIPS.metrics[label].length).toBeGreaterThan(0);
    }
    expect(Object.keys(APPROVAL_TIPS.metrics).sort()).toEqual(
      [...METRIC_LABELS].sort(),
    );
  });

  it('covers every fixed concept key with a non-empty tip', () => {
    for (const key of CONCEPT_KEYS) {
      expect(APPROVAL_TIPS[key].length).toBeGreaterThan(0);
    }
  });
});
