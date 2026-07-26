// SPDX-License-Identifier: Apache-2.0
// Queue triage controls: status, workflow/run, reviewer/requester, and an
// age preset, applied as ONE dashboard.setFilter call on the Apply button —
// apply-on-click, not per-keystroke, because every filter change is a server
// fetch. The mapping from drafts to an ApprovalListFilter is a pure function
// (buildTriageFilter) so the age math and field trimming are DOM-free
// testable; the component is a thin slot-rendered shell around it.

import { type JSX, useState } from 'react';

import type {
  ApprovalListFilter,
  ApprovalStatus,
} from '../approval-api/types.js';
import { APPROVAL_STATUSES, OPEN_STATUSES } from '../approval-api/types.js';
import { type SelectOption, useApprovalUIComponents } from './components.js';
import { DEFAULT_QUEUE_FILTER } from './use-approval-dashboard.js';

/** Status choices: the open set (default), each status alone, or everything. */
export type StatusDraft = 'open' | 'all' | ApprovalStatus;

/** Age presets: "older than" cutoffs mapped to createdBefore at APPLY time. */
export const AGE_PRESETS = {
  any: 0,
  '1h': 3_600_000,
  '4h': 4 * 3_600_000,
  '24h': 24 * 3_600_000,
} as const;

export type AgeDraft = keyof typeof AGE_PRESETS;

export interface TriageFilterDrafts {
  status: StatusDraft;
  workflowId: string;
  runId: string;
  claimedBy: string;
  requestedBy: string;
  age: AgeDraft;
}

export const DEFAULT_TRIAGE_DRAFTS: TriageFilterDrafts = {
  status: 'open',
  workflowId: '',
  runId: '',
  claimedBy: '',
  requestedBy: '',
  age: 'any',
};

/** The status draft a filter round-trips to (seeds the Select from dashboard.filter). */
export function statusDraftOf(filter: ApprovalListFilter): StatusDraft {
  const status = filter.status;
  if (status === undefined) return 'all';
  const list = Array.isArray(status) ? status : [status];
  if (list.length === 1 && list[0] !== undefined) return list[0];
  // Multi-status lists have exactly one draft representation: the open set
  // (the default). Any other combination came from code, not this bar.
  return 'open';
}

/**
 * Drafts -> ApprovalListFilter, evaluated at APPLY time. `nowMs` is injected
 * (the click handler passes its clock) so the age cutoff is deterministic
 * under test and no Date.now sits in a render path. The base filter's
 * limit/orderBy carry over so the page bound and reviewer ranking are not
 * lost, while any `after` cursor is deliberately dropped: a new filter
 * starts at page one (and 'reviewer' order rejects cursors).
 */
export function buildTriageFilter(
  drafts: TriageFilterDrafts,
  base: Pick<ApprovalListFilter, 'limit' | 'orderBy'>,
  nowMs: number,
): ApprovalListFilter {
  const next: ApprovalListFilter = {};
  if (drafts.status === 'open') next.status = [...OPEN_STATUSES];
  else if (drafts.status !== 'all') next.status = drafts.status;
  const workflowId = drafts.workflowId.trim();
  if (workflowId !== '') next.workflowId = workflowId;
  const runId = drafts.runId.trim();
  if (runId !== '') next.runId = runId;
  const claimedBy = drafts.claimedBy.trim();
  if (claimedBy !== '') next.claimedBy = claimedBy;
  const requestedBy = drafts.requestedBy.trim();
  if (requestedBy !== '') next.requestedBy = requestedBy;
  if (drafts.age !== 'any') {
    next.createdBefore = new Date(
      nowMs - AGE_PRESETS[drafts.age],
    ).toISOString();
  }
  if (base.limit !== undefined) next.limit = base.limit;
  if (base.orderBy !== undefined) next.orderBy = base.orderBy;
  return next;
}

const STATUS_OPTIONS: readonly SelectOption[] = [
  { value: 'open', label: 'Open (pending, claimed, escalated)' },
  ...APPROVAL_STATUSES.map((status) => ({ value: status, label: status })),
  { value: 'all', label: 'All statuses' },
];

const AGE_OPTIONS: readonly SelectOption[] = [
  { value: 'any', label: 'Any age' },
  { value: '1h', label: 'Older than 1 hour' },
  { value: '4h', label: 'Older than 4 hours' },
  { value: '24h', label: 'Older than 24 hours' },
];

export interface FilterBarProps {
  /** The dashboard's EFFECTIVE filter (dashboard.filter) — seeds the drafts. */
  filter: ApprovalListFilter;
  /** dashboard.setFilter — called with the built filter on Apply/Reset. */
  onApply: (filter: ApprovalListFilter) => void;
  disabled?: boolean;
  /** Injectable clock for the age presets; read in the Apply handler only. */
  now?: () => number;
}

export function FilterBar({
  filter,
  onApply,
  disabled,
  now = Date.now,
}: FilterBarProps): JSX.Element {
  const C = useApprovalUIComponents();
  // Drafts seed ONCE from the mount-time filter (a draft UI must not clobber
  // half-typed input when the dashboard refreshes). A consumer that changes
  // useApprovalDashboard's `filter` OPTION dynamically — neither shipped host
  // does — should remount this bar keyed on that option's approvalFilterKey
  // so the drafts reseed with it.
  const [drafts, setDrafts] = useState<TriageFilterDrafts>(() => ({
    ...DEFAULT_TRIAGE_DRAFTS,
    status: statusDraftOf(filter),
    workflowId: filter.workflowId ?? '',
    runId: filter.runId ?? '',
    claimedBy: filter.claimedBy ?? '',
    requestedBy: filter.requestedBy ?? '',
  }));

  const setDraft = <K extends keyof TriageFilterDrafts>(
    key: K,
    value: TriageFilterDrafts[K],
  ): void => {
    setDrafts((current) => ({ ...current, [key]: value }));
  };

  return (
    <C.Section aria-label="Queue filters">
      <C.Stack direction="horizontal" gap="md">
        <C.Select
          label="Status"
          value={drafts.status}
          options={STATUS_OPTIONS}
          disabled={disabled}
          onChange={(value) => setDraft('status', value as StatusDraft)}
        />
        <C.TextField
          label="Workflow"
          value={drafts.workflowId}
          disabled={disabled}
          onChange={(value) => setDraft('workflowId', value)}
        />
        <C.TextField
          label="Run"
          value={drafts.runId}
          disabled={disabled}
          onChange={(value) => setDraft('runId', value)}
        />
        <C.TextField
          label="Claimed by"
          value={drafts.claimedBy}
          disabled={disabled}
          onChange={(value) => setDraft('claimedBy', value)}
        />
        <C.TextField
          label="Requested by"
          value={drafts.requestedBy}
          disabled={disabled}
          onChange={(value) => setDraft('requestedBy', value)}
        />
        <C.Select
          label="Age"
          value={drafts.age}
          options={AGE_OPTIONS}
          disabled={disabled}
          onChange={(value) => setDraft('age', value as AgeDraft)}
        />
        <C.Button
          label="Apply filters"
          variant="primary"
          disabled={disabled}
          onClick={() => onApply(buildTriageFilter(drafts, filter, now()))}
        />
        <C.Button
          label="Reset"
          variant="ghost"
          disabled={disabled}
          onClick={() => {
            setDrafts(DEFAULT_TRIAGE_DRAFTS);
            onApply(DEFAULT_QUEUE_FILTER);
          }}
        />
      </C.Stack>
    </C.Section>
  );
}
