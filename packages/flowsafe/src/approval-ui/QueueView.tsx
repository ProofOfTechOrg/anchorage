import type { JSX } from 'react';

import type {
  ApprovalPriority,
  ApprovalRecord,
  ApprovalStatus,
} from '../approval-api/types.js';
import {
  type ApprovalColumn,
  type Tone,
  useApprovalUIComponents,
} from './components.js';
import { formatSlaCountdown, type SlaState, slaStateOf } from './view-model.js';

const STATUS_TONE: Record<ApprovalStatus, Tone> = {
  pending: 'neutral',
  claimed: 'info',
  approved: 'success',
  rejected: 'danger',
  escalated: 'warning',
};

const SLA_TONE: Record<SlaState, Tone> = {
  none: 'neutral',
  ok: 'success',
  warning: 'warning',
  breached: 'danger',
};

const PRIORITY_TONE: Record<ApprovalPriority, Tone> = {
  critical: 'danger',
  high: 'warning',
  normal: 'neutral',
  low: 'neutral',
};

export interface QueueViewProps {
  /** Pre-sorted (the dashboard hook applies sortQueue). */
  records: readonly ApprovalRecord[];
  nowMs: number;
  selectedId: string | null;
  onSelect: (id: string) => void;
}

export function QueueView({
  records,
  nowMs,
  selectedId,
  onSelect,
}: QueueViewProps): JSX.Element {
  const C = useApprovalUIComponents();

  // Selection lives in the Title cell: a focusable button per row keeps the
  // queue keyboard-navigable regardless of the injected Table implementation.
  const columns: ApprovalColumn[] = [
    {
      key: 'title',
      header: 'Title',
      renderCell: (record) => (
        <C.Button
          label={record.title}
          variant={record.id === selectedId ? 'secondary' : 'ghost'}
          pressed={record.id === selectedId}
          onClick={() => onSelect(record.id)}
        />
      ),
    },
    {
      key: 'run',
      header: 'Workflow',
      renderCell: (record) => `${record.workflowId}/${record.runId}`,
    },
    {
      key: 'priority',
      header: 'Priority',
      renderCell: (record) => (
        <C.Badge
          tone={PRIORITY_TONE[record.priority]}
          label={record.priority}
        />
      ),
    },
    {
      key: 'status',
      header: 'Status',
      renderCell: (record) => (
        <C.Badge tone={STATUS_TONE[record.status]} label={record.status} />
      ),
    },
    {
      key: 'sla',
      header: 'SLA',
      renderCell: (record) => (
        <C.Badge
          tone={SLA_TONE[slaStateOf(record, nowMs)]}
          label={formatSlaCountdown(record, nowMs)}
        />
      ),
    },
  ];

  // aria-label gives the queue table an accessible name (replacing the old
  // <caption>) — a <table> supports naming via its implicit role.
  return (
    <C.Table
      aria-label="Approval queue"
      data={records}
      columns={columns}
      idKey="id"
      emptyState={<C.EmptyState title="No approval requests." />}
    />
  );
}
