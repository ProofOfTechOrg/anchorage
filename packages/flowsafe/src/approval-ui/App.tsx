// SPDX-License-Identifier: Apache-2.0
import { type JSX, useState } from 'react';

import type { BatchDecideResult } from '../approval-api/types.js';
import type { ApprovalApiClient } from './client.js';
import { useApprovalUIComponents } from './components.js';
import { DetailView } from './DetailView.js';
import { FilterBar } from './FilterBar.js';
import { MetricsView } from './MetricsView.js';
import { QueueView } from './QueueView.js';
import { useApprovalDashboard } from './use-approval-dashboard.js';

/**
 * One line per failed record: "id: reason". The success side needs no prose;
 * the refreshed queue already shows decided records gone.
 */
export function batchFailureSummary(batch: BatchDecideResult): string {
  const failures = batch.results
    .flatMap((item) =>
      item.ok ? [] : [`${item.id}: ${item.error ?? item.code ?? 'failed'}`],
    )
    .join('; ');
  return `Batch decide: ${batch.decided} decided · ${batch.failed} failed. ${failures}`;
}

export interface ApprovalDashboardProps {
  client: ApprovalApiClient;
  /** Queue/metrics refresh cadence; <= 0 disables polling. Default 10s. */
  pollIntervalMs?: number;
  /** Injectable clock (deterministic SLA countdowns in tests/stories). */
  now?: () => number;
}

export function App({
  client,
  pollIntervalMs,
  now,
}: ApprovalDashboardProps): JSX.Element {
  const C = useApprovalUIComponents();
  const dashboard = useApprovalDashboard(client, { pollIntervalMs, now });
  const [batchComment, setBatchComment] = useState('');

  const decideSelected = (decision: 'approve' | 'reject'): void => {
    dashboard.decideSelected(decision, batchComment);
    setBatchComment('');
  };

  return (
    <C.Stack direction="vertical" gap="lg">
      <C.Heading level={1}>Approvals</C.Heading>
      {dashboard.error ? (
        <C.Banner tone="danger" title={dashboard.error} />
      ) : null}
      <MetricsView metrics={dashboard.metrics} />
      <FilterBar
        filter={dashboard.filter}
        onApply={dashboard.setFilter}
        disabled={dashboard.busy}
        now={now}
      />
      {dashboard.lastBatch && dashboard.lastBatch.failed > 0 ? (
        <C.Banner
          tone="warning"
          title={batchFailureSummary(dashboard.lastBatch)}
        />
      ) : null}
      {dashboard.selectedIds.length > 0 ? (
        <C.Section aria-label="Batch actions">
          <C.Stack direction="horizontal" gap="md">
            <C.Text>{dashboard.selectedIds.length} selected</C.Text>
            <C.TextField
              label="Batch comment"
              value={batchComment}
              disabled={dashboard.busy}
              onChange={setBatchComment}
            />
            <C.Button
              label="Approve selected"
              variant="primary"
              disabled={dashboard.busy}
              onClick={() => decideSelected('approve')}
            />
            <C.Button
              label="Reject selected"
              variant="danger"
              disabled={dashboard.busy}
              onClick={() => decideSelected('reject')}
            />
            <C.Button
              label="Clear selection"
              variant="ghost"
              disabled={dashboard.busy}
              onClick={dashboard.clearSelection}
            />
          </C.Stack>
        </C.Section>
      ) : null}
      <QueueView
        records={dashboard.records}
        nowMs={dashboard.nowMs}
        selectedId={dashboard.selectedId}
        onSelect={dashboard.select}
        selectedIds={dashboard.selectedIds}
        onToggleSelect={dashboard.toggleSelect}
      />
      {dashboard.selected ? (
        <DetailView
          // Keyed reset: remount on selection change so DetailView's local
          // comment/delegate state can't leak onto a different record.
          key={dashboard.selected.id}
          record={dashboard.selected}
          nowMs={dashboard.nowMs}
          busy={dashboard.busy}
          onClaim={dashboard.claim}
          onDecide={dashboard.decide}
          onDelegate={dashboard.delegate}
        />
      ) : null}
    </C.Stack>
  );
}
