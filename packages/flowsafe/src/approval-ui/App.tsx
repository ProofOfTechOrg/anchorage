import type { JSX } from 'react';

import type { ApprovalApiClient } from './client.js';
import { useApprovalUIComponents } from './components.js';
import { DetailView } from './DetailView.js';
import { MetricsView } from './MetricsView.js';
import { QueueView } from './QueueView.js';
import { useApprovalDashboard } from './use-approval-dashboard.js';

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

  return (
    <C.Stack direction="vertical" gap="lg">
      <C.Heading level={1}>Approvals</C.Heading>
      {dashboard.error ? (
        <C.Banner tone="danger" title={dashboard.error} />
      ) : null}
      <MetricsView metrics={dashboard.metrics} />
      <QueueView
        records={dashboard.records}
        nowMs={dashboard.nowMs}
        selectedId={dashboard.selectedId}
        onSelect={dashboard.select}
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
