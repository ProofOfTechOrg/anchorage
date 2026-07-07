import type { JSX } from 'react';

import type { ApprovalMetrics } from '../approval-api/types.js';
import { useApprovalUIComponents } from './components.js';
import { formatResolution } from './view-model.js';

export interface MetricsViewProps {
  metrics: ApprovalMetrics | null;
}

export function MetricsView({ metrics }: MetricsViewProps): JSX.Element {
  const C = useApprovalUIComponents();
  if (!metrics) return <C.Spinner label="Loading metrics…" />;
  const cells: Array<[string, string | number]> = [
    ['Open', metrics.openCount],
    ['SLA breached', metrics.slaBreachedCount],
    ['Escalations', metrics.escalationCount],
    ['Decided', metrics.decidedCount],
    ['Approved', metrics.approvedCount],
    ['Rejected', metrics.rejectedCount],
    ['Avg resolution', formatResolution(metrics.avgResolutionSeconds)],
  ];
  return (
    <C.MetadataList>
      {cells.map(([label, value]) => (
        <C.MetadataItem key={label} label={label}>
          {value}
        </C.MetadataItem>
      ))}
    </C.MetadataList>
  );
}
