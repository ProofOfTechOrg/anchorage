// SPDX-License-Identifier: Apache-2.0
import type { JSX } from 'react';

import type { ApprovalMetrics } from '../approval-api/types.js';
import { useApprovalUIComponents } from './components.js';
import { APPROVAL_TIPS, type MetricLabel } from './tips.js';
import { formatResolution } from './view-model.js';

export interface MetricsViewProps {
  metrics: ApprovalMetrics | null;
}

export function MetricsView({ metrics }: MetricsViewProps): JSX.Element {
  const C = useApprovalUIComponents();
  if (!metrics) return <C.Spinner label="Loading metrics…" />;
  // MetricLabel pins each label to an APPROVAL_TIPS.metrics entry at compile
  // time, so a renamed metric cannot silently lose its tip.
  const cells: Array<[MetricLabel, string | number]> = [
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
          <C.InfoTip label={value} tip={APPROVAL_TIPS.metrics[label]} />
        </C.MetadataItem>
      ))}
    </C.MetadataList>
  );
}
