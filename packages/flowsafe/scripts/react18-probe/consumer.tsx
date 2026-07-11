// Compile-only consumer probe: proves the approval-ui d.ts EMITTED under
// @types/react@19 stays consumable at the react-18 peer floor (the package
// declares React >=18). The tsconfig beside this file maps react/react-dom
// onto aliased @types/react@18 packages and keeps skipLibCheck OFF — with
// skipLibCheck the probe is vacuous, since d.ts incompatibilities are exactly
// what it exists to catch. Never executed, never shipped; requires a prior
// `pnpm --filter @proofoftech/flowsafe build` (it types against ../../dist).

import type { ReactElement } from 'react';
import { useMemo } from 'react';

import type { ApprovalUIComponents } from '../../dist/approval-ui/index.js';
import {
  App,
  ApprovalApiClient,
  ApprovalUIProvider,
  createApprovalDashboard,
  FilterBar,
  htmlComponents,
  QueueView,
  useApprovalDashboard,
} from '../../dist/approval-ui/index.js';

export function Probe(): ReactElement {
  const client = useMemo(
    () =>
      new ApprovalApiClient({
        fetch: async () => ({ ok: true, status: 200, json: async () => [] }),
      }),
    [],
  );
  const dashboard = useApprovalDashboard(client, { pollIntervalMs: 0 });
  const components: Partial<ApprovalUIComponents> = {
    Text: htmlComponents.Text,
    Checkbox: htmlComponents.Checkbox,
    Select: htmlComponents.Select,
  };
  // Domain types (BatchDecideResult, ApprovalRecord, …) live on the package
  // ROOT barrel, whose d.ts chain pulls Mastra (and its node-typed deps) —
  // out of this probe's charter (the UI surface at the react-18 floor), so
  // the type is named via indexed access instead of an import.
  const lastBatch: ReturnType<typeof useApprovalDashboard>['lastBatch'] =
    dashboard.lastBatch;
  return (
    <ApprovalUIProvider components={components}>
      <App client={client} pollIntervalMs={5000} />
      <FilterBar
        filter={dashboard.filter}
        onApply={dashboard.setFilter}
        disabled={dashboard.busy}
      />
      <QueueView
        records={dashboard.records}
        nowMs={dashboard.nowMs}
        selectedId={dashboard.selectedId}
        onSelect={dashboard.select}
        selectedIds={dashboard.selectedIds}
        onToggleSelect={dashboard.toggleSelect}
      />
      {lastBatch === null ? null : <p>{lastBatch.decided}</p>}
    </ApprovalUIProvider>
  );
}

// The imperative mount surface must type at 18 too (react-dom/client).
export function mountProbe(container: Element): void {
  createApprovalDashboard(container, { client: new ApprovalApiClient({}) });
}
