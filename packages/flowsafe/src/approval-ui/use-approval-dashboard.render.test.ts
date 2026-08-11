// @vitest-environment happy-dom
// SPDX-License-Identifier: Apache-2.0
//
// The ONE renderer-backed suite in approval-ui (README "No jsdom render
// tests", amended 2026-07-11): the P1 filter-identity request loop lived in
// useApprovalDashboard's dependency WIRING — an inline `filter` option (new
// object identity every render) recreated refresh(), whose effect refetched,
// whose state updates re-rendered — and no pure extraction can execute that
// interplay. use-approval-dashboard.test.ts pins approvalFilterKey's
// stability; THIS mounts the hook and pins that it actually stops
// refetching. Deliberately minimal footprint: raw createRoot + act on
// happy-dom via the per-file docblock above — no @testing-library, no jsdom,
// no global environment change. Markup stays untested by design; this
// exception covers hook wiring only.
//
// Excluded from the package-level tsconfig.test.json (react-dom/client needs
// DOM types its workers-typed program lacks); the UI test pass owns it, like
// components.test.ts.

import { act, createElement } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { describe, expect, it } from 'vitest';

import type { ApprovalMetrics, ApprovalRecord } from '../approval-api/types.js';
import { ApprovalApiClient } from './client.js';
import {
  type ApprovalDashboardState,
  useApprovalDashboard,
} from './use-approval-dashboard.js';

// Required for act() with createRoot outside a framework that sets it.
(
  globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }
).IS_REACT_ACT_ENVIRONMENT = true;

const METRICS: ApprovalMetrics = {
  openCount: 0,
  slaBreachedCount: 0,
  escalationCount: 0,
  decidedCount: 0,
  approvedCount: 0,
  rejectedCount: 0,
  avgResolutionSeconds: null,
};

// A REAL ApprovalApiClient over a counting fake fetch (the designed seam) —
// the hook takes the class, not a structural pick. The list payload is ONE
// shared array instance so setRecords bails out (Object.is) after the first
// fetch: on the pre-fix hook that defuses the self-sustaining loop into a
// bounded fetch-per-render, letting the assertions below fail by COUNT
// instead of hanging the test.
const RECORDS: never[] = [];

function makeClient(): { client: ApprovalApiClient; listCalls: () => number } {
  let listCalls = 0;
  const client = new ApprovalApiClient({
    fetch: async (url) => {
      if (!url.includes('/metrics')) listCalls += 1;
      return {
        ok: true,
        status: 200,
        json: async () => (url.includes('/metrics') ? METRICS : RECORDS),
      };
    },
  });
  return { client, listCalls: () => listCalls };
}

// The caller shape from the review report: options built INLINE in the
// component body, so filter (and now) are fresh identities on every render.
function Probe(props: {
  client: ApprovalApiClient;
  limit: number;
  bump: number;
}): null {
  useApprovalDashboard(props.client, {
    pollIntervalMs: 0,
    now: () => 1_700_000_000_000,
    filter: { status: ['pending'], limit: props.limit },
  });
  return null;
}

async function renderProbe(
  root: Root,
  props: { client: ApprovalApiClient; limit: number; bump: number },
): Promise<void> {
  await act(async () => {
    root.render(createElement(Probe, props));
  });
  // Belt: flush the state updates queued by the post-fetch microtasks.
  await act(async () => {});
}

describe('useApprovalDashboard mounted (renderer-backed P1 regression)', () => {
  it('issues exactly ONE list fetch across mount + value-equal inline-option rerenders', async () => {
    // #given
    const { client, listCalls } = makeClient();
    const root = createRoot(document.createElement('div'));

    // #when — mount, then three rerenders each rebuilding the inline
    // filter/now literals (identity churn, identical values)
    await renderProbe(root, { client, limit: 5, bump: 0 });
    await renderProbe(root, { client, limit: 5, bump: 1 });
    await renderProbe(root, { client, limit: 5, bump: 2 });
    await renderProbe(root, { client, limit: 5, bump: 3 });

    // #then — one fetch: refresh()'s identity survived the rerenders, so the
    // effect never re-fired (the pre-fix hook fetches on every render here)
    expect(listCalls()).toBe(1);
    await act(async () => {
      root.unmount();
    });
  });

  it('still refetches immediately when the filter VALUE changes', async () => {
    // #given — mounted with limit 5 (one fetch)
    const { client, listCalls } = makeClient();
    const root = createRoot(document.createElement('div'));
    await renderProbe(root, { client, limit: 5, bump: 0 });

    // #when — a semantic change (status-tab-switch class), not identity churn
    await renderProbe(root, { client, limit: 10, bump: 1 });

    // #then
    expect(listCalls()).toBe(2);
    await act(async () => {
      root.unmount();
    });
  });

  it('still refetches immediately when the client identity changes (deliberate signal)', async () => {
    // #given — a new client is a new endpoint/authorization (actor switch)
    const first = makeClient();
    const second = makeClient();
    const root = createRoot(document.createElement('div'));
    await renderProbe(root, { client: first.client, limit: 5, bump: 0 });

    // #when
    await renderProbe(root, { client: second.client, limit: 5, bump: 1 });

    // #then — one fetch on each client
    expect(first.listCalls()).toBe(1);
    expect(second.listCalls()).toBe(1);
    await act(async () => {
      root.unmount();
    });
  });
});

// ---- Triage wiring (setFilter override + selection + decideSelected) ------
// Same charter as above: hook WIRING that no pure extraction can execute —
// the override's interplay with the value-keyed refresh, and decideSelected's
// closure over the derived-pruned selection. Markup stays untested.

function triageRecord(
  id: string,
  status: ApprovalRecord['status'],
): ApprovalRecord {
  return {
    id,
    workflowId: 'wf',
    runId: 'acme_r1',
    title: `request ${id}`,
    connectors: [],
    priority: 'normal',
    status,
    createdAt: '2026-07-06T12:00:00.000Z',
    updatedAt: '2026-07-06T12:00:00.000Z',
  };
}

function makeTriageClient(records: ApprovalRecord[]): {
  client: ApprovalApiClient;
  listUrls: string[];
  batchBodies: string[];
} {
  const listUrls: string[] = [];
  const batchBodies: string[] = [];
  const client = new ApprovalApiClient({
    fetch: async (url, init) => {
      if (url.includes('/batch/decide')) {
        batchBodies.push(init?.body ?? '');
        const { ids } = JSON.parse(init?.body ?? '{}') as { ids: string[] };
        return {
          ok: true,
          status: 200,
          json: async () => ({
            results: ids.map((id) => ({ id, ok: true })),
            decided: ids.length,
            failed: 0,
          }),
        };
      }
      if (url.includes('/metrics')) {
        return { ok: true, status: 200, json: async () => METRICS };
      }
      listUrls.push(url);
      return { ok: true, status: 200, json: async () => records };
    },
  });
  return { client, listUrls, batchBodies };
}

interface Capture {
  state?: ApprovalDashboardState;
}

// Inline options like the loop-regression Probe: the triage wiring must hold
// under the same identity-churn caller shape.
function TriageProbe(props: {
  client: ApprovalApiClient;
  limit: number;
  capture: Capture;
}): null {
  props.capture.state = useApprovalDashboard(props.client, {
    pollIntervalMs: 0,
    now: () => 1_700_000_000_000,
    filter: { status: ['pending'], limit: props.limit },
  });
  return null;
}

async function renderTriageProbe(
  root: Root,
  props: { client: ApprovalApiClient; limit: number; capture: Capture },
): Promise<void> {
  await act(async () => {
    root.render(createElement(TriageProbe, props));
  });
  await act(async () => {});
}

describe('useApprovalDashboard triage wiring (renderer-backed)', () => {
  it('setFilter refetches with the override; an options VALUE change retires it', async () => {
    // #given
    const { client, listUrls } = makeTriageClient([]);
    const capture: Capture = {};
    const root = createRoot(document.createElement('div'));
    await renderTriageProbe(root, { client, limit: 5, capture });
    expect(listUrls).toHaveLength(1);

    // #when — a FilterBar apply
    await act(async () => {
      capture.state?.setFilter({ requestedBy: 'ada', limit: 5 });
    });
    await act(async () => {});

    // #then — immediate refetch carrying the override
    expect(listUrls).toHaveLength(2);
    expect(listUrls[1]).toContain('requestedBy=ada');

    // Value-equal inline rerenders keep the override without refetching.
    await renderTriageProbe(root, { client, limit: 5, capture });
    expect(listUrls).toHaveLength(2);
    expect(capture.state?.filter).toEqual({ requestedBy: 'ada', limit: 5 });

    // #when — the OPTIONS filter value changes (caller-side tab switch)
    await renderTriageProbe(root, { client, limit: 10, capture });

    // #then — override retired on the same render: options win, no stale fetch
    expect(listUrls).toHaveLength(3);
    expect(listUrls[2]).toContain('limit=10');
    expect(listUrls[2]).not.toContain('requestedBy');
    await act(async () => {
      root.unmount();
    });
  });

  it('prunes selection to open page records and fans decideSelected through decideBatch', async () => {
    // #given — two decidable records and a decided one in the page
    const records = [
      triageRecord('open-1', 'pending'),
      triageRecord('open-2', 'escalated'),
      triageRecord('done-1', 'approved'),
    ];
    const { client, listUrls, batchBodies } = makeTriageClient(records);
    const capture: Capture = {};
    const root = createRoot(document.createElement('div'));
    await renderTriageProbe(root, { client, limit: 5, capture });

    // #when — selection includes a decided record and an id not in the page
    await act(async () => {
      capture.state?.toggleSelect('open-1');
      capture.state?.toggleSelect('open-2');
      capture.state?.toggleSelect('done-1');
      capture.state?.toggleSelect('ghost');
    });

    // #then — derived prune: only open, present ids survive
    expect(capture.state?.selectedIds).toEqual(['open-1', 'open-2']);

    // #when — one batch decision over the selection
    await act(async () => {
      capture.state?.decideSelected('approve', 'batch ok');
    });
    await act(async () => {});

    // #then — pruned ids hit /batch/decide once; envelope stored; selection
    // cleared; the queue refreshed
    expect(batchBodies).toEqual([
      JSON.stringify({
        ids: ['open-1', 'open-2'],
        decision: 'approve',
        comment: 'batch ok',
      }),
    ]);
    expect(capture.state?.lastBatch).toMatchObject({ decided: 2, failed: 0 });
    expect(capture.state?.selectedIds).toEqual([]);
    expect(listUrls.length).toBe(2);
    await act(async () => {
      root.unmount();
    });
  });

  it('decideSelected on an empty selection is a no-op', async () => {
    // #given
    const { client, batchBodies } = makeTriageClient([]);
    const capture: Capture = {};
    const root = createRoot(document.createElement('div'));
    await renderTriageProbe(root, { client, limit: 5, capture });

    // #when
    await act(async () => {
      capture.state?.decideSelected('approve', '');
    });

    // #then
    expect(batchBodies).toEqual([]);
    await act(async () => {
      root.unmount();
    });
  });
});
