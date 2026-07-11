// @vitest-environment happy-dom
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

import type { ApprovalMetrics } from '../approval-api/types.js';
import { ApprovalApiClient } from './client.js';
import { useApprovalDashboard } from './use-approval-dashboard.js';

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
