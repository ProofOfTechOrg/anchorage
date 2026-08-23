// SPDX-License-Identifier: Apache-2.0
// Full-pipeline D3 proof: the real ApprovalApiClient → createApprovalRouter →
// ApprovalService → InMemoryApprovalStore, driven with NO limit at >MAX scale.
// Lives in approval-ui (not approval-api) because the "client" under test is
// approval-ui's ApprovalApiClient; approval-ui → approval-api is the allowed
// import direction. DOM-free (no hook, no JSX), so it runs in the main test
// pass like client.test.ts. Complements store.test.ts's store-layer reviewer
// tests (those use an EXPLICIT small limit) by exercising the DEFAULT bound
// through the whole stack.

import { describe, expect, it } from 'vitest';
import { createActorResolver } from '../approval-api/actor-context.js';
import type { ApprovalRole } from '../approval-api/contract.js';
import { createApprovalRouter } from '../approval-api/router.js';
import { ApprovalService } from '../approval-api/service.js';
import { InMemoryApprovalStoreFactory } from '../approval-api/store-factory.js';
import type { ApprovalRecord } from '../approval-api/types.js';
import {
  MAX_APPROVAL_LIST_LIMIT,
  OPEN_STATUSES,
} from '../approval-api/types.js';
import { ApprovalApiClient, type ResponseLike } from './client.js';

function seedRecord(
  index: number,
  overrides: Partial<ApprovalRecord> = {},
): ApprovalRecord {
  const at = new Date(1700000000000 + index * 1000).toISOString();
  return {
    id: `apr-${index}`,
    workflowId: 'wf',
    runId: `acme_run-${index}`,
    title: `approval ${index}`,
    connectors: [],
    priority: 'normal',
    status: 'pending',
    createdAt: at,
    updatedAt: at,
    ...overrides,
  };
}

describe('approval pipeline (client → router → service → store)', () => {
  it('a bare reviewer-ordered list() past MAX_APPROVAL_LIST_LIMIT bounds the page yet still surfaces the freshest critical at the top (D3)', async () => {
    // #given — a shared backend behind the router, seeded directly (fast) with
    // MAX normal requests plus one critical created LAST (newest createdAt, so
    // a FIFO-then-cap page would drop it at position MAX+1)
    const backend = new InMemoryApprovalStoreFactory();
    const store = backend.store();
    for (let index = 0; index < MAX_APPROVAL_LIST_LIMIT; index += 1) {
      await store.create(seedRecord(index));
    }
    const critical = seedRecord(MAX_APPROVAL_LIST_LIMIT, {
      id: 'apr-critical',
      runId: 'acme_run-hot',
      priority: 'critical',
    });
    await store.create(critical);

    const resolve = createActorResolver({
      authenticate: (request) => {
        const id = request.headers.get('x-actor-id');
        const role = request.headers.get('x-actor-role');
        return id && role ? { id, role: role as ApprovalRole } : undefined;
      },
      storeFactory: backend,
      // In-memory store, no database to fence against: the opt-out is written down
      // rather than defaulted — see ExecutionFenceWiring.
      buildService: (boundStore) =>
        new ApprovalService({ store: boundStore, executionFence: 'none' }),
    });
    const handle = createApprovalRouter({ resolve });

    // The real client over an in-process router (a native Response satisfies
    // ResponseLike).
    const client = new ApprovalApiClient({
      headers: {
        'x-actor-id': 'ray',
        'x-actor-role': 'reviewer',
      },
      fetch: async (url, init) => {
        const response = await handle(
          new Request(`http://queue.test${url}`, {
            method: init?.method ?? 'GET',
            headers: init?.headers,
            body: init?.body,
          }),
        );
        if (!response) throw new Error(`no route for ${url}`);
        return response as unknown as ResponseLike;
      },
    });

    // #when — no limit: exercises the D3 store-boundary default through the
    // whole stack
    const page = await client.list({
      status: [...OPEN_STATUSES],
      orderBy: 'reviewer',
    });

    // #then — bounded to the cap (never the full 501-row scan), and reviewer
    // order keeps the freshest critical visible at the top of that page
    expect(page).toHaveLength(MAX_APPROVAL_LIST_LIMIT);
    expect(page[0]?.id).toBe(critical.id);
  });
});
