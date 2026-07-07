// Dev-only Vite plugin: mounts the REAL approval-api (in-memory store +
// service + fetch router) at /api/approvals inside the dev server, seeded with
// approvals spanning every status / priority / SLA state. The browser app's
// ApprovalApiClient talks to these routes, so claim / decide / delegate drive
// real CAS transitions and live metrics — a working backend, not a static mock.
//
// This file runs in the Node dev-server context (Vite transpiles it with
// esbuild); it is intentionally outside the browser tsconfig's `src` root.

import type { Connect, Plugin } from 'vite';

import type { ApprovalActor } from '../src/approval-api/contract.js';
import { createApprovalRouter } from '../src/approval-api/router.js';
import { ApprovalService } from '../src/approval-api/service.js';
import { InMemoryApprovalStore } from '../src/approval-api/store.js';

const BASE = '/api/approvals';

// The principal the router authenticates every request as (dev stand-in for
// session/JWT auth). admin may review + read. Deliberately distinct from the
// seeded requester so the separation-of-duties gate still permits decisions.
const DEV_ACTOR: ApprovalActor = { id: 'you@local', role: 'admin' };
const SEED_OPERATOR: ApprovalActor = { id: 'workflow-svc', role: 'operator' };
const SEED_REVIEWER: ApprovalActor = { id: 'alice', role: 'reviewer' };

// This function is responsible for populating the store with a realistic
// spread the dashboard can render. It works by:
// 1. creating pending requests with ok / warning / no SLA deadlines,
// 2. claiming one and deciding two (approve + reject) as a reviewer,
// 3. creating one already-overdue request via a past-clock service, then
//    sweeping so it escalates — exercising the breached + escalated badges.
async function seed(
  service: ApprovalService,
  pastService: ApprovalService,
): Promise<void> {
  await service.create(
    {
      workflowId: 'refunds',
      runId: 'run-1001',
      title: 'Refund $4,200 to ACME Corp',
      summary:
        'Customer disputed a duplicate charge; refund pre-approved by support.',
      connectors: ['stripe-refunds'],
      priority: 'critical',
      slaSeconds: 7200,
      payload: { amountUsd: 4200, customer: 'ACME Corp', reason: 'duplicate' },
    },
    SEED_OPERATOR,
  );
  await service.create(
    {
      workflowId: 'deploys',
      runId: 'run-1002',
      title: 'Promote build 8fefe22 to production',
      priority: 'high',
      slaSeconds: 600,
      payload: { commit: '8fefe22', env: 'production' },
    },
    SEED_OPERATOR,
  );
  await service.create(
    {
      workflowId: 'access',
      runId: 'run-1003',
      title: 'Grant read-only DB access to intern',
      priority: 'low',
    },
    SEED_OPERATOR,
  );

  const claimed = await service.create(
    {
      workflowId: 'emails',
      runId: 'run-1004',
      title: 'Send Q3 launch announcement',
      priority: 'normal',
      slaSeconds: 7200,
    },
    SEED_OPERATOR,
  );
  await service.claim(claimed.record.id, SEED_REVIEWER);

  const approved = await service.create(
    {
      workflowId: 'github',
      runId: 'run-1005',
      title: 'Merge hotfix PR #412',
      connectors: ['github-write'],
      priority: 'high',
      slaSeconds: 7200,
    },
    SEED_OPERATOR,
  );
  await service.decide(
    approved.record.id,
    { decision: 'approve', comment: 'Reviewed the diff — ship it.' },
    SEED_REVIEWER,
  );

  const rejected = await service.create(
    {
      workflowId: 'payouts',
      runId: 'run-1006',
      title: 'Wire $50,000 to new vendor',
      priority: 'critical',
      slaSeconds: 7200,
    },
    SEED_OPERATOR,
  );
  await service.decide(
    rejected.record.id,
    { decision: 'reject', comment: 'Vendor not yet verified.' },
    SEED_REVIEWER,
  );

  // Past-clock create → deadline already elapsed → sweepSLA escalates it.
  await pastService.create(
    {
      workflowId: 'incidents',
      runId: 'run-1007',
      title: 'Approve emergency prod rollback',
      priority: 'critical',
      slaSeconds: 600,
      payload: { incident: 'INC-238', severity: 'sev1' },
    },
    SEED_OPERATOR,
  );
  await service.sweepSLA(SEED_OPERATOR);
}

async function nodeToWebRequest(
  req: Connect.IncomingMessage,
): Promise<Request> {
  const url = `http://localhost${req.url ?? '/'}`;
  const method = req.method ?? 'GET';
  const headers = new Headers();
  for (const [key, value] of Object.entries(req.headers)) {
    if (typeof value === 'string') headers.set(key, value);
    else if (Array.isArray(value)) headers.set(key, value.join(', '));
  }
  if (method === 'GET' || method === 'HEAD') {
    return new Request(url, { method, headers });
  }
  const body = await new Promise<string>((resolve, reject) => {
    let data = '';
    req.setEncoding('utf8');
    req.on('data', (chunk: string) => {
      data += chunk;
    });
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
  return new Request(url, { method, headers, body });
}

export function approvalApiDevPlugin(): Plugin {
  const store = new InMemoryApprovalStore();
  const service = new ApprovalService({ store, defaultSlaSeconds: 3600 });
  // Second service over the SAME store, clocked one hour in the past — the only
  // way to seed an already-breached record, since create() always sets the
  // deadline in the future relative to its own clock.
  const pastService = new ApprovalService({
    store,
    now: () => new Date(Date.now() - 3_600_000),
  });
  const router = createApprovalRouter({
    service,
    authenticate: () => DEV_ACTOR,
    basePath: BASE, // single source of truth for both the gate and the router
  });
  const ready = seed(service, pastService).catch((error) => {
    console.error('[approval-api] seed failed:', error);
  });

  return {
    name: 'flowsafe-approval-api-dev',
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        const url = req.url ?? '';
        const isApi =
          url === BASE ||
          url.startsWith(`${BASE}/`) ||
          url.startsWith(`${BASE}?`);
        if (!isApi) {
          next();
          return;
        }
        void (async () => {
          try {
            await ready;
            const response = await router(await nodeToWebRequest(req));
            if (!response) {
              next();
              return;
            }
            res.statusCode = response.status;
            response.headers.forEach((value, key) => res.setHeader(key, value));
            res.end(await response.text());
          } catch (error) {
            res.statusCode = 500;
            res.setHeader('content-type', 'application/json');
            res.end(
              JSON.stringify({
                error: error instanceof Error ? error.message : String(error),
              }),
            );
          }
        })();
      });
    },
  };
}
