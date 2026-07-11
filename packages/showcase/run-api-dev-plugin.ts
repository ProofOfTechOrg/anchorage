// Dev-only Vite plugin: the showcase host, in-process. It builds ONE
// buildShowcaseRuntime (all five workflows) over an InMemoryApprovalStoreFactory
// (INV-2: request-scoped tenant-bound stores over one shared backend), wired
// with the host-kit re-queue bridge so decisions actually resume the run (and
// multi-gate runs re-queue the next gate). It mounts the same two routers the
// deployed worker does — createApprovalRouter at /api/approvals and host-kit's
// createRunRouter at /workflows + /runs — over the same TenantResolver seam and
// the same demo tokens the UI offers. Only the topology differs: runs resume
// in-process instead of through a DO stub (so the grant provider recovers each
// leg's tenant from the runId prefix — approvalGrantProviderFromFactory). So
// `pnpm dev` is a real working backend with the real RBAC gates: launch a
// workflow, approve it in the dashboard, watch it run to success. No seeds — the
// queue fills as you launch.
//
// Runs in the Node dev-server context (Vite transpiles with esbuild), outside the
// browser tsconfig's `src` root.

import { InMemoryStore } from '@mastra/core/storage';
import type {
  ApprovalActor,
  TenantBoundApprovalStore,
} from '@proofoftech/flowsafe/approval-api';
import {
  ApprovalService,
  approvalGrantProviderFromFactory,
  createApprovalRouter,
  createTenantResolver,
  InMemoryApprovalStoreFactory,
  resumeViaRuntime,
} from '@proofoftech/flowsafe/approval-api';
import { tenantOfRunId } from '@proofoftech/flowsafe/do-runner';
import {
  bearerActorAuthenticator,
  createRunRouter,
  parseActorTokens,
  reconcileApprovalsOnStatus,
  resumeRunWithRequeue,
  staticTokenVerifier,
} from '@proofoftech/flowsafe/host-kit';
import type { Connect, Plugin } from 'vite';
import { DEMO_TENANT_ID, demoActorTokensJson } from './worker/demo-actors.js';
import { createDemoResetRouter } from './worker/demo-reset.js';
import { buildShowcaseRuntime, SHOWCASE_MODULES } from './worker/runtime.js';

const APPROVAL_BASE = '/api/approvals';

/** Id for system-created approval records (tenant is bound per request). */
const SYSTEM_ACTOR_ID = 'showcase-dev';

// Same auth seam as the deployed worker, over the same demo tokens the UI's
// ActorSwitcher offers — routed through parseActorTokens so dev exercises the
// production parse path rather than a hand-rolled map.
const authenticate = bearerActorAuthenticator(
  staticTokenVerifier(parseActorTokens(demoActorTokensJson())),
);

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

export function runApiDevPlugin(): Plugin {
  const storeFactory = new InMemoryApprovalStoreFactory();
  // Held by name so the /demo/reset seam below can purge its workflow rows.
  const storage = new InMemoryStore();
  const runtime = buildShowcaseRuntime({
    initInput: { storage },
    // In-process host: one runtime serves every tenant, so the provider binds
    // per LEG from the runId's tenant prefix (the DO topology binds
    // per-instance instead).
    grantProvider: approvalGrantProviderFromFactory(storeFactory),
    // Egress/artifact bindings unset => connectors simulate / write in-memory.
  });
  // Per-tenant service assembly, invoked lazily by the resolver. The service
  // forward-references itself in the resumeRun closure (invoked only on a
  // later decision) — the same const-with-deferred-ref pattern the worker
  // uses. resumeRunWithRequeue resumes in-process and re-queues the next gate.
  function buildService(store: TenantBoundApprovalStore): ApprovalService {
    const systemActor: ApprovalActor = {
      id: SYSTEM_ACTOR_ID,
      role: 'operator',
      tenantId: store.tenantId,
    };
    const service: ApprovalService = new ApprovalService({
      store,
      defaultSlaSeconds: 3600,
      resumeRun: resumeRunWithRequeue(
        resumeViaRuntime(runtime),
        () => service,
        systemActor,
      ),
    });
    return service;
  }
  const resolve = createTenantResolver({
    authenticate,
    storeFactory,
    buildService,
  });
  const approvalRouter = createApprovalRouter({
    resolve,
    basePath: APPROVAL_BASE,
  });
  // The same run surface the deployed worker mounts; only the topology differs
  // (in-process runtime instead of a DO stub), so the RBAC gates and the
  // suspension bridge are the shared, tested ones.
  const runRouter = createRunRouter({
    workflows: SHOWCASE_MODULES.map((entry) => entry.meta),
    resolve,
    systemActorId: SYSTEM_ACTOR_ID,
    start: (workflowId, runId, inputData) =>
      runtime.start(workflowId, { runId, inputData }),
    status: async (workflowId, runId) =>
      (await runtime.status(workflowId, runId)) ?? undefined,
    resume: (workflowId, runId, body) => {
      const { step, resumeData } = (body ?? {}) as {
        step?: string | string[];
        resumeData?: unknown;
      };
      return runtime.resume(workflowId, runId, { step, resumeData });
    },
    // D4: heal a suspended run whose approval never made it into the queue
    // on the next status() poll (see reconcileApprovalsOnStatus).
    reconcileApprovals: reconcileApprovalsOnStatus(SYSTEM_ACTOR_ID),
  });
  // The same reset router the deployed worker mounts, over in-memory seams:
  // every dev identity shares the 'demo' tenant (no D1 registry exists here,
  // so the discriminator is the constant), snapshots purge through Mastra's
  // workflows domain, approvals through the shared-Map factory. The runtime's
  // in-memory idempotency + artifact state is not cleared — mirrors the
  // deployed worker's orphaned-DO-state posture (runIds are never reused).
  const resetRouter = createDemoResetRouter({
    resolve,
    isDemoTenant: async (tenantId) => tenantId === DEMO_TENANT_ID,
    purgeTenantData: async (tenantId) => {
      let snapshots = 0;
      const workflows = await storage.getStore('workflows');
      if (workflows) {
        // listWorkflowRuns() paginates only when BOTH perPage and page are
        // given, so this enumerates every run. Ownership goes through the ONE
        // INV-1 decode (tenantOfRunId) rather than a hand-rolled prefix test —
        // the same reason D1's purge uses an exact range predicate.
        const { runs } = await workflows.listWorkflowRuns();
        for (const run of runs) {
          if (tenantOfRunId(run.runId) === tenantId) {
            await workflows.deleteWorkflowRunById({
              runId: run.runId,
              workflowName: run.workflowName,
            });
            snapshots += 1;
          }
        }
      }
      return {
        snapshots,
        approvals: storeFactory.purgeTenant(tenantId),
        artifacts: 0,
      };
    },
  });

  return {
    name: 'flowsafe-showcase-dev',
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        const url = req.url ?? '';
        const isShowcaseApi =
          url === APPROVAL_BASE ||
          url.startsWith(`${APPROVAL_BASE}/`) ||
          url.startsWith(`${APPROVAL_BASE}?`) ||
          url === '/workflows' ||
          url.startsWith('/workflows?') ||
          url === '/runs' ||
          url.startsWith('/runs/') ||
          url.startsWith('/runs?') ||
          url === '/demo/reset';
        if (!isShowcaseApi) {
          next();
          return;
        }
        void (async () => {
          try {
            const request = await nodeToWebRequest(req);
            // Reset first (exact-path, cheapest check), then approvals (null
            // for non-approval paths without touching the body); the run
            // surface handles the rest.
            const response =
              (await resetRouter(request)) ??
              (await approvalRouter(request)) ??
              (await runRouter(request));
            if (!response) {
              next();
              return;
            }
            res.statusCode = response.status;
            response.headers.forEach((value, key) => {
              res.setHeader(key, value);
            });
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
