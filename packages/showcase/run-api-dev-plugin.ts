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
  SelfDecisionPolicy,
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
  createHubTopology,
  createRunRouter,
  createStreamRouter,
  type HubNamespaceLike,
  type HubStubLike,
  parseActorTokens,
  type RunnerNamespaceLike,
  reconcileApprovalsOnStatus,
  resumeRunWithRequeue,
  staticTokenVerifier,
} from '@proofoftech/flowsafe/host-kit';
import type { Connect, Plugin } from 'vite';
import { DEMO_TENANT_ID, demoActorTokensJson } from './worker/demo-actors.js';
import { createDemoResetRouter } from './worker/demo-reset.js';
import { buildShowcaseRuntime, SHOWCASE_MODULES } from './worker/runtime.js';

const APPROVAL_BASE = '/api/approvals';
const STREAM_BASE = '/api/stream';

/** Id for system-created approval records (tenant is bound per request). */
const SYSTEM_ACTOR_ID = 'showcase-dev';

/**
 * Dev-only stream-ticket signing key. Not a secret (this host is unreachable
 * off localhost and mints only dev tenants); it just has to be present so the
 * stream ticket route mints. The deployed worker uses a real STREAM_TICKET_SECRET.
 */
const DEV_STREAM_TICKET_SECRET = 'showcase-dev-stream-secret';

/**
 * Dev SoD exemption: admin may decide its own requests (so one operator drives
 * product-launch's two gates alone). ONE source for both the service (enforce)
 * and the run-router (echo canSelfDecide). Mirrors the deployed showcase's
 * `APPROVAL_ALLOW_SELF_DECISION: "admin"` var.
 */
const DEV_SELF_DECISION: SelfDecisionPolicy = { roles: ['admin'] };

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

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

/**
 * A process-global in-memory hub mirroring InMemoryApprovalStore's role: a
 * structural HubNamespaceLike whose per-tenant stubs RECORD every published
 * ApprovalStreamEvent, keyed by the tenant `idFromName` addressed (tenant
 * isolation: each tenant's list is disjoint from every other's, exactly like
 * the deployed hub's `id.name === tenantId` binding). `pnpm dev` hosts NO
 * WebSocket upgrade — Vite's connect middleware cannot complete a WS handshake
 * and adding a raw ws server is out of scope — so /subscribe answers 426 and
 * the client degrades to polling (DL-019); there is accordingly no subscriber
 * fan-out here, only the recording that proves the publish seam itself (the
 * one thing `pnpm dev` CAN exercise) is wired end to end. Exported for
 * src/run-api-dev-plugin.test.ts.
 */
export function createInMemoryHub(): {
  namespace: HubNamespaceLike<string>;
  published: Map<string, unknown[]>;
} {
  const published = new Map<string, unknown[]>();

  const stubFor = (tenantId: string): HubStubLike => {
    // One impl over both fetch overloads: the string/init overload carries the
    // /internal/event publish; the raw-Request overload carries a WS upgrade.
    const fetch = (async (
      input: Request | string,
      init?: {
        method?: string;
        headers?: Record<string, string>;
        body?: string;
      },
    ): Promise<Response> => {
      const url = new URL(typeof input === 'string' ? input : input.url);
      const method =
        typeof input === 'string' ? (init?.method ?? 'GET') : input.method;
      if (method === 'POST' && url.pathname === '/internal/event') {
        const raw = typeof input === 'string' ? init?.body : await input.text();
        const event = raw ? (JSON.parse(raw) as unknown) : null;
        const list = published.get(tenantId) ?? [];
        list.push(event);
        published.set(tenantId, list);
        return json({ ok: true });
      }
      return json(
        { error: 'dev websocket unavailable; polling fallback' },
        426,
      );
    }) as HubStubLike['fetch'];
    return { fetch };
  };

  return {
    namespace: { idFromName: (name) => name, get: (id) => stubFor(id) },
    published,
  };
}

/**
 * Dev has no runner Durable Object (the runtime runs in-process), so the run WS
 * upgrade — which the browser routes through the httpServer 'upgrade' event,
 * bypassing this connect middleware anyway — is never reached here. This 426
 * stub only satisfies createStreamRouter's `runner` type; the client falls back
 * to the 3s run poll (DL-019).
 */
const DEV_RUNNER_NAMESPACE = {
  idFromName: (name: string) => name,
  get: () => ({
    fetch: async () =>
      json({ error: 'dev websocket unavailable; polling fallback' }, 426),
  }),
} as unknown as RunnerNamespaceLike;

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
  // The in-memory live-stream hub + the publish topology each tenant-bound
  // ApprovalService fires on every mutation. The dev SPA mints a ticket and
  // attempts a WebSocket (which degrades to polling here); the publish seam runs
  // regardless, mirroring the deployed composer's HUB fan-out.
  const hub = createInMemoryHub();
  const hubTopology = createHubTopology(hub.namespace);
  // Per-tenant service assembly, invoked lazily by the resolver. The service
  // forward-references itself in the resumeRun closure (invoked only on a
  // later decision) — the same const-with-deferred-ref pattern the worker
  // uses. resumeRunWithRequeue resumes in-process and re-queues the next gate.
  function buildService(store: TenantBoundApprovalStore): ApprovalService {
    const service: ApprovalService = new ApprovalService({
      store,
      defaultSlaSeconds: 3600,
      // Admin-scoped SoD exemption so `admin` can drive product-launch's two
      // gates solo (matches the deployed showcase's APPROVAL_ALLOW_SELF_DECISION
      // var). The reviewer lane still 403s on a self-request — SoD stays live.
      allowSelfDecision: DEV_SELF_DECISION,
      // Live fan-out to the in-memory hub (the deployed composer wraps this in
      // ctx.waitUntil; here it is fire-and-forget, which the service contains).
      stream: (event) => hubTopology.publish(event),
      resumeRun: resumeRunWithRequeue(
        resumeViaRuntime(runtime),
        () => service,
        SYSTEM_ACTOR_ID,
      ),
    });
    return service;
  }
  const resolve = createTenantResolver({
    authenticate,
    storeFactory,
    buildService,
    // Same SoD policy the service enforces, threaded through the resolver so
    // the catalog echo's canSelfDecide (tenant.canSelfDecide) matches
    // enforcement (admin => true, others => false).
    allowSelfDecision: DEV_SELF_DECISION,
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
        // No showcase workflow writes agent memory; when one does, sweep the
        // in-memory store's memory domain here (docs/agent-memory-tenancy.md
        // item 5) instead of leaving the constants.
        threads: 0,
        messages: 0,
        resources: 0,
        // No showcase workflow dispatches background tasks (Track B); when one
        // does, sweep the in-memory backgroundTasks domain here.
        backgroundTasks: 0,
        // No showcase workflow sends agent signals (Track C); when one does,
        // sweep the in-memory notifications + thread-state domains here.
        notifications: 0,
        threadState: 0,
        // No showcase workflow registers schedules (Track D); when one does,
        // sweep the in-memory schedules + schedule-triggers domains here.
        schedules: 0,
        scheduleTriggers: 0,
        approvals: storeFactory.purgeTenant(tenantId),
        // No showcase workflow registers signal providers (Track E); when one
        // does, sweep the in-memory subscription store here.
        subscriptions: 0,
        artifacts: 0,
      };
    },
  });

  // The stream surface (ticket mint + the hub/run WS-upgrade routes). Only the
  // POST /api/stream/ticket route is reachable here — a browser WebSocket
  // upgrade goes through the httpServer 'upgrade' event, not this connect
  // middleware — so the upgrade routes forward to the 426 stubs and the client
  // degrades to polling. Mounting the ticket route lets the SPA mint (matching
  // the deployed composer) instead of 404-looping on the ticket endpoint.
  const streamRouter = createStreamRouter({
    resolve,
    ticketSecret: DEV_STREAM_TICKET_SECRET,
    hub: hub.namespace,
    runner: DEV_RUNNER_NAMESPACE,
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
          url.startsWith(`${STREAM_BASE}/`) ||
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
            // Reset first (exact-path, cheapest check), then the stream surface
            // (owns only /api/stream/*, null otherwise), then approvals (null
            // for non-approval paths without touching the body); the run surface
            // handles the rest.
            const response =
              (await resetRouter(request)) ??
              (await streamRouter(request)) ??
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
