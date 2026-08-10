// Dev-only Vite plugin: the showcase host, in-process. It builds ONE
// buildShowcaseRuntime (all six workflows) over an InMemoryApprovalStoreFactory
// for the shared demo organization, wired with the host-kit re-queue bridge so
// decisions actually resume the run (and
// multi-gate runs re-queue the next gate). It mounts the same three routers the
// deployed worker does — createApprovalRouter at /api/approvals and host-kit's
// createRunRouter at /workflows + /runs — over the same ActorResolver seam and
// the same demo tokens the UI offers. Only the topology differs: runs resume
// in-process instead of through a DO stub. So
// `pnpm dev` is a real working backend with the real RBAC gates: launch a
// workflow, approve it in the dashboard, watch it run to success. No seeds — the
// queue fills as you launch.
//
// Runs in the Node dev-server context (Vite transpiles with esbuild), outside the
// browser tsconfig's `src` root.

import { InMemoryStore } from '@mastra/core/storage';
import { AGENT_AUDIT_CONTEXT_KEY } from '@proofoftech/breakwater';
import type {
  ApprovalStore,
  SelfDecisionPolicy,
} from '@proofoftech/flowsafe/approval-api';
import {
  ApprovalService,
  approvalGrantProvider,
  createActorResolver,
  createApprovalRouter,
  InMemoryApprovalStoreFactory,
  principalOwner,
  resumeViaRuntime,
} from '@proofoftech/flowsafe/approval-api';
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
import { demoActorTokensJson } from './worker/demo-actors.js';
import { buildShowcaseRuntime, SHOWCASE_MODULES } from './worker/runtime.js';

const APPROVAL_BASE = '/api/approvals';
const STREAM_BASE = '/api/stream';

/** Id for system-created approval records. */
const SYSTEM_PRINCIPAL_ID = 'showcase-dev';

/**
 * Dev-only stream-ticket signing key. Not a secret (this host is unreachable
 * off localhost and mints only dev identities); it just has to be present so the
 * stream ticket route mints. The deployed worker uses a real STREAM_TICKET_SECRET.
 */
const DEV_STREAM_TICKET_SECRET = 'showcase-dev-stream-secret';
const DEV_DEPLOYMENT_IDENTITY_SECRET =
  'showcase-dev-deployment-identity-secret';

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
 * structural HubNamespaceLike whose deployment stub records every published
 * ApprovalStreamEvent. `pnpm dev` hosts no
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

  const stubFor = (instanceName: string): HubStubLike => {
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
        const list = published.get(instanceName) ?? [];
        list.push(event);
        published.set(instanceName, list);
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
  const grants = approvalGrantProvider(storeFactory.store());
  const runtime = buildShowcaseRuntime({
    initInput: { storage: new InMemoryStore() },
    grantProvider: async (workflowId, runId, leg) => ({
      ...(await grants(workflowId, runId, leg)),
      [AGENT_AUDIT_CONTEXT_KEY]: {
        agentId: workflowId,
        tenantId: 'showcase-dev',
        runId,
        entryPath: leg.kind === 'start' ? 'workflow.start' : 'workflow.resume',
      },
    }),
    // Egress/artifact bindings unset => connectors simulate / write in-memory.
  });
  // The in-memory live-stream hub + publish topology fire on every mutation.
  // The dev SPA mints a ticket and
  // attempts a WebSocket (which degrades to polling here); the publish seam runs
  // regardless, mirroring the deployed composer's HUB fan-out.
  const hub = createInMemoryHub();
  const hubTopology = createHubTopology(
    hub.namespace,
    DEV_DEPLOYMENT_IDENTITY_SECRET,
  );
  // Service assembly is invoked lazily by the resolver. The service
  // forward-references itself in the resumeRun closure (invoked only on a
  // later decision) — the same const-with-deferred-ref pattern the worker
  // uses. resumeRunWithRequeue resumes in-process and re-queues the next gate.
  function buildService(store: ApprovalStore): ApprovalService {
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
        SYSTEM_PRINCIPAL_ID,
      ),
    });
    return service;
  }
  const resolve = createActorResolver({
    authenticate,
    storeFactory,
    deploymentTag: 'showcase-dev',
    buildService,
    // Same SoD policy the service enforces, threaded through the resolver so
    // the catalog echo's canSelfDecide matches
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
    systemPrincipalId: SYSTEM_PRINCIPAL_ID,
    start: async ({ workflowId, runId, inputData, principal }) => {
      const resources = storeFactory.resources();
      const resourceOwner = principalOwner(principal);
      if (!(await resources.claim('run', runId, resourceOwner))) {
        throw new Error(`run '${runId}' is already owned`);
      }
      try {
        return await runtime.start(workflowId, {
          runId,
          inputData,
          requestedBy: principal.id,
          requestedByKind: principal.kind,
        });
      } catch (error) {
        const persisted = await runtime.status(workflowId, runId);
        if (persisted) return persisted;
        await resources.release('run', runId, resourceOwner);
        throw error;
      }
    },
    status: async (workflowId, runId) =>
      (await runtime.status(workflowId, runId)) ?? undefined,
    resume: (workflowId, runId, body, requestedBy, requestedByKind) => {
      const { step, resumeData } = (body ?? {}) as {
        step?: string | string[];
        resumeData?: unknown;
      };
      return runtime.resume(workflowId, runId, {
        step,
        resumeData,
        requestedBy,
        requestedByKind,
      });
    },
    // D4: heal a suspended run whose approval never made it into the queue
    // on the next status() poll (see reconcileApprovalsOnStatus).
    reconcileApprovals: reconcileApprovalsOnStatus(SYSTEM_PRINCIPAL_ID),
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
    runStatus: async (workflowId, runId) =>
      (await runtime.status(workflowId, runId)) ?? undefined,
    deploymentIdentitySecret: DEV_DEPLOYMENT_IDENTITY_SECRET,
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
          url.startsWith('/runs?');
        if (!isShowcaseApi) {
          next();
          return;
        }
        void (async () => {
          try {
            const request = await nodeToWebRequest(req);
            // The stream surface owns only /api/stream/*; approvals return null
            // for other paths without touching the body, and the run surface
            // handles the rest.
            const response =
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
