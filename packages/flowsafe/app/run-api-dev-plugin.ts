// Dev-only Vite plugin: the showcase host, in-process. It builds ONE
// buildShowcaseRuntime (all five workflows) + ONE ApprovalService over the same
// InMemoryApprovalStore, wired with the host-kit re-queue bridge so decisions
// actually resume the run (and multi-gate runs re-queue the next gate). It mounts
// three surfaces on the dev server — /api/approvals (the dashboard), /runs +
// /workflows (the launcher + status panel) — all bearer→actor authenticated with
// the same demo tokens the deployed worker and the UI use. So `app:dev` is a real
// working backend: launch a workflow, approve it in the dashboard, watch it run
// to success. No seeds — the queue fills as you launch.
//
// Runs in the Node dev-server context (Vite transpiles with esbuild), outside the
// browser tsconfig's `src` root.

import { InMemoryStore } from '@mastra/core/storage';
import type { Connect, Plugin } from 'vite';

import type { ApprovalActor } from '../src/approval-api/index.js';
import {
  ApprovalService,
  createApprovalRouter,
  InMemoryApprovalStore,
  resumeViaRuntime,
} from '../src/approval-api/index.js';
import type { RunnerRuntime } from '../src/do-runner/index.js';
import {
  queueApprovalForSuspension,
  resumeRunWithRequeue,
} from '../src/host-kit/index.js';
import { buildShowcaseRuntime, SHOWCASE_MODULES } from '../showcase/runtime.js';

const APPROVAL_BASE = '/api/approvals';

/** Identity for system-created approval records (needs a create-capable role). */
const SYSTEM_ACTOR: ApprovalActor = { id: 'showcase-dev', role: 'operator' };

// The demo bearer tokens — must match showcase/wrangler.jsonc APPROVAL_ACTOR_TOKENS
// and the UI's DEMO_ACTORS.
const ACTOR_TOKENS: Record<string, ApprovalActor> = {
  'demo-admin': { id: 'admin', role: 'admin' },
  'demo-builder': { id: 'builder', role: 'builder' },
  'demo-operator': { id: 'operator', role: 'operator' },
  'demo-reviewer': { id: 'reviewer', role: 'reviewer' },
  'demo-viewer': { id: 'viewer', role: 'viewer' },
};

function authenticate(request: Request): ApprovalActor | undefined {
  const token = request.headers
    .get('authorization')
    ?.match(/^Bearer\s+(.+)$/i)?.[1];
  return token ? ACTOR_TOKENS[token] : undefined;
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

// The run surface (/workflows + /runs), mirroring the deployed worker's routing
// but resuming in-process instead of via a DO stub. Returns null for paths it
// does not own so the caller can fall through.
async function handleRunRoutes(
  request: Request,
  runtime: RunnerRuntime,
  service: ApprovalService,
): Promise<Response | null> {
  const url = new URL(request.url);
  const actor = authenticate(request);

  if (request.method === 'GET' && url.pathname === '/workflows') {
    if (!actor) return json({ error: 'authentication required' }, 401);
    return json({ workflows: SHOWCASE_MODULES.map((module) => module.meta) });
  }

  const segments = url.pathname.split('/').filter(Boolean);
  if (segments[0] !== 'runs') return null;

  if (!actor) return json({ error: 'authentication required' }, 401);
  if (
    request.method === 'POST' &&
    actor.role !== 'admin' &&
    actor.role !== 'operator' &&
    actor.role !== 'builder'
  ) {
    return json({ error: 'forbidden' }, 403);
  }

  if (request.method === 'POST' && segments.length === 1) {
    const body = (await request.json().catch(() => null)) as {
      workflowId?: string;
      inputData?: unknown;
    } | null;
    if (!body || typeof body.workflowId !== 'string') {
      return json({ error: 'workflowId is required' }, 400);
    }
    const workflowModule = SHOWCASE_MODULES.find(
      (candidate) => candidate.meta.id === body.workflowId,
    );
    if (!workflowModule) {
      return json({ error: `unknown workflow '${body.workflowId}'` }, 404);
    }
    const { allowedRoles } = workflowModule.meta;
    if (allowedRoles && !allowedRoles.includes(actor.role)) {
      return json(
        { error: `role '${actor.role}' may not start '${body.workflowId}'` },
        403,
      );
    }
    const summary = await runtime.start(body.workflowId, {
      inputData: body.inputData,
    });
    if (summary.status !== 'suspended') return json(summary);
    const record = await queueApprovalForSuspension(
      service,
      body.workflowId,
      summary,
      actor.id,
      SYSTEM_ACTOR,
    );
    return json({ ...summary, approval: record });
  }

  if (request.method === 'GET' && segments.length === 3) {
    const workflowId = segments[1];
    const runId = segments[2];
    if (!workflowId || !runId) return json({ error: 'not found' }, 404);
    const summary = await runtime.status(workflowId, runId);
    if (!summary) return json({ error: 'not found' }, 404);
    return json(summary);
  }

  return json({ error: 'not found' }, 404);
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

export function runApiDevPlugin(): Plugin {
  const store = new InMemoryApprovalStore();
  const runtime = buildShowcaseRuntime({
    initInput: { storage: new InMemoryStore() },
    approvalStore: store,
    // Egress/artifact bindings unset => connectors simulate / write in-memory.
  });
  // The service forward-references itself in the resumeRun closure (invoked only
  // on a later decision) — the same const-with-deferred-ref pattern the worker
  // uses. resumeRunWithRequeue resumes in-process and re-queues the next gate.
  const service: ApprovalService = new ApprovalService({
    store,
    defaultSlaSeconds: 3600,
    resumeRun: resumeRunWithRequeue(
      resumeViaRuntime(runtime),
      () => service,
      SYSTEM_ACTOR,
    ),
  });
  const approvalRouter = createApprovalRouter({
    service,
    authenticate,
    basePath: APPROVAL_BASE,
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
          url.startsWith('/runs?');
        if (!isShowcaseApi) {
          next();
          return;
        }
        void (async () => {
          try {
            const request = await nodeToWebRequest(req);
            // Approvals first (it returns null for non-approval paths without
            // touching the body); the run surface handles the rest.
            const response =
              (await approvalRouter(request)) ??
              (await handleRunRoutes(request, runtime, service));
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
