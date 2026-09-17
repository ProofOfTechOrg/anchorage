// SPDX-License-Identifier: Apache-2.0

import type {
  D1Database,
  DurableObjectNamespace,
  ExportedHandler,
  R2Bucket,
} from '@cloudflare/workers-types';
import {
  type ActorResolver,
  createActorResolver,
} from '@proofoftech/flowsafe/approval-api';
import {
  DurableObjectRunner,
  ExecutionFenceStore,
  init,
  isPathSafeId,
  type RunnerRuntime,
} from '@proofoftech/flowsafe/do-runner';
import {
  approvalStoreFactoryFor,
  createFlowsafeMaintenanceDurableObject,
  createFlowsafeRunnerLifecycle,
  createFlowsafeWorker,
  type FlowsafeWorkerConfig,
  type FlowsafeWorkerEnv,
  readBoundedBody,
  staticTokenVerifier,
} from '@proofoftech/flowsafe/host-kit';
import {
  createScheduleRouter,
  createScheduleTargetPolicy,
  D1SchedulesStorage,
} from '@proofoftech/flowsafe/schedules';
import {
  DIRECT_TENANT_OBJECT_BODY,
  DIRECT_TENANT_OBJECT_KEY,
  DIRECT_TENANT_ROUTES,
  directTenantMutationEpoch,
  directTenantProbeEpoch,
} from './direct-credentialed-tenant-object.mjs';

export interface DirectTenantEnv extends FlowsafeWorkerEnv {
  DB: D1Database;
  RUNNER: DurableObjectNamespace;
  MAINTENANCE: DurableObjectNamespace;
  APP_PROBE_TOKEN: string;
  APPLICATION_RELEASE: string;
  PROBE_BUCKET: R2Bucket;
}

const DIRECT_FENCE_WORKFLOW = 'direct-fence-probe';

interface FenceOutcome {
  response: Response;
  result: {
    schedule?: { id?: string };
    pending?: boolean;
    reason?: { code: string; classification?: string };
  };
}

type FenceRoute = (
  method: string,
  suffix: string,
  body?: string,
) => Promise<FenceOutcome>;

async function readFenceBody(request: Request, emptyBodyAllowed: boolean) {
  const input = await readBoundedBody(request, 256);
  if (!input.ok) return null;
  if (input.text === '') return emptyBodyAllowed ? {} : null;
  let value: unknown;
  try {
    value = JSON.parse(input.text);
  } catch {
    return null;
  }
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  return value as Record<string, unknown>;
}

function fenceRoute(
  request: Request,
  env: DirectTenantEnv,
  resolve: ActorResolver,
): FenceRoute {
  const router = createScheduleRouter({
    resolve,
    store: new D1SchedulesStorage(env.DB),
    executionFence: new ExecutionFenceStore(env.DB),
    targetPolicy: createScheduleTargetPolicy({
      workflows: [{ id: DIRECT_FENCE_WORKFLOW }],
      agents: [],
    }),
    validateThreadTarget: async () => {
      throw new Error('direct fence probe target cannot require a thread');
    },
  });
  return async (method, suffix, body) => {
    const routed = new Request(`https://tenant/api/schedules${suffix}`, {
      method,
      headers: {
        authorization: request.headers.get('authorization') ?? '',
        'content-type': 'application/json',
      },
      ...(body === undefined ? {} : { body }),
    });
    const response = await router(routed);
    if (!response) throw new Error('direct fence probe route did not match');
    const result = (await response.json()) as FenceOutcome['result'];
    return { response, result };
  };
}

const createFenceSchedule = (route: FenceRoute) =>
  route(
    'POST',
    '',
    JSON.stringify({
      workflowId: DIRECT_FENCE_WORKFLOW,
      cron: '0 0 1 1 *',
      status: 'paused',
    }),
  );

const deleteFenceSchedule = (route: FenceRoute, scheduleId: unknown) =>
  route('DELETE', `/${scheduleId}`);

// A conformance-only capability, bounded at the point that grants it: this
// resolver is built for `/__direct/fence-probe`, the caller has already passed
// `APP_PROBE_TOKEN`, and the epoch it honours comes from the artifact's own
// release through `directTenantProbeEpoch`, so the request names a position
// against the host epoch rather than a number of its own. `/__direct/fence-mutate`
// runs on the kit's resolver and selects no epoch at all.
function fenceProbeResolver(
  env: DirectTenantEnv,
  label: unknown,
): ActorResolver {
  return createActorResolver({
    authenticate: (candidate) =>
      candidate.headers.get('authorization') === `Bearer ${env.APP_PROBE_TOKEN}`
        ? { id: 'direct-conformance', role: 'admin' }
        : undefined,
    storeFactory: approvalStoreFactoryFor(env.DB),
    mutationEpoch: directTenantProbeEpoch(env.APPLICATION_RELEASE, label),
    buildService: () => {
      throw new Error('direct fence probe does not request approval');
    },
  });
}

function fenceProbeAnswer(label: unknown, { response, result }: FenceOutcome) {
  const { reason } = result;
  const status = response.status;
  if (response.ok)
    return Response.json({ epoch: label, classification: 'accepted' });
  if (status === 409 && reason?.code === 'MUTATION_EPOCH_MISMATCH')
    return Response.json({
      epoch: label,
      classification: reason.classification,
    });
  if (
    status === 503 &&
    (reason?.code === 'EXECUTION_FENCED' ||
      reason?.code === 'EXECUTION_FENCE_UNREADABLE')
  )
    return Response.json({ epoch: label, classification: 'fenced' });
  return Response.json({
    epoch: label,
    classification: 'unexpected',
    status,
  });
}

function fenceMutateAnswer({ response, result }: FenceOutcome) {
  const { reason } = result;
  const status = response.status;
  if (response.ok)
    return Response.json({
      accepted: true,
      ...(result.schedule?.id === undefined
        ? {}
        : { scheduleId: result.schedule.id }),
      ...(result.pending === true ? { pending: true } : {}),
    });
  if (reason && typeof reason === 'object' && !Array.isArray(reason))
    return Response.json({
      accepted: false,
      code: reason.code,
      ...(reason.classification === undefined
        ? {}
        : { classification: reason.classification }),
      status,
    });
  return Response.json({ accepted: false, code: 'unexpected', status });
}

async function handleFenceProbe(request: Request, env: DirectTenantEnv) {
  const parsed = await readFenceBody(request, false);
  if (!parsed) return new Response('Invalid body', { status: 400 });
  const label = parsed.epoch;
  if (
    label !== 'current' &&
    label !== 'stale' &&
    label !== 'missing' &&
    label !== 'future'
  )
    return new Response('Invalid body', { status: 400 });
  const route = fenceRoute(request, env, fenceProbeResolver(env, label));
  const created = await createFenceSchedule(route);
  const scheduleId = created.result.schedule?.id;
  if (created.response.ok && scheduleId) {
    if (!isPathSafeId(scheduleId))
      throw new Error('direct fence probe received an invalid schedule id');
    await deleteFenceSchedule(route, scheduleId);
  }
  // The probe reports on the create it made; the delete only returns the
  // tenant to the state the next label finds.
  return fenceProbeAnswer(label, created);
}

async function handleFenceMutate(
  request: Request,
  env: DirectTenantEnv,
  resolve: ActorResolver,
) {
  const parsed = await readFenceBody(request, true);
  if (!parsed) return new Response('Invalid body', { status: 400 });
  const phase = parsed.phase === undefined ? 'both' : parsed.phase;
  if (phase !== 'both' && phase !== 'create' && phase !== 'delete')
    return new Response('Invalid body', { status: 400 });
  if (phase === 'delete' && !isPathSafeId(parsed.scheduleId))
    return new Response('Invalid body', { status: 400 });
  const route = fenceRoute(request, env, resolve);
  if (phase === 'delete')
    return fenceMutateAnswer(
      await deleteFenceSchedule(route, parsed.scheduleId),
    );
  const created = await createFenceSchedule(route);
  const scheduleId = created.result.schedule?.id;
  if (!created.response.ok || !scheduleId || phase === 'create')
    return fenceMutateAnswer(created);
  if (!isPathSafeId(scheduleId))
    throw new Error('direct fence probe received an invalid schedule id');
  return fenceMutateAnswer(await deleteFenceSchedule(route, scheduleId));
}

const config: FlowsafeWorkerConfig<DirectTenantEnv> = {
  workflows: [],
  systemPrincipalId: 'direct-conformance',
  mutationEpoch: (env) => directTenantMutationEpoch(env.APPLICATION_RELEASE),
  buildVerifier(env) {
    return staticTokenVerifier(
      new Map(
        env.APP_PROBE_TOKEN
          ? [[env.APP_PROBE_TOKEN, { id: 'direct-conformance', role: 'admin' }]]
          : [],
      ),
    );
  },
  maintenance: {
    sweepIntervalMs: 60 * 60_000,
    purgeIntervalMs: 60 * 60_000,
  },
  async preRoutes(request, env, _ctx, kit) {
    const path = new URL(request.url).pathname;
    if (!path.startsWith('/__direct/')) return null;
    if (!(await kit.resolve(request)))
      return new Response('Unauthorized', { status: 401 });
    if (request.method === 'POST') {
      if (path === DIRECT_TENANT_ROUTES.fenceProbe)
        return handleFenceProbe(request, env);
      if (path === DIRECT_TENANT_ROUTES.fenceMutate)
        return handleFenceMutate(request, env, kit.resolve);
    }
    if (path === DIRECT_TENANT_ROUTES.health && request.method === 'GET') {
      const row = await env.DB.prepare(
        'SELECT marker FROM direct_conformance_fixture WHERE id = 1',
      ).first<{ marker: string }>();
      return Response.json({
        release: env.APPLICATION_RELEASE,
        marker: row?.marker ?? null,
      });
    }
    if (path === DIRECT_TENANT_ROUTES.object) {
      if (request.method === 'POST') {
        await env.PROBE_BUCKET.put(
          DIRECT_TENANT_OBJECT_KEY,
          DIRECT_TENANT_OBJECT_BODY,
        );
        return new Response(null, { status: 204 });
      }
      if (request.method === 'DELETE') {
        await env.PROBE_BUCKET.delete(DIRECT_TENANT_OBJECT_KEY);
        return new Response(null, { status: 204 });
      }
      if (request.method === 'GET') {
        const object = await env.PROBE_BUCKET.get(DIRECT_TENANT_OBJECT_KEY);
        if (!object) return Response.json({ present: false });
        if (
          object.size !==
          new TextEncoder().encode(DIRECT_TENANT_OBJECT_BODY).byteLength
        )
          return new Response('Unexpected fixture object', { status: 409 });
        const digest = await crypto.subtle.digest(
          'SHA-256',
          await object.arrayBuffer(),
        );
        return Response.json({
          present: true,
          size: object.size,
          sha256: Array.from(new Uint8Array(digest), (byte) =>
            byte.toString(16).padStart(2, '0'),
          ).join(''),
        });
      }
    }
    return new Response('Not found', { status: 404 });
  },
};

export class Runner extends DurableObjectRunner<DirectTenantEnv> {
  protected build(env: DirectTenantEnv): RunnerRuntime {
    return init(env).runtime;
  }

  protected runOwnership(env: DirectTenantEnv) {
    return approvalStoreFactoryFor(env.DB).resources();
  }

  protected runLifecycle(env: DirectTenantEnv) {
    return createFlowsafeRunnerLifecycle(config, env);
  }
}

export class Maintenance extends createFlowsafeMaintenanceDurableObject(
  config,
) {}

const worker = createFlowsafeWorker(config);

export default {
  fetch: (request, env, ctx) =>
    worker.fetch(request as unknown as Request, env, ctx),
} satisfies ExportedHandler<DirectTenantEnv>;
