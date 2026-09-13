// SPDX-License-Identifier: Apache-2.0

import type {
  D1Database,
  DurableObjectNamespace,
  ExportedHandler,
  R2Bucket,
} from '@cloudflare/workers-types';
import { createActorResolver } from '@proofoftech/flowsafe/approval-api';
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
  directTenantMutationEpoch,
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
    if (
      request.method === 'POST' &&
      (path === '/__direct/fence-mutate' || path === '/__direct/fence-probe')
    ) {
      const input = await readBoundedBody(request, 256);
      if (!input.ok) return new Response('Invalid body', { status: 400 });
      const probe = path === '/__direct/fence-probe';
      let parsed: Record<string, unknown> = {};
      if (input.text === '') {
        if (probe) return new Response('Invalid body', { status: 400 });
      } else {
        let value: unknown;
        try {
          value = JSON.parse(input.text);
        } catch {
          return new Response('Invalid body', { status: 400 });
        }
        if (!value || typeof value !== 'object' || Array.isArray(value))
          return new Response('Invalid body', { status: 400 });
        parsed = value as Record<string, unknown>;
      }
      const phase = parsed.phase === undefined ? 'both' : parsed.phase;
      const label = parsed.epoch;
      if (
        probe
          ? label !== 'current' &&
            label !== 'stale' &&
            label !== 'missing' &&
            label !== 'future'
          : phase !== 'both' && phase !== 'create' && phase !== 'delete'
      )
        return new Response('Invalid body', { status: 400 });
      if (!probe && phase === 'delete' && !isPathSafeId(parsed.scheduleId))
        return new Response('Invalid body', { status: 400 });

      const host = directTenantMutationEpoch(env.APPLICATION_RELEASE);
      const epoch =
        label === 'missing'
          ? undefined
          : label === 'stale'
            ? Math.max(0, host - 1)
            : label === 'future'
              ? host + 1
              : host;
      const resolve = probe
        ? createActorResolver({
            authenticate: (candidate) =>
              candidate.headers.get('authorization') ===
              `Bearer ${env.APP_PROBE_TOKEN}`
                ? { id: 'direct-conformance', role: 'admin' }
                : undefined,
            storeFactory: approvalStoreFactoryFor(env.DB),
            mutationEpoch: epoch,
            buildService: () => {
              throw new Error('direct fence probe does not request approval');
            },
          })
        : kit.resolve;
      const store = new D1SchedulesStorage(env.DB);
      const fence = new ExecutionFenceStore(env.DB);
      const router = createScheduleRouter({
        resolve,
        store,
        executionFence: fence,
        targetPolicy: createScheduleTargetPolicy({
          workflows: [{ id: DIRECT_FENCE_WORKFLOW }],
          agents: [],
        }),
        validateThreadTarget: async () => {
          throw new Error('direct fence probe target cannot require a thread');
        },
      });
      const route = async (method: string, suffix: string, body?: string) => {
        const routed = new Request(`https://tenant/api/schedules${suffix}`, {
          method,
          headers: {
            authorization: request.headers.get('authorization') ?? '',
            'content-type': 'application/json',
          },
          ...(body === undefined ? {} : { body }),
        });
        const response = await router(routed);
        if (!response)
          throw new Error('direct fence probe route did not match');
        const result = (await response.json()) as {
          schedule?: { id?: string };
          pending?: boolean;
          reason?: { code: string; classification?: string };
        };
        return { response, result };
      };
      const mutation = async () => {
        if (!probe && phase === 'delete')
          return route('DELETE', `/${parsed.scheduleId}`);
        const created = await route(
          'POST',
          '',
          JSON.stringify({
            workflowId: DIRECT_FENCE_WORKFLOW,
            cron: '0 0 1 1 *',
            status: 'paused',
          }),
        );
        const scheduleId = created.result.schedule?.id;
        if (
          !created.response.ok ||
          !scheduleId ||
          (!probe && phase === 'create')
        )
          return created;
        if (!isPathSafeId(scheduleId))
          throw new Error('direct fence probe received an invalid schedule id');
        const deleted = await route('DELETE', `/${scheduleId}`);
        return probe ? created : deleted;
      };
      const { response, result } = await mutation();
      const { reason } = result;
      const status = response.status;
      if (probe) {
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
    if (path === '/__direct/health' && request.method === 'GET') {
      const row = await env.DB.prepare(
        'SELECT marker FROM direct_conformance_fixture WHERE id = 1',
      ).first<{ marker: string }>();
      return Response.json({
        release: env.APPLICATION_RELEASE,
        marker: row?.marker ?? null,
      });
    }
    if (path === '/__direct/object') {
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
