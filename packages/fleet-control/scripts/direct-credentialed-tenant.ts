// SPDX-License-Identifier: Apache-2.0

import type {
  D1Database,
  DurableObjectNamespace,
  ExportedHandler,
  R2Bucket,
} from '@cloudflare/workers-types';
import {
  DurableObjectRunner,
  init,
  type RunnerRuntime,
} from '@proofoftech/flowsafe/do-runner';
import {
  approvalStoreFactoryFor,
  createFlowsafeMaintenanceDurableObject,
  createFlowsafeRunnerLifecycle,
  createFlowsafeWorker,
  type FlowsafeWorkerConfig,
  type FlowsafeWorkerEnv,
  staticTokenVerifier,
} from '@proofoftech/flowsafe/host-kit';
import {
  DIRECT_TENANT_OBJECT_BODY,
  DIRECT_TENANT_OBJECT_KEY,
} from './direct-credentialed-tenant-object.mjs';

export interface DirectTenantEnv extends FlowsafeWorkerEnv {
  DB: D1Database;
  RUNNER: DurableObjectNamespace;
  MAINTENANCE: DurableObjectNamespace;
  APP_PROBE_TOKEN: string;
  APPLICATION_RELEASE: string;
  PROBE_BUCKET: R2Bucket;
}

const config: FlowsafeWorkerConfig<DirectTenantEnv> = {
  workflows: [],
  systemPrincipalId: 'direct-conformance',
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
