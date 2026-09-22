// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type {
  D1Database,
  DurableObjectNamespace,
  R2Bucket,
} from '@cloudflare/workers-types';
import {
  DEPLOYMENT_IDENTITY_HEADER,
  type ExecutionFenceTransition,
  type ExecutionFenceVersionedReading,
  seedDeploymentIdentity,
} from '@proofoftech/flowsafe/do-runner';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  createTestHarness,
  unstable_splitSqlQuery as splitSqlQuery,
  type TestHarness,
  type WorkerHandle,
} from 'wrangler';
import {
  directDeploymentSpec,
  generateDirectDeploymentSecrets,
} from '../scripts/direct-credentialed-spec.js';
import {
  DIRECT_CONTINUATION_WORKFLOW,
  DIRECT_TENANT_OBJECT_BODY,
  DIRECT_TENANT_OBJECT_KEY,
} from '../scripts/direct-credentialed-tenant-object.mjs';
import { plainWorkerIngressModule } from '../src/plain-worker-backend.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import {
  DIRECT_FIXTURE_PROVIDER,
  directFixtureManifest,
} from './fixtures/direct-credentialed-config.js';
import { createDirectReferenceHarness } from './fixtures/direct-reference-harness.js';

const manifest = directFixtureManifest();
const secrets = generateDirectDeploymentSecrets();
const initial = directDeploymentSpec(
  manifest,
  'a',
  'initial',
  secrets,
  DIRECT_FIXTURE_PROVIDER,
);
const next = directDeploymentSpec(
  manifest,
  'a',
  'next',
  secrets,
  DIRECT_FIXTURE_PROVIDER,
);
const applicationHeaders = {
  authorization: `Bearer ${secrets.application?.APP_PROBE_TOKEN}`,
};
const maintenanceHeaders = {
  authorization: `Bearer ${secrets.maintenanceAdmin}`,
};

interface HarnessBindings {
  DB: D1Database;
  PROBE_BUCKET: R2Bucket;
  MAINTENANCE: DurableObjectNamespace;
  RUNNER: DurableObjectNamespace;
}

let directory: string | undefined;
let entrypoint: string | undefined;

function options(
  release: '1' | '2',
  token = secrets.application?.APP_PROBE_TOKEN ?? '',
  allowSelfDecision = true,
) {
  if (!entrypoint) throw new Error('test ingress is not prepared');
  return {
    root: fileURLToPath(new URL('..', import.meta.url)),
    workers: [
      {
        config: {
          name: 'direct-tenant-harness',
          main: entrypoint,
          compatibility_date: '2026-08-06',
          vars: {
            DEPLOYMENT_TENANT: initial.tenantTag,
            DEPLOYMENT_IDENTITY_SECRET: secrets.deploymentIdentity,
            MAINTENANCE_ADMIN_SECRET: secrets.maintenanceAdmin,
            APP_PROBE_TOKEN: token,
            ...(allowSelfDecision
              ? { APPROVAL_ALLOW_SELF_DECISION: 'true' }
              : {}),
            APPLICATION_RELEASE: release,
            FLEET_SPEC_DIGEST: deploymentSpecDigest(
              release === '1' ? initial : next,
            ),
          },
          d1_databases: [
            {
              binding: 'DB',
              database_name: 'direct-tenant-harness',
              database_id: '00000000-0000-0000-0000-000000000000',
            },
          ],
          r2_buckets: [
            { binding: 'PROBE_BUCKET', bucket_name: 'direct-tenant-harness' },
          ],
          durable_objects: {
            bindings: [
              { name: 'RUNNER', class_name: 'Runner' },
              { name: 'MAINTENANCE', class_name: 'Maintenance' },
            ],
          },
          migrations: [
            { tag: 'v1', new_sqlite_classes: ['Maintenance', 'Runner'] },
          ],
        },
      },
    ],
  } satisfies Parameters<typeof createTestHarness>[0];
}

describe.sequential('direct tenant fixture in workerd', {
  timeout: 30_000,
}, () => {
  let server: TestHarness;
  let worker: WorkerHandle<HarnessBindings>;

  const appFetch = (
    path: string,
    init?: Parameters<WorkerHandle['fetch']>[1],
  ) => worker.fetch(new URL(path, `https://${initial.routeHostname}`), init);
  const controlFetch = (
    path: string,
    init?: Parameters<WorkerHandle['fetch']>[1],
  ) => worker.fetch(new URL(path, initial.maintenanceBaseUrl), init);

  const readFence = async () => {
    const response = await appFetch('/admin/execution-fence', {
      headers: maintenanceHeaders,
    });
    expect(response.status).toBe(200);
    return response.json() as Promise<ExecutionFenceVersionedReading>;
  };
  const transitionFence = async (body: ExecutionFenceTransition) => {
    const response = await appFetch('/admin/execution-fence', {
      method: 'POST',
      headers: maintenanceHeaders,
      body: JSON.stringify(body),
    });
    expect(response.status).toBe(200);
    return response.json() as Promise<ExecutionFenceVersionedReading>;
  };
  const fencePost = (path: string, body?: string) =>
    appFetch(path, {
      method: 'POST',
      headers: applicationHeaders,
      ...(body === undefined ? {} : { body }),
    });
  const fenceProbe = (epoch: unknown) =>
    fencePost('/__direct/fence-probe', JSON.stringify({ epoch }));
  const fenceMutate = (body?: Record<string, unknown>) =>
    fencePost(
      '/__direct/fence-mutate',
      body === undefined ? undefined : JSON.stringify(body),
    );

  beforeAll(async () => {
    directory = await mkdtemp(join(tmpdir(), 'fleet-direct-tenant-'));
    const ingress = plainWorkerIngressModule(initial);
    entrypoint = join(directory, ingress.name);
    const source = fileURLToPath(
      new URL('../scripts/direct-credentialed-tenant.ts', import.meta.url),
    );
    await writeFile(
      join(directory, initial.mainModule),
      `export {default, Maintenance, Runner} from ${JSON.stringify(source)};\n`,
    );
    await writeFile(entrypoint, ingress.content);
    server = createTestHarness(options('1'));
    await server.listen();
    worker = server.getWorker<HarnessBindings>();
    const env = await worker.getEnv();
    await seedDeploymentIdentity(env.DB, initial.tenantTag, 'open');
    for (const migration of initial.migrations)
      await env.DB.batch(
        splitSqlQuery(migration.sql).map((sql) => env.DB.prepare(sql)),
      );
  }, 30_000);

  afterAll(async () => {
    try {
      await server?.close();
    } finally {
      if (directory) await rm(directory, { recursive: true, force: true });
    }
  }, 30_000);

  it('authenticates probes and reads real D1 state', async () => {
    for (const headers of [{}, { authorization: 'Bearer wrong' }]) {
      expect((await appFetch('/__direct/health', { headers })).status).toBe(
        401,
      );
      expect(
        (await appFetch('/__direct/object', { method: 'POST', headers }))
          .status,
      ).toBe(401);
    }
    const response = await appFetch('/__direct/health', {
      headers: applicationHeaders,
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ release: '1', marker: 'initial' });
    const env = await worker.getEnv();
    expect((await env.PROBE_BUCKET.list()).objects).toEqual([]);
  });

  it('uses the host maintenance authenticator and actual Durable Object', async () => {
    expect(
      (
        await controlFetch('/admin/ensure-maintenance', {
          method: 'POST',
          headers: applicationHeaders,
        })
      ).status,
    ).toBe(401);
    const ensured = await controlFetch('/admin/ensure-maintenance', {
      method: 'POST',
      headers: maintenanceHeaders,
    });
    expect(ensured.status).toBe(200);
    expect(await ensured.json()).toMatchObject({
      nextSweepAt: expect.any(Number),
      nextPurgeAt: expect.any(Number),
      alarmAt: expect.any(Number),
      deploymentSpecDigest: deploymentSpecDigest(initial),
    });
    const status = await controlFetch('/admin/maintenance-status', {
      headers: maintenanceHeaders,
    });
    expect(status.status).toBe(200);
    expect(await status.json()).toMatchObject({
      nextSweepAt: expect.any(Number),
      nextPurgeAt: expect.any(Number),
      alarmAt: expect.any(Number),
      deploymentSpecDigest: deploymentSpecDigest(initial),
    });
    expect(await worker.listDurableObjectIds('MAINTENANCE')).toHaveLength(1);
  });

  it('admits candidate maintenance on the control origin and rejects version selection on public ingress', async () => {
    const headers = {
      ...maintenanceHeaders,
      'Cloudflare-Workers-Version-Overrides': `${initial.scriptName}="11111111-1111-4111-8111-111111111111"`,
    };
    expect(
      (await appFetch('/admin/ensure-maintenance', { method: 'POST', headers }))
        .status,
    ).toBe(404);
    const response = await controlFetch('/admin/ensure-maintenance', {
      method: 'POST',
      headers,
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({
      deploymentSpecDigest: deploymentSpecDigest(initial),
      nextSweepAt: expect.any(Number),
    });
    expect(
      (await controlFetch('/__direct/health', { headers: applicationHeaders }))
        .status,
    ).toBe(404);
  });

  it('constructs the real Runner runtime behind its internal identity check', async () => {
    const env = await worker.getEnv();
    const runner = env.RUNNER.get(env.RUNNER.idFromName('fixture:run1'));
    const url = 'https://runner/runs/fixture/run1/start-liveness';
    expect((await runner.fetch(url)).status).toBe(503);
    const response = await runner.fetch(url, {
      headers: { [DEPLOYMENT_IDENTITY_HEADER]: secrets.deploymentIdentity },
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ live: false });
  });

  it('suspends the strict Zod continuation in the native Runner', async () => {
    for (const inputData of [
      {},
      { challenge: 'a'.repeat(63) },
      { challenge: 'A'.repeat(64) },
      { challenge: 'a'.repeat(64), extra: true },
    ]) {
      const invalid = await appFetch('/runs', {
        method: 'POST',
        headers: applicationHeaders,
        body: JSON.stringify({
          workflowId: DIRECT_CONTINUATION_WORKFLOW,
          inputData,
        }),
      });
      expect(invalid.status).toBe(400);
    }
    const response = await appFetch('/runs', {
      method: 'POST',
      headers: applicationHeaders,
      body: JSON.stringify({
        workflowId: DIRECT_CONTINUATION_WORKFLOW,
        inputData: { challenge: 'a'.repeat(64) },
      }),
    });
    expect(response.status).toBe(200);
    const summary = (await response.json()) as {
      runId: string;
      status: string;
    };
    expect(summary).toMatchObject({
      runId: expect.any(String),
      status: 'suspended',
    });
    for (const resumeData of [
      { proceed: false },
      { proceed: true, extra: true },
    ]) {
      const invalidResume = await appFetch(
        `/runs/${DIRECT_CONTINUATION_WORKFLOW}/${summary.runId}/resume`,
        {
          method: 'POST',
          headers: applicationHeaders,
          body: JSON.stringify({ step: 'hold', resumeData }),
        },
      );
      expect(invalidResume.status).toBe(400);
    }
    const status = await appFetch(
      `/runs/${DIRECT_CONTINUATION_WORKFLOW}/${summary.runId}`,
      { headers: applicationHeaders },
    );
    expect(await status.json()).toMatchObject({
      runId: summary.runId,
      status: 'suspended',
    });
  });

  it('allows the single static-token actor to decide its approval only with the live self-decision binding', async () => {
    try {
      await server.update(options('1', undefined, false));
      worker = server.getWorker<HarnessBindings>();
      const started = await appFetch('/runs', {
        method: 'POST',
        headers: applicationHeaders,
        body: JSON.stringify({
          workflowId: DIRECT_CONTINUATION_WORKFLOW,
          inputData: { challenge: 'b'.repeat(64) },
        }),
      });
      expect(started.status).toBe(200);
      const summary = (await started.json()) as {
        runId?: string;
        approval?: { id?: string };
      };
      const approvalId = summary.approval?.id;
      if (!summary.runId || !approvalId)
        throw new Error('native approval identity is missing');
      const decide = () =>
        appFetch(`/api/approvals/${approvalId}/decide`, {
          method: 'POST',
          headers: applicationHeaders,
          body: JSON.stringify({ decision: 'approve' }),
        });
      expect((await decide()).status).toBe(403);
      await server.update(options('1'));
      worker = server.getWorker<HarnessBindings>();
      const allowed = await decide();
      expect(allowed.status).toBe(200);
      expect(await allowed.json()).toMatchObject({
        record: { id: approvalId, runId: summary.runId, status: 'approved' },
      });
    } finally {
      await server.update(options('1'));
      worker = server.getWorker<HarnessBindings>();
    }
  }, 30_000);

  it('runs retained-token reference probes through the native tenant across reloads', async () => {
    const forwarded: {
      url: string;
      method: string;
      authorization: string | null;
    }[] = [];
    const reference = await createDirectReferenceHarness({
      applicationFetch: async (request) => {
        expect(request.body === undefined || request.body === '').toBe(true);
        forwarded.push({
          url: request.url,
          method: request.method,
          authorization: request.headers.get('authorization'),
        });
        return worker.fetch(request.url, {
          method: request.method,
          headers: [...request.headers],
        });
      },
    });
    const work = await Promise.allSettled([
      (async () => {
        const probeSpec = reference.specs.find(
          (spec) => spec.tenantTag === initial.tenantTag,
        );
        const token = reference.secrets.a.application?.APP_PROBE_TOKEN;
        if (!probeSpec || !token)
          throw new Error('reference fixture role is missing');
        expect(probeSpec.routeHostname).toBe(initial.routeHostname);
        const active = options('1', token);
        const configuration = active.workers[0]?.config;
        if (!configuration)
          throw new Error('test Worker configuration is missing');
        const databaseId = configuration.d1_databases[0]?.database_id;
        if (!databaseId) throw new Error('native tenant database is missing');
        await server.update(active);
        worker = server.getWorker<HarnessBindings>();
        await reference.fleetStore.withDeploymentLease(
          probeSpec.tenantTag,
          probeSpec.environment,
          (lease) =>
            lease.put({
              tenantTag: probeSpec.tenantTag,
              environment: probeSpec.environment,
              backend: 'plain-worker',
              scriptName: probeSpec.scriptName,
              databaseId,
              databaseName: probeSpec.databaseName,
              schemaVersion: probeSpec.schemaVersion,
              desiredSpecDigest: deploymentSpecDigest(probeSpec),
              artifactVersion: 'native-probe-fixture',
              durableObjectBindings: [],
              routeHostname: probeSpec.routeHostname,
              phase: 'ready',
              updatedAt: new Date().toISOString(),
            }),
        );
        const action = (
          operation: 'health' | 'object-put' | 'object-read' | 'object-delete',
        ) => ({ kind: 'tenant-probe', role: 'a', operation }) as const;
        expect(await reference.success(action('health'))).toEqual({
          role: 'a',
          operation: 'health',
          release: '1',
          marker: 'initial',
        });
        const put = await reference.call(action('object-put'));
        expect(reference.bridgeErrors).toEqual([]);
        expect(put.value).toEqual({
          contractVersion: 2,
          configSha256: reference.manifest.configSha256,
          action: 'tenant-probe',
          ok: true,
          result: { role: 'a', operation: 'object-put', returned: true },
        });
        expect(put.response.headers.get('X-Direct-Application-Attempts')).toBe(
          '1',
        );
        expect(put.response.headers.get('X-Direct-Provider-Attempts')).toBe(
          '0',
        );
        expect(put.response.headers.get('X-Direct-Maintenance-Attempts')).toBe(
          '0',
        );
        const expected = {
          role: 'a',
          operation: 'object-read',
          present: true,
          size: Buffer.byteLength(DIRECT_TENANT_OBJECT_BODY),
          sha256: createHash('sha256')
            .update(DIRECT_TENANT_OBJECT_BODY)
            .digest('hex'),
        };
        expect(await reference.success(action('object-read'))).toEqual(
          expected,
        );
        await reference.reload();
        expect(await reference.success(action('object-read'))).toEqual(
          expected,
        );
        await server.update(active);
        worker = server.getWorker<HarnessBindings>();
        expect(await reference.success(action('object-read'))).toEqual(
          expected,
        );
        await reference.success(action('object-delete'));
        expect(await reference.success(action('object-read'))).toEqual({
          role: 'a',
          operation: 'object-read',
          present: false,
        });
        expect(forwarded.length).toBeGreaterThan(0);
        for (const request of forwarded) {
          expect(request.authorization).toBe(`Bearer ${token}`);
          expect(new URL(request.url).origin).toBe(
            `https://${initial.routeHostname}`,
          );
        }
        expect(JSON.stringify(put.value)).not.toContain(token);
        await server.update(options('1', 'wrong-token'));
        worker = server.getWorker<HarnessBindings>();
        expect((await reference.call(action('health'))).value).toEqual({
          contractVersion: 2,
          ok: false,
          error: { code: 'operation-refused' },
        });
      })(),
    ]);
    const cleanup = await Promise.allSettled([
      reference.close(),
      (async () => {
        await server.update(options('1'));
        worker = server.getWorker<HarnessBindings>();
        await (await worker.getEnv()).PROBE_BUCKET.delete(
          DIRECT_TENANT_OBJECT_KEY,
        );
      })(),
    ]);
    const failures = [...work, ...cleanup].flatMap((result) =>
      result.status === 'rejected' ? [result.reason] : [],
    );
    if (failures.length)
      throw new AggregateError(failures, 'native probe or cleanup failed');
  }, 60_000);

  it('serves fence and inventory administration on public ingress and refuses the control origin', async () => {
    expect(await readFence()).toEqual({
      state: 'open',
      mutationEpoch: 0,
      requireMutationEpoch: false,
      transitionRevision: expect.any(Number),
    });
    for (const path of ['/admin/execution-fence', '/admin/inventory']) {
      const response = await appFetch(path, { headers: maintenanceHeaders });
      expect(response.status).toBe(200);
      await response.json();
      expect(
        (await controlFetch(path, { headers: maintenanceHeaders })).status,
      ).toBe(404);
    }
  });

  it('accepts the epoch labels before activation and refuses malformed probe members', async () => {
    expect(await readFence()).toMatchObject({
      state: 'open',
      mutationEpoch: 0,
      requireMutationEpoch: false,
    });
    for (const epoch of ['current', 'stale', 'missing', 'future']) {
      const response = await fenceProbe(epoch);
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual({
        epoch,
        classification: 'accepted',
      });
    }
    for (const body of [undefined, '{}']) {
      const response = await fencePost('/__direct/fence-mutate', body);
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual({ accepted: true });
    }
    const env = await worker.getEnv();
    for (const [path, body] of [
      ['/__direct/fence-mutate', '{"phase":"bogus"}'],
      ['/__direct/fence-probe', '{"epoch":"bogus"}'],
      ['/__direct/fence-probe', undefined],
    ] as const) {
      const response = await fencePost(path, body);
      expect(response.status).toBe(400);
      expect(
        (await env.DB.prepare('SELECT id FROM mastra_schedules').all()).results,
      ).toEqual([]);
    }
  });

  it('refuses a draining create and admits a draining delete after rejecting malformed ids', async () => {
    const created = await fenceMutate({ phase: 'create' });
    expect(created.status).toBe(200);
    const creation = (await created.json()) as {
      accepted: boolean;
      scheduleId: string;
    };
    expect(creation).toEqual({
      accepted: true,
      scheduleId: expect.any(String),
    });
    const { scheduleId } = creation;
    const before = await readFence();
    const draining = await transitionFence({
      expected: 'open',
      next: 'draining',
      expectedMutationEpoch: before.mutationEpoch,
      expectedRevision: before.transitionRevision,
    });
    expect(draining).toMatchObject({
      state: 'draining',
      mutationEpoch: 0,
      requireMutationEpoch: false,
    });
    const refused = await fenceMutate({ phase: 'create' });
    expect(refused.status).toBe(200);
    expect(await refused.json()).toEqual({
      accepted: false,
      code: 'EXECUTION_FENCED',
      status: 503,
    });
    const fenced = await fenceProbe('current');
    expect(fenced.status).toBe(200);
    // The probe reads the same refusal as a classification rather than as the
    // unclassified answer it reports for a code it cannot place.
    expect(await fenced.json()).toEqual({
      epoch: 'current',
      classification: 'fenced',
    });
    const env = await worker.getEnv();
    for (const malformed of ['..', 'x/y', '%', `${scheduleId}?ignored`]) {
      const response = await fenceMutate({
        phase: 'delete',
        scheduleId: malformed,
      });
      expect(response.status).toBe(400);
      expect(
        (await env.DB.prepare('SELECT id FROM mastra_schedules').all()).results,
      ).toEqual([{ id: scheduleId }]);
    }
    const deleted = await fenceMutate({ phase: 'delete', scheduleId });
    expect(deleted.status).toBe(200);
    expect(await deleted.json()).toMatchObject({ accepted: true });
    expect(
      await transitionFence({
        expected: 'draining',
        next: 'open',
        expectedMutationEpoch: draining.mutationEpoch,
        expectedRevision: draining.transitionRevision,
      }),
    ).toMatchObject({
      state: 'open',
      mutationEpoch: 0,
      requireMutationEpoch: false,
    });
  });

  it('separates an unclassified mutation answer from a fence refusal', async () => {
    const env = await worker.getEnv();
    const unknown = await fenceMutate({
      phase: 'delete',
      scheduleId: 'absent-schedule',
    });
    expect(unknown.status).toBe(200);
    // The schedule router answers a schedule it cannot find with no `reason`
    // member, so the route reports the answer it could not read rather than a
    // refusal code the tenant never received.
    expect(await unknown.json()).toEqual({
      accepted: false,
      code: 'unexpected',
      status: 404,
    });
    expect(
      (await env.DB.prepare('SELECT id FROM mastra_schedules').all()).results,
    ).toEqual([]);
  });

  it('refuses the pre-cutover artifact and admits the next artifact on the same activated fence', async () => {
    const before = await readFence();
    const draining = await transitionFence({
      expected: 'open',
      next: 'draining',
      expectedMutationEpoch: before.mutationEpoch,
      expectedRevision: before.transitionRevision,
      advanceMutationEpoch: true,
    });
    expect(draining).toEqual({
      state: 'draining',
      mutationEpoch: 1,
      requireMutationEpoch: true,
      transitionRevision: before.transitionRevision + 1,
    });
    const reopened = await transitionFence({
      expected: 'draining',
      next: 'open',
      expectedMutationEpoch: draining.mutationEpoch,
      expectedRevision: draining.transitionRevision,
    });
    expect(reopened).toEqual({
      state: 'open',
      mutationEpoch: 1,
      requireMutationEpoch: true,
      transitionRevision: draining.transitionRevision + 1,
    });
    const stale = await fenceMutate({ phase: 'both' });
    expect(stale.status).toBe(200);
    const staleOutcome = await stale.json();
    expect(staleOutcome).toEqual({
      accepted: false,
      code: 'MUTATION_EPOCH_MISMATCH',
      classification: 'stale',
      status: 409,
    });
    await server.update(options('2'));
    worker = server.getWorker<HarnessBindings>();
    expect(await readFence()).toEqual(reopened);
    const current = await fenceMutate({ phase: 'both' });
    expect(current.status).toBe(200);
    const currentOutcome = await current.json();
    expect(currentOutcome).toEqual({ accepted: true });
  });

  it('classifies missing, future and stale epochs after reopen and admits the current epoch', async () => {
    for (const [epoch, classification] of [
      ['missing', 'missing'],
      ['future', 'future'],
      ['stale', 'stale'],
      ['current', 'accepted'],
    ]) {
      const response = await fenceProbe(epoch);
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual({ epoch, classification });
    }
  });

  it('leaves no schedule or trigger row after the fence probes', async () => {
    const env = await worker.getEnv();
    expect(
      (await env.DB.prepare('SELECT id FROM mastra_schedules').all()).results,
    ).toEqual([]);
    expect(
      (await env.DB.prepare('SELECT id FROM mastra_schedule_triggers').all())
        .results,
    ).toEqual([]);
  });

  it('writes fixed R2 bytes and retains them across reload and additive D1 migration', async () => {
    expect(
      (
        await appFetch('/__direct/object', {
          method: 'POST',
          headers: applicationHeaders,
          body: 'ignored request body',
        })
      ).status,
    ).toBe(204);
    const body = DIRECT_TENANT_OBJECT_BODY;
    const env = await worker.getEnv();
    expect(
      await (await env.PROBE_BUCKET.get(DIRECT_TENANT_OBJECT_KEY))?.text(),
    ).toBe(body);
    for (const migration of next.migrations.slice(initial.migrations.length))
      await env.DB.batch(
        splitSqlQuery(migration.sql).map((sql) => env.DB.prepare(sql)),
      );
    await server.update(options('2'));
    worker = server.getWorker<HarnessBindings>();
    const health = await appFetch('/__direct/health', {
      headers: applicationHeaders,
    });
    expect(health.status).toBe(200);
    expect(await health.json()).toEqual({ release: '2', marker: 'next' });
    const object = await appFetch('/__direct/object', {
      headers: applicationHeaders,
    });
    expect(object.status).toBe(200);
    expect(await object.json()).toEqual({
      present: true,
      size: Buffer.byteLength(body),
      sha256: createHash('sha256').update(body).digest('hex'),
    });
    expect(
      (
        await appFetch('/__direct/object', {
          method: 'DELETE',
          headers: applicationHeaders,
        })
      ).status,
    ).toBe(204);
    expect(
      await (
        await appFetch('/__direct/object', { headers: applicationHeaders })
      ).json(),
    ).toEqual({ present: false });
  });

  it('keeps the host identity check before authenticated application probes', async () => {
    const changed = options('2');
    const input = changed.workers[0];
    if (!input) throw new Error('test Worker configuration is missing');
    input.config.vars.DEPLOYMENT_TENANT = manifest.names.roles.b.tenantTag;
    await server.update(changed);
    worker = server.getWorker<HarnessBindings>();
    expect(
      (await appFetch('/__direct/health', { headers: applicationHeaders }))
        .status,
    ).toBe(503);
    await server.update(options('2'));
    worker = server.getWorker<HarnessBindings>();
  });

  it('does not turn a missing application secret into a bearer credential', async () => {
    await server.update(options('2', ''));
    worker = server.getWorker<HarnessBindings>();
    for (const token of [secrets.application?.APP_PROBE_TOKEN, 'undefined', ''])
      expect(
        (
          await appFetch('/__direct/health', {
            headers: { authorization: `Bearer ${token}` },
          })
        ).status,
      ).toBe(401);
  });
});

describe('direct reference harness provider surface', () => {
  const ACCOUNT = 'https://api.cloudflare.com/client/v4/accounts/account';
  const deployment = `${ACCOUNT}/workers/scripts/absent-script/deployments/deployment`;
  const version = `${ACCOUNT}/workers/scripts/pinned-script/versions/pinned-version`;

  const seedVersion = (
    harness: Awaited<ReturnType<typeof createDirectReferenceHarness>>,
  ) =>
    harness.world.seedScript('pinned-script', {
      versions: [
        {
          versionId: 'pinned-version',
          tag: undefined,
          bindings: [],
          mainModule: 'index.mjs',
          modules: [],
        },
      ],
      subdomain: { enabled: false, previewsEnabled: false },
    });

  const versionResources = async (response: Response) =>
    ((await response.json()) as { result: { resources: object } }).result
      .resources;

  it('answers the Node-side deployment read only when the harness opts in', async () => {
    const gated = await createDirectReferenceHarness();
    try {
      expect((await gated.projection.fetch(deployment)).status).toBe(404);
    } finally {
      await gated.close();
    }
    const opted = await createDirectReferenceHarness({
      nodeProviderRest: true,
    });
    try {
      const response = await opted.projection.fetch(deployment);
      const body = (await response.json()) as { result: { id: string } };
      expect({ status: response.status, id: body.result.id }).toEqual({
        status: 200,
        id: 'deployment',
      });
    } finally {
      await opted.close();
    }
  });

  it('adds the observed runtime to a bare version read only when the harness opts in', async () => {
    const gated = await createDirectReferenceHarness();
    try {
      seedVersion(gated);
      const response = await gated.projection.fetch(version);
      expect(response.status).toBe(200);
      // The gated projection answers the fixture's own version record, so a
      // consumer reading `script_runtime` here would be reading the observer,
      // not the provider.
      expect(await versionResources(response)).not.toHaveProperty(
        'script_runtime',
      );
    } finally {
      await gated.close();
    }
    const opted = await createDirectReferenceHarness({
      nodeProviderRest: true,
    });
    try {
      seedVersion(opted);
      const response = await opted.projection.fetch(version);
      expect(response.status).toBe(200);
      expect(await versionResources(response)).toHaveProperty('script_runtime');
    } finally {
      await opted.close();
    }
  });
});
