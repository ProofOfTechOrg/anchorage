// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { createServer, type Server } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { fileURLToPath } from 'node:url';
import type { D1Database, R2Bucket } from '@cloudflare/workers-types';
import {
  assertExecutionFenceState,
  assertMutationEpoch,
  DeploymentInventory,
  DoStatusError,
  type ExecutionFenceDatabase,
  type ExecutionFenceStatement,
  ExecutionFenceStore,
  executionFenceReadingPayload,
  InvalidInventoryRequestError,
  type InventoryDatabase,
  type InventoryStatement,
  isInventoryCategory,
  MutationEpochMismatchError,
} from '@proofoftech/flowsafe/do-runner';
import { expect } from 'vitest';
import { createTestHarness, type TestHarness } from 'wrangler';
import type { DirectRunManifest } from '../../scripts/direct-credentialed-conformance-preflight.mjs';
import {
  type DirectFixtureRole,
  directDeploymentSpec,
} from '../../scripts/direct-credentialed-spec.js';
import {
  DIRECT_CONTINUATION_STEP,
  DIRECT_CONTINUATION_WORKFLOW,
  DIRECT_TENANT_OBJECT_BODY,
  DIRECT_TENANT_OBJECT_KEY,
  DIRECT_TENANT_ROUTES,
  directTenantMutationEpoch,
  directTenantProbeEpoch,
} from '../../scripts/direct-credentialed-tenant-object.mjs';
import type { DirectRunBinding } from '../../scripts/direct-reference-context.js';
import {
  DIRECT_REFERENCE_PATH,
  type DirectReferenceAction,
  directReferenceRequestSha256,
} from '../../scripts/direct-reference-contract.mjs';
import { DirectReferenceJournal } from '../../scripts/direct-reference-journal.js';
import { D1FleetStateDatabase } from '../../src/d1-fleet-state-database.js';
import { D1FleetStateStore } from '../../src/state-store.js';
import type { DeploymentSecrets } from '../../src/types.js';
import {
  type CloudflareFixtureRequest,
  recordingFetch,
  restProjection,
  single,
} from './cloudflare-fetch-fixture.js';
import { directFixtureManifest } from './direct-credentialed-config.js';
import type {
  directForceBudgetProbe,
  ForceBudgetStage,
} from './direct-force-budget-probe.js';
import { recoveryApplicationSpec } from './direct-reference-context-harness.js';
import {
  type D1State,
  deploymentIdentity,
  maintenanceResponder,
  providerWorld,
  type SqliteBinding,
} from './provider-world.js';

/** Tenant routes the harness answers with the maintenance credential. */
const ADMIN_ROUTES: readonly string[] = Object.freeze([
  '/admin/execution-fence',
  '/admin/inventory',
]);

/** Tenant routes the harness answers with the application probe credential. */
const APPLICATION_ROUTES: readonly string[] = Object.freeze(
  Object.values(DIRECT_TENANT_ROUTES),
);

/** The union a supplied `applicationFetch` is handed. */
const TENANT_ROUTES: readonly string[] = Object.freeze([
  ...APPLICATION_ROUTES,
  ...ADMIN_ROUTES,
]);

function nativeTenantRoute(request: CloudflareFixtureRequest, url: URL) {
  if (TENANT_ROUTES.includes(url.pathname)) return true;
  if (request.method === 'POST' && url.pathname === '/runs') return true;
  if (
    request.method === 'POST' &&
    /^\/api\/approvals\/[A-Za-z0-9_-]{1,128}\/decide$/u.test(url.pathname)
  )
    return true;
  const segments = url.pathname.split('/').filter(Boolean);
  if (
    segments[0] !== 'runs' ||
    segments[1] !== DIRECT_CONTINUATION_WORKFLOW ||
    !segments[2] ||
    !/^[A-Za-z0-9_-]{1,128}$/u.test(segments[2])
  )
    return false;
  return (
    (request.method === 'GET' && segments.length === 3) ||
    (request.method === 'POST' &&
      segments.length === 4 &&
      segments[3] === 'resume')
  );
}

/** Projects a do-runner fault onto the status response the tenant returns. */
function doStatusResponse(error: unknown): Response {
  if (!(error instanceof DoStatusError)) throw error;
  return Response.json(
    {
      error: error.message,
      ...(error.reason === undefined ? {} : { reason: error.reason }),
    },
    { status: error.status },
  );
}

/** Reads the epoch classification a fence comparison refused with. */
function epochMismatch(error: unknown): MutationEpochMismatchError {
  if (!(error instanceof MutationEpochMismatchError)) throw error;
  return error;
}

function fixtureFenceDatabase(
  state: D1State,
): ExecutionFenceDatabase & InventoryDatabase {
  return {
    prepare(query: string) {
      let bindings: readonly SqliteBinding[] = [];
      const statement: ExecutionFenceStatement & InventoryStatement = {
        bind(...values: unknown[]) {
          bindings = values as SqliteBinding[];
          return statement;
        },
        async all<T = unknown>(): Promise<{ results: T[] }> {
          const rows: readonly unknown[] = state.queryDatabase(query, bindings);
          return { results: [...rows] as T[] };
        },
        async run(): Promise<unknown> {
          return state.queryDatabase(query, bindings);
        },
      };
      return statement;
    },
  };
}

async function fixtureExecutionFence(
  request: CloudflareFixtureRequest,
  state: D1State,
): Promise<Response> {
  if (request.method !== 'GET' && request.method !== 'POST')
    return Response.json({ error: 'method not allowed' }, { status: 405 });
  const fence = new ExecutionFenceStore(fixtureFenceDatabase(state));
  try {
    if (request.method === 'GET')
      return Response.json(executionFenceReadingPayload(await fence.read()));
    const parsed = request.body;
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed))
      return Response.json(
        { error: 'a JSON object body is required' },
        { status: 400 },
      );
    const body = parsed as Record<string, unknown>;
    const reading = await fence.transition({
      expected: assertExecutionFenceState(body.expected, 'expected'),
      next: assertExecutionFenceState(body.next, 'next'),
      ...(body.proofKey === undefined ? {} : { proofKey: body.proofKey }),
      expectedMutationEpoch: body.expectedMutationEpoch,
      expectedRevision: body.expectedRevision,
      advanceMutationEpoch: body.advanceMutationEpoch,
    });
    return Response.json(executionFenceReadingPayload(reading));
  } catch (error) {
    return doStatusResponse(error);
  }
}

async function fixtureInventory(
  request: CloudflareFixtureRequest,
  url: URL,
  state: D1State,
): Promise<Response> {
  if (request.method !== 'GET')
    return Response.json({ error: 'method not allowed' }, { status: 405 });
  try {
    const inventory = new DeploymentInventory(fixtureFenceDatabase(state));
    const category = url.searchParams.get('category');
    if (category === null || category === '')
      return Response.json(inventory.index());
    if (!isInventoryCategory(category))
      throw new InvalidInventoryRequestError(
        `unknown inventory category '${category}'`,
      );
    const rawLimit = url.searchParams.get('limit');
    if (rawLimit !== null && !/^[0-9]{1,4}$/.test(rawLimit))
      throw new InvalidInventoryRequestError(
        'inventory limit must be a positive integer',
      );
    const cursor = url.searchParams.get('cursor');
    return Response.json(
      await inventory.read(category, {
        ...(cursor === null ? {} : { cursor }),
        ...(rawLimit === null ? {} : { limit: Number(rawLimit) }),
      }),
    );
  } catch (error) {
    return doStatusResponse(error);
  }
}

async function fixtureFenceOutcome(
  state: D1State,
  epoch: number,
): Promise<Response> {
  const reading = await new ExecutionFenceStore(
    fixtureFenceDatabase(state),
  ).read();
  try {
    assertMutationEpoch(reading, epoch);
    return Response.json({ accepted: true });
  } catch (error) {
    const mismatch = epochMismatch(error);
    return Response.json({
      accepted: false,
      code: mismatch.reason.code,
      classification: mismatch.reason.classification,
      status: mismatch.status,
    });
  }
}

async function fixtureFenceProbe(
  request: CloudflareFixtureRequest,
  state: D1State,
  release: string | undefined,
): Promise<Response> {
  const body = request.body;
  const epoch =
    body && typeof body === 'object' && !Array.isArray(body)
      ? Reflect.get(body, 'epoch')
      : undefined;
  if (!['current', 'missing', 'stale', 'future'].includes(epoch))
    return Response.json({ error: 'invalid epoch label' }, { status: 400 });
  const supplied = directTenantProbeEpoch(release, epoch);
  const reading = await new ExecutionFenceStore(
    fixtureFenceDatabase(state),
  ).read();
  try {
    assertMutationEpoch(reading, supplied);
    return Response.json({ epoch, classification: 'accepted' });
  } catch (error) {
    return Response.json({
      epoch,
      classification: epochMismatch(error).reason.classification,
    });
  }
}

export async function createDirectReferenceHarness(
  policy: Readonly<{
    maintenanceNow?: () => number;
    manifest?: DirectRunManifest;
    binding?: DirectRunBinding;
    recoveryApplicationR2Bucket?: string;
    applicationProbes?: boolean;
    nodeProviderRest?: boolean;
    nodeResponse?: (request: Request, response: Response) => Promise<Response>;
    applicationFetch?: (request: CloudflareFixtureRequest) => Promise<Response>;
    applicationFetchRole?: DirectFixtureRole;
    applicationFetchActive?: () => boolean;
    providerResponse?: (
      request: CloudflareFixtureRequest,
      response: Response,
    ) => Promise<Response>;
    providerRequest?: (
      request: CloudflareFixtureRequest,
    ) => Promise<Response | undefined>;
  }> = {},
) {
  const manifest = policy.manifest ?? directFixtureManifest();
  const roles = ['a', 'b', 'recovery'] as const;
  function fixtureSecrets(role: string): DeploymentSecrets {
    return {
      deploymentIdentity: `identity-${role}`.padEnd(40, 'i'),
      maintenanceAdmin: `maintenance-${role}`.padEnd(40, 'm'),
      application: { APP_PROBE_TOKEN: `probe-${role}`.padEnd(40, 'p') },
    };
  }
  const secrets = {
    a: fixtureSecrets('a'),
    b: fixtureSecrets('b'),
    recovery: fixtureSecrets('recovery'),
  };
  const binding = policy.binding ?? {
    version: 1,
    accountId: 'account',
    fleetDatabaseId: '00000000-0000-0000-0000-000000000011',
    quotaDatabaseId: '00000000-0000-0000-0000-000000000012',
    exportBucketName: manifest.names.exportBucket,
    referenceModuleSetSha256: 'c'.repeat(64),
    accountWorkersDevSubdomain: 'direct-fixture',
  };
  const specs = roles.map((role) => {
    const spec = directDeploymentSpec(
      manifest,
      role,
      'initial',
      secrets[role],
      binding,
    );
    return role === 'recovery'
      ? recoveryApplicationSpec(spec, policy.recoveryApplicationR2Bucket)
      : spec;
  });
  /** True for the run's own export bucket, false for a tenant's. */
  const isExportBucket = (name: string | undefined) =>
    name === binding.exportBucketName;
  const exportObjectsPath = `/r2/buckets/${binding.exportBucketName}/objects/`;
  const exportObjectsPrefix = `/client/v4/accounts/account${exportObjectsPath}`;
  const exportObjectKey = (pathname: string) =>
    decodeURIComponent(pathname.split('/objects/')[1] ?? '');

  let directory: string;
  let server: TestHarness;
  let bridge: Server;
  let db: D1Database;
  let fleetStore: D1FleetStateStore;
  let applicationBytes: R2Bucket;
  let exportBytes: R2Bucket;
  const world = providerWorld('uuid');
  world.accountSubdomain = binding.accountWorkersDevSubdomain;
  const bridgeErrors: unknown[] = [];
  const sqlFailures: string[] = [];
  const buckets = new Map<
    string,
    { name: string; jurisdiction: string; creation_date: string }
  >();
  const rest = restProjection(world);
  const versionRuntime = new Map<string, unknown>();
  const continuationRuns = new Map<
    string,
    { challenge: string; approvalId: string; status: 'suspended' | 'success' }
  >();
  const activeVersion = (script: ReturnType<typeof world.scripts.get>) =>
    script?.versions.find((version) =>
      script.deployment?.some(
        (entry) =>
          entry.versionId === version.versionId && entry.percentage === 100,
      ),
    );
  /**
   * Answers the export bucket's per-key object routes out of `exportBytes`.
   * `policy.nodeProviderRest` decides whether these answer or the plain
   * projection does.
   */
  async function exportObjectResponse(
    request: CloudflareFixtureRequest,
    url: URL,
  ): Promise<Response | undefined> {
    if (!url.pathname.startsWith(exportObjectsPrefix)) return undefined;
    if (request.method === 'GET') {
      const value = await exportBytes.get(exportObjectKey(url.pathname));
      return value
        ? new Response(await value.arrayBuffer())
        : new Response(null, { status: 404 });
    }
    if (request.method === 'DELETE') {
      await exportBytes.delete(exportObjectKey(url.pathname));
      return single({});
    }
    return undefined;
  }
  async function observedProviderRest(
    request: CloudflareFixtureRequest,
  ): Promise<Response> {
    const url = new URL(request.url);
    const scriptName = url.pathname
      .split('/workers/scripts/')[1]
      ?.split('/')[0];
    const script = scriptName ? world.scripts.get(scriptName) : undefined;
    const metadata =
      request.body &&
      typeof request.body === 'object' &&
      'metadata' in request.body
        ? (request.body.metadata as Record<string, unknown>)
        : undefined;
    if (
      request.method === 'GET' &&
      url.pathname.endsWith(`/deployments/${deploymentIdentity(script)}`)
    )
      return single({
        id: deploymentIdentity(script),
        strategy: 'percentage',
        versions: script?.deployment?.map(({ versionId, percentage }) => ({
          version_id: versionId,
          percentage,
        })),
      });
    if (
      request.method === 'GET' &&
      url.pathname.endsWith('/settings') &&
      script?.present
    ) {
      const active = activeVersion(script);
      const runtime = versionRuntime.get(active?.versionId ?? '') as
        | Record<string, unknown>
        | undefined;
      return single({
        ...runtime,
        bindings: active?.bindings.map((entry) => {
          const value = entry as Record<string, unknown>;
          return value.type === 'secret_text'
            ? { type: value.type, name: value.name }
            : value;
        }),
      });
    }
    if (
      request.method === 'POST' &&
      url.pathname ===
        `/client/v4/accounts/account/d1/database/${binding.fleetDatabaseId}/query`
    ) {
      const query = request.body as { sql: string; params: string[] };
      if (
        !query.sql.startsWith('SELECT ') ||
        (!query.sql.includes(' FROM direct_reference_observations WHERE ') &&
          !query.sql.includes(' FROM anchorage_fleet_deployments WHERE '))
      )
        throw new Error('unexpected Node D1 query');
      const result = await db
        .prepare(query.sql)
        .bind(...query.params)
        .all();
      return single([{ success: true, results: result.results }]);
    }
    const exported = await exportObjectResponse(request, url);
    if (exported) return exported;
    let response = await rest(request);
    if (metadata && response.ok && scriptName) {
      const current = world.scripts.get(scriptName);
      for (const version of current?.versions ?? [])
        if (!versionRuntime.has(version.versionId))
          versionRuntime.set(version.versionId, {
            compatibility_date: metadata.compatibility_date,
            compatibility_flags: metadata.compatibility_flags ?? [],
            limits: metadata.limits,
          });
    }
    if (
      request.method === 'GET' &&
      /\/versions\/[^/]+$/u.test(url.pathname) &&
      response.ok
    ) {
      const value = (await response.json()) as {
        result: { id: string; resources: Record<string, unknown> };
      };
      const runtime = versionRuntime.get(value.result.id) as
        | { limits?: { cpu_ms?: number } }
        | undefined;
      value.result.resources.script_runtime = {
        ...runtime,
        limits: { cpu_ms: runtime?.limits?.cpu_ms },
      };
      response = Response.json(value);
    }
    return response;
  }
  async function providerRest(
    request: CloudflareFixtureRequest,
  ): Promise<Response> {
    try {
      const supplied = await policy.providerRequest?.(request);
      const response =
        supplied ??
        (policy.nodeProviderRest
          ? await observedProviderRest(request)
          : await rest(request));
      return policy.providerResponse
        ? await policy.providerResponse(request, response)
        : response;
    } catch (error) {
      if (
        new URL(request.url).pathname.endsWith('/query') &&
        error instanceof Error &&
        'code' in error &&
        error.code === 'ERR_SQLITE_ERROR'
      ) {
        sqlFailures.push(error.message);
        return Response.json(
          {
            success: false,
            errors: [{ code: 1, message: 'fixture SQL query failed' }],
          },
          { status: 400 },
        );
      }
      throw error;
    }
  }
  async function applicationProbe(
    request: CloudflareFixtureRequest,
  ): Promise<Response> {
    const url = new URL(request.url);
    const role = roles.find(
      (candidateRole) =>
        url.hostname === manifest.names.roles[candidateRole].routeHostname,
    );
    if (!role) throw new Error('unknown fixture application role');
    const adminRoute = ADMIN_ROUTES.includes(url.pathname);
    if (adminRoute) {
      if (
        request.headers.get('authorization') !==
        `Bearer ${secrets[role].maintenanceAdmin}`
      )
        return new Response(null, { status: 401 });
    } else if (
      request.headers.get('authorization') !==
      `Bearer ${secrets[role].application?.APP_PROBE_TOKEN}`
    )
      return new Response(null, { status: 401 });
    const record = await fleetStore.get(
      manifest.names.roles[role].tenantTag,
      manifest.environment,
    );
    if (!record) throw new Error('missing fixture application record');
    const database = world.databases.find(
      (candidateDatabase) => candidateDatabase.databaseId === record.databaseId,
    );
    const active = activeVersion(world.scripts.get(record.scriptName));
    if (!database || !active)
      throw new Error('missing active fixture application');
    const releaseBinding = active.bindings.find(
      (candidateBinding) =>
        candidateBinding &&
        typeof candidateBinding === 'object' &&
        Reflect.get(candidateBinding, 'name') === 'APPLICATION_RELEASE',
    );
    const release =
      releaseBinding && typeof releaseBinding === 'object'
        ? Reflect.get(releaseBinding, 'text')
        : undefined;
    if (
      url.pathname === DIRECT_TENANT_ROUTES.health &&
      request.method === 'GET'
    ) {
      const rows = database.d1.queryDatabase(
        'SELECT marker FROM direct_conformance_fixture WHERE id=1',
      );
      return Response.json({ release, marker: rows[0]?.marker });
    }
    if (url.pathname === '/admin/execution-fence')
      return fixtureExecutionFence(request, database.d1);
    if (url.pathname === '/admin/inventory')
      return fixtureInventory(request, url, database.d1);
    if (
      url.pathname === DIRECT_TENANT_ROUTES.fenceMutate &&
      request.method === 'POST'
    )
      return fixtureFenceOutcome(
        database.d1,
        directTenantMutationEpoch(release),
      );
    if (
      url.pathname === DIRECT_TENANT_ROUTES.fenceProbe &&
      request.method === 'POST'
    )
      return fixtureFenceProbe(request, database.d1, release);
    if (url.pathname === '/runs' && request.method === 'POST') {
      const body = request.body as {
        workflowId?: unknown;
        inputData?: { challenge?: unknown };
      };
      const challenge = body.inputData?.challenge;
      if (
        body.workflowId !== DIRECT_CONTINUATION_WORKFLOW ||
        typeof challenge !== 'string' ||
        !/^[a-f0-9]{64}$/u.test(challenge)
      )
        return Response.json(
          { error: 'invalid continuation input' },
          { status: 400 },
        );
      const runId = crypto.randomUUID();
      const approvalId = crypto.randomUUID();
      continuationRuns.set(runId, {
        challenge,
        approvalId,
        status: 'suspended',
      });
      return Response.json({
        runId,
        status: 'suspended',
        suspended: [[DIRECT_CONTINUATION_STEP]],
        suspendPayload: {
          [DIRECT_CONTINUATION_STEP]: { reason: 'awaiting-resume' },
        },
        approval: { id: approvalId },
      });
    }
    const continuation = url.pathname.match(
      new RegExp(
        `^/runs/${DIRECT_CONTINUATION_WORKFLOW}/([A-Za-z0-9_-]{1,128})(/resume)?$`,
        'u',
      ),
    );
    if (continuation?.[1]) {
      const run = continuationRuns.get(continuation[1]);
      if (!run)
        return Response.json({ error: 'run not found' }, { status: 404 });
      if (request.method === 'GET' && !continuation[2])
        return Response.json(
          run.status === 'suspended'
            ? {
                runId: continuation[1],
                status: 'suspended',
                suspended: [[DIRECT_CONTINUATION_STEP]],
                suspendPayload: {
                  [DIRECT_CONTINUATION_STEP]: { reason: 'awaiting-resume' },
                },
              }
            : {
                runId: continuation[1],
                status: 'success',
                result: { challenge: run.challenge, release },
              },
        );
      if (request.method === 'POST' && continuation[2]) {
        const reading = await new ExecutionFenceStore(
          fixtureFenceDatabase(database.d1),
        ).read();
        if (reading.state === 'migration-locked')
          return Response.json(
            {
              error: 'execution fenced',
              reason: {
                code: 'EXECUTION_FENCED',
                state: 'migration-locked',
              },
            },
            { status: 503 },
          );
        run.status = 'success';
        return Response.json({
          runId: continuation[1],
          status: 'success',
          result: { challenge: run.challenge, release },
        });
      }
    }
    const decision = url.pathname.match(
      /^\/api\/approvals\/([A-Za-z0-9_-]{1,128})\/decide$/u,
    );
    if (request.method === 'POST' && decision?.[1]) {
      const found = [...continuationRuns.entries()].find(
        ([, run]) => run.approvalId === decision[1],
      );
      if (!found)
        return Response.json({ error: 'approval not found' }, { status: 404 });
      return Response.json({
        record: {
          id: decision[1],
          runId: found[0],
          status: 'approved',
        },
        resume: { attempted: true, ok: false },
      });
    }
    const bucket = record.applicationResources?.find(
      (resource) => resource.name === 'PROBE_BUCKET',
    );
    if (!bucket || url.pathname !== DIRECT_TENANT_ROUTES.object)
      throw new Error('unknown fixture application route');
    const key = `${bucket.jurisdiction}:${bucket.bucketName}/${DIRECT_TENANT_OBJECT_KEY}`;
    if (request.method === 'POST') {
      await applicationBytes.put(key, DIRECT_TENANT_OBJECT_BODY);
      return new Response(null, { status: 204 });
    }
    if (request.method === 'DELETE') {
      await applicationBytes.delete(key);
      return new Response(null, { status: 204 });
    }
    if (request.method !== 'GET')
      throw new Error('unexpected fixture application method');
    const value = await applicationBytes.get(key);
    return value
      ? Response.json({
          present: true,
          size: value.size,
          sha256: createHash('sha256')
            .update(Buffer.from(await value.arrayBuffer()))
            .digest('hex'),
        })
      : Response.json({ present: false });
  }
  const projection = recordingFetch(async (request) => {
    const url = new URL(request.url);
    const application = specs.some(
      (candidateSpec) =>
        url.origin === `https://${candidateSpec.routeHostname}`,
    );
    const applicationRole = roles.find(
      (candidateRole) =>
        url.origin ===
        `https://${manifest.names.roles[candidateRole].routeHostname}`,
    );
    if (
      application &&
      policy.applicationFetch &&
      (policy.applicationFetchRole === undefined ||
        applicationRole === policy.applicationFetchRole) &&
      (policy.applicationFetchActive?.() ?? true) &&
      nativeTenantRoute(request, url)
    )
      return policy.applicationFetch(request);
    if (application && policy.applicationProbes)
      return applicationProbe(request);
    const spec = specs.find(
      (candidate) => candidate.maintenanceBaseUrl === url.origin,
    );
    if (spec) {
      const override = request.headers.get(
        'Cloudflare-Workers-Version-Overrides',
      );
      const script = world.scripts.get(spec.scriptName);
      if (override) {
        const selected = override.match(/^([^=]+)="([^"]+)"$/u);
        if (
          selected?.[1] !== spec.scriptName ||
          !script?.versions.some((version) => version.versionId === selected[2])
        )
          return new Response('invalid fixture version', { status: 409 });
      }
      const view = new Proxy(world, {
        get(target, propertyKey) {
          if (propertyKey === 'maintenanceOrigin')
            return spec.maintenanceBaseUrl;
          if (propertyKey === 'routeOrigin')
            return `https://${spec.routeHostname}`;
          if (propertyKey === 'scripts')
            return new Map(script ? [[spec.scriptName, script]] : []);
          const value = Reflect.get(target, propertyKey);
          return typeof value === 'function' ? value.bind(target) : value;
        },
      });
      const response =
        (await maintenanceResponder(view, request)) ??
        new Response('unknown fixture maintenance route', { status: 404 });
      if (!response.ok || !policy.maintenanceNow) return response;
      const body = (await response.json()) as Record<string, unknown>;
      const now = policy.maintenanceNow();
      return Response.json({
        ...body,
        alarmAt: now + 60_000,
        lastSweepAt: now,
        lastPurgeAt: now,
      });
    }
    if (url.origin === 'https://d1-export.example.test') {
      expect(request.headers.has('Authorization')).toBe(false);
      return providerRest(request);
    }
    if (url.origin !== 'https://api.cloudflare.com')
      throw new Error('unexpected fixture origin');
    if (url.pathname.includes(exportObjectsPath)) return providerRest(request);
    const supplied = await policy.providerRequest?.(request);
    if (supplied) return supplied;
    const match = url.pathname.match(
      /^\/client\/v4\/accounts\/account\/r2\/buckets(?:\/([^/]+)(\/objects)?)?$/u,
    );
    if (!match) return providerRest(request);
    const jurisdiction = request.headers.get('cf-r2-jurisdiction') ?? 'default';
    const name = match[1] ? decodeURIComponent(match[1]) : undefined;
    if (!name && request.method === 'POST') {
      const requested = (request.body as { name?: unknown }).name;
      const records = await Promise.all(
        specs.map((deploymentSpec) =>
          fleetStore.get(deploymentSpec.tenantTag, deploymentSpec.environment),
        ),
      );
      // `scripts/direct-credentialed-bootstrap.mjs` creates the export bucket
      // in the `default` jurisdiction, and the first arm answers that create.
      // The acceptance seeds the bucket into `buckets` ahead of its run, so
      // there the application-resource arm is the one that admits a POST.
      const authorized =
        typeof requested === 'string' &&
        ((isExportBucket(requested) && jurisdiction === 'default') ||
          records.some((record) =>
            record?.applicationResources?.some(
              (resource) =>
                resource.bucketName === requested &&
                resource.jurisdiction === jurisdiction &&
                resource.state === 'create-authorized',
            ),
          ));
      if (!authorized) throw new Error('unexpected fixture bucket');
      const key = `${jurisdiction}:${requested}`;
      if (buckets.has(key))
        return new Response('bucket exists', { status: 409 });
      const descriptor = {
        name: requested,
        jurisdiction,
        creation_date: new Date().toISOString(),
      };
      buckets.set(key, descriptor);
      return single(descriptor);
    }
    if (!name && request.method === 'GET') {
      const selected = [...buckets.values()]
        .filter(
          (bucket) =>
            bucket.jurisdiction === jurisdiction &&
            bucket.name.includes(url.searchParams.get('name_contains') ?? '') &&
            bucket.name > (url.searchParams.get('start_after') ?? ''),
        )
        .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
      return single({ buckets: selected });
    }
    const key = `${jurisdiction}:${name}`;
    // The injected `getApplicationR2Bucket` failure is armed against a
    // tenant's application bucket. The export bucket the run bootstraps is not
    // one, so this read steps past the failure, as the delete below does.
    if (
      !match[2] &&
      !isExportBucket(name) &&
      request.method === 'GET' &&
      world.consumeFailure('getApplicationR2Bucket')
    )
      return Response.json(
        {
          success: false,
          errors: [{ code: 10000, message: 'fixture bucket read denied' }],
        },
        { status: 403 },
      );
    const descriptor = buckets.get(key);
    if (!descriptor) return Response.json({ errors: [] }, { status: 404 });
    const prefix = `${key}/`;
    if (isExportBucket(name) && match[2] && request.method === 'GET') {
      const objects = await exportBytes.list({
        prefix: url.searchParams.get('prefix') ?? '',
      });
      return single(
        objects.objects.map(({ key: objectKey }) => ({ key: objectKey })),
      );
    }
    if (match[2] && request.method === 'GET') {
      expect(url.searchParams.get('per_page')).toBe('1');
      const objects = await applicationBytes.list({
        prefix,
        limit: 1,
        ...(url.searchParams.get('cursor')
          ? { cursor: url.searchParams.get('cursor') as string }
          : {}),
      });
      return Response.json({
        success: true,
        errors: [],
        messages: [],
        result: objects.objects.map((object) => ({
          key: object.key.slice(prefix.length),
        })),
        result_info: objects.truncated ? { cursor: objects.cursor } : {},
      });
    }
    if (!match[2] && request.method === 'GET') return single(descriptor);
    if (!match[2] && request.method === 'DELETE') {
      if (isExportBucket(name)) {
        // This arm answers before the injected `deleteApplicationR2Bucket`
        // failure below is consumed: the export bucket the run bootstraps is
        // not a tenant application resource.
        const objects = await exportBytes.list({ limit: 1 });
        if (objects.objects.length)
          return new Response('bucket nonempty', { status: 409 });
        buckets.delete(key);
        return single({});
      }
      const failure = world.consumeFailure('deleteApplicationR2Bucket');
      const failed = () =>
        Response.json(
          {
            success: false,
            errors: [{ code: 1, message: 'fixture bucket deletion failed' }],
          },
          { status: 400 },
        );
      if (failure && !failure.dispatched) return failed();
      const objects = await applicationBytes.list({ prefix, limit: 1 });
      if (objects.objects.length)
        return new Response('bucket nonempty', { status: 409 });
      buckets.delete(key);
      await world.applyAfter('deleteApplicationR2Bucket');
      if (failure) return failed();
      return single({});
    }
    throw new Error('unexpected fixture R2 method');
  });

  let reload: () => Promise<void>;
  let bridgeUrl: string;
  async function refreshBindings() {
    const env = await server
      .getWorker<{
        FLEET_DB: D1Database;
        EXPORTS: R2Bucket;
        APPLICATION_BYTES: R2Bucket;
      }>()
      .getEnv();
    db = env.FLEET_DB;
    fleetStore = new D1FleetStateStore(new D1FleetStateDatabase(db), {
      accountId: binding.accountId,
    });
    exportBytes = env.EXPORTS;
    applicationBytes = env.APPLICATION_BYTES;
  }
  async function close() {
    const closed = await Promise.allSettled([
      (async () => server?.close())(),
      (async () => {
        if (bridge) {
          bridge.closeAllConnections();
          await new Promise<void>((resolve, reject) =>
            bridge.close((error) => (error ? reject(error) : resolve())),
          );
        }
      })(),
    ]);
    const failures = closed.flatMap((result) =>
      result.status === 'rejected' ? [result.reason] : [],
    );
    try {
      if (directory) await rm(directory, { recursive: true, force: true });
    } catch (error) {
      failures.push(error);
    }
    if (failures.length)
      throw new AggregateError(
        failures,
        'direct lifecycle fixture teardown failed',
      );
  }

  try {
    directory = await mkdtemp(join(tmpdir(), 'direct-lifecycle-'));
    bridge = createServer(async (incoming, outgoing) => {
      try {
        const original = incoming.headers['x-direct-fixture-url'];
        if (
          typeof original !== 'string' ||
          !['/', '/node'].includes(incoming.url ?? '')
        )
          throw new Error('invalid fixture bridge request');
        const headers = new Headers();
        for (const [name, values] of Object.entries(incoming.headers)) {
          if (
            name === 'x-direct-fixture-url' ||
            name === 'host' ||
            values === undefined
          )
            continue;
          for (const value of Array.isArray(values) ? values : [values])
            headers.append(name, value);
        }
        const method = incoming.method ?? 'GET';
        const init = {
          method,
          headers,
          body:
            method === 'GET' || method === 'HEAD'
              ? undefined
              : Readable.toWeb(incoming),
          duplex: 'half' as const,
        };
        const request = new Request(original, init as RequestInit);
        const body = request.body
          ? request.headers.get('content-type')?.includes('multipart/form-data')
            ? await request.formData()
            : await request.text()
          : undefined;
        const referenceOrigin = `https://${manifest.names.referenceWorker}.${binding.accountWorkersDevSubdomain}.workers.dev`;
        let response: Response;
        if (
          incoming.url === '/node' &&
          original === `${referenceOrigin}${DIRECT_REFERENCE_PATH}`
        ) {
          response = await server.getWorker().fetch(original, {
            method,
            headers: [...headers],
            body: body as string,
          });
        } else {
          if (
            incoming.url === '/node' &&
            new URL(original).origin !== 'https://api.cloudflare.com'
          )
            throw new Error('unexpected Node fixture origin');
          response = await projection.fetch(original, {
            method,
            headers,
            body,
            redirect: 'manual',
          });
        }
        if (incoming.url === '/node' && policy.nodeResponse)
          response = await policy.nodeResponse(request, response);
        outgoing.writeHead(
          response.status,
          Object.fromEntries(response.headers),
        );
        outgoing.end(Buffer.from(await response.arrayBuffer()));
      } catch (error) {
        bridgeErrors.push(error);
        outgoing.statusCode = 500;
        outgoing.end('fixture handler failed');
      }
    });
    await new Promise<void>((resolve) =>
      bridge.listen(0, '127.0.0.1', resolve),
    );
    const address = bridge.address();
    if (!address || typeof address === 'string')
      throw new Error('missing fixture listener');
    bridgeUrl = `http://127.0.0.1:${address.port}/node`;
    const main = join(directory, 'worker.ts');
    const workerSource = fileURLToPath(
      new URL('../../scripts/direct-reference-worker.ts', import.meta.url),
    );
    const contextHarnessSource = fileURLToPath(
      new URL('./direct-reference-context-harness.ts', import.meta.url),
    );
    const budgetProbeSource = fileURLToPath(
      new URL('./direct-force-budget-probe.ts', import.meta.url),
    );
    const recoveryEnvironment = policy.recoveryApplicationR2Bucket
      ? `,DIRECT_RECOVERY_R2_BUCKET:${JSON.stringify(policy.recoveryApplicationR2Bucket)}`
      : '';
    await writeFile(
      main,
      `import {createDirectReferenceWorker} from ${JSON.stringify(workerSource)};
import {directForceBudgetProbe} from ${JSON.stringify(budgetProbeSource)};
const manifest=${JSON.stringify(manifest)};
const providerFetch=async(input,init)=>{const request=new Request(input,init);if(request.url==='data:,')return fetch(request);const headers=new Headers(request.headers);headers.set('X-Direct-Fixture-Url',request.url);return fetch('http://127.0.0.1:${address.port}/',{method:request.method,headers,body:request.body,signal:request.signal,redirect:'manual'});};
const worker=createDirectReferenceWorker(manifest,{fetch:providerFetch});
let instance; export default {async fetch(request,env){instance??=crypto.randomUUID();const current={...env,CLOUDFLARE_API_TOKEN:'inert-provider-token',FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET:'inert-invoke',DIRECT_RUN_BINDING:${JSON.stringify(JSON.stringify(binding))},DIRECT_DEPLOYMENT_SECRETS:${JSON.stringify(JSON.stringify(secrets))}${recoveryEnvironment}};const response=new URL(request.url).pathname==='/__fixture/force-budget'?Response.json(await directForceBudgetProbe(manifest,current,(await request.json()).stage,providerFetch,request.signal)):await worker.fetch(request,current);response.headers.set('X-Fixture-Instance',instance);return response;}};`,
    );
    const options = {
      root: directory,
      workers: [
        {
          config: {
            name: 'direct-lifecycle-harness',
            main,
            compatibility_date: '2026-08-06',
            compatibility_flags: [
              'nodejs_compat',
              'global_fetch_strictly_public',
            ],
            ...(policy.recoveryApplicationR2Bucket
              ? {
                  alias: {
                    './direct-reference-context.js': contextHarnessSource,
                  },
                }
              : {}),
            d1_databases: [
              {
                binding: 'FLEET_DB',
                database_name: 'lifecycle-fleet',
                database_id: binding.fleetDatabaseId,
              },
              {
                binding: 'QUOTA_DB',
                database_name: 'lifecycle-quota',
                database_id: binding.quotaDatabaseId,
              },
            ],
            r2_buckets: [
              { binding: 'EXPORTS', bucket_name: binding.exportBucketName },
              {
                binding: 'APPLICATION_BYTES',
                bucket_name: 'fixture-application-bytes',
              },
            ],
          },
        },
      ],
    };
    server = createTestHarness(options);
    let revision = 0;
    reload = async () => {
      await server.update({
        ...options,
        workers: options.workers.map((worker) => ({
          ...worker,
          config: {
            ...worker.config,
            vars: { TEST_RELOAD: String(++revision) },
          },
        })),
      });
      await refreshBindings();
    };
    await server.listen();
    await refreshBindings();
  } catch (error) {
    try {
      await close();
    } catch (cleanupError) {
      throw new AggregateError(
        [error, cleanupError],
        'direct fixture startup and cleanup failed',
      );
    }
    throw error;
  }
  let fixtureOrdinal = manifest.referenceRuntime.maxInvocations;
  async function call(
    action: DirectReferenceAction,
    suppliedReservation?: Readonly<{
      ordinal: number;
      requestSha256: string;
    }>,
  ) {
    const core = {
      contractVersion: 2,
      configSha256: manifest.configSha256,
      action,
    };
    const reservation =
      action.kind === 'reconcile-invocation'
        ? null
        : (suppliedReservation ?? {
            ordinal: fixtureOrdinal--,
            requestSha256: directReferenceRequestSha256(core),
          });
    if (reservation && reservation.ordinal < 1)
      throw new Error('direct reference fixture invocation budget exhausted');
    const response = await server
      .getWorker()
      .fetch(`https://reference.test${DIRECT_REFERENCE_PATH}`, {
        method: 'POST',
        headers: { authorization: 'Bearer inert-invoke' },
        body: JSON.stringify({
          ...core,
          reservation,
        }),
      });
    return {
      response,
      value: (await response.json()) as {
        ok: boolean;
        result?: unknown;
        error?: unknown;
      },
    };
  }

  async function success<T>(action: DirectReferenceAction): Promise<T> {
    const { response, value } = await call(action);
    expect(bridgeErrors).toEqual([]);
    expect({ status: response.status, value }).toMatchObject({
      status: 200,
      value: { ok: true },
    });
    return value.result as T;
  }

  function journal() {
    return new DirectReferenceJournal(
      db,
      manifest.resourcePrefix,
      JSON.stringify({
        configSha256: manifest.configSha256,
        binding,
        quotaScope: manifest.resourcePrefix,
        tenantSecretsSha256: createHash('sha256')
          .update(JSON.stringify(secrets))
          .digest('hex'),
      }),
      manifest.referenceRuntime.maxInvocations,
    );
  }

  return {
    manifest,
    binding,
    secrets,
    specs,
    world,
    projection,
    get db() {
      return db;
    },
    get fleetStore() {
      return fleetStore;
    },
    get applicationBytes() {
      return applicationBytes;
    },
    get exportBytes() {
      return exportBytes;
    },
    buckets,
    versionRuntime,
    bridgeErrors,
    sqlFailures,
    bridgeUrl,
    fetch: (async (input, init) => {
      const request = new Request(input, init);
      const headers = new Headers(request.headers);
      headers.set('X-Direct-Fixture-Url', request.url);
      return fetch(bridgeUrl, {
        method: request.method,
        headers,
        body: request.body,
        signal: request.signal,
        redirect: 'manual',
        duplex: 'half',
      } as RequestInit);
    }) as typeof fetch,
    call,
    async forceBudgetProbe(stage: ForceBudgetStage) {
      const response = await server
        .getWorker()
        .fetch('https://reference.test/__fixture/force-budget', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ stage }),
        });
      if (!response.ok)
        throw new Error(`force budget probe failed: ${await response.text()}`);
      return response.json() as Promise<
        Awaited<ReturnType<typeof directForceBudgetProbe>>
      >;
    },
    success,
    journal,
    reload,
    close,
  };
}
export type DirectReferenceHarness = Awaited<
  ReturnType<typeof createDirectReferenceHarness>
>;
