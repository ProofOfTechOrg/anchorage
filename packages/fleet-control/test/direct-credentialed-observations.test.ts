// SPDX-License-Identifier: Apache-2.0

import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import {
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest';
import {
  observeDirectWorkerVersion,
  readDirectSettlementEffects,
  verifyDirectDecommissionExport,
} from '../scripts/direct-credentialed-observations.mjs';
import { expectBuiltDist } from './fixtures/built-dist.js';
import {
  directObservationFixture,
  OBSERVATION_TOKEN,
  observationHash,
  SQL_SENTINEL,
} from './fixtures/direct-observations.js';

const fixtures: Awaited<ReturnType<typeof directObservationFixture>>[] = [];
async function fixture(
  timeout?: number,
  stage?: Parameters<typeof directObservationFixture>[1],
) {
  const value = await directObservationFixture(timeout, stage);
  fixtures.push(value);
  return value;
}
const errorShape = {
  name: 'DirectObservationError',
  message: expect.stringMatching(
    /^(invalid-input|outcome-unknown|observation-mismatch|provider-unavailable|budget-exhausted)$/u,
  ),
};
function required<T>(value: T | undefined): T {
  if (value === undefined) throw new Error('fixture value absent');
  return value;
}
function recordAt(value: unknown, path: string): Record<string, unknown> {
  return path
    .split('.')
    .reduce<unknown>(
      (current, key) => (current as Record<string, unknown>)[key],
      value,
    ) as Record<string, unknown>;
}
const selected = (f: Awaited<ReturnType<typeof fixture>>) => ({
  ...f.input,
  ...required(f.expected[0]),
});
async function mutateResponse(
  f: Awaited<ReturnType<typeof fixture>>,
  suffix: string,
  mutate: (value: Record<string, unknown>) => void,
) {
  f.hook(async (request, fallback) => {
    const response = fallback();
    if (!new URL(request.url).pathname.endsWith(suffix)) return response;
    const value = (await response.json()) as Record<string, unknown>;
    mutate(value);
    return Response.json(value);
  });
}

beforeEach(() => {
  vi.stubGlobal(
    'fetch',
    vi.fn(() => {
      throw new Error('unexpected network');
    }),
  );
});
afterEach(async () => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  for (const f of fixtures.splice(0)) {
    await f.close();
    expect(f.unexpected).toEqual([]);
  }
});

describe('fixed Worker observations through the native SDK', () => {
  it('preserves budget exhaustion after SDK error wrapping for each observer', async () => {
    for (const kind of ['version', 'settlement', 'export'] as const) {
      const f = await fixture();
      const exportInput = await f.exportInput();
      const now = vi
        .spyOn(performance, 'now')
        .mockReturnValueOnce(0)
        .mockReturnValue(300_001);
      const before = f.requests.length;
      try {
        const operation =
          kind === 'version'
            ? observeDirectWorkerVersion(selected(f))
            : kind === 'settlement'
              ? readDirectSettlementEffects({
                  ...f.input,
                  expected: f.expected,
                })
              : verifyDirectDecommissionExport(exportInput);
        await expect(operation).rejects.toMatchObject({
          code: 'budget-exhausted',
        });
        expect(f.requests.length).toBe(before);
      } finally {
        now.mockRestore();
      }
    }
  });

  it('preserves budget exhaustion between metadata requests', async () => {
    const f = await fixture();
    let clock = 0;
    const now = vi.spyOn(performance, 'now').mockImplementation(() => clock);
    const before = f.requests.length;
    f.hook((_request, fallback) => {
      clock = 300_001;
      return fallback();
    });
    try {
      await expect(
        observeDirectWorkerVersion(selected(f)),
      ).rejects.toMatchObject({ code: 'budget-exhausted' });
      expect(f.requests.length - before).toBe(1);
    } finally {
      now.mockRestore();
    }
  });

  it.each([
    '/versions/version-a',
    '/settings',
  ])('attests the complete fixed binding inventory at %s', async (suffix) => {
    const corruptions: ((list: Record<string, unknown>[]) => void)[] = [];
    for (const name of [
      'DEPLOYMENT_IDENTITY_SECRET',
      'MAINTENANCE_ADMIN_SECRET',
      'APP_PROBE_TOKEN',
    ]) {
      corruptions.push((list) => {
        const i = list.findIndex((b) => b.name === name);
        list.splice(i, 1);
      });
      corruptions.push((list) => {
        const b = required(
          list.find((listedBinding) => listedBinding.name === name),
        );
        b.type = 'plain_text';
        b.text = 'not-a-secret-binding';
      });
    }
    for (const binding of [
      { type: 'd1', name: 'EXTRA_DB', database_id: 'foreign' },
      { type: 'service', name: 'EXTRA_SERVICE', service: 'foreign' },
      {
        type: 'r2_bucket',
        name: 'EXTRA_BUCKET',
        bucket_name: 'foreign-bucket',
      },
      { type: 'plain_text', name: 'EXTRA_VAR', text: 'foreign' },
      { type: 'secret_text', name: 'EXTRA_SECRET' },
    ])
      corruptions.push((list) => {
        list.push(binding);
      });
    corruptions.push((list) => {
      required(list.find((b) => b.name === 'FLEET_INGRESS_CONTRACT')).text =
        'foreign';
    });
    corruptions.push((list) => {
      list.splice(
        list.findIndex((b) => b.name === 'FLEET_SPEC_DIGEST'),
        1,
      );
    });
    for (const corrupt of corruptions) {
      const f = await fixture();
      required(f.deployment.versions[0]).version_id = 'old-version';
      await mutateResponse(f, suffix, (v) =>
        corrupt(
          recordAt(v, suffix === '/settings' ? 'result' : 'result.resources')
            .bindings as Record<string, unknown>[],
        ),
      );
      await expect(
        observeDirectWorkerVersion(selected(f)),
      ).rejects.toMatchObject({ code: 'observation-mismatch' });
    }
  });

  it.each([
    0, 100,
  ])('attests %i%% traffic, version CPU, current subrequests and allocated identities', async (percentage) => {
    const f = await fixture();
    if (!percentage)
      required(f.deployment.versions[0]).version_id = 'old-version';
    const before = f.journal.snapshot();
    const output = await observeDirectWorkerVersion(selected(f));
    expect(output).toMatchObject({
      role: 'a',
      versionId: 'version-a',
      trafficPercentage: percentage,
      cpuLimitMs: 50,
      subrequestLimit: 50,
      schemaVersion: 2,
      applicationRelease: '2',
      databaseId: 'database-a',
      namespaces: [
        { binding: 'MAINTENANCE', namespaceId: 'maintenance-id' },
        { binding: 'RUNNER', namespaceId: 'runner-id' },
      ],
      bucket: {
        name: 'allocated-probe-bucket',
        creationDate: '2026-09-09T13:00:00.000Z',
        jurisdiction: 'default',
      },
    });
    expect(Object.isFrozen(output)).toBe(true);
    expect(Object.isFrozen(output.currentDeployment.versions[0])).toBe(true);
    expect(f.requests).toHaveLength(5);
    expect(f.requests.every((request) => request.method === 'GET')).toBe(true);
    expect(f.requests.at(-1)?.headers.get('cf-r2-jurisdiction')).toBe(
      'default',
    );
    expect(f.journal.snapshot()).toEqual(before);
    expect(JSON.stringify(output)).not.toContain(OBSERVATION_TOKEN);
  });

  it('accepts explicit zero-weight candidate entries and captures inputs before awaits', async () => {
    const f = await fixture();
    f.deployment.versions.splice(
      0,
      1,
      { version_id: 'old-version', percentage: 100 },
      { version_id: 'version-a', percentage: 0 },
    );
    const input = selected(f);
    const operation = observeDirectWorkerVersion(input);
    input.role = 'b';
    input.versionId = 'foreign-version';
    input.databaseId = 'foreign-db';
    input.specDigest = 'f'.repeat(64);
    const result = await operation;
    expect(result.role).toBe('a');
    expect(result.trafficPercentage).toBe(0);
    expect(result.currentDeployment.versions).toHaveLength(2);
  });

  it('keeps version subrequests distinct from current settings and accepts optional metadata', async () => {
    const f = await fixture();
    await mutateResponse(f, '/versions/version-a', (value) => {
      recordAt(value, 'result.resources.script_runtime.limits').subrequests =
        999;
      recordAt(value, 'result.resources').script = { etag: 'provider-etag' };
      recordAt(value, 'result').number = 3;
    });
    expect(
      (await observeDirectWorkerVersion(selected(f))).subrequestLimit,
    ).toBe(50);
  });

  it('accepts absent version compatibility_flags when configuration declares no flags', async () => {
    const f = await fixture();
    expect(f.prepared.config.deployment.compatibilityFlags).toEqual([]);
    await mutateResponse(f, '/versions/version-a', (value) => {
      delete recordAt(value, 'result.resources.script_runtime')
        .compatibility_flags;
    });
    await expect(
      observeDirectWorkerVersion(selected(f)),
    ).resolves.toMatchObject({
      versionId: 'version-a',
      trafficPercentage: 100,
    });
  });

  it('refuses explicit null version compatibility_flags when configuration declares no flags', async () => {
    const f = await fixture();
    expect(f.prepared.config.deployment.compatibilityFlags).toEqual([]);
    await mutateResponse(f, '/versions/version-a', (value) => {
      recordAt(value, 'result.resources.script_runtime').compatibility_flags =
        null;
    });
    await expect(observeDirectWorkerVersion(selected(f))).rejects.toMatchObject(
      { code: 'observation-mismatch' },
    );
  });

  it('starts a fresh bounded SDK session for each observation', async () => {
    const f = await fixture();
    const now = vi.spyOn(performance, 'now').mockReturnValue(0);
    expect((await observeDirectWorkerVersion(selected(f))).versionId).toBe(
      'version-a',
    );
    now.mockReturnValue(600_000);
    expect((await observeDirectWorkerVersion(selected(f))).versionId).toBe(
      'version-a',
    );
    expect(f.requests).toHaveLength(10);
  });

  it.each([
    [
      'missing version id',
      '/versions/version-a',
      (v: Record<string, unknown>) => {
        delete recordAt(v, 'result').id;
      },
    ],
    [
      'wrong version id',
      '/versions/version-a',
      (v: Record<string, unknown>) => {
        recordAt(v, 'result').id = 'wrong';
      },
    ],
    [
      'missing CPU',
      '/versions/version-a',
      (v: Record<string, unknown>) => {
        delete recordAt(v, 'result.resources.script_runtime.limits').cpu_ms;
      },
    ],
    [
      'wrong CPU',
      '/versions/version-a',
      (v: Record<string, unknown>) => {
        recordAt(v, 'result.resources.script_runtime.limits').cpu_ms = 999;
      },
    ],
    [
      'wrong runtime date',
      '/versions/version-a',
      (v: Record<string, unknown>) => {
        recordAt(v, 'result.resources.script_runtime').compatibility_date =
          '2020-01-01';
      },
    ],
    [
      'wrong runtime flags',
      '/versions/version-a',
      (v: Record<string, unknown>) => {
        recordAt(v, 'result.resources.script_runtime').compatibility_flags = [
          'wrong',
        ];
      },
    ],
    [
      'missing subrequests',
      '/settings',
      (v: Record<string, unknown>) => {
        delete recordAt(v, 'result.limits').subrequests;
      },
    ],
    [
      'wrong subrequests',
      '/settings',
      (v: Record<string, unknown>) => {
        recordAt(v, 'result.limits').subrequests = 3;
      },
    ],
    [
      'different exact deployment',
      '/deployments/deployment-a',
      (v: Record<string, unknown>) => {
        recordAt(v, 'result').id = 'wrong';
      },
    ],
    [
      'missing deployments',
      '/deployments',
      (v: Record<string, unknown>) => {
        delete recordAt(v, 'result').deployments;
      },
    ],
    [
      'empty deployment list',
      '/deployments',
      (v: Record<string, unknown>) => {
        recordAt(v, 'result').deployments = [];
      },
    ],
    [
      'false envelope success',
      '/deployments',
      (v: Record<string, unknown>) => {
        v.success = false;
      },
    ],
    [
      'missing result',
      '/deployments',
      (v: Record<string, unknown>) => {
        delete v.result;
      },
    ],
    [
      'envelope errors',
      '/deployments',
      (v: Record<string, unknown>) => {
        v.errors = [{ message: OBSERVATION_TOKEN }];
      },
    ],
    [
      'wrong bucket incarnation name',
      '/allocated-probe-bucket',
      (v: Record<string, unknown>) => {
        recordAt(v, 'result').name = 'foreign';
      },
    ],
    [
      'missing creation date',
      '/allocated-probe-bucket',
      (v: Record<string, unknown>) => {
        delete recordAt(v, 'result').creation_date;
      },
    ],
    [
      'invalid creation date',
      '/allocated-probe-bucket',
      (v: Record<string, unknown>) => {
        recordAt(v, 'result').creation_date = 'invalid';
      },
    ],
    [
      'foreign jurisdiction',
      '/allocated-probe-bucket',
      (v: Record<string, unknown>) => {
        recordAt(v, 'result').jurisdiction = 'eu';
      },
    ],
  ])('rejects %s', async (_name, suffix, mutate) => {
    const f = await fixture();
    await mutateResponse(
      f,
      suffix as string,
      mutate as (value: Record<string, unknown>) => void,
    );
    await expect(observeDirectWorkerVersion(selected(f))).rejects.toMatchObject(
      errorShape,
    );
  });

  it.each([
    [],
    [{ version_id: 'version-a', percentage: 99 }],
    [{ version_id: 'version-a', percentage: -1 }],
    [{ version_id: 'version-a', percentage: '100' }],
    [
      { version_id: 'version-a', percentage: 100 },
      { version_id: 'version-a', percentage: 0 },
    ],
    [
      { version_id: 'version-a', percentage: 50 },
      { version_id: 'old', percentage: 50 },
    ],
  ])('rejects incomplete or ambiguous traffic %j', async (...versions) => {
    const f = await fixture();
    await mutateResponse(f, '/deployments', (v) => {
      recordAt(v, 'result.deployments.0').versions = versions;
    });
    await expect(observeDirectWorkerVersion(selected(f))).rejects.toMatchObject(
      errorShape,
    );
  });

  it.each([
    ['DB', 'database_id', 'foreign'],
    ['MAINTENANCE', 'class_name', 'Runner'],
    ['RUNNER', 'namespace_id', 'maintenance-id'],
    ['RUNNER', 'script_name', 'foreign'],
    ['RUNNER', 'dispatch_namespace', 'foreign'],
    ['RUNNER', 'environment', 'foreign'],
    ['PROBE_BUCKET', 'jurisdiction', 'eu'],
    ['PROBE_BUCKET', 'bucket_name', 'invalid/bucket'],
    ['DEPLOYMENT_TENANT', 'text', 'foreign'],
    ['FLEET_ENVIRONMENT', 'text', 'foreign'],
    ['FLEET_SCHEMA_VERSION', 'text', '1'],
    ['FLEET_SPEC_DIGEST', 'text', 'f'.repeat(64)],
    ['APPLICATION_RELEASE', 'text', '1'],
    ['APP_PROBE_TOKEN', 'text', OBSERVATION_TOKEN],
  ])('rejects binding drift %s.%s', async (name, field, value) => {
    const f = await fixture();
    await mutateResponse(f, '/versions/version-a', (v) => {
      required(
        (
          recordAt(v, 'result.resources').bindings as Record<string, unknown>[]
        ).find((b) => b.name === name),
      )[required(field)] = value;
    });
    await expect(observeDirectWorkerVersion(selected(f))).rejects.toMatchObject(
      errorShape,
    );
  });

  it.each([
    'missing',
    'duplicate',
    'unsupported',
  ])('rejects %s bindings', async (change) => {
    const f = await fixture();
    await mutateResponse(f, '/versions/version-a', (v) => {
      const list = recordAt(v, 'result.resources').bindings as Record<
        string,
        unknown
      >[];
      if (change === 'missing') list.shift();
      if (change === 'duplicate') list.push(required(list[0]));
      if (change === 'unsupported')
        list.push({ type: 'kv_namespace', name: 'OTHER' });
    });
    await expect(observeDirectWorkerVersion(selected(f))).rejects.toMatchObject(
      errorShape,
    );
  });

  it.each([
    'Application/JSON; charset=utf-8',
    'APPLICATION/problem+JSON; profile="CaseSensitive"',
  ])('routes %s through validated JSON before SDK parser selection', async (mime) => {
    const f = await fixture();
    f.hook((_request, fallback) => {
      const r = fallback();
      r.headers.set('content-type', mime);
      return r;
    });
    expect((await observeDirectWorkerVersion(selected(f))).versionId).toBe(
      'version-a',
    );
    f.hook(
      () =>
        new Response('{"success":true}', { headers: { 'content-type': mime } }),
    );
    await expect(observeDirectWorkerVersion(selected(f))).rejects.toMatchObject(
      errorShape,
    );
  });
});

describe('confirmed context before provider work', () => {
  it.each([
    'absent',
    'provider-pending',
    'invocation-pending',
    'wrong-prefix',
    'wrong-names',
    'wrong-fleet',
    'no-control',
    'wrong-digest',
  ])('refuses %s context for every helper without dispatch', async (kind) => {
    const stage =
      kind === 'absent' || kind === 'provider-pending' || kind === 'no-control'
        ? kind
        : 'confirmed';
    const f = await fixture(undefined, stage);
    if (kind === 'invocation-pending')
      await f.journal.reserveInvocation(
        JSON.stringify({
          contractVersion: 1,
          configSha256: f.prepared.configSha256,
          action: { kind: 'control-read' },
        }),
      );
    const prepared = { ...structuredClone(f.prepared) };
    if (kind === 'wrong-prefix')
      recordAt(prepared, 'config').resourcePrefix = 'foreign';
    if (kind === 'wrong-names')
      recordAt(prepared, 'names.roles.a').scriptName = 'foreign';
    if (kind === 'wrong-fleet')
      recordAt(prepared, 'names').fleetDatabase = 'foreign';
    if (kind === 'wrong-digest') prepared.configSha256 = 'f'.repeat(64);
    const input = { ...f.input, prepared };
    await expect(
      observeDirectWorkerVersion({ ...input, ...required(f.expected[0]) }),
    ).rejects.toMatchObject(errorShape);
    await expect(
      readDirectSettlementEffects({ ...input, expected: f.expected }),
    ).rejects.toMatchObject(errorShape);
    await expect(
      verifyDirectDecommissionExport({
        ...input,
        role: 'a',
        metadata: f.metadata,
        sourceInvocationOrdinal: 1,
      }),
    ).rejects.toMatchObject(errorShape);
    expect(f.requests).toHaveLength(0);
  });

  it.each([
    'bad\ntoken',
    ' leading',
    '',
    '\u0100',
  ])('rejects invalid auth before dispatch', async (apiToken) => {
    const f = await fixture();
    await expect(
      observeDirectWorkerVersion({ ...selected(f), apiToken }),
    ).rejects.toMatchObject(errorShape);
    expect(f.requests).toHaveLength(0);
  });

  it('sanitizes SDK ambient overrides and retains caller journal ownership', async () => {
    const f = await fixture();
    process.env.CLOUDFLARE_CUSTOM_HEADERS = 'authorization: wrong';
    process.env.CLOUDFLARE_LOG = 'debug';
    process.env.CLOUDFLARE_BASE_URL = 'https://unexpected.invalid';
    const result = await observeDirectWorkerVersion(selected(f));
    expect(result.versionId).toBe('version-a');
    for (const key of [
      'CLOUDFLARE_CUSTOM_HEADERS',
      'CLOUDFLARE_LOG',
      'CLOUDFLARE_BASE_URL',
    ])
      expect(process.env[key]).toBeUndefined();
    await f.settle({ kind: 'control-read' });
  });
});

describe('fixed settlement query and ready-row correlation', () => {
  it('accepts null errors and messages on successful D1 statements', async () => {
    const f = await fixture();
    await mutateResponse(f, '/query', (body) => {
      const statement = recordAt(body, 'result.0');
      statement.errors = null;
      statement.messages = null;
    });
    const output = await readDirectSettlementEffects({
      ...f.input,
      expected: f.expected,
    });
    expect(output.map((row) => row.role)).toEqual(['a', 'b']);
    expect(f.requests).toHaveLength(3);
  });

  it.each([
    { kind: 'non-empty array', errors: [{ code: 1000, message: 'x' }] },
    { kind: 'non-array', errors: 'bad' },
  ])('refuses $kind errors on successful D1 statements', async ({ errors }) => {
    const f = await fixture();
    await mutateResponse(f, '/query', (body) => {
      recordAt(body, 'result.0').errors = errors;
    });
    await expect(
      readDirectSettlementEffects({ ...f.input, expected: f.expected }),
    ).rejects.toMatchObject({
      name: 'DirectObservationError',
      code: 'observation-mismatch',
    });
  });

  it('reads reference D1 only, binds prefix/tenants, validates hashes and returns allowlisted effects', async () => {
    const f = await fixture();
    const before = f.journal.snapshot();
    const output = await readDirectSettlementEffects({
      ...f.input,
      expected: f.expected,
    });
    expect(output.map((row) => row.role)).toEqual(['a', 'b']);
    expect(output[0]).toMatchObject({
      versionId: 'version-a',
      databaseId: 'database-a',
      settlementKey: f.effects[0]?.observation_key,
      identitySha256: f.effects[0]?.identity_sha256,
    });
    expect(Object.isFrozen(output[0])).toBe(true);
    expect(JSON.stringify(output)).not.toMatch(
      /opaque-token-sentinel|entry|identity_json|provenance_json/u,
    );
    expect(f.requests).toHaveLength(3);
    expect(
      f.requests.every(
        (request) =>
          request.method === 'POST' &&
          request.url.endsWith('/d1/database/fleet-id/query'),
      ),
    ).toBe(true);
    expect(f.journal.snapshot()).toEqual(before);
  });

  it('captures expected settlement inputs before SDK construction', async () => {
    const f = await fixture();
    const expected = f.expected.map((value) => ({ ...value }));
    const operation = readDirectSettlementEffects({ ...f.input, expected });
    required(expected[0]).versionId = 'foreign';
    expected.splice(1);
    expect((await operation).map((value) => value.versionId)).toEqual([
      'version-a',
      'version-b',
    ]);
  });

  it.each([
    'success',
    'result',
    'rows',
    'query-success',
    'query-errors',
    'query-error',
    'two-results',
    'pagination',
    'cursor',
    'count',
    'truncated',
  ])('rejects malformed query %s before SDK defaults', async (kind) => {
    const f = await fixture();
    f.hook(async (_request, fallback) => {
      const body = (await fallback().json()) as Record<string, unknown>;
      if (kind === 'success') body.success = false;
      if (kind === 'result') delete body.result;
      if (kind === 'rows') delete recordAt(body, 'result.0').results;
      if (kind === 'query-success') recordAt(body, 'result.0').success = false;
      if (kind === 'query-errors')
        recordAt(body, 'result.0').errors = ['opaque-token-sentinel'];
      if (kind === 'query-error')
        recordAt(body, 'result.0').error = 'opaque-token-sentinel';
      if (kind === 'two-results')
        (body.result as unknown[]).push(recordAt(body, 'result.0'));
      if (kind === 'pagination') body.result_info = { total_pages: 2 };
      if (kind === 'cursor') body.result_info = { cursor: 'next' };
      if (kind === 'count') body.result_info = { count: 0 };
      if (kind === 'truncated')
        return new Response('{', {
          headers: { 'content-type': 'application/json' },
        });
      return Response.json(body, {
        headers: { 'content-type': 'Application/JSON; charset=utf-8' },
      });
    });
    await expect(
      readDirectSettlementEffects({ ...f.input, expected: f.expected }),
    ).rejects.toMatchObject(errorShape);
  });

  it.each([
    'identity-hash',
    'provenance-hash',
    'key',
    'run-key',
    'kind',
    'duplicate',
    'identity-version',
    'role',
    'tenant',
    'environment',
    'target-version',
    'target-spec',
    'target-script',
    'ready-key',
    'ready-phase',
    'ready-database',
  ])('rejects %s mismatch even with valid outer envelopes', async (kind) => {
    const f = await fixture();
    f.hook(async (request, fallback) => {
      const body = (await fallback().json()) as Record<string, unknown>;
      const query = (await request.clone().json()) as { sql: string };
      if (query.sql.includes('direct_reference_observations')) {
        const row = recordAt(body, 'result.0.results.0');
        const identity = JSON.parse(String(row.identity_json));
        if (kind === 'identity-version') identity.version = 2;
        if (kind === 'role') identity.role = 'b';
        if (kind === 'tenant') identity.tenantTag = 'foreign';
        if (kind === 'environment') identity.environment = 'foreign';
        if (kind === 'target-version')
          identity.target.artifactVersion = 'foreign';
        if (kind === 'target-spec') identity.target.specDigest = 'f'.repeat(64);
        if (kind === 'target-script')
          identity.target.physicalScriptName = 'foreign';
        row.identity_json = JSON.stringify(identity);
        row.identity_sha256 = observationHash(
          JSON.stringify([
            'observation',
            f.prepared.config.resourcePrefix,
            'settlement',
            row.observation_key,
            'identity',
            row.identity_json,
          ]),
        );
        if (kind === 'identity-hash') row.identity_sha256 = 'f'.repeat(64);
        if (kind === 'provenance-hash') row.provenance_sha256 = 'f'.repeat(64);
        if (kind === 'key') row.observation_key = 'f'.repeat(64);
        if (kind === 'run-key') row.run_key = 'foreign';
        if (kind === 'kind') row.observation_kind = 'resource';
        if (kind === 'duplicate') recordAt(body, 'result.0.results')['1'] = row;
      } else {
        const row = recordAt(body, 'result.0.results.0');
        if (kind === 'ready-key') row.settled_settlement_key = 'f'.repeat(64);
        if (kind === 'ready-phase') row.phase = 'migrating';
        if (kind === 'ready-database') row.database_id = 'foreign';
      }
      return Response.json(body);
    });
    await expect(
      readDirectSettlementEffects({ ...f.input, expected: f.expected }),
    ).rejects.toMatchObject(errorShape);
  });
});

describe('normal export raw-byte proof', () => {
  it.each([
    undefined,
    'identity',
  ])('verifies a chunked identity body with content-encoding %s and no content-length', async (encoding) => {
    const f = await fixture();
    const input = await f.exportInput();
    f.hook(() => {
      const headers = new Headers({ 'transfer-encoding': 'chunked' });
      if (encoding) headers.set('content-encoding', encoding);
      return new Response(SQL_SENTINEL, { headers });
    });
    await expect(verifyDirectDecommissionExport(input)).resolves.toMatchObject({
      verified: true,
      size: input.metadata.size,
      sha256: input.metadata.sha256,
    });
    expect(f.requests[0]?.headers.get('accept-encoding')).toBe('identity');
  });

  it.each([
    {
      name: 'gzip encoding',
      headers: new Headers({ 'content-encoding': 'gzip' }),
    },
    {
      name: 'a length differing from the receipt',
      headers: new Headers({ 'content-length': '1' }),
    },
  ])('refuses $name with observation-mismatch despite exact body bytes', async ({
    headers,
  }) => {
    const f = await fixture();
    const input = await f.exportInput();
    f.hook(() => new Response(SQL_SENTINEL, { headers }));
    await expect(verifyDirectDecommissionExport(input)).rejects.toMatchObject({
      code: 'observation-mismatch',
    });
  });

  describe('releases through the body-cancel leaf', () => {
    beforeAll(() => {
      expectBuiltDist(
        '../dist/database-export-store.js',
        import.meta.url,
        'run pnpm --filter @proofoftech/fleet-control build before this block',
      );
    });

    it('releases the unread export body the refusal leaves behind', async () => {
      const f = await fixture();
      const input = await f.exportInput();
      const releases: unknown[] = [];
      f.hook(
        () =>
          new Response(
            new ReadableStream({
              start(controller) {
                controller.enqueue(Buffer.from(SQL_SENTINEL));
              },
              cancel(reason) {
                releases.push(reason);
              },
            }),
            { status: 206 },
          ),
      );
      await expect(verifyDirectDecommissionExport(input)).rejects.toMatchObject(
        errorShape,
      );
      // A release issued before the leaf's load resolves is deferred to it.
      await vi.waitFor(() => {
        expect(releases).toHaveLength(1);
      });
      expect(f.requests).toHaveLength(1);
    });

    it('hands the refusal to the source as the export body cancel reason', async () => {
      const f = await fixture();
      const input = await f.exportInput();
      const releases: unknown[] = [];
      f.hook(
        () =>
          new Response(
            new ReadableStream({
              start(controller) {
                controller.enqueue(Buffer.from(SQL_SENTINEL));
              },
              cancel(reason) {
                releases.push(reason);
              },
            }),
            { headers: { 'content-length': '1' } },
          ),
      );
      await expect(verifyDirectDecommissionExport(input)).rejects.toMatchObject(
        {
          code: 'observation-mismatch',
        },
      );
      await vi.waitFor(() => {
        expect(releases.at(-1)).toEqual(
          expect.objectContaining({
            name: 'DirectObservationError',
            message: 'observation-mismatch',
          }),
        );
      });
    });
  });

  it('derives exact R2 key and returns frozen receipt with source ordinal', async () => {
    const f = await fixture();
    const input = await f.exportInput();
    const before = f.journal.snapshot();
    const output = await verifyDirectDecommissionExport(input);
    expect(output).toEqual({
      verified: true,
      role: 'a',
      receipt: input.metadata.receipt,
      location: input.metadata.location,
      size: Buffer.byteLength(SQL_SENTINEL),
      sha256: observationHash(SQL_SENTINEL),
      sourceInvocationOrdinal: input.sourceInvocationOrdinal,
    });
    expect(Object.isFrozen(output.receipt)).toBe(true);
    expect(JSON.stringify(output)).not.toContain(SQL_SENTINEL);
    expect(f.requests[0]?.headers.get('cf-r2-jurisdiction')).toBe('default');
    expect(f.requests[0]?.headers.get('accept')).toBe(
      'application/octet-stream',
    );
    expect(f.journal.snapshot()).toEqual(before);
  });

  it('hashes raw binary streams larger than the JSON cap without buffering', async () => {
    const f = await fixture();
    const input = await f.exportInput();
    const chunk = Buffer.alloc(64 * 1024, 255);
    const count = 145;
    const { createHash } = await import('node:crypto');
    const hash = createHash('sha256');
    for (let i = 0; i < count; i++) hash.update(chunk);
    input.metadata = {
      ...input.metadata,
      size: chunk.length * count,
      sha256: hash.digest('hex'),
    };
    let pulled = 0;
    f.hook(
      () =>
        new Response(
          new ReadableStream({
            pull(controller) {
              if (pulled++ === count) controller.close();
              else controller.enqueue(chunk);
            },
          }),
        ),
    );
    const output = await verifyDirectDecommissionExport(input);
    expect(output.size).toBeGreaterThan(8 * 1024 * 1024);
    expect(output.sha256).toBe(input.metadata.sha256);
    expect(pulled).toBe(count + 1);
  });

  it('captures metadata, role and ordinal before awaiting SDK work', async () => {
    const f = await fixture();
    const input = await f.exportInput();
    const operation = verifyDirectDecommissionExport(input);
    input.metadata = {
      ...input.metadata,
      receipt: { ...input.metadata.receipt, databaseId: 'foreign' },
    };
    input.sourceInvocationOrdinal = 100;
    expect((await operation).receipt.databaseId).toBe('database-a');
  });

  it.each([
    'unavailable',
    'role',
    'ordinal',
    'action',
    'authority',
    'location',
    'database-path',
    'device-id',
    'operation-path',
    'size-zero',
    'size-unsafe',
    'uppercase-hash',
    'phase',
    'revision',
    'generation',
  ])('refuses %s metadata before GET', async (kind) => {
    const f = await fixture();
    const input = await f.exportInput();
    const m = structuredClone(input.metadata) as unknown as Record<
      string,
      unknown
    >;
    if (kind === 'unavailable') m.available = false;
    if (kind === 'role') m.role = 'b';
    if (kind === 'ordinal') input.sourceInvocationOrdinal--;
    if (kind === 'action') await f.settle({ kind: 'control-read' });
    if (kind === 'authority')
      recordAt(m, 'receipt').authority = 'r2://foreign/receipts/v1';
    if (kind === 'location') m.location += '?signature=opaque-token-sentinel';
    if (kind === 'database-path')
      recordAt(m, 'receipt').databaseId = '../foreign';
    if (kind === 'device-id') recordAt(m, 'receipt').databaseId = 'CON';
    if (kind === 'operation-path') recordAt(m, 'receipt').operationId = 'a/b';
    if (kind === 'size-zero') m.size = 0;
    if (kind === 'size-unsafe') m.size = Number.MAX_SAFE_INTEGER + 1;
    if (kind === 'uppercase-hash') m.sha256 = String(m.sha256).toUpperCase();
    if (kind === 'phase') m.lifecyclePhase = 'ready';
    if (kind === 'revision') m.revision = -1;
    if (kind === 'generation') m.generation = 0.5;
    await expect(
      verifyDirectDecommissionExport({
        ...input,
        metadata: m as unknown as typeof input.metadata,
      }),
    ).rejects.toMatchObject(errorShape);
    expect(f.requests).toHaveLength(0);
  });

  it.each([
    'partial',
    'content-range',
    'redirect',
    'missing',
    'no-body',
    'short',
    'long',
    'hash',
    'understated-length',
    'wrong-length',
    'encoded',
    'body-error',
  ])('rejects %s export without leaking raw content', async (kind) => {
    const f = await fixture();
    const input = await f.exportInput();
    f.hook(() => {
      if (kind === 'partial')
        return new Response(SQL_SENTINEL, { status: 206 });
      if (kind === 'content-range')
        return new Response(SQL_SENTINEL, {
          headers: { 'content-range': 'bytes 0-25/26' },
        });
      if (kind === 'redirect')
        return new Response(null, {
          status: 302,
          headers: { location: 'https://unexpected.invalid' },
        });
      if (kind === 'missing')
        return Response.json(
          { errors: [{ message: SQL_SENTINEL + OBSERVATION_TOKEN }] },
          { status: 404 },
        );
      if (kind === 'no-body') return new Response(null);
      if (kind === 'short') return new Response(SQL_SENTINEL.slice(1));
      if (kind === 'long') return new Response(`${SQL_SENTINEL}x`);
      if (kind === 'hash') return new Response('x'.repeat(SQL_SENTINEL.length));
      if (kind === 'understated-length')
        return new Response(`${SQL_SENTINEL}x`, {
          headers: { 'content-length': String(input.metadata.size) },
        });
      if (kind === 'wrong-length')
        return new Response(SQL_SENTINEL, {
          headers: { 'content-length': '1' },
        });
      if (kind === 'encoded')
        return new Response(SQL_SENTINEL, {
          headers: { 'content-encoding': 'gzip' },
        });
      return new Response(
        new ReadableStream({
          start(controller) {
            controller.error(new Error(SQL_SENTINEL + OBSERVATION_TOKEN));
          },
        }),
      );
    });
    const error = await verifyDirectDecommissionExport(input).catch(
      (failure: unknown) => failure,
    );
    expect(error).toMatchObject(errorShape);
    expect(String(error)).not.toMatch(/private_sql_sentinel|provider-token/u);
    expect(error).not.toHaveProperty('cause');
    expect(f.requests).toHaveLength(1);
  });

  it.each([
    'headers',
    'body',
    'eof',
    'reject-cancel',
    'hang-cancel',
  ])('bounds %s with the same request deadline', async (kind) => {
    const f = await fixture(30);
    const input = await f.exportInput();
    const cancellations = vi.fn();
    f.hook(() => {
      if (kind === 'headers') return new Promise<Response>(() => {});
      return new Response(
        new ReadableStream({
          start(controller) {
            if (kind === 'eof') controller.enqueue(Buffer.from(SQL_SENTINEL));
          },
          cancel() {
            cancellations();
            if (kind === 'reject-cancel')
              return Promise.reject(new Error('cancellation sentinel'));
            if (kind === 'hang-cancel') return new Promise<void>(() => {});
          },
        }),
      );
    });
    await expect(verifyDirectDecommissionExport(input)).rejects.toMatchObject(
      errorShape,
    );
    if (kind !== 'headers') expect(cancellations).toHaveBeenCalled();
    expect(f.requests).toHaveLength(1);
  });

  it('shares one deadline between headers and body rather than resetting at headers', async () => {
    const f = await fixture(80);
    const input = await f.exportInput();
    let bodyTimer: ReturnType<typeof setTimeout>;
    f.hook(async () => {
      await new Promise((resolve) => setTimeout(resolve, 50));
      return new Response(
        new ReadableStream({
          start(controller) {
            bodyTimer = setTimeout(() => {
              controller.enqueue(Buffer.from(SQL_SENTINEL));
              controller.close();
            }, 50);
          },
          cancel() {
            clearTimeout(bodyTimer);
          },
        }),
      );
    });
    await expect(verifyDirectDecommissionExport(input)).rejects.toMatchObject(
      errorShape,
    );
  });

  it('bounds oversized JSON error bodies and handles late responses after timeout', async () => {
    const f = await fixture(30);
    const input = await f.exportInput();
    f.hook(
      () =>
        new Response('x'.repeat(8 * 1024 * 1024 + 1), {
          status: 500,
          headers: { 'content-type': 'application/json' },
        }),
    );
    await expect(verifyDirectDecommissionExport(input)).rejects.toMatchObject(
      errorShape,
    );
    let deliver!: (response: Response) => void;
    f.hook(
      () =>
        new Promise<Response>((resolve) => {
          deliver = resolve;
        }),
    );
    await expect(verifyDirectDecommissionExport(input)).rejects.toMatchObject(
      errorShape,
    );
    const cancelled = vi.fn(() => Promise.reject(new Error('cancel')));
    deliver(new Response(new ReadableStream({ cancel: cancelled })));
    await vi.waitFor(() => expect(cancelled).toHaveBeenCalled());
  });
});

it('executes the same synthetic fixture natively outside Vitest', async () => {
  const fixtureUrl = new URL(
    './fixtures/direct-observations.ts',
    import.meta.url,
  ).href;
  const moduleUrl = new URL(
    '../scripts/direct-credentialed-observations.mjs',
    import.meta.url,
  ).href;
  const source = `import assert from 'node:assert/strict';
    globalThis.fetch = () => { throw Error('unexpected network'); };
    const { directObservationFixture } = await import(${JSON.stringify(fixtureUrl)});
    const { observeDirectWorkerVersion, readDirectSettlementEffects, verifyDirectDecommissionExport } = await import(${JSON.stringify(moduleUrl)});
    const f = await directObservationFixture();
    try {
      process.env.CLOUDFLARE_LOG = 'debug';
      process.env.CLOUDFLARE_BASE_URL = 'https://unexpected.invalid';
      assert.equal((await observeDirectWorkerVersion({...f.input, ...f.expected[0]})).trafficPercentage, 100);
      assert.equal((await readDirectSettlementEffects({...f.input, expected:f.expected})).length, 2);
      assert.equal((await verifyDirectDecommissionExport(await f.exportInput())).verified, true);
      assert.deepEqual(f.unexpected, []);
      console.log('native-observations-ok');
    } finally { await f.close(); }`;
  const output = await promisify(execFile)(
    process.execPath,
    ['--input-type=module', '-e', source],
    { timeout: 15_000 },
  );
  expect(output.stdout.trim()).toBe('native-observations-ok');
  expect(output.stderr).not.toMatch(
    /provider-token|private_sql_sentinel|CLOUDFLARE/u,
  );
});
