// SPDX-License-Identifier: Apache-2.0

import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  mkdir,
  mkdtemp,
  readFile,
  rm,
  stat,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { gzipSync } from 'node:zlib';
import type { D1Database } from '@cloudflare/workers-types';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { createTestHarness, type TestHarness } from 'wrangler';
import {
  type BuiltDirectConformanceArtifacts,
  buildDirectConformanceArtifacts,
} from '../scripts/direct-credentialed-artifacts.mjs';
import { preflightDirectConformance } from '../scripts/direct-credentialed-conformance-preflight.mjs';
import { DIRECT_REFERENCE_PATH } from '../scripts/direct-reference-contract.mjs';

const NOW = Date.parse('2026-09-10T12:00:00Z');

vi.mock('node:child_process', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:child_process')>();
  return { ...actual, spawn: vi.fn(actual.spawn) };
});

describe.sequential('default direct conformance artifacts', {
  timeout: 180_000,
}, () => {
  let directory: string;
  let configPath: string;
  let built: BuiltDirectConformanceArtifacts;
  let server: TestHarness | undefined;

  beforeAll(async () => {
    vi.stubEnv('CLOUDFLARE_API_TOKEN', 'artifact-child-env-sentinel');
    vi.stubEnv('CLOUDFLARE_BASE_URL', 'https://unexpected.example.test');
    directory = await mkdtemp(join(tmpdir(), 'direct-built-artifacts-'));
    configPath = join(directory, 'input.json');
    await writeFile(
      configPath,
      await readFile(
        new URL(
          '../scripts/direct-credentialed-conformance.example.json',
          import.meta.url,
        ),
      ),
    );
    built = await buildDirectConformanceArtifacts({
      configPath,
      outputDirectory: join(directory, 'built'),
      now: NOW,
    });
  }, 180_000);

  afterAll(async () => {
    try {
      await server?.close();
    } finally {
      if (directory) await rm(directory, { recursive: true, force: true });
      vi.unstubAllEnvs();
    }
  }, 30_000);

  it('preserves runtime intent and emits exact preflight snapshots from the fixed sources', async () => {
    const input = JSON.parse(await readFile(configPath, 'utf8'));
    const output = JSON.parse(await readFile(built.configPath, 'utf8'));
    for (const role of ['referenceWorker', 'deployment']) {
      const { artifact: _before, ...before } = input[role];
      const { artifact: _after, ...after } = output[role];
      expect(after).toEqual(before);
    }
    expect(
      await preflightDirectConformance({
        configPath: built.configPath,
        now: NOW,
      }),
    ).toEqual(built.prepared);
    expect(
      built.prepared.referenceModules.map((module) => module.name),
    ).toEqual(['worker.js', 'direct-run-manifest.js']);
    expect(built.prepared.manifest.tenantWasm.length).toBeGreaterThan(0);
    for (const role of ['reference', 'tenant'] as const) {
      const bytes = await readFile(
        join(directory, 'built', role, 'out', `${role}.js`),
      );
      expect(built.builds[role]).toMatchObject({
        rawBytes: bytes.length,
        gzipBytes: gzipSync(bytes).length,
        sha256: createHash('sha256').update(bytes).digest('hex'),
      });
      const command = JSON.parse(
        await readFile(join(directory, 'built', role, 'command.json'), 'utf8'),
      );
      expect(command.status).toBe(0);
      expect(command.args).toContain('--dry-run');
      expect(command.environmentKeys).toEqual([
        'PATH',
        'WRANGLER_SEND_METRICS',
      ]);
      const call = vi
        .mocked(spawn)
        .mock.calls.find(
          ([, args]) =>
            Array.isArray(args) &&
            args.includes(join(directory, 'built', role, 'wrangler.json')),
        );
      expect(call).toBeDefined();
      expect(call?.[2]?.env).toEqual({
        PATH: process.env.PATH ?? '',
        WRANGLER_SEND_METRICS: 'false',
      });
      expect(command.environmentKeys).toEqual(
        Object.keys(call?.[2]?.env ?? {}),
      );
      expect(
        (await stat(join(directory, 'built', role, 'out', `${role}.js`))).mode &
          0o777,
      ).toBe(0o600);
    }
    expect((await stat(join(directory, 'built'))).mode & 0o777).toBe(0o700);
    expect((await stat(built.configPath)).mode & 0o777).toBe(0o600);
    expect(
      await readFile(join(directory, 'built', 'input-conformance.json')),
    ).toEqual(await readFile(configPath));
  });

  it('refuses an existing output directory without changing its contents', async () => {
    const snapshot = await readFile(built.configPath);
    await expect(
      buildDirectConformanceArtifacts({
        configPath,
        outputDirectory: join(directory, 'built'),
        now: NOW,
      }),
    ).rejects.toThrow();
    expect(await readFile(built.configPath)).toEqual(snapshot);
  });

  it('rejects invalid operator intent before creating build output', async () => {
    const invalidPath = join(directory, 'invalid.json');
    const input = JSON.parse(await readFile(configPath, 'utf8'));
    input.providerUrl = 'https://unexpected.example.test';
    await writeFile(invalidPath, JSON.stringify(input));
    const outputDirectory = join(directory, 'invalid-output');
    await expect(
      buildDirectConformanceArtifacts({
        configPath: invalidPath,
        outputDirectory,
        now: NOW,
      }),
    ).rejects.toThrow();
    await expect(stat(outputDirectory)).rejects.toMatchObject({
      code: 'ENOENT',
    });
  });

  it('loads the exact reference upload with native state and the configured runtime flags', async () => {
    const prepared = built.prepared;
    const runtime = join(directory, 'native-reference');
    await mkdir(runtime);
    for (const module of prepared.referenceModules) {
      const bytes =
        'source' in module
          ? Buffer.from(module.source)
          : Buffer.from(module.base64, 'base64');
      expect(createHash('sha256').update(bytes).digest('hex')).toBe(
        module.sha256,
      );
      await writeFile(join(runtime, module.name), bytes);
    }
    const binding = {
      version: 1,
      accountId: 'account',
      fleetDatabaseId: '00000000-0000-0000-0000-000000000011',
      quotaDatabaseId: '00000000-0000-0000-0000-000000000012',
      exportBucketName: prepared.names.exportBucket,
      referenceModuleSetSha256: prepared.referenceModuleSetSha256,
      accountWorkersDevSubdomain: 'artifact-fixture',
    };
    const secretMap = Object.fromEntries(
      ['a', 'b', 'recovery'].map((role) => [
        role,
        {
          deploymentIdentity: `identity-${role}`.padEnd(40, 'i'),
          maintenanceAdmin: `maintenance-${role}`.padEnd(40, 'm'),
          application: { APP_PROBE_TOKEN: `probe-${role}`.padEnd(40, 'p') },
        },
      ]),
    );
    server = createTestHarness({
      root: fileURLToPath(new URL('..', import.meta.url)),
      workers: [
        {
          config: {
            name: 'direct-built-reference',
            main: join(runtime, prepared.referenceModules[0].name),
            no_bundle: true,
            find_additional_modules: true,
            rules: [
              { type: 'ESModule', globs: ['**/*.js'], fallthrough: true },
              { type: 'CompiledWasm', globs: ['**/*.wasm'], fallthrough: true },
            ],
            compatibility_date:
              prepared.config.referenceWorker.compatibilityDate,
            compatibility_flags: [
              ...prepared.config.referenceWorker.compatibilityFlags,
            ],
            vars: {
              CLOUDFLARE_API_TOKEN: 'inert-provider-token',
              FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET: 'inert-invoke',
              DIRECT_RUN_BINDING: JSON.stringify(binding),
              DIRECT_DEPLOYMENT_SECRETS: JSON.stringify(secretMap),
            },
            d1_databases: [
              {
                binding: 'FLEET_DB',
                database_name: prepared.names.fleetDatabase,
                database_id: binding.fleetDatabaseId,
              },
              {
                binding: 'QUOTA_DB',
                database_name: prepared.names.quotaDatabase,
                database_id: binding.quotaDatabaseId,
              },
            ],
            r2_buckets: [
              { binding: 'EXPORTS', bucket_name: binding.exportBucketName },
            ],
          },
        },
      ],
    });
    await server.listen();
    const worker = server.getWorker<{ FLEET_DB: D1Database }>();
    const url = `https://reference.example.test${DIRECT_REFERENCE_PATH}`;
    const body = JSON.stringify({
      contractVersion: 1,
      configSha256: prepared.configSha256,
      action: { kind: 'control-read' },
    });
    expect((await worker.fetch(url, { method: 'POST', body })).status).toBe(
      401,
    );
    const response = await worker.fetch(url, {
      method: 'POST',
      headers: { authorization: 'Bearer inert-invoke' },
      body,
    });
    expect(response.status).toBe(200);
    expect(response.headers.get('X-Direct-Provider-Attempts')).toBe('0');
    expect(await response.json()).toMatchObject({
      ok: true,
      configSha256: prepared.configSha256,
      result: {
        binding,
        operations: [],
        records: [
          { role: 'a', present: false },
          { role: 'b', present: false },
          { role: 'recovery', present: false },
        ],
      },
    });
    const env = await worker.getEnv();
    expect(
      await env.FLEET_DB.prepare(
        'SELECT COUNT(*) AS count FROM anchorage_fleet_deployments',
      ).first('count'),
    ).toBe(0);
    await server.close();
    server = undefined;
    for (const module of prepared.referenceModules) {
      expect(
        createHash('sha256')
          .update(await readFile(join(runtime, module.name)))
          .digest('hex'),
      ).toBe(module.sha256);
    }
  });
});
