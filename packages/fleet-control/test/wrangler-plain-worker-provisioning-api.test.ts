// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { mkdtemp, readFile, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';
import type { DurableDatabaseExportStore } from '../src/cloudflare-client.js';
import {
  assertSupportedPlainWorkerBindings,
  plainWorkerBindingsToProviderShape,
} from '../src/provider-binding-inventory.js';
import type {
  ExternalMutationFence,
  PlainWorkerRouteApi,
  PlainWorkerUploadIntent,
} from '../src/types.js';
import { WranglerPlainWorkerProvisioningApi } from '../src/wrangler-plain-worker-provisioning-api.js';
import type { CommandResult, CommandRunner } from '../src/wrangler-runner.js';
import {
  drain,
  memoryStore,
  mutationFence,
  routeApi,
} from './fixtures/plain-worker-port-probe.js';
import {
  type PlainWorkerFsControl,
  registerScratchCleanup,
} from './fixtures/wrangler-fs-mock.js';

const fsControl = vi.hoisted<PlainWorkerFsControl>(() => ({
  failFleetCleanup: false,
  residualDirectory: undefined,
  cleanupError: new Error('scratch cleanup failed'),
  failOperation: undefined,
  operationError: new Error('filesystem operation failed'),
  scratchDirectories: [],
}));

function scratchDirectories(): string[] {
  const directories = fsControl.scratchDirectories;
  if (!directories) {
    throw new Error(
      'adapter test filesystem control requires scratch tracking',
    );
  }
  return directories;
}

vi.mock('node:fs/promises', async () => {
  const { createFsPromisesMock } = await import(
    './fixtures/wrangler-fs-mock.js'
  );
  return createFsPromisesMock(fsControl);
});

const exportDirectories = registerScratchCleanup(fsControl, {
  cleanupError: fsControl.cleanupError,
  operationError: fsControl.operationError,
});

interface RunnerCall {
  readonly arguments: readonly string[];
}

class FakeRunner implements CommandRunner {
  readonly maxDurationMs = 5 * 60_000;
  readonly calls: RunnerCall[] = [];

  constructor(
    readonly handler: (
      arguments_: readonly string[],
    ) => Promise<CommandResult> = async () => ({ stdout: '', stderr: '' }),
  ) {}

  run(arguments_: readonly string[]): Promise<CommandResult> {
    this.calls.push({ arguments: [...arguments_] });
    return this.handler(arguments_);
  }
}

async function api(
  runner: CommandRunner,
  options: {
    readonly routeApi?: PlainWorkerRouteApi;
    readonly exportStore?: DurableDatabaseExportStore;
    readonly exportDirectory?: string;
  } = {},
): Promise<WranglerPlainWorkerProvisioningApi> {
  const exportDirectory =
    options.exportDirectory ??
    (await mkdtemp(join(tmpdir(), 'adapter-export-')));
  if (!options.exportDirectory) exportDirectories.add(exportDirectory);
  return new WranglerPlainWorkerProvisioningApi({
    runner,
    routeApi: options.routeApi ?? routeApi(),
    exportDirectory,
    exportStore: options.exportStore ?? memoryStore(),
  });
}

function uploadIntent(mode: 'initial' | 'staged'): PlainWorkerUploadIntent {
  const shared = {
    scriptName: 'worker-name',
    candidateTag: 'candidate-tag',
    mainModule: 'worker.js',
    modules: [{ name: 'worker.js', content: 'export default {}' }],
    compatibilityDate: '2026-08-10',
    compatibilityFlags: undefined,
    bindings: {
      plainText: [{ name: 'TEXT', value: 'value' }],
      secrets: [{ name: 'SECRET', value: 'secret' }],
      d1: [{ name: 'DB', databaseId: 'db-id', databaseName: 'db-name' }],
      durableObjects: [{ name: 'OBJECT', className: 'ObjectClass' }],
      services: [],
      queueProducers: [],
      r2Buckets: [{ name: 'BUCKET', bucketName: 'bucket-name' }],
    },
    limits: { cpuMs: 25 },
    publicAccess: { workersDevEnabled: true, previewUrlsEnabled: false },
  } as const;
  return mode === 'initial'
    ? {
        ...shared,
        mode,
        durableObjectMigrations: [
          {
            tag: 'v1',
            newSqliteClasses: ['ObjectClass'],
            newClasses: [],
            deletedClasses: [],
            renamedClasses: [],
          },
        ],
      }
    : { ...shared, mode };
}

async function expectUploadScratchRemoved(): Promise<void> {
  const actual =
    await vi.importActual<typeof import('node:fs/promises')>(
      'node:fs/promises',
    );
  expect(scratchDirectories().length).toBeGreaterThan(0);
  for (const directory of scratchDirectories()) {
    await expect(actual.stat(directory)).rejects.toThrow();
  }
}

async function expectExportScratchRemoved(outputPath: string): Promise<void> {
  const actual =
    await vi.importActual<typeof import('node:fs/promises')>(
      'node:fs/promises',
    );
  await expect(actual.stat(dirname(outputPath))).rejects.toThrow();
}

describe('WranglerPlainWorkerProvisioningApi parsing', () => {
  it.each([
    ['array', '[{"uuid":"db-1","name":"one"}]'],
    ['wrapped array', '{"result":[{"uuid":"db-1","name":"one"}]}'],
    ['wrapped object', '{"result":{"uuid":"db-1","name":"one"}}'],
  ])('parses %s JSON results', async (_name, stdout) => {
    const subject = await api(
      new FakeRunner(async () => ({ stdout, stderr: '' })),
    );
    await expect(subject.listDatabases()).resolves.toEqual([
      { databaseId: 'db-1', name: 'one' },
    ]);
  });

  it('rejects invalid JSON with the operation name', async () => {
    const subject = await api(
      new FakeRunner(async () => ({ stdout: '{', stderr: '' })),
    );
    await expect(subject.listDatabases()).rejects.toThrow(
      'wrangler d1 list returned invalid JSON',
    );
  });

  it('normalizes every binding branch and preserves D1 id precedence', async () => {
    const bindings = [
      { type: 'd1', name: 'DB', id: '', database_id: 'db-alias' },
      {
        type: 'durable_object_namespace',
        name: 'OBJECT',
        class_name: 'ObjectClass',
        namespace_id: 'namespace',
      },
      { type: 'service', name: 'SERVICE', service: 'upstream' },
      { type: 'queue', name: 'QUEUE', queue_name: 'queue-name' },
      { type: 'r2_bucket', name: 'BUCKET', bucket_name: 'bucket-name' },
      { type: 'plain_text', name: 'TEXT', text: 'value' },
      { type: 'secret_text', name: 'SECRET' },
      { type: 'kv_namespace', name: 'KV' },
      { type: ' ', name: 'INVALID' },
      null,
    ];
    const subject = await api(
      new FakeRunner(async () => ({
        stdout: JSON.stringify({ resources: { bindings } }),
        stderr: '',
      })),
    );
    const viewed = await subject.viewVersion('worker', 'requested');
    expect(viewed).toEqual({
      versionId: undefined,
      tag: undefined,
      bindings: [
        { type: 'd1', name: 'DB', databaseId: '' },
        {
          type: 'durable-object',
          name: 'OBJECT',
          className: 'ObjectClass',
          namespaceId: 'namespace',
        },
        { type: 'service', name: 'SERVICE', service: 'upstream' },
        { type: 'queue-producer', name: 'QUEUE', queueName: 'queue-name' },
        { type: 'r2-bucket', name: 'BUCKET', bucketName: 'bucket-name' },
        { type: 'plain-text', name: 'TEXT', value: 'value' },
        { type: 'secret-text', name: 'SECRET' },
        {
          type: 'unsupported',
          name: 'KV',
          providerType: 'kv_namespace',
          issue: 'unsupported-type',
        },
        {
          type: 'unsupported',
          name: 'INVALID',
          providerType: ' ',
          issue: 'invalid-type',
        },
        {
          type: 'unsupported',
          name: undefined,
          issue: 'not-object',
        },
      ],
    });
    expect(() =>
      assertSupportedPlainWorkerBindings(
        viewed.bindings.slice(0, 1),
        "plain Worker 'worker'",
      ),
    ).toThrow(
      "plain Worker 'worker' has an unsupported or malformed provider binding",
    );
  });

  it('normalizes wrapped binding inventories like direct arrays', async () => {
    const bindings = [
      { type: 'd1', name: 'DB', database_id: 'db-id' },
      { type: 'plain_text', name: 'TEXT', text: 'value' },
    ];
    const subject = await api(
      new FakeRunner(async () => ({
        stdout: JSON.stringify({
          resources: { bindings: { result: bindings } },
        }),
        stderr: '',
      })),
    );

    await expect(
      subject.viewVersion('worker', 'version'),
    ).resolves.toMatchObject({
      bindings: [
        { type: 'd1', name: 'DB', databaseId: 'db-id' },
        { type: 'plain-text', name: 'TEXT', value: 'value' },
      ],
    });
  });

  it.each([
    [
      'D1 id',
      { type: 'd1', name: 'DB', id: 'db-id' },
      { type: 'd1', name: 'DB', id: 'db-id' },
    ],
    [
      'D1 database_id',
      { type: 'd1', name: 'DB', database_id: 'db-id' },
      { type: 'd1', name: 'DB', id: 'db-id' },
    ],
    [
      'Durable Object',
      {
        type: 'durable_object_namespace',
        name: 'OBJECT',
        namespace_id: 'namespace-id',
        class_name: 'ObjectClass',
      },
      {
        type: 'durable_object_namespace',
        name: 'OBJECT',
        namespace_id: 'namespace-id',
        class_name: 'ObjectClass',
      },
    ],
    [
      'service',
      { type: 'service', name: 'SERVICE', service: 'upstream' },
      { type: 'service', name: 'SERVICE', service: 'upstream' },
    ],
    [
      'queue producer',
      { type: 'queue', name: 'QUEUE', queue_name: 'queue-name' },
      { type: 'queue', name: 'QUEUE', queue_name: 'queue-name' },
    ],
    [
      'R2 bucket',
      { type: 'r2_bucket', name: 'BUCKET', bucket_name: 'bucket-name' },
      { type: 'r2_bucket', name: 'BUCKET', bucket_name: 'bucket-name' },
    ],
    [
      'plain text',
      { type: 'plain_text', name: 'TEXT', text: 'value' },
      { type: 'plain_text', name: 'TEXT', text: 'value' },
    ],
    [
      'secret text',
      { type: 'secret_text', name: 'SECRET' },
      { type: 'secret_text', name: 'SECRET' },
    ],
  ] as const)('round-trips the exact valid %s wire object through inventory reconstruction', async (_title, binding, reconstructedBinding) => {
    const subject = await api(
      new FakeRunner(async () => ({
        stdout: JSON.stringify({ resources: { bindings: [binding] } }),
        stderr: '',
      })),
    );
    const viewed = await subject.viewVersion('worker', 'version');
    expect(plainWorkerBindingsToProviderShape(viewed.bindings)).toEqual([
      reconstructedBinding,
    ]);
    expect(
      assertSupportedPlainWorkerBindings(viewed.bindings, 'version'),
    ).toEqual([{ type: reconstructedBinding.type, name: binding.name }]);
  });

  it('reconstructs the exact unsupported provider wire objects', () => {
    expect(
      plainWorkerBindingsToProviderShape([
        { type: 'unsupported', name: undefined, issue: 'not-object' },
        {
          type: 'unsupported',
          name: 'INVALID_STRING',
          providerType: ' ',
          issue: 'invalid-type',
        },
        {
          type: 'unsupported',
          name: 'INVALID_NON_STRING',
          providerType: undefined,
          issue: 'invalid-type',
        },
        {
          type: 'unsupported',
          name: 'KV',
          providerType: 'kv_namespace',
          issue: 'unsupported-type',
        },
      ]),
    ).toStrictEqual([
      undefined,
      { type: ' ', name: 'INVALID_STRING' },
      { type: undefined, name: 'INVALID_NON_STRING' },
      { type: 'kv_namespace', name: 'KV' },
    ]);
  });

  it.each([
    ['D1', { type: 'd1', name: ' ', id: 'db-id' }],
    [
      'Durable Object',
      {
        type: 'durable_object_namespace',
        name: ' ',
        namespace_id: 'namespace',
        class_name: 'ObjectClass',
      },
    ],
    ['service', { type: 'service', name: ' ', service: 'upstream' }],
    ['queue', { type: 'queue', name: ' ', queue_name: 'queue' }],
    ['R2', { type: 'r2_bucket', name: ' ', bucket_name: 'bucket' }],
    ['plain text', { type: 'plain_text', name: ' ', text: '' }],
    ['secret text', { type: 'secret_text', name: ' ' }],
  ] as const)('reports the exact index for a blank %s binding name', async (_title, binding) => {
    const subject = await api(
      new FakeRunner(async () => ({
        stdout: JSON.stringify({ resources: { bindings: [binding] } }),
        stderr: '',
      })),
    );
    const viewed = await subject.viewVersion('worker', 'version');
    expect(() =>
      assertSupportedPlainWorkerBindings(viewed.bindings, 'version'),
    ).toThrow('version binding 0 has no valid name');
  });

  it.each([
    ['D1 missing id', { type: 'd1', name: 'DB' }],
    ['D1 blank id', { type: 'd1', name: 'DB', id: ' ' }],
    [
      'Durable Object missing namespace',
      {
        type: 'durable_object_namespace',
        name: 'OBJECT',
        class_name: 'ObjectClass',
      },
    ],
    [
      'Durable Object blank class',
      {
        type: 'durable_object_namespace',
        name: 'OBJECT',
        namespace_id: 'namespace',
        class_name: ' ',
      },
    ],
    ['service missing target', { type: 'service', name: 'SERVICE' }],
    [
      'service blank target',
      { type: 'service', name: 'SERVICE', service: ' ' },
    ],
    ['queue missing target', { type: 'queue', name: 'QUEUE' }],
    ['queue blank target', { type: 'queue', name: 'QUEUE', queue_name: ' ' }],
    ['R2 missing bucket', { type: 'r2_bucket', name: 'BUCKET' }],
    [
      'R2 blank bucket',
      { type: 'r2_bucket', name: 'BUCKET', bucket_name: ' ' },
    ],
    ['plain text missing value', { type: 'plain_text', name: 'TEXT' }],
  ] as const)('refuses malformed %s with the exact inventory message', async (_title, binding) => {
    const subject = await api(
      new FakeRunner(async () => ({
        stdout: JSON.stringify({ resources: { bindings: [binding] } }),
        stderr: '',
      })),
    );
    const viewed = await subject.viewVersion('worker', 'version');
    expect(() =>
      assertSupportedPlainWorkerBindings(viewed.bindings, 'version'),
    ).toThrow('version has an unsupported or malformed provider binding');
  });

  it('exposes the raw provider type on the unsupported binding fact', async () => {
    const subject = await api(
      new FakeRunner(async () => ({
        stdout: JSON.stringify({
          resources: { bindings: [{ type: 'kv_namespace', name: 'KV' }] },
        }),
        stderr: '',
      })),
    );
    const viewed = await subject.viewVersion('worker', 'version');
    expect(viewed.bindings).toEqual([
      {
        type: 'unsupported',
        name: 'KV',
        providerType: 'kv_namespace',
        issue: 'unsupported-type',
      },
    ]);
    expect(() =>
      assertSupportedPlainWorkerBindings(viewed.bindings, 'version'),
    ).toThrow('version has an unsupported or malformed provider binding');
  });

  it.each([
    [
      'invalid string type',
      { type: ' ', name: 'INVALID' },
      {
        type: 'unsupported',
        name: 'INVALID',
        providerType: ' ',
        issue: 'invalid-type',
      },
    ],
    [
      'invalid non-string type',
      { type: 42, name: 'INVALID' },
      {
        type: 'unsupported',
        name: 'INVALID',
        providerType: undefined,
        issue: 'invalid-type',
      },
    ],
  ] as const)('refuses the %s binding with the exact inventory message', async (_title, binding, expectedBinding) => {
    const subject = await api(
      new FakeRunner(async () => ({
        stdout: JSON.stringify({ resources: { bindings: [binding] } }),
        stderr: '',
      })),
    );
    const viewed = await subject.viewVersion('worker', 'version');
    expect(viewed.bindings[0]).toStrictEqual(expectedBinding);
    expect(() =>
      assertSupportedPlainWorkerBindings(viewed.bindings, 'version'),
    ).toThrowError(new Error('version binding 0 has no valid type'));
  });

  it('reports binding indexes and duplicate names through the shared inventory', () => {
    expect(() =>
      assertSupportedPlainWorkerBindings(
        [
          {
            type: 'unsupported',
            name: undefined,
            issue: 'not-object',
          },
        ],
        'version',
      ),
    ).toThrow('version binding 0 is not an object');
    expect(() =>
      assertSupportedPlainWorkerBindings(
        [
          { type: 'secret-text', name: 'DUPLICATE' },
          { type: 'plain-text', name: 'DUPLICATE', value: 'value' },
        ],
        'version',
      ),
    ).toThrow('version has duplicate provider binding names');
  });

  it('uses route D1 reads, falls back to Wrangler, and classifies absence only', async () => {
    const routeRead = vi.fn(async () => ({
      id: 'db',
      name: 'route',
      created: false,
    }));
    const routeSubject = await api(new FakeRunner(), {
      routeApi: routeApi({ getDatabase: routeRead }),
    });
    await expect(routeSubject.getDatabase('db')).resolves.toEqual({
      id: 'db',
      name: 'route',
      created: false,
    });
    expect(routeRead).toHaveBeenCalledWith('db');

    const success = await api(
      new FakeRunner(async () => ({
        stdout: '{"result":{"uuid":"db","name":"fallback"}}',
        stderr: '',
      })),
    );
    await expect(success.getDatabase('db')).resolves.toEqual({
      id: 'db',
      name: 'fallback',
      created: false,
    });

    const absent = await api(
      new FakeRunner(async () => {
        throw new Error('D1 database does not exist');
      }),
    );
    await expect(absent.getDatabase('db')).resolves.toBeUndefined();
    const denied = new Error('authentication failed');
    const failure = await api(
      new FakeRunner(async () => {
        throw denied;
      }),
    );
    await expect(failure.getDatabase('db')).rejects.toBe(denied);
    const malformed = await api(
      new FakeRunner(async () => ({ stdout: '{"uuid":"other"}', stderr: '' })),
    );
    await expect(malformed.getDatabase('db')).rejects.toThrow(
      'D1 info result has an invalid uuid or name',
    );
  });

  it('distinguishes an absent version inventory from an empty one and preserves missing ids', async () => {
    const absent = await api(
      new FakeRunner(async () => {
        throw new Error('Worker not found');
      }),
    );
    await expect(absent.listVersions('worker')).resolves.toBeUndefined();
    const empty = await api(
      new FakeRunner(async () => ({ stdout: '[]', stderr: '' })),
    );
    await expect(empty.listVersions('worker')).resolves.toEqual([]);
    const incomplete = await api(
      new FakeRunner(async () => ({
        stdout: '[{"tag":"candidate"}]',
        stderr: '',
      })),
    );
    await expect(incomplete.listVersions('worker')).resolves.toEqual([
      { versionId: undefined, tag: 'candidate' },
    ]);
    const denied = new Error('version inventory permission denied');
    const failure = await api(
      new FakeRunner(async () => {
        throw denied;
      }),
    );
    await expect(failure.listVersions('worker')).rejects.toBe(denied);
  });

  it('keeps strict and absence-classifying version reads separate', async () => {
    const missing = new Error('version 10090 not found');
    const subject = await api(
      new FakeRunner(async () => {
        throw missing;
      }),
    );
    await expect(subject.viewVersion('worker', 'v1')).rejects.toBe(missing);
    await expect(subject.findVersion('worker', 'v1')).resolves.toBeUndefined();
    const denied = new Error('permission denied');
    const failure = await api(
      new FakeRunner(async () => {
        throw denied;
      }),
    );
    await expect(failure.findVersion('worker', 'v1')).rejects.toBe(denied);
  });

  it('returns raw deployment facts without applying backend refusals', async () => {
    const subject = await api(
      new FakeRunner(async () => ({
        stdout:
          '{"result":{"versions":[{"id":"v1","percentage":"25"},{"id":"v2"},{"id":"v3","percentage":"not-a-number"},{"id":"v4","percentage":null}]}}',
        stderr: '',
      })),
    );
    await expect(subject.deploymentStatus('worker')).resolves.toEqual({
      versions: [
        { versionId: 'v1', percentage: 25 },
        { versionId: 'v2', percentage: undefined },
        { versionId: 'v3', percentage: Number.NaN },
        { versionId: 'v4', percentage: 0 },
      ],
    });
    const denied = new Error('deployment status permission denied');
    const failure = await api(
      new FakeRunner(async () => {
        throw denied;
      }),
    );
    await expect(failure.deploymentStatus('worker')).rejects.toBe(denied);
  });
});

describe('WranglerPlainWorkerProvisioningApi mutations', () => {
  it.each([
    'initial',
    'staged',
  ] as const)('writes the exact %s config, secret mode, and argv', async (mode) => {
    let config: unknown;
    let secretMode: number | undefined;
    const runner = new FakeRunner(async (arguments_) => {
      const configPath = arguments_[
        arguments_.indexOf('--config') + 1
      ] as string;
      const secretsPath = arguments_[
        arguments_.indexOf('--secrets-file') + 1
      ] as string;
      config = JSON.parse(await readFile(configPath, 'utf8'));
      secretMode = (await stat(secretsPath)).mode & 0o777;
      expect(arguments_).toEqual([
        ...(mode === 'initial' ? ['deploy'] : ['versions', 'upload']),
        '--config',
        configPath,
        '--secrets-file',
        secretsPath,
        '--tag',
        'candidate-tag',
      ]);
      return { stdout: '', stderr: '' };
    });
    const outcome = await (await api(runner)).uploadCandidate(
      uploadIntent(mode),
      mutationFence(),
    );
    expect(outcome).toEqual({
      status: 'succeeded',
      cleanup: { status: 'succeeded' },
    });
    expect(secretMode).toBe(0o600);
    expect(config).toEqual({
      name: 'worker-name',
      main: 'worker.js',
      workers_dev: true,
      preview_urls: false,
      compatibility_date: '2026-08-10',
      vars: { TEXT: 'value' },
      d1_databases: [
        { binding: 'DB', database_name: 'db-name', database_id: 'db-id' },
      ],
      durable_objects: {
        bindings: [{ name: 'OBJECT', class_name: 'ObjectClass' }],
      },
      ...(mode === 'initial'
        ? {
            migrations: [
              {
                tag: 'v1',
                new_sqlite_classes: ['ObjectClass'],
                new_classes: [],
                deleted_classes: [],
                renamed_classes: [],
              },
            ],
          }
        : {}),
      r2_buckets: [{ binding: 'BUCKET', bucket_name: 'bucket-name' }],
      limits: { cpu_ms: 25 },
    });
    await expectUploadScratchRemoved();
  });

  it('asserts immediately before every Wrangler mutation dispatch', async () => {
    const events: string[] = [];
    const runner = new FakeRunner(async (arguments_) => {
      events.push(`run:${arguments_[0]}`);
      if (arguments_[0] === 'd1' && arguments_[1] === 'export') {
        await writeFile(
          arguments_[arguments_.indexOf('--output') + 1] as string,
          'select 1;',
        );
      }
      return { stdout: '', stderr: '' };
    });
    const subject = await api(runner);
    const owned = mutationFence(
      vi.fn(async () => {
        events.push('assert');
      }),
    );
    const calls = [
      ['d1', () => subject.createDatabase('db', owned)],
      [
        'versions',
        () =>
          subject.createDeployment(
            'worker',
            [{ versionId: 'v1', percentage: 100 }],
            owned,
          ),
      ],
      ['delete', () => subject.deleteWorkerScript('worker', owned)],
      [
        'versions',
        () => subject.uploadCandidate(uploadIntent('staged'), owned),
      ],
      ['d1', () => subject.exportDatabase({ id: 'db', name: 'name' }, owned)],
    ] as const;
    for (const [command, call] of calls) {
      events.length = 0;
      await call();
      expect(events).toEqual(['assert', `run:${command}`]);
    }
  });

  it.each([
    'createDatabase',
    'createDeployment',
    'deleteWorkerScript',
    'uploadCandidate',
    'exportDatabase',
  ] as const)('rejects a not-found-shaped fence failure before %s dispatch', async (method) => {
    const runner = new FakeRunner();
    const subject = await api(runner);
    const denied = new Error('lease not found');
    const deniedFence = mutationFence(
      vi.fn(async () => {
        throw denied;
      }),
    );
    const operation = {
      createDatabase: () => subject.createDatabase('db', deniedFence),
      createDeployment: () =>
        subject.createDeployment('worker', [], deniedFence),
      deleteWorkerScript: () =>
        subject.deleteWorkerScript('worker', deniedFence),
      uploadCandidate: () =>
        subject.uploadCandidate(uploadIntent('staged'), deniedFence),
      exportDatabase: () =>
        subject.exportDatabase({ id: 'db', name: 'db' }, deniedFence),
    }[method];
    await expect(operation()).rejects.toBe(denied);
    expect(runner.calls).toEqual([]);
    expect(deniedFence.assertOwned).toHaveBeenCalledTimes(1);
    if (method === 'uploadCandidate') await expectUploadScratchRemoved();
  });

  it('retains a pre-dispatch upload denial when scratch cleanup also fails', async () => {
    fsControl.failFleetCleanup = true;
    const cleanupError = fsControl.cleanupError;
    const denied = new Error('lease lost before upload dispatch');
    const runner = new FakeRunner();
    const subject = await api(runner);
    const rejection = await subject
      .uploadCandidate(
        uploadIntent('staged'),
        mutationFence(
          vi.fn(async () => {
            throw denied;
          }),
        ),
      )
      .then(
        () => new Error('expected upload to reject'),
        (error: unknown) => error,
      );
    expect(rejection).toBeInstanceOf(AggregateError);
    const aggregate = rejection as AggregateError;
    expect(aggregate.message).toBe(
      'Worker upload preparation and adapter scratch cleanup both failed',
    );
    expect(aggregate.errors).toHaveLength(2);
    expect(aggregate.errors[0]).toBe(denied);
    expect(aggregate.errors[1]).toBe(cleanupError);
    expect(runner.calls).toEqual([]);
  });

  it('classifies undefined upload rejection values by dispatch state', async () => {
    fsControl.failOperation = 'writeFile';
    fsControl.operationError = undefined;
    const preparationRunner = new FakeRunner();
    const preparation = await api(preparationRunner);
    await expect(
      preparation.uploadCandidate(uploadIntent('staged'), mutationFence()),
    ).rejects.toBeUndefined();
    expect(preparationRunner.calls).toEqual([]);
    await expectUploadScratchRemoved();

    fsControl.failOperation = undefined;
    scratchDirectories().length = 0;
    const preDispatchRunner = new FakeRunner();
    const preDispatch = await api(preDispatchRunner);
    await expect(
      preDispatch.uploadCandidate(
        uploadIntent('staged'),
        mutationFence(vi.fn(() => Promise.reject(undefined))),
      ),
    ).rejects.toBeUndefined();
    expect(preDispatchRunner.calls).toEqual([]);
    await expectUploadScratchRemoved();

    scratchDirectories().length = 0;
    const dispatched = await api(
      new FakeRunner(() => Promise.reject(undefined)),
    );
    await expect(
      dispatched.uploadCandidate(uploadIntent('staged'), mutationFence()),
    ).resolves.toEqual({
      status: 'failed',
      error: undefined,
      cleanup: { status: 'succeeded' },
    });
    await expectUploadScratchRemoved();

    scratchDirectories().length = 0;
    fsControl.failFleetCleanup = true;
    fsControl.cleanupError = undefined;
    const cleanup = await api(new FakeRunner());
    await expect(
      cleanup.uploadCandidate(uploadIntent('staged'), mutationFence()),
    ).resolves.toEqual({
      status: 'succeeded',
      cleanup: { status: 'failed', error: undefined },
    });
    expect(fsControl.residualDirectory).toBeDefined();
  });

  it('requires both exact-ID route methods and has no unfenced delete member', async () => {
    let runInsideCalls = 0;
    // vi.fn erases this generic, so keep the hand-rolled fenced scope.
    async function runInside<T>(
      fence: ExternalMutationFence,
      operation: () => Promise<T>,
    ): Promise<T> {
      runInsideCalls += 1;
      await fence.assertOwned();
      return operation();
    }
    const deleteDatabase = vi.fn(async () => {});
    const subject = await api(new FakeRunner(), {
      routeApi: routeApi({
        withMutationFence: runInside,
        getDatabase: async () => undefined,
        deleteDatabase,
      }),
    });
    expect('deleteDatabase' in subject).toBe(false);
    await subject.deleteDatabaseFenced('db', mutationFence());
    expect(runInsideCalls).toBe(1);
    expect(deleteDatabase).toHaveBeenCalledWith('db');
    const unsupported = await api(new FakeRunner());
    await expect(
      unsupported.deleteDatabaseFenced('db', mutationFence()),
    ).rejects.toThrow(
      'Wrangler plain Worker adapter requires immutable-ID D1 route methods',
    );
  });

  it('returns delete outcomes and rethrows non-absence failures', async () => {
    const deleted = await api(new FakeRunner());
    await expect(
      deleted.deleteWorkerScript('worker', mutationFence()),
    ).resolves.toBe('deleted');
    const absent = await api(
      new FakeRunner(async () => {
        throw new Error('script not found');
      }),
    );
    await expect(
      absent.deleteWorkerScript('worker', mutationFence()),
    ).resolves.toBe('absent');
    const denied = new Error('denied');
    const failed = await api(
      new FakeRunner(async () => {
        throw denied;
      }),
    );
    await expect(
      failed.deleteWorkerScript('worker', mutationFence()),
    ).rejects.toBe(denied);
  });

  it('reports upload preparation, dispatch, and cleanup failures separately', async () => {
    const prepRunner = new FakeRunner();
    const prep = await api(prepRunner);
    await expect(
      prep.uploadCandidate(
        {
          ...uploadIntent('staged'),
          modules: [{ name: '../escape.js', content: '' }],
        },
        mutationFence(),
      ),
    ).rejects.toThrow('escapes the staging directory');
    expect(prepRunner.calls).toEqual([]);
    await expectUploadScratchRemoved();

    scratchDirectories().length = 0;
    const dispatchError = new Error('dispatch failed');
    const dispatch = await api(
      new FakeRunner(async () => {
        throw dispatchError;
      }),
    );
    await expect(
      dispatch.uploadCandidate(uploadIntent('staged'), mutationFence()),
    ).resolves.toEqual({
      status: 'failed',
      error: dispatchError,
      cleanup: { status: 'succeeded' },
    });
    await expectUploadScratchRemoved();

    scratchDirectories().length = 0;
    fsControl.failFleetCleanup = true;
    const cleanup = await api(new FakeRunner());
    await expect(
      cleanup.uploadCandidate(uploadIntent('staged'), mutationFence()),
    ).resolves.toEqual({
      status: 'succeeded',
      cleanup: { status: 'failed', error: fsControl.cleanupError },
    });
    expect(fsControl.residualDirectory).toBeDefined();
  });

  it('retains dispatch and cleanup errors together', async () => {
    fsControl.failFleetCleanup = true;
    const dispatchError = new Error('dispatch failed');
    const subject = await api(
      new FakeRunner(async () => {
        throw dispatchError;
      }),
    );
    await expect(
      subject.uploadCandidate(uploadIntent('staged'), mutationFence()),
    ).resolves.toEqual({
      status: 'failed',
      error: dispatchError,
      cleanup: { status: 'failed', error: fsControl.cleanupError },
    });
  });

  it('delegates optional R2 capabilities only when present', async () => {
    const listWorkerR2Attachments = vi.fn(async () => []);
    const getR2Bucket = vi.fn(async () => undefined);
    const createR2Bucket = vi.fn(async () => {});
    const assertR2BucketEmpty = vi.fn(async () => {});
    const deleteR2Bucket = vi.fn(async () => {});
    const present = await api(new FakeRunner(), {
      routeApi: routeApi({
        listWorkerR2Attachments,
        getR2Bucket,
        createR2Bucket,
        assertR2BucketEmpty,
        deleteR2Bucket,
      }),
    });
    const resource = {
      name: 'BUCKET',
      bucketName: 'bucket',
      jurisdiction: 'default',
    } as const;
    const owned = mutationFence();
    await present.listWorkerR2Attachments?.('bucket');
    await present.getR2Bucket?.('bucket', 'default');
    await present.createR2Bucket?.(resource, owned);
    await present.assertR2BucketEmpty?.(resource);
    await present.deleteR2Bucket?.(resource, owned);
    expect(listWorkerR2Attachments).toHaveBeenCalledWith('bucket');
    expect(getR2Bucket).toHaveBeenCalledWith('bucket', 'default');
    expect(createR2Bucket).toHaveBeenCalledWith(resource, owned);
    expect(assertR2BucketEmpty).toHaveBeenCalledWith(resource);
    expect(deleteR2Bucket).toHaveBeenCalledWith(resource, owned);
    const absent = await api(new FakeRunner());
    expect(absent.getR2Bucket).toBeUndefined();
    expect(absent.listWorkerR2Attachments).toBeUndefined();
    expect(absent.createR2Bucket).toBeUndefined();
    expect(absent.assertR2BucketEmpty).toBeUndefined();
    expect(absent.deleteR2Bucket).toBeUndefined();
  });
});

describe('WranglerPlainWorkerProvisioningApi exports', () => {
  it('removes export scratch when the fence denies dispatch', async () => {
    const exportDirectory = await mkdtemp(join(tmpdir(), 'adapter-export-'));
    exportDirectories.add(exportDirectory);
    const denied = new Error('lease lost before export dispatch');
    const runner = new FakeRunner();
    const subject = await api(runner, { exportDirectory });
    await expect(
      subject.exportDatabase(
        { id: 'db', name: 'name' },
        mutationFence(
          vi.fn(async () => {
            throw denied;
          }),
        ),
      ),
    ).rejects.toBe(denied);
    const actual =
      await vi.importActual<typeof import('node:fs/promises')>(
        'node:fs/promises',
      );
    await expect(actual.readdir(exportDirectory)).resolves.toEqual([]);
    expect(runner.calls).toEqual([]);
  });

  async function exportSubject(
    options: {
      readonly bytes?: string;
      readonly store?: DurableDatabaseExportStore;
      readonly output?: 'file' | 'empty' | 'directory';
    } = {},
  ) {
    let outputPath = '';
    const runner = new FakeRunner(async (arguments_) => {
      outputPath = arguments_[arguments_.indexOf('--output') + 1] as string;
      if (options.output === 'directory') {
        const fs =
          await vi.importActual<typeof import('node:fs/promises')>(
            'node:fs/promises',
          );
        await fs.mkdir(outputPath);
      } else {
        await writeFile(
          outputPath,
          options.output === 'empty' ? '' : (options.bytes ?? 'select 1;'),
        );
      }
      return { stdout: '', stderr: '' };
    });
    return {
      subject: await api(runner, { exportStore: options.store }),
      output: () => outputPath,
    };
  }

  it('rejects a store that reads only one prefix byte', async () => {
    const store: DurableDatabaseExportStore = {
      async write(input) {
        const reader = input.body.getReader();
        const first = await reader.read();
        const bytes = first.done ? new Uint8Array() : first.value.slice(0, 1);
        return {
          location: 'memory://prefix',
          size: bytes.byteLength,
          sha256: createHash('sha256').update(bytes).digest('hex'),
        };
      },
    };
    const { subject, output } = await exportSubject({
      bytes: 'complete export',
      store,
    });
    await expect(
      subject.exportDatabase({ id: 'db', name: 'name' }, mutationFence()),
    ).rejects.toThrow(
      'durable database export store returned mismatched committed integrity',
    );
    await expectExportScratchRemoved(output());
  });

  it('rejects a store that never reads the body and claims the empty digest', async () => {
    const store: DurableDatabaseExportStore = {
      async write(input) {
        await input.body.cancel();
        return {
          location: 'memory://empty',
          size: 0,
          sha256: createHash('sha256').digest('hex'),
        };
      },
    };
    const { subject, output } = await exportSubject({
      bytes: 'complete export',
      store,
    });
    await expect(
      subject.exportDatabase({ id: 'db', name: 'name' }, mutationFence()),
    ).rejects.toThrow(
      'durable database export store returned mismatched committed integrity',
    );
    await expectExportScratchRemoved(output());
  });

  it('rejects a multi-chunk export store that commits only the first chunk', async () => {
    const store: DurableDatabaseExportStore = {
      async write(input) {
        const reader = input.body.getReader();
        const first = await reader.read();
        if (first.done) throw new Error('expected a first export chunk');
        await reader.cancel();
        return {
          location: 'memory://first-chunk',
          size: first.value.byteLength,
          sha256: createHash('sha256').update(first.value).digest('hex'),
        };
      },
    };
    const { subject, output } = await exportSubject({
      bytes: 'x'.repeat(128 * 1024 + 1),
      store,
    });
    await expect(
      subject.exportDatabase({ id: 'db', name: 'name' }, mutationFence()),
    ).rejects.toThrow(
      'durable database export store returned mismatched committed integrity',
    );
    await expectExportScratchRemoved(output());
  });

  it.each([
    'empty',
    'directory',
  ] as const)('refuses %s export output and removes scratch', async (outputKind) => {
    const { subject, output } = await exportSubject({ output: outputKind });
    await expect(
      subject.exportDatabase({ id: 'db', name: 'name' }, mutationFence()),
    ).rejects.toThrow('Wrangler database export is not a non-empty file');
    await expectExportScratchRemoved(output());
  });

  it.each([
    'chmod',
    'stat',
  ] as const)('propagates %s failure and removes export scratch', async (operation) => {
    fsControl.failOperation = operation;
    const failure = new Error(`${operation} failed`);
    fsControl.operationError = failure;
    const { subject, output } = await exportSubject();
    await expect(
      subject.exportDatabase({ id: 'db', name: 'name' }, mutationFence()),
    ).rejects.toBe(failure);
    await expectExportScratchRemoved(output());
  });

  it('propagates runner failure and removes export scratch', async () => {
    const failure = new Error('export dispatch failed');
    let outputPath = '';
    const runner = new FakeRunner(async (arguments_) => {
      outputPath = arguments_[arguments_.indexOf('--output') + 1] as string;
      throw failure;
    });
    const subject = await api(runner);
    await expect(
      subject.exportDatabase({ id: 'db', name: 'name' }, mutationFence()),
    ).rejects.toBe(failure);
    await expectExportScratchRemoved(outputPath);
  });

  it('propagates store failures and removes scratch', async () => {
    const storeError = new Error('store failed');
    const { subject, output } = await exportSubject({
      store: {
        async write() {
          throw storeError;
        },
      },
    });
    await expect(
      subject.exportDatabase({ id: 'db', name: 'name' }, mutationFence()),
    ).rejects.toBe(storeError);
    await expectExportScratchRemoved(output());
  });

  it('returns the independent digest, size, location, and secure file mode', async () => {
    const bytes = 'fixture export bytes';
    let mode: number | undefined;
    let outputPath = '';
    const runner = new FakeRunner(async (arguments_) => {
      outputPath = arguments_[arguments_.indexOf('--output') + 1] as string;
      await writeFile(outputPath, bytes);
      return { stdout: '', stderr: '' };
    });
    const store: DurableDatabaseExportStore = {
      async write(input) {
        mode = (await stat(outputPath)).mode & 0o777;
        const body = await drain(input.body);
        return {
          location: 'memory://complete',
          size: body.size,
          sha256: body.sha256,
        };
      },
    };
    const subject = await api(runner, { exportStore: store });
    await expect(
      subject.exportDatabase({ id: 'db', name: 'name' }, mutationFence()),
    ).resolves.toEqual({
      location: 'memory://complete',
      size: Buffer.byteLength(bytes),
      sha256: createHash('sha256').update(bytes).digest('hex'),
    });
    expect(mode).toBe(0o600);
    await expectExportScratchRemoved(outputPath);
  });

  it('refuses an empty durable location', async () => {
    const store: DurableDatabaseExportStore = {
      async write(input) {
        const body = await drain(input.body);
        return {
          location: '',
          size: body.size,
          sha256: body.sha256,
        };
      },
    };
    const { subject, output } = await exportSubject({ store });
    await expect(
      subject.exportDatabase({ id: 'db', name: 'name' }, mutationFence()),
    ).rejects.toThrow(
      'durable database export store returned mismatched committed integrity',
    );
    await expectExportScratchRemoved(output());
  });
});
