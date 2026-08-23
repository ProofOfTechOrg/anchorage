// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { describe, expect, it, vi } from 'vitest';
import type { DurableDatabaseExportStore } from '../src/cloudflare-client.js';
import { WorkerDeploymentError } from '../src/deployment-error.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import type {
  DatabaseReference,
  DeploymentSecrets,
  DeploymentSpec,
  ExternalMutationFence,
  ExternalReleaseSnapshot,
  FleetRecord,
} from '../src/types.js';
import {
  type PlainWorkerCustomDomain,
  type PlainWorkerRouteApi,
  plainWorkerIngressModule,
  WranglerLoopBackend,
} from '../src/wrangler-loop-backend.js';
import type { CommandResult, CommandRunner } from '../src/wrangler-runner.js';

const deployment: DeploymentSpec = {
  tenantTag: 'acme',
  environment: 'production',
  scriptName: 'acme-production',
  databaseName: 'acme-production',
  compatibilityDate: '2026-08-10',
  compatibilityFlags: ['nodejs_compat'],
  mainModule: 'worker.js',
  modules: [
    {
      name: 'worker.js',
      content:
        'export class Maintenance {}; export default {fetch(){return new Response("ok")}}',
    },
  ],
  authoredBy: 'platform',
  schemaVersion: 3,
  migrations: [],
  durableObjectMigrations: [],
  durableObjectBindings: [{ name: 'MAINTENANCE', className: 'Maintenance' }],
  egressProxyService: 'fleet-egress-proxy',
  maintenanceBaseUrl: 'https://control-acme.example.test',
  routeHostname: 'acme.example.test',
};

const database: DatabaseReference = {
  id: 'database-acme',
  name: 'acme-production',
  created: true,
};

const secrets: DeploymentSecrets = {
  deploymentIdentity: 'deployment-identity-secret-value-0001',
  maintenanceAdmin: 'maintenance-admin-secret-value-00001',
};

const fleetRecord: FleetRecord = {
  tenantTag: deployment.tenantTag,
  backend: 'plain-worker',
  environment: deployment.environment,
  scriptName: deployment.scriptName,
  databaseId: database.id,
  databaseName: database.name,
  schemaVersion: deployment.schemaVersion,
  artifactVersion: 'version-owned',
  desiredSpecDigest: deploymentSpecDigest(deployment),
  durableObjectBindings: [],
  routeHostname: deployment.routeHostname,
  phase: 'worker-deleted',
  updatedAt: '2026-08-11T00:00:00.000Z',
};

const activeRelease: ExternalReleaseSnapshot = {
  physicalScriptName: deployment.scriptName,
  specDigest: deploymentSpecDigest(deployment),
  artifactVersion: 'version-owned',
  releaseSchemaVersion: deployment.schemaVersion,
};

const mutationFence: ExternalMutationFence = {
  mutationLeaseTtlMs: 15 * 60_000,
  assertOwned: vi.fn(async () => {}),
};

interface RunnerCall {
  readonly arguments: readonly string[];
  readonly options: { readonly input?: string; readonly cwd?: string };
}

type RunnerHandler = (
  arguments_: readonly string[],
  options: { readonly input?: string; readonly cwd?: string },
) => Promise<CommandResult>;

class FakeRunner implements CommandRunner {
  readonly maxDurationMs: number;
  readonly calls: RunnerCall[] = [];
  readonly #handler: RunnerHandler;

  constructor(
    handler: RunnerHandler = async () => ({ stdout: '', stderr: '' }),
    maxDurationMs = 5 * 60_000,
  ) {
    this.#handler = handler;
    this.maxDurationMs = maxDurationMs;
  }

  run(
    arguments_: readonly string[],
    options: { readonly input?: string; readonly cwd?: string } = {},
  ): Promise<CommandResult> {
    this.calls.push({ arguments: [...arguments_], options: { ...options } });
    return this.#handler(arguments_, options);
  }
}

class FakeRouteApi implements PlainWorkerRouteApi {
  readonly calls: Array<
    | {
        readonly operation: 'attach';
        readonly hostname: string;
        readonly service: string;
      }
    | { readonly operation: 'detach'; readonly domainId: string }
    | {
        readonly operation: 'delete-secret';
        readonly scriptName: string;
        readonly secretName: string;
      }
    | { readonly operation: 'delete-database'; readonly databaseId: string }
  > = [];
  listCalls = 0;
  scriptPresent = false;
  databaseAttachments: Array<{
    readonly scriptName: string;
    readonly plane: 'ordinary' | 'dispatch';
  }> = [];
  readonly namespaceIds = new Set<string>();
  readonly secretNames = new Set([
    'DEPLOYMENT_IDENTITY_SECRET',
    'MAINTENANCE_ADMIN_SECRET',
  ]);
  secretRevocationNoop = false;
  afterDeleteControlSecret: ((secretName: string) => void) | undefined;
  secretListReads = 0;
  secretListError: Error | undefined;
  databasePresent = true;
  readonly databaseQueries: Array<{
    readonly databaseId: string;
    readonly sql: string;
    readonly bindings: readonly unknown[];
  }> = [];
  readonly databaseBatches: Array<{
    readonly databaseId: string;
    readonly statements: readonly {
      readonly sql: string;
      readonly bindings?: readonly unknown[];
    }[];
  }> = [];
  queryHandler: (
    sql: string,
    bindings: readonly unknown[],
  ) => Promise<readonly Readonly<Record<string, unknown>>[]> = async () => [];
  workersDevEnabled = false;
  previewUrlsEnabled = false;
  zoneRoutes: Array<{
    readonly zoneId: string;
    readonly routeId: string;
    readonly pattern: string;
  }> = [];
  beforeListCustomDomains: (() => void) | undefined;
  domains: PlainWorkerCustomDomain[];

  constructor(domains: readonly PlainWorkerCustomDomain[] = []) {
    this.domains = [...domains];
  }

  async withMutationFence<T>(
    fence: ExternalMutationFence,
    operation: () => Promise<T>,
  ): Promise<T> {
    await fence.assertOwned();
    return operation();
  }

  async queryDatabase(
    databaseId: string,
    sql: string,
    bindings: readonly unknown[] = [],
  ): Promise<readonly Readonly<Record<string, unknown>>[]> {
    this.databaseQueries.push({ databaseId, sql, bindings: [...bindings] });
    return this.queryHandler(sql, bindings);
  }

  async batchDatabase(
    databaseId: string,
    statements: readonly {
      readonly sql: string;
      readonly bindings?: readonly unknown[];
    }[],
  ): Promise<void> {
    this.databaseBatches.push({ databaseId, statements });
    for (const statement of statements) {
      await this.queryHandler(statement.sql, statement.bindings ?? []);
    }
  }

  async getDatabase(
    databaseId: string,
  ): Promise<DatabaseReference | undefined> {
    return this.databasePresent && databaseId === database.id
      ? { ...database, created: false }
      : undefined;
  }

  async deleteDatabase(databaseId: string): Promise<void> {
    this.calls.push({ operation: 'delete-database', databaseId });
    this.databasePresent = false;
  }

  async listCustomDomains(): Promise<readonly PlainWorkerCustomDomain[]> {
    this.beforeListCustomDomains?.();
    this.listCalls += 1;
    return [...this.domains];
  }

  async inspectOrdinaryWorkerFootprint(scriptName: string) {
    return {
      scriptPresent: this.scriptPresent,
      workersDevEnabled: this.workersDevEnabled,
      previewUrlsEnabled: this.previewUrlsEnabled,
      customDomains: this.domains.filter(
        (domain) => domain.service === scriptName,
      ),
      zoneRoutes: [...this.zoneRoutes],
    };
  }

  async listDurableObjectNamespaces(): Promise<readonly string[]> {
    return [...this.namespaceIds].sort();
  }

  async listOrdinaryWorkerSecretNames(): Promise<readonly string[]> {
    this.secretListReads += 1;
    if (this.secretListError) throw this.secretListError;
    return [...this.secretNames].sort();
  }

  async deleteControlSecrets(
    scriptName: string,
    secretNames: readonly string[],
  ): Promise<void> {
    for (const secretName of [...new Set(secretNames)].sort()) {
      this.calls.push({ operation: 'delete-secret', scriptName, secretName });
      if (!this.secretRevocationNoop) this.secretNames.delete(secretName);
      this.afterDeleteControlSecret?.(secretName);
    }
  }

  async listWorkerDatabaseAttachments(): Promise<
    readonly {
      readonly scriptName: string;
      readonly plane: 'ordinary' | 'dispatch';
    }[]
  > {
    return [...this.databaseAttachments];
  }

  async attachCustomDomain(target: {
    readonly hostname: string;
    readonly service: string;
  }): Promise<void> {
    this.calls.push({ operation: 'attach', ...target });
    this.domains = this.domains.filter(
      (domain) =>
        domain.hostname.toLowerCase() !== target.hostname.toLowerCase(),
    );
    this.domains.push({ id: 'domain-attached', ...target });
  }

  async detachCustomDomain(domainId: string): Promise<void> {
    this.calls.push({ operation: 'detach', domainId });
    this.domains = this.domains.filter((domain) => domain.id !== domainId);
  }

  async disableOrdinaryWorkerPublicAccess(): Promise<void> {
    this.workersDevEnabled = false;
    this.previewUrlsEnabled = false;
  }
}

function databaseRouteMethods(): Pick<
  PlainWorkerRouteApi,
  | 'withMutationFence'
  | 'queryDatabase'
  | 'batchDatabase'
  | 'getDatabase'
  | 'deleteDatabase'
> {
  return {
    async withMutationFence(fence, operation) {
      await fence.assertOwned();
      return operation();
    },
    async queryDatabase() {
      return [];
    },
    async batchDatabase() {},
    async getDatabase() {
      return undefined;
    },
    async deleteDatabase() {},
  };
}

function notFound(resource = 'resource'): Error {
  return new Error(`wrangler exited 1: ${resource} not found`);
}

function operation(call: RunnerCall): string {
  if (call.arguments[0] === 'secret') {
    return call.arguments.slice(0, 3).join(' ');
  }
  return call.arguments.slice(0, 2).join(' ');
}

function errorChain(error: unknown): string {
  const messages: string[] = [];
  let current = error;
  while (current instanceof Error) {
    messages.push(current.message);
    current = current.cause;
  }
  return messages.join(' | ');
}

function backend(
  runner: CommandRunner,
  options: {
    readonly fetch?: typeof fetch;
    readonly exportDirectory?: string;
    readonly exportStore?: DurableDatabaseExportStore;
    readonly routeApi?: PlainWorkerRouteApi;
    readonly maintenanceRequestTimeoutMs?: number;
  } = {},
): WranglerLoopBackend {
  const exportStore: DurableDatabaseExportStore = options.exportStore ?? {
    async write(input) {
      const hash = createHash('sha256');
      let size = 0;
      const reader = input.body.getReader();
      for (;;) {
        const chunk = await reader.read();
        if (chunk.done) break;
        hash.update(chunk.value);
        size += chunk.value.byteLength;
      }
      return {
        location: `memory://fleet-exports/${input.fileName}`,
        size,
        sha256: hash.digest('hex'),
      };
    },
  };
  return new WranglerLoopBackend({
    runner,
    routeApi: options.routeApi ?? new FakeRouteApi(),
    exportDirectory: options.exportDirectory ?? '/tmp/fleet-exports',
    exportStore,
    fetch: options.fetch,
    maintenanceRequestTimeoutMs: options.maintenanceRequestTimeoutMs,
  });
}

function revokeDeploymentCredentials(subject: WranglerLoopBackend) {
  return subject.revokeCredentials(
    deployment,
    undefined,
    undefined,
    database,
    mutationFence,
  );
}

function deleteDeploymentWorker(subject: WranglerLoopBackend) {
  return subject.deleteWorker(
    deployment,
    undefined,
    database,
    undefined,
    mutationFence,
  );
}

function removeDeploymentTraffic(subject: WranglerLoopBackend) {
  return subject.removeTraffic(
    deployment,
    undefined,
    undefined,
    database,
    mutationFence,
  );
}

function assertDeploymentDatabaseDetached(subject: WranglerLoopBackend) {
  return subject.assertDatabaseDetached(
    deployment,
    fleetRecord,
    database,
    mutationFence,
  );
}

function maintenanceResponse(): Response {
  return Response.json({
    nextSweepAt: 2_000,
    nextPurgeAt: 3_000,
    alarmAt: 2_000,
    lastSweepAt: 1_000,
    deploymentSpecDigest: deploymentSpecDigest(deployment),
  });
}

function listedVersion(
  id: string,
  digest: string,
): Readonly<Record<string, unknown>> {
  return {
    id,
    annotations: { 'workers/tag': digest },
  };
}

function viewedVersion(
  digest: string,
  artifactDatabase: DatabaseReference = database,
): Readonly<Record<string, unknown>> {
  return {
    resources: {
      bindings: [
        { type: 'd1', name: 'DB', database_id: artifactDatabase.id },
        {
          type: 'durable_object_namespace',
          name: 'MAINTENANCE',
          class_name: 'Maintenance',
          namespace_id: 'namespace-maintenance',
        },
        {
          type: 'service',
          name: 'EGRESS_PROXY',
          service: deployment.egressProxyService,
        },
        { type: 'plain_text', name: 'DEPLOYMENT_TENANT', text: 'acme' },
        {
          type: 'plain_text',
          name: 'FLEET_ENVIRONMENT',
          text: 'production',
        },
        { type: 'plain_text', name: 'FLEET_SCHEMA_VERSION', text: '3' },
        { type: 'plain_text', name: 'FLEET_SPEC_DIGEST', text: digest },
        {
          type: 'plain_text',
          name: 'FLEET_INGRESS_CONTRACT',
          text: 'guarded-object-v1',
        },
      ],
    },
  };
}

function ownedWorkerRunner(
  options: {
    readonly version?: Readonly<Record<string, unknown>>;
    readonly survivesDelete?: boolean;
    readonly onDelete?: () => void;
    readonly deleteError?: Error;
  } = {},
): FakeRunner {
  const digest = deploymentSpecDigest(deployment);
  let workerExists = true;
  return new FakeRunner(async (arguments_) => {
    const command = arguments_.slice(0, 2).join(' ');
    if (command === 'deployments status') {
      if (!workerExists) throw notFound('Worker');
      return {
        stdout: JSON.stringify({
          versions: [{ version_id: 'version-owned', percentage: 100 }],
        }),
        stderr: '',
      };
    }
    if (command === 'versions list') {
      if (!workerExists) throw notFound('Worker');
      return {
        stdout: JSON.stringify([listedVersion('version-owned', digest)]),
        stderr: '',
      };
    }
    if (command === 'versions view') {
      return {
        stdout: JSON.stringify(options.version ?? viewedVersion(digest)),
        stderr: '',
      };
    }
    if (command === 'd1 list') {
      return {
        stdout: JSON.stringify([
          { name: deployment.databaseName, uuid: database.id },
        ]),
        stderr: '',
      };
    }
    if (arguments_[0] === 'delete' && !options.survivesDelete) {
      workerExists = false;
      options.onDelete?.();
      if (options.deleteError) throw options.deleteError;
    }
    return { stdout: '', stderr: '' };
  });
}

describe('WranglerLoopBackend', () => {
  it('guards workers.dev, control, route override, and unknown ingress before user code', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'plain-ingress-'));
    const userModuleName = 'user module.js';
    const calls: string[] = [];
    const callKey = `__anchorageIngressCalls${Date.now()}`;
    Object.assign(globalThis, { [callKey]: calls });
    const guardedSpec: DeploymentSpec = {
      ...deployment,
      mainModule: userModuleName,
      modules: [
        {
          name: userModuleName,
          content: `export class Maintenance {}
export default {
  marker: 'receiver-ok',
  fetch(request, env) {
    globalThis[${JSON.stringify(callKey)}].push(new URL(request.url).pathname);
    if (
      new URL(request.url).pathname.startsWith('/admin/') &&
      request.headers.get('authorization') !== \`Bearer \${env.MAINTENANCE_ADMIN_SECRET}\`
    ) {
      return new Response(null, { status: 401 });
    }
    return new Response(this.marker);
  },
};`,
        },
      ],
    };
    const ingress = plainWorkerIngressModule(guardedSpec);
    try {
      await writeFile(join(directory, 'package.json'), '{"type":"module"}');
      await writeFile(
        join(directory, userModuleName),
        guardedSpec.modules[0]?.content ?? '',
      );
      await writeFile(join(directory, ingress.name), ingress.content);
      const loaded = (await import(
        `${pathToFileURL(join(directory, ingress.name)).href}?test=${Date.now()}`
      )) as {
        readonly Maintenance: unknown;
        readonly default: {
          fetch(
            request: Request,
            env: Readonly<Record<string, unknown>>,
            context: unknown,
          ): Promise<Response>;
        };
      };
      const env = {
        MAINTENANCE_ADMIN_SECRET: secrets.maintenanceAdmin,
      };
      const invoke = (url: string, init?: RequestInit) =>
        loaded.default.fetch(new Request(url, init), env, {});

      await expect(
        invoke('https://acme-production.account.workers.dev/app'),
      ).resolves.toMatchObject({ status: 404 });
      await expect(
        invoke(`https://${deployment.routeHostname}/app`, {
          headers: {
            'Cloudflare-Workers-Version-Overrides':
              'acme-production="candidate"',
          },
        }),
      ).resolves.toMatchObject({ status: 404 });
      await expect(
        invoke('https://control-acme.example.test/app', {
          headers: {
            authorization: `Bearer ${secrets.maintenanceAdmin}`,
          },
        }),
      ).resolves.toMatchObject({ status: 404 });
      await expect(
        invoke('https://unknown.example.test/app'),
      ).resolves.toMatchObject({ status: 404 });
      await expect(
        invoke('https://control-acme.example.test/admin/ensure-maintenance', {
          method: 'POST',
          headers: { authorization: 'Bearer too-short' },
        }),
      ).resolves.toMatchObject({ status: 404 });
      await expect(
        invoke('https://control-acme.example.test/admin/ensure-maintenance', {
          method: 'POST',
          headers: {
            authorization: `Bearer ${'x'.repeat(32)}`,
          },
        }),
      ).resolves.toMatchObject({ status: 401 });
      await expect(
        invoke(`https://${deployment.routeHostname}/app`).then((response) =>
          response.text(),
        ),
      ).resolves.toBe('receiver-ok');
      await expect(
        invoke('https://control-acme.example.test/admin/ensure-maintenance', {
          method: 'POST',
          headers: {
            authorization: `Bearer ${secrets.maintenanceAdmin}`,
            'Cloudflare-Workers-Version-Overrides':
              'acme-production="candidate"',
          },
        }).then((response) => response.text()),
      ).resolves.toBe('receiver-ok');
      expect(calls).toEqual([
        '/admin/ensure-maintenance',
        '/app',
        '/admin/ensure-maintenance',
      ]);
      expect(typeof loaded.Maintenance).toBe('function');
    } finally {
      Reflect.deleteProperty(globalThis, callKey);
      await rm(directory, { recursive: true, force: true });
    }
  });

  it('fails guarded module evaluation for a default export without callable fetch', async () => {
    for (const unsupported of [
      'export default class Worker { fetch() {} }',
      'export default {scheduled() {}}',
    ]) {
      const directory = await mkdtemp(join(tmpdir(), 'plain-ingress-invalid-'));
      const unsupportedSpec: DeploymentSpec = {
        ...deployment,
        modules: [{ name: 'worker.js', content: unsupported }],
        durableObjectBindings: [],
      };
      const ingress = plainWorkerIngressModule(unsupportedSpec);
      try {
        await writeFile(join(directory, 'package.json'), '{"type":"module"}');
        await writeFile(join(directory, 'worker.js'), unsupported);
        await writeFile(join(directory, ingress.name), ingress.content);
        await expect(
          import(
            `${pathToFileURL(join(directory, ingress.name)).href}?test=${Math.random()}`
          ),
        ).rejects.toThrow(/default-export an object with fetch/);
      } finally {
        await rm(directory, { recursive: true, force: true });
      }
    }
  });

  it('immediately deploys an initial DO version from a route-free config and a 0600 secrets file', async () => {
    const initial = {
      ...deployment,
      durableObjectMigrations: [
        { tag: 'v1', newSqliteClasses: ['Maintenance'] },
      ],
    } satisfies DeploymentSpec;
    const digest = deploymentSpecDigest(initial);
    let deployed = false;
    const runner = new FakeRunner(async (arguments_, options) => {
      if (arguments_[0] === 'deployments') {
        if (!deployed) throw notFound('Worker');
        return {
          stdout: JSON.stringify({
            versions: [{ version_id: 'version-new', percentage: 100 }],
          }),
          stderr: '',
        };
      }
      if (arguments_[0] === 'versions' && arguments_[1] === 'list') {
        if (!deployed) throw notFound('Worker');
        return {
          stdout: JSON.stringify([listedVersion('version-new', digest)]),
          stderr: '',
        };
      }
      if (arguments_[0] === 'versions' && arguments_[1] === 'view') {
        return { stdout: JSON.stringify(viewedVersion(digest)), stderr: '' };
      }
      if (arguments_[0] === 'deploy') {
        const configPath = arguments_[arguments_.indexOf('--config') + 1];
        const secretsPath =
          arguments_[arguments_.indexOf('--secrets-file') + 1];
        expect(configPath).toBeTruthy();
        expect(secretsPath).toBeTruthy();
        const config = JSON.parse(
          await readFile(configPath as string, 'utf8'),
        ) as Record<string, unknown>;
        expect(config).not.toHaveProperty('routes');
        expect(config).not.toHaveProperty('triggers');
        expect(config).toMatchObject({
          main: '__anchorage_guarded_entry__.js',
          workers_dev: true,
          preview_urls: false,
          vars: { FLEET_INGRESS_CONTRACT: 'guarded-object-v1' },
          migrations: [{ tag: 'v1', new_sqlite_classes: ['Maintenance'] }],
          services: [
            {
              binding: 'EGRESS_PROXY',
              service: deployment.egressProxyService,
            },
          ],
        });
        expect((await stat(secretsPath as string)).mode & 0o777).toBe(0o600);
        await expect(
          readFile(secretsPath as string, 'utf8').then(JSON.parse),
        ).resolves.toEqual({
          DEPLOYMENT_IDENTITY_SECRET: secrets.deploymentIdentity,
          MAINTENANCE_ADMIN_SECRET: secrets.maintenanceAdmin,
        });
        expect(options.input).toBeUndefined();
        expect(arguments_).toContain(digest);
        deployed = true;
      }
      return { stdout: '', stderr: '' };
    });

    await expect(
      backend(runner).deployWorker(
        initial,
        database,
        secrets,
        undefined,
        mutationFence,
      ),
    ).resolves.toEqual({ artifactVersion: 'version-new', created: true });
    expect(runner.calls.some((call) => call.arguments[0] === 'secret')).toBe(
      false,
    );
    expect(runner.calls.some((call) => call.arguments[0] === 'triggers')).toBe(
      false,
    );
  });

  it('uploads an existing candidate with secrets, validates it at 0%, promotes it, then publishes only its custom domain', async () => {
    const digest = deploymentSpecDigest(deployment);
    const events: string[] = [];
    let versions = [
      listedVersion('version-old', 'b'.repeat(64)),
      listedVersion('malicious-copy', digest),
    ];
    let deployedVersions = [{ version_id: 'version-old', percentage: 100 }];
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'deployments status') {
        return {
          stdout: JSON.stringify({ versions: deployedVersions }),
          stderr: '',
        };
      }
      if (command === 'versions list') {
        return { stdout: JSON.stringify(versions), stderr: '' };
      }
      if (command === 'versions view') {
        return { stdout: JSON.stringify(viewedVersion(digest)), stderr: '' };
      }
      if (command === 'versions upload') {
        const configPath = arguments_[arguments_.indexOf('--config') + 1];
        const secretsPath =
          arguments_[arguments_.indexOf('--secrets-file') + 1];
        const config = JSON.parse(
          await readFile(configPath as string, 'utf8'),
        ) as Record<string, unknown>;
        expect(config).not.toHaveProperty('routes');
        expect(config).not.toHaveProperty('triggers');
        expect(config).not.toHaveProperty('migrations');
        expect(config).toMatchObject({
          main: '__anchorage_guarded_entry__.js',
          workers_dev: true,
          preview_urls: false,
          vars: { FLEET_INGRESS_CONTRACT: 'guarded-object-v1' },
        });
        expect((await stat(secretsPath as string)).mode & 0o777).toBe(0o600);
        versions = [...versions, listedVersion('version-next', digest)];
      }
      if (command === 'versions deploy') {
        if (arguments_.includes('version-next@100%')) {
          deployedVersions = [{ version_id: 'version-next', percentage: 100 }];
        } else {
          expect(arguments_).toEqual([
            'versions',
            'deploy',
            'version-old@100%',
            'version-next@0%',
            '--name',
            deployment.scriptName,
            '-y',
          ]);
          deployedVersions = [
            { version_id: 'version-old', percentage: 100 },
            { version_id: 'version-next', percentage: 0 },
          ];
        }
      }
      return { stdout: '', stderr: '' };
    });
    const request = vi.fn(async () => {
      events.push('maintenance');
      return maintenanceResponse();
    });
    const routeApi = new FakeRouteApi();
    const subject = backend(runner, { fetch: request, routeApi });

    await expect(
      subject.deployWorker(
        deployment,
        database,
        secrets,
        undefined,
        mutationFence,
      ),
    ).resolves.toEqual({ artifactVersion: 'version-next', created: false });
    expect(runner.calls.some((call) => call.arguments[0] === 'triggers')).toBe(
      false,
    );

    await expect(
      subject.ensureMaintenance(
        deployment,
        secrets.maintenanceAdmin,
        mutationFence,
        'version-next',
      ),
    ).resolves.toMatchObject({
      armed: true,
      deploymentSpecDigest: digest,
    });
    expect(request).toHaveBeenLastCalledWith(
      new URL('https://control-acme.example.test/admin/ensure-maintenance'),
      expect.objectContaining({
        method: 'POST',
        headers: {
          authorization: `Bearer ${secrets.maintenanceAdmin}`,
          'Cloudflare-Workers-Version-Overrides': `${deployment.scriptName}="version-next"`,
        },
        signal: expect.any(AbortSignal),
      }),
    );

    await subject.promoteWorker(
      deployment,
      {
        allowedCurrentScriptNames: [deployment.scriptName],
        allowUnrouted: true,
      },
      undefined,
      mutationFence,
      'version-next',
    );
    expect(routeApi.calls).toEqual([
      {
        operation: 'attach',
        hostname: deployment.routeHostname,
        service: deployment.scriptName,
      },
    ]);
    expect(routeApi.listCalls).toBe(3);
    expect(events).toEqual(['maintenance']);
  });

  it('refuses to upload over an existing Worker with different deployment ownership', async () => {
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'deployments status') {
        return {
          stdout: JSON.stringify({
            versions: [{ version_id: 'version-foreign', percentage: 100 }],
          }),
          stderr: '',
        };
      }
      if (command === 'versions list') {
        return {
          stdout: JSON.stringify([
            listedVersion('version-foreign', 'b'.repeat(64)),
          ]),
          stderr: '',
        };
      }
      if (command === 'versions view') {
        const version = viewedVersion('b'.repeat(64)) as {
          resources: { bindings: Array<Record<string, unknown>> };
        };
        const tenant = version.resources.bindings.find(
          (binding) => binding.name === 'DEPLOYMENT_TENANT',
        );
        if (tenant) tenant.text = 'platform-plane';
        return { stdout: JSON.stringify(version), stderr: '' };
      }
      throw new Error(`unexpected mutation ${command}`);
    });

    await expect(
      backend(runner).deployWorker(
        deployment,
        database,
        secrets,
        undefined,
        mutationFence,
      ),
    ).rejects.toThrow(/different deployment ownership|drifted tenant/);
    expect(runner.calls.map(operation)).not.toContain('versions upload');
  });

  it('rejects an unbounded maintenance mutation before dispatch', async () => {
    const digest = deploymentSpecDigest(deployment);
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'versions list') {
        return {
          stdout: JSON.stringify([listedVersion('version-next', digest)]),
          stderr: '',
        };
      }
      if (command === 'versions view') {
        return { stdout: JSON.stringify(viewedVersion(digest)), stderr: '' };
      }
      if (command === 'deployments status') {
        return {
          stdout: JSON.stringify({
            versions: [{ version_id: 'version-next', percentage: 100 }],
          }),
          stderr: '',
        };
      }
      return { stdout: '', stderr: '' };
    });
    const request = vi.fn(async () => maintenanceResponse());
    const shortFence: ExternalMutationFence = {
      mutationLeaseTtlMs: 30_000,
      assertOwned: vi.fn(async () => {}),
    };
    const subject = backend(runner, {
      fetch: request,
      maintenanceRequestTimeoutMs: shortFence.mutationLeaseTtlMs,
    });

    await expect(
      subject.ensureMaintenance(
        deployment,
        secrets.maintenanceAdmin,
        shortFence,
        'version-next',
      ),
    ).rejects.toThrow(
      'maintenance request timeout must be below the external mutation fence lease TTL',
    );
    expect(request).not.toHaveBeenCalled();
    expect(shortFence.assertOwned).not.toHaveBeenCalled();
  });

  it('aborts a maintenance mutation before its lease can expire', async () => {
    const digest = deploymentSpecDigest(deployment);
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'versions list') {
        return {
          stdout: JSON.stringify([listedVersion('version-next', digest)]),
          stderr: '',
        };
      }
      if (command === 'versions view') {
        return { stdout: JSON.stringify(viewedVersion(digest)), stderr: '' };
      }
      if (command === 'deployments status') {
        return {
          stdout: JSON.stringify({
            versions: [{ version_id: 'version-next', percentage: 100 }],
          }),
          stderr: '',
        };
      }
      return { stdout: '', stderr: '' };
    });
    const request = vi.fn(
      (_input: Parameters<typeof fetch>[0], init?: RequestInit) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener(
            'abort',
            () => reject(init.signal?.reason),
            { once: true },
          );
        }),
    );
    const subject = backend(runner, {
      fetch: request,
      maintenanceRequestTimeoutMs: 5,
    });

    await expect(
      subject.ensureMaintenance(
        deployment,
        secrets.maintenanceAdmin,
        mutationFence,
        'version-next',
      ),
    ).rejects.toMatchObject({ name: 'TimeoutError' });
    expect(request).toHaveBeenCalledOnce();
  });

  it('refuses to reuse a custom domain owned by a Worker outside the promotion guard', async () => {
    const digest = deploymentSpecDigest(deployment);
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'versions list') {
        return {
          stdout: JSON.stringify([listedVersion('version-owned', digest)]),
          stderr: '',
        };
      }
      if (command === 'versions view') {
        return { stdout: JSON.stringify(viewedVersion(digest)), stderr: '' };
      }
      if (command === 'deployments status') {
        return {
          stdout: JSON.stringify({
            versions: [{ version_id: 'version-owned', percentage: 100 }],
          }),
          stderr: '',
        };
      }
      return { stdout: '', stderr: '' };
    });
    const routeApi = new FakeRouteApi([
      {
        id: 'foreign-domain',
        hostname: deployment.routeHostname,
        service: 'foreign-worker',
      },
    ]);

    await expect(
      backend(runner, { routeApi }).promoteWorker(
        deployment,
        {
          allowedCurrentScriptNames: [deployment.scriptName],
          allowUnrouted: false,
        },
        undefined,
        mutationFence,
        'version-owned',
      ),
    ).rejects.toThrow(/owned by unexpected Worker 'foreign-worker'/);
    expect(routeApi.calls).toEqual([]);
  });

  it('requires allowUnrouted before attaching an absent custom domain', async () => {
    const digest = deploymentSpecDigest(deployment);
    const runner = new FakeRunner(async (arguments_) => {
      if (arguments_[1] === 'list') {
        return {
          stdout: JSON.stringify([listedVersion('version-owned', digest)]),
          stderr: '',
        };
      }
      if (arguments_[1] === 'view') {
        return { stdout: JSON.stringify(viewedVersion(digest)), stderr: '' };
      }
      return {
        stdout: JSON.stringify({
          versions: [{ version_id: 'version-owned', percentage: 100 }],
        }),
        stderr: '',
      };
    });
    const routeApi = new FakeRouteApi();

    await expect(
      backend(runner, { routeApi }).promoteWorker(
        deployment,
        {
          allowedCurrentScriptNames: [deployment.scriptName],
          allowUnrouted: false,
        },
        undefined,
        mutationFence,
        'version-owned',
      ),
    ).rejects.toThrow(/unexpectedly absent/);
    expect(routeApi.calls).toEqual([]);
  });

  it('re-reads route ownership immediately before attach and rejects a concurrent claimant', async () => {
    const digest = deploymentSpecDigest(deployment);
    const runner = new FakeRunner(async (arguments_) => {
      if (arguments_[1] === 'list') {
        return {
          stdout: JSON.stringify([listedVersion('version-owned', digest)]),
          stderr: '',
        };
      }
      if (arguments_[1] === 'view') {
        return { stdout: JSON.stringify(viewedVersion(digest)), stderr: '' };
      }
      return {
        stdout: JSON.stringify({
          versions: [{ version_id: 'version-owned', percentage: 100 }],
        }),
        stderr: '',
      };
    });
    let reads = 0;
    const routeApi: PlainWorkerRouteApi = {
      ...databaseRouteMethods(),
      async listWorkerDatabaseAttachments() {
        return [];
      },
      async listOrdinaryWorkerSecretNames() {
        return [];
      },
      async deleteControlSecrets() {},
      async listCustomDomains() {
        reads += 1;
        return reads === 1
          ? []
          : [
              {
                id: 'racing-domain',
                hostname: deployment.routeHostname,
                service: 'racing-worker',
              },
            ];
      },
      async inspectOrdinaryWorkerFootprint() {
        return { scriptPresent: false, customDomains: [], zoneRoutes: [] };
      },
      async listDurableObjectNamespaces() {
        return [];
      },
      attachCustomDomain: vi.fn(),
      detachCustomDomain: vi.fn(),
      disableOrdinaryWorkerPublicAccess: vi.fn(),
    };

    await expect(
      backend(runner, { routeApi }).promoteWorker(
        deployment,
        {
          allowedCurrentScriptNames: [deployment.scriptName],
          allowUnrouted: true,
        },
        undefined,
        mutationFence,
        'version-owned',
      ),
    ).rejects.toThrow(/owned by unexpected Worker 'racing-worker'/);
    expect(routeApi.attachCustomDomain).not.toHaveBeenCalled();
    expect(reads).toBe(2);
  });

  it('rejects an existing Worker DO migration before uploading or changing secrets', async () => {
    const migration = {
      ...deployment,
      previousDurableObjectTag: 'v1',
      durableObjectMigrations: [
        { tag: 'v1', newSqliteClasses: ['Maintenance'] },
        { tag: 'v2', newSqliteClasses: ['Owned'] },
      ],
    } satisfies DeploymentSpec;
    const runner = new FakeRunner(async (arguments_) => {
      if (arguments_[0] === 'deployments') {
        return {
          stdout: JSON.stringify({
            versions: [{ version_id: 'version-old', percentage: 100 }],
          }),
          stderr: '',
        };
      }
      if (arguments_[0] === 'versions' && arguments_[1] === 'list') {
        return {
          stdout: JSON.stringify([
            listedVersion('version-old', 'b'.repeat(64)),
          ]),
          stderr: '',
        };
      }
      return { stdout: '', stderr: '' };
    });

    await expect(
      backend(runner).deployWorker(
        migration,
        database,
        secrets,
        undefined,
        mutationFence,
      ),
    ).rejects.toThrow(/immediate\/manual migration boundary/);
    expect(
      runner.calls.some((call) =>
        ['deploy', 'upload'].includes(
          call.arguments[1] ?? call.arguments[0] ?? '',
        ),
      ),
    ).toBe(false);
  });

  it('reports and cleans up a failed first deployment as a typed absent resource', async () => {
    const routeApi = new FakeRouteApi();
    routeApi.secretNames.clear();
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'deployments status' || command === 'versions list') {
        throw notFound('Worker');
      }
      if (arguments_[0] === 'deploy') throw new Error('upload failed');
      return { stdout: '', stderr: '' };
    });

    const result = backend(runner, { routeApi }).deployWorker(
      deployment,
      database,
      secrets,
      undefined,
      mutationFence,
    );
    await expect(result).rejects.toBeInstanceOf(WorkerDeploymentError);
    await expect(result).rejects.toMatchObject({
      createdByAttempt: true,
      resourceState: 'absent',
    });
    expect(runner.calls.map(operation)).not.toContain('delete --name');
  });

  it('does not revoke credentials when failed-deployment cleanup cannot attest ownership before mutation', async () => {
    let deploymentAttempted = false;
    const routeApi = new FakeRouteApi();
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'deployments status') {
        if (!deploymentAttempted) throw notFound('Worker');
        return {
          stdout: JSON.stringify({
            versions: [{ version_id: 'version-unresolved', percentage: 100 }],
          }),
          stderr: '',
        };
      }
      if (command === 'versions list') {
        if (!deploymentAttempted) throw notFound('Worker');
        return { stdout: JSON.stringify([]), stderr: '' };
      }
      if (arguments_[0] === 'deploy') {
        deploymentAttempted = true;
        routeApi.scriptPresent = true;
        throw new Error('connection closed before upload outcome');
      }
      return { stdout: '', stderr: '' };
    });

    await expect(
      backend(runner, { routeApi }).deployWorker(
        deployment,
        database,
        secrets,
        undefined,
        mutationFence,
      ),
    ).rejects.toMatchObject({
      createdByAttempt: true,
      resourceState: 'unknown',
    });
    expect(
      routeApi.calls.some((call) => call.operation === 'delete-secret'),
    ).toBe(false);
    expect(runner.calls.map(operation)).not.toContain('delete --name');
  });

  it('cleans a failed first deployment after secret versions leave Wrangler list output', async () => {
    const digest = deploymentSpecDigest(deployment);
    const routeApi = new FakeRouteApi();
    routeApi.secretNames.clear();
    for (let index = 1; index <= 12; index += 1) {
      routeApi.secretNames.add(`SECRET_${String(index).padStart(2, '0')}`);
    }
    let workerExists = false;
    let initialStatusFailed = false;
    let deployedVersionId = 'version-new';
    let listedVersionIds: string[] = [];
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'deployments status') {
        if (!workerExists) throw notFound('Worker');
        if (!initialStatusFailed) {
          initialStatusFailed = true;
          throw new Error('deployment status interrupted');
        }
        return {
          stdout: JSON.stringify({
            versions: [{ version_id: deployedVersionId, percentage: 100 }],
          }),
          stderr: '',
        };
      }
      if (command === 'versions list') {
        if (!workerExists) throw notFound('Worker');
        return {
          stdout: JSON.stringify(
            listedVersionIds.slice(-10).map((id) => listedVersion(id, digest)),
          ),
          stderr: '',
        };
      }
      if (command === 'versions view') {
        return { stdout: JSON.stringify(viewedVersion(digest)), stderr: '' };
      }
      if (arguments_[0] === 'deploy') {
        workerExists = true;
        routeApi.scriptPresent = true;
        routeApi.workersDevEnabled = true;
        listedVersionIds = ['version-new'];
      }
      if (arguments_[0] === 'delete') {
        workerExists = false;
        routeApi.scriptPresent = false;
      }
      return { stdout: '', stderr: '' };
    });
    let secretVersion = 1;
    routeApi.afterDeleteControlSecret = () => {
      secretVersion += 1;
      deployedVersionId = `version-secret-delete-${secretVersion}`;
      listedVersionIds = [...listedVersionIds, deployedVersionId];
    };

    await expect(
      backend(runner, { routeApi }).deployWorker(
        deployment,
        database,
        secrets,
        undefined,
        mutationFence,
      ),
    ).rejects.toMatchObject({
      createdByAttempt: true,
      resourceState: 'absent',
    });
    expect(listedVersionIds.slice(-10)).not.toContain('version-new');
    expect(
      routeApi.calls.filter((call) => call.operation === 'delete-secret'),
    ).toHaveLength(12);
    expect(runner.calls.map(operation)).toContain('delete --name');
  });

  it.each([
    ['scriptName', { scriptName: 'remote-state' }],
    ['dispatchNamespace', { dispatchNamespace: 'remote-namespace' }],
  ] as const)('rejects a platform-authored Durable Object %s target before any provider command', async (_label, remoteTarget) => {
    const runner = new FakeRunner();
    const remoteSpec: DeploymentSpec = {
      ...deployment,
      durableObjectBindings: [
        {
          name: 'MAINTENANCE',
          className: 'Maintenance',
          ...remoteTarget,
        },
      ],
    };

    await expect(
      backend(runner).deployWorker(
        remoteSpec,
        database,
        secrets,
        undefined,
        mutationFence,
      ),
    ).rejects.toThrow(/only local Durable Object bindings/);
    expect(runner.calls).toEqual([]);
  });

  it('parses deployment resources and checks authenticated maintenance health', async () => {
    let extraBindings: readonly Readonly<Record<string, unknown>>[] = [];
    const runner = new FakeRunner(async (arguments_) => {
      if (arguments_[0] === 'deployments') {
        return {
          stdout: JSON.stringify({
            versions: [
              { version_id: 'version-old', percentage: 0 },
              { version_id: 'version-live', percentage: 100 },
            ],
          }),
          stderr: '',
        };
      }
      return {
        stdout: JSON.stringify({
          resources: {
            bindings: [
              { type: 'd1', name: 'DB', database_id: database.id },
              {
                type: 'durable_object_namespace',
                name: 'MAINTENANCE',
                class_name: 'Maintenance',
                namespace_id: 'namespace-maintenance',
              },
              {
                type: 'service',
                name: 'EGRESS_PROXY',
                service: deployment.egressProxyService,
              },
              { type: 'plain_text', name: 'DEPLOYMENT_TENANT', text: 'acme' },
              {
                type: 'plain_text',
                name: 'FLEET_ENVIRONMENT',
                text: 'production',
              },
              { type: 'plain_text', name: 'FLEET_SCHEMA_VERSION', text: '3' },
              {
                type: 'plain_text',
                name: 'FLEET_SPEC_DIGEST',
                text: deploymentSpecDigest(deployment),
              },
              ...extraBindings,
            ],
          },
        }),
        stderr: '',
      };
    });
    const request = vi.fn(async () => maintenanceResponse());

    await expect(
      backend(runner, { fetch: request }).inspect(
        deployment,
        secrets.maintenanceAdmin,
      ),
    ).resolves.toEqual({
      tenantTag: 'acme',
      environment: 'production',
      scriptName: 'acme-production',
      databaseId: database.id,
      durableObjectBindings: [
        {
          name: 'MAINTENANCE',
          className: 'Maintenance',
          namespaceId: 'namespace-maintenance',
        },
      ],
      serviceBindings: [
        {
          name: 'EGRESS_PROXY',
          service: deployment.egressProxyService,
        },
      ],
      queueProducerBindings: [],
      plainTextBindings: {
        DEPLOYMENT_TENANT: deployment.tenantTag,
        FLEET_ENVIRONMENT: deployment.environment,
        FLEET_SCHEMA_VERSION: String(deployment.schemaVersion),
        FLEET_SPEC_DIGEST: deploymentSpecDigest(deployment),
      },
      secretNames: ['DEPLOYMENT_IDENTITY_SECRET', 'MAINTENANCE_ADMIN_SECRET'],
      providerBindingIdentities: [
        { type: 'd1', name: 'DB' },
        { type: 'durable_object_namespace', name: 'MAINTENANCE' },
        { type: 'plain_text', name: 'DEPLOYMENT_TENANT' },
        { type: 'plain_text', name: 'FLEET_ENVIRONMENT' },
        { type: 'plain_text', name: 'FLEET_SCHEMA_VERSION' },
        { type: 'plain_text', name: 'FLEET_SPEC_DIGEST' },
        { type: 'secret_text', name: 'DEPLOYMENT_IDENTITY_SECRET' },
        { type: 'secret_text', name: 'MAINTENANCE_ADMIN_SECRET' },
        { type: 'service', name: 'EGRESS_PROXY' },
      ],
      artifactVersion: 'version-live',
      desiredSpecDigest: deploymentSpecDigest(deployment),
      schemaVersion: 3,
      maintenance: {
        armed: true,
        nextAlarmAt: 2_000,
        deploymentSpecDigest: deploymentSpecDigest(deployment),
        lastSweepAt: 1_000,
        lastPurgeAt: null,
      },
    });
    expect(runner.calls[2]?.arguments).toEqual([
      'versions',
      'view',
      'version-live',
      '--name',
      deployment.scriptName,
      '--json',
    ]);
    expect(request).toHaveBeenCalledWith(
      new URL('https://control-acme.example.test/admin/maintenance-status'),
      {
        headers: { authorization: `Bearer ${secrets.maintenanceAdmin}` },
      },
    );
    extraBindings = [
      { type: 'kv_namespace', name: 'OUT_OF_BAND_KV', namespace_id: 'kv-id' },
    ];
    await expect(
      backend(runner, { fetch: request }).inspect(
        deployment,
        secrets.maintenanceAdmin,
      ),
    ).rejects.toThrow(/unsupported or malformed provider binding/u);
    expect(request).toHaveBeenCalledTimes(1);
  });

  it('fails closed when an expected-empty secret inventory cannot be inspected', async () => {
    const routeApi = new FakeRouteApi();
    routeApi.secretListError = new Error('secret inventory unavailable');

    await expect(
      backend(ownedWorkerRunner(), {
        routeApi,
        fetch: async () => maintenanceResponse(),
      }).inspect(deployment, secrets.maintenanceAdmin),
    ).rejects.toThrow(/secret inventory unavailable/u);
    expect(routeApi.secretListReads).toBe(1);
  });

  it.each([
    ['missing', undefined],
    ['redirected', 'foreign-egress-proxy'],
  ])('rejects a %s EGRESS_PROXY service binding', async (_label, service) => {
    const digest = deploymentSpecDigest(deployment);
    const version = viewedVersion(digest) as {
      resources: { bindings: Array<Record<string, unknown>> };
    };
    version.resources.bindings = version.resources.bindings.filter(
      (binding) => binding.type !== 'service',
    );
    if (service) {
      version.resources.bindings.push({
        type: 'service',
        name: 'EGRESS_PROXY',
        service,
      });
    }
    const runner = new FakeRunner(async (arguments_) => {
      if (arguments_[0] === 'deployments') {
        return {
          stdout: JSON.stringify({
            versions: [{ version_id: 'version-live', percentage: 100 }],
          }),
          stderr: '',
        };
      }
      return { stdout: JSON.stringify(version), stderr: '' };
    });

    await expect(
      backend(runner).inspect(deployment, secrets.maintenanceAdmin),
    ).rejects.toThrow(/different resource mapping/);
  });

  it('inspects the tagged 0% candidate through a version override and rejects silent fallback', async () => {
    const digest = deploymentSpecDigest(deployment);
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'deployments status') {
        return {
          stdout: JSON.stringify({
            versions: [
              { version_id: 'version-old', percentage: 100 },
              { version_id: 'version-next', percentage: 0 },
            ],
          }),
          stderr: '',
        };
      }
      if (command === 'versions list') {
        return {
          stdout: JSON.stringify([listedVersion('version-next', digest)]),
          stderr: '',
        };
      }
      if (command === 'versions view') {
        return { stdout: JSON.stringify(viewedVersion(digest)), stderr: '' };
      }
      return { stdout: '', stderr: '' };
    });
    const request = vi.fn(async () =>
      Response.json({
        alarmAt: 2_000,
        deploymentSpecDigest: 'b'.repeat(64),
      }),
    );

    await expect(
      backend(runner, { fetch: request }).inspect(
        deployment,
        secrets.maintenanceAdmin,
      ),
    ).rejects.toThrow(/did not attest fleet specification digest/);
    expect(request).toHaveBeenCalledWith(
      new URL('https://control-acme.example.test/admin/maintenance-status'),
      {
        headers: {
          authorization: `Bearer ${secrets.maintenanceAdmin}`,
          'Cloudflare-Workers-Version-Overrides': `${deployment.scriptName}="version-next"`,
        },
      },
    );
  });

  it.each([
    'inspect',
    'maintenance',
    'promotion',
  ] as const)('rejects a copied-tag replacement before %s can fetch credentials or mutate the provider', async (operationName) => {
    const digest = deploymentSpecDigest(deployment);
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'deployments status') {
        return {
          stdout: JSON.stringify({
            versions: [{ version_id: 'malicious-copy', percentage: 100 }],
          }),
          stderr: '',
        };
      }
      if (command === 'versions list') {
        return {
          stdout: JSON.stringify([listedVersion('malicious-copy', digest)]),
          stderr: '',
        };
      }
      return { stdout: JSON.stringify(viewedVersion(digest)), stderr: '' };
    });
    const request = vi.fn(async () => maintenanceResponse());
    const subject = backend(runner, { fetch: request });
    const result =
      operationName === 'inspect'
        ? subject.inspect(deployment, secrets.maintenanceAdmin, 'version-owned')
        : operationName === 'maintenance'
          ? subject.ensureMaintenance(
              deployment,
              secrets.maintenanceAdmin,
              mutationFence,
              'version-owned',
            )
          : subject.promoteWorker(
              deployment,
              {
                allowUnrouted: true,
                allowedCurrentScriptNames: [deployment.scriptName],
              },
              undefined,
              mutationFence,
              'version-owned',
            );

    await expect(result).rejects.toThrow(/missing persisted artifact version/);
    expect(request).not.toHaveBeenCalled();
    expect(
      runner.calls.some((call) =>
        ['deploy', 'secret'].includes(call.arguments[0] ?? ''),
      ),
    ).toBe(false);
  });

  it('reconciles ambiguous upload and 0% deployment failures by deterministic tag', async () => {
    const digest = deploymentSpecDigest(deployment);
    let versions = [listedVersion('version-old', 'b'.repeat(64))];
    let deployedVersions = [{ version_id: 'version-old', percentage: 100 }];
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'deployments status') {
        return {
          stdout: JSON.stringify({ versions: deployedVersions }),
          stderr: '',
        };
      }
      if (command === 'versions list') {
        return { stdout: JSON.stringify(versions), stderr: '' };
      }
      if (command === 'versions view') {
        return { stdout: JSON.stringify(viewedVersion(digest)), stderr: '' };
      }
      if (command === 'versions upload') {
        versions = [...versions, listedVersion('version-next', digest)];
        throw new Error('connection closed after upload');
      }
      if (command === 'versions deploy') {
        deployedVersions = [
          { version_id: 'version-old', percentage: 100 },
          { version_id: 'version-next', percentage: 0 },
        ];
        throw new Error('connection closed after deployment');
      }
      return { stdout: '', stderr: '' };
    });

    await expect(
      backend(runner).deployWorker(
        deployment,
        database,
        secrets,
        undefined,
        mutationFence,
      ),
    ).resolves.toEqual({ artifactVersion: 'version-next', created: false });

    const callsBeforeRetry = runner.calls.length;
    await expect(
      backend(runner).deployWorker(
        deployment,
        database,
        secrets,
        undefined,
        mutationFence,
        'version-next',
      ),
    ).resolves.toEqual({ artifactVersion: 'version-next', created: false });
    expect(
      runner.calls
        .slice(callsBeforeRetry)
        .some((call) => ['upload', 'deploy'].includes(call.arguments[1] ?? '')),
    ).toBe(false);
  });

  it('replaces a same-spec pre-wrapper version instead of accepting it as a candidate', async () => {
    const digest = deploymentSpecDigest(deployment);
    let versions = [listedVersion('version-unguarded', digest)];
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'deployments status') {
        return {
          stdout: JSON.stringify({
            versions: [{ version_id: 'version-unguarded', percentage: 100 }],
          }),
          stderr: '',
        };
      }
      if (command === 'versions list') {
        return { stdout: JSON.stringify(versions), stderr: '' };
      }
      if (command === 'versions view') {
        const viewed = structuredClone(viewedVersion(digest)) as {
          resources: { bindings: Array<Record<string, unknown>> };
        };
        if (arguments_[2] === 'version-unguarded') {
          viewed.resources.bindings = viewed.resources.bindings.filter(
            (binding) => binding.name !== 'FLEET_INGRESS_CONTRACT',
          );
        }
        return { stdout: JSON.stringify(viewed), stderr: '' };
      }
      if (command === 'versions upload') {
        versions = [...versions, listedVersion('version-guarded', digest)];
      }
      return { stdout: '', stderr: '' };
    });

    await expect(
      backend(runner).deployWorker(
        deployment,
        database,
        secrets,
        undefined,
        mutationFence,
      ),
    ).resolves.toEqual({
      artifactVersion: 'version-guarded',
      created: false,
    });
    expect(runner.calls.map(operation)).toContain('versions upload');
  });

  it('reads and seeds database ownership through fenced provider-native SQL', async () => {
    const sentinelDdl = `CREATE TABLE IF NOT EXISTS flowsafe_deployment (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  tenant_tag TEXT NOT NULL,
  provisioned_at TEXT NOT NULL
)`;
    let sentinelExists = false;
    let fenceExists = false;
    let owner: string | undefined;
    let fenceBindings: readonly unknown[] | undefined;
    const runner = new FakeRunner();
    const routeApi = new FakeRouteApi();
    routeApi.queryHandler = async (sql, bindings) => {
      let results: readonly Readonly<Record<string, unknown>>[] = [];
      if (sql.includes("sqlite_schema WHERE type = 'table' ORDER BY name")) {
        results = [
          ...(sentinelExists
            ? [{ name: 'flowsafe_deployment', sql: sentinelDdl }]
            : []),
          ...(fenceExists
            ? [{ name: 'flowsafe_execution_fence', sql: 'CREATE' }]
            : []),
        ];
      } else if (
        sql.includes('name = ?') &&
        bindings[0] === 'flowsafe_deployment'
      ) {
        results = sentinelExists ? [{ sql: sentinelDdl }] : [];
      } else if (sql.startsWith('PRAGMA table_info')) {
        results = [
          { name: 'id', type: 'INTEGER', notnull: 0, pk: 1 },
          { name: 'tenant_tag', type: 'TEXT', notnull: 1, pk: 0 },
          { name: 'provisioned_at', type: 'TEXT', notnull: 1, pk: 0 },
        ];
      } else if (sql.startsWith('SELECT id, tenant_tag')) {
        results = owner ? [{ id: 1, tenant_tag: owner }] : [];
      } else if (
        // Matched on the TARGET table, ahead of the generic arms: the ownership
        // insert names the fence table inside its exclusion list.
        sql.startsWith('CREATE TABLE IF NOT EXISTS flowsafe_execution_fence')
      ) {
        fenceExists = true;
      } else if (
        sql.startsWith('INSERT OR IGNORE INTO flowsafe_execution_fence')
      ) {
        fenceBindings = bindings;
      } else if (sql.startsWith('CREATE TABLE')) {
        sentinelExists = true;
      } else if (sql.startsWith('INSERT OR IGNORE')) {
        owner = String(bindings[0]);
      }
      return results;
    };
    const subject = backend(runner, { routeApi });

    await expect(
      subject.readDeploymentIdentity(database, mutationFence),
    ).resolves.toBeUndefined();
    await subject.seedDeploymentIdentity(
      database,
      'acme',
      mutationFence,
      'migration-locked',
    );
    await expect(
      subject.readDeploymentIdentity(database, mutationFence),
    ).resolves.toBe('acme');
    expect(runner.calls).toEqual([]);
    expect(routeApi.databaseQueries.length).toBeGreaterThan(0);
    expect(
      routeApi.databaseQueries.every(
        (query) => query.databaseId === database.id,
      ),
    ).toBe(true);
    // The fence rides the same fenced provider-native path, and every binding
    // reaches it as a STRING — the plain-Worker adapter rejects anything else
    // (restD1Bindings), which is why the seeded timestamp is bound as text and
    // left to SQLite's INTEGER affinity.
    expect(fenceExists).toBe(true);
    expect(fenceBindings).toEqual([
      'deployment',
      'migration-locked',
      expect.stringMatching(/^\d+$/),
    ]);
  });

  it('forwards SQLite literals, identifiers, comments, and numbered parameters unchanged', async () => {
    const routeApi = new FakeRouteApi();
    const statement = `CREATE TABLE "literal?" (
      \`backtick?\` TEXT DEFAULT 'it''s ?',
      [bracket?] TEXT
    );
    -- line-comment ?
    /* block-comment ? */`;
    let applied = false;
    routeApi.queryHandler = async (sql) => {
      if (sql.startsWith('SELECT version')) {
        return applied
          ? [
              {
                version: 1,
                sql_sha256:
                  routeApi.databaseBatches[0]?.statements[1]?.bindings?.[1],
              },
            ]
          : [];
      }
      return [];
    };
    const batchDatabase = routeApi.batchDatabase.bind(routeApi);
    routeApi.batchDatabase = async (databaseId, statements) => {
      await batchDatabase(databaseId, statements);
      applied = true;
    };

    await expect(
      backend(new FakeRunner(), { routeApi }).applyMigrations(
        database,
        [{ version: 1, sql: statement }],
        mutationFence,
      ),
    ).resolves.toBeUndefined();
    expect(routeApi.databaseBatches).toHaveLength(1);
    expect(routeApi.databaseBatches[0]?.statements[0]).toEqual({
      sql: statement,
      bindings: [],
    });
    expect(routeApi.databaseBatches[0]?.statements[1]).toMatchObject({
      sql: expect.stringContaining('VALUES (?, ?, ?)'),
      bindings: ['1', expect.any(String), expect.any(String)],
    });
  });

  it('propagates provider placeholder-arity and unsupported-name failures', async () => {
    for (const [sql, message] of [
      ['SELECT ?', 'not enough SQL bindings'],
      ['SELECT 1', 'too many SQL bindings'],
      ['SELECT :named', 'named SQLite parameters are unsupported'],
    ] as const) {
      const routeApi = new FakeRouteApi();
      routeApi.batchDatabase = async () => {
        throw new Error(message);
      };
      let failure: unknown;
      try {
        await backend(new FakeRunner(), { routeApi }).applyMigrations(
          database,
          [{ version: 1, sql }],
          mutationFence,
        );
      } catch (error) {
        failure = error;
      }
      expect(errorChain(failure)).toContain(message);
    }
  });

  it('creates a database after core authorization', async () => {
    const missingRunner = new FakeRunner(async (arguments_) => ({
      stdout:
        arguments_[1] === 'list'
          ? JSON.stringify([
              { uuid: 'database-created', name: deployment.databaseName },
            ])
          : '',
      stderr: '',
    }));
    await expect(
      backend(missingRunner).ensureDatabase(deployment, mutationFence),
    ).resolves.toEqual({
      id: 'database-created',
      name: deployment.databaseName,
      created: true,
    });
    expect(missingRunner.calls.map(operation)).toEqual([
      'd1 create',
      'd1 list',
    ]);
    expect(missingRunner.calls[0]?.arguments).toEqual([
      'd1',
      'create',
      deployment.databaseName,
    ]);
    expect(missingRunner.calls[1]?.arguments).toEqual(['d1', 'list', '--json']);
  });

  it('recovers a D1 create committed before Wrangler lost its response', async () => {
    let committed = false;
    const runner = new FakeRunner(async (arguments_) => {
      if (arguments_[1] === 'create') {
        committed = true;
        throw new Error('create response lost');
      }
      return {
        stdout: committed
          ? JSON.stringify([
              { uuid: 'database-created', name: deployment.databaseName },
            ])
          : '[]',
        stderr: '',
      };
    });

    await expect(
      backend(runner).ensureDatabase(deployment, mutationFence),
    ).resolves.toEqual({
      id: 'database-created',
      name: deployment.databaseName,
      created: true,
    });
    expect(runner.calls.map(operation)).toEqual(['d1 create', 'd1 list']);
  });

  it('rejects an authorized D1 create race that resolves to another owner', async () => {
    const runner = new FakeRunner(async (arguments_) => {
      if (arguments_[1] === 'create') throw new Error('D1 name conflict');
      return {
        stdout: JSON.stringify([
          { uuid: 'database-foreign', name: deployment.databaseName },
        ]),
        stderr: '',
      };
    });
    const subject = backend(runner);
    subject.readDeploymentIdentity = async () => 'other-tenant';

    await expect(
      subject.ensureDatabase(deployment, mutationFence),
    ).rejects.toThrow(/owned by 'other-tenant'/);
    expect(runner.calls.map(operation)).toEqual(['d1 create', 'd1 list']);
  });

  it('fails the external mutation fence immediately before spawning Wrangler', async () => {
    const runner = new FakeRunner(async () => ({
      stdout: '[]',
      stderr: '',
    }));
    const deniedFence: ExternalMutationFence = {
      mutationLeaseTtlMs: 15 * 60_000,
      assertOwned: vi.fn(async () => {
        throw new Error('lease lost');
      }),
    };

    await expect(
      backend(runner).ensureDatabase(deployment, deniedFence),
    ).rejects.toThrow('lease lost');
    expect(runner.calls.map(operation)).toEqual([]);
    expect(deniedFence.assertOwned).toHaveBeenCalledTimes(1);
  });

  it('rejects a Wrangler mutation whose maximum duration can outlive the lease', async () => {
    const runner = new FakeRunner(
      async () => ({ stdout: '[]', stderr: '' }),
      mutationFence.mutationLeaseTtlMs,
    );
    const fence: ExternalMutationFence = {
      mutationLeaseTtlMs: mutationFence.mutationLeaseTtlMs,
      assertOwned: vi.fn(async () => {}),
    };

    await expect(
      backend(runner).ensureDatabase(deployment, fence),
    ).rejects.toThrow(
      'Wrangler command maximum duration must be below the external mutation fence lease TTL',
    );
    expect(runner.calls.map(operation)).toEqual([]);
    expect(fence.assertOwned).not.toHaveBeenCalled();
  });

  it('reads a database by immutable id through the REST route API and treats a missing id as absent', async () => {
    const runner = new FakeRunner();
    const routeApi = new FakeRouteApi();
    const subject = backend(runner, { routeApi });
    await expect(subject.getDatabase(database.id)).resolves.toEqual({
      ...database,
      created: false,
    });
    routeApi.databasePresent = false;
    await expect(subject.getDatabase(database.id)).resolves.toBeUndefined();
    expect(runner.calls).toEqual([]);
  });

  it('returns undefined when the inspected Worker is missing', async () => {
    const runner = new FakeRunner(async () => {
      throw notFound('Worker');
    });
    const request = vi.fn(async () => maintenanceResponse());

    await expect(
      backend(runner, { fetch: request }).inspect(
        deployment,
        secrets.maintenanceAdmin,
      ),
    ).resolves.toBeUndefined();
    expect(request).not.toHaveBeenCalled();
  });

  it('streams a mode-0600 temporary export to durable storage and removes the scratch file', async () => {
    const exportDirectory = await mkdtemp(join(tmpdir(), 'fleet-export-test-'));
    const content = 'CREATE TABLE exported(id TEXT);';
    let temporaryLocation: string | undefined;
    let stored = '';
    const exportStore: DurableDatabaseExportStore = {
      async write(input) {
        if (!temporaryLocation) throw new Error('missing temporary export');
        expect((await stat(temporaryLocation)).mode & 0o777).toBe(0o600);
        const chunks: Uint8Array[] = [];
        const reader = input.body.getReader();
        for (;;) {
          const chunk = await reader.read();
          if (chunk.done) break;
          chunks.push(chunk.value);
        }
        stored = Buffer.concat(chunks).toString('utf8');
        return {
          location: 'r2://fleet-exports/acme-production.sql',
          size: Buffer.byteLength(stored),
          sha256: createHash('sha256').update(stored).digest('hex'),
        };
      },
    };
    try {
      const runner = new FakeRunner(async (arguments_) => {
        const outputIndex = arguments_.indexOf('--output');
        const location = arguments_[outputIndex + 1];
        if (!location) throw new Error('export output is missing');
        temporaryLocation = location;
        await writeFile(location, content);
        return { stdout: '', stderr: '' };
      });

      const exported = await backend(runner, {
        exportDirectory,
        exportStore,
      }).exportDatabase(database, mutationFence);
      expect(exported).toEqual({
        databaseId: database.id,
        location: 'r2://fleet-exports/acme-production.sql',
        sha256: createHash('sha256').update(content).digest('hex'),
        size: Buffer.byteLength(content),
      });
      expect(stored).toBe(content);
      if (!temporaryLocation) throw new Error('missing temporary export');
      await expect(stat(temporaryLocation)).rejects.toThrow();
    } finally {
      await rm(exportDirectory, { recursive: true, force: true });
    }
  });

  it('propagates an export command failure without claiming an artifact', async () => {
    const exportDirectory = await mkdtemp(join(tmpdir(), 'fleet-export-test-'));
    try {
      const runner = new FakeRunner(async () => {
        throw new Error('wrangler export failed');
      });
      await expect(
        backend(runner, { exportDirectory }).exportDatabase(
          database,
          mutationFence,
        ),
      ).rejects.toThrow('wrangler export failed');
    } finally {
      await rm(exportDirectory, { recursive: true, force: true });
    }
  });

  it.each([
    'integrity-mismatch',
    'write-failure',
  ] as const)('does not claim a durable export after %s and removes its ephemeral file', async (failure) => {
    const exportDirectory = await mkdtemp(join(tmpdir(), 'fleet-export-test-'));
    let temporaryLocation: string | undefined;
    const exportStore: DurableDatabaseExportStore = {
      async write(input) {
        const reader = input.body.getReader();
        while (!(await reader.read()).done) {}
        if (failure === 'write-failure') {
          throw new Error('durable export write failed');
        }
        return {
          location: 'r2://fleet-exports/mismatched.sql',
          size: 1,
          sha256: '0'.repeat(64),
        };
      },
    };
    try {
      const runner = new FakeRunner(async (arguments_) => {
        const outputIndex = arguments_.indexOf('--output');
        const location = arguments_[outputIndex + 1];
        if (!location) throw new Error('export output is missing');
        temporaryLocation = location;
        await writeFile(location, 'CREATE TABLE durable(id TEXT);');
        return { stdout: '', stderr: '' };
      });

      await expect(
        backend(runner, { exportDirectory, exportStore }).exportDatabase(
          database,
          mutationFence,
        ),
      ).rejects.toThrow(
        failure === 'write-failure'
          ? /durable export write failed/
          : /mismatched committed integrity/,
      );
      if (!temporaryLocation) throw new Error('missing temporary export');
      await expect(stat(temporaryLocation)).rejects.toThrow();
    } finally {
      await rm(exportDirectory, { recursive: true, force: true });
    }
  });

  it('treats already-missing credentials and Workers as deleted', async () => {
    const runner = new FakeRunner(async () => {
      throw notFound();
    });
    const routeApi = new FakeRouteApi();
    routeApi.secretNames.clear();
    const subject = backend(runner, { routeApi });

    await expect(revokeDeploymentCredentials(subject)).resolves.toBeUndefined();
    await expect(deleteDeploymentWorker(subject)).resolves.toBeUndefined();
    await expect(
      subject.deleteDatabase(database, mutationFence),
    ).resolves.toBeUndefined();
    expect(runner.calls.map(operation)).toEqual([
      'deployments status',
      'versions list',
      'deployments status',
      'versions list',
    ]);
    expect(routeApi.calls).toContainEqual({
      operation: 'delete-database',
      databaseId: database.id,
    });
  });

  it('deletes D1 by immutable id through the fenced REST route API without spawning Wrangler', async () => {
    const runner = new FakeRunner();
    const routeApi = new FakeRouteApi();
    const fence: ExternalMutationFence = {
      mutationLeaseTtlMs: mutationFence.mutationLeaseTtlMs,
      assertOwned: vi.fn(async () => {}),
    };

    await expect(
      backend(runner, { routeApi }).deleteDatabase(database, fence),
    ).resolves.toBeUndefined();

    expect(routeApi.calls).toEqual([
      { operation: 'delete-database', databaseId: database.id },
    ]);
    expect(routeApi.databasePresent).toBe(false);
    expect(fence.assertOwned).toHaveBeenCalledTimes(1);
    expect(runner.calls).toEqual([]);
  });

  it('force-tears down ordinary ingress, secrets, and exact-id D1 without a specification or artifact read', async () => {
    const runner = new FakeRunner(() => {
      throw new Error('force decommission must not spawn Wrangler');
    });
    const routeApi = new FakeRouteApi([
      {
        id: 'domain-acme',
        hostname: deployment.routeHostname,
        service: deployment.scriptName,
      },
    ]);
    routeApi.scriptPresent = true;
    routeApi.workersDevEnabled = true;
    routeApi.previewUrlsEnabled = true;
    const subject = backend(runner, { routeApi });

    await subject.forceDecommissionStep(
      fleetRecord,
      'remove-traffic',
      mutationFence,
    );
    await subject.forceDecommissionStep(
      fleetRecord,
      'revoke-credentials',
      mutationFence,
    );
    await subject.forceDecommissionStep(
      fleetRecord,
      'delete-database',
      mutationFence,
    );

    expect(routeApi.calls).toEqual([
      { operation: 'detach', domainId: 'domain-acme' },
      {
        operation: 'delete-secret',
        scriptName: deployment.scriptName,
        secretName: 'DEPLOYMENT_IDENTITY_SECRET',
      },
      {
        operation: 'delete-secret',
        scriptName: deployment.scriptName,
        secretName: 'MAINTENANCE_ADMIN_SECRET',
      },
      { operation: 'delete-database', databaseId: database.id },
    ]);
    expect(routeApi.workersDevEnabled).toBe(false);
    expect(routeApi.previewUrlsEnabled).toBe(false);
    expect(routeApi.secretNames.size).toBe(0);
    expect(routeApi.databasePresent).toBe(false);
    expect(runner.calls).toEqual([]);
  });

  it('refuses force deletion when the exact D1 id resolves to another fleet name', async () => {
    const runner = new FakeRunner();
    const routeApi = new FakeRouteApi();
    routeApi.getDatabase = async () => ({
      id: database.id,
      name: 'foreign-database',
      created: false,
    });

    await expect(
      backend(runner, { routeApi }).forceDecommissionStep(
        fleetRecord,
        'delete-database',
        mutationFence,
      ),
    ).rejects.toThrow(/resolved with unexpected identity/);
    expect(routeApi.calls).toEqual([]);
    expect(runner.calls).toEqual([]);
  });

  it('replays force D1 deletion as success after the exact id is absent', async () => {
    const runner = new FakeRunner();
    const routeApi = new FakeRouteApi();
    const subject = backend(runner, { routeApi });

    await subject.forceDecommissionStep(
      fleetRecord,
      'delete-database',
      mutationFence,
    );
    await expect(
      subject.forceDecommissionStep(
        fleetRecord,
        'delete-database',
        mutationFence,
      ),
    ).resolves.toBeUndefined();

    expect(routeApi.calls).toEqual([
      { operation: 'delete-database', databaseId: database.id },
    ]);
    expect(runner.calls).toEqual([]);
  });

  it.each([
    'getDatabase',
    'deleteDatabase',
  ] as const)('fails force D1 deletion without route API %s and never falls back to Wrangler', async (missingCapability) => {
    const runner = new FakeRunner(() => {
      throw new Error('force D1 deletion must not spawn Wrangler');
    });
    const fakeRouteApi = new FakeRouteApi();
    const routeApi = new Proxy(fakeRouteApi, {
      get(target, property) {
        if (property === missingCapability) return undefined;
        const value = Reflect.get(target, property, target);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });

    await expect(
      backend(runner, { routeApi }).forceDecommissionStep(
        fleetRecord,
        'delete-database',
        mutationFence,
      ),
    ).rejects.toThrow(/does not support exact-ID D1 database deletion/u);
    expect(routeApi.calls).toEqual([]);
    expect(runner.calls).toEqual([]);
  });

  it('fails force decommission closed when ordinary ingress remains enabled', async () => {
    const runner = new FakeRunner();
    const routeApi = new FakeRouteApi();
    routeApi.scriptPresent = true;
    routeApi.zoneRoutes = [
      { zoneId: 'zone-acme', routeId: 'route-acme', pattern: 'acme.test/*' },
    ];

    await expect(
      backend(runner, { routeApi }).forceDecommissionStep(
        fleetRecord,
        'remove-traffic',
        mutationFence,
      ),
    ).rejects.toThrow(/retains public ingress/);
    expect(runner.calls).toEqual([]);
  });

  it('rejects a REST deletion that does not make the immutable D1 id absent', async () => {
    const runner = new FakeRunner();
    const routeApi = new FakeRouteApi();
    routeApi.deleteDatabase = async (databaseId: string) => {
      routeApi.calls.push({ operation: 'delete-database', databaseId });
    };

    await expect(
      backend(runner, { routeApi }).deleteDatabase(database, mutationFence),
    ).rejects.toThrow(`database '${database.id}' remains after deletion`);
    expect(runner.calls).toEqual([]);
  });

  it('fails closed when the REST route API cannot delete D1', async () => {
    const runner = new FakeRunner();
    const fakeRouteApi = new FakeRouteApi();
    const routeApi = new Proxy(fakeRouteApi, {
      get(target, property) {
        if (property === 'deleteDatabase') return undefined;
        const value = Reflect.get(target, property, target);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });

    await expect(
      backend(runner, { routeApi }).deleteDatabase(database, mutationFence),
    ).rejects.toThrow(/does not support exact-ID D1 database deletion/u);
    expect(runner.calls).toEqual([]);
  });

  it('attests exact Worker ownership while removing traffic before credential revocation', async () => {
    const drifted = viewedVersion(deploymentSpecDigest(deployment), {
      ...database,
      id: 'foreign-database',
    });
    const runner = ownedWorkerRunner({ version: drifted });

    await expect(removeDeploymentTraffic(backend(runner))).rejects.toThrow(
      /drifted tenant, environment, specification, or D1/,
    );

    const mismatchedDigest = ownedWorkerRunner({
      version: viewedVersion('b'.repeat(64)),
    });
    await expect(
      removeDeploymentTraffic(backend(mismatchedDigest)),
    ).rejects.toThrow(/mismatched fleet specification digest/);
  });

  it('revokes credentials through the REST route API without spawning Wrangler secret commands', async () => {
    const routeApi = new FakeRouteApi();
    routeApi.scriptPresent = true;
    const runner = ownedWorkerRunner();

    await expect(
      revokeDeploymentCredentials(backend(runner, { routeApi })),
    ).resolves.toBeUndefined();

    expect(routeApi.calls).toEqual([
      {
        operation: 'delete-secret',
        scriptName: deployment.scriptName,
        secretName: 'DEPLOYMENT_IDENTITY_SECRET',
      },
      {
        operation: 'delete-secret',
        scriptName: deployment.scriptName,
        secretName: 'MAINTENANCE_ADMIN_SECRET',
      },
    ]);
    expect(runner.calls.some((call) => call.arguments[0] === 'secret')).toBe(
      false,
    );
  });

  it('deletes an attested Worker after secret versions push its persisted anchor out of Wrangler list output', async () => {
    const digest = deploymentSpecDigest(deployment);
    const routeApi = new FakeRouteApi([
      {
        id: 'owned-domain',
        hostname: deployment.routeHostname,
        service: deployment.scriptName,
      },
    ]);
    routeApi.scriptPresent = true;
    routeApi.workersDevEnabled = true;
    routeApi.previewUrlsEnabled = true;
    routeApi.secretNames.clear();
    for (let index = 1; index <= 12; index += 1) {
      routeApi.secretNames.add(`SECRET_${String(index).padStart(2, '0')}`);
    }
    let workerExists = true;
    let deployedVersionIds = ['version-owned'];
    let listedVersionIds = ['version-owned'];
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'deployments status') {
        if (!workerExists) throw notFound('Worker');
        return {
          stdout: JSON.stringify({
            versions: deployedVersionIds.map((version_id) => ({
              version_id,
              percentage: version_id === deployedVersionIds.at(-1) ? 100 : 0,
            })),
          }),
          stderr: '',
        };
      }
      if (command === 'versions list') {
        if (!workerExists) throw notFound('Worker');
        return {
          stdout: JSON.stringify(
            listedVersionIds.slice(-10).map((id) => listedVersion(id, digest)),
          ),
          stderr: '',
        };
      }
      if (command === 'versions view') {
        return { stdout: JSON.stringify(viewedVersion(digest)), stderr: '' };
      }
      if (arguments_[0] === 'delete') {
        workerExists = false;
        routeApi.scriptPresent = false;
      }
      return { stdout: '', stderr: '' };
    });
    let secretVersion = 1;
    routeApi.afterDeleteControlSecret = () => {
      secretVersion += 1;
      const id = `version-secret-delete-${secretVersion}`;
      deployedVersionIds = [id];
      listedVersionIds = [...listedVersionIds, id];
    };
    const subject = backend(runner, { routeApi });

    await subject.removeTraffic(
      deployment,
      undefined,
      activeRelease,
      database,
      mutationFence,
    );
    const callsBeforeRevocation = runner.calls.length;
    await subject.revokeCredentials(
      deployment,
      undefined,
      activeRelease,
      database,
      mutationFence,
    );
    expect(runner.calls.length).toBeGreaterThan(callsBeforeRevocation);

    expect(deployedVersionIds).toEqual(['version-secret-delete-13']);
    expect(listedVersionIds).toHaveLength(13);
    expect(listedVersionIds.slice(-10)).not.toContain('version-owned');
    await expect(
      subject.deleteWorker(
        deployment,
        undefined,
        database,
        activeRelease,
        mutationFence,
      ),
    ).resolves.toBeUndefined();

    expect(
      runner.calls.filter(
        (call) =>
          call.arguments.slice(0, 3).join(' ') ===
          'versions view version-owned',
      ).length,
    ).toBeGreaterThan(1);
    expect(
      routeApi.calls.filter((call) => call.operation === 'delete-secret'),
    ).toHaveLength(12);
    expect(runner.calls.map(operation)).toContain('delete --name');
  });

  it('accepts an exact deployed anchor excluded by newer undeployed list entries', async () => {
    const digest = deploymentSpecDigest(deployment);
    const routeApi = new FakeRouteApi();
    routeApi.scriptPresent = true;
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'deployments status') {
        return {
          stdout: JSON.stringify({
            versions: [{ version_id: 'version-owned', percentage: 100 }],
          }),
          stderr: '',
        };
      }
      if (command === 'versions list') {
        return {
          stdout: JSON.stringify(
            Array.from({ length: 10 }, (_, index) =>
              listedVersion(`version-newer-${index}`, digest),
            ),
          ),
          stderr: '',
        };
      }
      if (command === 'versions view') {
        return { stdout: JSON.stringify(viewedVersion(digest)), stderr: '' };
      }
      return { stdout: '', stderr: '' };
    });

    await expect(
      backend(runner, { routeApi }).revokeCredentials(
        deployment,
        undefined,
        activeRelease,
        database,
        mutationFence,
      ),
    ).resolves.toBeUndefined();
    expect(
      routeApi.calls.filter((call) => call.operation === 'delete-secret'),
    ).toHaveLength(2);
  });

  it('refuses secret revocation when the Worker is replaced after the pre-mutation gate', async () => {
    const digest = deploymentSpecDigest(deployment);
    const routeApi = new FakeRouteApi([
      {
        id: 'owned-domain',
        hostname: deployment.routeHostname,
        service: deployment.scriptName,
      },
    ]);
    routeApi.scriptPresent = true;
    routeApi.workersDevEnabled = true;
    let deployedVersionId = 'version-owned';
    let currentVersion = viewedVersion(digest);
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'deployments status') {
        return {
          stdout: JSON.stringify({
            versions: [{ version_id: deployedVersionId, percentage: 100 }],
          }),
          stderr: '',
        };
      }
      if (command === 'versions list') {
        return {
          stdout: JSON.stringify([
            listedVersion('version-owned', digest),
            ...(deployedVersionId === 'version-owned'
              ? []
              : [listedVersion(deployedVersionId, digest)]),
          ]),
          stderr: '',
        };
      }
      if (command === 'versions view') {
        return { stdout: JSON.stringify(currentVersion), stderr: '' };
      }
      return { stdout: '', stderr: '' };
    });
    const subject = backend(runner, { routeApi });

    await subject.removeTraffic(
      deployment,
      undefined,
      activeRelease,
      database,
      mutationFence,
    );
    deployedVersionId = 'version-foreign';
    currentVersion = viewedVersion(digest, {
      ...database,
      id: 'foreign-database',
    });

    await expect(
      subject.revokeCredentials(
        deployment,
        undefined,
        activeRelease,
        database,
        mutationFence,
      ),
    ).rejects.toThrow(/drifted live teardown ownership/u);
    expect(
      routeApi.calls.some((call) => call.operation === 'delete-secret'),
    ).toBe(false);
  });

  it('refuses deletion when a credentials-revoked Worker is replaced immediately before live attestation', async () => {
    const digest = deploymentSpecDigest(deployment);
    const routeApi = new FakeRouteApi();
    routeApi.scriptPresent = true;
    let deployedVersionId = 'version-owned';
    let currentVersion = viewedVersion(digest);
    routeApi.beforeListCustomDomains = () => {
      deployedVersionId = 'version-foreign';
      currentVersion = viewedVersion(digest, {
        ...database,
        id: 'foreign-database',
      });
      routeApi.beforeListCustomDomains = undefined;
    };
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'deployments status') {
        return {
          stdout: JSON.stringify({
            versions: [{ version_id: deployedVersionId, percentage: 100 }],
          }),
          stderr: '',
        };
      }
      if (command === 'versions list') {
        return {
          stdout: JSON.stringify([
            listedVersion('version-owned', digest),
            listedVersion(deployedVersionId, digest),
          ]),
          stderr: '',
        };
      }
      if (command === 'versions view') {
        return { stdout: JSON.stringify(currentVersion), stderr: '' };
      }
      return { stdout: '', stderr: '' };
    });

    await expect(
      backend(runner, { routeApi }).deleteWorker(
        deployment,
        undefined,
        database,
        activeRelease,
        mutationFence,
      ),
    ).rejects.toThrow(/drifted live teardown ownership/u);
    expect(runner.calls.map(operation)).not.toContain('delete --name');
  });

  it('refuses a same-name recreated Worker even when it copies the persisted identity bindings', async () => {
    const digest = deploymentSpecDigest(deployment);
    const routeApi = new FakeRouteApi();
    routeApi.scriptPresent = true;
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'deployments status') {
        return {
          stdout: JSON.stringify({
            versions: [{ version_id: 'version-recreated', percentage: 100 }],
          }),
          stderr: '',
        };
      }
      if (command === 'versions list') {
        return {
          stdout: JSON.stringify([listedVersion('version-recreated', digest)]),
          stderr: '',
        };
      }
      if (command === 'versions view') {
        if (arguments_[2] === 'version-owned') throw notFound('version');
        return { stdout: JSON.stringify(viewedVersion(digest)), stderr: '' };
      }
      return { stdout: '', stderr: '' };
    });

    await expect(
      backend(runner, { routeApi }).revokeCredentials(
        deployment,
        undefined,
        activeRelease,
        database,
        mutationFence,
      ),
    ).rejects.toThrow(/trusted artifact version anchor/u);
    expect(
      routeApi.calls.some((call) => call.operation === 'delete-secret'),
    ).toBe(false);
  });

  it('verifies Worker absence when the provider footprint reports no script', async () => {
    const routeApi = new FakeRouteApi();
    const runner = ownedWorkerRunner();

    await expect(
      deleteDeploymentWorker(backend(runner, { routeApi })),
    ).rejects.toThrow(/remains after its footprint reported absence/u);
    expect(runner.calls.map(operation)).not.toContain('delete --name');
  });

  it('refuses to advance credential revocation when the provider leaves any ordinary Worker secret', async () => {
    const routeApi = new FakeRouteApi();
    routeApi.scriptPresent = true;
    routeApi.secretRevocationNoop = true;
    const runner = ownedWorkerRunner();

    await expect(
      revokeDeploymentCredentials(backend(runner, { routeApi })),
    ).rejects.toThrow(/failed exact secret revocation/);
    expect(
      runner.calls.filter((call) => call.arguments[0] === 'secret'),
    ).toHaveLength(0);
    expect(
      routeApi.calls.filter((call) => call.operation === 'delete-secret'),
    ).toHaveLength(2);
  });

  it('detaches only the attested custom domain and proves Worker and route absence', async () => {
    const routeApi = new FakeRouteApi([
      {
        id: 'owned-domain',
        hostname: deployment.routeHostname,
        service: deployment.scriptName,
      },
    ]);
    routeApi.scriptPresent = true;
    routeApi.workersDevEnabled = true;
    routeApi.previewUrlsEnabled = true;
    const runner = ownedWorkerRunner({
      onDelete: () => {
        routeApi.scriptPresent = false;
      },
    });

    const subject = backend(runner, { routeApi });
    await expect(removeDeploymentTraffic(subject)).resolves.toBeUndefined();
    await expect(deleteDeploymentWorker(subject)).resolves.toBeUndefined();
    expect(routeApi.calls).toEqual([
      { operation: 'detach', domainId: 'owned-domain' },
    ]);
    expect(routeApi.listCalls).toBeGreaterThanOrEqual(4);
    expect(routeApi.workersDevEnabled).toBe(false);
    expect(routeApi.previewUrlsEnabled).toBe(false);
    expect(runner.calls.some((call) => call.arguments[0] === 'delete')).toBe(
      true,
    );
  });

  it('blocks deletion when a zone route appears after traffic removal', async () => {
    const routeApi = new FakeRouteApi([
      {
        id: 'owned-domain',
        hostname: deployment.routeHostname,
        service: deployment.scriptName,
      },
    ]);
    routeApi.scriptPresent = true;
    const runner = ownedWorkerRunner();
    const subject = backend(runner, { routeApi });
    await removeDeploymentTraffic(subject);
    routeApi.zoneRoutes.push({
      zoneId: 'zone-id',
      routeId: 'late-route',
      pattern: 'late.example.test/*',
    });

    await expect(deleteDeploymentWorker(subject)).rejects.toThrow(
      /retains public ingress/u,
    );
    expect(runner.calls.some((call) => call.arguments[0] === 'delete')).toBe(
      false,
    );
    expect(routeApi.scriptPresent).toBe(true);
  });

  it('fails deletion when the Worker or route survives the mutation readback', async () => {
    const routeApi = new FakeRouteApi();
    routeApi.scriptPresent = true;
    const runner = ownedWorkerRunner({ survivesDelete: true });

    await expect(
      deleteDeploymentWorker(backend(runner, { routeApi })),
    ).rejects.toThrow(/remains after delete/);

    const stickyRoute: PlainWorkerRouteApi = {
      ...databaseRouteMethods(),
      async listWorkerDatabaseAttachments() {
        return [];
      },
      async listOrdinaryWorkerSecretNames() {
        return [];
      },
      async deleteControlSecrets() {},
      async listCustomDomains() {
        return [
          {
            id: 'sticky-domain',
            hostname: deployment.routeHostname,
            service: deployment.scriptName,
          },
        ];
      },
      async inspectOrdinaryWorkerFootprint() {
        return {
          scriptPresent: true,
          customDomains: [
            {
              id: 'sticky-domain',
              hostname: deployment.routeHostname,
              service: deployment.scriptName,
            },
          ],
          zoneRoutes: [],
        };
      },
      async listDurableObjectNamespaces() {
        return [];
      },
      attachCustomDomain: vi.fn(),
      detachCustomDomain: vi.fn(),
      disableOrdinaryWorkerPublicAccess: vi.fn(),
    };
    const stickyRunner = ownedWorkerRunner();
    const stickySubject = backend(stickyRunner, { routeApi: stickyRoute });
    await expect(
      removeDeploymentTraffic(stickySubject),
    ).resolves.toBeUndefined();
    await expect(deleteDeploymentWorker(stickySubject)).rejects.toThrow(
      /retains public ingress/,
    );
    expect(
      stickyRunner.calls.some((call) => call.arguments[0] === 'delete'),
    ).toBe(false);
  });

  it('blocks D1 teardown until the deleted Worker has no Durable Object namespaces', async () => {
    const routeApi = new FakeRouteApi();
    routeApi.scriptPresent = true;
    routeApi.namespaceIds.add('namespace-maintenance');
    const runner = ownedWorkerRunner({
      onDelete: () => {
        routeApi.scriptPresent = false;
      },
    });
    const subject = backend(runner, { routeApi });

    await expect(deleteDeploymentWorker(subject)).rejects.toThrow(
      /remains after delete/,
    );
    await expect(assertDeploymentDatabaseDetached(subject)).rejects.toThrow(
      /Durable Object namespace footprint/,
    );

    routeApi.namespaceIds.clear();
    await expect(
      assertDeploymentDatabaseDetached(subject),
    ).resolves.toBeUndefined();
  });

  it('refuses deletion when the hostname or another domain has unexpected ownership', async () => {
    const foreignHostname = new FakeRouteApi([
      {
        id: 'foreign-domain',
        hostname: deployment.routeHostname,
        service: 'foreign-worker',
      },
    ]);
    await expect(
      removeDeploymentTraffic(
        backend(ownedWorkerRunner(), {
          routeApi: foreignHostname,
        }),
      ),
    ).rejects.toThrow(/owned by Worker 'foreign-worker'/);

    const extraDomain = new FakeRouteApi([
      {
        id: 'extra-domain',
        hostname: 'extra.example.test',
        service: deployment.scriptName,
      },
    ]);
    extraDomain.scriptPresent = true;
    const extraDomainSubject = backend(ownedWorkerRunner(), {
      routeApi: extraDomain,
    });
    await expect(
      removeDeploymentTraffic(extraDomainSubject),
    ).resolves.toBeUndefined();
    await expect(deleteDeploymentWorker(extraDomainSubject)).rejects.toThrow(
      /retains public ingress/,
    );
  });

  it('authoritatively attests an absent Worker and both route surfaces before database mutation', async () => {
    const runner = new FakeRunner(async () => {
      throw notFound('Worker');
    });
    const routeApi = new FakeRouteApi();

    await expect(
      assertDeploymentDatabaseDetached(backend(runner, { routeApi })),
    ).resolves.toBeUndefined();
  });

  it('rejects an unrelated account Worker attachment before database mutation', async () => {
    const runner = new FakeRunner(async () => {
      throw notFound('Worker');
    });
    const routeApi = new FakeRouteApi();
    routeApi.databaseAttachments = [
      { scriptName: 'unregistered-rogue-worker', plane: 'ordinary' },
    ];

    await expect(
      assertDeploymentDatabaseDetached(backend(runner, { routeApi })),
    ).rejects.toThrow(
      /remains attached to ordinary Worker 'unregistered-rogue-worker'/,
    );
    expect(runner.calls).toEqual([]);
  });

  it('rejects an owned or mismatched Worker footprint before database mutation', async () => {
    const ownedRoutes = new FakeRouteApi();
    ownedRoutes.scriptPresent = true;
    await expect(
      assertDeploymentDatabaseDetached(
        backend(ownedWorkerRunner(), {
          routeApi: ownedRoutes,
        }),
      ),
    ).rejects.toThrow(/remains attached to owned Worker/);

    const driftedRoutes = new FakeRouteApi();
    driftedRoutes.scriptPresent = true;
    await expect(
      assertDeploymentDatabaseDetached(
        backend(
          ownedWorkerRunner({
            version: viewedVersion(deploymentSpecDigest(deployment), {
              ...database,
              id: 'foreign-database',
            }),
          }),
          { routeApi: driftedRoutes },
        ),
      ),
    ).rejects.toThrow(/foreign or mismatched Worker footprint/);
  });

  it('rejects lingering custom domains and traditional zone routes', async () => {
    const absentRunner = new FakeRunner(async () => {
      throw notFound('Worker');
    });
    const foreignDomain = new FakeRouteApi([
      {
        id: 'foreign-domain',
        hostname: deployment.routeHostname,
        service: 'foreign-worker',
      },
    ]);
    await expect(
      assertDeploymentDatabaseDetached(
        backend(absentRunner, { routeApi: foreignDomain }),
      ),
    ).rejects.toThrow(/residual route or Durable Object namespace footprint/);

    const zoneRoute = new FakeRouteApi();
    zoneRoute.zoneRoutes = [
      {
        zoneId: 'zone-acme',
        routeId: 'route-acme',
        pattern: `${deployment.routeHostname}/*`,
      },
    ];
    await expect(
      assertDeploymentDatabaseDetached(
        backend(absentRunner, { routeApi: zoneRoute }),
      ),
    ).rejects.toThrow(/residual route or Durable Object namespace footprint/);
  });

  it('converges after a lost Worker delete response before database mutation', async () => {
    const routeApi = new FakeRouteApi();
    routeApi.scriptPresent = true;
    const runner = ownedWorkerRunner({
      deleteError: new Error('connection lost after delete'),
      onDelete: () => {
        routeApi.scriptPresent = false;
      },
    });
    const subject = backend(runner, { routeApi });

    await expect(deleteDeploymentWorker(subject)).rejects.toThrow(
      /connection lost after delete/,
    );
    await expect(
      assertDeploymentDatabaseDetached(subject),
    ).resolves.toBeUndefined();
  });

  it('propagates deletion failures that are not missing-resource errors', async () => {
    const runner = new FakeRunner(async () => {
      throw new Error('wrangler authentication failed');
    });

    await expect(deleteDeploymentWorker(backend(runner))).rejects.toThrow(
      'wrangler authentication failed',
    );
  });
});
