// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';
import type { DurableDatabaseExportStore } from '../src/cloudflare-client.js';
import { WorkerDeploymentError } from '../src/deployment-error.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import type {
  DatabaseReference,
  DeploymentSecrets,
  DeploymentSpec,
  FleetRecord,
  PlainWorkerRouteApi,
} from '../src/types.js';
import { WranglerLoopBackend } from '../src/wrangler-loop-backend.js';
import type { CommandResult, CommandRunner } from '../src/wrangler-runner.js';
import {
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
  cleanupError: new Error('adapter cleanup failed'),
}));

vi.mock('node:fs/promises', async () => {
  const { createFsPromisesMock } = await import(
    './fixtures/wrangler-fs-mock.js'
  );
  return createFsPromisesMock(fsControl);
});

const exportDirectories = registerScratchCleanup(fsControl, {
  cleanupError: fsControl.cleanupError,
});

const spec: DeploymentSpec = {
  tenantTag: 'acme',
  environment: 'production',
  scriptName: 'acme-production',
  databaseName: 'acme-production',
  compatibilityDate: '2026-08-10',
  compatibilityFlags: ['nodejs_compat'],
  mainModule: 'worker.js',
  modules: [{ name: 'worker.js', content: 'export default { fetch() {} }' }],
  authoredBy: 'platform',
  schemaVersion: 3,
  migrations: [],
  durableObjectMigrations: [],
  durableObjectBindings: [],
  maintenanceBaseUrl: 'https://control.example.test',
  routeHostname: 'app.example.test',
};

const database: DatabaseReference = {
  id: 'database-id',
  name: spec.databaseName,
  created: true,
};

const secrets: DeploymentSecrets = {
  deploymentIdentity: 'deployment-identity-secret-value-0001',
  maintenanceAdmin: 'maintenance-admin-secret-value-00001',
};

const fleetRecord: FleetRecord = {
  tenantTag: spec.tenantTag,
  backend: 'plain-worker',
  environment: spec.environment,
  scriptName: spec.scriptName,
  databaseId: database.id,
  databaseName: database.name,
  schemaVersion: spec.schemaVersion,
  artifactVersion: 'candidate',
  desiredSpecDigest: deploymentSpecDigest(spec),
  durableObjectBindings: [],
  routeHostname: spec.routeHostname,
  phase: 'worker-deleted',
  updatedAt: '2026-08-11T00:00:00.000Z',
};

class FakeRunner implements CommandRunner {
  readonly maxDurationMs = 5 * 60_000;
  readonly mutableCalls: string[][] = [];
  readonly calls: readonly string[][] = this.mutableCalls;

  constructor(
    readonly handler: (arguments_: readonly string[]) => Promise<CommandResult>,
  ) {}

  run(arguments_: readonly string[]): Promise<CommandResult> {
    this.mutableCalls.push([...arguments_]);
    return this.handler(arguments_);
  }
}

async function backend(
  runner: CommandRunner,
  options: {
    readonly routeApi?: PlainWorkerRouteApi;
    readonly exportStore?: DurableDatabaseExportStore;
  } = {},
): Promise<WranglerLoopBackend> {
  const exportDirectory = await mkdtemp(join(tmpdir(), 'backend-export-'));
  exportDirectories.add(exportDirectory);
  return new WranglerLoopBackend({
    runner,
    routeApi: options.routeApi ?? routeApi(),
    exportDirectory,
    exportStore: options.exportStore ?? memoryStore(),
  });
}

function notFound(message: string): never {
  throw new Error(`${message} not found`);
}

async function rejectedValue(operation: Promise<unknown>): Promise<unknown> {
  try {
    await operation;
  } catch (error) {
    return error;
  }
  throw new Error('expected operation to reject');
}

function uploadRunner({
  dispatchFails = false,
  dispatchFailure,
  commitBeforeFailure = true,
}: {
  readonly dispatchFails?: boolean;
  readonly dispatchFailure?: unknown;
  readonly commitBeforeFailure?: boolean;
} = {}): FakeRunner {
  let uploaded = false;
  const digest = deploymentSpecDigest(spec);
  return new FakeRunner(async (arguments_) => {
    const command = arguments_.slice(0, 2).join(' ');
    if (command === 'deployments status') {
      if (!uploaded) return notFound('deployment');
      return {
        stdout: JSON.stringify({
          versions: [{ id: 'candidate', percentage: 100 }],
        }),
        stderr: '',
      };
    }
    if (command === 'versions list') {
      if (!uploaded) return notFound('script');
      return {
        stdout: JSON.stringify([
          { id: 'candidate', annotations: { 'workers/tag': digest } },
        ]),
        stderr: '',
      };
    }
    if (command === 'versions view') {
      return {
        stdout: JSON.stringify({
          id: 'candidate',
          resources: {
            bindings: [
              { type: 'plain_text', name: 'FLEET_SPEC_DIGEST', text: digest },
              {
                type: 'plain_text',
                name: 'FLEET_INGRESS_CONTRACT',
                text: 'guarded-object-v1',
              },
            ],
          },
        }),
        stderr: '',
      };
    }
    if (arguments_[0] === 'deploy') {
      if (dispatchFails) {
        if (commitBeforeFailure) uploaded = true;
        throw dispatchFailure;
      }
      uploaded = true;
      return { stdout: 'ignored', stderr: '' };
    }
    throw new Error(`unexpected command ${arguments_.join(' ')}`);
  });
}

describe('WranglerLoopBackend provisioning port contract', () => {
  it('catches a truncating durable export store end to end', async () => {
    const runner = new FakeRunner(async (arguments_) => {
      await writeFile(
        arguments_[arguments_.indexOf('--output') + 1] as string,
        'complete export',
      );
      return { stdout: '', stderr: '' };
    });
    const exportStore: DurableDatabaseExportStore = {
      async write(input) {
        const reader = input.body.getReader();
        const first = await reader.read();
        const prefix = first.done ? new Uint8Array() : first.value.slice(0, 2);
        await reader.cancel();
        return {
          location: 'memory://prefix',
          size: prefix.byteLength,
          sha256: createHash('sha256').update(prefix).digest('hex'),
        };
      },
    };
    await expect(
      (await backend(runner, { exportStore })).exportDatabase(
        database,
        mutationFence(),
      ),
    ).rejects.toThrow(
      'durable database export store returned mismatched committed integrity',
    );
  });

  it('reconciles a failed upload by tag rediscovery', async () => {
    const dispatchFailure = new Error('dispatch result unknown');
    const runner = uploadRunner({ dispatchFails: true, dispatchFailure });
    await expect(
      (await backend(runner)).deployWorker(
        spec,
        database,
        secrets,
        undefined,
        mutationFence(),
      ),
    ).resolves.toEqual({ artifactVersion: 'candidate', created: true });
  });

  it('propagates a rejected upload without rollback calls', async () => {
    const runner = uploadRunner();
    const denied = new Error('lease lost before dispatch');
    await expect(
      (await backend(runner)).deployWorker(
        spec,
        database,
        secrets,
        undefined,
        mutationFence(
          vi.fn(async () => {
            throw denied;
          }),
        ),
      ),
    ).rejects.toBe(denied);
    expect(runner.calls.map((arguments_) => arguments_.slice(0, 2))).toEqual([
      ['deployments', 'status'],
      ['versions', 'list'],
    ]);
  });

  it('does no adapter scratch work for a persisted candidate no-op', async () => {
    fsControl.failFleetCleanup = true;
    fsControl.mkdtempCalls = 0;
    const digest = deploymentSpecDigest(spec);
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'deployments status') {
        return {
          stdout: JSON.stringify({
            versions: [{ id: 'candidate', percentage: 100 }],
          }),
          stderr: '',
        };
      }
      if (command === 'versions list') {
        return {
          stdout: JSON.stringify([
            { id: 'candidate', annotations: { 'workers/tag': digest } },
          ]),
          stderr: '',
        };
      }
      if (command === 'versions view') {
        return {
          stdout: JSON.stringify({
            id: 'candidate',
            resources: {
              bindings: [
                { type: 'd1', name: 'DB', id: database.id },
                {
                  type: 'plain_text',
                  name: 'DEPLOYMENT_TENANT',
                  text: spec.tenantTag,
                },
                {
                  type: 'plain_text',
                  name: 'FLEET_ENVIRONMENT',
                  text: spec.environment,
                },
                {
                  type: 'plain_text',
                  name: 'FLEET_SPEC_DIGEST',
                  text: digest,
                },
                {
                  type: 'plain_text',
                  name: 'FLEET_INGRESS_CONTRACT',
                  text: 'guarded-object-v1',
                },
              ],
            },
          }),
          stderr: '',
        };
      }
      throw new Error(`unexpected command ${arguments_.join(' ')}`);
    });

    await expect(
      (await backend(runner)).deployWorker(
        spec,
        database,
        secrets,
        undefined,
        mutationFence(),
        'candidate',
      ),
    ).resolves.toEqual({ artifactVersion: 'candidate', created: false });
    expect(fsControl.mkdtempCalls).toBe(0);
    expect(fsControl.residualDirectory).toBeUndefined();
    expect(runner.calls.some((arguments_) => arguments_[0] === 'deploy')).toBe(
      false,
    );
  });

  it('rejects a scratch allocation failure after provider reads and before dispatch', async () => {
    const allocationFailure = new Error('scratch allocation failed');
    const events: string[] = [];
    fsControl.failOperation = 'mkdtemp';
    fsControl.operationError = allocationFailure;
    fsControl.mkdtempCalls = 0;
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'deployments status') {
        events.push('status read');
        return notFound('deployment');
      }
      if (command === 'versions list') {
        events.push('version read');
        return notFound('script');
      }
      throw new Error(`unexpected command ${arguments_.join(' ')}`);
    });
    const operation = (await backend(runner))
      .deployWorker(spec, database, secrets, undefined, mutationFence())
      .catch((error: unknown) => {
        events.push('rejection');
        throw error;
      });

    await expect(operation).rejects.toBe(allocationFailure);
    expect(events).toEqual(['status read', 'version read', 'rejection']);
    expect(fsControl.mkdtempCalls).toBe(1);
    expect(runner.calls.map((arguments_) => arguments_.slice(0, 2))).toEqual([
      ['deployments', 'status'],
      ['versions', 'list'],
    ]);
  });

  it('does not status-read back a pre-dispatch createDeployment rejection', async () => {
    const digest = deploymentSpecDigest(spec);
    let uploaded = false;
    let statusReads = 0;
    const runner = new FakeRunner(async (arguments_) => {
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'deployments status') {
        statusReads += 1;
        return {
          stdout: JSON.stringify({
            versions: [{ id: 'current', percentage: 100 }],
          }),
          stderr: '',
        };
      }
      if (command === 'versions list') {
        return {
          stdout: JSON.stringify([
            { id: 'current' },
            ...(uploaded
              ? [
                  {
                    id: 'candidate',
                    annotations: { 'workers/tag': digest },
                  },
                ]
              : []),
          ]),
          stderr: '',
        };
      }
      if (command === 'versions view') {
        const versionId = arguments_[2];
        return {
          stdout: JSON.stringify({
            id: versionId,
            resources: {
              bindings: [
                { type: 'd1', name: 'DB', id: database.id },
                {
                  type: 'plain_text',
                  name: 'DEPLOYMENT_TENANT',
                  text: spec.tenantTag,
                },
                {
                  type: 'plain_text',
                  name: 'FLEET_ENVIRONMENT',
                  text: spec.environment,
                },
                {
                  type: 'plain_text',
                  name: 'FLEET_SPEC_DIGEST',
                  text: digest,
                },
                {
                  type: 'plain_text',
                  name: 'FLEET_INGRESS_CONTRACT',
                  text: 'guarded-object-v1',
                },
              ],
            },
          }),
          stderr: '',
        };
      }
      if (command === 'versions upload') {
        uploaded = true;
        return { stdout: '', stderr: '' };
      }
      throw new Error(`unexpected command ${arguments_.join(' ')}`);
    });
    const denied = new Error('lease lost before deployment dispatch');
    let assertions = 0;
    const operation = (await backend(runner)).deployWorker(
      spec,
      database,
      secrets,
      undefined,
      mutationFence(
        vi.fn(async () => {
          assertions += 1;
          if (assertions === 2) throw denied;
        }),
      ),
    );
    await expect(operation).rejects.toBeInstanceOf(WorkerDeploymentError);
    await expect(operation).rejects.toMatchObject({ cause: denied });
    expect(statusReads).toBe(2);
    expect(
      runner.calls.some(
        (arguments_) =>
          arguments_[0] === 'versions' && arguments_[1] === 'deploy',
      ),
    ).toBe(false);
  });

  it('classifies cleanup failure after reconciliation for caller-owned rollback', async () => {
    fsControl.failFleetCleanup = true;
    const runner = uploadRunner();
    const rejection = await rejectedValue(
      (await backend(runner)).deployWorker(
        spec,
        database,
        secrets,
        undefined,
        mutationFence(),
      ),
    );
    expect(rejection).toEqual(
      expect.objectContaining({
        message: `installed Worker '${spec.scriptName}' but failed to clean up the adapter credential scratch: adapter cleanup failed`,
        createdByAttempt: true,
        resourceState: 'present',
      }),
    );
    expect((rejection as Error).cause).toBe(fsControl.cleanupError);
    expect(
      runner.calls.some(
        (arguments_) =>
          arguments_[0] === 'versions' && arguments_[1] === 'view',
      ),
    ).toBe(true);
    expect(runner.calls.some((arguments_) => arguments_[0] === 'delete')).toBe(
      false,
    );
    expect(fsControl.residualDirectory).toBeDefined();
  });

  it('wraps an undefined adapter cleanup rejection after backend reconciliation', async () => {
    fsControl.failFleetCleanup = true;
    fsControl.cleanupError = undefined;
    const rejection = await rejectedValue(
      (await backend(uploadRunner())).deployWorker(
        spec,
        database,
        secrets,
        undefined,
        mutationFence(),
      ),
    );
    expect(rejection).toEqual(
      expect.objectContaining({
        message: `installed Worker '${spec.scriptName}' but failed to clean up the adapter credential scratch`,
        createdByAttempt: true,
        resourceState: 'present',
      }),
    );
    expect((rejection as Error).cause).toBeUndefined();
  });

  it('uses rediscovery failure when a dispatched upload rejects with undefined', async () => {
    const rejection = await rejectedValue(
      (
        await backend(
          uploadRunner({
            dispatchFails: true,
            dispatchFailure: undefined,
            commitBeforeFailure: false,
          }),
        )
      ).deployWorker(spec, database, secrets, undefined, mutationFence()),
    );
    expect(rejection).toBeInstanceOf(WorkerDeploymentError);
    if (!(rejection instanceof WorkerDeploymentError)) return;
    expect(rejection.cause).toBeInstanceOf(Error);
    if (!(rejection.cause instanceof Error)) return;
    expect(rejection.cause.message).toContain(
      'did not create exactly one new tagged Worker version',
    );
  });

  it('preserves WorkerDeploymentError metadata and cause order through adapter cleanup aggregation', async () => {
    const dispatchFailure = new Error('dispatch failed before commit');
    const withoutCleanup = await rejectedValue(
      (
        await backend(
          uploadRunner({
            dispatchFails: true,
            dispatchFailure,
            commitBeforeFailure: false,
          }),
        )
      ).deployWorker(spec, database, secrets, undefined, mutationFence()),
    );
    expect(withoutCleanup).toBeInstanceOf(WorkerDeploymentError);
    if (!(withoutCleanup instanceof WorkerDeploymentError)) return;
    expect(withoutCleanup.cause).toBe(dispatchFailure);
    expect(withoutCleanup.message).toBe(
      "failed to install Worker 'acme-production': dispatch failed before commit",
    );

    fsControl.failFleetCleanup = true;
    const cleanupFailure = fsControl.cleanupError;
    const withCleanup = await rejectedValue(
      (
        await backend(
          uploadRunner({
            dispatchFails: true,
            dispatchFailure,
            commitBeforeFailure: false,
          }),
        )
      ).deployWorker(spec, database, secrets, undefined, mutationFence()),
    );
    expect(withCleanup).toBeInstanceOf(WorkerDeploymentError);
    if (!(withCleanup instanceof WorkerDeploymentError)) return;
    expect(withCleanup.createdByAttempt).toBe(withoutCleanup.createdByAttempt);
    expect(withCleanup.resourceState).toBe(withoutCleanup.resourceState);
    expect(withCleanup.cause).toBeInstanceOf(AggregateError);
    if (!(withCleanup.cause instanceof AggregateError)) return;
    expect(withCleanup.message).toBe(
      "failed to install Worker 'acme-production': Worker upload and adapter scratch cleanup both failed",
    );
    expect(withCleanup.message).not.toContain(': :');
    const errors = withCleanup.cause.errors;
    expect(errors).toHaveLength(2);
    expect(errors[0]).toBe(dispatchFailure);
    expect(errors[1]).toBe(cleanupFailure);
  });

  it.each([
    [
      'missing version id',
      'plain Worker deployment status has an invalid version',
      { versions: [{ percentage: 10 }] },
    ],
    [
      'empty inventory',
      'plain Worker deployment status has no versions',
      { versions: [] },
    ],
    [
      'negative percentage',
      'plain Worker deployment status has an invalid version',
      { versions: [{ id: 'v1', percentage: -1 }] },
    ],
    [
      'omitted percentage',
      'plain Worker deployment status has an invalid version',
      { versions: [{ id: 'v1' }] },
    ],
    [
      'non-numeric percentage',
      'plain Worker deployment status has an invalid version',
      { versions: [{ id: 'v1', percentage: 'NaN' }] },
    ],
  ])('keeps the %s backend deployment-status refusal', async (_title, message, status) => {
    const runner = new FakeRunner(async () => ({
      stdout: JSON.stringify(status),
      stderr: '',
    }));
    await expect(
      (await backend(runner)).inspect(spec, secrets.maintenanceAdmin),
    ).rejects.toThrow(message);
  });

  it('name-reconciles a dispatched D1 create failure', async () => {
    const dispatchFailure = new Error('name conflict');
    const runner = new FakeRunner(async (arguments_) => {
      if (arguments_[1] === 'create') throw dispatchFailure;
      return {
        stdout: JSON.stringify([{ uuid: database.id, name: database.name }]),
        stderr: '',
      };
    });
    const subject = await backend(runner);
    subject.readDeploymentIdentity = async () => undefined;
    await expect(
      subject.ensureDatabase(spec, mutationFence()),
    ).resolves.toEqual({
      ...database,
      created: true,
    });
  });

  it('refuses a matching D1 list row whose uuid is empty', async () => {
    const runner = new FakeRunner(async () => ({
      stdout: JSON.stringify([{ uuid: '', name: spec.databaseName }]),
      stderr: '',
    }));
    await expect((await backend(runner)).findDatabase(spec)).rejects.toThrow(
      'D1 list result has no uuid',
    );
  });

  it('runs force-decommission D1 read, identity, deletion, and verification in one fenced scope', async () => {
    const events: string[] = [];
    let present = true;
    const route = routeApi({
      async withMutationFence(fence, operation) {
        events.push('scope');
        await fence.assertOwned();
        return operation();
      },
      async getDatabase() {
        events.push('read');
        return present ? { ...database, created: false } : undefined;
      },
      async deleteDatabase() {
        events.push('delete');
        present = false;
      },
    });
    const owned = vi.fn(async () => {});
    await (
      await backend(new FakeRunner(async () => ({ stdout: '', stderr: '' })), {
        routeApi: route,
      })
    ).forceDecommissionStep(
      fleetRecord,
      'delete-database',
      mutationFence(owned),
    );
    expect(events).toEqual(['scope', 'read', 'delete', 'read']);
    expect(owned).toHaveBeenCalledTimes(1);
  });
});
