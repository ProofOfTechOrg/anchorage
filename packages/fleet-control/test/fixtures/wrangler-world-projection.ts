// SPDX-License-Identifier: Apache-2.0

import { readdir, readFile, stat, writeFile } from 'node:fs/promises';
import { basename, dirname, join, relative } from 'node:path';
import type { WorkerModule } from '../../src/types.js';
import type {
  CommandResult,
  CommandRunner,
} from '../../src/wrangler-runner.js';
import type { ProviderFailure, ProviderWorld } from './provider-world.js';

const dynamicArgument = Symbol('dynamicArgument');
type ExpectedArgument = string | typeof dynamicArgument;

class WranglerArgvContractError extends Error {
  declare readonly received: readonly string[];

  constructor(
    command: string,
    expected: readonly ExpectedArgument[],
    actual: readonly string[],
  ) {
    super(
      `Wrangler argv contract for ${command}: expected ${JSON.stringify(
        expected.map((value) =>
          value === dynamicArgument ? '<dynamic>' : value,
        ),
      )}`,
    );
    this.name = 'WranglerArgvContractError';
    Object.defineProperty(this, 'received', {
      value: [...actual],
      enumerable: false,
    });
  }
}

function assertArgv(
  command: string,
  actual: readonly string[],
  expected: readonly ExpectedArgument[],
): void {
  if (
    actual.length !== expected.length ||
    expected.some(
      (value, index) => value !== dynamicArgument && value !== actual[index],
    )
  ) {
    throw new WranglerArgvContractError(command, expected, actual);
  }
}

function assertDeploymentArgv(arguments_: readonly string[]): void {
  const nameIndex = arguments_.indexOf('--name');
  const versions = arguments_.slice(2, nameIndex);
  if (
    arguments_[0] !== 'versions' ||
    arguments_[1] !== 'deploy' ||
    nameIndex < 3 ||
    versions.some((value) => !/^[^@]+@\d+(?:\.\d+)?%$/u.test(value)) ||
    arguments_.length !== nameIndex + 3 ||
    arguments_[nameIndex + 2] !== '-y'
  ) {
    throw new WranglerArgvContractError(
      'versions deploy',
      ['versions', 'deploy', dynamicArgument, '--name', dynamicArgument, '-y'],
      arguments_,
    );
  }
}

function readField(value: unknown, name: string): unknown {
  return value && typeof value === 'object'
    ? Reflect.get(value, name)
    : undefined;
}

function readString(value: unknown, name: string): string | undefined {
  const candidate = readField(value, name);
  return typeof candidate === 'string' ? candidate : undefined;
}

function readBoolean(value: unknown, name: string): boolean {
  return readField(value, name) === true;
}

function readArray(value: unknown, name: string): readonly unknown[] {
  const candidate = readField(value, name);
  return Array.isArray(candidate) ? candidate : [];
}

function objectEntries(value: unknown): Array<readonly [string, unknown]> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return [];
  return Reflect.ownKeys(value).flatMap((key) =>
    typeof key === 'string' ? [[key, Reflect.get(value, key)] as const] : [],
  );
}

function json(value: unknown): CommandResult {
  return { stdout: JSON.stringify(value), stderr: '' };
}

function versionViewBindings(bindings: readonly unknown[]): readonly unknown[] {
  return bindings.map((binding) => {
    const cloned = structuredClone(binding);
    if (
      !cloned ||
      typeof cloned !== 'object' ||
      Array.isArray(cloned) ||
      Reflect.get(cloned, 'type') !== 'secret_text'
    ) {
      return cloned;
    }
    return { name: Reflect.get(cloned, 'name'), type: 'secret_text' };
  });
}

function success(): CommandResult {
  return { stdout: '', stderr: '' };
}

function argumentAfter(
  arguments_: readonly string[],
  flag: string,
): string | undefined {
  const index = arguments_.indexOf(flag);
  return index < 0 ? undefined : arguments_[index + 1];
}

function injectedFailure(operation: string): Error {
  return new Error(`injected Wrangler failure for ${operation}`);
}

async function settleFailure(
  world: ProviderWorld,
  operation: string,
  failure: ProviderFailure | undefined,
): Promise<void> {
  if (failure?.response) {
    throw new Error(
      `ProviderFailure.response is REST-only; the CLI projection cannot settle ${operation}`,
    );
  }
  await world.applyAfter(operation);
  if (failure) throw failure.error ?? injectedFailure(operation);
}

async function stagedModules(
  configPath: string,
): Promise<readonly WorkerModule[]> {
  const directory = dirname(configPath);
  const entries = await readdir(directory, {
    recursive: true,
    encoding: 'utf8',
  });
  const modules: WorkerModule[] = [];
  for (const entry of entries) {
    if (
      basename(entry) === 'wrangler.candidate.json' ||
      basename(entry) === 'wrangler.secrets.json'
    ) {
      continue;
    }
    const path = join(directory, entry);
    if (!(await stat(path)).isFile()) continue;
    modules.push({
      name: relative(directory, path),
      content: await readFile(path, 'utf8'),
    });
  }
  return modules.sort((left, right) => left.name.localeCompare(right.name));
}

async function configUpload(arguments_: readonly string[]): Promise<{
  readonly scriptName: string;
  readonly tag: string | undefined;
  readonly mainModule: string;
  readonly modules: readonly WorkerModule[];
  readonly bindings: readonly unknown[];
  readonly publicAccess: {
    readonly workersDevEnabled: boolean;
    readonly previewUrlsEnabled: boolean;
  };
}> {
  const configPath = argumentAfter(arguments_, '--config');
  const secretsPath = argumentAfter(arguments_, '--secrets-file');
  if (!configPath || !secretsPath) {
    throw new Error('Wrangler projection requires config and secrets paths');
  }
  const config: unknown = JSON.parse(await readFile(configPath, 'utf8'));
  const secrets: unknown = JSON.parse(await readFile(secretsPath, 'utf8'));
  const scriptName = readString(config, 'name');
  const mainModule = readString(config, 'main');
  if (!scriptName || !mainModule) {
    throw new Error('Wrangler candidate config has no name or main module');
  }
  const bindings: unknown[] = [];
  for (const [name, value] of objectEntries(readField(config, 'vars'))) {
    bindings.push({ name, type: 'plain_text', text: String(value) });
  }
  for (const [name, value] of objectEntries(secrets)) {
    bindings.push({ name, type: 'secret_text', text: String(value) });
  }
  for (const binding of readArray(config, 'd1_databases')) {
    bindings.push({
      name: readString(binding, 'binding'),
      type: 'd1',
      database_id: readString(binding, 'database_id'),
    });
  }
  for (const binding of readArray(
    readField(config, 'durable_objects'),
    'bindings',
  )) {
    bindings.push({
      name: readString(binding, 'name'),
      type: 'durable_object_namespace',
      class_name: readString(binding, 'class_name'),
    });
  }
  for (const binding of readArray(config, 'services')) {
    bindings.push({
      name: readString(binding, 'binding'),
      type: 'service',
      service: readString(binding, 'service'),
    });
  }
  for (const binding of readArray(readField(config, 'queues'), 'producers')) {
    bindings.push({
      name: readString(binding, 'binding'),
      type: 'queue',
      queue_name: readString(binding, 'queue'),
    });
  }
  for (const binding of readArray(config, 'r2_buckets')) {
    bindings.push({
      name: readString(binding, 'binding'),
      type: 'r2_bucket',
      bucket_name: readString(binding, 'bucket_name'),
    });
  }
  return {
    scriptName,
    tag: argumentAfter(arguments_, '--tag'),
    mainModule,
    modules: await stagedModules(configPath),
    bindings,
    publicAccess: {
      workersDevEnabled: readBoolean(config, 'workers_dev'),
      previewUrlsEnabled: readBoolean(config, 'preview_urls'),
    },
  };
}

export function cliProjection(world: ProviderWorld): CommandRunner {
  return {
    maxDurationMs: 5 * 60_000,
    async run(arguments_) {
      if (arguments_[0] === 'd1' && arguments_[1] === 'list') {
        assertArgv('d1 list', arguments_, ['d1', 'list', '--json']);
        return json(
          world.databases.map(({ databaseId, name }) => ({
            uuid: databaseId,
            name,
          })),
        );
      }
      if (arguments_[0] === 'd1' && arguments_[1] === 'info') {
        assertArgv('d1 info', arguments_, [
          'd1',
          'info',
          dynamicArgument,
          '--json',
        ]);
        const databaseId = arguments_[2] ?? '';
        const database = world.databases.find(
          (candidate) => candidate.databaseId === databaseId,
        );
        if (!database) throw new Error(`D1 database '${databaseId}' not found`);
        return json({ uuid: database.databaseId, name: database.name });
      }
      if (arguments_[0] === 'd1' && arguments_[1] === 'create') {
        assertArgv('d1 create', arguments_, ['d1', 'create', dynamicArgument]);
        const name = arguments_[2] ?? '';
        if (world.databases.some((database) => database.name === name)) {
          throw new Error(`D1 database '${name}' already exists`);
        }
        const failure = world.consumeFailure('createDatabase');
        if (!failure || failure.dispatched) {
          world.createDatabase(name);
        }
        await settleFailure(world, 'createDatabase', failure);
        return success();
      }
      if (arguments_[0] === 'deployments' && arguments_[1] === 'status') {
        assertArgv('deployments status', arguments_, [
          'deployments',
          'status',
          '--name',
          dynamicArgument,
          '--json',
        ]);
        const scriptName = argumentAfter(arguments_, '--name') ?? '';
        const script = world.scripts.get(scriptName);
        if (!script?.present || !script.deployment) {
          throw new Error(`Worker '${scriptName}' has no deployments`);
        }
        await world.applyAfter('deploymentStatus');
        return json({
          versions: script.deployment.map(({ versionId, percentage }) => ({
            version_id: versionId,
            percentage,
          })),
        });
      }
      if (arguments_[0] === 'versions' && arguments_[1] === 'list') {
        assertArgv('versions list', arguments_, [
          'versions',
          'list',
          '--name',
          dynamicArgument,
          '--json',
        ]);
        const scriptName = argumentAfter(arguments_, '--name') ?? '';
        const script = world.scripts.get(scriptName);
        if (!script?.present) {
          throw new Error(`Worker '${scriptName}' does not exist`);
        }
        await world.applyAfter('listVersions');
        return json(
          script.versions.map(({ versionId, tag }) => ({
            id: versionId,
            annotations: tag === undefined ? undefined : { 'workers/tag': tag },
          })),
        );
      }
      if (arguments_[0] === 'versions' && arguments_[1] === 'view') {
        assertArgv('versions view', arguments_, [
          'versions',
          'view',
          dynamicArgument,
          '--name',
          dynamicArgument,
          '--json',
        ]);
        const scriptName = argumentAfter(arguments_, '--name') ?? '';
        const versionId = arguments_[2] ?? '';
        const script = world.scripts.get(scriptName);
        const version = script?.present
          ? script.versions.find(
              (candidate) => candidate.versionId === versionId,
            )
          : undefined;
        if (!version) {
          throw new Error(`Worker version '${versionId}' not found`);
        }
        await world.applyAfter('viewVersion');
        return json({
          id: version.versionId,
          annotations:
            version.tag === undefined
              ? undefined
              : { 'workers/tag': version.tag },
          resources: { bindings: versionViewBindings(version.bindings) },
        });
      }
      if (
        arguments_[0] === 'deploy' ||
        (arguments_[0] === 'versions' && arguments_[1] === 'upload')
      ) {
        assertArgv(
          arguments_[0] === 'deploy' ? 'deploy' : 'versions upload',
          arguments_,
          [
            ...(arguments_[0] === 'deploy'
              ? ['deploy']
              : ['versions', 'upload']),
            '--config',
            dynamicArgument,
            '--secrets-file',
            dynamicArgument,
            '--tag',
            dynamicArgument,
          ],
        );
        const failure = world.consumeFailure('uploadCandidate');
        if (!failure || failure.dispatched) {
          const { publicAccess, ...upload } = await configUpload(arguments_);
          const initial = arguments_[0] === 'deploy';
          world.applyUpload(
            {
              ...upload,
              mode: initial ? 'initial' : 'staged',
              ...(initial ? { publicAccess } : {}),
            },
            { duplicate: failure?.duplicate },
          );
        }
        await settleFailure(world, 'uploadCandidate', failure);
        return success();
      }
      if (arguments_[0] === 'versions' && arguments_[1] === 'deploy') {
        assertDeploymentArgv(arguments_);
        const scriptName = argumentAfter(arguments_, '--name') ?? '';
        const script = world.scripts.get(scriptName);
        if (!script?.present)
          throw new Error(`Worker '${scriptName}' not found`);
        const deployment = arguments_
          .slice(2, arguments_.indexOf('--name'))
          .map((entry) => {
            const match = entry.match(/^(.+)@(.+)%$/u);
            return {
              versionId: match?.[1] ?? '',
              percentage: Number(match?.[2]),
            };
          });
        const operation =
          deployment.length === 1 && deployment[0]?.percentage === 100
            ? 'promoteWorker'
            : 'deployCandidate';
        const failure = world.consumeFailure(operation);
        if (!failure || failure.dispatched)
          world.applyDeployment(scriptName, deployment);
        await settleFailure(world, operation, failure);
        return success();
      }
      if (arguments_[0] === 'delete') {
        assertArgv('delete', arguments_, [
          'delete',
          '--name',
          dynamicArgument,
          '--force',
        ]);
        const scriptName = argumentAfter(arguments_, '--name') ?? '';
        const script = world.scripts.get(scriptName);
        if (!script?.present)
          throw new Error(`Worker '${scriptName}' not found`);
        const failure = world.consumeFailure('deleteWorkerScript');
        if (!failure || failure.dispatched) world.deleteScript(scriptName);
        await settleFailure(world, 'deleteWorkerScript', failure);
        return success();
      }
      if (arguments_[0] === 'd1' && arguments_[1] === 'export') {
        assertArgv('d1 export', arguments_, [
          'd1',
          'export',
          dynamicArgument,
          '--remote',
          '--skip-confirmation',
          '--output',
          dynamicArgument,
        ]);
        const databaseId = arguments_[2] ?? '';
        const output = argumentAfter(arguments_, '--output');
        if (!output) throw new Error('Wrangler D1 export has no output path');
        const failure = world.consumeFailure('exportDatabase');
        if (!failure || failure.dispatched) {
          const bytes = world.exports.get(databaseId);
          if (!bytes) throw new Error(`D1 database '${databaseId}' not found`);
          await writeFile(output, bytes);
          world.mutationLog.push(`export:${databaseId}`);
        }
        await settleFailure(world, 'exportDatabase', failure);
        return success();
      }
      throw new WranglerArgvContractError('unknown command', [], arguments_);
    },
  };
}
