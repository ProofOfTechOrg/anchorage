// SPDX-License-Identifier: Apache-2.0

import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { lstat, mkdir, open, rename, rmdir, unlink } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import PQueue from 'p-queue';
import { deriveDirectConformanceNames } from './direct-credentialed-conformance-config.mjs';
import {
  DIRECT_REFERENCE_BODY_LIMIT,
  readDirectReferenceRequest,
} from './direct-reference-contract.mjs';

const ERROR_CODES = new Set([
  'invalid-state',
  'run-exists',
  'run-missing',
  'lock-unavailable',
  'outcome-unknown',
  'invocation-budget-exhausted',
]);
const SUMMARY_FIELDS = [
  'kind',
  'role',
  'slot',
  'operation',
  'release',
  'limit',
  'afterOrdinal',
];
const MAX_JOURNAL_BYTES = 16 * 1024;

export class DirectRunStateError extends Error {
  constructor(code = 'invalid-state') {
    const accepted = ERROR_CODES.has(code) ? code : 'invalid-state';
    super(accepted);
    this.name = 'DirectRunStateError';
    this.code = accepted;
  }
}

function invalid() {
  throw new DirectRunStateError();
}

function stateError(error) {
  return error instanceof DirectRunStateError
    ? error
    : new DirectRunStateError();
}

function object(value, keys) {
  if (
    !value ||
    typeof value !== 'object' ||
    Array.isArray(value) ||
    Object.keys(value).length !== keys.length ||
    keys.some((key) => !Object.hasOwn(value, key))
  )
    invalid();
  return value;
}

function digest(value) {
  if (typeof value !== 'string' || !/^[a-f0-9]{64}$/u.test(value)) invalid();
  return value;
}

function bindingFromInput(input) {
  const accountId = input.accountId;
  if (
    typeof accountId !== 'string' ||
    !accountId ||
    accountId !== accountId.trim() ||
    accountId.length > 128 ||
    [...accountId].some(
      (character) =>
        character.charCodeAt(0) <= 31 || character.charCodeAt(0) === 127,
    )
  )
    invalid();
  const resourcePrefix = input.prepared.config.resourcePrefix;
  const maxInvocations = input.prepared.config.referenceWorker.maxInvocations;
  if (
    typeof resourcePrefix !== 'string' ||
    !/^fc[a-f0-9]{24}$/u.test(resourcePrefix) ||
    !Number.isSafeInteger(maxInvocations) ||
    maxInvocations < 1
  )
    invalid();
  return Object.freeze({
    accountId,
    configSha256: digest(input.prepared.configSha256),
    referenceModuleSetSha256: digest(input.prepared.referenceModuleSetSha256),
    resourcePrefix,
    maxInvocations,
  });
}

function actionSummary(action) {
  return Object.freeze(
    Object.fromEntries(
      SUMMARY_FIELDS.filter((key) => Object.hasOwn(action, key)).map((key) => [
        key,
        action[key],
      ]),
    ),
  );
}

async function decodeRequest(serialized, configSha256) {
  if (
    typeof serialized !== 'string' ||
    Buffer.byteLength(serialized) > DIRECT_REFERENCE_BODY_LIMIT
  )
    invalid();
  try {
    return await readDirectReferenceRequest(
      new Request('https://direct-conformance.invalid/', {
        method: 'POST',
        body: serialized,
      }),
      configSha256,
    );
  } catch {
    invalid();
  }
}

async function decodeSnapshot(value, binding) {
  const keys = ['version', 'binding', 'invocationCount', 'lastInvocation'];
  object(value, value.version === 1 ? keys : [...keys, 'bootstrap']);
  object(value.binding, Object.keys(binding));
  if (
    ![1, 2].includes(value.version) ||
    Object.entries(binding).some(
      ([key, expected]) => value.binding[key] !== expected,
    ) ||
    !Number.isSafeInteger(value.invocationCount) ||
    value.invocationCount < 0 ||
    value.invocationCount > binding.maxInvocations
  )
    invalid();
  let lastInvocation = null;
  if (value.invocationCount === 0) {
    if (value.lastInvocation !== null) invalid();
  } else {
    const last = object(value.lastInvocation, [
      'ordinal',
      'requestSha256',
      'action',
      'state',
    ]);
    if (
      last.ordinal !== value.invocationCount ||
      (last.state !== 'pending' && last.state !== 'settled') ||
      !last.action ||
      typeof last.action !== 'object' ||
      Array.isArray(last.action) ||
      Object.hasOwn(last.action, 'token')
    )
      invalid();
    const action = { ...last.action };
    if (
      action.kind === 'cleanup-restart-blocked' ||
      action.kind === 'decommission-restart-blocked'
    )
      action.token = null;
    const decoded = await decodeRequest(
      JSON.stringify({
        contractVersion: 1,
        configSha256: binding.configSha256,
        action,
      }),
      binding.configSha256,
    );
    lastInvocation = Object.freeze({
      ordinal: last.ordinal,
      requestSha256: digest(last.requestSha256),
      action: actionSummary(decoded.action),
      state: last.state,
    });
  }
  return Object.freeze({
    version: 2,
    binding,
    invocationCount: value.invocationCount,
    lastInvocation,
    bootstrap: decodeBootstrap(
      value.version === 1 ? null : value.bootstrap,
      binding,
      value.invocationCount,
      lastInvocation,
    ),
  });
}

function identifier(value, max = 128) {
  if (
    typeof value !== 'string' ||
    !value ||
    value !== value.trim() ||
    value.length > max ||
    [...value].some(
      (character) =>
        character.charCodeAt(0) <= 31 || character.charCodeAt(0) === 127,
    )
  )
    invalid();
  return value;
}

function equalShape(value, expected) {
  if (expected !== null && typeof expected === 'object') {
    object(value, Object.keys(expected));
    for (const key of Object.keys(expected))
      equalShape(value[key], expected[key]);
  } else if (value !== expected) invalid();
}

function bootstrapContext(value, binding) {
  object(value, [
    'names',
    'zoneId',
    'zoneName',
    'accountWorkersDevSubdomain',
    'dispatch',
  ]);
  const hostname = identifier(value.names?.referenceHostname, 253);
  const prefix = `${binding.resourcePrefix}-reference.`;
  if (!hostname.startsWith(prefix)) invalid();
  const ownedHostname = hostname.slice(prefix.length);
  const names = deriveDirectConformanceNames({
    resourcePrefix: binding.resourcePrefix,
    ownedHostname,
  });
  equalShape(value.names, names);
  const zoneId = identifier(value.zoneId);
  const zoneName = identifier(value.zoneName, 253);
  if (
    !/^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$/u.test(zoneName) ||
    zoneName
      .split('.')
      .some(
        (label) => !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/u.test(label),
      ) ||
    (ownedHostname !== zoneName && !ownedHostname.endsWith(`.${zoneName}`))
  )
    invalid();
  const subdomain = identifier(value.accountWorkersDevSubdomain);
  if (!/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/u.test(subdomain)) invalid();
  object(value.dispatch, ['kind', 'count']);
  const { kind, count } = value.dispatch;
  if (
    !Number.isSafeInteger(count) ||
    count < 0 ||
    count > 10_000 ||
    !['first-page-404', 'empty', 'enumerated'].includes(kind) ||
    (kind === 'enumerated' ? count === 0 : count !== 0)
  )
    invalid();
  return Object.freeze({
    names,
    zoneId,
    zoneName,
    accountWorkersDevSubdomain: subdomain,
    dispatch: Object.freeze({ kind, count }),
  });
}

const MUTATION_FIELD = Object.freeze({
  'create-fleet-d1': 'fleet',
  'create-quota-d1': 'quota',
  'create-export-r2': 'exports',
  'upload-reference': 'upload',
  'enable-reference-ingress': 'ingress',
});

function mutationField(kind) {
  if (typeof kind !== 'string' || !Object.hasOwn(MUTATION_FIELD, kind))
    invalid();
  return MUTATION_FIELD[kind];
}

function decodeBootstrap(value, binding, invocationCount, lastInvocation) {
  if (value === null) return null;
  object(value, [
    'context',
    'fleet',
    'quota',
    'exports',
    'upload',
    'active',
    'ingress',
    'controlReadOrdinal',
    'pending',
  ]);
  const context = bootstrapContext(value.context, binding);
  const result = { context };
  for (const [key, name] of [
    ['fleet', context.names.fleetDatabase],
    ['quota', context.names.quotaDatabase],
  ]) {
    const receipt = value[key];
    if (receipt === null) result[key] = null;
    else {
      object(receipt, ['uuid', 'name']);
      if (receipt.name !== name) invalid();
      result[key] = Object.freeze({ uuid: identifier(receipt.uuid), name });
    }
  }
  if (result.fleet && result.quota && result.fleet.uuid === result.quota.uuid)
    invalid();
  if (value.exports !== null) {
    object(value.exports, ['name', 'jurisdiction', 'creationDate']);
    const { name, jurisdiction, creationDate } = value.exports;
    if (
      name !== context.names.exportBucket ||
      jurisdiction !== 'default' ||
      typeof creationDate !== 'string' ||
      creationDate.length > 32 ||
      !Number.isFinite(Date.parse(creationDate)) ||
      new Date(creationDate).toISOString() !== creationDate
    )
      invalid();
    result.exports = Object.freeze({ name, jurisdiction, creationDate });
  } else result.exports = null;
  if (value.upload !== null) {
    object(value.upload, ['scriptName', 'tag', 'etag']);
    if (value.upload.scriptName !== context.names.referenceWorker) invalid();
    result.upload = Object.freeze({
      scriptName: value.upload.scriptName,
      tag: value.upload.tag === null ? null : identifier(value.upload.tag),
      etag: value.upload.etag === null ? null : identifier(value.upload.etag),
    });
  } else result.upload = null;
  if (value.active !== null) {
    object(value.active, ['deploymentId', 'versionId']);
    result.active = Object.freeze({
      deploymentId: identifier(value.active.deploymentId),
      versionId: identifier(value.active.versionId),
    });
  } else result.active = null;
  if (value.ingress !== null) {
    equalShape(value.ingress, { enabled: true, previewsEnabled: false });
    result.ingress = Object.freeze({ enabled: true, previewsEnabled: false });
  } else result.ingress = null;
  const ordinal = value.controlReadOrdinal;
  if (
    ordinal !== null &&
    (!Number.isSafeInteger(ordinal) ||
      ordinal < 1 ||
      ordinal > invocationCount ||
      (ordinal === invocationCount &&
        (lastInvocation?.state !== 'settled' ||
          lastInvocation.action.kind !== 'control-read')))
  )
    invalid();
  result.controlReadOrdinal = ordinal;
  const fields = [
    'fleet',
    'quota',
    'exports',
    'upload',
    'active',
    'ingress',
    'controlReadOrdinal',
  ];
  for (let index = 1; index < fields.length; index++) {
    if (result[fields[index]] !== null && result[fields[index - 1]] === null)
      invalid();
  }
  result.pending = value.pending;
  if (value.pending !== null) {
    const index = fields.indexOf(mutationField(value.pending));
    if (
      lastInvocation?.state === 'pending' ||
      (index > 0 && result[fields[index - 1]] === null) ||
      fields.slice(index + 1).some((field) => result[field] !== null)
    )
      invalid();
  }
  return Object.freeze(result);
}

function assertPrivate(stat, directory) {
  if (
    stat.uid !== process.getuid() ||
    (stat.mode & 0o7777) !== (directory ? 0o700 : 0o600) ||
    (directory ? !stat.isDirectory() : !stat.isFile() || stat.nlink !== 1)
  )
    invalid();
}

function fileFlags(access) {
  return access | constants.O_NOFOLLOW | constants.O_NONBLOCK;
}

async function privateDirectory(path) {
  const handle = await open(
    path,
    fileFlags(constants.O_RDONLY | constants.O_DIRECTORY),
  );
  try {
    assertPrivate(await handle.stat(), true);
    return handle;
  } catch (error) {
    await handle.close();
    throw error;
  }
}

async function ensureBase(path) {
  try {
    await mkdir(path, { mode: 0o700 });
  } catch (error) {
    if (error.code !== 'EEXIST') throw error;
  }
  const handle = await privateDirectory(path);
  try {
    const parent = await open(
      dirname(path),
      constants.O_RDONLY | constants.O_DIRECTORY,
    );
    try {
      await parent.sync();
    } finally {
      await parent.close();
    }
    return handle;
  } catch (error) {
    await handle.close();
    throw error;
  }
}

async function acquireLock(path, base) {
  const handle = await open(
    path,
    fileFlags(constants.O_RDWR | constants.O_CREAT),
    0o600,
  );
  try {
    assertPrivate(await handle.stat(), false);
    const acquired = await new Promise((fulfill) => {
      const child = spawn(
        '/usr/bin/flock',
        ['--exclusive', '--nonblock', '3'],
        { stdio: ['ignore', 'ignore', 'ignore', handle.fd], env: {} },
      );
      let failed = false;
      child.once('error', () => {
        failed = true;
      });
      child.once('close', (status, signal) => {
        fulfill(!failed && status === 0 && signal === null);
      });
    });
    if (!acquired) throw new DirectRunStateError('lock-unavailable');
    await handle.sync();
    await base.sync();
    return handle;
  } catch (error) {
    await handle.close();
    throw error;
  }
}

async function readSnapshot(path, binding) {
  const handle = await open(path, fileFlags(constants.O_RDONLY));
  try {
    const stat = await handle.stat();
    assertPrivate(stat, false);
    if (stat.size < 1 || stat.size > MAX_JOURNAL_BYTES) invalid();
    const bytes = Buffer.alloc(stat.size);
    let offset = 0;
    while (offset < bytes.length) {
      const { bytesRead } = await handle.read(
        bytes,
        offset,
        bytes.length - offset,
        offset,
      );
      if (bytesRead === 0) invalid();
      offset += bytesRead;
    }
    if ((await handle.read(Buffer.alloc(1), 0, 1, offset)).bytesRead !== 0)
      invalid();
    return await decodeSnapshot(
      JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)),
      binding,
    );
  } finally {
    await handle.close();
  }
}

async function writeSnapshot(directory, handle, snapshot) {
  const temporary = join(directory, `.journal-${randomUUID()}.tmp`);
  let file;
  let created = false;
  try {
    file = await open(
      temporary,
      fileFlags(constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL),
      0o600,
    );
    created = true;
    assertPrivate(await file.stat(), false);
    const serialized = `${JSON.stringify(snapshot)}\n`;
    if (Buffer.byteLength(serialized) > MAX_JOURNAL_BYTES) invalid();
    await file.writeFile(serialized);
    await file.sync();
    await file.close();
    file = undefined;
    await rename(temporary, join(directory, 'journal.json'));
    created = false;
    await handle.sync();
  } finally {
    const cleanup = await Promise.allSettled([
      ...(file ? [file.close()] : []),
      ...(created ? [unlink(temporary)] : []),
    ]);
    if (cleanup.some((result) => result.status === 'rejected')) invalid();
  }
}

async function exists(path) {
  try {
    await lstat(path);
    return true;
  } catch (error) {
    if (error.code === 'ENOENT') return false;
    throw error;
  }
}

async function initializeRun(basePath, base, directory, binding) {
  if (await exists(directory)) throw new DirectRunStateError('run-exists');
  const staging = join(
    basePath,
    `.${binding.resourcePrefix}-${randomUUID()}.tmp`,
  );
  await mkdir(staging, { mode: 0o700 });
  let handle;
  let published = false;
  try {
    handle = await privateDirectory(staging);
    const snapshot = Object.freeze({
      version: 2,
      binding,
      invocationCount: 0,
      lastInvocation: null,
      bootstrap: null,
    });
    await writeSnapshot(staging, handle, snapshot);
    if (await exists(directory)) throw new DirectRunStateError('run-exists');
    await rename(staging, directory);
    published = true;
    await base.sync();
    return { handle, snapshot };
  } catch (error) {
    await handle?.close();
    if (!published) {
      if (await exists(join(staging, 'journal.json')))
        await unlink(join(staging, 'journal.json'));
      await rmdir(staging);
    }
    throw error;
  }
}

function runJournal(directory, directoryHandle, base, lock, initial) {
  const queue = new PQueue({ concurrency: 1 });
  let snapshot = initial;
  let poisoned = false;
  let closePromise;
  const enqueue = (operation) => {
    if (closePromise) return Promise.reject(new DirectRunStateError());
    return queue.add(async () => {
      try {
        if (poisoned) invalid();
        const current = await readSnapshot(
          join(directory, 'journal.json'),
          snapshot.binding,
        );
        if (JSON.stringify(current) !== JSON.stringify(snapshot)) invalid();
        return await operation();
      } catch (error) {
        throw stateError(error);
      }
    });
  };
  const publish = async (next) => {
    try {
      await writeSnapshot(directory, directoryHandle, next);
      snapshot = next;
    } catch (error) {
      poisoned = true;
      throw error;
    }
  };
  const publishBootstrap = async (bootstrap) => {
    const next = await decodeSnapshot(
      { ...snapshot, bootstrap },
      snapshot.binding,
    );
    await publish(next);
  };
  const assertSettled = () => {
    if (
      snapshot.lastInvocation?.state === 'pending' ||
      snapshot.bootstrap?.pending
    )
      throw new DirectRunStateError('outcome-unknown');
  };
  return Object.freeze({
    directory,
    snapshot() {
      return snapshot;
    },
    bindBootstrapContext(context) {
      return enqueue(async () => {
        assertSettled();
        const checked = bootstrapContext(context, snapshot.binding);
        if (snapshot.bootstrap) {
          const { dispatch: _historical, ...previous } =
            snapshot.bootstrap.context;
          const { dispatch: _fresh, ...current } = checked;
          equalShape(current, previous);
          return;
        }
        if (snapshot.invocationCount !== 0) invalid();
        await publishBootstrap({
          context: checked,
          fleet: null,
          quota: null,
          exports: null,
          upload: null,
          active: null,
          ingress: null,
          controlReadOrdinal: null,
          pending: null,
        });
      });
    },
    beginBootstrapMutation(kind) {
      return enqueue(async () => {
        assertSettled();
        const field = mutationField(kind);
        if (!snapshot.bootstrap || snapshot.bootstrap[field] !== null)
          invalid();
        await publishBootstrap({ ...snapshot.bootstrap, pending: kind });
      });
    },
    confirmBootstrapMutation(value) {
      return enqueue(async () => {
        object(value, ['kind', 'receipt']);
        if (value.receipt === null) invalid();
        const field = mutationField(value.kind);
        if (
          !snapshot.bootstrap ||
          snapshot.bootstrap.pending !== value.kind ||
          snapshot.bootstrap[field] !== null
        )
          invalid();
        await publishBootstrap({
          ...snapshot.bootstrap,
          [field]: value.receipt,
        });
        await publishBootstrap({ ...snapshot.bootstrap, pending: null });
      });
    },
    recordBootstrapObservation(value) {
      return enqueue(async () => {
        assertSettled();
        const bootstrap = snapshot.bootstrap;
        if (!bootstrap) invalid();
        if (value?.kind === 'active') {
          object(value, ['kind', 'deploymentId', 'versionId']);
          const active = {
            deploymentId: value.deploymentId,
            versionId: value.versionId,
          };
          if (bootstrap.active) {
            equalShape(active, bootstrap.active);
            return;
          }
          await publishBootstrap({ ...bootstrap, active });
        } else if (value?.kind === 'control-read') {
          object(value, ['kind', 'ordinal']);
          const last = snapshot.lastInvocation;
          if (
            last?.state !== 'settled' ||
            last.action.kind !== 'control-read' ||
            last.ordinal !== value.ordinal
          )
            invalid();
          await publishBootstrap({
            ...bootstrap,
            controlReadOrdinal: value.ordinal,
          });
        } else invalid();
      });
    },
    reserveInvocation(serializedRequest) {
      return enqueue(async () => {
        assertSettled();
        if (snapshot.invocationCount >= snapshot.binding.maxInvocations)
          throw new DirectRunStateError('invocation-budget-exhausted');
        const request = await decodeRequest(
          serializedRequest,
          snapshot.binding.configSha256,
        );
        const reservation = Object.freeze({
          ordinal: snapshot.invocationCount + 1,
          requestSha256: createHash('sha256')
            .update(serializedRequest)
            .digest('hex'),
        });
        await publish(
          Object.freeze({
            ...snapshot,
            invocationCount: reservation.ordinal,
            lastInvocation: Object.freeze({
              ...reservation,
              action: actionSummary(request.action),
              state: 'pending',
            }),
          }),
        );
        return reservation;
      });
    },
    settleInvocation(reservation) {
      return enqueue(async () => {
        object(reservation, ['ordinal', 'requestSha256']);
        const last = snapshot.lastInvocation;
        if (
          !last ||
          reservation.ordinal !== last.ordinal ||
          reservation.requestSha256 !== last.requestSha256
        )
          invalid();
        if (last.state === 'settled') return;
        await publish(
          Object.freeze({
            ...snapshot,
            lastInvocation: Object.freeze({ ...last, state: 'settled' }),
          }),
        );
      });
    },
    close() {
      closePromise ??= (async () => {
        await queue.onIdle();
        const closed = await Promise.allSettled([
          directoryHandle.close(),
          base.close(),
          lock.close(),
        ]);
        if (closed.some((result) => result.status === 'rejected')) invalid();
      })();
      return closePromise;
    },
  });
}

export async function openDirectRunState(input) {
  let base;
  let lock;
  let directoryHandle;
  try {
    if (
      process.platform !== 'linux' ||
      typeof process.getuid !== 'function' ||
      !Number.isInteger(constants.O_NOFOLLOW) ||
      !Number.isInteger(constants.O_NONBLOCK) ||
      !Number.isInteger(constants.O_DIRECTORY)
    )
      throw new DirectRunStateError('lock-unavailable');
    if (input.mode !== 'run' && input.mode !== 'resume') invalid();
    const binding = bindingFromInput(input);
    const basePath = join(
      dirname(resolve(input.configPath)),
      '.direct-conformance',
    );
    const directory = join(basePath, binding.resourcePrefix);
    base = await ensureBase(basePath);
    lock = await acquireLock(
      join(basePath, `${binding.resourcePrefix}.lock`),
      base,
    );
    let snapshot;
    if (input.mode === 'run') {
      const initialized = await initializeRun(
        basePath,
        base,
        directory,
        binding,
      );
      directoryHandle = initialized.handle;
      snapshot = initialized.snapshot;
    } else {
      if (!(await exists(directory)))
        throw new DirectRunStateError('run-missing');
      directoryHandle = await privateDirectory(directory);
      snapshot = await readSnapshot(join(directory, 'journal.json'), binding);
      if (
        snapshot.lastInvocation?.state === 'pending' ||
        snapshot.bootstrap?.pending
      )
        throw new DirectRunStateError('outcome-unknown');
    }
    return runJournal(directory, directoryHandle, base, lock, snapshot);
  } catch (error) {
    await Promise.allSettled(
      [directoryHandle, base, lock]
        .filter((handle) => handle !== undefined)
        .map((handle) => handle.close()),
    );
    throw stateError(error);
  }
}
