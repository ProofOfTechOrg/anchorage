// SPDX-License-Identifier: Apache-2.0

import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  chmod,
  link,
  mkdir,
  mkdtemp,
  open,
  readdir,
  readFile,
  rename,
  rm,
  stat,
  symlink,
  unlink,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { preflightDirectConformance } from '../scripts/direct-credentialed-conformance-preflight.mjs';
import {
  type DirectBootstrapContext,
  type DirectBootstrapMutationReceipt,
  type DirectRunJournal,
  openDirectRunState,
} from '../scripts/direct-credentialed-run-state.mjs';

vi.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>();
  return { ...actual, rename: vi.fn(actual.rename) };
});

const directories: string[] = [];
const journals = new Set<DirectRunJournal>();
const hash = (value: string) =>
  createHash('sha256').update(value).digest('hex');
const CLAIM = 'opaque-claim-must-not-enter-local-state';

async function fixture(limit = 3) {
  const directory = await mkdtemp(join(tmpdir(), 'direct-run-state-'));
  directories.push(directory);
  const config = JSON.parse(
    await readFile(
      new URL(
        '../scripts/direct-credentialed-conformance.example.json',
        import.meta.url,
      ),
      'utf8',
    ),
  );
  const reference =
    "import manifest from './direct-run-manifest.js'; export default {fetch(){return Response.json(manifest.contractVersion)}};";
  const tenant =
    'export class Maintenance {} export class Runner {} export default {};';
  config.referenceWorker.artifact = {
    bundle: './reference.mjs',
    mainModule: 'worker.js',
    sha256: hash(reference),
  };
  config.referenceWorker.maxInvocations = limit;
  config.deployment.artifact = {
    bundle: './tenant.mjs',
    mainModule: 'worker.js',
    sha256: hash(tenant),
  };
  const configPath = join(directory, 'config.json');
  await writeFile(configPath, JSON.stringify(config));
  await writeFile(join(directory, 'reference.mjs'), reference);
  await writeFile(join(directory, 'tenant.mjs'), tenant);
  const prepared = await preflightDirectConformance({
    configPath,
    now: Date.parse('2026-09-10T12:00:00Z'),
  });
  const base = join(directory, '.direct-conformance');
  const runDirectory = join(base, config.resourcePrefix);
  const lockPath = join(base, `${config.resourcePrefix}.lock`);
  const input = { configPath, prepared, accountId: 'account' };
  const request = (action: unknown = { kind: 'control-read' }) =>
    JSON.stringify({
      contractVersion: 1,
      configSha256: prepared.configSha256,
      action,
    });
  return {
    directory,
    configPath,
    prepared,
    base,
    runDirectory,
    lockPath,
    input,
    request,
  };
}

async function opened(input: Parameters<typeof openDirectRunState>[0]) {
  const journal = await openDirectRunState(input);
  journals.add(journal);
  return journal;
}

async function closed(journal: DirectRunJournal) {
  await journal.close();
  journals.delete(journal);
}

afterEach(async () => {
  vi.restoreAllMocks();
  try {
    await Promise.all([...journals].map((journal) => journal.close()));
  } finally {
    journals.clear();
    await Promise.all(
      directories
        .splice(0)
        .map((directory) => rm(directory, { recursive: true, force: true })),
    );
  }
});

const describeLinux =
  process.platform === 'linux' ? describe.sequential : describe.skip;

function bootstrapContext(
  f: Awaited<ReturnType<typeof fixture>>,
): DirectBootstrapContext {
  return {
    names: f.prepared.names,
    zoneId: 'zone',
    zoneName: 'example.test',
    accountWorkersDevSubdomain: 'attested-account',
    dispatch: { kind: 'empty', count: 0 },
  };
}

function receipts(f: Awaited<ReturnType<typeof fixture>>) {
  return [
    {
      kind: 'create-fleet-d1',
      receipt: { uuid: 'fleet-uuid', name: f.prepared.names.fleetDatabase },
    },
    {
      kind: 'create-quota-d1',
      receipt: { uuid: 'quota-uuid', name: f.prepared.names.quotaDatabase },
    },
    {
      kind: 'create-export-r2',
      receipt: {
        name: f.prepared.names.exportBucket,
        jurisdiction: 'default',
        creationDate: '2026-09-10T00:00:00.000Z',
      },
    },
    {
      kind: 'upload-reference',
      receipt: {
        scriptName: f.prepared.names.referenceWorker,
        tag: null,
        etag: null,
      },
    },
    {
      kind: 'enable-reference-ingress',
      receipt: { enabled: true, previewsEnabled: false },
    },
  ] as const satisfies readonly DirectBootstrapMutationReceipt[];
}

async function confirmedBootstrap(
  f: Awaited<ReturnType<typeof fixture>>,
  journal: DirectRunJournal,
) {
  await journal.bindBootstrapContext(bootstrapContext(f));
  for (const value of receipts(f)) {
    if (value.kind === 'enable-reference-ingress')
      await journal.recordBootstrapObservation({
        kind: 'active',
        deploymentId: 'deployment',
        versionId: 'version',
      });
    await journal.beginBootstrapMutation(value.kind);
    await journal.confirmBootstrapMutation(value);
  }
}

describeLinux('durable bootstrap state', () => {
  it('enforces mutation order, exact receipt association and exclusive invocation/provider pending state', async () => {
    const f = await fixture();
    const journal = await opened({ ...f.input, mode: 'run' });
    await expect(
      journal.beginBootstrapMutation('create-fleet-d1'),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await journal.bindBootstrapContext(bootstrapContext(f));
    await expect(
      journal.beginBootstrapMutation('create-quota-d1'),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await expect(
      journal.recordBootstrapObservation({
        kind: 'active',
        deploymentId: 'deployment',
        versionId: 'version',
      }),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    const reservation = await journal.reserveInvocation(f.request());
    await expect(
      journal.beginBootstrapMutation('create-fleet-d1'),
    ).rejects.toMatchObject({ code: 'outcome-unknown' });
    await journal.settleInvocation(reservation);
    await journal.beginBootstrapMutation('create-fleet-d1');
    await expect(journal.reserveInvocation(f.request())).rejects.toMatchObject({
      code: 'outcome-unknown',
    });
    await expect(
      journal.beginBootstrapMutation('create-quota-d1'),
    ).rejects.toMatchObject({ code: 'outcome-unknown' });
    await expect(
      journal.confirmBootstrapMutation(receipts(f)[1]),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await expect(
      journal.confirmBootstrapMutation({
        kind: 'create-fleet-d1',
        receipt: null,
      } as unknown as DirectBootstrapMutationReceipt),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await expect(
      journal.confirmBootstrapMutation({
        kind: 'create-fleet-d1',
        receipt: { uuid: 'fleet-uuid', name: 'foreign' },
      }),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await journal.confirmBootstrapMutation(receipts(f)[0]);
    await expect(
      journal.beginBootstrapMutation('create-fleet-d1'),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await expect(
      journal.confirmBootstrapMutation(receipts(f)[0]),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await journal.beginBootstrapMutation('create-quota-d1');
    await expect(
      journal.confirmBootstrapMutation({
        kind: 'create-quota-d1',
        receipt: { uuid: 'fleet-uuid', name: f.prepared.names.quotaDatabase },
      }),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    expect(journal.snapshot().bootstrap).toMatchObject({
      pending: 'create-quota-d1',
      fleet: { uuid: 'fleet-uuid' },
      quota: null,
    });
  });

  it('freezes closed context/receipts and preserves historical control-read ordinals across later invocations and resume', async () => {
    const f = await fixture(4);
    const journal = await opened({ ...f.input, mode: 'run' });
    await confirmedBootstrap(f, journal);
    const first = await journal.reserveInvocation(f.request());
    await expect(
      journal.recordBootstrapObservation({
        kind: 'control-read',
        ordinal: first.ordinal,
      }),
    ).rejects.toMatchObject({ code: 'outcome-unknown' });
    await journal.settleInvocation(first);
    await journal.recordBootstrapObservation({
      kind: 'control-read',
      ordinal: first.ordinal,
    });
    const second = await journal.reserveInvocation(
      f.request({ kind: 'tenant-probe', role: 'a', operation: 'health' }),
    );
    await journal.settleInvocation(second);
    await expect(
      journal.recordBootstrapObservation({
        kind: 'control-read',
        ordinal: second.ordinal,
      }),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await expect(
      journal.recordBootstrapObservation({
        kind: 'active',
        deploymentId: 'replacement',
        versionId: 'version',
      }),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await journal.bindBootstrapContext({
      ...bootstrapContext(f),
      dispatch: { kind: 'enumerated', count: 9 },
    });
    const frozen = (value: unknown) => {
      if (!value || typeof value !== 'object') return;
      expect(Object.isFrozen(value)).toBe(true);
      Object.values(value).forEach(frozen);
    };
    frozen(journal.snapshot());
    await closed(journal);
    const resumed = await opened({ ...f.input, mode: 'resume' });
    expect(resumed.snapshot()).toMatchObject({
      invocationCount: 2,
      bootstrap: {
        controlReadOrdinal: 1,
        context: { dispatch: { kind: 'empty', count: 0 } },
      },
    });
    const third = await resumed.reserveInvocation(f.request());
    await resumed.settleInvocation(third);
    await resumed.recordBootstrapObservation({
      kind: 'control-read',
      ordinal: 3,
    });
    expect(resumed.snapshot().bootstrap?.controlReadOrdinal).toBe(3);
    expect(
      (await stat(join(resumed.directory, 'journal.json'))).size,
    ).toBeLessThanOrEqual(16 * 1024);
  });

  it('normalizes v1 without dropping settled history or resetting the budget', async () => {
    const f = await fixture(2);
    const journal = await opened({ ...f.input, mode: 'run' });
    const first = await journal.reserveInvocation(f.request());
    await journal.settleInvocation(first);
    await closed(journal);
    const path = join(journal.directory, 'journal.json');
    const original = JSON.parse(await readFile(path, 'utf8'));
    delete original.bootstrap;
    original.version = 1;
    await writeFile(path, JSON.stringify(original));
    const resumed = await opened({ ...f.input, mode: 'resume' });
    expect(resumed.snapshot()).toMatchObject({
      version: 2,
      bootstrap: null,
      invocationCount: 1,
      lastInvocation: { state: 'settled', ordinal: 1 },
    });
    await expect(
      resumed.bindBootstrapContext(bootstrapContext(f)),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    const second = await resumed.reserveInvocation(f.request());
    await resumed.settleInvocation(second);
    expect(JSON.parse(await readFile(path, 'utf8')).version).toBe(2);
    await expect(resumed.reserveInvocation(f.request())).rejects.toMatchObject({
      code: 'invocation-budget-exhausted',
    });
  });

  it.each([
    'file-1',
    'rename-1',
    'directory-1',
    'file-2',
    'rename-2',
    'directory-2',
  ])('retains the receipt barrier disposition and poisons the handle after %s failure', async (failure) => {
    const f = await fixture();
    const journal = await opened({ ...f.input, mode: 'run' });
    await journal.bindBootstrapContext(bootstrapContext(f));
    await journal.beginBootstrapMutation('create-fleet-d1');
    const path = join(journal.directory, 'journal.json');
    const directory = await stat(journal.directory);
    const probe = await open(path, 'r');
    const prototype = Object.getPrototypeOf(probe) as typeof probe;
    const originalSync = prototype.sync;
    const actualFs =
      await vi.importActual<typeof import('node:fs/promises')>(
        'node:fs/promises',
      );
    let occurrences = 0;
    const [point, barrier] = failure.split('-');
    const check = () => {
      if (++occurrences === Number(barrier))
        throw new Error('receipt-sync-secret-sentinel');
    };
    const sync = vi.spyOn(prototype, 'sync').mockImplementation(async function (
      this: typeof probe,
    ) {
      const current = await this.stat();
      if (
        (point === 'file' && current.isFile()) ||
        (point === 'directory' &&
          current.isDirectory() &&
          current.ino === directory.ino &&
          current.dev === directory.dev)
      )
        check();
      await originalSync.call(this);
    });
    vi.mocked(rename).mockImplementation(async (...args) => {
      if (point === 'rename') check();
      await actualFs.rename(...args);
    });
    try {
      const error = await journal
        .confirmBootstrapMutation(receipts(f)[0])
        .catch((error: unknown) => error);
      expect(error).toMatchObject({ code: 'invalid-state' });
      expect(String(error)).not.toContain('receipt-sync-secret-sentinel');
      await expect(
        journal.reserveInvocation(f.request()),
      ).rejects.toMatchObject({ code: 'invalid-state' });
      const disk = JSON.parse(await readFile(path, 'utf8'));
      expect(disk.bootstrap.pending).toBe(
        failure === 'directory-2' ? null : 'create-fleet-d1',
      );
      expect(disk.bootstrap.fleet).toEqual(
        ['file-1', 'rename-1'].includes(failure)
          ? null
          : receipts(f)[0].receipt,
      );
      expect(await readdir(journal.directory)).toEqual(['journal.json']);
    } finally {
      sync.mockRestore();
      vi.mocked(rename).mockImplementation(actualFs.rename);
      await probe.close();
    }
    await closed(journal);
    if (failure === 'directory-2') {
      const resumed = await opened({ ...f.input, mode: 'resume' });
      expect(resumed.snapshot().bootstrap?.fleet).toEqual(
        receipts(f)[0].receipt,
      );
    } else
      await expect(
        openDirectRunState({ ...f.input, mode: 'resume' }),
      ).rejects.toMatchObject({ code: 'outcome-unknown' });
  });

  it('rejects malformed context, extra receipt fields, missing prerequisites and invalid historical ordinals on resume', async () => {
    const f = await fixture();
    const journal = await opened({ ...f.input, mode: 'run' });
    await confirmedBootstrap(f, journal);
    const reservation = await journal.reserveInvocation(f.request());
    await journal.settleInvocation(reservation);
    await journal.recordBootstrapObservation({
      kind: 'control-read',
      ordinal: 1,
    });
    await closed(journal);
    const path = join(journal.directory, 'journal.json');
    const original = JSON.parse(await readFile(path, 'utf8'));
    for (const mutate of [
      (value: typeof original) => {
        value.bootstrap.context.token = CLAIM;
      },
      (value: typeof original) => {
        value.bootstrap.context.names.roles.a.scriptName = 'foreign';
      },
      (value: typeof original) => {
        value.bootstrap.context.zoneName = 'notexample.test';
      },
      (value: typeof original) => {
        value.bootstrap.context.dispatch.count = 1;
      },
      (value: typeof original) => {
        value.bootstrap.fleet.secret = CLAIM;
      },
      (value: typeof original) => {
        value.bootstrap.quota.uuid = value.bootstrap.fleet.uuid;
      },
      (value: typeof original) => {
        value.bootstrap.exports.creationDate = '2026-09-10';
      },
      (value: typeof original) => {
        value.bootstrap.exports.jurisdiction = 'eu';
      },
      (value: typeof original) => {
        value.bootstrap.upload.tag = 'x'.repeat(129);
      },
      (value: typeof original) => {
        value.bootstrap.active = null;
      },
      (value: typeof original) => {
        value.bootstrap.controlReadOrdinal = 2;
      },
      (value: typeof original) => {
        value.bootstrap.pending = 'create-fleet-d1';
      },
    ]) {
      const corrupted = structuredClone(original);
      mutate(corrupted);
      await writeFile(path, JSON.stringify(corrupted));
      await expect(
        openDirectRunState({ ...f.input, mode: 'resume' }),
      ).rejects.toMatchObject({ code: 'invalid-state' });
    }
    await writeFile(path, JSON.stringify(original));
    await closed(await opened({ ...f.input, mode: 'resume' }));
  });
});

describeLinux('durable direct invocation state', () => {
  it('persists the reservation before return and resumes the original budget without retaining claims', async () => {
    const f = await fixture(2);
    const journal = await opened({ ...f.input, mode: 'run' });
    expect(journal.snapshot().invocationCount).toBe(0);
    const body = f.request({
      kind: 'cleanup-restart-blocked',
      role: 'a',
      token: { private: CLAIM },
    });
    const reservation = await journal.reserveInvocation(body);
    expect(reservation).toEqual({ ordinal: 1, requestSha256: hash(body) });
    const path = join(journal.directory, 'journal.json');
    const bytes = await readFile(path, 'utf8');
    expect(bytes).not.toContain(CLAIM);
    expect(bytes).not.toContain('"token"');
    expect(JSON.parse(bytes)).toMatchObject({
      invocationCount: 1,
      lastInvocation: {
        ...reservation,
        action: { kind: 'cleanup-restart-blocked', role: 'a' },
        state: 'pending',
      },
    });
    expect(Object.isFrozen(journal.snapshot())).toBe(true);
    expect(Object.isFrozen(journal.snapshot().binding)).toBe(true);
    expect(Object.isFrozen(journal.snapshot().lastInvocation?.action)).toBe(
      true,
    );
    await expect(journal.reserveInvocation(f.request())).rejects.toMatchObject({
      code: 'outcome-unknown',
    });
    await expect(
      journal.settleInvocation({ ...reservation, ordinal: 2 }),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await expect(
      journal.settleInvocation({
        ...reservation,
        requestSha256: 'f'.repeat(64),
      }),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await journal.settleInvocation(reservation);
    const settled = await readFile(path);
    await journal.settleInvocation(reservation);
    expect(await readFile(path)).toEqual(settled);
    await closed(journal);
    const resumed = await opened({ ...f.input, mode: 'resume' });
    expect(resumed.snapshot()).toMatchObject({
      invocationCount: 1,
      lastInvocation: {
        state: 'settled',
        action: { kind: 'cleanup-restart-blocked', role: 'a' },
      },
    });
    const second = await resumed.reserveInvocation(
      f.request({ kind: 'tenant-probe', role: 'b', operation: 'health' }),
    );
    await resumed.settleInvocation(second);
    await closed(resumed);
    const exhausted = await opened({ ...f.input, mode: 'resume' });
    await expect(
      exhausted.reserveInvocation(f.request()),
    ).rejects.toMatchObject({ code: 'invocation-budget-exhausted' });
    expect(exhausted.snapshot().invocationCount).toBe(2);
    for (const directory of [f.base, f.runDirectory])
      expect((await stat(directory)).mode & 0o777).toBe(0o700);
    for (const file of [f.lockPath, path]) {
      const info = await stat(file);
      expect(info.mode & 0o777).toBe(0o600);
      expect(info.nlink).toBe(1);
      expect(info.uid).toBe(process.getuid?.());
    }
  });

  it('refuses pending outcomes and invalid bodies without hiding the durable count', async () => {
    const f = await fixture();
    const journal = await opened({ ...f.input, mode: 'run' });
    const before = await readFile(join(journal.directory, 'journal.json'));
    for (const body of [
      '{',
      f.request({ kind: 'force-recovery', script: CLAIM }),
      f.request().replace(f.prepared.configSha256, 'f'.repeat(64)),
    ]) {
      const error = await journal
        .reserveInvocation(body)
        .catch((error: unknown) => error);
      expect(error).toMatchObject({ code: 'invalid-state' });
      expect(String(error)).not.toContain(CLAIM);
      expect(await readFile(join(journal.directory, 'journal.json'))).toEqual(
        before,
      );
    }
    await journal.reserveInvocation(
      f.request({ kind: 'migration-continue', token: { private: CLAIM } }),
    );
    await closed(journal);
    await expect(
      openDirectRunState({ ...f.input, mode: 'resume' }),
    ).rejects.toMatchObject({ code: 'outcome-unknown' });
    await expect(
      openDirectRunState({ ...f.input, mode: 'run' }),
    ).rejects.toMatchObject({ code: 'run-exists' });
    expect(
      JSON.parse(await readFile(join(f.runDirectory, 'journal.json'), 'utf8'))
        .invocationCount,
    ).toBe(1);
  });

  it('serializes concurrent reservations and waits for queued writes before close', async () => {
    const f = await fixture();
    const journal = await opened({ ...f.input, mode: 'run' });
    const results = await Promise.allSettled([
      journal.reserveInvocation(f.request()),
      journal.reserveInvocation(f.request()),
    ]);
    expect(results.map((result) => result.status)).toEqual([
      'fulfilled',
      'rejected',
    ]);
    if (results[0].status !== 'fulfilled')
      throw new Error('first reservation failed');
    await journal.settleInvocation(results[0].value);
    const next = journal.reserveInvocation(f.request());
    const closing = journal.close();
    await expect(journal.reserveInvocation(f.request())).rejects.toMatchObject({
      code: 'invalid-state',
    });
    expect((await next).ordinal).toBe(2);
    await closing;
    journals.delete(journal);
    expect(
      JSON.parse(await readFile(join(f.runDirectory, 'journal.json'), 'utf8'))
        .lastInvocation.state,
    ).toBe('pending');
    await expect(
      openDirectRunState({ ...f.input, mode: 'resume' }),
    ).rejects.toMatchObject({ code: 'outcome-unknown' });
  });

  it('rejects changed bindings and corrupted counters or summaries on resume', async () => {
    const f = await fixture();
    const journal = await opened({ ...f.input, mode: 'run' });
    const reservation = await journal.reserveInvocation(f.request());
    await journal.settleInvocation(reservation);
    await closed(journal);
    const path = join(f.runDirectory, 'journal.json');
    const original = JSON.parse(await readFile(path, 'utf8'));
    for (const mutate of [
      (value: typeof original) => {
        value.binding.accountId = 'foreign';
      },
      (value: typeof original) => {
        value.binding.configSha256 = 'f'.repeat(64);
      },
      (value: typeof original) => {
        value.binding.referenceModuleSetSha256 = 'f'.repeat(64);
      },
      (value: typeof original) => {
        value.binding.maxInvocations += 1;
      },
      (value: typeof original) => {
        value.invocationCount = 0;
      },
      (value: typeof original) => {
        value.invocationCount = 4;
      },
      (value: typeof original) => {
        value.lastInvocation.ordinal = 2;
      },
      (value: typeof original) => {
        value.lastInvocation.requestSha256 = 'invalid';
      },
      (value: typeof original) => {
        value.lastInvocation.action.token = CLAIM;
      },
      (value: typeof original) => {
        value.lastInvocation.action.kind = 'unknown-action';
      },
      (value: typeof original) => {
        value.unexpected = true;
      },
    ]) {
      const corrupted = structuredClone(original);
      mutate(corrupted);
      await writeFile(path, JSON.stringify(corrupted));
      await expect(
        openDirectRunState({ ...f.input, mode: 'resume' }),
      ).rejects.toMatchObject({ code: 'invalid-state' });
    }
    await writeFile(path, JSON.stringify(original));
    await closed(await opened({ ...f.input, mode: 'resume' }));
  });

  it('retains the old file on failed publication and refuses a poisoned open handle', async () => {
    const f = await fixture();
    const journal = await opened({ ...f.input, mode: 'run' });
    const path = join(journal.directory, 'journal.json');
    const before = await readFile(path);
    vi.mocked(rename).mockRejectedValueOnce(
      new Error('fixture publication failure'),
    );
    await expect(journal.reserveInvocation(f.request())).rejects.toMatchObject({
      code: 'invalid-state',
    });
    expect(await readFile(path)).toEqual(before);
    expect(await readdir(journal.directory)).toEqual(['journal.json']);
    await expect(journal.reserveInvocation(f.request())).rejects.toMatchObject({
      code: 'invalid-state',
    });
    await closed(journal);
    const resumed = await opened({ ...f.input, mode: 'resume' });
    const reservation = await resumed.reserveInvocation(f.request());
    expect(reservation.ordinal).toBe(1);
    await resumed.settleInvocation(reservation);
  });

  it('retries the base parent barrier after failed initialization and on resume', async () => {
    const f = await fixture();
    const parent = await stat(f.directory);
    const probe = await open(f.configPath, 'r');
    const prototype = Object.getPrototypeOf(probe) as typeof probe;
    const originalSync = prototype.sync;
    let failed = true;
    let attempts = 0;
    let completed = 0;
    const sync = vi.spyOn(prototype, 'sync').mockImplementation(async function (
      this: typeof probe,
    ) {
      const current = await this.stat();
      if (
        current.isDirectory() &&
        current.dev === parent.dev &&
        current.ino === parent.ino
      ) {
        attempts += 1;
        if (failed) throw new Error('fixture parent directory sync failure');
        await originalSync.call(this);
        completed += 1;
        return;
      }
      return originalSync.call(this);
    });
    try {
      for (let retry = 0; retry < 2; retry++) {
        await expect(
          openDirectRunState({ ...f.input, mode: 'run' }),
        ).rejects.toMatchObject({ code: 'invalid-state' });
        expect((await stat(f.base)).isDirectory()).toBe(true);
        await expect(stat(f.runDirectory)).rejects.toMatchObject({
          code: 'ENOENT',
        });
      }
      expect(attempts).toBe(2);
      expect(completed).toBe(0);
      failed = false;
      const journal = await opened({ ...f.input, mode: 'run' });
      const reservation = await journal.reserveInvocation(f.request());
      expect(completed).toBe(1);
      await journal.settleInvocation(reservation);
      await closed(journal);
      failed = true;
      await expect(
        openDirectRunState({ ...f.input, mode: 'resume' }),
      ).rejects.toMatchObject({ code: 'invalid-state' });
      failed = false;
      await closed(await opened({ ...f.input, mode: 'resume' }));
      expect(completed).toBe(2);
    } finally {
      sync.mockRestore();
      await probe.close();
    }
  });

  it('preserves an uncertain published reservation when directory sync fails', async () => {
    const f = await fixture();
    const journal = await opened({ ...f.input, mode: 'run' });
    const directoryStat = await stat(journal.directory);
    const probe = await open(join(journal.directory, 'journal.json'), 'r');
    const prototype = Object.getPrototypeOf(probe) as typeof probe;
    const originalSync = prototype.sync;
    const sync = vi.spyOn(prototype, 'sync').mockImplementation(async function (
      this: typeof probe,
    ) {
      const current = await this.stat();
      if (
        current.isDirectory() &&
        current.ino === directoryStat.ino &&
        current.dev === directoryStat.dev
      )
        throw new Error('fixture directory sync failure');
      return originalSync.call(this);
    });
    try {
      await expect(
        journal.reserveInvocation(f.request()),
      ).rejects.toMatchObject({ code: 'invalid-state' });
      expect(
        JSON.parse(
          await readFile(join(journal.directory, 'journal.json'), 'utf8'),
        ),
      ).toMatchObject({
        invocationCount: 1,
        lastInvocation: { state: 'pending' },
      });
    } finally {
      sync.mockRestore();
      await probe.close();
    }
    await closed(journal);
    await expect(
      openDirectRunState({ ...f.input, mode: 'resume' }),
    ).rejects.toMatchObject({ code: 'outcome-unknown' });
  });

  it('refuses linked or public state files and permits a config-parent alias', async () => {
    const f = await fixture();
    await expect(
      openDirectRunState({ ...f.input, mode: 'resume' }),
    ).rejects.toMatchObject({ code: 'run-missing' });
    const journal = await opened({ ...f.input, mode: 'run' });
    await closed(journal);
    const path = join(f.runDirectory, 'journal.json');
    await chmod(path, 0o644);
    await expect(
      openDirectRunState({ ...f.input, mode: 'resume' }),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await chmod(path, 0o600);
    const extra = join(f.directory, 'extra-link');
    await link(path, extra);
    await expect(
      openDirectRunState({ ...f.input, mode: 'resume' }),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await unlink(extra);
    const saved = join(f.runDirectory, 'saved.json');
    await rename(path, saved);
    await symlink(saved, path);
    await expect(
      openDirectRunState({ ...f.input, mode: 'resume' }),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await unlink(path);
    await rename(saved, path);
    const alias = join(f.directory, 'alias');
    await symlink(f.directory, alias, 'dir');
    await closed(
      await opened({
        ...f.input,
        configPath: join(alias, 'config.json'),
        mode: 'resume',
      }),
    );
  });

  it('refuses insecure directories and lock aliases while preserving existing data', async () => {
    const f = await fixture();
    await mkdir(f.base, { mode: 0o755 });
    await expect(
      openDirectRunState({ ...f.input, mode: 'run' }),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    expect(await readdir(f.base)).toEqual([]);
    await chmod(f.base, 0o700);
    const target = join(f.directory, 'untouched');
    await writeFile(target, 'preserve', { mode: 0o600 });
    await symlink(target, f.lockPath);
    await expect(
      openDirectRunState({ ...f.input, mode: 'run' }),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    expect(await readFile(target, 'utf8')).toBe('preserve');
    await unlink(f.lockPath);
    const journal = await opened({ ...f.input, mode: 'run' });
    await closed(journal);
    const original = await stat(f.lockPath);
    await chmod(f.runDirectory, 0o755);
    await expect(
      openDirectRunState({ ...f.input, mode: 'resume' }),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await chmod(f.runDirectory, 0o700);
    await chmod(f.lockPath, 0o644);
    await expect(
      openDirectRunState({ ...f.input, mode: 'resume' }),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await chmod(f.lockPath, 0o600);
    const alias = join(f.directory, 'lock-alias');
    await link(f.lockPath, alias);
    await expect(
      openDirectRunState({ ...f.input, mode: 'resume' }),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await unlink(alias);
    await closed(await opened({ ...f.input, mode: 'resume' }));
    const final = await stat(f.lockPath);
    expect([final.dev, final.ino]).toEqual([original.dev, original.ino]);
  });

  it('does not publish an empty final run directory when initialization fails', async () => {
    const f = await fixture();
    const actual =
      await vi.importActual<typeof import('node:fs/promises')>(
        'node:fs/promises',
      );
    vi.mocked(rename)
      .mockImplementationOnce(actual.rename)
      .mockRejectedValueOnce(new Error('fixture staging publication failure'));
    await expect(
      openDirectRunState({ ...f.input, mode: 'run' }),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await expect(stat(f.runDirectory)).rejects.toMatchObject({
      code: 'ENOENT',
    });
    expect(await readdir(f.base)).toEqual([
      `${f.prepared.config.resourcePrefix}.lock`,
    ]);
    await closed(await opened({ ...f.input, mode: 'run' }));
  });

  it('keeps the maximum safe invocation count compact and refuses overflow', async () => {
    const f = await fixture(Number.MAX_SAFE_INTEGER);
    const journal = await opened({ ...f.input, mode: 'run' });
    const first = await journal.reserveInvocation(f.request());
    await journal.settleInvocation(first);
    await closed(journal);
    const path = join(f.runDirectory, 'journal.json');
    const value = JSON.parse(await readFile(path, 'utf8'));
    value.invocationCount = Number.MAX_SAFE_INTEGER - 1;
    value.lastInvocation.ordinal = value.invocationCount;
    await writeFile(path, JSON.stringify(value));
    const resumed = await opened({ ...f.input, mode: 'resume' });
    const last = await resumed.reserveInvocation(f.request());
    expect(last.ordinal).toBe(Number.MAX_SAFE_INTEGER);
    await resumed.settleInvocation(last);
    await expect(resumed.reserveInvocation(f.request())).rejects.toMatchObject({
      code: 'invocation-budget-exhausted',
    });
    expect((await stat(path)).size).toBeLessThan(2048);
  });

  it.each([
    false,
    true,
  ])('releases a dead process lock while preserving pending=%s and its inode', async (pending) => {
    const f = await fixture();
    const module = new URL(
      '../scripts/direct-credentialed-run-state.mjs',
      import.meta.url,
    ).href;
    const code = `import {openDirectRunState} from ${JSON.stringify(module)};
let input='';for await(const chunk of process.stdin)input+=chunk;
const {options,body,pending}=JSON.parse(input);const journal=await openDirectRunState(options);
if(pending)await journal.reserveInvocation(body);
process.stdout.write(JSON.stringify({ready:true})+'\\n');setInterval(()=>journal.snapshot(),1000);`;
    const child = spawn(process.execPath, ['--input-type=module', '-e', code], {
      stdio: 'pipe',
      env: { PATH: process.env.PATH ?? '' },
    });
    let output = '';
    let errors = '';
    child.stderr.on('data', (chunk) => {
      errors += chunk;
    });
    const ready = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error('fixture owner readiness timed out')),
        10_000,
      );
      const failed = (error: Error) => {
        clearTimeout(timer);
        reject(error);
      };
      child.once('error', failed);
      child.stdin.once('error', failed);
      child.once('close', () =>
        failed(new Error(`fixture owner exited before readiness: ${errors}`)),
      );
      child.stdout.on('data', (chunk) => {
        output += chunk;
        if (output.includes('\n')) {
          clearTimeout(timer);
          try {
            expect(JSON.parse(output)).toEqual({ ready: true });
            resolve();
          } catch (error) {
            reject(error);
          }
        }
      });
    });
    child.stdin.end(
      JSON.stringify({
        options: { ...f.input, mode: 'run' },
        body: f.request({
          kind: 'migration-continue',
          token: { private: CLAIM },
        }),
        pending,
      }),
    );
    const stopped = new Promise<void>((resolve) =>
      child.once('close', () => resolve()),
    );
    try {
      await ready;
      const before = await stat(f.lockPath);
      await expect(
        openDirectRunState({ ...f.input, mode: 'resume' }),
      ).rejects.toMatchObject({ code: 'lock-unavailable' });
      child.kill('SIGKILL');
      await stopped;
      if (pending) {
        await expect(
          openDirectRunState({ ...f.input, mode: 'resume' }),
        ).rejects.toMatchObject({ code: 'outcome-unknown' });
        expect(
          await readFile(join(f.runDirectory, 'journal.json'), 'utf8'),
        ).not.toContain(CLAIM);
      } else {
        await closed(await opened({ ...f.input, mode: 'resume' }));
      }
      const after = await stat(f.lockPath);
      expect([after.dev, after.ino]).toEqual([before.dev, before.ino]);
    } finally {
      if (child.exitCode === null && child.signalCode === null)
        child.kill('SIGKILL');
      await stopped;
    }
  }, 30_000);
});
