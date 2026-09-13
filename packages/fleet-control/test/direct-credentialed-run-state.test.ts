// SPDX-License-Identifier: Apache-2.0

import { spawn } from 'node:child_process';
import {
  chmod,
  link,
  mkdir,
  open,
  readdir,
  readFile,
  rename,
  stat,
  symlink,
  unlink,
  writeFile,
} from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  actionSummary,
  DIRECT_RUN_MAX_JOURNAL_BYTES,
  DIRECT_SCENARIO_ARRAY_MAXIMA,
  DIRECT_SCENARIO_OPERATION_SLOTS,
  type DirectBootstrapMutationReceipt,
  type DirectRunJournal,
  openDirectRunState,
} from '../scripts/direct-credentialed-run-state.mjs';
import type { DirectOperationSlot } from '../scripts/direct-reference-journal.js';
import {
  bootstrapContext,
  cleanupDirectRunState,
  closed,
  completeScenario,
  completeScenarioJournal,
  confirmedBootstrap,
  DIGEST,
  first,
  fixture,
  hash,
  journals,
  MAX_COUNT,
  MAX_NAME,
  type MutableScenario,
  type MutableTeardown,
  maximalScenario,
  maximalTeardown,
  opened,
  PROCESS,
  present,
  RESUMED,
  receipts,
  residualObservation,
  scenarioJournal,
  scenarioWith,
  teardownState,
  teardownWith,
} from './fixtures/direct-run-state-builder.js';

vi.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>();
  return { ...actual, rename: vi.fn(actual.rename) };
});

const CLAIM = 'opaque-claim-must-not-enter-local-state';

afterEach(async () => {
  vi.restoreAllMocks();
  await cleanupDirectRunState();
});

const describeLinux =
  process.platform === 'linux' ? describe.sequential : describe.skip;

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

const refuses = (journal: DirectRunJournal, state: MutableScenario) =>
  expect(journal.recordScenario(state)).rejects.toMatchObject({
    code: 'invalid-state',
  });

describeLinux('durable scenario state', () => {
  it('projects and round-trips a drain summary with both expected counters', async () => {
    const action = {
      kind: 'tenant-fence' as const,
      role: 'a' as const,
      operation: 'drain' as const,
      expectedMutationEpoch: 0,
      expectedRevision: 2,
    };
    expect(actionSummary(action)).toEqual(action);
    const { f, journal } = await scenarioJournal();
    const state = scenarioWith((value) => {
      present(value.lastCall).action = action;
      present(value.mutation).action = action;
    });
    await journal.recordScenario(state);
    await closed(journal);
    const resumed = await opened({ ...f.input, mode: 'resume' });
    expect(resumed.snapshot().scenario?.lastCall?.action).toEqual(action);
    expect(resumed.snapshot().scenario?.mutation?.action).toEqual(action);
  });

  it.each([
    'expectedMutationEpoch',
    'expectedRevision',
  ] as const)('refuses a non-integer %s in a journalled drain call', async (field) => {
    const { journal } = await scenarioJournal();
    await refuses(
      journal,
      scenarioWith((state) => {
        present(state.lastCall).action = {
          kind: 'tenant-fence',
          role: 'a',
          operation: 'drain',
          expectedMutationEpoch: 0,
          expectedRevision: 2,
          [field]: 0.5,
        };
      }),
    );
  });

  it('decodes an action summary written before the expected counters existed', async () => {
    const { journal } = await scenarioJournal();
    const state = maximalScenario();
    const summary = present(state.lastCall).action;
    expect(summary).not.toHaveProperty('expectedMutationEpoch');
    expect(summary).not.toHaveProperty('expectedRevision');
    await journal.recordScenario(state);
    expect(journal.snapshot().scenario?.lastCall?.action).toEqual(summary);
  });

  it('decodes a fence group with every role entry null', async () => {
    const { journal } = await scenarioJournal();
    const state = scenarioWith((value) => {
      value.proofs.fence = {
        drain: { a: null, b: null },
        sweeps: { a: null, b: null },
        reopen: { a: null, b: null },
        probes: { a: null, b: null },
      };
    });
    await journal.recordScenario(state);
    expect(journal.snapshot().scenario?.proofs.fence).toEqual(
      state.proofs.fence,
    );
  });

  it('decodes partial fence transitions and sweeps with nullable later members', async () => {
    const { journal } = await scenarioJournal();
    const state = scenarioWith((value) => {
      for (const role of ['a', 'b'] as const) {
        for (const group of ['drain', 'reopen'] as const) {
          const entry = present(value.proofs.fence[group][role]);
          entry.before = {
            state: 'open',
            mutationEpoch: 0,
            requireMutationEpoch: false,
            transitionRevision: 0,
          };
          entry.after = null;
          entry.ordinal = null;
        }
        const sweep = present(value.proofs.fence.sweeps[role]);
        sweep.second = null;
        sweep.intervalMs = null;
        first(sweep.first.categories).empty = true;
        first(sweep.first.categories).class = 'work';
      }
    });
    await journal.recordScenario(state);
    expect(journal.snapshot().scenario?.proofs.fence).toEqual(
      state.proofs.fence,
    );
  });

  it('decodes filled fence evidence with a zero sweep interval', async () => {
    const { journal } = await scenarioJournal();
    const state = completeScenario();
    for (const role of ['a', 'b'] as const)
      present(state.proofs.fence.sweeps[role]).intervalMs = 0;
    await journal.recordScenario(state);
    expect(journal.snapshot().scenario?.proofs.fence).toEqual(
      state.proofs.fence,
    );
  });

  it('refuses an unknown fence proof key', async () => {
    const { journal } = await scenarioJournal();
    await refuses(
      journal,
      scenarioWith((state) => {
        Object.assign(state.proofs.fence, { unknown: null });
      }),
    );
  });

  it('refuses one sweep category past DIRECT_SCENARIO_ARRAY_MAXIMA.inventoryCategories', async () => {
    const { journal } = await scenarioJournal();
    await refuses(
      journal,
      scenarioWith((state) => {
        const sweep = present(state.proofs.fence.sweeps.a).first;
        sweep.categories = Array.from(
          { length: DIRECT_SCENARIO_ARRAY_MAXIMA.inventoryCategories + 1 },
          () => structuredClone(first(sweep.categories)),
        );
      }),
    );
  });

  it.each([
    'a',
    'b',
  ] as const)('refuses null before and first members for fence role %s', async (role) => {
    const { journal } = await scenarioJournal();
    for (const group of ['drain', 'reopen'] as const)
      await refuses(
        journal,
        scenarioWith((state) => {
          Object.assign(present(state.proofs.fence[group][role]), {
            before: null,
          });
        }),
      );
    await refuses(
      journal,
      scenarioWith((state) => {
        Object.assign(present(state.proofs.fence.sweeps[role]), {
          first: null,
        });
      }),
    );
  });

  it.each([
    'a',
    'b',
  ] as const)('refuses fence ordinals above invocationCount for role %s', async (role) => {
    const { journal } = await scenarioJournal();
    for (const group of ['drain', 'reopen', 'probes'] as const)
      await refuses(
        journal,
        scenarioWith((state) => {
          present(state.proofs.fence[group][role]).ordinal = 4;
        }),
      );
    for (const member of ['first', 'second'] as const)
      await refuses(
        journal,
        scenarioWith((state) => {
          present(present(state.proofs.fence.sweeps[role])[member]).ordinal = 4;
        }),
      );
  });

  it.each([
    'a',
    'b',
  ] as const)('refuses a null drain ordinal past fence-drain for role %s', async (role) => {
    const { journal } = await scenarioJournal();
    const state = maximalScenario();
    state.phase = 'migration-start';
    present(state.proofs.fence.drain[role]).ordinal = null;
    await refuses(journal, state);
  });

  it.each([
    'a',
    'b',
  ] as const)('refuses a null reopen ordinal past fence-reopen for role %s', async (role) => {
    const { journal } = await scenarioJournal();
    const state = completeScenario();
    state.phase = 'fence-proofs';
    present(state.proofs.fence.reopen[role]).ordinal = null;
    await refuses(journal, state);
  });

  it.each([
    'a',
    'b',
  ] as const)('preserves established fence entries and their populated members for role %s', async (role) => {
    const { journal } = await scenarioJournal();
    const baseline = maximalScenario();
    await journal.recordScenario(baseline);
    const rejectChange = async (mutate: (state: MutableScenario) => void) => {
      const state = structuredClone(baseline);
      mutate(state);
      await refuses(journal, state);
    };
    for (const group of ['drain', 'reopen', 'sweeps', 'probes'] as const)
      await rejectChange((state) => {
        state.proofs.fence[group][role] = null;
      });
    for (const group of ['drain', 'reopen'] as const) {
      await rejectChange((state) => {
        present(state.proofs.fence[group][role]).before.transitionRevision--;
      });
      for (const reset of [false, true]) {
        await rejectChange((state) => {
          const entry = present(state.proofs.fence[group][role]);
          if (reset) entry.after = null;
          else present(entry.after).transitionRevision--;
        });
        await rejectChange((state) => {
          present(state.proofs.fence[group][role]).ordinal = reset ? null : 2;
        });
      }
    }
    await rejectChange((state) => {
      present(state.proofs.fence.sweeps[role]).first.observedAt--;
    });
    for (const reset of [false, true]) {
      await rejectChange((state) => {
        const entry = present(state.proofs.fence.sweeps[role]);
        if (reset) entry.second = null;
        else present(entry.second).observedAt--;
      });
      await rejectChange((state) => {
        present(state.proofs.fence.sweeps[role]).intervalMs = reset ? null : 0;
      });
    }
    for (const member of ['mutationEpoch', 'ordinal'] as const)
      await rejectChange((state) => {
        present(state.proofs.fence.probes[role])[member]--;
      });
    expect(journal.snapshot().scenario?.proofs.fence).toEqual(
      baseline.proofs.fence,
    );
  });

  it('completes a null transition after and ordinal on an established fence entry', async () => {
    const { journal } = await scenarioJournal();
    const state = maximalScenario();
    for (const group of ['drain', 'reopen'] as const)
      for (const role of ['a', 'b'] as const) {
        const entry = present(state.proofs.fence[group][role]);
        entry.after = null;
        entry.ordinal = null;
      }
    await journal.recordScenario(state);
    const complete = maximalScenario();
    await journal.recordScenario(complete);
    expect(journal.snapshot().scenario?.proofs.fence).toEqual(
      complete.proofs.fence,
    );
  });

  it('completes a null second sweep and interval with a zero interval', async () => {
    const { journal } = await scenarioJournal();
    const state = maximalScenario();
    for (const role of ['a', 'b'] as const) {
      const entry = present(state.proofs.fence.sweeps[role]);
      entry.second = null;
      entry.intervalMs = null;
    }
    await journal.recordScenario(state);
    const complete = maximalScenario();
    for (const role of ['a', 'b'] as const)
      present(complete.proofs.fence.sweeps[role]).intervalMs = 0;
    await journal.recordScenario(complete);
    expect(journal.snapshot().scenario?.proofs.fence).toEqual(
      complete.proofs.fence,
    );
  });

  it('fills a null fence entry with its partial transition and first sweep', async () => {
    const { journal } = await scenarioJournal();
    const state = maximalScenario();
    state.proofs.fence.drain = { a: null, b: null };
    state.proofs.fence.sweeps = { a: null, b: null };
    await journal.recordScenario(state);
    const partial = maximalScenario();
    for (const role of ['a', 'b'] as const) {
      const transition = present(partial.proofs.fence.drain[role]);
      transition.after = null;
      transition.ordinal = null;
      const sweeps = present(partial.proofs.fence.sweeps[role]);
      sweeps.second = null;
      sweeps.intervalMs = null;
    }
    await journal.recordScenario(partial);
    expect(journal.snapshot().scenario?.proofs.fence).toEqual(
      partial.proofs.fence,
    );
  });

  it('publishes a maximal scenario inside the journal byte bound', async () => {
    const { f, journal } = await scenarioJournal();
    await journal.recordScenario(maximalScenario());
    const serialized = await readFile(
      join(f.runDirectory, 'journal.json'),
      'utf8',
    );
    console.log(
      'A1_MAXIMAL_SCENARIO_JOURNAL_BYTES',
      Buffer.byteLength(serialized),
    );
    expect(Buffer.byteLength(serialized)).toBeLessThan(
      DIRECT_RUN_MAX_JOURNAL_BYTES - 118 * 1024,
    );
    expect(journal.snapshot().scenario?.proofs.cleanup?.evidence.scan).toEqual({
      discover: { evidenceSha256: DIGEST, evidenceCount: MAX_COUNT },
      verify: { evidenceSha256: DIGEST, evidenceCount: MAX_COUNT },
    });
  });

  it('publishes the journal fields in the order the decoder establishes', async () => {
    const { f, journal } = await scenarioJournal();
    await journal.recordScenario(maximalScenario());
    const serialized = await readFile(
      join(f.runDirectory, 'journal.json'),
      'utf8',
    );
    expect(Object.keys(JSON.parse(serialized))).toEqual([
      'version',
      'binding',
      'invocationCount',
      'lastInvocation',
      'bootstrap',
      'scenario',
    ]);
  });

  it('refuses a stored journal larger than the byte bound before parsing it', async () => {
    const { f, journal } = await scenarioJournal();
    await journal.recordScenario(maximalScenario());
    const path = join(f.runDirectory, 'journal.json');
    const serialized = await readFile(path, 'utf8');
    await closed(journal);
    await writeFile(
      path,
      serialized.padEnd(DIRECT_RUN_MAX_JOURNAL_BYTES + 1, ' '),
    );
    await expect(
      openDirectRunState({ ...f.input, mode: 'resume' }),
    ).rejects.toMatchObject({ code: 'invalid-state' });
  });

  it('refuses ordinals the durable invocation count cannot account for', async () => {
    const { journal } = await scenarioJournal();
    for (const mutate of [
      (state: MutableScenario) => {
        state.startedOrdinal = 4;
      },
      (state: MutableScenario) => {
        state.callCount = 4;
      },
      (state: MutableScenario) => {
        state.callCount = 2;
      },
      (state: MutableScenario) => {
        state.reconciledOrdinal = 4;
      },
      (state: MutableScenario) => {
        state.phaseCalls['provision-a'] = 2;
      },
      (state: MutableScenario) => {
        present(state.lastCall).ordinal = 4;
      },
      (state: MutableScenario) => {
        present(state.lastCall).attempts = null;
      },
      (state: MutableScenario) => {
        present(state.proofs.exports.a).sourceInvocationOrdinal = 4;
      },
      (state: MutableScenario) => {
        first(state.proofs.health).ordinal = 4;
      },
      (state: MutableScenario) => {
        present(state.proofs.restart).replayOrdinal = 1;
      },
    ])
      await refuses(journal, scenarioWith(mutate));
    await journal.recordScenario(maximalScenario());
  });

  it('refuses a restart proof whose resumed process is the process that recorded the loss', async () => {
    const { journal } = await scenarioJournal();
    await refuses(
      journal,
      scenarioWith((state) => {
        present(state.proofs.restart).resumedProcess = { ...PROCESS };
      }),
    );
    await journal.recordScenario(maximalScenario());
  });

  it('refuses duplicate operation slots and duplicate record roles', async () => {
    const { journal } = await scenarioJournal();
    await refuses(
      journal,
      scenarioWith((state) => {
        state.operations = [first(state.operations), first(state.operations)];
      }),
    );
    await refuses(
      journal,
      scenarioWith((state) => {
        state.records = [first(state.records), first(state.records)];
      }),
    );
  });

  it('refuses a later phase whose implied proofs are absent', async () => {
    const { journal } = await scenarioJournal();
    await refuses(
      journal,
      scenarioWith((state) => {
        state.phase = 'provision-b';
        state.proofs.initial.a = null;
      }),
    );
    await refuses(
      journal,
      scenarioWith((state) => {
        state.phase = 'provision-b';
        state.proofs.objects.a = null;
      }),
    );
    await refuses(
      journal,
      scenarioWith((state) => {
        state.phase = 'provision-b';
        state.proofs.health = state.proofs.health.filter(
          (entry) => entry.role !== 'a' || entry.release !== '1',
        );
      }),
    );
  });

  it('refuses a settlement-effect list and an operation list past the schema bound', async () => {
    const { journal } = await scenarioJournal();
    for (const mutate of [
      (state: MutableScenario) => {
        state.proofs.effects.push(first(state.proofs.effects));
      },
      (state: MutableScenario) => {
        state.operations.push(first(state.operations));
      },
    ])
      await refuses(journal, scenarioWith(mutate));
  });

  it('carries every operation slot the reference control read can return', () => {
    const slots: readonly DirectOperationSlot[] =
      DIRECT_SCENARIO_OPERATION_SLOTS;
    const unlisted: Exclude<
      DirectOperationSlot,
      (typeof DIRECT_SCENARIO_OPERATION_SLOTS)[number]
    > extends never
      ? true
      : false = true;
    expect({ count: slots.length, unlisted }).toEqual({
      count: 12,
      unlisted: true,
    });
  });

  it('refuses one entry past each cap the shared array maxima declare', async () => {
    const { journal } = await scenarioJournal();
    const caps = new Map<string, number>();
    for (const [key, value] of Object.entries(DIRECT_SCENARIO_ARRAY_MAXIMA))
      if (typeof value === 'number') caps.set(key, value);
      else
        for (const [nested, bound] of Object.entries(value))
          caps.set(`inventory.${nested}`, bound);
    const bounded = (state: MutableScenario): Record<string, unknown[]> => {
      const inventory = present(state.proofs.inventories.before);
      return {
        health: state.proofs.health,
        steps: state.proofs.steps,
        exportVerifications: state.proofs.exportVerifications,
        auditFindings: present(state.proofs.audits.before).findings,
        footprintVersionIds: present(
          present(state.proofs.force).worker.currentVersionIds,
        ),
        deploymentVersions: present(state.proofs.initial.a).currentDeployment
          .versions,
        inventoryCategories: present(present(state.proofs.fence.sweeps.a).first)
          .categories,
        'inventory.databaseIds': inventory.databaseIds,
        'inventory.namespaceIds': inventory.namespaceIds,
        'inventory.scriptNames': inventory.scriptNames,
        'inventory.bucketNames': inventory.bucketNames,
        'inventory.findings': inventory.findings,
      };
    };
    expect(Object.keys(bounded(maximalScenario())).sort()).toEqual(
      [...caps.keys()].sort(),
    );
    for (const [key, cap] of caps) {
      const state = maximalScenario();
      const array = bounded(state)[key];
      if (!array) throw new Error(`unbounded scenario array ${key}`);
      expect({ key, length: array.length }).toEqual({ key, length: cap });
      array.push(first(array));
      await refuses(journal, state);
    }
  });

  it('refuses a scenario version the decoder does not implement', async () => {
    const { f, journal } = await scenarioJournal();
    await expect(
      journal.recordScenario(
        scenarioWith((state) => {
          (state as unknown as { version: number }).version = 2;
        }),
      ),
    ).rejects.toMatchObject({ code: 'unsupported-scenario-version' });
    await journal.recordScenario(maximalScenario());
    await closed(journal);
    const path = join(f.runDirectory, 'journal.json');
    const stored = JSON.parse(await readFile(path, 'utf8'));
    stored.scenario.version = 2;
    await writeFile(path, JSON.stringify(stored));
    await expect(
      openDirectRunState({ ...f.input, mode: 'resume' }),
    ).rejects.toMatchObject({ code: 'unsupported-scenario-version' });
  });

  it('refuses to load the journal module when a declared maximum is absent', async () => {
    const source = new URL(
      '../scripts/direct-credentialed-run-state.mjs',
      import.meta.url,
    );
    const text = await readFile(source, 'utf8');
    const declarations: [string, string][] = [];
    for (const [key, value] of Object.entries(DIRECT_SCENARIO_ARRAY_MAXIMA))
      if (typeof value === 'number')
        declarations.push([key, `  ${key}: ${value},\n`]);
      else
        for (const [nested, bound] of Object.entries(value))
          declarations.push([
            `inventory.${nested}`,
            `    ${nested}: ${bound},\n`,
          ]);
    const load = async (code: string) => {
      const child = spawn(
        process.execPath,
        ['--input-type=module', '-e', code],
        {
          cwd: fileURLToPath(new URL('..', import.meta.url)),
          stdio: ['ignore', 'ignore', 'pipe'],
          env: { PATH: process.env.PATH ?? '' },
        },
      );
      let stderr = '';
      child.stderr.on('data', (chunk) => {
        stderr += String(chunk);
      });
      const status = await new Promise<number | null>((resolve, reject) => {
        child.once('error', reject);
        child.once('close', (value) => resolve(value));
      });
      return { status, refused: stderr.includes('invalid-state') };
    };
    const absolute = (value: string) =>
      value.replace(
        /from '\.\/([^']+)'/gu,
        (_match, name: string) =>
          `from ${JSON.stringify(new URL(name, source).href)}`,
      );
    expect(await load(absolute(text))).toEqual({ status: 0, refused: false });
    for (const [key, declaration] of declarations) {
      expect({ key, occurrences: text.split(declaration).length - 1 }).toEqual({
        key,
        occurrences: 1,
      });
      expect({
        key,
        ...(await load(absolute(text.replace(declaration, '')))),
      }).toEqual({ key, status: 1, refused: true });
    }
  }, 60_000);

  it('accepts the optional refusal detail and refuses one outside the vocabulary', async () => {
    const unknown = await scenarioJournal();
    await refuses(
      unknown.journal,
      scenarioWith((state) => {
        (present(state.failure) as { detail?: string }).detail = 'not-a-detail';
      }),
    );
    await unknown.journal.recordScenario(maximalScenario());
    expect(unknown.journal.snapshot().scenario?.failure).toEqual({
      code: 'observation-mismatch',
      ordinal: MAX_COUNT,
    });
    const carried = await scenarioJournal();
    await carried.journal.recordScenario(
      scenarioWith((state) => {
        present(state.failure).detail = 'run-reserve';
      }),
    );
    expect(carried.journal.snapshot().scenario?.failure).toEqual({
      code: 'observation-mismatch',
      ordinal: MAX_COUNT,
      detail: 'run-reserve',
    });
  });

  it('refuses a phase regression, a phase skip and a weakened proof after publication', async () => {
    const { journal } = await scenarioJournal();
    await journal.recordScenario(maximalScenario());
    await refuses(
      journal,
      scenarioWith((state) => {
        state.phase = 'inventory-before';
      }),
    );
    await journal.recordScenario(
      scenarioWith((state) => {
        state.phase = 'provision-b';
      }),
    );
    await refuses(journal, maximalScenario());
    for (const mutate of [
      (state: MutableScenario) => {
        state.startedOrdinal = 1;
      },
      (state: MutableScenario) => {
        state.attempts.provider = 0;
      },
      (state: MutableScenario) => {
        state.sdkRequests = 3;
      },
      (state: MutableScenario) => {
        state.phaseCalls['provision-a'] = 2;
        state.phaseCalls['provision-b'] = 1;
      },
      (state: MutableScenario) => {
        state.proofs.steps.pop();
      },
      (state: MutableScenario) => {
        first(state.proofs.steps).beforeCursor = 5;
      },
      (state: MutableScenario) => {
        state.proofs.objects.a = { size: 30, sha256: DIGEST };
      },
      (state: MutableScenario) => {
        present(state.proofs.exports.a).sourceInvocationOrdinal = 0;
      },
      (state: MutableScenario) => {
        present(state.proofs.restart).resumedProcess = { ...RESUMED, pid: 3 };
      },
      (state: MutableScenario) => {
        present(state.failure).code = 'proof-unavailable';
      },
    ])
      await refuses(
        journal,
        scenarioWith((state) => {
          state.phase = 'provision-b';
          mutate(state);
        }),
      );
  });
});

const settlement = (ordinal = 1) => ({ ordinal, settledByReread: false });

function filledReceipts(state: MutableTeardown) {
  state.receipts.ingress = settlement();
  state.receipts.worker = {
    scriptName: 'reference',
    secretNames: [],
    ...settlement(),
  };
  state.receipts.fleet = { uuid: 'fleet-uuid', ...settlement() };
  state.receipts.quota = { uuid: 'quota-uuid', ...settlement() };
}

describeLinux('durable teardown state', () => {
  it('publishes teardown after scenario and leaves the earlier bytes unchanged', async () => {
    const { f, journal } = await completeScenarioJournal();
    const path = join(f.runDirectory, 'journal.json');
    const before = await readFile(path, 'utf8');
    expect(before).not.toContain('teardown');
    await journal.recordTeardown(teardownState());
    const after = JSON.parse(await readFile(path, 'utf8'));
    expect(Object.keys(after)).toEqual([
      'version',
      'binding',
      'invocationCount',
      'lastInvocation',
      'bootstrap',
      'scenario',
      'teardown',
    ]);
    delete after.teardown;
    expect(`${JSON.stringify(after)}\n`).toBe(before);
  });

  it('refuses a teardown key on a version 1 snapshot', async () => {
    const { f, journal } = await completeScenarioJournal();
    await journal.recordTeardown(teardownState());
    await closed(journal);
    const path = join(f.runDirectory, 'journal.json');
    const stored = JSON.parse(await readFile(path, 'utf8'));
    await writeFile(
      path,
      JSON.stringify({
        version: 1,
        binding: stored.binding,
        invocationCount: stored.invocationCount,
        lastInvocation: stored.lastInvocation,
        teardown: stored.teardown,
      }),
    );
    await expect(
      openDirectRunState({ ...f.input, mode: 'resume' }),
    ).rejects.toMatchObject({ code: 'invalid-state' });
  });

  it('refuses every malformed teardown shape', async () => {
    const { journal } = await completeScenarioJournal();
    const cases: ((state: MutableTeardown) => unknown)[] = [
      (state) => {
        (state as unknown as Record<string, unknown>).extra = 1;
      },
      (state) => {
        state.phase = 'worker';
        state.receipts.worker = {
          scriptName: 'reference',
          secretNames: [],
          ...settlement(),
        };
      },
      (state) => {
        state.pending = { kind: 'disable-reference-ingress' };
        state.receipts.ingress = settlement();
      },
      (state) => {
        state.pending = { kind: 'disable-reference-ingress', key: 'key' };
      },
      (state) => {
        state.pending = { kind: 'delete-export-object' };
      },
      (state) => {
        filledReceipts(state);
        state.phase = 'export-objects';
        state.pending = { kind: 'delete-export-object', key: 'first' };
        state.receipts.exportObjects = [{ key: 'first', ...settlement() }];
      },
      (state) => {
        filledReceipts(state);
        state.phase = 'export-objects';
        state.receipts.exportObjects = ['one', 'two', 'three'].map((key) => ({
          key,
          ...settlement(),
        }));
      },
      (state) => {
        filledReceipts(state);
        state.phase = 'export-objects';
        state.receipts.exportObjects = [
          { key: 'same', ...settlement() },
          { key: 'same', ...settlement() },
        ];
      },
      (state) => {
        filledReceipts(state);
        state.phase = 'worker';
        state.receipts.worker = {
          scriptName: `${MAX_NAME}x`,
          secretNames: [],
          ...settlement(),
        };
      },
      (state) => {
        state.residual = residualObservation();
      },
      (state) => {
        state.phase = 'refused';
      },
      (state) => {
        state.phase = 'refused';
        state.failure = 'scenario-incomplete';
        state.receipts.ingress = settlement();
      },
      (state) => {
        state.phase = 'refused';
        state.failure = 'scenario-incomplete';
        state.pending = { kind: 'disable-reference-ingress' };
      },
    ];
    for (const [index, mutate] of cases.entries())
      await expect({
        index,
        outcome: await journal.recordTeardown(teardownWith(mutate)).then(
          () => 'accepted',
          (error: { code?: string }) => error.code,
        ),
      }).toEqual({ index, outcome: 'invalid-state' });
  });

  it('publishes a receipt while its mutation is pending and refuses every regression', async () => {
    const { journal } = await completeScenarioJournal();
    await journal.recordTeardown(
      teardownWith((state) => {
        state.pending = { kind: 'disable-reference-ingress' };
      }),
    );
    expect(journal.snapshot().teardown?.pending).toEqual({
      kind: 'disable-reference-ingress',
    });
    await journal.recordTeardown(
      teardownWith((state) => {
        state.receipts.ingress = settlement(4);
        state.providerRequests = 4;
      }),
    );
    await journal.recordTeardown(
      teardownWith((state) => {
        state.phase = 'worker';
        state.receipts.ingress = settlement(4);
        state.receipts.worker = {
          scriptName: 'reference',
          secretNames: [],
          ...settlement(6),
        };
        state.providerRequests = 6;
      }),
    );
    const carried = (state: MutableTeardown) => {
      state.phase = 'worker';
      state.receipts.ingress = settlement(4);
      state.receipts.worker = {
        scriptName: 'reference',
        secretNames: [],
        ...settlement(6),
      };
      state.providerRequests = 6;
    };
    for (const mutate of [
      (state: MutableTeardown) => {
        carried(state);
        state.phase = 'ingress';
      },
      (state: MutableTeardown) => {
        carried(state);
        state.phase = 'quota';
        state.receipts.fleet = { uuid: 'fleet-uuid', ...settlement(8) };
      },
      (state: MutableTeardown) => {
        carried(state);
        state.receipts.ingress = settlement(5);
      },
      (state: MutableTeardown) => {
        carried(state);
        state.providerRequests = 5;
      },
      (state: MutableTeardown) => {
        carried(state);
        state.receipts.worker = null;
      },
    ])
      await expect(
        journal.recordTeardown(teardownWith(mutate)),
      ).rejects.toMatchObject({ code: 'invalid-state' });
  });

  it('keeps a refused teardown terminal and rewritable in place', async () => {
    const { journal } = await completeScenarioJournal();
    const refused = (attempts: number) =>
      teardownWith((state) => {
        state.phase = 'refused';
        state.failure = 'scenario-incomplete';
        state.residual = {
          ...residualObservation(),
          settleAttempts: attempts,
        };
        state.providerRequests = attempts;
      });
    await journal.recordTeardown(refused(1));
    await journal.recordTeardown(refused(2));
    expect(journal.snapshot().teardown?.residual?.settleAttempts).toBe(2);
    await expect(
      journal.recordTeardown(
        teardownWith((state) => {
          state.pending = { kind: 'disable-reference-ingress' };
          state.providerRequests = 2;
        }),
      ),
    ).rejects.toMatchObject({ code: 'invalid-state' });
  });

  it('accepts the worst-case teardown inside the byte bound and refuses an undecodable one without poisoning', async () => {
    const { f, journal } = await completeScenarioJournal();
    await journal.assertTeardownCapacity(maximalTeardown());
    expect(journal.snapshot().teardown).toBeUndefined();
    await expect(
      journal.assertTeardownCapacity(
        teardownWith((state) => {
          Object.assign(state, maximalTeardown());
          const worker = present(state.receipts.worker);
          worker.scriptName = `${MAX_NAME}x`;
        }),
      ),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await journal.recordTeardown(maximalTeardown());
    const serialized = await readFile(
      join(f.runDirectory, 'journal.json'),
      'utf8',
    );
    console.log(
      'A1_COMPLETE_TEARDOWN_JOURNAL_BYTES',
      Buffer.byteLength(serialized),
    );
    console.log(
      'A1_COMPLETE_TEARDOWN_JOURNAL_FREE_BYTES',
      DIRECT_RUN_MAX_JOURNAL_BYTES - Buffer.byteLength(serialized),
    );
    expect(
      DIRECT_RUN_MAX_JOURNAL_BYTES - Buffer.byteLength(serialized),
    ).toBeGreaterThan(90 * 1024);
  });

  it('refuses scenario and invocation writes once a teardown is pending or receipted', async () => {
    const { f, journal } = await completeScenarioJournal();
    await journal.recordTeardown(
      teardownWith((state) => {
        state.pending = { kind: 'disable-reference-ingress' };
      }),
    );
    for (const call of [
      () => journal.recordScenario(completeScenario()),
      () => journal.reserveInvocation(f.request()),
      () => journal.bindBootstrapContext(bootstrapContext(f)),
      () => journal.beginBootstrapMutation('create-fleet-d1'),
      () =>
        journal.recordBootstrapObservation({
          kind: 'active',
          deploymentId: 'deployment',
          versionId: 'version',
        }),
    ])
      await expect(call()).rejects.toMatchObject({ code: 'outcome-unknown' });
    await journal.recordTeardown(
      teardownWith((state) => {
        state.receipts.ingress = settlement();
      }),
    );
    await expect(
      journal.recordScenario(completeScenario()),
    ).rejects.toMatchObject({ code: 'invalid-state' });
    await expect(journal.reserveInvocation(f.request())).rejects.toMatchObject({
      code: 'invalid-state',
    });
  });

  it('resumes a run whose teardown mutation is still pending', async () => {
    const { f, journal } = await completeScenarioJournal();
    await journal.recordTeardown(
      teardownWith((state) => {
        state.pending = { kind: 'disable-reference-ingress' };
      }),
    );
    await closed(journal);
    const resumed = await opened({ ...f.input, mode: 'resume' });
    expect(resumed.snapshot().teardown?.pending).toEqual({
      kind: 'disable-reference-ingress',
    });
  });
});
