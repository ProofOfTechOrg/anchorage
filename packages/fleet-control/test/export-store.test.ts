// SPDX-License-Identifier: Apache-2.0

import { execFile } from 'node:child_process';
import { createHash } from 'node:crypto';
import type { BigIntStats, PathLike } from 'node:fs';
import {
  chmod,
  link,
  lstat,
  mkdir,
  mkdtemp,
  open,
  readdir,
  readFile,
  realpath,
  rm,
  stat,
  symlink,
  unlink,
  writeFile,
} from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { isAbsolute, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { promisify } from 'node:util';
import { runInNewContext } from 'node:vm';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  databaseExportIntegrityFromUnknown,
  databaseExportReceiptAuthorityFromUnknown,
  databaseExportReceiptIdentityFromUnknown,
  isDatabaseExportReceiptError,
} from '../src/database-export-store.js';
import {
  createFileSystemDatabaseExportStoreWithReceiptPrimitives,
  FileSystemDatabaseExportStore,
} from '../src/export-store.js';
import type {
  DatabaseExportIntegrity,
  DatabaseExportReceiptIdentity,
} from '../src/types.js';

const encoder = new TextEncoder();
const execFileAsync = promisify(execFile);
const temporaryDirectories: string[] = [];
const DATABASE_ID = '01234567-89ab-cdef-0123-456789abcdef';
const OPERATION_ID = '12345678-1234-4234-9234-123456789abc';
const SECOND_OPERATION_ID = '22345678-1234-4234-9234-123456789abc';
const THIRD_OPERATION_ID = '32345678-1234-4234-9234-123456789abc';

function integrity(value: string): DatabaseExportIntegrity {
  return {
    size: encoder.encode(value).byteLength,
    sha256: createHash('sha256').update(value).digest('hex'),
  };
}

function receiptIdentity(
  store: FileSystemDatabaseExportStore,
  operationId = OPERATION_ID,
): DatabaseExportReceiptIdentity {
  const authority = store.receiptAuthority;
  if (authority === undefined) {
    throw new Error('filesystem receipt capability is unavailable');
  }
  return {
    version: 1,
    authority,
    databaseId: DATABASE_ID,
    operationId,
  };
}

async function writeReceipt(
  store: FileSystemDatabaseExportStore,
  value: string,
  operationId = OPERATION_ID,
  expected = integrity(value),
) {
  const writer = store.writeReceipt;
  if (writer === undefined) {
    throw new Error('filesystem receipt capability is unavailable');
  }
  return writer({
    identity: receiptIdentity(store, operationId),
    body: body(value),
    contentLength: encoder.encode(value).byteLength,
    expectedIntegrity: Promise.resolve(expected),
  });
}

function receiptDirectory(root: string): string {
  return join(root, '.anchorage-receipts', 'v1', DATABASE_ID);
}

function receiptTarget(root: string, operationId = OPERATION_ID): string {
  return join(receiptDirectory(root), `${operationId}.sql`);
}

function trackedOpen(paths: WeakMap<object, string>): typeof open {
  return (async (
    path: PathLike,
    flags: string | number,
    mode?: string | number,
  ) => {
    const handle = await open(path, flags, mode);
    paths.set(handle, String(path));
    return handle;
  }) as typeof open;
}

function withBigIntIdentity(
  statistics: BigIntStats,
  device: bigint,
  inode: bigint,
  size = statistics.size,
): BigIntStats {
  return new Proxy(statistics, {
    get(target, property, receiver) {
      if (property === 'dev') return device;
      if (property === 'ino') return inode;
      if (property === 'size') return size;
      return Reflect.get(target, property, receiver);
    },
  });
}

async function temporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), 'fleet-export-store-'));
  temporaryDirectories.push(directory);
  return directory;
}

function body(...chunks: readonly string[]): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      controller.close();
    },
  });
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

describe('FileSystemDatabaseExportStore', () => {
  it('streams, syncs, closes, and publishes an absolute durable location', async () => {
    const parent = await temporaryDirectory();
    const root = join(parent, 'nested', 'exports');
    const store = new FileSystemDatabaseExportStore(root);

    const result = await store.write({
      databaseId: 'database-1',
      fileName: 'database-1.sql',
      body: body('CREATE ', 'TABLE example;'),
      contentLength: 21,
    });

    const location = fileURLToPath(result.location);
    expect(result.location).toMatch(/^file:\/\//);
    expect(result).toMatchObject({
      size: 21,
      sha256: createHash('sha256')
        .update('CREATE TABLE example;')
        .digest('hex'),
    });
    expect(isAbsolute(location)).toBe(true);
    expect(location).toBe(join(await realpath(root), 'database-1.sql'));
    await expect(readFile(location, 'utf8')).resolves.toBe(
      'CREATE TABLE example;',
    );
    expect((await stat(location)).mode & 0o777).toBe(0o600);
    await expect(readdir(root)).resolves.toEqual(['database-1.sql']);

    const empty = await store.write({
      databaseId: 'database-1',
      fileName: 'empty.sql',
      body: body(),
      contentLength: 0,
    });
    expect(empty).toEqual({
      location: pathToFileURL(join(await realpath(root), 'empty.sql')).href,
      size: 0,
      sha256: createHash('sha256').update('').digest('hex'),
    });
  });

  it.each([
    '',
    '.',
    '..',
    '../outside.sql',
    'nested/export.sql',
    'nested\\export.sql',
    '/tmp/outside.sql',
    'C:\\outside.sql',
    'export.sql:alternate',
    'nul\0byte.sql',
    '.. ',
    'CON.sql',
    'trailing.',
    'line\nbreak.sql',
  ])('rejects escaping or non-segment file name %j', async (fileName) => {
    const parent = await temporaryDirectory();
    const root = join(parent, 'exports');
    const store = new FileSystemDatabaseExportStore(root);

    await expect(
      store.write({
        databaseId: 'database-1',
        fileName,
        body: body('private export'),
      }),
    ).rejects.toThrow(/one portable path segment/);
    await expect(readdir(parent)).resolves.toEqual([]);
  });

  it('keeps the previous target intact until the complete stream is renamed', async () => {
    const parent = await temporaryDirectory();
    const root = join(parent, 'exports');
    await mkdir(root);
    const target = join(root, 'database-1.sql');
    await writeFile(target, 'previous export');
    let streamController:
      | ReadableStreamDefaultController<Uint8Array>
      | undefined;
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        streamController = controller;
        controller.enqueue(encoder.encode('replacement export'));
      },
    });
    const store = new FileSystemDatabaseExportStore(root);

    const pending = store.write({
      databaseId: 'database-1',
      fileName: 'database-1.sql',
      body: stream,
    });
    await vi.waitFor(async () => {
      expect((await readdir(root)).some((name) => name.endsWith('.tmp'))).toBe(
        true,
      );
    });
    await expect(readFile(target, 'utf8')).resolves.toBe('previous export');
    streamController?.close();
    await pending;

    await expect(readFile(target, 'utf8')).resolves.toBe('replacement export');
    await expect(readdir(root)).resolves.toEqual(['database-1.sql']);
  });

  it('cleans its temporary file when the source stream fails', async () => {
    const parent = await temporaryDirectory();
    const root = join(parent, 'exports');
    let reads = 0;
    const stream = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (reads++ === 0) {
          controller.enqueue(encoder.encode('partial export'));
          return;
        }
        throw new Error('download interrupted');
      },
    });
    const store = new FileSystemDatabaseExportStore(root);

    await expect(
      store.write({
        databaseId: 'database-1',
        fileName: 'database-1.sql',
        body: stream,
      }),
    ).rejects.toThrow('download interrupted');
    await expect(readdir(root)).resolves.toEqual([]);
  });

  it('does not await a tee branch cancel when the write fails', async () => {
    const parent = await temporaryDirectory();
    const root = join(parent, 'exports');
    const source = new ReadableStream<Uint8Array>({
      start(controller) {
        // A chunk with no byteLength drives the store's own size refusal
        // while the tee source stays readable.
        // The real guard case is oversized; this uses size += undefined.
        controller.enqueue('not bytes' as unknown as Uint8Array);
      },
    });
    const [branch] = source.tee();
    const store = new FileSystemDatabaseExportStore(root);
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(
        () => reject(new Error('write did not settle')),
        1_000,
      );
    });

    try {
      await expect(
        Promise.race([
          store.write({
            databaseId: 'database-1',
            fileName: 'database-1.sql',
            body: branch,
          }),
          timeout,
        ]),
      ).rejects.toThrow('export size exceeds the safe integer range');
    } finally {
      clearTimeout(timer);
    }
    await expect(readdir(root)).resolves.toEqual([]);
  });

  it('preserves a refusal when an injected reader cancel throws', async () => {
    const parent = await temporaryDirectory();
    const root = join(parent, 'exports');
    const cancellationReasons: unknown[] = [];
    const injected = {
      getReader() {
        return {
          async read(): Promise<{ done: true; value: undefined }> {
            return { done: true, value: undefined };
          },
          cancel(reason: unknown): never {
            cancellationReasons.push(reason);
            throw new Error('synchronous reader cancel failure');
          },
          releaseLock(): void {},
        };
      },
    } as unknown as ReadableStream<Uint8Array>;
    const store = new FileSystemDatabaseExportStore(root);

    let refusal: unknown;
    try {
      await store.write({
        databaseId: 'database-1',
        fileName: 'database-1.sql',
        body: injected,
        contentLength: 1,
      });
    } catch (error) {
      refusal = error;
    }
    expect(refusal).toBeInstanceOf(Error);
    expect(refusal).toMatchObject({
      message: 'export size differs from contentLength',
    });
    expect(cancellationReasons).toHaveLength(1);
    expect(cancellationReasons[0]).toBe(refusal);
    await expect(readdir(root)).resolves.toEqual([]);
  });

  it('cleans its temporary file when content length does not match', async () => {
    const parent = await temporaryDirectory();
    const root = join(parent, 'exports');
    const store = new FileSystemDatabaseExportStore(root);

    await expect(
      store.write({
        databaseId: 'database-1',
        fileName: 'database-1.sql',
        body: body('short'),
        contentLength: 20,
      }),
    ).rejects.toThrow(/differs from contentLength/);
    await expect(readdir(root)).resolves.toEqual([]);
  });

  it('cleans its temporary file when atomic publication fails', async () => {
    const parent = await temporaryDirectory();
    const root = join(parent, 'exports');
    const target = join(root, 'database-1.sql');
    await mkdir(target, { recursive: true });
    const store = new FileSystemDatabaseExportStore(root);

    await expect(
      store.write({
        databaseId: 'database-1',
        fileName: 'database-1.sql',
        body: body('complete export'),
      }),
    ).rejects.toThrow();
    await expect(readdir(root)).resolves.toEqual(['database-1.sql']);
    expect((await stat(target)).isDirectory()).toBe(true);
  });

  it('atomically replaces a target symlink without writing through it', async () => {
    const parent = await temporaryDirectory();
    const root = join(parent, 'exports');
    await mkdir(root);
    const outside = join(parent, 'outside.sql');
    const target = join(root, 'database-1.sql');
    await writeFile(outside, 'outside data');
    await symlink(outside, target);
    const store = new FileSystemDatabaseExportStore(root);

    await store.write({
      databaseId: 'database-1',
      fileName: 'database-1.sql',
      body: body('inside export'),
    });

    await expect(readFile(outside, 'utf8')).resolves.toBe('outside data');
    await expect(readFile(target, 'utf8')).resolves.toBe('inside export');
    expect((await lstat(target)).isSymbolicLink()).toBe(false);
  });

  it('converges an exact operation receipt after a lost result', async () => {
    const root = await temporaryDirectory();
    const store = new FileSystemDatabaseExportStore(root);
    const value = 'CREATE TABLE exact_receipt(id INTEGER);';
    const expectedLocation = `${pathToFileURL(root).href}/.anchorage-receipts/v1/${DATABASE_ID}/${OPERATION_ID}.sql`;

    const first = await writeReceipt(store, value);
    expect(first).toEqual({ location: expectedLocation, ...integrity(value) });
    await expect(readFile(receiptTarget(root), 'utf8')).resolves.toBe(value);
    for (const directory of [
      join(root, '.anchorage-receipts'),
      join(root, '.anchorage-receipts', 'v1'),
      receiptDirectory(root),
    ]) {
      expect((await stat(directory)).mode & 0o777).toBe(0o700);
    }
    expect((await stat(receiptTarget(root))).mode & 0o777).toBe(0o600);

    let pulls = 0;
    const cancellationReasons: unknown[] = [];
    const unusedBody = {
      locked: false,
      getReader() {
        pulls += 1;
        throw new Error('preflight receipt body must not be read');
      },
      cancel(reason: unknown) {
        cancellationReasons.push(reason);
        return Promise.resolve();
      },
    } as unknown as ReadableStream<Uint8Array>;
    const replay = await store.writeReceipt?.({
      identity: receiptIdentity(store),
      body: unusedBody,
      contentLength: integrity(value).size,
      expectedIntegrity: Promise.resolve(integrity(value)),
    });
    expect(replay).toEqual(first);
    expect(pulls).toBe(0);
    expect(cancellationReasons).toHaveLength(1);

    const lostUnlink = new Error('temporary unlink result lost');
    const lostStore = createFileSystemDatabaseExportStoreWithReceiptPrimitives(
      root,
      {
        unlink: async (path) => {
          if (String(path).includes('.receipt-')) throw lostUnlink;
          await unlink(path);
        },
      },
    );
    let lostFailure: unknown;
    try {
      await writeReceipt(lostStore, value, SECOND_OPERATION_ID);
    } catch (error) {
      lostFailure = error;
    }
    expect(lostFailure).toBeInstanceOf(AggregateError);
    expect(isAbsolute(receiptTarget(root, SECOND_OPERATION_ID))).toBe(true);
    await expect(
      readFile(receiptTarget(root, SECOND_OPERATION_ID), 'utf8'),
    ).resolves.toBe(value);
    const crashedNames = await readdir(receiptDirectory(root));
    const matchingAlias = crashedNames.find((name) =>
      name.startsWith('.receipt-'),
    );
    expect(matchingAlias).toBeDefined();
    expect((await stat(receiptTarget(root, SECOND_OPERATION_ID))).nlink).toBe(
      2,
    );

    const unrelatedAlias = '.receipt-42345678-1234-4234-9234-123456789abc.tmp';
    await writeFile(join(receiptDirectory(root), unrelatedAlias), 'unrelated', {
      mode: 0o600,
    });
    await chmod(join(receiptDirectory(root), unrelatedAlias), 0o600);
    const paths = new WeakMap<object, string>();
    const high = 9_007_199_254_740_993n;
    const target = receiptTarget(root, SECOND_OPERATION_ID);
    const matching = join(receiptDirectory(root), matchingAlias as string);
    const unrelated = join(receiptDirectory(root), unrelatedAlias);
    const recoveringStore =
      createFileSystemDatabaseExportStoreWithReceiptPrimitives(root, {
        open: trackedOpen(paths),
        stat: async (handle) => {
          const actual = await handle.stat({ bigint: true });
          const path = paths.get(handle);
          if (path === target || path === matching) {
            return withBigIntIdentity(actual, high, high + 2n);
          }
          if (path === unrelated) {
            return withBigIntIdentity(actual, high - 1n, high + 1n);
          }
          return actual;
        },
      });
    const recovered = await writeReceipt(
      recoveringStore,
      value,
      SECOND_OPERATION_ID,
    );
    expect(recovered).toEqual({
      location: pathToFileURL(target).href,
      ...integrity(value),
    });
    await expect(lstat(matching)).rejects.toMatchObject({ code: 'ENOENT' });
    await expect(readFile(unrelated, 'utf8')).resolves.toBe('unrelated');
    expect((await stat(target)).nlink).toBe(1);
    expect(
      (await readdir(receiptDirectory(root))).filter((name) =>
        name.endsWith('.sql'),
      ),
    ).toEqual([`${OPERATION_ID}.sql`, `${SECOND_OPERATION_ID}.sql`]);
  });

  it('converges concurrent matching receipts and preserves a mismatched winner', async () => {
    const root = await temporaryDirectory();
    const store = new FileSystemDatabaseExportStore(root);
    const value = 'CREATE TABLE concurrent_receipt(id INTEGER);';

    const concurrent = await Promise.all([
      writeReceipt(store, value),
      writeReceipt(store, value),
    ]);
    expect(concurrent[0]).toEqual(concurrent[1]);
    await expect(readFile(receiptTarget(root), 'utf8')).resolves.toBe(value);
    expect(await readdir(receiptDirectory(root))).toEqual([
      `${OPERATION_ID}.sql`,
    ]);

    await expect(
      writeReceipt(store, 'X'.repeat(integrity(value).size), OPERATION_ID),
    ).rejects.toThrow(
      'database export receipt collision differs from the committed export',
    );
    await expect(writeReceipt(store, 'short', OPERATION_ID)).rejects.toThrow(
      'database export receipt collision differs from the committed export',
    );
    await expect(readFile(receiptTarget(root), 'utf8')).resolves.toBe(value);

    const raceValue = 'CREATE TABLE race_winner(id INTEGER);';
    let raced = false;
    const targetRaceStore =
      createFileSystemDatabaseExportStoreWithReceiptPrimitives(root, {
        link: async (temporary, target) => {
          if (!raced) {
            raced = true;
            await writeFile(target, raceValue, { flag: 'wx', mode: 0o600 });
            await chmod(target, 0o600);
          }
          await link(temporary, target);
        },
      });
    await expect(
      writeReceipt(targetRaceStore, raceValue, SECOND_OPERATION_ID),
    ).resolves.toEqual({
      location: pathToFileURL(receiptTarget(root, SECOND_OPERATION_ID)).href,
      ...integrity(raceValue),
    });

    const mismatchRaceValue = 'CREATE TABLE mismatch_race(id INTEGER);';
    let mismatchedRace = false;
    const mismatchRaceStore =
      createFileSystemDatabaseExportStoreWithReceiptPrimitives(root, {
        link: async (temporary, target) => {
          if (!mismatchedRace) {
            mismatchedRace = true;
            await writeFile(target, 'different committed bytes', {
              flag: 'wx',
              mode: 0o600,
            });
            await chmod(target, 0o600);
          }
          await link(temporary, target);
        },
      });
    await expect(
      writeReceipt(mismatchRaceStore, mismatchRaceValue, THIRD_OPERATION_ID),
    ).rejects.toThrow(
      'database export receipt collision differs from the committed export',
    );
    await expect(
      readFile(receiptTarget(root, THIRD_OPERATION_ID), 'utf8'),
    ).resolves.toBe('different committed bytes');

    const ambiguousOperation = '42345678-1234-4234-9234-123456789abc';
    const ambiguousFailure = Object.assign(new Error('link result lost'), {
      code: 'EIO',
    });
    const ambiguousStore =
      createFileSystemDatabaseExportStoreWithReceiptPrimitives(root, {
        link: async (temporary, target) => {
          await link(temporary, target);
          throw ambiguousFailure;
        },
      });
    await expect(
      writeReceipt(ambiguousStore, value, ambiguousOperation),
    ).resolves.toEqual({
      location: pathToFileURL(receiptTarget(root, ambiguousOperation)).href,
      ...integrity(value),
    });
    expect((await stat(receiptTarget(root, ambiguousOperation))).nlink).toBe(1);

    const competingUnlinkOperation = '92345678-1234-4234-9234-123456789abc';
    const competingUnlinkTarget = receiptTarget(root, competingUnlinkOperation);
    const competingUnlinkAlias = join(
      receiptDirectory(root),
      '.receipt-92345678-1234-4234-9234-223456789abc.tmp',
    );
    await writeFile(competingUnlinkTarget, value, { mode: 0o600 });
    await chmod(competingUnlinkTarget, 0o600);
    await link(competingUnlinkTarget, competingUnlinkAlias);
    let competed = false;
    const competingUnlinkStore =
      createFileSystemDatabaseExportStoreWithReceiptPrimitives(root, {
        unlink: async (path) => {
          if (String(path) === competingUnlinkAlias && !competed) {
            competed = true;
            await unlink(path);
          }
          await unlink(path);
        },
      });
    await expect(
      writeReceipt(competingUnlinkStore, value, competingUnlinkOperation),
    ).resolves.toEqual({
      location: pathToFileURL(competingUnlinkTarget).href,
      ...integrity(value),
    });
    expect((await stat(competingUnlinkTarget)).nlink).toBe(1);

    const absentOperation = '52345678-1234-4234-9234-123456789abc';
    const nonExistFailure = Object.assign(new Error('link unavailable'), {
      code: 'EIO',
    });
    const absentStore =
      createFileSystemDatabaseExportStoreWithReceiptPrimitives(root, {
        link: async () => {
          throw nonExistFailure;
        },
      });
    await expect(
      writeReceipt(absentStore, value, absentOperation),
    ).rejects.toBe(nonExistFailure);
    await expect(
      lstat(receiptTarget(root, absentOperation)),
    ).rejects.toMatchObject({ code: 'ENOENT' });

    const syncOperation = '62345678-1234-4234-9234-123456789abc';
    const syncFailure = new Error('database directory sync result lost');
    const syncTarget = receiptTarget(root, syncOperation);
    const syncPaths = new WeakMap<object, string>();
    const syncLossStore =
      createFileSystemDatabaseExportStoreWithReceiptPrimitives(root, {
        open: trackedOpen(syncPaths),
        sync: async (handle) => {
          if (syncPaths.get(handle) === receiptDirectory(root)) {
            throw syncFailure;
          }
          await handle.sync();
        },
      });
    await expect(
      writeReceipt(syncLossStore, value, syncOperation),
    ).rejects.toBe(syncFailure);
    await expect(readFile(syncTarget, 'utf8')).resolves.toBe(value);

    const replaySyncFailure = new Error('replay directory sync failed');
    const replayPaths = new WeakMap<object, string>();
    const replaySyncedPaths: string[] = [];
    const replaySyncStore =
      createFileSystemDatabaseExportStoreWithReceiptPrimitives(root, {
        open: trackedOpen(replayPaths),
        sync: async (handle) => {
          const path = replayPaths.get(handle);
          if (path !== undefined) replaySyncedPaths.push(path);
          if (path === receiptDirectory(root)) throw replaySyncFailure;
          await handle.sync();
        },
      });
    await expect(
      writeReceipt(replaySyncStore, value, syncOperation),
    ).rejects.toThrow('database export receipt readback failed');
    expect(replaySyncedPaths).toEqual([
      root,
      join(root, '.anchorage-receipts'),
      join(root, '.anchorage-receipts', 'v1'),
      receiptDirectory(root),
    ]);
    await expect(readFile(syncTarget, 'utf8')).resolves.toBe(value);

    const durablePaths = new WeakMap<object, string>();
    const durableSyncedPaths: string[] = [];
    const durableReplayStore =
      createFileSystemDatabaseExportStoreWithReceiptPrimitives(root, {
        open: trackedOpen(durablePaths),
        sync: async (handle) => {
          const path = durablePaths.get(handle);
          if (path !== undefined) durableSyncedPaths.push(path);
          await handle.sync();
        },
      });
    await expect(
      writeReceipt(durableReplayStore, value, syncOperation),
    ).resolves.toEqual({
      location: pathToFileURL(syncTarget).href,
      ...integrity(value),
    });
    expect(durableSyncedPaths).toEqual(replaySyncedPaths);

    const aggregateOperation = '72345678-1234-4234-9234-123456789abc';
    const aggregateStore =
      createFileSystemDatabaseExportStoreWithReceiptPrimitives(root, {
        link: async () => {
          throw null;
        },
        unlink: async (path) => {
          if (String(path).includes('.receipt-')) throw undefined;
          await unlink(path);
        },
      });
    let aggregate: unknown;
    try {
      await writeReceipt(aggregateStore, value, aggregateOperation);
    } catch (error) {
      aggregate = error;
    }
    expect(aggregate).toBeInstanceOf(AggregateError);
    expect(aggregate).toMatchObject({
      message: 'database export receipt and temporary-file cleanup failed',
      errors: [null, undefined],
    });
    await expect(
      lstat(receiptTarget(root, aggregateOperation)),
    ).rejects.toMatchObject({ code: 'ENOENT' });
  });

  it('refuses malformed receipt identity or integrity before publication', async () => {
    const root = await temporaryDirectory();
    const unsupported =
      createFileSystemDatabaseExportStoreWithReceiptPrimitives(root, {
        platform: 'win32',
      });
    expect(Object.hasOwn(unsupported, 'receiptAuthority')).toBe(false);
    expect(Object.hasOwn(unsupported, 'writeReceipt')).toBe(false);
    expect('receiptAuthority' in unsupported).toBe(false);
    expect('writeReceipt' in unsupported).toBe(false);
    const missingFlag =
      createFileSystemDatabaseExportStoreWithReceiptPrimitives(root, {
        flags: { O_NOFOLLOW: undefined },
      });
    expect(Object.hasOwn(missingFlag, 'receiptAuthority')).toBe(false);
    expect(Object.hasOwn(missingFlag, 'writeReceipt')).toBe(false);
    expect(
      () => new FileSystemDatabaseExportStore(`/${'a'.repeat(4_097)}`),
    ).toThrow('database export receipt capability is malformed');

    const openCalls: string[] = [];
    const inspectingStore =
      createFileSystemDatabaseExportStoreWithReceiptPrimitives(root, {
        open: (async (
          path: PathLike,
          flags: string | number,
          mode?: string | number,
        ) => {
          openCalls.push(String(path));
          return open(path, flags, mode);
        }) as typeof open,
      });
    const valid = receiptIdentity(inspectingStore);
    const accessorIdentity = { ...valid };
    Object.defineProperty(accessorIdentity, 'databaseId', {
      enumerable: true,
      get() {
        throw new Error('identity getter must not escape');
      },
    });
    const symbolIdentity = { ...valid } as Record<PropertyKey, unknown>;
    symbolIdentity[Symbol('extra')] = true;
    const revoked = Proxy.revocable({ ...valid }, {});
    revoked.revoke();
    const malformedIdentities: unknown[] = [
      null,
      { ...valid, version: 2 },
      { ...valid, databaseId: DATABASE_ID.toUpperCase() },
      { ...valid, operationId: OPERATION_ID.toUpperCase() },
      { ...valid, operationId: DATABASE_ID },
      { ...valid, extra: true },
      {
        version: 1,
        authority: valid.authority,
        databaseId: DATABASE_ID,
      },
      accessorIdentity,
      symbolIdentity,
      new Proxy({ ...valid }, {}),
      new Proxy(
        { ...valid },
        {
          ownKeys() {
            throw new Error('identity proxy trap must not escape');
          },
        },
      ),
      revoked.proxy,
    ];
    const writer = inspectingStore.writeReceipt;
    if (writer === undefined) throw new Error('receipt writer unavailable');
    for (const malformed of malformedIdentities) {
      const cancellations: unknown[] = [];
      const hostileCancelBody = {
        cancel(reason: unknown) {
          cancellations.push(reason);
          if (cancellations.length % 2 === 0) {
            return Promise.reject(new Error('cancel rejection'));
          }
          throw new Error('cancel throw');
        },
      } as unknown as ReadableStream<Uint8Array>;
      let refusal: unknown;
      try {
        await writer({
          identity: malformed as DatabaseExportReceiptIdentity,
          body: hostileCancelBody,
          expectedIntegrity: Promise.resolve(integrity('valid')),
        });
      } catch (error) {
        refusal = error;
      }
      expect(refusal).toMatchObject({
        message: 'database export receipt identity is malformed',
      });
      expect(isDatabaseExportReceiptError(refusal)).toBe(true);
      expect(refusal).not.toHaveProperty('cause');
      expect(cancellations).toHaveLength(1);
    }
    expect(openCalls).toEqual([]);

    const authorityBodyCancellations: unknown[] = [];
    const authorityBody = {
      cancel(reason: unknown) {
        authorityBodyCancellations.push(reason);
        return Promise.reject(new Error('ignored cancellation rejection'));
      },
    } as unknown as ReadableStream<Uint8Array>;
    let authorityRefusal: unknown;
    try {
      await writer({
        identity: { ...valid, authority: `${valid.authority}/different` },
        body: authorityBody,
        expectedIntegrity: Promise.resolve(integrity('valid')),
      });
    } catch (error) {
      authorityRefusal = error;
    }
    expect(authorityRefusal).toMatchObject({
      message:
        'database export receipt authority differs from configured authority',
    });
    expect(isDatabaseExportReceiptError(authorityRefusal)).toBe(true);
    expect(authorityRefusal).not.toHaveProperty('cause');
    expect(authorityBodyCancellations).toHaveLength(1);
    expect(openCalls).toEqual([]);

    const digest = integrity('native promise');
    const escapedAuthority = '\0'.repeat(4_096);
    expect(databaseExportReceiptAuthorityFromUnknown(escapedAuthority)).toBe(
      escapedAuthority,
    );
    expect(
      databaseExportReceiptIdentityFromUnknown(
        { ...valid, authority: escapedAuthority },
        escapedAuthority,
      ),
    ).toEqual({ ...valid, authority: escapedAuthority });
    expect(() =>
      databaseExportReceiptAuthorityFromUnknown(`${escapedAuthority}\0`),
    ).toThrow('database export receipt capability is malformed');
    expect(
      databaseExportIntegrityFromUnknown({
        size: Number.MAX_SAFE_INTEGER,
        sha256: digest.sha256,
      }),
    ).toEqual({ size: Number.MAX_SAFE_INTEGER, sha256: digest.sha256 });
    for (const malformed of [
      { size: 0, sha256: digest.sha256 },
      { size: Number.MAX_SAFE_INTEGER + 1, sha256: digest.sha256 },
      { size: 1, sha256: digest.sha256.toUpperCase() },
      { size: 1, sha256: digest.sha256, extra: true },
      { size: 1 },
      Object.defineProperty({ sha256: digest.sha256 }, 'size', {
        enumerable: true,
        get() {
          throw new Error('integrity getter must not escape');
        },
      }),
      new Proxy(
        { size: 1, sha256: digest.sha256 },
        {
          getPrototypeOf() {
            throw new Error('integrity proxy trap must not escape');
          },
        },
      ),
      new Proxy({ size: 1, sha256: digest.sha256 }, {}),
    ]) {
      let refusal: unknown;
      try {
        databaseExportIntegrityFromUnknown(malformed);
      } catch (error) {
        refusal = error;
      }
      expect(refusal).toMatchObject({
        message: 'database export receipt integrity is malformed',
      });
      expect(isDatabaseExportReceiptError(refusal)).toBe(true);
      expect(refusal).not.toHaveProperty('cause');
    }

    const promiseMethodTrap = vi.fn(() => {
      throw new Error('caller-owned then must not run');
    });
    const nativeSubclass = new (class<
      T,
    > extends Promise<T> {})<DatabaseExportIntegrity>((resolvePromise) =>
      resolvePromise(integrity('subclass')),
    );
    Object.defineProperty(nativeSubclass, 'then', {
      configurable: true,
      value: promiseMethodTrap,
    });
    await expect(
      writer({
        identity: { ...valid, operationId: SECOND_OPERATION_ID },
        body: body('subclass'),
        contentLength: integrity('subclass').size,
        expectedIntegrity: nativeSubclass,
      }),
    ).resolves.toMatchObject(integrity('subclass'));
    expect(promiseMethodTrap).not.toHaveBeenCalled();

    const crossRealmPromise = runInNewContext(
      '(value) => Promise.resolve(value)',
    ) as (value: DatabaseExportIntegrity) => Promise<DatabaseExportIntegrity>;
    const crossRealm = crossRealmPromise(integrity('realm'));
    await expect(
      writer({
        identity: { ...valid, operationId: THIRD_OPERATION_ID },
        body: body('realm'),
        contentLength: integrity('realm').size,
        expectedIntegrity: crossRealm,
      }),
    ).resolves.toMatchObject(integrity('realm'));

    const malformedPromises: readonly Readonly<{
      value: unknown;
      synchronous: boolean;
    }>[] = [
      {
        value: Promise.reject(new Error('expected integrity rejected')),
        synchronous: false,
      },
      {
        value: new Proxy(Promise.resolve(digest), {}),
        synchronous: true,
      },
      {
        // biome-ignore lint/suspicious/noThenProperty: deliberately hostile thenable input
        value: { then: () => undefined },
        synchronous: true,
      },
      {
        value: Promise.resolve({
          size: 1,
          sha256: digest.sha256,
          extra: true,
        }),
        synchronous: false,
      },
      {
        value: Promise.resolve(
          new Proxy({ size: 1, sha256: digest.sha256 }, {}),
        ),
        synchronous: false,
      },
      {
        value: Promise.resolve(
          Object.defineProperty({ sha256: digest.sha256 }, 'size', {
            enumerable: true,
            get() {
              throw new Error('resolved getter must not escape');
            },
          }),
        ),
        synchronous: false,
      },
    ];
    for (const malformedPromise of malformedPromises) {
      let cancellations = 0;
      const cancellable = {
        locked: false,
        getReader() {
          throw new Error('synchronous integrity refusal must not read');
        },
        cancel() {
          cancellations += 1;
          return Promise.reject(new Error('ignored cancel rejection'));
        },
      } as unknown as ReadableStream<Uint8Array>;
      let refusal: unknown;
      try {
        await writer({
          identity: { ...valid, operationId: OPERATION_ID },
          body: malformedPromise.synchronous ? cancellable : body('invalid'),
          expectedIntegrity:
            malformedPromise.value as Promise<DatabaseExportIntegrity>,
        });
      } catch (error) {
        refusal = error;
      }
      expect(refusal).toMatchObject({
        message: 'database export receipt integrity is malformed',
      });
      expect(isDatabaseExportReceiptError(refusal)).toBe(true);
      expect(refusal).not.toHaveProperty('cause');
      expect(cancellations).toBe(malformedPromise.synchronous ? 1 : 0);
    }

    const validForDirectParser = databaseExportReceiptIdentityFromUnknown(
      { ...valid },
      valid.authority,
    );
    expect(Object.keys(validForDirectParser)).toEqual([
      'version',
      'authority',
      'databaseId',
      'operationId',
    ]);
    expect(Object.isFrozen(validForDirectParser)).toBe(true);

    const emptyOperation = '42345678-1234-4234-9234-123456789abc';
    await expect(
      writer({
        identity: { ...valid, operationId: emptyOperation },
        body: body(),
        expectedIntegrity: Promise.resolve(integrity('nonempty')),
      }),
    ).rejects.toThrow('database export receipt refuses an empty body');
    expect(await readdir(receiptDirectory(root))).not.toContain(
      `${emptyOperation}.sql`,
    );

    const shortOperation = '52345678-1234-4234-9234-123456789abc';
    await expect(
      writer({
        identity: { ...valid, operationId: shortOperation },
        body: body('short'),
        contentLength: 6,
        expectedIntegrity: Promise.resolve(integrity('short')),
      }),
    ).rejects.toThrow(
      'database export receipt source integrity differs from the streamed export',
    );
    const mismatchOperation = '62345678-1234-4234-9234-123456789abc';
    await expect(
      writer({
        identity: { ...valid, operationId: mismatchOperation },
        body: body('actual'),
        expectedIntegrity: Promise.resolve(integrity('wanted')),
      }),
    ).rejects.toThrow(
      'database export receipt source integrity differs from the streamed export',
    );

    const streamFailure = new Error('receipt source failed');
    const failingStream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoder.encode('partial'));
      },
      pull() {
        throw streamFailure;
      },
    });
    await expect(
      writer({
        identity: {
          ...valid,
          operationId: '72345678-1234-4234-9234-123456789abc',
        },
        body: failingStream,
        expectedIntegrity: Promise.resolve(integrity('partial')),
      }),
    ).rejects.toBe(streamFailure);
    expect(
      (await readdir(receiptDirectory(root))).some((name) =>
        name.startsWith('.receipt-'),
      ),
    ).toBe(false);

    const missingRoot = join(root, 'missing-root');
    const missingRootStore = new FileSystemDatabaseExportStore(missingRoot);
    let missingCancellation = 0;
    const missingBody = new ReadableStream<Uint8Array>({
      cancel() {
        missingCancellation += 1;
      },
    });
    await expect(
      missingRootStore.writeReceipt?.({
        identity: receiptIdentity(missingRootStore),
        body: missingBody,
        expectedIntegrity: Promise.resolve(integrity('missing')),
      }),
    ).rejects.toThrow();
    expect(missingCancellation).toBe(1);
    await expect(lstat(missingRoot)).rejects.toMatchObject({ code: 'ENOENT' });

    const trustedRoot = join(root, 'trusted-root');
    const linkedRoot = join(root, 'linked-root');
    await mkdir(trustedRoot, { mode: 0o700 });
    await symlink(trustedRoot, linkedRoot);
    const linkedRootStore = new FileSystemDatabaseExportStore(linkedRoot);
    await expect(writeReceipt(linkedRootStore, 'symlink root')).rejects.toThrow(
      'database export receipt root must be an existing trusted directory',
    );

    const unsafeRoot = join(root, 'unsafe-root');
    await mkdir(join(unsafeRoot, '.anchorage-receipts'), {
      mode: 0o755,
      recursive: true,
    });
    await chmod(join(unsafeRoot, '.anchorage-receipts'), 0o755);
    const unsafeStore = new FileSystemDatabaseExportStore(unsafeRoot);
    await expect(writeReceipt(unsafeStore, 'unsafe mode')).rejects.toThrow(
      'database export receipt directory must be a mode-0700 directory',
    );
    expect(
      (await stat(join(unsafeRoot, '.anchorage-receipts'))).mode & 0o777,
    ).toBe(0o755);
  });

  it('preserves nonregular and unreadable receipt collisions', async () => {
    const root = await temporaryDirectory();
    const store = new FileSystemDatabaseExportStore(root);
    await writeReceipt(store, 'directory bootstrap');
    const directory = receiptDirectory(root);
    const collisionMessage =
      'database export receipt collision differs from the committed export';

    const directoryOperation = SECOND_OPERATION_ID;
    const directoryTarget = receiptTarget(root, directoryOperation);
    await mkdir(directoryTarget, { mode: 0o700 });
    await expect(
      writeReceipt(store, 'directory collision', directoryOperation),
    ).rejects.toThrow(collisionMessage);
    expect((await lstat(directoryTarget)).isDirectory()).toBe(true);

    const symlinkOperation = THIRD_OPERATION_ID;
    const symlinkTarget = receiptTarget(root, symlinkOperation);
    const outside = join(root, 'outside-receipt.sql');
    await writeFile(outside, 'outside bytes');
    await symlink(outside, symlinkTarget);
    await expect(
      writeReceipt(store, 'symlink collision', symlinkOperation),
    ).rejects.toThrow(collisionMessage);
    expect((await lstat(symlinkTarget)).isSymbolicLink()).toBe(true);
    await expect(readFile(outside, 'utf8')).resolves.toBe('outside bytes');

    const wrongModeOperation = '42345678-1234-4234-9234-123456789abc';
    const wrongModeTarget = receiptTarget(root, wrongModeOperation);
    await writeFile(wrongModeTarget, 'wrong mode', { mode: 0o644 });
    await chmod(wrongModeTarget, 0o644);
    await expect(
      writeReceipt(store, 'wrong mode', wrongModeOperation),
    ).rejects.toThrow(collisionMessage);
    expect((await stat(wrongModeTarget)).mode & 0o777).toBe(0o644);

    const hardlinkOperation = '52345678-1234-4234-9234-123456789abc';
    const hardlinkSource = join(root, 'unrelated-hardlink-source.sql');
    const hardlinkTarget = receiptTarget(root, hardlinkOperation);
    await writeFile(hardlinkSource, 'hardlink collision', { mode: 0o600 });
    await chmod(hardlinkSource, 0o600);
    await link(hardlinkSource, hardlinkTarget);
    await expect(
      writeReceipt(store, 'hardlink collision', hardlinkOperation),
    ).rejects.toThrow(collisionMessage);
    expect((await stat(hardlinkSource)).nlink).toBe(2);
    expect((await stat(hardlinkTarget)).nlink).toBe(2);

    const fifoOperation = '62345678-1234-4234-9234-123456789abc';
    const fifoTarget = receiptTarget(root, fifoOperation);
    await execFileAsync('mkfifo', [fifoTarget]);
    let timeout: ReturnType<typeof setTimeout> | undefined;
    const fifoDeadline = new Promise<never>((_, reject) => {
      timeout = setTimeout(
        () => reject(new Error('FIFO receipt inspection blocked')),
        1_000,
      );
    });
    try {
      await expect(
        Promise.race([
          writeReceipt(store, 'fifo collision', fifoOperation),
          fifoDeadline,
        ]),
      ).rejects.toThrow(collisionMessage);
    } finally {
      clearTimeout(timeout);
    }
    expect((await lstat(fifoTarget)).isFIFO()).toBe(true);

    const socketOperation = '72345678-1234-4234-9234-123456789abc';
    const socketTarget = receiptTarget(root, socketOperation);
    const socketPath = join(root, 'receipt.sock');
    await writeFile(socketTarget, 'preserved socket stand-in', { mode: 0o600 });
    const server = createServer();
    await new Promise<void>((resolvePromise, rejectPromise) => {
      server.once('error', rejectPromise);
      server.listen(socketPath, resolvePromise);
    });
    try {
      const socketStore =
        createFileSystemDatabaseExportStoreWithReceiptPrimitives(root, {
          open: (async (
            path: PathLike,
            flags: string | number,
            mode?: string | number,
          ) => {
            if (String(path) === socketTarget) {
              return open(socketPath, flags, mode);
            }
            return open(path, flags, mode);
          }) as typeof open,
        });
      await expect(
        writeReceipt(socketStore, 'socket collision', socketOperation),
      ).rejects.toThrow(collisionMessage);
      expect((await lstat(socketPath)).isSocket()).toBe(true);
      await expect(readFile(socketTarget, 'utf8')).resolves.toBe(
        'preserved socket stand-in',
      );
    } finally {
      await new Promise<void>((resolvePromise, rejectPromise) => {
        server.close((error) => {
          if (error) rejectPromise(error);
          else resolvePromise();
        });
      });
    }

    const deviceOperation = '82345678-1234-4234-9234-123456789abc';
    const deviceTarget = receiptTarget(root, deviceOperation);
    await writeFile(deviceTarget, 'preserved device stand-in', { mode: 0o600 });
    const deviceStore =
      createFileSystemDatabaseExportStoreWithReceiptPrimitives(root, {
        open: (async (
          path: PathLike,
          flags: string | number,
          mode?: string | number,
        ) => {
          if (String(path) === deviceTarget) {
            return open('/dev/null', flags, mode);
          }
          return open(path, flags, mode);
        }) as typeof open,
      });
    await expect(
      writeReceipt(deviceStore, 'device collision', deviceOperation),
    ).rejects.toThrow(collisionMessage);
    await expect(readFile(deviceTarget, 'utf8')).resolves.toBe(
      'preserved device stand-in',
    );

    const unreadableOperation = '92345678-1234-4234-9234-123456789abc';
    const unreadableTarget = receiptTarget(root, unreadableOperation);
    await writeFile(unreadableTarget, 'unreadable collision', { mode: 0o600 });
    const unreadableStore =
      createFileSystemDatabaseExportStoreWithReceiptPrimitives(root, {
        open: (async (
          path: PathLike,
          flags: string | number,
          mode?: string | number,
        ) => {
          if (String(path) === unreadableTarget) {
            throw Object.assign(new Error('permission denied'), {
              code: 'EACCES',
            });
          }
          return open(path, flags, mode);
        }) as typeof open,
      });
    await expect(
      writeReceipt(
        unreadableStore,
        'unreadable collision',
        unreadableOperation,
      ),
    ).rejects.toThrow(collisionMessage);
    await expect(readFile(unreadableTarget, 'utf8')).resolves.toBe(
      'unreadable collision',
    );

    const readFailureOperation = 'a2345678-1234-4234-9234-123456789abc';
    const readFailureTarget = receiptTarget(root, readFailureOperation);
    await writeFile(readFailureTarget, 'read failure collision', {
      mode: 0o600,
    });
    await chmod(readFailureTarget, 0o600);
    const readFailure = Object.assign(new Error('read failed'), {
      cause: new Error('foreign cause'),
    });
    const readFailureStore =
      createFileSystemDatabaseExportStoreWithReceiptPrimitives(root, {
        read: async () => {
          throw readFailure;
        },
      });
    let readRefusal: unknown;
    try {
      await writeReceipt(
        readFailureStore,
        'read failure collision',
        readFailureOperation,
      );
    } catch (error) {
      readRefusal = error;
    }
    expect(readRefusal).toMatchObject({
      message: 'database export receipt readback failed',
    });
    expect(isDatabaseExportReceiptError(readRefusal)).toBe(true);
    expect(readRefusal).not.toHaveProperty('cause');
    await expect(readFile(readFailureTarget, 'utf8')).resolves.toBe(
      'read failure collision',
    );

    const disappearingOperation = 'b2345678-1234-4234-9234-123456789abc';
    const disappearingTarget = receiptTarget(root, disappearingOperation);
    const disappearingAlias = join(
      directory,
      '.receipt-c2345678-1234-4234-9234-123456789abc.tmp',
    );
    await writeFile(disappearingTarget, 'disappearing alias', { mode: 0o600 });
    await chmod(disappearingTarget, 0o600);
    await link(disappearingTarget, disappearingAlias);
    let aliasDisappeared = false;
    const disappearingStore =
      createFileSystemDatabaseExportStoreWithReceiptPrimitives(root, {
        open: (async (
          path: PathLike,
          flags: string | number,
          mode?: string | number,
        ) => {
          if (String(path) === disappearingAlias && !aliasDisappeared) {
            aliasDisappeared = true;
            await unlink(disappearingAlias);
            throw Object.assign(new Error('alias disappeared'), {
              code: 'ENOENT',
            });
          }
          return open(path, flags, mode);
        }) as typeof open,
      });
    await expect(
      writeReceipt(
        disappearingStore,
        'disappearing alias',
        disappearingOperation,
      ),
    ).resolves.toEqual({
      location: pathToFileURL(disappearingTarget).href,
      ...integrity('disappearing alias'),
    });
    await expect(readFile(disappearingTarget, 'utf8')).resolves.toBe(
      'disappearing alias',
    );
    expect((await stat(disappearingTarget)).nlink).toBe(1);

    const unsafeSizeOperation = 'd2345678-1234-4234-9234-123456789abc';
    const unsafeSizeTarget = receiptTarget(root, unsafeSizeOperation);
    await writeFile(unsafeSizeTarget, 'unsafe size', { mode: 0o600 });
    await chmod(unsafeSizeTarget, 0o600);
    const paths = new WeakMap<object, string>();
    const unsafeSizeStore =
      createFileSystemDatabaseExportStoreWithReceiptPrimitives(root, {
        open: trackedOpen(paths),
        stat: async (handle) => {
          const actual = await handle.stat({ bigint: true });
          if (paths.get(handle) === unsafeSizeTarget) {
            return withBigIntIdentity(
              actual,
              actual.dev,
              actual.ino,
              BigInt(Number.MAX_SAFE_INTEGER) + 1n,
            );
          }
          return actual;
        },
      });
    await expect(
      writeReceipt(unsafeSizeStore, 'unsafe size', unsafeSizeOperation),
    ).rejects.toThrow(collisionMessage);
    await expect(readFile(unsafeSizeTarget, 'utf8')).resolves.toBe(
      'unsafe size',
    );
  });
});
