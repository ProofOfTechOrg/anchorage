// SPDX-License-Identifier: Apache-2.0

import { createHash, randomUUID } from 'node:crypto';
import { type BigIntStats, constants as fsConstants } from 'node:fs';
import {
  link,
  mkdir,
  open,
  readdir,
  realpath,
  rename,
  rm,
  unlink,
} from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import {
  cancelBodyWithoutAwait,
  captureDatabaseExportIntegrityPromise,
  type DurableDatabaseExportStore,
  databaseExportReceiptAuthorityFromUnknown,
  databaseExportReceiptError,
  databaseExportReceiptIdentityFromUnknown,
} from './database-export-store.js';
import { assertFileName } from './export-file-name.js';
import type {
  DatabaseExportIntegrity,
  DatabaseExportReceiptIdentity,
} from './types.js';

type FileHandle = Awaited<ReturnType<typeof open>>;

const DIRECTORY_MODE = 0o700n;
const FILE_MODE = 0o600n;
const PERMISSION_MODE = 0o7777n;
const MAX_SAFE_SIZE = BigInt(Number.MAX_SAFE_INTEGER);
const RECEIPT_TEMP_PATTERN =
  /^\.receipt-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\.tmp$/u;
const RECEIPT_TEMP_UUID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const READABLE_STREAM_GET_READER = ReadableStream.prototype.getReader;
const READER_CANCEL = ReadableStreamDefaultReader.prototype.cancel;
const READER_READ = ReadableStreamDefaultReader.prototype.read;
const READER_RELEASE_LOCK = ReadableStreamDefaultReader.prototype.releaseLock;
const PROMISE_THEN = Promise.prototype.then;

interface ReceiptOpenFlags {
  readonly O_DIRECTORY: unknown;
  readonly O_NOFOLLOW: unknown;
  readonly O_NONBLOCK: unknown;
  readonly O_RDONLY: unknown;
}

export interface FileSystemDatabaseExportStoreReceiptPrimitives {
  readonly platform: string;
  readonly flags: ReceiptOpenFlags;
  readonly randomUUID: () => string;
  readonly open: typeof open;
  readonly mkdir: typeof mkdir;
  readonly realpath: typeof realpath;
  readonly link: typeof link;
  readonly unlink: typeof unlink;
  readonly readdir: (path: string) => Promise<string[]>;
  readonly stat: (handle: FileHandle) => Promise<BigIntStats>;
  readonly chmod: (handle: FileHandle, mode: number) => Promise<void>;
  readonly sync: (handle: FileHandle) => Promise<void>;
  readonly close: (handle: FileHandle) => Promise<void>;
  readonly read: (
    handle: FileHandle,
    buffer: Uint8Array,
    offset: number,
    length: number,
    position: number | null,
  ) => Promise<{ bytesRead: number }>;
  readonly write: (
    handle: FileHandle,
    buffer: Uint8Array,
    offset: number,
    length: number,
  ) => Promise<{ bytesWritten: number }>;
}

export interface FileSystemDatabaseExportStoreReceiptPrimitiveOverrides
  extends Partial<
    Omit<FileSystemDatabaseExportStoreReceiptPrimitives, 'flags'>
  > {
  readonly flags?: Partial<ReceiptOpenFlags>;
}

const defaultReceiptPrimitives: FileSystemDatabaseExportStoreReceiptPrimitives =
  {
    platform: process.platform,
    flags: {
      O_DIRECTORY: fsConstants.O_DIRECTORY,
      O_NOFOLLOW: fsConstants.O_NOFOLLOW,
      O_NONBLOCK: fsConstants.O_NONBLOCK,
      O_RDONLY: fsConstants.O_RDONLY,
    },
    randomUUID,
    open,
    mkdir,
    realpath,
    link,
    unlink,
    readdir: (path) => readdir(path),
    stat: (handle) => handle.stat({ bigint: true }),
    chmod: (handle, mode) => handle.chmod(mode),
    sync: (handle) => handle.sync(),
    close: (handle) => handle.close(),
    read: (handle, buffer, offset, length, position) =>
      handle.read(buffer, offset, length, position),
    write: (handle, buffer, offset, length) =>
      handle.write(buffer, offset, length),
  };

type Settlement<T> =
  | Readonly<{ status: 'fulfilled'; value: T }>
  | Readonly<{ status: 'rejected'; reason: unknown }>;

interface OpenReceiptDirectories {
  readonly directory: string;
  readonly handle: FileHandle;
  readonly handles: readonly FileHandle[];
}

interface ReceiptFileState {
  readonly location: string;
  readonly size: number;
  readonly sha256: string;
}

type ReceiptTargetInspection =
  | Readonly<{ status: 'absent' }>
  | Readonly<{ status: 'present'; receipt: ReceiptFileState }>;

function receiptPrimitives(
  overrides: FileSystemDatabaseExportStoreReceiptPrimitiveOverrides = {},
): FileSystemDatabaseExportStoreReceiptPrimitives {
  return {
    ...defaultReceiptPrimitives,
    ...overrides,
    flags: {
      ...defaultReceiptPrimitives.flags,
      ...overrides.flags,
    },
  };
}

function supportsFileSystemReceipts(
  primitives: FileSystemDatabaseExportStoreReceiptPrimitives,
): primitives is FileSystemDatabaseExportStoreReceiptPrimitives & {
  readonly flags: {
    readonly O_DIRECTORY: number;
    readonly O_NOFOLLOW: number;
    readonly O_NONBLOCK: number;
    readonly O_RDONLY: number;
  };
} {
  const flags = [
    primitives.flags.O_DIRECTORY,
    primitives.flags.O_NOFOLLOW,
    primitives.flags.O_NONBLOCK,
    primitives.flags.O_RDONLY,
  ];
  return (
    primitives.platform !== 'win32' &&
    flags.every(
      (flag) =>
        typeof flag === 'number' &&
        Number.isInteger(flag) &&
        flag >= 0 &&
        flag <= 0x7fff_ffff,
    )
  );
}

async function settled<T>(
  operation: () => T | PromiseLike<T>,
): Promise<Settlement<Awaited<T>>> {
  try {
    return { status: 'fulfilled', value: await operation() };
  } catch (reason) {
    return { status: 'rejected', reason };
  }
}

function errorHasCode(error: unknown, code: string): boolean {
  try {
    return (
      typeof error === 'object' &&
      error !== null &&
      Reflect.get(error, 'code') === code
    );
  } catch {
    return false;
  }
}

function directoryOpenFlags(
  primitives: FileSystemDatabaseExportStoreReceiptPrimitives & {
    readonly flags: Record<keyof ReceiptOpenFlags, number>;
  },
): number {
  return (
    primitives.flags.O_RDONLY |
    primitives.flags.O_DIRECTORY |
    primitives.flags.O_NOFOLLOW
  );
}

function fileOpenFlags(
  primitives: FileSystemDatabaseExportStoreReceiptPrimitives & {
    readonly flags: Record<keyof ReceiptOpenFlags, number>;
  },
): number {
  return (
    primitives.flags.O_RDONLY |
    primitives.flags.O_NOFOLLOW |
    primitives.flags.O_NONBLOCK
  );
}

function isExactDirectory(statistics: BigIntStats): boolean {
  return (
    statistics.isDirectory() &&
    (statistics.mode & PERMISSION_MODE) === DIRECTORY_MODE
  );
}

function isExactReceiptFile(
  statistics: BigIntStats,
  expectedLinks: 1n | 2n,
): boolean {
  return (
    statistics.isFile() &&
    (statistics.mode & PERMISSION_MODE) === FILE_MODE &&
    statistics.nlink === expectedLinks
  );
}

async function closeAll(
  handles: readonly FileHandle[],
  primitives: FileSystemDatabaseExportStoreReceiptPrimitives,
): Promise<void> {
  const errors: unknown[] = [];
  for (const handle of [...handles].reverse()) {
    const state = await settled(() => primitives.close(handle));
    if (state.status === 'rejected') errors.push(state.reason);
  }
  if (errors.length === 1) throw errors[0];
  if (errors.length > 1) {
    throw new AggregateError(
      errors,
      'database export receipt directory-handle cleanup failed',
    );
  }
}

async function openReceiptDirectories(
  root: string,
  databaseId: string,
  primitives: FileSystemDatabaseExportStoreReceiptPrimitives & {
    readonly flags: Record<keyof ReceiptOpenFlags, number>;
  },
): Promise<OpenReceiptDirectories> {
  const canonicalRoot = await primitives.realpath(root);
  if (canonicalRoot !== root) {
    throw new Error(
      'database export receipt root must be an existing trusted directory',
    );
  }
  const handles: FileHandle[] = [];
  try {
    const rootHandle = await primitives.open(
      root,
      directoryOpenFlags(primitives),
    );
    handles.push(rootHandle);
    const rootStat = await primitives.stat(rootHandle);
    if (!rootStat.isDirectory()) {
      throw new Error(
        'database export receipt root must be an existing trusted directory',
      );
    }

    let parentPath = root;
    let parentHandle = rootHandle;
    for (const segment of ['.anchorage-receipts', 'v1', databaseId] as const) {
      const childPath = join(parentPath, segment);
      let created = false;
      try {
        await primitives.mkdir(childPath, { mode: Number(DIRECTORY_MODE) });
        created = true;
      } catch (error) {
        if (!errorHasCode(error, 'EEXIST')) throw error;
      }
      const childHandle = await primitives.open(
        childPath,
        directoryOpenFlags(primitives),
      );
      handles.push(childHandle);
      if (created) {
        await primitives.chmod(childHandle, Number(DIRECTORY_MODE));
      }
      const childStat = await primitives.stat(childHandle);
      if (!isExactDirectory(childStat)) {
        throw new Error(
          'database export receipt directory must be a mode-0700 directory',
        );
      }
      await primitives.sync(parentHandle);
      parentPath = childPath;
      parentHandle = childHandle;
    }
    return {
      directory: parentPath,
      handle: parentHandle,
      handles,
    };
  } catch (error) {
    const cleanup = await settled(() => closeAll(handles, primitives));
    if (cleanup.status === 'rejected') {
      throw new AggregateError(
        [error, cleanup.reason],
        'database export receipt and directory-handle cleanup failed',
      );
    }
    throw error;
  }
}

async function hashReceiptHandle(
  handle: FileHandle,
  expectedSize: bigint,
  primitives: FileSystemDatabaseExportStoreReceiptPrimitives,
): Promise<DatabaseExportIntegrity> {
  if (expectedSize <= 0n || expectedSize > MAX_SAFE_SIZE) {
    throw databaseExportReceiptError('collision');
  }
  const hash = createHash('sha256');
  const buffer = new Uint8Array(64 * 1024);
  let size = 0n;
  try {
    for (;;) {
      const { bytesRead } = await primitives.read(
        handle,
        buffer,
        0,
        buffer.byteLength,
        null,
      );
      if (!Number.isSafeInteger(bytesRead) || bytesRead < 0) {
        throw databaseExportReceiptError('readback');
      }
      if (bytesRead === 0) break;
      hash.update(buffer.subarray(0, bytesRead));
      size += BigInt(bytesRead);
      if (size > MAX_SAFE_SIZE) {
        throw databaseExportReceiptError('readback');
      }
    }
  } catch {
    throw databaseExportReceiptError('readback');
  }
  if (size !== expectedSize) throw databaseExportReceiptError('readback');
  return { size: Number(size), sha256: hash.digest('hex') };
}

async function recoverReceiptTempAlias(
  targetHandle: FileHandle,
  targetStat: BigIntStats,
  databaseReceiptDirectory: string,
  databaseDirectoryHandle: FileHandle,
  primitives: FileSystemDatabaseExportStoreReceiptPrimitives & {
    readonly flags: Record<keyof ReceiptOpenFlags, number>;
  },
): Promise<void> {
  const hasRecoveredTarget = async (): Promise<boolean> => {
    let recovered: BigIntStats;
    try {
      recovered = await primitives.stat(targetHandle);
    } catch {
      throw databaseExportReceiptError('readback');
    }
    return (
      isExactReceiptFile(recovered, 1n) &&
      recovered.dev === targetStat.dev &&
      recovered.ino === targetStat.ino
    );
  };
  let names: string[];
  try {
    names = await primitives.readdir(databaseReceiptDirectory);
  } catch {
    throw databaseExportReceiptError('readback');
  }
  if (!Array.isArray(names) || names.some((name) => typeof name !== 'string')) {
    throw databaseExportReceiptError('readback');
  }
  const matches: { path: string; handle: FileHandle; stat: BigIntStats }[] = [];
  try {
    for (const name of names) {
      if (!RECEIPT_TEMP_PATTERN.test(name)) continue;
      const candidatePath = join(databaseReceiptDirectory, name);
      let candidate: FileHandle;
      try {
        candidate = await primitives.open(
          candidatePath,
          fileOpenFlags(primitives),
        );
      } catch (error) {
        if (errorHasCode(error, 'ENOENT')) continue;
        throw databaseExportReceiptError('readback');
      }
      let retained = false;
      try {
        let candidateStat: BigIntStats;
        try {
          candidateStat = await primitives.stat(candidate);
        } catch {
          throw databaseExportReceiptError('readback');
        }
        if (
          candidateStat.dev === targetStat.dev &&
          candidateStat.ino === targetStat.ino
        ) {
          retained = true;
          matches.push({
            path: candidatePath,
            handle: candidate,
            stat: candidateStat,
          });
        }
      } finally {
        if (!retained) await primitives.close(candidate);
      }
    }
    if (matches.length === 0 && (await hasRecoveredTarget())) return;
    if (matches.length !== 1) {
      throw databaseExportReceiptError('collision');
    }
    const match = matches[0];
    if (!match || !isExactReceiptFile(match.stat, 2n)) {
      throw databaseExportReceiptError('collision');
    }
    try {
      await primitives.unlink(match.path);
    } catch (error) {
      if (!errorHasCode(error, 'ENOENT')) {
        throw databaseExportReceiptError('readback');
      }
    }
    try {
      await primitives.sync(databaseDirectoryHandle);
    } catch {
      throw databaseExportReceiptError('readback');
    }
    if (!(await hasRecoveredTarget())) {
      throw databaseExportReceiptError('collision');
    }
  } finally {
    await closeAll(
      matches.map((match) => match.handle),
      primitives,
    );
  }
}

async function inspectReceiptTarget(
  targetPath: string,
  databaseReceiptDirectory: string,
  databaseDirectoryHandle: FileHandle,
  primitives: FileSystemDatabaseExportStoreReceiptPrimitives & {
    readonly flags: Record<keyof ReceiptOpenFlags, number>;
  },
): Promise<ReceiptTargetInspection> {
  let targetHandle: FileHandle;
  try {
    targetHandle = await primitives.open(targetPath, fileOpenFlags(primitives));
  } catch (error) {
    if (errorHasCode(error, 'ENOENT')) return { status: 'absent' };
    throw databaseExportReceiptError('collision');
  }
  try {
    let targetStat: BigIntStats;
    try {
      targetStat = await primitives.stat(targetHandle);
    } catch {
      throw databaseExportReceiptError('readback');
    }
    if (
      !targetStat.isFile() ||
      (targetStat.mode & PERMISSION_MODE) !== FILE_MODE ||
      (targetStat.nlink !== 1n && targetStat.nlink !== 2n)
    ) {
      throw databaseExportReceiptError('collision');
    }
    if (targetStat.nlink === 2n) {
      await recoverReceiptTempAlias(
        targetHandle,
        targetStat,
        databaseReceiptDirectory,
        databaseDirectoryHandle,
        primitives,
      );
    }
    let currentStat: BigIntStats;
    try {
      currentStat = await primitives.stat(targetHandle);
    } catch {
      throw databaseExportReceiptError('readback');
    }
    if (!isExactReceiptFile(currentStat, 1n)) {
      throw databaseExportReceiptError('collision');
    }
    try {
      await primitives.sync(databaseDirectoryHandle);
    } catch {
      throw databaseExportReceiptError('readback');
    }
    const integrity = await hashReceiptHandle(
      targetHandle,
      currentStat.size,
      primitives,
    );
    return {
      status: 'present',
      receipt: {
        location: pathToFileURL(targetPath).href,
        ...integrity,
      },
    };
  } finally {
    await primitives.close(targetHandle);
  }
}

function sameIntegrity(
  first: DatabaseExportIntegrity,
  second: DatabaseExportIntegrity,
): boolean {
  return first.size === second.size && first.sha256 === second.sha256;
}

function cancelReaderWithoutAwait(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  reason: unknown,
): void {
  try {
    const cancellation = Reflect.apply(READER_CANCEL, reader, [reason]);
    try {
      void Reflect.apply(PROMISE_THEN, cancellation, [
        undefined,
        () => undefined,
      ]);
    } catch {}
  } catch {}
}

async function streamReceipt(
  file: FileHandle,
  body: ReadableStream<Uint8Array>,
  primitives: FileSystemDatabaseExportStoreReceiptPrimitives,
  markCancellationAttempted: () => void,
): Promise<DatabaseExportIntegrity> {
  let reader: ReadableStreamDefaultReader<Uint8Array>;
  try {
    reader = Reflect.apply(
      READABLE_STREAM_GET_READER,
      body,
      [],
    ) as ReadableStreamDefaultReader<Uint8Array>;
  } catch {
    throw new Error('database export receipt body is malformed');
  }
  const hash = createHash('sha256');
  let size = 0n;
  try {
    for (;;) {
      const chunk = await Reflect.apply(READER_READ, reader, []);
      if (chunk.done) break;
      if (!(chunk.value instanceof Uint8Array)) {
        throw new Error('database export receipt body is malformed');
      }
      let offset = 0;
      while (offset < chunk.value.byteLength) {
        const { bytesWritten } = await primitives.write(
          file,
          chunk.value,
          offset,
          chunk.value.byteLength - offset,
        );
        if (!Number.isSafeInteger(bytesWritten) || bytesWritten <= 0) {
          throw new Error(
            'database export receipt file write made no progress',
          );
        }
        offset += bytesWritten;
      }
      hash.update(chunk.value);
      size += BigInt(chunk.value.byteLength);
      if (size > MAX_SAFE_SIZE) {
        throw new Error(
          'database export receipt size exceeds the safe integer range',
        );
      }
    }
  } catch (error) {
    markCancellationAttempted();
    cancelReaderWithoutAwait(reader, error);
    throw error;
  } finally {
    Reflect.apply(READER_RELEASE_LOCK, reader, []);
  }
  if (size === 0n) {
    throw new Error('database export receipt refuses an empty body');
  }
  return { size: Number(size), sha256: hash.digest('hex') };
}

async function unlinkOwnedTempPath(
  path: string,
  retainedStat: BigIntStats,
  expectedLinks: 1n | 2n | 'either',
  primitives: FileSystemDatabaseExportStoreReceiptPrimitives & {
    readonly flags: Record<keyof ReceiptOpenFlags, number>;
  },
): Promise<void> {
  let aliasHandle: FileHandle;
  try {
    aliasHandle = await primitives.open(path, fileOpenFlags(primitives));
  } catch (error) {
    if (errorHasCode(error, 'ENOENT')) return;
    throw error;
  }
  try {
    const aliasStat = await primitives.stat(aliasHandle);
    const linkCountMatches =
      expectedLinks === 'either'
        ? aliasStat.nlink === 1n || aliasStat.nlink === 2n
        : aliasStat.nlink === expectedLinks;
    if (
      !isExactReceiptFile(aliasStat, aliasStat.nlink === 2n ? 2n : 1n) ||
      !linkCountMatches ||
      aliasStat.dev !== retainedStat.dev ||
      aliasStat.ino !== retainedStat.ino
    ) {
      throw new Error(
        'database export receipt temporary file identity changed',
      );
    }
    await primitives.unlink(path);
  } finally {
    await primitives.close(aliasHandle);
  }
}

async function cleanupTemporaryHandle(
  path: string,
  retainedStat: BigIntStats,
  handle: FileHandle,
  primitives: FileSystemDatabaseExportStoreReceiptPrimitives & {
    readonly flags: Record<keyof ReceiptOpenFlags, number>;
  },
): Promise<void> {
  const pathCleanup = await settled(() =>
    unlinkOwnedTempPath(path, retainedStat, 'either', primitives),
  );
  const closeCleanup = await settled(() => primitives.close(handle));
  if (pathCleanup.status === 'rejected' && closeCleanup.status === 'rejected') {
    throw new AggregateError(
      [pathCleanup.reason, closeCleanup.reason],
      'database export receipt temporary-file cleanup failed',
    );
  }
  if (pathCleanup.status === 'rejected') throw pathCleanup.reason;
  if (closeCleanup.status === 'rejected') throw closeCleanup.reason;
}

async function publishReceipt(
  input: {
    readonly identity: DatabaseExportReceiptIdentity;
    readonly body: ReadableStream<Uint8Array>;
    readonly contentLength?: number;
  },
  expectedPromise: Promise<DatabaseExportIntegrity>,
  directories: OpenReceiptDirectories,
  targetPath: string,
  primitives: FileSystemDatabaseExportStoreReceiptPrimitives & {
    readonly flags: Record<keyof ReceiptOpenFlags, number>;
  },
  markCancellationAttempted: () => void,
): Promise<ReceiptFileState> {
  const uuid = primitives.randomUUID();
  if (!RECEIPT_TEMP_UUID.test(uuid)) {
    throw new Error('database export receipt temporary UUID is malformed');
  }
  const temporaryPath = join(directories.directory, `.receipt-${uuid}.tmp`);
  const temporaryHandle = await primitives.open(temporaryPath, 'wx', 0o600);
  const preparation = await settled(async () => {
    await primitives.chmod(temporaryHandle, Number(FILE_MODE));
    const statistics = await primitives.stat(temporaryHandle);
    if (!isExactReceiptFile(statistics, 1n)) {
      throw new Error(
        'database export receipt temporary file must be a mode-0600 regular file',
      );
    }
    return statistics;
  });
  if (preparation.status === 'rejected') {
    const recoveryStat = await settled(() => primitives.stat(temporaryHandle));
    let cleanup: Settlement<void>;
    if (recoveryStat.status === 'fulfilled') {
      cleanup = await settled(() =>
        cleanupTemporaryHandle(
          temporaryPath,
          recoveryStat.value,
          temporaryHandle,
          primitives,
        ),
      );
    } else {
      const closeCleanup = await settled(() =>
        primitives.close(temporaryHandle),
      );
      cleanup = {
        status: 'rejected',
        reason:
          closeCleanup.status === 'rejected'
            ? new AggregateError(
                [recoveryStat.reason, closeCleanup.reason],
                'database export receipt temporary-file cleanup failed',
              )
            : recoveryStat.reason,
      };
    }
    if (cleanup.status === 'rejected') {
      throw new AggregateError(
        [preparation.reason, cleanup.reason],
        'database export receipt and temporary-file cleanup failed',
      );
    }
    throw preparation.reason;
  }
  const retainedStat = preparation.value;
  let operation: Settlement<ReceiptFileState>;
  try {
    const streamed = await streamReceipt(
      temporaryHandle,
      input.body,
      primitives,
      markCancellationAttempted,
    );
    const expected = await expectedPromise;
    if (
      (input.contentLength !== undefined &&
        streamed.size !== input.contentLength) ||
      !sameIntegrity(streamed, expected)
    ) {
      throw databaseExportReceiptError('source-mismatch');
    }
    await primitives.sync(temporaryHandle);
    const committed = {
      location: pathToFileURL(targetPath).href,
      ...streamed,
    };
    let published = false;
    operation = await settled(async () => {
      try {
        await primitives.link(temporaryPath, targetPath);
        published = true;
        await unlinkOwnedTempPath(temporaryPath, retainedStat, 2n, primitives);
        await primitives.sync(directories.handle);
        return committed;
      } catch (primary) {
        if (published) throw primary;
        const existing = await inspectReceiptTarget(
          targetPath,
          directories.directory,
          directories.handle,
          primitives,
        );
        if (existing.status === 'absent') throw primary;
        if (!sameIntegrity(existing.receipt, expected)) {
          throw databaseExportReceiptError('collision');
        }
        return existing.receipt;
      }
    });
  } catch (reason) {
    operation = { status: 'rejected', reason };
  }

  const cleanup = await settled(() =>
    cleanupTemporaryHandle(
      temporaryPath,
      retainedStat,
      temporaryHandle,
      primitives,
    ),
  );
  if (operation.status === 'rejected' && cleanup.status === 'rejected') {
    throw new AggregateError(
      [operation.reason, cleanup.reason],
      'database export receipt and temporary-file cleanup failed',
    );
  }
  if (operation.status === 'rejected') throw operation.reason;
  if (cleanup.status === 'rejected') throw cleanup.reason;
  return operation.value;
}

function readReceiptInputField(
  input: unknown,
  field: 'body' | 'contentLength' | 'expectedIntegrity' | 'identity',
  errorKind: 'identity-malformed' | 'integrity-malformed',
): unknown {
  try {
    if (
      (typeof input !== 'object' || input === null) &&
      typeof input !== 'function'
    ) {
      throw databaseExportReceiptError(errorKind);
    }
    return Reflect.get(input, field);
  } catch {
    throw databaseExportReceiptError(errorKind);
  }
}

async function writeReceipt(
  root: string,
  authority: string,
  inputValue: unknown,
  primitives: FileSystemDatabaseExportStoreReceiptPrimitives & {
    readonly flags: Record<keyof ReceiptOpenFlags, number>;
  },
): Promise<ReceiptFileState> {
  const body = readReceiptInputField(inputValue, 'body', 'integrity-malformed');
  let cancellationAttempted = false;
  const cancel = (reason: unknown) => {
    if (cancellationAttempted) return;
    cancellationAttempted = true;
    cancelBodyWithoutAwait(body, reason);
  };
  let expected: Promise<DatabaseExportIntegrity>;
  try {
    const expectedValue = readReceiptInputField(
      inputValue,
      'expectedIntegrity',
      'integrity-malformed',
    );
    expected = captureDatabaseExportIntegrityPromise(expectedValue);
  } catch (error) {
    cancel(error);
    throw error;
  }
  try {
    const identity = databaseExportReceiptIdentityFromUnknown(
      readReceiptInputField(inputValue, 'identity', 'identity-malformed'),
      authority,
    );
    const contentLengthValue = readReceiptInputField(
      inputValue,
      'contentLength',
      'integrity-malformed',
    );
    if (
      contentLengthValue !== undefined &&
      (typeof contentLengthValue !== 'number' ||
        !Number.isSafeInteger(contentLengthValue) ||
        contentLengthValue < 1)
    ) {
      throw new Error(
        'database export receipt contentLength must be a positive safe integer',
      );
    }
    let locked: unknown;
    try {
      locked = Reflect.get(body as object, 'locked');
    } catch {
      throw new Error('database export receipt body is malformed');
    }
    if (locked !== false) {
      throw new Error('database export receipt body is locked or malformed');
    }
    const directories = await openReceiptDirectories(
      root,
      identity.databaseId,
      primitives,
    );
    try {
      const targetPath = join(
        directories.directory,
        `${identity.operationId}.sql`,
      );
      const existing = await inspectReceiptTarget(
        targetPath,
        directories.directory,
        directories.handle,
        primitives,
      );
      if (existing.status === 'present') {
        cancel(databaseExportReceiptError('collision'));
        const declared = await expected;
        if (!sameIntegrity(existing.receipt, declared)) {
          throw databaseExportReceiptError('collision');
        }
        return existing.receipt;
      }
      return await publishReceipt(
        {
          identity,
          body: body as ReadableStream<Uint8Array>,
          ...(contentLengthValue === undefined
            ? {}
            : { contentLength: contentLengthValue as number }),
        },
        expected,
        directories,
        targetPath,
        primitives,
        () => {
          cancellationAttempted = true;
        },
      );
    } finally {
      await closeAll(directories.handles, primitives);
    }
  } catch (error) {
    cancel(error);
    throw error;
  }
}

function configureReceiptCapability(
  store: FileSystemDatabaseExportStore,
  directory: string,
  primitives: FileSystemDatabaseExportStoreReceiptPrimitives,
): void {
  Reflect.deleteProperty(store, 'receiptAuthority');
  Reflect.deleteProperty(store, 'writeReceipt');
  if (!supportsFileSystemReceipts(primitives)) return;
  const authority = databaseExportReceiptAuthorityFromUnknown(
    pathToFileURL(join(directory, '.anchorage-receipts', 'v1')).href,
  );
  Object.defineProperties(store, {
    receiptAuthority: {
      configurable: true,
      enumerable: true,
      value: authority,
      writable: false,
    },
    writeReceipt: {
      configurable: true,
      enumerable: true,
      value: (input: unknown) =>
        writeReceipt(directory, authority, input, primitives),
      writable: false,
    },
  });
}

async function writeChunk(
  file: Awaited<ReturnType<typeof open>>,
  chunk: Uint8Array,
): Promise<void> {
  let offset = 0;
  while (offset < chunk.byteLength) {
    const { bytesWritten } = await file.write(
      chunk,
      offset,
      chunk.byteLength - offset,
    );
    if (bytesWritten === 0)
      throw new Error('export file write made no progress');
    offset += bytesWritten;
  }
}

export class FileSystemDatabaseExportStore
  implements DurableDatabaseExportStore
{
  readonly #directory: string;
  declare readonly receiptAuthority?: string;
  declare readonly writeReceipt?: NonNullable<
    DurableDatabaseExportStore['writeReceipt']
  >;

  constructor(directory: string) {
    if (directory.length === 0) throw new Error('export directory is required');
    this.#directory = resolve(directory);
    configureReceiptCapability(this, this.#directory, defaultReceiptPrimitives);
  }

  async write(input: {
    readonly databaseId: string;
    readonly fileName: string;
    readonly body: ReadableStream<Uint8Array>;
    readonly contentLength?: number;
  }): Promise<{
    readonly location: string;
    readonly size: number;
    readonly sha256: string;
  }> {
    assertFileName(input.fileName);
    if (
      input.contentLength !== undefined &&
      (!Number.isSafeInteger(input.contentLength) || input.contentLength < 0)
    ) {
      throw new Error(
        'export contentLength must be a non-negative safe integer',
      );
    }
    await mkdir(this.#directory, { recursive: true });
    const root = await realpath(this.#directory);
    const target = resolve(root, input.fileName);
    if (dirname(target) !== root) {
      throw new Error('export fileName resolves outside the configured root');
    }
    const temporary = join(root, `.${input.fileName}.${randomUUID()}.tmp`);
    let file: Awaited<ReturnType<typeof open>> | undefined;
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    try {
      file = await open(temporary, 'wx', 0o600);
      reader = input.body.getReader();
      let size = 0;
      const hash = createHash('sha256');
      for (;;) {
        const chunk = await reader.read();
        if (chunk.done) break;
        await writeChunk(file, chunk.value);
        hash.update(chunk.value);
        size += chunk.value.byteLength;
        if (!Number.isSafeInteger(size)) {
          throw new Error('export size exceeds the safe integer range');
        }
      }
      if (input.contentLength !== undefined && size !== input.contentLength) {
        throw new Error('export size differs from contentLength');
      }
      await file.sync();
      await file.close();
      file = undefined;
      await rename(temporary, target);
      const rootHandle = await open(root, 'r');
      try {
        await rootHandle.sync();
      } finally {
        await rootHandle.close();
      }
      return {
        location: pathToFileURL(target).href,
        size,
        sha256: hash.digest('hex'),
      };
    } catch (error) {
      const cleanupErrors: unknown[] = [];
      try {
        await file?.close();
      } catch (cleanupError) {
        cleanupErrors.push(cleanupError);
      }
      try {
        // The reader's cancel on a tee branch settles when the tee source is
        // exhausted or errors, or the other branch is cancelled, so cleanup
        // does not await it.
        void reader?.cancel(error).catch(() => undefined);
      } catch {}
      try {
        await rm(temporary, { force: true });
      } catch (cleanupError) {
        cleanupErrors.push(cleanupError);
      }
      if (cleanupErrors.length > 0) {
        throw new AggregateError(
          [error, ...cleanupErrors],
          'database export and temporary-file cleanup failed',
        );
      }
      throw error;
    } finally {
      reader?.releaseLock();
    }
  }
}

export function createFileSystemDatabaseExportStoreWithReceiptPrimitives(
  directory: string,
  overrides: FileSystemDatabaseExportStoreReceiptPrimitiveOverrides,
): FileSystemDatabaseExportStore {
  const store = new FileSystemDatabaseExportStore(directory);
  configureReceiptCapability(
    store,
    resolve(directory),
    receiptPrimitives(overrides),
  );
  return store;
}
