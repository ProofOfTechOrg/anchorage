// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import {
  lstat,
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  realpath,
  rm,
  stat,
  symlink,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { isAbsolute, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { FileSystemDatabaseExportStore } from '../src/export-store.js';

const encoder = new TextEncoder();
const temporaryDirectories: string[] = [];

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
});
