// SPDX-License-Identifier: Apache-2.0

import { createHash, randomUUID } from 'node:crypto';
import { mkdir, open, realpath, rename, rm } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import type { DurableDatabaseExportStore } from './database-export-store.js';
import { assertFileName } from './export-file-name.js';

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

  constructor(directory: string) {
    if (directory.length === 0) throw new Error('export directory is required');
    this.#directory = resolve(directory);
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
