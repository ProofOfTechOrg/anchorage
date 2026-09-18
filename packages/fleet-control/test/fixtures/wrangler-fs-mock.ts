// SPDX-License-Identifier: Apache-2.0

import { afterEach, vi } from 'vitest';

export interface PlainWorkerFsControl {
  failFleetCleanup: boolean;
  residualDirectory: string | undefined;
  cleanupError: unknown;
  failOperation?: 'mkdtemp' | 'writeFile' | 'chmod' | 'stat';
  operationError?: unknown;
  mkdtempCalls?: number;
  scratchDirectories?: string[];
}

export async function createFsPromisesMock(control: PlainWorkerFsControl) {
  const actual =
    await vi.importActual<typeof import('node:fs/promises')>(
      'node:fs/promises',
    );
  return {
    ...actual,
    async mkdtemp(...arguments_: Parameters<typeof actual.mkdtemp>) {
      if (String(arguments_[0]).includes('anchorage-fleet-')) {
        if (control.mkdtempCalls !== undefined) control.mkdtempCalls += 1;
        if (control.failOperation === 'mkdtemp') {
          throw control.operationError;
        }
      }
      return actual.mkdtemp(...arguments_);
    },
    async writeFile(...arguments_: Parameters<typeof actual.writeFile>) {
      if (
        control.failOperation === 'writeFile' &&
        String(arguments_[0]).includes('anchorage-fleet-')
      ) {
        throw control.operationError;
      }
      return actual.writeFile(...arguments_);
    },
    async chmod(...arguments_: Parameters<typeof actual.chmod>) {
      if (control.failOperation === 'chmod') throw control.operationError;
      return actual.chmod(...arguments_);
    },
    async stat(...arguments_: Parameters<typeof actual.stat>) {
      if (control.failOperation === 'stat') throw control.operationError;
      return actual.stat(...arguments_);
    },
    async rm(
      path: Parameters<typeof actual.rm>[0],
      options: Parameters<typeof actual.rm>[1],
    ) {
      if (String(path).includes('anchorage-fleet-')) {
        control.scratchDirectories?.push(String(path));
      }
      if (
        control.failFleetCleanup &&
        String(path).includes('anchorage-fleet-')
      ) {
        control.residualDirectory = String(path);
        throw control.cleanupError;
      }
      return actual.rm(path, options);
    },
  };
}

export function registerScratchCleanup(
  control: PlainWorkerFsControl,
  defaults: {
    readonly cleanupError: unknown;
    readonly operationError?: unknown;
  },
): Set<string> {
  const exportDirectories = new Set<string>();
  afterEach(async () => {
    const actual =
      await vi.importActual<typeof import('node:fs/promises')>(
        'node:fs/promises',
      );
    control.failFleetCleanup = false;
    control.failOperation = undefined;
    if (control.residualDirectory) {
      await actual.rm(control.residualDirectory, {
        recursive: true,
        force: true,
      });
      control.residualDirectory = undefined;
    }
    await Promise.all(
      [...exportDirectories].map((directory) =>
        actual.rm(directory, { recursive: true, force: true }),
      ),
    );
    exportDirectories.clear();
    if (control.mkdtempCalls !== undefined) control.mkdtempCalls = 0;
    if (control.scratchDirectories) control.scratchDirectories.length = 0;
    control.cleanupError = defaults.cleanupError;
    control.operationError = defaults.operationError;
  });
  return exportDirectories;
}
