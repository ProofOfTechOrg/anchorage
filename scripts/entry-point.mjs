// SPDX-License-Identifier: Apache-2.0

import { realpathSync } from 'node:fs';
import { isAbsolute, resolve } from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';
import { isMainThread } from 'node:worker_threads';

/**
 * Reports whether the process entry point is the module the `file:` URL
 * `moduleUrl` (a string or `URL`) names.
 *
 * Both sides are realpathed, so a symlinked invocation still resolves to the
 * module's own path, and an entry path that resolves to nothing names some
 * other module — which importers of a guarded module rely on.
 *
 * `-` is stdin's entry name and a worker's virtual name is not a path, so a
 * file of either name would compare equal to a module it does not name. An
 * `--eval` or `--print` program has no module of its own, and worker files
 * inherit the parent process's eval flags, so `execArgv` decides on the main
 * thread alone.
 */
export function isInvokedAsEntryPoint(moduleUrl) {
  const entry = process.argv[1];
  if (!entry) return false;
  if (
    entry === '-' ||
    (!isMainThread && !isAbsolute(entry)) ||
    (isMainThread &&
      process.execArgv.some((argument) =>
        /^(?:-[ep]|--(?:eval|print)(?:=|$))/u.test(argument),
      ))
  )
    return false;
  let invokedFilePath;
  try {
    invokedFilePath = realpathSync(resolve(entry));
  } catch (error) {
    if (error?.code === 'ENOENT' || error?.code === 'ENOTDIR') return false;
    throw error;
  }
  return invokedFilePath === realpathSync(fileURLToPath(moduleUrl));
}
