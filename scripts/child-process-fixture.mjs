// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

export const repositoryRoot = dirname(dirname(fileURLToPath(import.meta.url)));

const watchdogMs = 15_000;

/** Reads the event log a fixture's child appends its marks to. */
export function activity(f) {
  return existsSync(f.events) ? readFileSync(f.events, 'utf8') : '';
}

/**
 * Builds the child-process harness a `node:test` suite drives its cases with:
 * `createRoot` returns `{ root, directory, events }` for a temporary repository.
 * `run(f, { entry, argv, flags, cwd, env, input })` requires a nonempty string
 * entry, which can be a script path, stdin's dash, or eval program text.
 *
 * `prefix` names the temporary directory, `watchdog` labels the timeout
 * diagnostic, `scripts` lists the repository scripts each root receives, and
 * `flags` and `argv` are the child's command line either side of its entry.
 * Children inherit the environment, then default `NODE_OPTIONS` to empty and
 * `COREPACK_ENABLE_NETWORK` and `COREPACK_ENABLE_AUTO_PIN` to `0`. Per-call
 * `env` overrides those defaults and inherited values.
 * These defaults exclude inherited Node flags and keep child package-manager
 * calls offline without Corepack pinning a version in the fixture manifest.
 */
export function createChildProcessFixture({
  prefix,
  watchdog,
  scripts = [],
  flags: defaultFlags = ['--experimental-transform-types'],
  argv: defaultArgv = [],
}) {
  function createRoot(t) {
    const directory = mkdtempSync(join(tmpdir(), prefix));
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    const root = join(directory, 'repo');
    mkdirSync(join(root, 'scripts'), { recursive: true });
    for (const name of scripts) {
      copyFileSync(
        join(repositoryRoot, 'scripts', name),
        join(root, 'scripts', name),
      );
    }
    return { root, directory, events: join(root, 'events.log') };
  }

  async function run(
    f,
    {
      entry,
      argv = defaultArgv,
      flags = defaultFlags,
      cwd = f.directory,
      env = {},
      input,
    } = {},
  ) {
    if (typeof entry !== 'string' || entry.length === 0) {
      throw new TypeError(
        'child-process fixture entry must be a nonempty string',
      );
    }
    const child = spawn(
      process.execPath,
      ['--no-warnings', ...flags, entry, ...argv],
      {
        cwd,
        env: {
          ...process.env,
          NODE_OPTIONS: '',
          COREPACK_ENABLE_NETWORK: '0',
          COREPACK_ENABLE_AUTO_PIN: '0',
          ...env,
        },
        stdio: [input === undefined ? 'ignore' : 'pipe', 'pipe', 'pipe'],
        detached: process.platform !== 'win32',
      },
    );
    let stdout = '';
    let stderr = '';
    let timedOut = false;
    if (input !== undefined) child.stdin.end(input);
    child.stdout.setEncoding('utf8').on('data', (chunk) => {
      stdout += chunk;
    });
    child.stderr.setEncoding('utf8').on('data', (chunk) => {
      stderr += chunk;
    });
    const timer = setTimeout(() => {
      timedOut = true;
      if (process.platform === 'win32') child.kill('SIGKILL');
      else process.kill(-child.pid, 'SIGKILL');
    }, watchdogMs);
    try {
      const result = await new Promise((settle, reject) => {
        child.once('error', reject);
        child.once('close', (status, signal) => settle({ status, signal }));
      });
      assert.equal(
        timedOut,
        false,
        `${watchdog} watchdog expired\n${stdout}\n${stderr}`,
      );
      assert.equal(result.signal, null, stderr);
      return { ...result, stdout, stderr };
    } finally {
      clearTimeout(timer);
    }
  }

  return { createRoot, run };
}
