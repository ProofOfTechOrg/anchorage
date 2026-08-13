// SPDX-License-Identifier: Apache-2.0

import { chmod, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  assertWranglerCommandPlatform,
  WranglerCommandRunner,
} from '../src/wrangler-runner.js';

afterEach(() => {
  vi.unstubAllEnvs();
});

describe('WranglerCommandRunner', () => {
  it('exposes and validates its maximum command duration', () => {
    expect(
      new WranglerCommandRunner({
        apiToken: 'token',
        accountId: 'account',
        timeoutMs: 12_345,
      }).maxDurationMs,
    ).toBe(12_345);
    expect(
      () =>
        new WranglerCommandRunner({
          apiToken: 'token',
          accountId: 'account',
          timeoutMs: 0,
        }),
    ).toThrow('Wrangler timeoutMs must be a positive integer');
  });

  it('validates the Wrangler command executable and fixed arguments', () => {
    expect(
      () =>
        new WranglerCommandRunner({
          apiToken: 'token',
          accountId: 'account',
          wranglerCommand: [],
        }),
    ).toThrow(/wranglerCommand must contain an executable/);
    expect(
      () =>
        new WranglerCommandRunner({
          apiToken: 'token',
          accountId: 'account',
          wranglerCommand: ['wrangler', ''],
        }),
    ).toThrow(/only non-empty string arguments/);
  });

  it.skipIf(process.platform === 'win32')(
    'runs a defensively copied custom executable with fixed arguments and Cloudflare credentials',
    async () => {
      const directory = await mkdtemp(join(tmpdir(), 'wrangler-runner-'));
      const commandPath = join(directory, 'custom-wrangler');
      try {
        await writeFile(
          commandPath,
          `#!/usr/bin/env node
process.stdout.write(JSON.stringify({
  arguments: process.argv.slice(2),
  accountId: process.env.CLOUDFLARE_ACCOUNT_ID,
  apiToken: process.env.CLOUDFLARE_API_TOKEN,
}));
`,
        );
        await chmod(commandPath, 0o755);
        const wranglerCommand = [commandPath, '--fixed', 'fixed-value'];
        const runner = new WranglerCommandRunner({
          apiToken: 'test-token',
          accountId: 'test-account',
          wranglerCommand,
        });
        wranglerCommand[0] = join(directory, 'mutated-command');
        wranglerCommand[2] = 'mutated-value';

        const result = await runner.run(['deploy', '--name', 'test-worker']);

        expect(JSON.parse(result.stdout)).toEqual({
          arguments: [
            '--fixed',
            'fixed-value',
            'deploy',
            '--name',
            'test-worker',
          ],
          accountId: 'test-account',
          apiToken: 'test-token',
        });
      } finally {
        await rm(directory, { recursive: true, force: true });
      }
    },
  );

  it('rejects win32 where descendant termination cannot be proven', () => {
    expect(() => assertWranglerCommandPlatform('win32')).toThrow(
      /does not support win32/,
    );
    expect(() => assertWranglerCommandPlatform('linux')).not.toThrow();
  });

  it.skipIf(process.platform === 'win32')(
    'terminates the complete spawned process group at the maximum duration',
    async () => {
      const directory = await mkdtemp(join(tmpdir(), 'wrangler-runner-'));
      const commandPath = join(directory, 'pnpm');
      const descendantPidPath = join(directory, 'descendant.pid');
      try {
        await writeFile(
          commandPath,
          `#!/usr/bin/env node
const { spawn } = require('node:child_process');
const { writeFileSync } = require('node:fs');
const descendant = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' });
writeFileSync(process.env.WRANGLER_RUNNER_DESCENDANT_PID, String(descendant.pid));
setInterval(() => {}, 1000);
`,
        );
        await chmod(commandPath, 0o755);
        vi.stubEnv('PATH', `${directory}:${process.env.PATH ?? ''}`);
        vi.stubEnv('WRANGLER_RUNNER_DESCENDANT_PID', descendantPidPath);
        const runner = new WranglerCommandRunner({
          apiToken: 'token',
          accountId: 'account',
          timeoutMs: 500,
        });

        await expect(runner.run(['deploy'])).rejects.toThrow(
          'wrangler timed out after 500ms',
        );
        const descendantPid = Number(await readFile(descendantPidPath, 'utf8'));
        expect(Number.isSafeInteger(descendantPid)).toBe(true);
        await vi.waitFor(
          () => {
            expect(processExists(descendantPid)).toBe(false);
          },
          { timeout: 2_000 },
        );
      } finally {
        await rm(directory, { recursive: true, force: true });
      }
    },
    10_000,
  );
});

function processExists(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ESRCH') return false;
    throw error;
  }
}
