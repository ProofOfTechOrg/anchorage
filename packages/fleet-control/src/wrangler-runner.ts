// SPDX-License-Identifier: Apache-2.0

import { type ChildProcess, spawn } from 'node:child_process';

export interface CommandResult {
  readonly stdout: string;
  readonly stderr: string;
}

export interface CommandRunner {
  /** Hard upper bound after which the command and its subprocess tree terminate. */
  readonly maxDurationMs: number;
  run(
    arguments_: readonly string[],
    options?: { readonly input?: string; readonly cwd?: string },
  ): Promise<CommandResult>;
}

export function assertWranglerCommandPlatform(platform: NodeJS.Platform): void {
  if (platform === 'win32') {
    throw new Error(
      'WranglerCommandRunner does not support win32 because task-tree termination cannot be proven before the mutation lease expires',
    );
  }
}

export class WranglerCommandRunner implements CommandRunner {
  readonly #environment: NodeJS.ProcessEnv;
  readonly maxDurationMs: number;

  constructor(options: {
    readonly apiToken: string;
    readonly accountId: string;
    /** Must remain below the fleet lease TTL so a command cannot outlive ownership. */
    readonly timeoutMs?: number;
  }) {
    if (!options.apiToken || !options.accountId) {
      throw new Error('Wrangler requires apiToken and accountId');
    }
    assertWranglerCommandPlatform(process.platform);
    this.#environment = {
      ...process.env,
      CLOUDFLARE_ACCOUNT_ID: options.accountId,
      CLOUDFLARE_API_TOKEN: options.apiToken,
    };
    this.maxDurationMs = options.timeoutMs ?? 5 * 60_000;
    if (!Number.isSafeInteger(this.maxDurationMs) || this.maxDurationMs < 1) {
      throw new Error('Wrangler timeoutMs must be a positive integer');
    }
  }

  run(
    arguments_: readonly string[],
    options: { readonly input?: string; readonly cwd?: string } = {},
  ): Promise<CommandResult> {
    return new Promise((resolve, reject) => {
      const child = spawn('pnpm', ['exec', 'wrangler', ...arguments_], {
        cwd: options.cwd,
        detached: true,
        env: this.#environment,
        shell: false,
        stdio: ['pipe', 'pipe', 'pipe'],
        windowsHide: true,
      });
      let stdout = '';
      let stderr = '';
      child.stdout.setEncoding('utf8');
      child.stderr.setEncoding('utf8');
      child.stdout.on('data', (chunk: string) => {
        stdout += chunk;
      });
      child.stderr.on('data', (chunk: string) => {
        stderr += chunk;
      });
      let timedOut = false;
      const timeout = setTimeout(() => {
        timedOut = true;
        terminateProcessTree(child);
      }, this.maxDurationMs);
      child.once('error', (error) => {
        clearTimeout(timeout);
        reject(error);
      });
      child.once('close', (code, signal) => {
        clearTimeout(timeout);
        if (timedOut) {
          reject(new Error(`wrangler timed out after ${this.maxDurationMs}ms`));
          return;
        }
        if (code === 0) {
          resolve({ stdout, stderr });
          return;
        }
        reject(
          new Error(
            `wrangler exited ${code ?? `from signal ${signal ?? 'unknown'}`}: ${stderr.trim()}`,
          ),
        );
      });
      child.stdin.end(options.input);
    });
  }
}

function terminateProcessTree(child: ChildProcess): void {
  if (!child.pid) return;
  try {
    process.kill(-child.pid, 'SIGKILL');
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ESRCH') {
      child.kill('SIGKILL');
    }
  }
}
