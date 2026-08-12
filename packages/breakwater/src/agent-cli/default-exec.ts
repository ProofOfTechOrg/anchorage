// SPDX-License-Identifier: Apache-2.0
// Package-internal Node executor. Runtime built-ins stay behind structural
// lookups so importing the public agent-cli entry point remains portable.

import type { AgentCliExec, AgentCliExecResult } from './index.js';
import {
  type TextCodecLookups,
  type TextDecoderLike,
  type TextEncoderLike,
  tailAccumulator,
} from './tail-accumulator.js';

export type ProcessTreeTerminationMethod = 'process-group' | 'taskkill';

type DefaultExecFailureCode =
  | 'runtime-unavailable'
  | 'codec-unavailable'
  | 'spawn-failed'
  | 'timeout'
  | 'termination-failed';

interface DefaultExecFailureOptions {
  code: DefaultExecFailureCode;
  timeoutMs?: number;
  exitCode?: number;
  systemCode?: string;
  terminationMethod?: ProcessTreeTerminationMethod;
}

export class DefaultExecFailure {
  readonly code: DefaultExecFailureCode;
  readonly timeoutMs?: number;
  readonly exitCode?: number;
  readonly systemCode?: string;
  readonly terminationMethod?: ProcessTreeTerminationMethod;

  constructor(options: DefaultExecFailureOptions) {
    this.code = options.code;
    this.timeoutMs = options.timeoutMs;
    this.exitCode = options.exitCode;
    this.systemCode = options.systemCode;
    this.terminationMethod = options.terminationMethod;
  }
}

export interface SpawnedProcess {
  readonly pid?: number;
  stdout: {
    on(event: 'data', listener: (chunk: unknown) => void): unknown;
  } | null;
  stderr: {
    on(event: 'data', listener: (chunk: unknown) => void): unknown;
  } | null;
  on(event: 'error', listener: (error: unknown) => void): unknown;
  on(event: 'close', listener: (code: number | null) => void): unknown;
}

export interface SpawnOptions {
  cwd?: string;
  stdio: readonly [string, string, string] | 'ignore';
  shell: false;
  detached?: true;
  windowsHide?: true;
}

export interface ChildProcessModule {
  spawn(
    command: string,
    args: readonly string[],
    options: SpawnOptions,
  ): SpawnedProcess;
}

export interface TimerRuntime {
  setTimeout(handler: () => void, timeoutMs: number): unknown;
  clearTimeout(handle: unknown): void;
}

export interface DefaultExecRuntime {
  readonly childProcess: ChildProcessModule;
  readonly platform: string;
  readonly taskkillPath?: string;
  readonly kill: (pid: number, signal: string | number) => unknown;
  readonly delay: (delayMs: number) => Promise<void>;
  readonly timers: TimerRuntime;
  readonly codecs: TextCodecLookups;
}

interface WindowsPathModule {
  readonly win32: {
    isAbsolute(path: string): boolean;
    join(...paths: string[]): string;
  };
}

interface ProcessTreeFailureOptions {
  method: ProcessTreeTerminationMethod;
  exitCode?: number;
  systemCode?: string;
}

class ProcessTreeFailure {
  readonly method: ProcessTreeTerminationMethod;
  readonly exitCode?: number;
  readonly systemCode?: string;

  constructor(options: ProcessTreeFailureOptions) {
    this.method = options.method;
    this.exitCode = options.exitCode;
    this.systemCode = options.systemCode;
  }
}

function safeSystemCode(error: unknown): string | undefined {
  if (typeof error !== 'object' || error === null) return undefined;
  const code = (error as { code?: unknown }).code;
  return typeof code === 'string' && /^[A-Z][A-Z0-9_]{0,31}$/.test(code)
    ? code
    : undefined;
}

function globalTextDecoder(): TextDecoderLike {
  const Ctor = (globalThis as { TextDecoder?: new () => TextDecoderLike })
    .TextDecoder;
  if (!Ctor) {
    throw new DefaultExecFailure({ code: 'codec-unavailable' });
  }
  try {
    return new Ctor();
  } catch {
    throw new DefaultExecFailure({ code: 'codec-unavailable' });
  }
}

function globalTextEncoder(): TextEncoderLike {
  const Ctor = (globalThis as { TextEncoder?: new () => TextEncoderLike })
    .TextEncoder;
  if (!Ctor) {
    throw new DefaultExecFailure({ code: 'codec-unavailable' });
  }
  try {
    return new Ctor();
  } catch {
    throw new DefaultExecFailure({ code: 'codec-unavailable' });
  }
}

export function resolveWindowsTaskkillPath(
  platform: string,
  environment: Record<string, string | undefined> | undefined,
  pathModule: WindowsPathModule | undefined,
): string | undefined {
  if (platform !== 'win32') return undefined;
  const systemRoot = [environment?.SystemRoot, environment?.WINDIR].find(
    (candidate): candidate is string =>
      isDriveAbsoluteWindowsPath(candidate) &&
      pathModule?.win32.isAbsolute(candidate) === true,
  );
  if (!systemRoot || !pathModule) {
    throw new DefaultExecFailure({ code: 'runtime-unavailable' });
  }
  const taskkillPath = pathModule.win32.join(
    systemRoot,
    'System32',
    'taskkill.exe',
  );
  if (
    !isDriveAbsoluteWindowsPath(taskkillPath) ||
    !pathModule.win32.isAbsolute(taskkillPath)
  ) {
    throw new DefaultExecFailure({ code: 'runtime-unavailable' });
  }
  return taskkillPath;
}

function isDriveAbsoluteWindowsPath(path: string | undefined): path is string {
  return typeof path === 'string' && /^[A-Za-z]:[\\/]/.test(path);
}

function defaultRuntime(): DefaultExecRuntime {
  const proc = (
    globalThis as {
      process?: {
        platform?: unknown;
        env?: Record<string, string | undefined>;
        getBuiltinModule?: (id: string) => unknown;
        kill?: (pid: number, signal: string | number) => unknown;
      };
    }
  ).process;
  const childProcess = proc?.getBuiltinModule?.('node:child_process') as
    | ChildProcessModule
    | undefined;
  if (
    !childProcess ||
    typeof childProcess.spawn !== 'function' ||
    typeof proc?.platform !== 'string' ||
    typeof proc.kill !== 'function'
  ) {
    throw new DefaultExecFailure({ code: 'runtime-unavailable' });
  }
  const pathModule = proc.getBuiltinModule?.('node:path') as
    | WindowsPathModule
    | undefined;
  const taskkillPath = resolveWindowsTaskkillPath(
    proc.platform,
    proc.env,
    pathModule,
  );
  return {
    childProcess,
    platform: proc.platform,
    taskkillPath,
    kill: proc.kill.bind(proc),
    delay: (delayMs) =>
      new Promise((resolve) => {
        globalThis.setTimeout(resolve, delayMs);
      }),
    timers: globalThis as unknown as TimerRuntime,
    codecs: {
      encoder: globalTextEncoder,
      decoder: globalTextDecoder,
    },
  };
}

const PROCESS_GROUP_EXIT_PROBES = 50;
const PROCESS_GROUP_EXIT_PROBE_MS = 100;

async function waitForProcessGroupExit(
  runtime: DefaultExecRuntime,
  pid: number,
): Promise<void> {
  for (let attempt = 0; attempt < PROCESS_GROUP_EXIT_PROBES; attempt += 1) {
    try {
      runtime.kill(-pid, 0);
    } catch (error) {
      const systemCode = safeSystemCode(error);
      if (systemCode === 'ESRCH') return;
      throw new ProcessTreeFailure({
        method: 'process-group',
        systemCode,
      });
    }
    if (attempt + 1 < PROCESS_GROUP_EXIT_PROBES) {
      await runtime.delay(PROCESS_GROUP_EXIT_PROBE_MS);
    }
  }
  throw new ProcessTreeFailure({
    method: 'process-group',
    systemCode: 'PROCESS_GROUP_STILL_ALIVE',
  });
}

function spawnAgentCli(
  runtime: DefaultExecRuntime,
  command: string,
  args: readonly string[],
  cwd: string | undefined,
): SpawnedProcess {
  const options: SpawnOptions =
    runtime.platform === 'win32'
      ? {
          cwd,
          stdio: ['ignore', 'pipe', 'pipe'],
          shell: false,
        }
      : {
          cwd,
          stdio: ['ignore', 'pipe', 'pipe'],
          shell: false,
          // On non-Windows Node creates a new process group and session whose
          // id is the child pid. A negative-pid signal can then reach every
          // descendant that remains in that group.
          detached: true,
        };
  return runtime.childProcess.spawn(command, args, options);
}

function waitForTaskkill(
  runtime: DefaultExecRuntime,
  pid: number,
): Promise<void> {
  return new Promise((resolve, reject) => {
    let taskkill: SpawnedProcess;
    try {
      taskkill = runtime.childProcess.spawn(
        runtime.taskkillPath as string,
        ['/pid', String(pid), '/T', '/F'],
        {
          stdio: 'ignore',
          shell: false,
          windowsHide: true,
        },
      );
    } catch (error) {
      reject(
        new ProcessTreeFailure({
          method: 'taskkill',
          systemCode: safeSystemCode(error),
        }),
      );
      return;
    }
    let settled = false;
    taskkill.on('error', (error) => {
      if (settled) return;
      settled = true;
      reject(
        new ProcessTreeFailure({
          method: 'taskkill',
          systemCode: safeSystemCode(error),
        }),
      );
    });
    taskkill.on('close', (code) => {
      if (settled) return;
      settled = true;
      if (code === 0) {
        resolve();
        return;
      }
      reject(
        new ProcessTreeFailure({
          method: 'taskkill',
          exitCode: code ?? -1,
        }),
      );
    });
  });
}

async function terminateProcessTree(
  runtime: DefaultExecRuntime,
  pid: number | undefined,
): Promise<void> {
  const method = runtime.platform === 'win32' ? 'taskkill' : 'process-group';
  if (!Number.isSafeInteger(pid) || (pid ?? 0) <= 0) {
    throw new ProcessTreeFailure({
      method,
      systemCode: 'PROCESS_ID_UNAVAILABLE',
    });
  }
  const safePid = pid as number;
  if (method === 'taskkill') {
    await waitForTaskkill(runtime, safePid);
    return;
  }
  try {
    runtime.kill(-safePid, 'SIGKILL');
  } catch (error) {
    const systemCode = safeSystemCode(error);
    if (systemCode === 'ESRCH') return;
    throw new ProcessTreeFailure({
      method,
      systemCode,
    });
  }
  await waitForProcessGroupExit(runtime, safePid);
}

export function createDefaultExec(
  maxOutputBytes: number,
  injectedRuntime?: DefaultExecRuntime,
): AgentCliExec {
  return (command, args, options) => {
    let runtime: DefaultExecRuntime;
    try {
      runtime = injectedRuntime ?? defaultRuntime();
      if (
        runtime.platform === 'win32' &&
        !isDriveAbsoluteWindowsPath(runtime.taskkillPath)
      ) {
        throw new DefaultExecFailure({ code: 'runtime-unavailable' });
      }
    } catch (error) {
      return Promise.reject(error);
    }
    return new Promise<AgentCliExecResult>((resolve, reject) => {
      let child: SpawnedProcess;
      try {
        child = spawnAgentCli(runtime, command, args, options.cwd);
      } catch (error) {
        reject(
          new DefaultExecFailure({
            code: 'spawn-failed',
            systemCode: safeSystemCode(error),
          }),
        );
        return;
      }
      const stdout = tailAccumulator(maxOutputBytes, runtime.codecs);
      const stderr = tailAccumulator(maxOutputBytes, runtime.codecs);
      let settled = false;
      let timer: unknown;

      const abort = async (failure: DefaultExecFailure): Promise<void> => {
        if (settled) return;
        settled = true;
        runtime.timers.clearTimeout(timer);
        try {
          await terminateProcessTree(runtime, child.pid);
        } catch (error) {
          const processTreeFailure =
            error instanceof ProcessTreeFailure
              ? error
              : new ProcessTreeFailure({
                  method:
                    runtime.platform === 'win32' ? 'taskkill' : 'process-group',
                });
          reject(
            new DefaultExecFailure({
              code: 'termination-failed',
              timeoutMs: failure.timeoutMs,
              exitCode: processTreeFailure.exitCode,
              systemCode: processTreeFailure.systemCode,
              terminationMethod: processTreeFailure.method,
            }),
          );
          return;
        }
        reject(failure);
      };

      timer = runtime.timers.setTimeout(() => {
        void abort(
          new DefaultExecFailure({
            code: 'timeout',
            timeoutMs: options.timeoutMs,
          }),
        );
      }, options.timeoutMs);
      child.stdout?.on('data', (chunk) => {
        try {
          stdout.push(chunk);
        } catch {
          void abort(new DefaultExecFailure({ code: 'codec-unavailable' }));
        }
      });
      child.stderr?.on('data', (chunk) => {
        try {
          stderr.push(chunk);
        } catch {
          void abort(new DefaultExecFailure({ code: 'codec-unavailable' }));
        }
      });
      child.on('error', (error) => {
        if (settled) return;
        settled = true;
        runtime.timers.clearTimeout(timer);
        reject(
          new DefaultExecFailure({
            code: 'spawn-failed',
            systemCode: safeSystemCode(error),
          }),
        );
      });
      child.on('close', (code) => {
        if (settled) return;
        settled = true;
        runtime.timers.clearTimeout(timer);
        try {
          resolve({
            stdout: stdout.value(),
            stderr: stderr.value(),
            exitCode: code ?? -1,
          });
        } catch {
          reject(new DefaultExecFailure({ code: 'codec-unavailable' }));
        }
      });
    });
  };
}
