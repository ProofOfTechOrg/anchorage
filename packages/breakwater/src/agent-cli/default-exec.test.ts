// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';

import {
  type ChildProcessModule,
  createDefaultExec,
  type DefaultExecRuntime,
  type SpawnedProcess,
  type SpawnOptions,
  type TimerRuntime,
} from './default-exec.js';

class FakeStream {
  readonly #listeners: Array<(chunk: unknown) => void> = [];

  on(_event: 'data', listener: (chunk: unknown) => void): void {
    this.#listeners.push(listener);
  }

  emit(chunk: unknown): void {
    for (const listener of this.#listeners) listener(chunk);
  }
}

class FakeProcess implements SpawnedProcess {
  readonly stdout = new FakeStream();
  readonly stderr = new FakeStream();
  readonly #errorListeners: Array<(error: unknown) => void> = [];
  readonly #closeListeners: Array<(code: number | null) => void> = [];

  constructor(readonly pid?: number) {}

  on(
    event: 'error' | 'close',
    listener: ((error: unknown) => void) | ((code: number | null) => void),
  ): void {
    if (event === 'error') {
      this.#errorListeners.push(listener as (error: unknown) => void);
      return;
    }
    this.#closeListeners.push(listener as (code: number | null) => void);
  }

  emitError(error: unknown): void {
    for (const listener of this.#errorListeners) listener(error);
  }

  emitClose(code: number | null): void {
    for (const listener of this.#closeListeners) listener(code);
  }
}

interface FakeRuntime {
  runtime: DefaultExecRuntime;
  spawn: ReturnType<
    typeof vi.fn<
      (
        command: string,
        args: readonly string[],
        options: SpawnOptions,
      ) => SpawnedProcess
    >
  >;
  kill: ReturnType<
    typeof vi.fn<(pid: number, signal: string | number) => unknown>
  >;
  delay: ReturnType<typeof vi.fn<(delayMs: number) => Promise<void>>>;
  clearTimeout: ReturnType<typeof vi.fn<(handle: unknown) => void>>;
  fireTimeout(): void;
}

function fakeRuntime(platform: string): FakeRuntime {
  let timeoutHandler: (() => void) | undefined;
  const spawn =
    vi.fn<
      (
        command: string,
        args: readonly string[],
        options: SpawnOptions,
      ) => SpawnedProcess
    >();
  const kill = vi.fn<(pid: number, signal: string | number) => unknown>(
    (_pid, signal) => {
      if (signal === 0) throw systemError('ESRCH');
    },
  );
  const delay = vi.fn(async () => {});
  const clearTimeout = vi.fn<(handle: unknown) => void>();
  const timers: TimerRuntime = {
    setTimeout(handler) {
      timeoutHandler = handler;
      return 'timer-handle';
    },
    clearTimeout,
  };
  const childProcess: ChildProcessModule = { spawn };
  return {
    runtime: {
      childProcess,
      platform,
      kill,
      delay,
      timers,
      codecs: {
        encoder: () => new TextEncoder(),
        decoder: () => new TextDecoder(),
      },
    },
    spawn,
    kill,
    delay,
    clearTimeout,
    fireTimeout() {
      if (!timeoutHandler) throw new Error('timeout was not armed');
      timeoutHandler();
    },
  };
}

function systemError(code: string): Error & { code: string } {
  return Object.assign(new Error('private operating-system detail'), { code });
}

describe('default Agent CLI process-tree execution', () => {
  it('starts a POSIX session and kills its process group with a negative pid', async () => {
    const fake = fakeRuntime('linux');
    const child = new FakeProcess(8123);
    fake.spawn.mockReturnValue(child);
    const promise = createDefaultExec(1024, fake.runtime)(
      'agent-cli',
      ['--', 'private prompt'],
      { cwd: '/workspace', timeoutMs: 50 },
    );

    expect(fake.spawn).toHaveBeenCalledWith(
      'agent-cli',
      ['--', 'private prompt'],
      {
        cwd: '/workspace',
        stdio: ['ignore', 'pipe', 'pipe'],
        shell: false,
        detached: true,
      },
    );
    fake.fireTimeout();

    await expect(promise).rejects.toMatchObject({
      code: 'timeout',
      timeoutMs: 50,
    });
    expect(fake.kill).toHaveBeenNthCalledWith(1, -8123, 'SIGKILL');
    expect(fake.kill).toHaveBeenNthCalledWith(2, -8123, 0);
    expect(fake.delay).not.toHaveBeenCalled();
  });

  it('waits until the POSIX process group is absent before timeout settles', async () => {
    const fake = fakeRuntime('linux');
    fake.spawn.mockReturnValue(new FakeProcess(8123));
    let probes = 0;
    fake.kill.mockImplementation((_pid, signal) => {
      if (signal !== 0) return;
      probes += 1;
      if (probes > 1) throw systemError('ESRCH');
    });
    const promise = createDefaultExec(1024, fake.runtime)('agent-cli', [], {
      timeoutMs: 50,
    });

    fake.fireTimeout();

    await expect(promise).rejects.toMatchObject({ code: 'timeout' });
    expect(fake.delay).toHaveBeenCalledExactlyOnceWith(100);
    expect(fake.kill).toHaveBeenLastCalledWith(-8123, 0);
  });

  it('fails closed when a POSIX process group remains alive', async () => {
    const fake = fakeRuntime('linux');
    fake.spawn.mockReturnValue(new FakeProcess(8123));
    fake.kill.mockImplementation(() => {});
    const promise = createDefaultExec(1024, fake.runtime)('agent-cli', [], {
      timeoutMs: 50,
    });

    fake.fireTimeout();

    await expect(promise).rejects.toMatchObject({
      code: 'termination-failed',
      terminationMethod: 'process-group',
      systemCode: 'PROCESS_GROUP_STILL_ALIVE',
    });
    expect(fake.delay).toHaveBeenCalledTimes(49);
  });

  it('spawns normally on Windows and waits for exact argv taskkill completion', async () => {
    const fake = fakeRuntime('win32');
    const child = new FakeProcess(1234);
    const taskkill = new FakeProcess(5678);
    fake.spawn.mockReturnValueOnce(child).mockReturnValueOnce(taskkill);
    const promise = createDefaultExec(1024, fake.runtime)(
      'agent-cli.exe',
      ['--', 'private prompt'],
      { cwd: 'C:\\workspace', timeoutMs: 50 },
    );
    let settled = false;
    void promise.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );

    expect(fake.spawn).toHaveBeenNthCalledWith(
      1,
      'agent-cli.exe',
      ['--', 'private prompt'],
      {
        cwd: 'C:\\workspace',
        stdio: ['ignore', 'pipe', 'pipe'],
        shell: false,
      },
    );
    fake.fireTimeout();
    await Promise.resolve();

    expect(fake.spawn).toHaveBeenNthCalledWith(
      2,
      'taskkill.exe',
      ['/pid', '1234', '/T', '/F'],
      { stdio: 'ignore', shell: false, windowsHide: true },
    );
    expect(settled).toBe(false);
    taskkill.emitClose(0);
    await expect(promise).rejects.toMatchObject({
      code: 'timeout',
      timeoutMs: 50,
    });
    expect(settled).toBe(true);
  });

  it.each([
    'linux',
    'win32',
  ])('%s normal close clears the timer and never terminates', async (platform) => {
    const fake = fakeRuntime(platform);
    const child = new FakeProcess(42);
    fake.spawn.mockReturnValue(child);
    const promise = createDefaultExec(1024, fake.runtime)('agent-cli', [], {
      timeoutMs: 50,
    });

    child.stdout.emit(new TextEncoder().encode('done'));
    child.emitClose(0);

    await expect(promise).resolves.toEqual({
      stdout: 'done',
      stderr: '',
      exitCode: 0,
    });
    expect(fake.clearTimeout).toHaveBeenCalledWith('timer-handle');
    expect(fake.kill).not.toHaveBeenCalled();
    expect(fake.spawn).toHaveBeenCalledTimes(1);
  });

  it('treats POSIX ESRCH as an already-terminated process group', async () => {
    const fake = fakeRuntime('darwin');
    fake.spawn.mockReturnValue(new FakeProcess(42));
    fake.kill.mockImplementation(() => {
      throw systemError('ESRCH');
    });
    const promise = createDefaultExec(1024, fake.runtime)('agent-cli', [], {
      timeoutMs: 50,
    });

    fake.fireTimeout();

    await expect(promise).rejects.toMatchObject({
      code: 'timeout',
      timeoutMs: 50,
    });
  });

  it('reports POSIX EPERM as a redacted process-group termination failure', async () => {
    const fake = fakeRuntime('linux');
    fake.spawn.mockReturnValue(new FakeProcess(42));
    fake.kill.mockImplementation(() => {
      throw systemError('EPERM');
    });
    const promise = createDefaultExec(1024, fake.runtime)('agent-cli', [], {
      timeoutMs: 50,
    });

    fake.fireTimeout();

    await expect(promise).rejects.toEqual(
      expect.objectContaining({
        code: 'termination-failed',
        timeoutMs: 50,
        systemCode: 'EPERM',
        terminationMethod: 'process-group',
      }),
    );
  });

  it('reports an asynchronous taskkill spawn error without exposing its message', async () => {
    const fake = fakeRuntime('win32');
    const child = new FakeProcess(42);
    const taskkill = new FakeProcess();
    fake.spawn.mockReturnValueOnce(child).mockReturnValueOnce(taskkill);
    const promise = createDefaultExec(1024, fake.runtime)('agent-cli', [], {
      timeoutMs: 50,
    });

    fake.fireTimeout();
    taskkill.emitError(systemError('ENOENT'));

    const failure = await promise.catch((error: unknown) => error);
    expect(failure).toEqual(
      expect.objectContaining({
        code: 'termination-failed',
        timeoutMs: 50,
        systemCode: 'ENOENT',
        terminationMethod: 'taskkill',
      }),
    );
    expect(JSON.stringify(failure)).not.toContain('private operating-system');
  });

  it('reports a synchronous taskkill spawn failure with sanitized metadata', async () => {
    const fake = fakeRuntime('win32');
    const child = new FakeProcess(42);
    fake.spawn.mockReturnValueOnce(child).mockImplementationOnce(() => {
      throw systemError('EACCES');
    });
    const promise = createDefaultExec(1024, fake.runtime)('agent-cli', [], {
      timeoutMs: 50,
    });

    fake.fireTimeout();

    await expect(promise).rejects.toMatchObject({
      code: 'termination-failed',
      timeoutMs: 50,
      systemCode: 'EACCES',
      terminationMethod: 'taskkill',
    });
  });

  it('reports a nonzero taskkill exit with safe numeric metadata', async () => {
    const fake = fakeRuntime('win32');
    const child = new FakeProcess(42);
    const taskkill = new FakeProcess();
    fake.spawn.mockReturnValueOnce(child).mockReturnValueOnce(taskkill);
    const promise = createDefaultExec(1024, fake.runtime)('agent-cli', [], {
      timeoutMs: 50,
    });

    fake.fireTimeout();
    taskkill.emitClose(5);

    await expect(promise).rejects.toMatchObject({
      code: 'termination-failed',
      timeoutMs: 50,
      exitCode: 5,
      terminationMethod: 'taskkill',
    });
  });

  it('contains synchronous and asynchronous CLI spawn errors without terminating', async () => {
    const synchronous = fakeRuntime('linux');
    synchronous.spawn.mockImplementation(() => {
      throw systemError('ENOENT');
    });
    const syncPromise = createDefaultExec(1024, synchronous.runtime)(
      'agent-cli',
      [],
      { timeoutMs: 50 },
    );
    await expect(syncPromise).rejects.toMatchObject({
      code: 'spawn-failed',
      systemCode: 'ENOENT',
    });
    expect(synchronous.kill).not.toHaveBeenCalled();

    const asynchronous = fakeRuntime('linux');
    const child = new FakeProcess();
    asynchronous.spawn.mockReturnValue(child);
    const asyncPromise = createDefaultExec(1024, asynchronous.runtime)(
      'agent-cli',
      [],
      { timeoutMs: 50 },
    );
    child.emitError(systemError('EACCES'));
    await expect(asyncPromise).rejects.toMatchObject({
      code: 'spawn-failed',
      systemCode: 'EACCES',
    });
    expect(asynchronous.clearTimeout).toHaveBeenCalledWith('timer-handle');
    expect(asynchronous.kill).not.toHaveBeenCalled();
  });

  it('fails closed when a timed-out child has no process id', async () => {
    const fake = fakeRuntime('linux');
    fake.spawn.mockReturnValue(new FakeProcess());
    const promise = createDefaultExec(1024, fake.runtime)('agent-cli', [], {
      timeoutMs: 50,
    });

    fake.fireTimeout();

    await expect(promise).rejects.toMatchObject({
      code: 'termination-failed',
      timeoutMs: 50,
      systemCode: 'PROCESS_ID_UNAVAILABLE',
      terminationMethod: 'process-group',
    });
  });
});
