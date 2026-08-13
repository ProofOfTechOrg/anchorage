import { describe, expect, it, vi } from 'vitest';

import {
  createWorkerdServerLifecycle,
  parsePort,
  WorkerdNetworkConnectionLostError,
  workerdNetworkConnectionLostResponseError,
} from './workerd-server-lifecycle.mjs';

function operations(overrides = {}) {
  let now = 0;
  return {
    now: () => now,
    sleep: vi.fn(async (ms) => {
      now += ms;
    }),
    portState: async () => 'refused',
    descendantsOf: () => [],
    alive: () => false,
    killGroup: vi.fn(),
    killPort: vi.fn(),
    probeHttp: async () => {},
    ...overrides,
  };
}

function server(overrides = {}) {
  return {
    generation: 'test',
    child: {
      pid: 42,
      exitCode: null,
      signalCode: null,
      ...overrides,
    },
  };
}

const networkConnectionLostBody =
  'Error: Network connection lost.\n' +
  '    at async Object.fetch (file:///repo/node_modules/miniflare/dist/src/workers/core/entry.worker.js:4719:22)';

describe('workerd server lifecycle', () => {
  it('rejects invalid ports', () => {
    for (const value of [0, 65_536, 1.5, Number.NaN, 'abc']) {
      expect(() => parsePort(value)).toThrow(RangeError);
    }
    expect(parsePort('8799')).toBe(8799);
  });

  it('refuses an occupied port before spawning', async () => {
    const launch = vi.fn(() => server());
    const lifecycle = createWorkerdServerLifecycle({
      port: 8799,
      operations: operations({ portState: async () => 'listening' }),
    });
    await expect(lifecycle.preflight()).rejects.toThrow('already in use');
    expect(launch).not.toHaveBeenCalled();
  });

  it('fails when the child exits before readiness', async () => {
    const lifecycle = createWorkerdServerLifecycle({
      port: 8799,
      operations: operations({ probeHttp: async () => {} }),
    });
    await expect(
      lifecycle.start('dead', () => server({ exitCode: 1 })),
    ).rejects.toThrow('exited before ready');
  });

  it('classifies only the exact Miniflare core network-loss response', () => {
    expect(
      workerdNetworkConnectionLostResponseError(
        500,
        networkConnectionLostBody,
        'GET /state',
      ),
    ).toBeInstanceOf(WorkerdNetworkConnectionLostError);
    for (const [status, candidate] of [
      [503, networkConnectionLostBody],
      [500, '{"error":"Network connection lost"}'],
      [500, 'Error: Network connection lost.\n    at application.js:1:1'],
      [
        500,
        'Error: Network connection lost.\n' +
          '    at application.js:1:1\n' +
          '    at async Object.fetch (file:///repo/node_modules/miniflare/dist/src/workers/core/entry.worker.js:4719:22)',
      ],
      [500, 'Error: another failure.\n    at entry.worker.js:4719:22'],
    ]) {
      expect(
        workerdNetworkConnectionLostResponseError(
          status,
          candidate,
          'GET /state',
        ),
      ).toBeUndefined();
    }
  });

  it('retries transient network loss while the child stays alive', async () => {
    const ops = operations();
    const lifecycle = createWorkerdServerLifecycle({
      port: 8799,
      operations: ops,
    });
    const active = server();
    active.generation = 'recovering';
    await lifecycle.start('recovering', () => active);
    const operation = vi
      .fn()
      .mockRejectedValueOnce(new WorkerdNetworkConnectionLostError('transient'))
      .mockResolvedValue({ status: 200 });

    await expect(
      lifecycle.retryNetworkConnectionLost(operation),
    ).resolves.toEqual({ status: 200 });
    expect(operation).toHaveBeenCalledTimes(2);
    expect(ops.sleep).toHaveBeenCalledExactlyOnceWith(250);
  });

  it('bounds transient network recovery and retains the last failure', async () => {
    const ops = operations();
    const active = server();
    active.generation = 'recovering';
    const lifecycle = createWorkerdServerLifecycle({
      port: 8799,
      operations: ops,
    });
    await lifecycle.start('recovering', () => active);
    const transient = new WorkerdNetworkConnectionLostError('transient');
    const operation = vi.fn().mockRejectedValue(transient);

    let failure;
    try {
      await lifecycle.retryNetworkConnectionLost(operation, 500);
    } catch (error) {
      failure = error;
    }
    expect(failure).toMatchObject({
      message: 'recovering network connection did not recover within 500ms',
      cause: transient,
    });
    expect(operation).toHaveBeenCalledTimes(2);
    expect(ops.sleep).toHaveBeenCalledTimes(2);
  });

  it('rejects a successful operation that completes after the deadline', async () => {
    const ops = operations();
    const lifecycle = createWorkerdServerLifecycle({
      port: 8799,
      operations: ops,
    });
    await lifecycle.start('recovering', () => server());
    const operation = vi.fn(async () => {
      await ops.sleep(501);
      return { status: 200 };
    });

    await expect(
      lifecycle.retryNetworkConnectionLost(operation, 500),
    ).rejects.toThrow('test network connection did not recover within 500ms');
    expect(operation).toHaveBeenCalledOnce();
  });

  it('aborts an operation that remains pending at the deadline', async () => {
    let operationSignal;
    const lifecycle = createWorkerdServerLifecycle({
      port: 8799,
      operations: operations({
        setTimer: (callback) => {
          callback();
          return 1;
        },
        clearTimer: vi.fn(),
      }),
    });
    await lifecycle.start('recovering', () => server());
    const operation = vi.fn(({ signal }) => {
      operationSignal = signal;
      return new Promise(() => {});
    });

    await expect(
      lifecycle.retryNetworkConnectionLost(operation, 500),
    ).rejects.toThrow('test network connection did not recover within 500ms');
    expect(operation).toHaveBeenCalledOnce();
    expect(operationSignal.aborted).toBe(true);
  });

  it('reports the recovery deadline when an aborted operation rejects', async () => {
    let timerCount = 0;
    const transient = new WorkerdNetworkConnectionLostError('transient');
    const lifecycle = createWorkerdServerLifecycle({
      port: 8799,
      operations: operations({
        setTimer: (callback) => {
          timerCount += 1;
          if (timerCount === 2) callback();
          return timerCount;
        },
        clearTimer: vi.fn(),
      }),
    });
    await lifecycle.start('recovering', () => server());
    const operation = vi.fn().mockRejectedValueOnce(transient);
    operation.mockImplementationOnce(({ signal }) => {
      if (signal.aborted) return Promise.reject(new DOMException('aborted'));
      return new Promise((_, reject) => {
        signal.addEventListener(
          'abort',
          () => reject(new DOMException('aborted')),
          { once: true },
        );
      });
    });

    let failure;
    try {
      await lifecycle.retryNetworkConnectionLost(operation, 500);
    } catch (error) {
      failure = error;
    }
    expect(failure).toMatchObject({
      message: 'test network connection did not recover within 500ms',
      cause: transient,
    });
    expect(operation).toHaveBeenCalledTimes(2);
  });

  it('retries an exact transient response for a replay-safe JSON request', async () => {
    const ops = operations();
    const lifecycle = createWorkerdServerLifecycle({
      port: 8799,
      operations: ops,
    });
    await lifecycle.start('recovering', () => server());
    const request = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(networkConnectionLostBody, { status: 500 }),
      )
      .mockResolvedValue(new Response('{"ready":true}', { status: 200 }));

    await expect(
      lifecycle.requestJson(request, {
        requestLabel: 'GET /state',
        replaySafe: true,
      }),
    ).resolves.toEqual({ status: 200, body: { ready: true } });
    expect(request).toHaveBeenCalledTimes(2);
    expect(ops.sleep).toHaveBeenCalledExactlyOnceWith(250);
  });

  it('returns a non-transient JSON response without retrying', async () => {
    const ops = operations();
    const lifecycle = createWorkerdServerLifecycle({
      port: 8799,
      operations: ops,
    });
    await lifecycle.start('recovering', () => server());
    const request = vi
      .fn()
      .mockResolvedValue(
        new Response('{"error":"application failure"}', { status: 500 }),
      );

    await expect(
      lifecycle.requestJson(request, {
        requestLabel: 'GET /state',
        replaySafe: true,
      }),
    ).resolves.toEqual({
      status: 500,
      body: { error: 'application failure' },
    });
    expect(request).toHaveBeenCalledOnce();
    expect(ops.sleep).not.toHaveBeenCalled();
  });

  it('propagates a malformed success response without retrying', async () => {
    const ops = operations();
    const lifecycle = createWorkerdServerLifecycle({
      port: 8799,
      operations: ops,
    });
    await lifecycle.start('recovering', () => server());
    const request = vi
      .fn()
      .mockResolvedValue(new Response('not JSON', { status: 200 }));

    await expect(
      lifecycle.requestJson(request, {
        requestLabel: 'GET /state',
        replaySafe: true,
      }),
    ).rejects.toThrow('GET /state -> 200 non-JSON: not JSON');
    expect(request).toHaveBeenCalledOnce();
    expect(ops.sleep).not.toHaveBeenCalled();
  });

  it('does not retry an exact transient response for a mutation', async () => {
    const lifecycle = createWorkerdServerLifecycle({
      port: 8799,
      operations: operations(),
    });
    await lifecycle.start('recovering', () => server());
    const request = vi
      .fn()
      .mockResolvedValue(
        new Response(networkConnectionLostBody, { status: 500 }),
      );

    await expect(
      lifecycle.requestJson(request, {
        requestLabel: 'POST /mutate',
        replaySafe: false,
      }),
    ).rejects.toBeInstanceOf(WorkerdNetworkConnectionLostError);
    expect(request).toHaveBeenCalledOnce();
  });

  it('propagates an assertion failure without retrying', async () => {
    const ops = operations();
    const lifecycle = createWorkerdServerLifecycle({
      port: 8799,
      operations: ops,
    });
    await lifecycle.start('recovering', () => server());
    const failure = new Error('expected suspended status');
    const operation = vi.fn().mockRejectedValue(failure);

    await expect(lifecycle.retryNetworkConnectionLost(operation)).rejects.toBe(
      failure,
    );
    expect(operation).toHaveBeenCalledOnce();
    expect(ops.sleep).not.toHaveBeenCalled();
  });

  it('fails immediately when the child exits during network recovery', async () => {
    const ops = operations();
    const active = server();
    active.generation = 'recovering';
    const lifecycle = createWorkerdServerLifecycle({
      port: 8799,
      operations: ops,
    });
    await lifecycle.start('recovering', () => active);
    const operation = vi.fn(async () => {
      active.child.exitCode = 1;
      throw new WorkerdNetworkConnectionLostError('transient');
    });

    await expect(
      lifecycle.retryNetworkConnectionLost(operation),
    ).rejects.toThrow(
      'recovering exited while recovering network connection (code 1, signal null)',
    );
    expect(operation).toHaveBeenCalledOnce();
    expect(ops.sleep).not.toHaveBeenCalled();
  });

  it('snapshots descendants before kill and clears only after refusal', async () => {
    const calls = [];
    const lifecycle = createWorkerdServerLifecycle({
      port: 8799,
      operations: operations({
        descendantsOf: (pid) => {
          calls.push(`descendants:${pid}`);
          return [43];
        },
        killGroup: (pid) => calls.push(`kill:${pid}`),
      }),
    });
    const active = await lifecycle.start('ok', () => server());
    await lifecycle.stop();
    expect(calls).toEqual(['descendants:42', 'kill:42']);
    expect(lifecycle.activeServer).toBeUndefined();
    expect(active.child.pid).toBe(42);
  });

  it('fails a surviving listener and retains the active server', async () => {
    const killPort = vi.fn();
    const lifecycle = createWorkerdServerLifecycle({
      port: 8799,
      operations: operations({
        portState: async () => 'listening',
        killPort,
      }),
    });
    await lifecycle.start('survivor', () => server());
    await expect(lifecycle.stop()).rejects.toThrow('still listening');
    expect(killPort).toHaveBeenCalledOnce();
    expect(lifecycle.activeServer).toBeDefined();
  });

  it('preserves shutdown failure over cleanup failure', async () => {
    const lifecycle = createWorkerdServerLifecycle({
      port: 8799,
      operations: operations({
        portState: async () => 'listening',
      }),
    });
    await lifecycle.start('survivor', () => server());
    await expect(
      lifecycle.cleanup(() => {
        throw new Error('cleanup failed');
      }),
    ).rejects.toThrow('still listening');
  });
});
