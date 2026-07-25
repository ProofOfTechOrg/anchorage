import { describe, expect, it, vi } from 'vitest';

import {
  createSpikeServerLifecycle,
  parseLlmSpikeConfig,
  parseSpikePort,
} from './spike-server-lifecycle.mjs';

function operations(overrides = {}) {
  let now = 0;
  return {
    now: () => now,
    sleep: async (ms) => {
      now += ms;
    },
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

describe('spike server lifecycle', () => {
  it('rejects invalid ports', () => {
    for (const value of [0, 65_536, 1.5, Number.NaN, 'abc']) {
      expect(() => parseSpikePort(value)).toThrow(RangeError);
    }
    expect(parseSpikePort('8799')).toBe(8799);
  });

  it('refuses an occupied port before spawning', async () => {
    const launch = vi.fn(() => server());
    const lifecycle = createSpikeServerLifecycle({
      port: 8799,
      operations: operations({ portState: async () => 'listening' }),
    });
    await expect(lifecycle.preflight()).rejects.toThrow('already in use');
    expect(launch).not.toHaveBeenCalled();
  });

  it('fails when the child exits before readiness', async () => {
    const lifecycle = createSpikeServerLifecycle({
      port: 8799,
      operations: operations({ probeHttp: async () => {} }),
    });
    await expect(
      lifecycle.start('dead', () => server({ exitCode: 1 })),
    ).rejects.toThrow('exited before ready');
  });

  it('snapshots descendants before kill and clears only after refusal', async () => {
    const calls = [];
    const lifecycle = createSpikeServerLifecycle({
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
    const lifecycle = createSpikeServerLifecycle({
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
    const lifecycle = createSpikeServerLifecycle({
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

describe('LLM spike configuration', () => {
  it('accepts provider/model, a nonblank key, and HTTP(S) base URLs', () => {
    expect(
      parseLlmSpikeConfig({
        SPIKE_LLM_MODEL_ID: 'openai/gpt-5',
        SPIKE_LLM_API_KEY: 'secret',
        SPIKE_LLM_BASE_URL: 'https://example.com/v1',
      }),
    ).toEqual({
      modelId: 'openai/gpt-5',
      apiKey: 'secret',
      baseUrl: 'https://example.com/v1',
    });
  });

  it('rejects malformed models, blank keys, and non-HTTP base URLs', () => {
    expect(() =>
      parseLlmSpikeConfig({
        SPIKE_LLM_MODEL_ID: 'missing-provider',
        SPIKE_LLM_API_KEY: 'secret',
      }),
    ).toThrow('provider/model');
    expect(() =>
      parseLlmSpikeConfig({
        SPIKE_LLM_MODEL_ID: 'openai/gpt-5',
        SPIKE_LLM_API_KEY: '   ',
      }),
    ).toThrow('non-empty');
    expect(() =>
      parseLlmSpikeConfig({
        SPIKE_LLM_MODEL_ID: 'openai/gpt-5',
        SPIKE_LLM_API_KEY: 'secret',
        SPIKE_LLM_BASE_URL: 'file:///tmp/model',
      }),
    ).toThrow('HTTP(S)');
  });
});
