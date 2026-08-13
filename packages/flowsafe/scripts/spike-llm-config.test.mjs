// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import { parseLlmSpikeConfig } from './spike-llm-config.mjs';

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
