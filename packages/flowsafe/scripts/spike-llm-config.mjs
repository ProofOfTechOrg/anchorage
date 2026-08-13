// SPDX-License-Identifier: Apache-2.0
// Credentialed-spike model configuration. Kept beside the spike rather than in
// the shared lifecycle at scripts/workerd-server-lifecycle.mjs because these
// environment variables are this spike's, not every harness's.

export function parseLlmSpikeConfig(env) {
  const modelId =
    env.SPIKE_LLM_MODEL_ID ??
    (env.DEEPSEEK_MODEL ? `deepseek/${env.DEEPSEEK_MODEL}` : undefined);
  if (typeof modelId !== 'string' || !/^[^\s/]+\/[^\s]+$/.test(modelId)) {
    throw new Error('SPIKE_LLM_MODEL_ID must be a non-empty provider/model');
  }
  const apiKey = env.SPIKE_LLM_API_KEY ?? env.DEEPSEEK_API_KEY;
  if (typeof apiKey !== 'string' || apiKey.trim().length === 0) {
    throw new Error('SPIKE_LLM_API_KEY must be non-empty');
  }
  let baseUrl;
  if (env.SPIKE_LLM_BASE_URL !== undefined) {
    try {
      baseUrl = new URL(env.SPIKE_LLM_BASE_URL);
    } catch {
      throw new Error('SPIKE_LLM_BASE_URL must be a valid HTTP(S) URL');
    }
    if (baseUrl.protocol !== 'http:' && baseUrl.protocol !== 'https:') {
      throw new Error('SPIKE_LLM_BASE_URL must be a valid HTTP(S) URL');
    }
  }
  return {
    modelId,
    apiKey,
    ...(baseUrl ? { baseUrl: baseUrl.toString() } : {}),
  };
}
