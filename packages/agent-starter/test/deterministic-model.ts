// SPDX-License-Identifier: Apache-2.0

import type { MastraModelConfig } from '@mastra/core/llm';

const usage = {
  inputTokens: 1,
  outputTokens: 3,
  totalTokens: 4,
};

export const deterministicModel = {
  specificationVersion: 'v2',
  provider: 'anchorage-starter-test',
  modelId: 'deterministic',
  supportedUrls: {},
  doGenerate: async () => ({
    content: [
      {
        type: 'text',
        text: 'deterministic starter response',
      },
    ],
    finishReason: 'stop',
    usage,
    warnings: [],
  }),
  doStream: async () => ({
    stream: new ReadableStream({
      start(controller) {
        controller.enqueue({
          type: 'stream-start',
          warnings: [],
        });
        controller.enqueue({
          type: 'text-start',
          id: 'answer',
        });
        controller.enqueue({
          type: 'text-delta',
          id: 'answer',
          delta: 'deterministic starter response',
        });
        controller.enqueue({
          type: 'text-end',
          id: 'answer',
        });
        controller.enqueue({
          type: 'finish',
          finishReason: 'stop',
          usage,
        });
        controller.close();
      },
    }),
  }),
} satisfies MastraModelConfig;
