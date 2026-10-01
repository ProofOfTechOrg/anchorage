// SPDX-License-Identifier: Apache-2.0

import type { MastraToolInvocationPart } from '@mastra/core/agent/message-list';
import type { InputProcessor, ProcessInputArgs } from '@mastra/core/processors';
import type { RequestContext } from '@mastra/core/request-context';
import type { CoreTool } from '@mastra/core/tools';

import { callerMessages } from '../processor-additions.js';

export const CLIENT_TOOL_OUTPUT_PROCESSOR_ID = 'breakwater-client-tool-output';

function unwrapToolOutput(
  value: unknown,
): { skip: true } | { skip: false; output: unknown } {
  const isV5Wrapper =
    typeof value === 'object' &&
    value !== null &&
    'type' in value &&
    'value' in value &&
    Object.keys(value).length === 2;
  if (
    isV5Wrapper &&
    (value.type === 'error-text' || value.type === 'error-json')
  ) {
    return { skip: true };
  }
  return {
    skip: false,
    output: isV5Wrapper ? value.value : value,
  };
}

function normalizeModelOutput(output: unknown): unknown {
  if (output == null || typeof output !== 'object') return output;
  const obj = output as Record<string, unknown>;
  if (obj.type !== 'content' || !Array.isArray(obj.value)) return output;
  return {
    ...obj,
    value: obj.value.map((item: unknown) => {
      if (item == null || typeof item !== 'object') return item;
      const part = item as Record<string, unknown>;
      if (part.type === 'image-url' && typeof part.url === 'string') {
        const mediaType =
          typeof part.mediaType === 'string' && part.mediaType
            ? part.mediaType
            : part.url.startsWith('data:')
              ? part.url.slice(5, part.url.indexOf(';')) || 'image/jpeg'
              : 'image/jpeg';
        return { type: 'media', data: part.url, mediaType };
      }
      if (part.type === 'image-data' && typeof part.data === 'string') {
        return {
          type: 'media',
          data: part.data,
          mediaType: part.mediaType ?? 'image/jpeg',
        };
      }
      if (part.type === 'file-data' && typeof part.data === 'string') {
        return {
          type: 'media',
          data: part.data,
          mediaType: part.mediaType ?? 'application/octet-stream',
        };
      }
      return part;
    }),
  };
}

/**
 * @internal Map caller-supplied tool results before guarded input policies read them.
 */
export function clientToolOutputProcessor(
  resolveTools: (options: {
    requestContext?: RequestContext;
    threadId?: string;
    resourceId?: string;
  }) => Promise<Record<string, CoreTool>>,
  fail: (args: ProcessInputArgs) => never,
): InputProcessor {
  return {
    id: CLIENT_TOOL_OUTPUT_PROCESSOR_ID,
    async processInput(args) {
      try {
        // Mirrors core's applyClientToolModelOutput for caller parts so policies
        // read mapped outputs before core renders them into the model prompt.
        const candidates: MastraToolInvocationPart[] = [];
        for (const message of callerMessages(
          args.messageList,
          args.messageList.get.input.db(),
        )) {
          if (
            message.role !== 'assistant' ||
            message.content?.format !== 2 ||
            !message.content.parts
          ) {
            continue;
          }
          for (const part of message.content.parts) {
            if (
              part.type !== 'tool-invocation' ||
              part.toolInvocation?.state !== 'result'
            ) {
              continue;
            }
            const mastra = part.providerMetadata?.mastra;
            if (
              mastra &&
              typeof mastra === 'object' &&
              ('modelOutput' in mastra || mastra.modelOutputComputed)
            ) {
              continue;
            }
            candidates.push(part);
          }
        }
        if (candidates.length === 0) return args.messageList;
        const memoryInfo = args.messageList.serialize().memoryInfo;
        const tools = await resolveTools({
          requestContext: args.requestContext,
          threadId: memoryInfo?.threadId,
          resourceId: memoryInfo?.resourceId,
        });
        for (const part of candidates) {
          const tool = tools[part.toolInvocation.toolName];
          if (
            !tool ||
            tool.execute ||
            tool.type === 'provider-defined' ||
            typeof tool.toModelOutput !== 'function'
          )
            continue;
          const unwrapped = unwrapToolOutput(part.toolInvocation.result);
          if (unwrapped.skip) continue;
          const modelOutput = normalizeModelOutput(
            await tool.toModelOutput(unwrapped.output),
          );
          const providerModelOutput = modelOutput as unknown as NonNullable<
            MastraToolInvocationPart['providerMetadata']
          >[string][string];
          const mastra = part.providerMetadata?.mastra;
          part.providerMetadata = {
            ...part.providerMetadata,
            mastra: {
              ...(mastra && typeof mastra === 'object' ? mastra : {}),
              modelOutputComputed: true,
              ...(modelOutput != null
                ? { modelOutput: providerModelOutput }
                : {}),
            },
          };
        }
      } catch {
        return fail(args);
      }
      return args.messageList;
    },
  };
}
