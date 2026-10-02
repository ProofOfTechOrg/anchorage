// SPDX-License-Identifier: Apache-2.0

import type {
  MastraDBMessage,
  MastraToolInvocationPart,
  MessageList,
} from '@mastra/core/agent/message-list';
import type { InputProcessor, ProcessInputArgs } from '@mastra/core/processors';
import type { RequestContext } from '@mastra/core/request-context';
import type { CoreTool } from '@mastra/core/tools';

import {
  callerMessages,
  recordedMessageIds,
  recordProcessorAdditions,
  snapshotPromptMessages,
} from '../processor-additions.js';

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

// Mirrors core's applyClientToolModelOutput so policies read its mapped output
// and core skips the result parts this step marks as computed.
function eligibleToolResults(
  messages: readonly MastraDBMessage[],
  seen: Set<MastraToolInvocationPart>,
): MastraToolInvocationPart[] {
  const candidates: MastraToolInvocationPart[] = [];
  for (const message of messages) {
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
      if (seen.has(part)) continue;
      seen.add(part);
      candidates.push(part);
    }
  }
  return candidates;
}

function selectClientToolResults(messageList: MessageList): {
  changed: MastraToolInvocationPart[];
  caller: MastraToolInvocationPart[];
  changedMessageIds: ReadonlySet<string>;
} {
  const input = messageList.get.input.db();
  const recorded = recordedMessageIds(messageList);
  const seen = new Set<MastraToolInvocationPart>();
  const changedMessages = input.filter(({ id }) => recorded.has(id));
  const changed = eligibleToolResults(changedMessages, seen);
  const caller = eligibleToolResults(callerMessages(messageList, input), seen);
  return {
    changed,
    caller,
    changedMessageIds: new Set(changedMessages.map(({ id }) => id)),
  };
}

/**
 * @internal Map client tool results before guarded input policies read caller
 * input and recorded prompt additions.
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
        const { changed, caller, changedMessageIds } = selectClientToolResults(
          args.messageList,
        );
        if (changed.length === 0 && caller.length === 0)
          return args.messageList;
        const memoryInfo = args.messageList.serialize().memoryInfo;
        const tools = await resolveTools({
          requestContext: args.requestContext,
          threadId: memoryInfo?.threadId,
          resourceId: memoryInfo?.resourceId,
        });
        const mapPart = async (part: MastraToolInvocationPart) => {
          const tool = tools[part.toolInvocation.toolName];
          if (
            !tool ||
            tool.execute ||
            tool.type === 'provider-defined' ||
            typeof tool.toModelOutput !== 'function'
          )
            return;
          const unwrapped = unwrapToolOutput(part.toolInvocation.result);
          if (unwrapped.skip) return;
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
        };
        // A shared result part also changes messages the processor leaves untouched;
        // the id set prevents recording those whole messages. Caller parts are read live.
        if (changed.length > 0) {
          const snapshot = snapshotPromptMessages(args.messageList);
          for (const part of changed) await mapPart(part);
          recordProcessorAdditions(
            args.messageList,
            snapshot,
            changedMessageIds,
          );
        }
        for (const part of caller) await mapPart(part);
      } catch {
        return fail(args);
      }
      return args.messageList;
    },
  };
}
