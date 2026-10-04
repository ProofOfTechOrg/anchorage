// SPDX-License-Identifier: Apache-2.0

import {
  type AIV5Type,
  convertMessages,
  type MastraDBMessage,
  type MastraToolInvocation,
  type MastraToolInvocationPart,
} from '@mastra/core/agent/message-list';

/** @internal */
export const UNCLASSIFIED_INPUT_CONTENT =
  'input message content is not classified';

/** @internal */
export function convertedPrompt(
  messages: readonly MastraDBMessage[],
): AIV5Type.ModelMessage[] {
  // The conversion merges into and rewrites the message objects it is given.
  return convertMessages(structuredClone([...messages])).to('AIV5.Model');
}

/** @internal */
export type PromptMedia =
  | { kind: 'inline-bytes'; bytes: Uint8Array; mediaType: string | undefined }
  | { kind: 'inline-base64'; payload: string; mediaType: string | undefined }
  | { kind: 'network-url'; url: URL; mediaType: string | undefined };

// Mirrors @mastra/core/dist/content-DX_6irdy.js:3-15 and :33-60 so the
// policy reads the same data URL fields as Mastra's prompt conversion.
function splitDataUrl(dataUrl: string): {
  mediaType: string | undefined;
  base64Content: string | undefined;
} {
  try {
    const [header, base64Content] = dataUrl.split(',');
    return {
      mediaType: header?.split(';')[0]?.split(':')[1],
      base64Content,
    };
  } catch {
    return { mediaType: undefined, base64Content: undefined };
  }
}

/** @internal */
export function classifyPromptMedia(
  data: unknown,
  declaredMediaType?: unknown,
): PromptMedia | undefined {
  const mediaType =
    typeof declaredMediaType === 'string' ? declaredMediaType : undefined;
  if (data instanceof Uint8Array) {
    return { kind: 'inline-bytes', bytes: data, mediaType };
  }
  if (data instanceof ArrayBuffer) {
    return { kind: 'inline-bytes', bytes: new Uint8Array(data), mediaType };
  }
  if (typeof data === 'string') {
    const value = data;
    try {
      data = new URL(value);
    } catch {
      return { kind: 'inline-base64', payload: value, mediaType };
    }
  }
  if (data instanceof URL) {
    if (data.protocol === 'data:') {
      const { mediaType: dataUrlMediaType, base64Content } = splitDataUrl(
        data.toString(),
      );
      if (dataUrlMediaType == null || base64Content == null) return undefined;
      return {
        kind: 'inline-base64',
        payload: base64Content,
        mediaType: dataUrlMediaType,
      };
    }
    return { kind: 'network-url', url: data, mediaType };
  }
  return undefined;
}

function pushNetworkUrl(urls: URL[], data: unknown): void {
  const media = classifyPromptMedia(data);
  if (media?.kind === 'network-url') {
    urls.push(media.url);
  } else if (media?.kind === 'inline-base64' && media.payload.includes('://')) {
    // Mastra can wrap a non-HTTP URL in a data URI without encoding it.
    try {
      urls.push(new URL(media.payload));
    } catch {
      // A non-URL payload remains inline data.
    }
  }
}

/** @internal */
export function inputAssetUrls(messages: readonly MastraDBMessage[]): URL[] {
  const converted = convertedPrompt(messages);
  const urls: URL[] = [];
  for (const message of converted) {
    if (message.role !== 'user' || !Array.isArray(message.content)) continue;
    for (const part of message.content) {
      if (part.type !== 'file' && part.type !== 'image') continue;
      pushNetworkUrl(urls, part.type === 'file' ? part.data : part.image);
    }
  }
  return urls;
}

/** @internal */
export interface StoredModelOutput {
  readonly toolCallId: string;
  readonly state: MastraToolInvocation['state'];
  readonly output: unknown;
}

function storedPartModelOutputs(
  parts: readonly MastraToolInvocationPart[],
): readonly StoredModelOutput[] {
  const stored: StoredModelOutput[] = [];
  for (const part of parts) {
    const mastra: unknown = part.providerMetadata?.mastra;
    if (typeof mastra !== 'object' || mastra === null) continue;
    const output = (mastra as { modelOutput?: unknown }).modelOutput;
    if (output !== undefined && output !== null) {
      stored.push({
        toolCallId: part.toolInvocation.toolCallId,
        state: part.toolInvocation.state,
        output,
      });
    }
  }
  return stored;
}

/** @internal */
export function toolInvocationParts(
  messages: readonly MastraDBMessage[],
): MastraToolInvocationPart[] {
  return messages.flatMap((message) =>
    message.content.parts.filter((part) => part.type === 'tool-invocation'),
  );
}

/** @internal */
export function storedModelOutputs(
  messages: readonly MastraDBMessage[],
): readonly StoredModelOutput[] {
  return storedPartModelOutputs(toolInvocationParts(messages));
}

/** @internal */
export function toolOutputAssetUrls(
  parts: readonly MastraToolInvocationPart[],
): URL[] {
  const urls: URL[] = [];
  for (const { output } of storedPartModelOutputs(parts)) {
    if (typeof output !== 'object' || output === null) continue;
    const { type, value } = output as { type?: unknown; value?: unknown };
    if (type !== 'content' || !Array.isArray(value)) continue;
    for (const item of value as unknown[]) {
      if (typeof item !== 'object' || item === null) continue;
      const fields = item as { type?: unknown; url?: unknown; data?: unknown };
      if (fields.type === 'image-url' || fields.type === 'file-url') {
        pushNetworkUrl(urls, fields.url);
      } else if (
        fields.type === 'media' ||
        fields.type === 'image-data' ||
        fields.type === 'file-data'
      ) {
        pushNetworkUrl(urls, fields.data);
      }
    }
  }
  return urls;
}
