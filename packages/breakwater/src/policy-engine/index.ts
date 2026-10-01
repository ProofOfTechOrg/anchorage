// SPDX-License-Identifier: Apache-2.0
// Policy Engine — pre-gate (input) and post-gate (output) policy evaluation
// as a single Mastra processor registered in both inputProcessors and
// outputProcessors. Policies are evaluator functions returning
// { allowed } | { allowed: false, reason }.
//
// Output is gated per chunk and at the final result so agent.stream() cannot
// leak forbidden text: processOutputStream gates each streamed chunk against
// the output accumulated so far. processOutputResult is the final gate on
// Mastra's standard loop; the durable loop logs its refusal without stopping
// the returned result or saved thread message.
// Structured objects that Mastra returns outside the processor chain are not
// covered; the guarded agent therefore rejects structured output.
//
// Output is gated per CHANNEL — 'answer' (client-visible text), 'reasoning'
// (the model's reasoning trace), 'object' (structured-output snapshots) —
// each accumulated and evaluated independently; a policy declares which
// channels it gates (default: answer only). Tool-boundary policies (network
// egress, write-permission, cross-workflow isolation) live in tool-policy.ts,
// enforced by the connector SDK's execute wrapper; data retention is a
// storage-layer property, shipped as flowsafe's purgeExpiredWorkflowRuns —
// see docs/policy-engine-design.md.

import type {
  AIV5Type,
  AIV6Type,
  MastraDBMessage,
  MastraMessagePart,
  MastraToolInvocation,
} from '@mastra/core/agent/message-list';
import type {
  ProcessInputArgs,
  ProcessInputResult,
  ProcessOutputResultArgs,
  ProcessOutputStreamArgs,
  Processor,
} from '@mastra/core/processors';
import { RequestContext } from '@mastra/core/request-context';
import type { ChunkType } from '@mastra/core/stream';
import { type JSONType, z } from 'zod';

import {
  type AuditLogger,
  agentAuditDetail,
  malformedAgentAuditContextEvent,
} from '../audit/index.js';
import {
  assertKnownFields,
  describeEntry,
  readFrozenList,
  readNumberInRange,
  unknownFieldOf,
} from '../host-input.js';
import { stopWithoutCallMessages } from '../input-refusal.js';
import {
  additionsToRead,
  callerMessages,
  type ProcessorAddition,
  type RecordedSystemMessage,
  takeProcessorAdditions,
} from '../processor-additions.js';
import { type Actor, actorFromRequestContext } from '../rbac/index.js';
import {
  assertPolicyText,
  copyRegExpEntry,
  readHoldBackChars,
  terminalPassStreamStates,
} from './content-inspection.js';
import type {
  OutputChannel,
  PolicyContext,
  PolicyEvaluator,
  PolicyPhase,
} from './evaluator-contract.js';
import {
  classifyPromptMedia,
  convertedPrompt,
  UNCLASSIFIED_INPUT_CONTENT,
} from './prompt-media.js';
import { isPlainRecord, providerOptionValues } from './provider-options.js';
import type { PolicyDecision } from './tool-policy.js';

export type {
  OutputChannel,
  PolicyContext,
  PolicyEvaluator,
  PolicyPhase,
} from './evaluator-contract.js';

const DEFAULT_CHANNELS: readonly OutputChannel[] = ['answer'];

// A table below that `satisfies` a type built on a union Mastra declares fails
// to compile when a Mastra release adds a member, until the member is
// classified; at runtime, a value outside such a table stops input evaluation.

// Mastra keeps a system-role message out of the input list, and the prompt
// conversion below drops one, so its text would reach no policy.
const INPUT_MESSAGE_ROLES = {
  user: true,
  assistant: true,
  signal: true,
  system: false,
} satisfies Record<MastraDBMessage['role'], boolean>;

const INPUT_PART_TYPES = {
  text: true,
  reasoning: true,
  file: true,
  'step-start': true,
  error: true,
  'tool-invocation': true,
  source: true,
  'source-document': true,
} satisfies Record<Exclude<MastraMessagePart['type'], `data-${string}`>, true>;

const TOOL_INVOCATION_STATES = {
  'partial-call': true,
  call: true,
  result: true,
  'approval-requested': true,
  'approval-responded': true,
  'output-error': true,
  'output-denied': true,
} satisfies Record<MastraToolInvocation['state'], true>;

type PromptContentPart = Extract<
  AIV5Type.ModelMessage['content'],
  readonly unknown[]
>[number];
// Mastra's `modelOutput` substitution and its MCP content conversion place
// outputs from the wider AI SDK v6 union in a v5 prompt.
type PromptToolResultOutput = AIV6Type.ToolResultOutput;
type PromptToolResultContentItem = Extract<
  PromptToolResultOutput,
  { type: 'content' }
>['value'][number];

type PromptValueReaders<TMember extends { type: string }> = {
  [K in TMember['type']]: (
    member: Extract<TMember, { type: K }>,
  ) => readonly unknown[] | undefined;
};

function isTableMember(
  table: Readonly<Record<string, boolean>>,
  key: unknown,
): boolean {
  return (
    typeof key === 'string' && Object.hasOwn(table, key) && table[key] === true
  );
}

// The values a member carries to the model, or undefined when its type has
// no reader.
function readPromptValues(
  readers: Readonly<
    Record<string, (member: never) => readonly unknown[] | undefined>
  >,
  member: unknown,
): readonly unknown[] | undefined {
  if (typeof member !== 'object' || member === null) return undefined;
  const type: unknown = (member as { type?: unknown }).type;
  if (typeof type !== 'string' || !Object.hasOwn(readers, type)) {
    return undefined;
  }
  const reader = readers[type] as (
    member: unknown,
  ) => readonly unknown[] | undefined;
  return reader(member);
}

// The values of every member, or undefined when any member has no reader.
function readEachPromptValues(
  readers: Readonly<
    Record<string, (member: never) => readonly unknown[] | undefined>
  >,
  members: unknown,
): readonly unknown[] | undefined {
  if (!Array.isArray(members)) return undefined;
  const values: unknown[] = [];
  for (const member of members) {
    const memberValues = readPromptValues(readers, member);
    if (memberValues === undefined) return undefined;
    values.push(...memberValues);
  }
  return values;
}

// The values a member carries, or undefined when its provider options are
// refused or unclassified.
function withProviderOptionValues(
  member: object,
  values: readonly unknown[],
): readonly unknown[] | undefined {
  const options = providerOptionValues(
    (member as { providerOptions?: unknown }).providerOptions,
  );
  return options === undefined ? undefined : [...values, ...options];
}

// The adapters that send a content output as JSON text send every field of
// every item to the model, so each item is read whole. Base64 `data` is not
// read, as binary file data is not; those adapters send it as tool text too.
function contentItemValues(fields: object): readonly unknown[] | undefined {
  const options: unknown = (fields as { providerOptions?: unknown })
    .providerOptions;
  return providerOptionValues(options) === undefined ? undefined : [fields];
}

const TOOL_RESULT_CONTENT_VALUES = {
  text: ({ text, ...fields }) => {
    const values = contentItemValues(fields);
    return values === undefined ? undefined : [text, ...values];
  },
  media: ({ data: _data, ...fields }) => contentItemValues(fields),
  'file-data': ({ data: _data, ...fields }) => contentItemValues(fields),
  'file-url': (item) => contentItemValues(item),
  'file-id': (item) => contentItemValues(item),
  'image-data': ({ data: _data, ...fields }) => contentItemValues(fields),
  'image-url': (item) => contentItemValues(item),
  'image-file-id': (item) => contentItemValues(item),
  custom: (item) => contentItemValues(item),
} satisfies PromptValueReaders<PromptToolResultContentItem>;

const TOOL_RESULT_OUTPUT_VALUES = {
  text: (output) => withProviderOptionValues(output, [output.value]),
  json: (output) => withProviderOptionValues(output, [output.value]),
  'execution-denied': (output) =>
    withProviderOptionValues(output, [output.reason]),
  'error-text': (output) => withProviderOptionValues(output, [output.value]),
  'error-json': (output) => withProviderOptionValues(output, [output.value]),
  content: (output) => {
    const items = readEachPromptValues(
      TOOL_RESULT_CONTENT_VALUES,
      output.value,
    );
    return items === undefined
      ? undefined
      : withProviderOptionValues(output, items);
  },
} satisfies PromptValueReaders<PromptToolResultOutput>;

// Text media payloads contribute decoded text; binary payloads stay unread.
function promptMediaValues(
  part: object,
  data: unknown,
  declaredMediaType: unknown,
  initialValues: readonly unknown[],
): readonly unknown[] | undefined {
  const media = classifyPromptMedia(data, declaredMediaType);
  if (media === undefined) return undefined;
  const values: unknown[] = [...initialValues];
  if (typeof declaredMediaType === 'string') values.push(declaredMediaType);
  if (media.mediaType !== undefined && media.mediaType !== declaredMediaType) {
    values.push(media.mediaType);
  }
  if (media.mediaType !== undefined && /^text\//i.test(media.mediaType)) {
    try {
      if (media.kind === 'network-url') {
        values.push(media.url.toString());
      } else {
        const bytes =
          media.kind === 'inline-bytes'
            ? media.bytes
            : Uint8Array.from(
                atob(media.payload.replace(/-/g, '+').replace(/_/g, '/')),
                (byte) => byte.codePointAt(0) as number,
              );
        values.push(new TextDecoder().decode(bytes));
      }
    } catch {
      return undefined;
    }
  }
  return withProviderOptionValues(part, values);
}

// Tool-call ids are provider pairing keys, often random enough that an
// entropy detector would deny every replay that carries one.
const PROMPT_PART_VALUES = {
  text: (part) => withProviderOptionValues(part, [part.text]),
  reasoning: (part) => withProviderOptionValues(part, [part.text]),
  'tool-call': (part) =>
    withProviderOptionValues(part, [part.toolName, part.input]),
  'tool-result': (part) => {
    const output = readPromptValues(TOOL_RESULT_OUTPUT_VALUES, part.output);
    return output === undefined
      ? undefined
      : withProviderOptionValues(part, [part.toolName, ...output]);
  },
  file: (part) =>
    promptMediaValues(part, part.data, part.mediaType, [part.filename]),
  image: (part) => promptMediaValues(part, part.image, part.mediaType, []),
} satisfies PromptValueReaders<PromptContentPart>;

type PromptToolResultPart = Extract<PromptContentPart, { type: 'tool-result' }>;

type ResultRecord = Readonly<Record<string, unknown>>;

function withoutField(record: ResultRecord, field: string): ResultRecord {
  const { [field]: _omitted, ...rest } = record;
  return rest;
}

const CODE_EXECUTION_OUTPUT_TYPES: readonly unknown[] = [
  'code_execution_output',
  'bash_code_execution_output',
];

function withoutOutputFileIds(result: ResultRecord): ResultRecord {
  const { content } = result;
  if (!Array.isArray(content)) return result;
  return {
    ...result,
    content: content.map((item: unknown) =>
      isPlainRecord(item) && CODE_EXECUTION_OUTPUT_TYPES.includes(item.type)
        ? withoutField(item, 'file_id')
        : item,
    ),
  };
}

function withoutBase64Document(result: ResultRecord): ResultRecord {
  const { content } = result;
  if (
    !isPlainRecord(content) ||
    !isPlainRecord(content.source) ||
    content.source.type !== 'base64'
  ) {
    return result;
  }
  return {
    ...result,
    content: { ...content, source: withoutField(content.source, 'data') },
  };
}

// Provider-executed results whose root `type` names a result that a model
// adapter bundled with Mastra stores with the provider's encrypted payload,
// file reference or file data, keyed by that type.
const TYPED_PROVIDER_RESULTS: Readonly<
  Record<string, (result: ResultRecord) => ResultRecord>
> = {
  advisor_redacted_result: (result) => withoutField(result, 'encryptedContent'),
  code_execution_result: withoutOutputFileIds,
  encrypted_code_execution_result: (result) =>
    withoutOutputFileIds(withoutField(result, 'encrypted_stdout')),
  bash_code_execution_result: withoutOutputFileIds,
  web_fetch_result: withoutBase64Document,
};

// What the input policies read of a provider-executed result: the value
// without the encrypted payload, file reference or file data at the place a
// bundled adapter stores it for that tool, which the adapter sends back as
// received and an entropy detector would deny in every genuine replay. A shape
// is matched at the value's root alone, because an adapter can send a
// result's nested members as they stand, such as the tool definitions of an
// OpenAI Responses `tool_search` result; a value no shape matches is read
// whole. Mastra declares no type for these stored results, so the replays in
// `agent/provider-options.test.ts` pin them.
function providerResultValues(value: unknown): readonly unknown[] {
  if (Array.isArray(value)) {
    return [
      value.map((item: unknown) =>
        isPlainRecord(item) && item.type === 'web_search_result'
          ? withoutField(item, 'encryptedContent')
          : item,
      ),
    ];
  }
  if (!isPlainRecord(value)) return [value];
  const { type, results } = value;
  const typed =
    typeof type === 'string' && Object.hasOwn(TYPED_PROVIDER_RESULTS, type)
      ? TYPED_PROVIDER_RESULTS[type]
      : undefined;
  if (typed !== undefined) return [typed(value)];
  if (
    Object.hasOwn(value, 'queries') &&
    (Array.isArray(results) || results === null)
  ) {
    return [
      {
        ...value,
        results:
          results?.map((item: unknown) =>
            isPlainRecord(item) ? withoutField(item, 'fileId') : item,
          ) ?? null,
      },
    ];
  }
  // A generated image, which no adapter sends back.
  const keys = Object.keys(value);
  if (
    keys.length === 1 &&
    keys[0] === 'result' &&
    typeof value.result === 'string'
  ) {
    return [];
  }
  return [value];
}

const PROVIDER_RESULT_OUTPUT_VALUES = {
  ...TOOL_RESULT_OUTPUT_VALUES,
  json: (output) =>
    withProviderOptionValues(output, providerResultValues(output.value)),
} satisfies PromptValueReaders<PromptToolResultOutput>;

function someNamespace(
  options: unknown,
  test: (namespace: Readonly<Record<string, unknown>>) => boolean,
): boolean {
  return (
    isPlainRecord(options) &&
    Object.values(options).some(
      (namespace) => isPlainRecord(namespace) && test(namespace),
    )
  );
}

// The call ids of the prompt's tool calls that Anthropic sends as MCP calls.
function mcpToolCallIds(
  prompt: readonly AIV5Type.ModelMessage[],
): ReadonlySet<unknown> {
  const ids = new Set<unknown>();
  for (const message of prompt) {
    const content: unknown = message.content;
    if (!Array.isArray(content)) continue;
    for (const part of content as readonly unknown[]) {
      if (
        isPlainRecord(part) &&
        part.type === 'tool-call' &&
        someNamespace(
          part.providerOptions,
          (namespace) => namespace.type === 'mcp-tool-use',
        )
      ) {
        ids.add(part.toolCallId);
      }
    }
  }
  return ids;
}

// Mastra's conversion puts a provider-executed tool result in the assistant
// message and every other result in a tool message, but a caller chooses the
// position. So a result shape's opaque field goes unread only where no adapter
// sends the value on as content: Google sends a result naming a server tool
// call as it stands, and Anthropic the result of an MCP call.
function assistantPartValues(
  mcpCallIds: ReadonlySet<unknown>,
): PromptValueReaders<PromptContentPart> {
  return {
    ...PROMPT_PART_VALUES,
    'tool-result': (part: PromptToolResultPart) => {
      const sentAsContent =
        mcpCallIds.has(part.toolCallId) ||
        someNamespace(
          part.providerOptions,
          (namespace) =>
            namespace.serverToolCallId != null &&
            namespace.serverToolType != null,
        );
      if (sentAsContent) return PROMPT_PART_VALUES['tool-result'](part);
      const output = readPromptValues(
        PROVIDER_RESULT_OUTPUT_VALUES,
        part.output,
      );
      return output === undefined
        ? undefined
        : withProviderOptionValues(part, [part.toolName, ...output]);
    },
  };
}

function isClassifiedInputMessage(message: MastraDBMessage): boolean {
  if (!isTableMember(INPUT_MESSAGE_ROLES, message.role)) return false;
  const content: unknown = message.content;
  if (typeof content !== 'object' || content === null) return false;
  const { format, parts, toolInvocations } = content as Partial<
    Record<'format' | 'parts' | 'toolInvocations', unknown>
  >;
  if (format !== 2 || !Array.isArray(parts)) return false;
  for (const part of parts) {
    const type: unknown = (part as { type?: unknown } | null)?.type;
    if (typeof type === 'string' && type.startsWith('data-')) continue;
    if (!isTableMember(INPUT_PART_TYPES, type)) return false;
    if (
      type === 'tool-invocation' &&
      !isTableMember(
        TOOL_INVOCATION_STATES,
        (part as { toolInvocation?: { state?: unknown } | null }).toolInvocation
          ?.state,
      )
    ) {
      return false;
    }
  }
  if (toolInvocations === undefined) return true;
  return (
    Array.isArray(toolInvocations) &&
    toolInvocations.every((invocation: unknown) =>
      isTableMember(
        TOOL_INVOCATION_STATES,
        (invocation as { state?: unknown } | null)?.state,
      ),
    )
  );
}

interface StoredModelOutput {
  readonly toolCallId: string;
  readonly state: MastraToolInvocation['state'];
  readonly output: unknown;
}

// Every `mastra.modelOutput` a tool invocation in these messages stores,
// whatever the message's role and the invocation's state, in message order.
function storedModelOutputs(
  messages: readonly MastraDBMessage[],
): readonly StoredModelOutput[] {
  const stored: StoredModelOutput[] = [];
  for (const message of messages) {
    for (const part of message.content.parts) {
      if (part.type !== 'tool-invocation') continue;
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
  }
  return stored;
}

// Mastra builds the model prompt with MessageList's `get.all.aiV5.llmPrompt`,
// which runs the conversion `convertMessages` exposes and then gives a tool
// result the last stored output that a `result`-state invocation holds for its
// call id. llmPrompt also downloads the messages' assets, so the substitution
// is repeated here. The outputs it places are returned with the prompt.
function withStoredModelOutputs(
  modelMessages: readonly AIV5Type.ModelMessage[],
  stored: readonly StoredModelOutput[],
): {
  prompt: readonly AIV5Type.ModelMessage[];
  placed: ReadonlySet<unknown>;
} {
  const outputs = new Map<string, unknown>();
  for (const { toolCallId, state, output } of stored) {
    if (state === 'result') outputs.set(toolCallId, output);
  }
  const placed = new Set<unknown>();
  if (outputs.size === 0) return { prompt: modelMessages, placed };
  const prompt = modelMessages.map((message) =>
    message.role !== 'tool'
      ? message
      : {
          ...message,
          content: message.content.map((part) => {
            if (part.type !== 'tool-result' || !outputs.has(part.toolCallId)) {
              return part;
            }
            const output = outputs.get(part.toolCallId);
            placed.add(output);
            return { ...part, output: output as typeof part.output };
          }),
        },
  );
  return { prompt, placed };
}

// A string stays as written so a pattern matches it as the model reads it;
// any other value is matched through its JSON text. JSON has no text for
// `undefined`, a function or a symbol.
function promptValueText(value: unknown): string | undefined {
  return typeof value === 'string'
    ? value
    : (JSON.stringify(value) as string | undefined);
}

function joinedPromptText(values: readonly unknown[]): string {
  const texts: string[] = [];
  for (const value of values) {
    const text = promptValueText(value);
    if (text) texts.push(text);
  }
  return texts.join('\n');
}

// The text Mastra renders into the model prompt from these messages, or
// undefined when one of them carries content no table above classifies or a
// provider option this module refuses. Mastra substitutes a stored output into
// any tool result with its call id, in memory-loaded history and in later loop
// steps too, so every stored output is read, whether placed here or not.
function inputPromptText(
  messages: readonly MastraDBMessage[],
): string | undefined {
  if (!messages.every(isClassifiedInputMessage)) return undefined;
  const converted = convertedPrompt(messages);
  const stored = storedModelOutputs(messages);
  const { prompt, placed } = withStoredModelOutputs(converted, stored);
  const assistantReaders = assistantPartValues(mcpToolCallIds(prompt));
  const values: unknown[] = [];
  for (const message of prompt) {
    const content: unknown = message.content;
    const contentValues =
      typeof content === 'string'
        ? [content]
        : readEachPromptValues(
            message.role === 'assistant'
              ? assistantReaders
              : PROMPT_PART_VALUES,
            content,
          );
    const optionValues = providerOptionValues(message.providerOptions);
    if (contentValues === undefined || optionValues === undefined) {
      return undefined;
    }
    values.push(...contentValues, ...optionValues);
  }
  for (const { output } of stored) {
    if (placed.has(output)) continue;
    const outputValues = readPromptValues(TOOL_RESULT_OUTPUT_VALUES, output);
    if (outputValues === undefined) return undefined;
    values.push(...outputValues);
  }
  return joinedPromptText(values);
}

// The text a recorded system message carries to the model: its text parts
// joined as Mastra's conversion joins them, and what this module's provider
// option table reads from the options Mastra sends with it. Undefined when an
// option is refused or unclassified.
function recordedSystemText(
  message: RecordedSystemMessage,
): string | undefined {
  const { content } = message;
  const parts = typeof content === 'string' ? [] : content;
  const values: unknown[] = [
    typeof content === 'string'
      ? content
      : parts.map(({ text }) => text).join(''),
  ];
  for (const options of [
    message.providerOptions,
    message.experimental_providerMetadata,
    ...parts.map(({ providerOptions }) => providerOptions),
  ]) {
    const optionValues = providerOptionValues(options);
    if (optionValues === undefined) return undefined;
    values.push(...optionValues);
  }
  return joinedPromptText(values);
}

// The text of what application input processors added or changed outside the
// call's input, read as the caller's messages are read, or undefined when any
// of it is unclassified.
function processorAdditionText(
  additions: readonly ProcessorAddition[],
): string | undefined {
  const texts: string[] = [];
  for (const addition of additions) {
    const text =
      addition.kind === 'system'
        ? recordedSystemText(addition.message)
        : inputPromptText([addition.message]);
    if (text === undefined) return undefined;
    if (text) texts.push(text);
  }
  return texts.join('\n');
}

interface CallerInput {
  readonly messages: MastraDBMessage[];
  readonly text: string;
}

// Input policies read caller messages and client tool outcomes merged into
// remembered messages, together with application processor additions.
// Without a message list, every message is the caller's.
function callerInput(
  args: ProcessInputArgs,
  additions: readonly ProcessorAddition[],
): CallerInput | undefined {
  const messages =
    args.messageList == null
      ? args.messages
      : callerMessages(args.messageList, args.messages);
  const text = inputPromptText(messages);
  const added = processorAdditionText(additionsToRead(additions, messages));
  if (text === undefined || added === undefined) return undefined;
  return {
    messages,
    text: [text, added].filter((part) => part !== '').join('\n'),
  };
}

/**
 * Policy text of format-2 messages: the text Mastra renders into the model
 * prompt from them, and every stored model output they carry, joined for
 * policy matching. Tool-call ids, provider metadata such as signatures and
 * item ids, and binary file and image data are not included. File and image
 * media types and decoded text/* payloads, including data: URL payloads, are
 * included. A text/* network URL contributes its URL string; its target is
 * not read. Nor is the encrypted payload, file reference or file data of a
 * provider-executed tool result in an assistant message, where the root of
 * the result's value has the shape a model adapter bundled with Mastra stores
 * for that tool; the same field anywhere else is included, and so is the
 * whole of a result that names a Google server-tool call or answers an
 * Anthropic MCP call, which those adapters send on as content.
 *
 * @throws TypeError when a message's role, part type, tool-invocation state,
 * rendered prompt part or provider option is outside what this function
 * classifies, or is a provider option it refuses because a model adapter
 * renders it as content. When the conversion itself throws, the `TypeError`
 * carries that error as its `cause`.
 */
export function extractMessageText(
  messages: readonly MastraDBMessage[],
): string {
  let text: string | undefined;
  try {
    text = inputPromptText(messages);
  } catch (error) {
    throw new TypeError(`extractMessageText: ${UNCLASSIFIED_INPUT_CONTENT}`, {
      cause: error,
    });
  }
  if (text === undefined) {
    throw new TypeError(`extractMessageText: ${UNCLASSIFIED_INPUT_CONTENT}`);
  }
  return text;
}

// Per-stream accumulated text by channel, kept in the processor's `state`
// (core persists that object across every method call of one request), so
// accumulation is O(chunk) instead of rebuilding from all streamParts on
// every chunk. Keys are namespaced: `state` is per-processor, but a subclass
// or wrapper sharing it must not collide with the accumulator.
const CHANNELS_STATE_KEY = 'breakwater.channels';
// Per-policy incremental-scan namespaces, keyed by policy index — names can
// collide across instances (two denyPatterns both named 'deny-patterns');
// indexes cannot.
const POLICY_STATE_KEY = 'breakwater.policyState';
const OBJECT_CHANNEL_EVALUATED_STATE_KEY = 'breakwater.objectChannelEvaluated';
const STREAM_EVALUATED_POLICIES_STATE_KEY =
  'breakwater.streamEvaluatedPolicies';
const JSON_VALUE_SCHEMA = z.json();

interface CanonicalJsonValue {
  value: JSONType;
  snapshot: string;
}

interface StreamEvaluatedPolicies {
  names: string[];
  channels: OutputChannel[];
}

function streamEvaluatedPoliciesOf(
  state: Record<string, unknown>,
): StreamEvaluatedPolicies {
  let evaluated = state[STREAM_EVALUATED_POLICIES_STATE_KEY] as
    | StreamEvaluatedPolicies
    | undefined;
  if (!evaluated) {
    evaluated = { names: [], channels: [] };
    state[STREAM_EVALUATED_POLICIES_STATE_KEY] = evaluated;
  }
  return evaluated;
}

function canonicalJsonValue(value: unknown): CanonicalJsonValue {
  const parsed = JSON_VALUE_SCHEMA.parse(value);
  const snapshot = JSON.stringify(parsed);
  return {
    value: JSON.parse(snapshot) as JSONType,
    snapshot,
  };
}

interface ChannelTexts {
  answer: string;
  reasoning: string;
  object: string;
}

function channelTextsOf(state: Record<string, unknown>): ChannelTexts {
  let texts = state[CHANNELS_STATE_KEY] as ChannelTexts | undefined;
  if (!texts) {
    texts = { answer: '', reasoning: '', object: '' };
    state[CHANNELS_STATE_KEY] = texts;
  }
  return texts;
}

function policyStreamStateOf(
  state: Record<string, unknown>,
  index: number,
): Record<string, unknown> {
  let namespaces = state[POLICY_STATE_KEY] as
    | Record<number, Record<string, unknown>>
    | undefined;
  if (!namespaces) {
    namespaces = {};
    state[POLICY_STATE_KEY] = namespaces;
  }
  let namespace = namespaces[index];
  if (!namespace) {
    namespace = {};
    namespaces[index] = namespace;
  }
  return namespace;
}

// ---------------------------------------------------------------------------
// Hold-back buffering (opt-in via PolicyEngineOptions.holdBack)
// ---------------------------------------------------------------------------

// Well-known ProcessorRunner state key: a stream processor may return one
// chunk AND stash a second part under this key; the runner re-drives the
// stashed part through the full output-processor chain after the returned
// chunk is emitted. Not exported from any public @mastra/core subpath —
// literal per @mastra/core dist/processors/stream-reprocess.d.ts
// (REPROCESS_PART_KEY, v1.49.0). Drift protection: the real-ProcessorRunner
// tripwire test in policy-engine.test.ts drives core's actual drain, so a
// core rename/semantics change fails the suite instead of silently
// degrading the finish-flush.
const REPROCESS_PART_KEY = '__mastraReprocessPart';

// Hold-back state, next to the channel accumulator in the processor's
// per-request `state`.
const HOLD_STATE_KEY = 'breakwater.holdBack';

// The append-only text channels hold-back applies to. The object channel needs
// no window: intermediate snapshots are suppressed outright.
type HoldableChannel = 'answer' | 'reasoning';

type DeltaChunk = Extract<
  ChunkType,
  { type: 'text-delta' | 'reasoning-delta' }
>;

interface HeldChannel {
  /** Text not yet emitted (the trailing window + backlog). */
  pending: string;
  /** Last delta chunk of this channel — template for coalesced emissions. */
  shape: DeltaChunk;
  /** Prevents repeated evaluation of a second channel on a re-driven finish. */
  terminalEvaluated?: boolean;
}

type HoldState = Partial<Record<HoldableChannel, HeldChannel>>;

function holdStateOf(state: Record<string, unknown>): HoldState {
  let hold = state[HOLD_STATE_KEY] as HoldState | undefined;
  if (!hold) {
    hold = {};
    state[HOLD_STATE_KEY] = hold;
  }
  return hold;
}

// Rebuilds on the channel's own chunk shape so ids/metadata stay coherent;
// only the text differs. The cast is sound: shape's type/payload pairing is
// preserved and text is a string in both delta payloads.
function coalescedDelta(shape: DeltaChunk, text: string): DeltaChunk {
  return { ...shape, payload: { ...shape.payload, text } } as DeltaChunk;
}

// Per-channel hold-back window: max holdBackChars over output-phase policies
// gating that channel. Policies without the hint contribute 0.
function holdBackWindowFor(
  policies: readonly PolicyEvaluator[],
  channel: HoldableChannel,
): number {
  let window = 0;
  for (const policy of policies) {
    if (policy.phases && !policy.phases.includes('output')) continue;
    if (!(policy.channels ?? DEFAULT_CHANNELS).includes(channel)) continue;
    window = Math.max(window, policy.holdBackChars ?? 0);
  }
  return window;
}

function isPolicyPhase(value: unknown): value is PolicyPhase {
  return value === 'input' || value === 'output';
}

function isOutputChannel(value: unknown): value is OutputChannel {
  return value === 'answer' || value === 'reasoning' || value === 'object';
}

// An empty, non-array or misspelled selector can select nothing, and a policy
// that never runs at a security boundary fails open.
function snapshotSelector<T>(
  subject: string,
  selector: unknown,
  isMember: (value: unknown) => value is T,
  members: string,
): readonly T[] {
  return readFrozenList(
    subject,
    selector,
    (entry, index) => {
      if (!isMember(entry)) {
        throw new TypeError(
          `${subject} entry ${index} must be ${members} (got ${describeEntry(entry)})`,
        );
      }
      return entry;
    },
    true,
  );
}

// A class-based evaluator keeps `evaluate` on its prototype, so the check reads
// own keys and never requires `evaluate` to be one.
const POLICY_EVALUATOR_KEYS = {
  name: true,
  phases: true,
  channels: true,
  holdBackChars: true,
  evaluate: true,
} satisfies Record<keyof PolicyEvaluator, true>;

// Only a plain object, such as a literal or a spread of a factory's output, is
// held to the declared fields. A class-based evaluator keeps its receiver, and
// with it any instance state its constructor or fields assign.
function assertPolicyEntryFields(
  subject: string,
  entry: unknown,
): asserts entry is object {
  if (typeof entry === 'object' && entry !== null && !Array.isArray(entry)) {
    const prototype: unknown = Object.getPrototypeOf(entry);
    if (prototype !== Object.prototype && prototype !== null) return;
  }
  assertKnownFields(subject, entry, POLICY_EVALUATOR_KEYS);
}

function snapshotPolicies(
  caller: 'PolicyEngine' | 'createContentPolicyGate',
  policies: unknown,
  requireEntries = false,
): readonly PolicyEvaluator[] {
  return readFrozenList(
    `${caller}: policies`,
    policies,
    (entry, index) => {
      assertPolicyEntryFields(`${caller}: policies entry ${index}`, entry);
      const policy = entry as PolicyEvaluator;
      const name = policy.name;
      const phases = policy.phases;
      const channels = policy.channels;
      const holdBackChars = policy.holdBackChars;
      const evaluate = policy.evaluate.bind(policy);
      const label = `${caller}: policy ${typeof name === 'string' ? `'${name}'` : index}`;
      return Object.freeze({
        name,
        ...(phases !== undefined
          ? {
              phases: snapshotSelector(
                `${label} phases`,
                phases,
                isPolicyPhase,
                "'input' or 'output'",
              ),
            }
          : {}),
        ...(channels !== undefined
          ? {
              channels: snapshotSelector(
                `${label} channels`,
                channels,
                isOutputChannel,
                "'answer', 'reasoning' or 'object'",
              ),
            }
          : {}),
        ...(holdBackChars !== undefined
          ? {
              holdBackChars: readHoldBackChars(
                `${label} holdBackChars`,
                holdBackChars,
              ),
            }
          : {}),
        evaluate,
      });
    },
    requireEntries,
  );
}

/**
 * The reason an evaluator failure surfaces. Static because exception text may
 * carry the inspected payload; shared so the audit record and the streaming
 * abort reason cannot drift apart.
 */
const POLICY_EVALUATION_FAILED = 'policy evaluation failed';

/** The reason an output text chunk or reasoning step that is not a string aborts. */
const NON_STRING_OUTPUT_TEXT = 'output text is not a string';

type OrderedPolicyEvaluation =
  | { outcome: 'allowed'; evaluated: string[] }
  | { outcome: 'denied'; reason: string }
  | { outcome: 'error'; error: unknown };

interface OrderedPolicyEvaluationOptions {
  policies: readonly PolicyEvaluator[];
  context: PolicyContext;
  actor: Actor | null;
  audit?: AuditLogger;
  resource: string;
  streamAccumulator?: Record<string, unknown>;
}

/** A policy decision, read once from what an evaluator returned. */
type ReadPolicyDecision =
  | { readonly allowed: true }
  | { readonly allowed: false; readonly reason: string | undefined };

// An evaluator that returns no decision has failed as surely as one that
// throws, and allowing its call would fail open.
function readPolicyDecision(decision: unknown): ReadPolicyDecision {
  if (typeof decision === 'object' && decision !== null) {
    const { allowed, reason } = decision as {
      allowed?: unknown;
      reason?: unknown;
    };
    if (allowed === true) return { allowed };
    if (
      allowed === false &&
      (reason === undefined || typeof reason === 'string')
    ) {
      return { allowed, reason };
    }
  }
  throw new TypeError('policy evaluator returned no decision');
}

/**
 * The terminal allow record, shared so every policy boundary emits one audit
 * vocabulary.
 */
function recordAllowedPolicyDecision(options: {
  audit?: AuditLogger;
  phase: PolicyPhase;
  actor: Actor | null;
  resource: string;
  evaluated: readonly string[];
  requestContext?: RequestContext;
  channels?: readonly OutputChannel[];
}): void {
  options.audit?.record({
    actor: options.actor,
    action: `agent.${options.phase}.policy`,
    resource: options.resource,
    decision: 'allowed',
    detail: agentAuditDetail(options.requestContext, {
      evaluated: options.evaluated,
      ...(options.channels !== undefined ? { channels: options.channels } : {}),
    }),
  });
}

async function evaluatePoliciesInOrder(
  options: OrderedPolicyEvaluationOptions,
): Promise<OrderedPolicyEvaluation> {
  const { policies, context, actor, audit, resource, streamAccumulator } =
    options;
  const { phase, channel } = context;
  const evaluated: string[] = [];
  for (const [index, policy] of policies.entries()) {
    if (policy.phases && !policy.phases.includes(phase)) continue;
    if (!(policy.channels ?? DEFAULT_CHANNELS).includes(channel)) continue;
    evaluated.push(policy.name);
    let decision: ReadPolicyDecision;
    try {
      const streamState = streamAccumulator
        ? policyStreamStateOf(streamAccumulator, index)
        : undefined;
      decision = readPolicyDecision(
        await policy.evaluate(
          streamState ? { ...context, streamState } : context,
        ),
      );
    } catch (error) {
      // An evaluator crash is worse than a denial; it must not leave less
      // audit evidence than one. Opaque exception text may contain the
      // inspected payload, so the audit and every abort reason stay static,
      // and the thrown value never goes into the record.
      audit?.record({
        actor,
        action: `agent.${phase}.policy`,
        resource,
        decision: 'error',
        reason: POLICY_EVALUATION_FAILED,
        detail: agentAuditDetail(context.requestContext, {
          policy: policy.name,
          channel,
        }),
      });
      return { outcome: 'error', error };
    }
    if (!decision.allowed) {
      audit?.record({
        actor,
        action: `agent.${phase}.policy`,
        resource,
        decision: 'denied',
        reason: 'policy denied',
        detail: agentAuditDetail(context.requestContext, {
          policy: policy.name,
          channel,
        }),
      });
      return {
        outcome: 'denied',
        reason: `${policy.name}: ${decision.reason}`,
      };
    }
  }
  return { outcome: 'allowed', evaluated };
}

/** Result of evaluating text at a standalone content-policy boundary. */
export type ContentPolicyGateResult =
  | { allowed: true }
  | { allowed: false; outcome: 'denied' | 'error' };

/**
 * Text and trusted Mastra context evaluated by a standalone content gate. An
 * input that is not an object, has a field this interface does not declare,
 * carries a non-string `text`, or carries a present `requestContext` that is
 * not a `RequestContext` is an error outcome.
 */
export interface ContentPolicyGateInput {
  /** Canonical text that the downstream model would observe. */
  text: string;
  /** Trusted request context associated with the content. */
  requestContext?: RequestContext;
}

const CONTENT_POLICY_GATE_INPUT_KEYS = {
  text: true,
  requestContext: true,
} satisfies Record<keyof ContentPolicyGateInput, true>;

// Each policy would otherwise read a malformed input its own way, and some
// allow what they cannot read.
function readContentPolicyGateInput(
  input: unknown,
): ContentPolicyGateInput | undefined {
  if (typeof input !== 'object' || input === null || Array.isArray(input)) {
    return undefined;
  }
  if (unknownFieldOf(input, CONTENT_POLICY_GATE_INPUT_KEYS) !== undefined) {
    return undefined;
  }
  const { text, requestContext } = input as Record<string, unknown>;
  if (typeof text !== 'string') return undefined;
  if (requestContext === undefined) return { text };
  return requestContext instanceof RequestContext
    ? { text, requestContext }
    : undefined;
}

/** Configuration for {@link createContentPolicyGate}. */
export interface ContentPolicyGateOptions {
  /** Policies snapshotted at construction and evaluated in array order. */
  policies: readonly PolicyEvaluator[];
  /** Optional audit logger for policy decisions and evaluator failures. */
  audit?: AuditLogger;
  /** Audit resource. Defaults to `breakwater-content-policy-gate`. */
  resource?: string;
}

const CONTENT_POLICY_GATE_OPTION_KEYS = {
  policies: true,
  audit: true,
  resource: true,
} satisfies Record<keyof ContentPolicyGateOptions, true>;

/** Standalone input-content policy boundary. */
export type ContentPolicyGate = (
  input: ContentPolicyGateInput,
) => Promise<ContentPolicyGateResult>;

/**
 * Create an input-content gate for model-visible text outside a Mastra
 * processor call. Denials and evaluator failures return opaque outcomes;
 * policy names, reasons, inspected text, and thrown values remain internal.
 *
 * Every policy must be able to run here: this gate only ever evaluates the
 * input phase on the answer channel, so a policy selecting anything else is
 * a silent hole at a security boundary rather than a harmless no-op, and is
 * rejected at construction. The list needs at least one policy.
 */
export function createContentPolicyGate(
  options: ContentPolicyGateOptions,
): ContentPolicyGate {
  assertKnownFields(
    'createContentPolicyGate: options',
    options,
    CONTENT_POLICY_GATE_OPTION_KEYS,
  );
  // A gate with no policy allows every input, and inspection is all it does.
  const policies = snapshotPolicies(
    'createContentPolicyGate',
    options.policies,
    true,
  );
  for (const policy of policies) {
    const selector =
      policy.phases && !policy.phases.includes('input')
        ? `phases ${JSON.stringify(policy.phases)}`
        : !(policy.channels ?? DEFAULT_CHANNELS).includes('answer')
          ? `channels ${JSON.stringify(policy.channels)}`
          : undefined;
    if (selector !== undefined) {
      throw new TypeError(
        `createContentPolicyGate: policy '${policy.name}' declares ${selector} — this gate only evaluates the 'input' phase on the 'answer' channel, so the policy would never run. Widen its selectors or register it on a PolicyEngine instead.`,
      );
    }
  }
  const audit = options.audit;
  const resource = options.resource ?? 'breakwater-content-policy-gate';

  return async (input) => {
    const read = readContentPolicyGateInput(input);
    if (read === undefined) {
      audit?.record({
        actor: null,
        action: 'agent.input.policy',
        resource,
        decision: 'error',
        reason: 'content gate input is malformed',
      });
      return { allowed: false, outcome: 'error' };
    }
    const { text, requestContext } = read;
    const actor = actorFromRequestContext(requestContext) ?? null;
    const malformedAuditContext = malformedAgentAuditContextEvent(
      requestContext,
      resource,
      actor,
    );
    if (malformedAuditContext) audit?.record(malformedAuditContext);
    const result = await evaluatePoliciesInOrder({
      policies,
      context: {
        phase: 'input',
        channel: 'answer',
        messages: [],
        text,
        requestContext,
      },
      actor,
      audit,
      resource,
    });
    if (result.outcome === 'denied') {
      return { allowed: false, outcome: 'denied' };
    }
    if (result.outcome === 'error') {
      return { allowed: false, outcome: 'error' };
    }
    recordAllowedPolicyDecision({
      audit,
      phase: 'input',
      actor,
      resource,
      evaluated: result.evaluated,
      requestContext,
    });
    return { allowed: true };
  };
}

/** Configuration for `PolicyEngine`. */
export interface PolicyEngineOptions {
  /** Policies snapshotted at construction and evaluated in array order. */
  policies: readonly PolicyEvaluator[];
  /** Optional audit logger for policy decisions and evaluator failures. */
  audit?: AuditLogger;
  /** Audit resource. Defaults to the stable processor identifier. */
  resource?: string;
  /**
   * Opt-in zero-leak streaming: hold back a trailing window of each text
   * channel so a violating span is caught BEFORE any of it is emitted.
   * Windows come from the registered policies' `holdBackChars` hints (per
   * channel, max wins). Released text arrives as modified delta chunks;
   * intermediate 'object' snapshots are suppressed (only a passing
   * 'object-result' is emitted); the held tail is flushed at the channel's
   * end chunk ('text-end'/'reasoning-end') and, as a backstop for streams
   * without end chunks, at 'finish' — both through the runner's reprocess
   * convention, so the flush precedes its end marker. The guarantee is
   * therefore PER SEGMENT: before release, every applicable output policy
   * evaluates the channel's whole text once more, including the held tail.
   * A host evaluator receives this call even when the text is unchanged;
   * a host cadence evaluator must decide for itself whether to classify.
   * A match completing across segment boundaries (multi-step or
   * multi-text-block runs) aborts the stream after earlier segments were
   * already released — bounded by the window for string patterns or the
   * whole prior segment for RegExp (Infinity) policies. Default false —
   * evaluated chunks flow through unmodified, and already-emitted earlier
   * chunks of a violating span may have leaked by abort time.
   */
  holdBack?: boolean;
}

const POLICY_ENGINE_OPTION_KEYS = {
  policies: true,
  audit: true,
  resource: true,
  holdBack: true,
} satisfies Record<keyof PolicyEngineOptions, true>;

/**
 * Mastra Processor implementing input/output policy gating — see the module
 * comment for the phase/channel model.
 *
 * Under the supported `@mastra/core` peer, a final output result has no
 * structured-object field. The constructor therefore requires an audit sink
 * when a policy selects `object` without `answer`. A final-result call fails
 * closed unless this engine actually evaluated an object chunk. Policies that
 * include `answer` inspect JSON carried as answer text.
 *
 * The constructor also rejects an explicit input policy whose channels
 * exclude `answer`, because input evaluation has no other channel.
 */
export class PolicyEngine implements Processor<'breakwater-policy-engine'> {
  /** Stable Mastra processor identifier. */
  readonly id = 'breakwater-policy-engine' as const;
  readonly #policies: readonly PolicyEvaluator[];
  readonly #audit?: AuditLogger;
  readonly #resource: string;
  readonly #holdBack: boolean;
  readonly #holdBackWindow: Record<HoldableChannel, number>;
  readonly #objectOnlyPolicyNames: readonly string[];

  constructor(options: PolicyEngineOptions) {
    assertKnownFields(
      'PolicyEngine: options',
      options,
      POLICY_ENGINE_OPTION_KEYS,
    );
    const audit = options.audit;
    // The object-only fence below requires a logger that can record the
    // coverage error, so a present value without a callable `record`, null
    // included, is refused rather than read as omitted.
    if (
      audit !== undefined &&
      (typeof audit !== 'object' ||
        audit === null ||
        typeof audit.record !== 'function')
    ) {
      throw new TypeError(
        `PolicyEngine: audit must be an AuditLogger when provided (got ${describeEntry(audit)})`,
      );
    }
    const policies = snapshotPolicies('PolicyEngine', options.policies);
    for (const policy of policies) {
      if (
        policy.phases?.includes('input') &&
        policy.channels !== undefined &&
        !policy.channels.includes('answer')
      ) {
        throw new TypeError(
          `PolicyEngine: policy '${policy.name}' declares phases including 'input' but channels excluding 'answer' — processInput only ever evaluates the answer channel, so this policy would never run on input. Include 'answer' in channels or drop 'input' from phases.`,
        );
      }
    }
    this.#policies = policies;
    this.#audit = audit;
    this.#resource = options.resource ?? this.id;
    this.#holdBack = options.holdBack ?? false;
    this.#holdBackWindow = {
      answer: holdBackWindowFor(policies, 'answer'),
      reasoning: holdBackWindowFor(policies, 'reasoning'),
    };
    this.#objectOnlyPolicyNames = Object.freeze(
      policies
        .filter(
          (policy) =>
            policy.channels?.includes('object') &&
            !policy.channels.includes('answer'),
        )
        .map((policy) => policy.name),
    );
    // D1 fence (construction time): an object-only policy needs an audit sink
    // to record a fail-closed coverage error when processors expose no object.
    // Reuses the object-only set computed above.
    if (this.#objectOnlyPolicyNames.length > 0 && audit === undefined) {
      const names = this.#objectOnlyPolicyNames.join(', ');
      const plural = this.#objectOnlyPolicyNames.length === 1 ? 'y' : 'ies';
      throw new TypeError(
        `PolicyEngine: polic${plural} [${names}] scoped to the 'object' channel without 'answer' require an audit sink for fail-closed coverage errors when no object reaches the processor — provide options.audit, or include 'answer' in channels.`,
      );
    }
  }

  async processInput(args: ProcessInputArgs): Promise<ProcessInputResult> {
    const actor = actorFromRequestContext(args.requestContext) ?? null;
    this.#recordMalformedAuditContext(args.requestContext, actor);
    const additions =
      args.messageList == null ? [] : takeProcessorAdditions(args.messageList);
    const abort = (reason: string): never =>
      stopWithoutCallMessages(args.messageList, { additions }, () =>
        args.abort(reason),
      );
    // A throw stops input as unclassified content does: Mastra's durable
    // preparation logs an input processor's error, unless it is a tripwire,
    // and runs the model anyway.
    let input: CallerInput | undefined;
    try {
      input = callerInput(args, additions);
    } catch {
      input = undefined;
    }
    if (input === undefined) {
      this.#audit?.record({
        actor,
        action: 'agent.input.policy',
        resource: this.#resource,
        decision: 'error',
        reason: UNCLASSIFIED_INPUT_CONTENT,
        detail: agentAuditDetail(args.requestContext),
      });
      return abort(UNCLASSIFIED_INPUT_CONTENT);
    }
    const evaluated = await this.#evaluate(
      {
        phase: 'input',
        channel: 'answer',
        messages: input.messages,
        text: input.text,
        requestContext: args.requestContext,
      },
      actor,
      abort,
      true,
    );
    this.#recordAllowed('input', actor, evaluated, args.requestContext);
    return args.messages;
  }

  async processOutputResult(
    args: ProcessOutputResultArgs,
  ): Promise<MastraDBMessage[]> {
    this.#assertObjectChannelCoverage(args);
    // result.text is the authoritative generation output (non-optional in
    // core); messages also carry earlier conversation turns the output
    // policies should not re-gate. This is the final processor gate for
    // answer and reasoning on both agent.generate() and, after the stream
    // drains, agent.stream().
    const actor = actorFromRequestContext(args.requestContext) ?? null;
    this.#recordMalformedAuditContext(args.requestContext, actor);
    const evaluated = await this.#evaluate(
      {
        phase: 'output',
        channel: 'answer',
        messages: args.messages,
        text: args.result.text,
        requestContext: args.requestContext,
      },
      actor,
      args.abort,
    );
    const evaluatedChannels: OutputChannel[] =
      evaluated.length > 0 ? ['answer'] : [];
    // Reasoning is gated from the per-step aggregates. OutputResult carries
    // no structured-object field, so this processor gates the object channel
    // only when 'object'/'object-result' chunks reach processOutputStream.
    // JSON carried as answer text is covered by the answer pass above.
    const reasoningTexts: string[] = [];
    for (const step of args.result.steps) {
      const text: unknown = step.reasoningText;
      if (text === undefined || text === '') continue;
      if (typeof text !== 'string') {
        return this.#abortOnNonStringText(args, 'reasoning');
      }
      reasoningTexts.push(text);
    }
    const reasoningText = reasoningTexts.join('\n');
    if (reasoningText !== '') {
      const reasoningEvaluated = await this.#evaluate(
        {
          phase: 'output',
          channel: 'reasoning',
          messages: args.messages,
          text: reasoningText,
          requestContext: args.requestContext,
        },
        actor,
        args.abort,
      );
      evaluated.push(...reasoningEvaluated);
      if (reasoningEvaluated.length > 0) {
        evaluatedChannels.push('reasoning');
      }
    }
    const streamedEvaluated = args.state[STREAM_EVALUATED_POLICIES_STATE_KEY] as
      | StreamEvaluatedPolicies
      | undefined;
    if (streamedEvaluated) {
      evaluated.push(...streamedEvaluated.names);
      evaluatedChannels.push(...streamedEvaluated.channels);
    }
    this.#recordAllowed(
      'output',
      actor,
      [...new Set(evaluated)],
      args.requestContext,
      [...new Set(evaluatedChannels)],
    );
    return args.messages;
  }

  /** Names of policies scoped to `object` without `answer` (never result-covered by the engine alone). */
  get objectOnlyPolicyNames(): readonly string[] {
    return this.#objectOnlyPolicyNames;
  }

  // agent.stream() emits chunks to the client before processOutputResult
  // runs, so the result gate alone lets forbidden output through mid-stream.
  // Each gated chunk feeds its channel's accumulated text — text-delta →
  // answer, reasoning-delta → reasoning, object/object-result → the latest
  // stringified snapshot — and that channel is evaluated on the accumulated
  // total before the chunk is emitted: a length cap needs the cumulative sum,
  // and a pattern split across chunks is caught on the chunk that completes
  // it — aborted before that chunk is emitted. Without holdBack the residual
  // limit is that already-emitted earlier chunks of a violating span have
  // leaked by abort time; with holdBack on, each channel's trailing window
  // stays unemitted, so evaluation runs before the held suffix is released.
  // Ungated chunk types pass through untouched. A driver that never emits
  // 'finish' can truncate hold-back tail emission.
  async processOutputStream(
    args: ProcessOutputStreamArgs,
  ): Promise<ChunkType | null | undefined> {
    const { part } = args;
    const texts = channelTextsOf(args.state);
    let channel: OutputChannel;
    let delta: DeltaChunk | undefined;
    let forwardedPart = part;
    if (part.type === 'text-delta') {
      // A chunk whose text is not a string cannot be evaluated, so forwarding
      // it would release text no policy has seen.
      if (typeof part.payload.text !== 'string') {
        return this.#abortOnNonStringText(args, 'answer');
      }
      texts.answer += part.payload.text;
      channel = 'answer';
      delta = part;
    } else if (part.type === 'reasoning-delta') {
      if (typeof part.payload.text !== 'string') {
        return this.#abortOnNonStringText(args, 'reasoning');
      }
      texts.reasoning += part.payload.text;
      channel = 'reasoning';
      delta = part;
    } else if (part.type === 'object' || part.type === 'object-result') {
      // Successive 'object' chunks are growing partial snapshots, not deltas
      // (ChunkType, stream/types.d.ts): replace, never concatenate. The
      // stringify lib type lies — it returns undefined for undefined input
      // (a malformed chunk), which must not corrupt the tracked text.
      let canonical: CanonicalJsonValue;
      try {
        canonical = canonicalJsonValue(part.object);
      } catch {
        this.#audit?.record({
          actor: actorFromRequestContext(args.requestContext) ?? null,
          action: 'agent.output.policy',
          resource: this.#resource,
          decision: 'error',
          reason: 'structured object is not JSON data',
          detail: agentAuditDetail(args.requestContext, {
            channel: 'object',
          }),
        });
        args.abort('structured object is not JSON data');
      }
      texts.object = canonical.snapshot;
      channel = 'object';
      forwardedPart = {
        ...part,
        object: canonical.value,
      } as unknown as ChunkType;
    } else {
      return this.#holdBack ? this.#forwardUngated(args) : part;
    }
    const evaluated = await this.#evaluateStreamChannel(args, channel);
    if (evaluated.length > 0) {
      const streamedEvaluated = streamEvaluatedPoliciesOf(args.state);
      for (const name of evaluated) {
        if (!streamedEvaluated.names.includes(name)) {
          streamedEvaluated.names.push(name);
        }
      }
      if (!streamedEvaluated.channels.includes(channel)) {
        streamedEvaluated.channels.push(channel);
      }
    }
    if (channel === 'object') {
      args.state[OBJECT_CHANNEL_EVALUATED_STATE_KEY] = true;
    }
    if (!this.#holdBack) return forwardedPart;
    if (delta && (channel === 'answer' || channel === 'reasoning')) {
      return this.#releaseHeld(args, channel, delta);
    }
    // Intermediate 'object' snapshots are suppressed under hold-back
    // (evaluated, never emitted); the final object-result is emitted once it
    // passes. Trade-off: consumers get only the final object.
    return part.type === 'object-result' ? forwardedPart : null;
  }

  // Hold-back release for a just-evaluated delta. The full accumulated
  // channel text (INCLUDING the held tail) was passed to applicable policies
  // above. Text behind the largest declared window lies outside every
  // applicable policy's window and is returned as a MODIFIED chunk carrying
  // the prefix; the tail stays pending (null when nothing is releasable yet).
  #releaseHeld(
    args: ProcessOutputStreamArgs,
    channel: HoldableChannel,
    part: DeltaChunk,
  ): ChunkType | null {
    const window = this.#holdBackWindow[channel];
    const hold = holdStateOf(args.state);
    const held = hold[channel];
    // Window 0 (e.g. only length policies): nothing is ever held for this
    // channel; the chunk flows through unmodified.
    if (window === 0 && (!held || held.pending === '')) return part;
    let entry = held;
    if (!entry) {
      entry = { pending: '', shape: part };
      hold[channel] = entry;
    }
    entry.shape = part;
    entry.pending += part.payload.text;
    entry.terminalEvaluated = false;
    const releaseLength = Math.max(0, entry.pending.length - window);
    if (releaseLength === 0) return null;
    const releasable = entry.pending.slice(0, releaseLength);
    entry.pending = entry.pending.slice(releaseLength);
    return coalescedDelta(part, releasable);
  }

  // Ungated chunk types under hold-back. A channel's end chunk flushes that
  // channel's held tail; 'finish' drains any channel still pending (streams
  // without end chunks) — both via the reprocess convention, returning the
  // coalesced flush and stashing the trigger part for the runner to re-drive
  // through the chain until nothing is pending. 'error'/'abort' drop
  // pending: the stream is dead, and emitting held tail text
  // after the failure the client already saw would reorder the stream.
  // Everything else passes through with pending untouched (per-delta release
  // already respects the window, so no mid-stream flush is needed).
  async #forwardUngated(args: ProcessOutputStreamArgs): Promise<ChunkType> {
    const { part } = args;
    const hold = holdStateOf(args.state);
    if (part.type === 'error' || part.type === 'abort') {
      hold.answer = undefined;
      hold.reasoning = undefined;
      return part;
    }
    // A channel's end chunk closes its segment: flush the held tail FIRST,
    // then re-drive the end chunk — otherwise the tail would surface after
    // its end marker (or only at finish), reordering the stream for clean
    // runs.
    const endedChannel =
      part.type === 'text-end'
        ? ('answer' as const)
        : part.type === 'reasoning-end'
          ? ('reasoning' as const)
          : undefined;
    if (endedChannel) {
      await this.#evaluateHeldTail(args, endedChannel);
      return this.#flushHeld(hold[endedChannel], part, args.state) ?? part;
    }
    if (part.type !== 'finish') return part;
    for (const channel of ['answer', 'reasoning'] as const) {
      await this.#evaluateHeldTail(args, channel);
    }
    for (const channel of ['answer', 'reasoning'] as const) {
      const flush = this.#flushHeld(hold[channel], part, args.state);
      if (flush) return flush;
    }
    return part;
  }

  async #evaluateStreamChannel(
    args: ProcessOutputStreamArgs,
    channel: OutputChannel,
  ): Promise<string[]> {
    // Stream passes emit no terminal "allowed" record: per-pass records
    // would flood the audit log. processOutputResult emits one at stream end.
    // abortOnError=true: Mastra emits a chunk on a raw throw but suppresses
    // it on a TripWire, so errors abort on delta and held-tail passes.
    return this.#evaluate(
      {
        phase: 'output',
        channel,
        messages: [],
        text: channelTextsOf(args.state)[channel],
        requestContext: args.requestContext,
      },
      actorFromRequestContext(args.requestContext) ?? null,
      args.abort,
      true,
      args.state,
    );
  }

  async #evaluateHeldTail(
    args: ProcessOutputStreamArgs,
    channel: HoldableChannel,
  ): Promise<void> {
    const held = holdStateOf(args.state)[channel];
    if (!held || held.pending === '' || held.terminalEvaluated) return;
    // A pending suffix can remain below the classifier cadence when its
    // channel ends.
    const streamStates = this.#policies.map((_, index) =>
      policyStreamStateOf(args.state, index),
    );
    for (const streamState of streamStates) {
      terminalPassStreamStates.add(streamState);
    }
    try {
      await this.#evaluateStreamChannel(args, channel);
      held.terminalEvaluated = true;
    } finally {
      for (const streamState of streamStates) {
        terminalPassStreamStates.delete(streamState);
      }
    }
  }

  // Coalesce a channel's pending tail into a single delta, stash the
  // triggering part for the runner to re-drive, and return the flush —
  // undefined when nothing is pending. The stash convention is
  // core-version-coupled (REPROCESS_PART_KEY), so every flush goes through
  // this one path.
  #flushHeld(
    held: HeldChannel | undefined,
    part: ChunkType,
    state: Record<string, unknown>,
  ): ChunkType | undefined {
    if (!held || held.pending === '') return undefined;
    const flush = coalescedDelta(held.shape, held.pending);
    held.pending = '';
    state[REPROCESS_PART_KEY] = part;
    return flush;
  }

  // Runs phase- and channel-applicable policies against `context`. On denial
  // or evaluator error it records the audit event and aborts/throws (fail
  // closed), never returning past a violation. Returns the evaluated policy
  // names for the caller's terminal "allowed" record — which the streaming
  // path omits. abortOnError converts an evaluator failure into abort()
  // instead of a rethrow. Input needs it because Mastra's durable preparation
  // runs the model past an input processor's error that is not a tripwire,
  // and the stream because Mastra's stream driver emits the chunk on one (see
  // processOutputStream). The final result rethrows, which stops Mastra's
  // standard loop. During streaming, `streamAccumulator` (the processor's
  // per-request state) hands each policy a private namespace, exposed as
  // context.streamState for incremental scanning.
  async #evaluate(
    context: PolicyContext,
    actor: Actor | null,
    abort: (reason: string) => never,
    abortOnError = false,
    streamAccumulator?: Record<string, unknown>,
  ): Promise<string[]> {
    const result = await evaluatePoliciesInOrder({
      policies: this.#policies,
      context,
      actor,
      audit: this.#audit,
      resource: this.#resource,
      streamAccumulator,
    });
    if (result.outcome === 'denied') {
      abort(result.reason);
    }
    if (result.outcome === 'error') {
      if (abortOnError) abort(POLICY_EVALUATION_FAILED);
      throw result.error;
    }
    return result.evaluated;
  }

  #assertObjectChannelCoverage(args: ProcessOutputResultArgs): void {
    if (
      this.#objectOnlyPolicyNames.length === 0 ||
      args.state[OBJECT_CHANNEL_EVALUATED_STATE_KEY] === true
    ) {
      return;
    }
    const names = this.#objectOnlyPolicyNames.join(', ');
    const plural = this.#objectOnlyPolicyNames.length === 1 ? 'y' : 'ies';
    this.#audit?.record({
      actor: actorFromRequestContext(args.requestContext) ?? null,
      action: 'agent.output.policy',
      resource: this.#resource,
      decision: 'error',
      reason: 'required object output channel was not observable',
      detail: agentAuditDetail(args.requestContext, {
        policies: [...this.#objectOnlyPolicyNames],
      }),
    });
    args.abort(
      `PolicyEngine: polic${plural} [${names}] require the 'object' output channel, but this invocation exposed no object to the processor`,
    );
  }

  #abortOnNonStringText(
    args: ProcessOutputStreamArgs | ProcessOutputResultArgs,
    channel: HoldableChannel,
  ): never {
    this.#audit?.record({
      actor: actorFromRequestContext(args.requestContext) ?? null,
      action: 'agent.output.policy',
      resource: this.#resource,
      decision: 'error',
      reason: NON_STRING_OUTPUT_TEXT,
      detail: agentAuditDetail(args.requestContext, { channel }),
    });
    return args.abort(NON_STRING_OUTPUT_TEXT);
  }

  #recordMalformedAuditContext(
    requestContext: RequestContext | undefined,
    actor: Actor | null,
  ): void {
    const event = malformedAgentAuditContextEvent(
      requestContext,
      this.#resource,
      actor,
    );
    if (event) this.#audit?.record(event);
  }

  #recordAllowed(
    phase: PolicyPhase,
    actor: Actor | null,
    evaluated: string[],
    requestContext?: RequestContext,
    channels?: readonly OutputChannel[],
  ): void {
    recordAllowedPolicyDecision({
      audit: this.#audit,
      phase,
      actor,
      resource: this.#resource,
      evaluated,
      requestContext,
      channels,
    });
  }
}

/**
 * Deny when any pattern matches the gated text. Strings match as
 * case-insensitive substrings; RegExps match as-is. Gates ALL output
 * channels by default (answer, reasoning, object) — leak prevention is its
 * purpose, and a secret is no less leaked through a reasoning trace; narrow
 * with `options.channels` when a channel must stay ungated. The 'object'
 * channel is enforced only for object chunks that reach this processor; see
 * {@link OutputChannel} for the guarded structured-output limitation.
 *
 * Substring matching is plain toLowerCase — no Unicode folding or
 * normalization — so alternate spellings evade it (e.g. 'strasse' does not
 * match 'straße'). Do not rely on it alone against adversarial input.
 *
 * `patterns` must be a non-empty array of strings and RegExps. Construction
 * matches with its own copy of each RegExp, so a method later assigned to the
 * caller's RegExp does not change what the policy denies.
 */
export function denyPatterns(
  patterns: readonly (RegExp | string)[],
  options: {
    name?: string;
    phases?: readonly PolicyPhase[];
    channels?: readonly OutputChannel[];
    /**
     * Override the computed hold-back hint — e.g. a caller-known match
     * bound for a RegExp, which otherwise forces Infinity (buffer-all).
     */
    holdBackChars?: number;
  } = {},
): PolicyEvaluator {
  const compiled = readFrozenList(
    'denyPatterns: patterns',
    patterns,
    (entry, index) =>
      typeof entry === 'string'
        ? entry.toLowerCase()
        : copyRegExpEntry('denyPatterns: patterns', entry, index),
    true,
  );
  const stringPatterns = compiled.filter(
    (pattern): pattern is string => typeof pattern === 'string',
  );
  const allStrings = stringPatterns.length === compiled.length;
  // A substring match ending in newly-appended text must start within
  // maxPatternLength-1 chars before the previous scan frontier, so rescanning
  // only that window is equivalent to rescanning everything — the O(n²)
  // streaming fix.
  const maxPatternLength = stringPatterns.reduce(
    (max, pattern) => Math.max(max, pattern.length),
    0,
  );
  // Zero-leak hint: a string match straddling the emission frontier spans at
  // most maxPatternLength-1 already-held chars; an arbitrary RegExp match is
  // unbounded, so any RegExp forces Infinity unless the caller overrides.
  const holdBackChars = readHoldBackChars(
    'denyPatterns: holdBackChars',
    options.holdBackChars ??
      (allStrings
        ? Math.max(0, maxPatternLength - 1)
        : Number.POSITIVE_INFINITY),
  );
  return {
    name: options.name ?? 'deny-patterns',
    phases: options.phases,
    channels: options.channels ?? ['answer', 'reasoning', 'object'],
    holdBackChars,
    evaluate({ text, channel, streamState }): PolicyDecision {
      assertPolicyText('denyPatterns', text);
      // Incremental streaming scan — string patterns only (an arbitrary
      // regex has no bounded lookbehind window, so any RegExp forces a full
      // scan per chunk), and never for the object channel, whose text is a
      // REPLACED snapshot, not append-only.
      if (allStrings && streamState && channel !== 'object') {
        const cursorKey = `scannedUpTo:${channel}`;
        const cursor = streamState[cursorKey];
        const scannedUpTo = typeof cursor === 'number' ? cursor : 0;
        if (text.length <= scannedUpTo) return { allowed: true };
        const window = text
          .slice(Math.max(0, scannedUpTo - (maxPatternLength - 1)))
          .toLowerCase();
        for (const pattern of stringPatterns) {
          if (window.includes(pattern)) {
            return {
              allowed: false,
              reason: `matched blocked pattern ${pattern}`,
            };
          }
        }
        streamState[cursorKey] = text.length;
        return { allowed: true };
      }
      const lower = text.toLowerCase();
      for (const pattern of compiled) {
        const matched =
          typeof pattern === 'string'
            ? lower.includes(pattern)
            : pattern.test(text);
        if (matched) {
          return {
            allowed: false,
            reason: `matched blocked pattern ${String(pattern)}`,
          };
        }
      }
      return { allowed: true };
    },
  };
}

const MAX_TEXT_LENGTH_OPTION_KEYS = {
  name: true,
  phases: true,
  channels: true,
} satisfies Record<
  keyof NonNullable<Parameters<typeof maxTextLength>[1]>,
  true
>;

/**
 * Deny when the gated text exceeds maxChars, a finite number of at least 0.
 * Defaults to the output phase and the answer channel — reasoning does NOT
 * count toward an answer cap.
 * Cap another channel with an explicit second instance, e.g.
 * `maxTextLength(50_000, { channels: ['reasoning'] })`.
 */
export function maxTextLength(
  maxChars: number,
  options: {
    name?: string;
    phases?: readonly PolicyPhase[];
    channels?: readonly OutputChannel[];
  } = {},
): PolicyEvaluator {
  // An infinite cap never denies.
  readNumberInRange(
    'maxTextLength: maxChars',
    maxChars,
    (value) => Number.isFinite(value) && value >= 0,
    'a finite number of at least 0',
  );
  assertKnownFields(
    'maxTextLength: options',
    options,
    MAX_TEXT_LENGTH_OPTION_KEYS,
  );
  return {
    name: options.name ?? 'max-text-length',
    phases: options.phases ?? ['output'],
    channels: options.channels ?? DEFAULT_CHANNELS,
    // A length violation completes on the current chunk — no straddle window.
    holdBackChars: 0,
    evaluate({ text }): PolicyDecision {
      assertPolicyText('maxTextLength', text);
      return text.length <= maxChars
        ? { allowed: true }
        : {
            allowed: false,
            reason: `text length ${text.length} exceeds limit ${maxChars}`,
          };
    },
  };
}

export type {
  ClassifierPolicyOptions,
  PiiSecretsDetectorId,
  PiiSecretsOptions,
} from './content-inspection.js';
// Agent-boundary content-inspection evaluators — see content-inspection.ts.
export {
  classifierPolicy,
  PII_SECRETS_DETECTOR_IDS,
  piiSecrets,
} from './content-inspection.js';
export type {
  BackgroundExecutionOptions,
  CrossWorkflowIsolationOptions,
  NetworkEgressOptions,
  PolicyDecision,
  SideEffect,
  ToolCallContext,
  ToolPolicyEvaluator,
  WritePermissionsPolicy,
} from './tool-policy.js';
// Tool-boundary policies — evaluated by the connector SDK's execute
// wrapper, not this processor. See tool-policy.ts. PolicyDecision lives
// there, in the leaf module.
export {
  approvalRequired,
  backgroundExecution,
  crossWorkflowIsolation,
  egressDomainAllowed,
  ISOLATION_SCOPE_CONTEXT_KEY,
  LLM_BACKGROUND_OVERRIDE_KEY,
  networkEgress,
  tenantIsolation,
  WORKFLOW_SCOPE_CONTEXT_KEY,
} from './tool-policy.js';
