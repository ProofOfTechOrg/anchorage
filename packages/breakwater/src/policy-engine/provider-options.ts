// SPDX-License-Identifier: Apache-2.0

/** @internal */
export function isPlainRecord(
  value: unknown,
): value is Readonly<Record<string, unknown>> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isPlainObject(
  value: unknown,
): value is Readonly<Record<string, unknown>> {
  if (!isPlainRecord(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

// JSON text of a provider-option value without the named keys, at any depth.
function jsonWithoutKeys(
  ...keys: readonly string[]
): (value: unknown) => readonly unknown[] {
  return (value) => [
    JSON.stringify(value, (key, member: unknown) =>
      keys.includes(key) ? undefined : member,
    ),
  ];
}

type ProviderOptionRule = 'refuse' | ((value: unknown) => readonly unknown[]);

// Provider options that a model adapter bundled with the supported
// `@mastra/core` renders into the request as message content, role, text,
// tool input or tool output. What no genuine stored replay carries is refused;
// what a provider's responses store on the parts Mastra replays is read, less
// its signatures, ids and encrypted values. Every other namespace and key is
// replay metadata, such as signatures, item ids and cache control, and is not
// read: an entropy detector would deny the replays that carry it.
const PROVIDER_CONTENT_OPTIONS: Readonly<
  Record<string, 'refuse' | Readonly<Record<string, ProviderOptionRule>>>
> = {
  openaiCompatible: 'refuse',
  anthropic: {
    title: 'refuse',
    context: 'refuse',
    citations: jsonWithoutKeys('encrypted_index'),
  },
  openrouter: {
    filename: 'refuse',
    annotations: 'refuse',
    reasoning_details: jsonWithoutKeys('data', 'signature', 'id'),
  },
};

/**
 * The text a provider-options object carries to the model, or undefined when
 * it holds an entry this module refuses or a value that is not an object.
 * @internal
 */
export function providerOptionValues(
  options: unknown,
): readonly unknown[] | undefined {
  if (options === undefined) return [];
  if (!isPlainRecord(options)) return undefined;
  const values: unknown[] = [];
  for (const [namespace, entries] of Object.entries(options)) {
    if (entries === undefined) continue;
    const rules = Object.hasOwn(PROVIDER_CONTENT_OPTIONS, namespace)
      ? PROVIDER_CONTENT_OPTIONS[namespace]
      : undefined;
    if (rules === 'refuse' || !isPlainRecord(entries)) return undefined;
    if (rules === undefined) continue;
    for (const [key, value] of Object.entries(entries)) {
      if (value === undefined || !Object.hasOwn(rules, key)) continue;
      const rule = rules[key];
      if (rule === 'refuse') return undefined;
      if (rule !== undefined) values.push(...rule(value));
    }
  }
  return values;
}

// Call-level options are never stored replay. Bundled adapters spread some
// namespaces into the request body; the openai-compatible adapter does so for
// any provider id or model with a custom URL, so only listed settings pass.
const OPENAI_CALL_OPTIONS = [
  'reasoningEffort',
  'reasoningSummary',
  'reasoningMode',
  'textVerbosity',
  'serviceTier',
  'parallelToolCalls',
  'maxToolCalls',
  'maxCompletionTokens',
  'logprobs',
  'store',
  'strictJsonSchema',
  'truncation',
  'include',
  'promptCacheKey',
  'promptCacheRetention',
  'promptCacheOptions',
  'forceReasoning',
] as const;

const GOOGLE_CALL_OPTIONS = [
  'thinkingConfig',
  'responseModalities',
  'mediaResolution',
  'audioTimestamp',
  'structuredOutputs',
  'streamFunctionCallArguments',
  'serviceTier',
  'sharedRequestType',
  'requestType',
] as const;

const CALL_PROVIDER_OPTIONS: Readonly<Record<string, readonly string[]>> = {
  mastra: ['schedule'],
  openai: OPENAI_CALL_OPTIONS,
  azure: OPENAI_CALL_OPTIONS,
  anthropic: [
    'thinking',
    'effort',
    'sendReasoning',
    'structuredOutputMode',
    'disableParallelToolUse',
    'cacheControl',
    'toolStreaming',
    'taskBudget',
    'speed',
    'inferenceGeo',
  ],
  google: GOOGLE_CALL_OPTIONS,
  vertex: GOOGLE_CALL_OPTIONS,
  xai: [
    'reasoningEffort',
    'logprobs',
    'topLogprobs',
    'store',
    'include',
    'parallel_function_calling',
  ],
  mistral: [
    'safePrompt',
    'documentImageLimit',
    'documentPageLimit',
    'structuredOutputs',
    'strictJsonSchema',
    'parallelToolCalls',
    'reasoningEffort',
  ],
  groq: [
    'reasoningFormat',
    'reasoningEffort',
    'parallelToolCalls',
    'structuredOutputs',
    'strictJsonSchema',
    'serviceTier',
  ],
  deepseek: ['thinking', 'reasoningEffort', 'strictJsonSchema'],
  alibaba: ['enableThinking', 'thinkingBudget', 'parallelToolCalls'],
};

/**
 * Accept only a closed list of call-level generation-setting namespaces and
 * keys, and refuse everything else. Omitted options and entries with
 * `undefined` values are accepted.
 */
export function assertAcceptedCallProviderOptions(
  providerOptions: unknown,
): void {
  if (providerOptions === undefined) return;
  if (!isPlainObject(providerOptions)) {
    throw new TypeError('GuardedAgent: providerOptions must be a plain object');
  }
  for (const [namespace, entries] of Object.entries(providerOptions)) {
    if (entries === undefined) continue;
    if (!Object.hasOwn(CALL_PROVIDER_OPTIONS, namespace)) {
      throw new TypeError(
        `GuardedAgent: providerOptions namespace ${JSON.stringify(namespace)} is not accepted`,
      );
    }
    if (!isPlainObject(entries)) {
      throw new TypeError(
        `GuardedAgent: providerOptions namespace ${JSON.stringify(namespace)} must be a plain object`,
      );
    }
    const allowed = CALL_PROVIDER_OPTIONS[namespace] ?? [];
    for (const [key, value] of Object.entries(entries)) {
      if (value === undefined) continue;
      if (!allowed.includes(key)) {
        throw new TypeError(
          `GuardedAgent: providerOptions ${JSON.stringify(namespace)} key ${JSON.stringify(key)} is not accepted`,
        );
      }
    }
  }
}

/**
 * Tell a host whether message-level provider options contain text an input
 * policy reads or options it refuses. Hosts use this when those options travel
 * as a message, such as a schedule signal, where no input policy runs.
 */
export function providerOptionsCarryContent(providerOptions: unknown): boolean {
  const values = providerOptionValues(providerOptions);
  return values === undefined || values.length > 0;
}
