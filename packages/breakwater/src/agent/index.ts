// SPDX-License-Identifier: Apache-2.0

import {
  Agent,
  type AgentConfig,
  type AgentExecutionOptionsBase,
  MessageList,
  type ToolsInput,
  TripWire,
} from '@mastra/core/agent';
import type {
  MastraDBMessage,
  MessageListInput,
} from '@mastra/core/agent/message-list';
import type {
  InputProcessor,
  InputProcessorOrWorkflow,
  OutputProcessorOrWorkflow,
  ProcessInputArgs,
  ProcessInputResult,
  Processor,
} from '@mastra/core/processors';
import { RequestContext } from '@mastra/core/request-context';
import type { FullOutput, MastraModelOutput } from '@mastra/core/stream';

import { type AuditLogger, agentAuditDetail } from '../audit/index.js';
import { assertKnownFields, readFrozenList } from '../host-input.js';
import { stopWithoutCallMessages } from '../input-refusal.js';
import { PolicyEngine, type PolicyEvaluator } from '../policy-engine/index.js';
import {
  inputAssetUrls,
  UNCLASSIFIED_INPUT_CONTENT,
} from '../policy-engine/prompt-media.js';
import {
  type PromptSnapshot,
  recordProcessorAdditions,
  snapshotPromptMessages,
} from '../processor-additions.js';
import { authorizeActor } from '../rbac/authorize.js';
import {
  actorFromRequestContext,
  RBACMiddleware,
  type Role,
} from '../rbac/index.js';
import { assertPrincipalKinds, type PrincipalKind } from '../rbac/principal.js';
import { readAllowedRoles } from '../rbac/roles.js';
import {
  CLIENT_TOOL_OUTPUT_PROCESSOR_ID,
  clientToolOutputProcessor,
} from './client-tool-output.js';

const RESERVED_PROCESSOR_IDS = new Set([
  'breakwater-rbac',
  'breakwater-input-assets',
  CLIENT_TOOL_OUTPUT_PROCESSOR_ID,
  'breakwater-policy-engine',
]);

const INPUT_ASSET_SCHEME_DENIED = 'input asset URL scheme is not allowed';
const INPUT_ASSET_CREDENTIALS_DENIED =
  'input asset URL credentials are not allowed';
const INPUT_ASSET_ORIGIN_DENIED = 'input asset URL origin is not allowed';
/** The reason a call stops when an input processor fails. */
const INPUT_PROCESSOR_FAILED = 'input processor failed';

// Exhaustive over GuardedAgentCallOptions: a member the interface gains is a
// missing property here, one it drops an excess property.
const GUARDED_CALL_OPTION_KEY_SET: Record<keyof GuardedAgentCallOptions, true> =
  {
    requestContext: true,
    runId: true,
    memory: true,
    abortSignal: true,
  };

const GUARDED_CALL_OPTION_KEYS = new Set(
  Object.keys(GUARDED_CALL_OPTION_KEY_SET),
);

/** Well-known inter-package key for guarded host compatibility metadata. */
export const GUARDED_AGENT_HOST_PROTOCOL = Symbol.for(
  '@proofoftech/breakwater/guarded-agent-host/v1',
);

/** Runtime metadata consumed by hosts that cannot call the narrow handle. */
export interface GuardedAgentHostProtocol {
  readonly version: 1;
  readonly supportsDurableStructuredOutput: false;
}

const UNSAFE_CONSTRUCTION_KEYS = new Set([
  'agent',
  'inputProcessors',
  'outputProcessors',
  'errorProcessors',
  'maxProcessorRetries',
  'defaultGenerateOptionsLegacy',
  'defaultStreamOptionsLegacy',
  'defaultOptions',
  'defaultNetworkOptions',
  'backgroundTasks',
  'channels',
  'durable',
  'goal',
  'signals',
  'editor',
  'rawConfig',
]);

// The hooks of core's Processor the guarded surfaces reason about. The
// forbidden sets below are derived from it, so a name dropped here is an excess
// property there.
type ProcessorHook = Extract<
  keyof Processor,
  | 'processInput'
  | 'processInputStep'
  | 'computeStateSignal'
  | 'processLLMRequest'
  | 'processLLMResponse'
  | 'processOutputStream'
  | 'processOutputResult'
  | 'processOutputStep'
  | 'processAPIError'
>;

// The hooks an application input processor must implement.
const INPUT_PROCESSOR_REQUIRED_HOOKS = ['processInput'] as const;

// The hooks an application output processor must implement, in the order the
// validator reports a missing one.
const OUTPUT_PROCESSOR_REQUIRED_HOOKS = [
  'processOutputStream',
  'processOutputResult',
] as const;

// The hooks an application input processor may not implement: ProcessorHook
// without INPUT_PROCESSOR_REQUIRED_HOOKS.
type InputProcessorForbiddenHook = Exclude<
  ProcessorHook,
  (typeof INPUT_PROCESSOR_REQUIRED_HOOKS)[number]
>;

// The hooks an application output processor may not implement: ProcessorHook
// without OUTPUT_PROCESSOR_REQUIRED_HOOKS.
type OutputProcessorForbiddenHook = Exclude<
  ProcessorHook,
  (typeof OUTPUT_PROCESSOR_REQUIRED_HOOKS)[number]
>;

// Exhaustive over InputProcessorForbiddenHook: a hook ProcessorHook gains is a
// missing property here, one it drops an excess property. `Object.keys`
// preserves the literal's insertion order, so the order written here is the
// order checked, and a processor implementing several is refused for the first.
const INPUT_PROCESSOR_FORBIDDEN_HOOK_SET: Record<
  InputProcessorForbiddenHook,
  true
> = {
  processInputStep: true,
  computeStateSignal: true,
  processLLMRequest: true,
  processLLMResponse: true,
  processOutputStream: true,
  processOutputResult: true,
  processOutputStep: true,
  processAPIError: true,
};

const INPUT_PROCESSOR_FORBIDDEN_HOOKS = Object.keys(
  INPUT_PROCESSOR_FORBIDDEN_HOOK_SET,
) as readonly InputProcessorForbiddenHook[];

// Exhaustive over OutputProcessorForbiddenHook on the same terms.
const OUTPUT_PROCESSOR_FORBIDDEN_HOOK_SET: Record<
  OutputProcessorForbiddenHook,
  true
> = {
  processInput: true,
  processInputStep: true,
  computeStateSignal: true,
  processLLMRequest: true,
  processLLMResponse: true,
  processOutputStep: true,
  processAPIError: true,
};

const OUTPUT_PROCESSOR_FORBIDDEN_HOOKS = Object.keys(
  OUTPUT_PROCESSOR_FORBIDDEN_HOOK_SET,
) as readonly OutputProcessorForbiddenHook[];

type ProcessorMember = Exclude<keyof Processor, ProcessorHook>;

// Exhaustive over ProcessorMember: whether the application input processor
// wrapper carries each member from the processor it wraps. A member a Mastra
// release adds is a missing property here until it is classified.
const PROCESSOR_MEMBER_FORWARDING = {
  id: 'forward',
  name: 'forward',
  description: 'forward',
  // Mastra reads it from every configured input processor to decide whether
  // to add its own skill processor.
  providesSkillDiscovery: 'forward',
  spanType: 'forward',
  spanName: 'forward',
  spanAttributes: 'forward',
  onViolation: 'forward',
  __registerMastra: 'forward',
  // An output-phase hook, which Mastra never runs on an input processor.
  processToolResult: 'omit',
  // Read for output streams only.
  processDataParts: 'omit',
  // Read with computeStateSignal only, which validation refuses.
  stateId: 'omit',
  // Mastra writes it onto the processor object it runs.
  processorIndex: 'omit',
} satisfies Record<ProcessorMember, 'forward' | 'omit'>;

const FORWARDED_PROCESSOR_MEMBERS = (
  Object.keys(PROCESSOR_MEMBER_FORWARDING) as ProcessorMember[]
).filter((member) => PROCESSOR_MEMBER_FORWARDING[member] === 'forward');

// Forwarded members Mastra calls as methods of the object it holds, so they
// run bound to the wrapped processor. Mastra calls a function-valued span
// member unbound, so those are copied as they are.
const BOUND_PROCESSOR_MEMBERS: ReadonlySet<ProcessorMember> = new Set([
  'onViolation',
  '__registerMastra',
]);

/**
 * Application input processor accepted by {@link createGuardedAgent}.
 *
 * It can transform or reject the initial input only. Per-step, provider,
 * output, and error hooks are unavailable because they can mutate execution
 * after the mandatory input gates have run.
 *
 * Its return value is applied to the call's message list as Mastra's durable
 * runner applies one, on every loop. A processor that throws other than
 * through its `abort` or a `TripWire`, or returns a value that cannot be
 * applied, stops the call with one `agent.input.processor` error event and
 * the reason `input processor failed`.
 *
 * The input policies read what the processor adds to the prompt, or changes
 * in it, outside the call's input, as they read the input: system messages
 * with their provider options, and messages of every other source, a history
 * message it rewrites through any source included. A system message it adds
 * or changes must hold text alone in data properties, and what it adds or
 * changes must be a value `structuredClone` can copy, or the call stops as
 * above.
 *
 * A call that RBAC, an input policy or an application input processor
 * refuses has its input, its response messages, and every other non-system
 * message its application input processors added or changed, the refusing
 * processor's own included, removed from Mastra's message list before it
 * stops. It saves none of them to memory, Mastra's durable loop generates no
 * thread title from them, and on `generate()` and `stream()` the result's
 * `messages` and `rememberedMessages` omit them, a history message such a
 * processor changed included.
 */
export interface GuardedInputProcessor {
  readonly id: string;
  readonly name?: string;
  readonly description?: string;
  processDataParts?: boolean;
  onViolation?: Processor['onViolation'];
  processInput: (args: ProcessInputArgs) =>
    | ProcessInputResult
    | null
    | undefined
    | void
    // biome-ignore lint/suspicious/noConfusingVoidType: an async processor with no `return` resolves to void, which the wrapper applies as nothing.
    | Promise<ProcessInputResult | null | undefined | void>;
  processInputStep?: never;
  computeStateSignal?: never;
  processLLMRequest?: never;
  processLLMResponse?: never;
  processOutputStream?: never;
  processOutputResult?: never;
  processOutputStep?: never;
  processAPIError?: never;
}

/**
 * Application output processor accepted by {@link createGuardedAgent}.
 *
 * Both stream and final-result hooks are required so the processor enforces
 * the same rule on `stream()` and `generate()`.
 */
export interface GuardedOutputProcessor {
  readonly id: string;
  readonly name?: string;
  readonly description?: string;
  processDataParts?: boolean;
  onViolation?: Processor['onViolation'];
  processInput?: never;
  processInputStep?: never;
  computeStateSignal?: never;
  processLLMRequest?: never;
  processLLMResponse?: never;
  processOutputStream: NonNullable<Processor['processOutputStream']>;
  processOutputResult: NonNullable<Processor['processOutputResult']>;
  processOutputStep?: never;
  processAPIError?: never;
}

/** Fixed tool-selection behavior for every guarded execution. */
export type GuardedToolChoice = NonNullable<
  AgentExecutionOptionsBase<undefined>['toolChoice']
>;

/**
 * Construction-time configuration for a guarded Mastra agent.
 *
 * Processor and execution defaults are replaced by dedicated validated
 * fields. The factory never accepts an existing raw `Agent`.
 */
export type GuardedAgentConfig<
  TAgentId extends string = string,
  TTools extends ToolsInput = ToolsInput,
  TRequestContext extends Record<string, unknown> | unknown = unknown,
> = Omit<
  AgentConfig<TAgentId, TTools, undefined, TRequestContext, false>,
  | 'inputProcessors'
  | 'outputProcessors'
  | 'errorProcessors'
  | 'maxProcessorRetries'
  | 'defaultGenerateOptionsLegacy'
  | 'defaultStreamOptionsLegacy'
  | 'defaultOptions'
  | 'defaultNetworkOptions'
  | 'backgroundTasks'
  | 'channels'
  | 'durable'
  | 'goal'
  | 'signals'
  | 'editor'
  | 'rawConfig'
> & {
  /** Exact actor roles authorized for direct and durable execution. */
  allowedRoles: readonly Role[];
  /**
   * Exact principal kinds authorized for direct and durable execution.
   * Defaults to `['human']`: an agent that does not name its automation denies
   * every scheduled, signal, service, and agent-delegated entry.
   */
  allowedPrincipalKinds?: readonly PrincipalKind[];
  /**
   * Origins user file and image parts may name as network URLs, regardless of
   * who fetches them. Absent or empty refuses all network URLs; data: URLs
   * are inline data. URLs in mapped client tool results are not checked.
   * This is independent of connector egress allowlists.
   */
  allowedInputAssetOrigins?: readonly string[];
  /** Mandatory input and output policies, evaluated in array order. */
  policies: readonly PolicyEvaluator[];
  /** Required failure-isolated audit logger for every mandatory gate. */
  audit: AuditLogger;
  /** Fixed positive step budget for every execution. */
  maxSteps: number;
  /** Fixed tool-selection behavior for every execution. */
  toolChoice: GuardedToolChoice;
  /** Initial-input-only application processors. */
  applicationInputProcessors?: readonly GuardedInputProcessor[];
  /** Stream-and-result application output processors. */
  applicationOutputProcessors?: readonly GuardedOutputProcessor[];
};

// Every GuardedAgentConfig key: Breakwater's own options, then the keys it
// keeps from Mastra's AgentConfig. Mastra ignores a key its config does not
// declare, so a misspelled one would drop what the host configured; a key
// Mastra adds or removes fails to compile here.
const GUARDED_AGENT_CONFIG_KEYS = {
  allowedRoles: true,
  allowedPrincipalKinds: true,
  allowedInputAssetOrigins: true,
  policies: true,
  audit: true,
  maxSteps: true,
  toolChoice: true,
  applicationInputProcessors: true,
  applicationOutputProcessors: true,
  id: true,
  name: true,
  description: true,
  metadata: true,
  instructions: true,
  model: true,
  maxRetries: true,
  tools: true,
  hooks: true,
  workflows: true,
  mastra: true,
  pubsub: true,
  agents: true,
  scorers: true,
  memory: true,
  skills: true,
  skillsFormat: true,
  browser: true,
  voice: true,
  workspace: true,
  options: true,
  requestContextSchema: true,
  notifications: true,
  transform: true,
} satisfies Record<keyof GuardedAgentConfig, true>;

/** The only call options accepted by a guarded agent handle. */
export interface GuardedAgentCallOptions {
  /** Trusted context containing the authenticated actor and host correlation. */
  requestContext: RequestContext;
  /** Optional host-minted run identifier. */
  runId?: string;
  /**
   * Optional memory binding: the thread, as its id or an object holding only
   * its id, and the resource that owns it. Memory configuration and thread
   * fields such as metadata belong on the agent's `Memory`.
   */
  memory?: {
    readonly thread: string | { readonly id: string };
    readonly resource: string;
  };
  /** Optional caller cancellation signal. */
  abortSignal?: AbortSignal;
}

/**
 * Narrow in-process API for a guarded agent.
 *
 * This handle prevents accidental access to raw Mastra execution methods. It
 * is not a sandbox against hostile code running in the same JavaScript
 * process.
 */
export interface GuardedAgentHandle {
  /** Agent identifier used by catalogs and audit resources. */
  readonly id: string;
  /** Exact role allowlist enforced at every guarded entry. */
  readonly allowedRoles: readonly Role[];
  /** Exact principal-kind allowlist enforced at every guarded entry. */
  readonly allowedPrincipalKinds: readonly PrincipalKind[];
  /** Fixed maximum execution steps. */
  readonly maxSteps: number;
  /** Host compatibility metadata; not an alternate execution surface. */
  readonly [GUARDED_AGENT_HOST_PROTOCOL]: GuardedAgentHostProtocol;

  /** Generate one unstructured result through all mandatory gates. */
  generate(
    messages: MessageListInput,
    options: GuardedAgentCallOptions,
  ): Promise<FullOutput<undefined>>;

  /** Stream one unstructured result through all mandatory gates. */
  stream(
    messages: MessageListInput,
    options: GuardedAgentCallOptions,
  ): Promise<MastraModelOutput<undefined>>;
}

const guardedAgentHandles = new WeakSet<object>();

/** Return whether `value` was created by this package's guarded factory. */
export function isGuardedAgentHandle(
  value: unknown,
): value is GuardedAgentHandle {
  return (
    (typeof value === 'object' || typeof value === 'function') &&
    value !== null &&
    guardedAgentHandles.has(value)
  );
}

function assertConstructionOptions(options: object): void {
  for (const key of Reflect.ownKeys(options)) {
    if (typeof key === 'string' && UNSAFE_CONSTRUCTION_KEYS.has(key)) {
      throw new TypeError(
        `createGuardedAgent: construction option '${key}' is not allowed`,
      );
    }
  }
}

function readAllowedInputAssetOrigins(value: unknown): readonly string[] {
  if (value === undefined) return Object.freeze([]);
  const subject = 'createGuardedAgent: allowedInputAssetOrigins';
  return readFrozenList(subject, value, (entry, index) => {
    const prefix = `${subject}[${index}]`;
    if (typeof entry !== 'string') {
      throw new TypeError(`${prefix} must be a string`);
    }
    if (entry.includes('*')) {
      throw new TypeError(
        `${prefix}: wildcards are not supported; list each origin`,
      );
    }
    if (/[@?#]/.test(entry)) {
      throw new TypeError(
        `${prefix} must not contain userinfo, query, or fragment`,
      );
    }
    let url: URL;
    try {
      url = new URL(entry);
    } catch {
      throw new TypeError(`${prefix} must be a URL origin`);
    }
    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      throw new TypeError(`${prefix} must use http or https`);
    }
    if (entry.includes('\\') || !/^https?:\/\/[^/]+\/?$/i.test(entry)) {
      throw new TypeError(`${prefix} must be an origin without a path`);
    }
    return url.origin;
  });
}

// Mastra downloads user asset URLs after input processors, including history
// on every call (message-list-DCUwKHqe.js:11191-11226). Its durable runner
// skips a non-tripwire processor error (create-durable-agent-DFHwqN2K.js:1336-1342).
function inputAssetProcessor(
  origins: readonly string[],
  resource: string,
  audit: AuditLogger,
): InputProcessor {
  const allowed = new Set(origins);
  return {
    id: 'breakwater-input-assets',
    processInput(args) {
      let reason: string | undefined;
      let decision: 'denied' | 'error' = 'denied';
      try {
        for (const url of inputAssetUrls(args.messageList.get.all.db())) {
          if (url.protocol !== 'http:' && url.protocol !== 'https:') {
            reason = INPUT_ASSET_SCHEME_DENIED;
          } else if (url.username || url.password) {
            reason = INPUT_ASSET_CREDENTIALS_DENIED;
          } else if (!allowed.has(url.origin)) {
            reason = INPUT_ASSET_ORIGIN_DENIED;
          }
          if (reason !== undefined) break;
        }
      } catch {
        // An unreadable asset prompt has the same audit result as unreadable policy input.
        reason = UNCLASSIFIED_INPUT_CONTENT;
        decision = 'error';
      }
      const refusalReason = reason;
      if (refusalReason === undefined) return args.messageList;
      try {
        audit.record({
          actor: actorFromRequestContext(args.requestContext) ?? null,
          action:
            decision === 'error' ? 'agent.input.policy' : 'agent.input.asset',
          resource,
          decision,
          reason: refusalReason,
          detail: agentAuditDetail(args.requestContext),
        });
      } finally {
        stopWithoutCallMessages(args.messageList, {}, () =>
          args.abort(refusalReason),
        );
      }
    },
  };
}

function assertToolChoice(toolChoice: GuardedToolChoice): GuardedToolChoice {
  if (
    toolChoice === 'auto' ||
    toolChoice === 'none' ||
    toolChoice === 'required'
  ) {
    return toolChoice;
  }
  if (
    !toolChoice ||
    typeof toolChoice !== 'object' ||
    Object.getPrototypeOf(toolChoice) !== Object.prototype ||
    Reflect.ownKeys(toolChoice).some(
      (key) => key !== 'type' && key !== 'toolName',
    ) ||
    toolChoice.type !== 'tool' ||
    typeof toolChoice.toolName !== 'string' ||
    toolChoice.toolName.length === 0
  ) {
    throw new TypeError(
      'createGuardedAgent: toolChoice must be auto, none, required, or one named tool',
    );
  }
  return Object.freeze({
    type: 'tool' as const,
    toolName: toolChoice.toolName,
  });
}

function assertProcessorId(processor: Processor, kind: string): void {
  if (typeof processor.id !== 'string' || processor.id.length === 0) {
    throw new TypeError(
      `createGuardedAgent: ${kind} processor id must be non-empty`,
    );
  }
  if (RESERVED_PROCESSOR_IDS.has(processor.id)) {
    throw new TypeError(
      `createGuardedAgent: application processor id '${processor.id}' is reserved`,
    );
  }
}

function hasHook(processor: Processor, hook: ProcessorHook): boolean {
  return typeof processor[hook] === 'function';
}

function validateInputProcessors(
  processors: readonly GuardedInputProcessor[],
): readonly GuardedInputProcessor[] {
  if (!Array.isArray(processors)) {
    throw new TypeError(
      'createGuardedAgent: applicationInputProcessors must be an array',
    );
  }
  for (const processor of processors) {
    if (!processor || typeof processor !== 'object') {
      throw new TypeError(
        'createGuardedAgent: application input processors must be processor objects',
      );
    }
    assertProcessorId(processor, 'application input');
    for (const hook of INPUT_PROCESSOR_REQUIRED_HOOKS) {
      if (!hasHook(processor, hook)) {
        throw new TypeError(
          `createGuardedAgent: input processor '${processor.id}' must implement ${hook}`,
        );
      }
    }
    for (const hook of INPUT_PROCESSOR_FORBIDDEN_HOOKS) {
      if (hasHook(processor, hook)) {
        throw new TypeError(
          `createGuardedAgent: input processor '${processor.id}' must not implement ${hook}`,
        );
      }
    }
  }
  return Object.freeze([...processors]);
}

const INPUT_RESULT_NOT_APPLICABLE =
  'an application input processor returned a value that cannot be applied';

function isObjectValue(value: unknown): value is object {
  return (
    (typeof value === 'object' && value !== null) || typeof value === 'function'
  );
}

type MessageSourceChecker = ReturnType<MessageList['makeMessageSourceChecker']>;

function applyInputMessages(
  messageList: MessageList,
  messages: readonly MastraDBMessage[],
  before: readonly string[],
  check: MessageSourceChecker,
): void {
  const omitted = before.filter(
    (id) => !messages.some((message) => message.id === id),
  );
  if (omitted.length > 0) messageList.removeByIds(omitted);
  const systemEntries = messages.filter(({ role }) => role === 'system');
  const otherEntries = messages.filter(({ role }) => role !== 'system');
  for (const entry of systemEntries) {
    messageList.addSystem(
      entry.content.content ??
        entry.content.parts
          ?.map((part) => (part.type === 'text' ? part.text : ''))
          .join('\n') ??
        '',
    );
  }
  for (const entry of otherEntries) {
    messageList.removeByIds([entry.id]);
    messageList.add(entry, check.getSource(entry) ?? 'input', {
      merge: false,
    });
  }
}

// Applies an input processor's return value to the call's message list with
// the steps of Mastra's durable runner, which keeps a system entry's id in the
// input. Mastra's standard loop removes that id, so a processor returning its
// input as system entries would leave the input policies an empty input.
// `before` holds the ids of the processor's `messages`, which on the standard
// loop include the thread history, as the standard loop's own deletion does.
function applyInputResult(
  messageList: MessageList,
  result: unknown,
  before: readonly string[],
  check: MessageSourceChecker,
): void {
  if (!result || result === messageList) return;
  if (result instanceof MessageList || typeof result !== 'object') {
    throw new TypeError(INPUT_RESULT_NOT_APPLICABLE);
  }
  if (Array.isArray(result)) {
    applyInputMessages(messageList, result, before, check);
    return;
  }
  const { messages, systemMessages } = result as {
    messages?: unknown;
    systemMessages?: unknown;
  };
  if (!Array.isArray(messages) || !Array.isArray(systemMessages)) {
    throw new TypeError(INPUT_RESULT_NOT_APPLICABLE);
  }
  messageList.replaceAllSystemMessages(systemMessages);
  applyInputMessages(messageList, messages, before, check);
}

// The members of `inner` that the wrapper carries.
function forwardedMembers(
  inner: GuardedInputProcessor,
): Record<string, unknown> {
  const members: Record<string, unknown> = {};
  for (const member of FORWARDED_PROCESSOR_MEMBERS) {
    const value: unknown = (inner as Processor)[member];
    if (value === undefined) continue;
    if (!BOUND_PROCESSOR_MEMBERS.has(member)) {
      members[member] = value;
    } else if (typeof value === 'function') {
      members[member] = value.bind(inner);
    }
  }
  return members;
}

function failInputProcessor(
  args: ProcessInputArgs,
  resource: string,
  audit: AuditLogger,
  processorId: string,
  snapshot?: PromptSnapshot,
): never {
  try {
    audit.record({
      actor: actorFromRequestContext(args.requestContext) ?? null,
      action: 'agent.input.processor',
      resource,
      decision: 'error',
      reason: INPUT_PROCESSOR_FAILED,
      detail: agentAuditDetail(args.requestContext, { processor: processorId }),
    });
  } finally {
    stopWithoutCallMessages(args.messageList, { snapshot }, () =>
      args.abort(INPUT_PROCESSOR_FAILED),
    );
  }
}

// Mastra's durable preparation logs an input processor's error, unless it is
// a tripwire, and runs the model past every later processor, the policy
// engine included. The wrapper applies the processor's return value itself
// and returns the list, so Mastra applies nothing after it, and turns any
// error other than the processor's own abort or tripwire into an audited
// abort. Either way the refusal drops the call's messages, those the
// processor added or changed before it failed included. What the processor
// adds or changes outside the input is recorded for the policy engine. The
// wrapper stays extensible: Mastra writes `processorIndex` onto it.
function guardInputProcessor(
  inner: GuardedInputProcessor,
  resource: string,
  audit: AuditLogger,
): InputProcessor {
  return {
    ...forwardedMembers(inner),
    async processInput(args: ProcessInputArgs): Promise<ProcessInputResult> {
      // Every error the forwarded abort threw, so a processor that aborts
      // twice is still recognised.
      const abortErrors = new WeakSet<object>();
      const abort: ProcessInputArgs['abort'] = (reason, options) => {
        try {
          return args.abort(reason, options);
        } catch (error) {
          if (isObjectValue(error)) abortErrors.add(error);
          throw error;
        }
      };
      let snapshot: PromptSnapshot | undefined;
      try {
        const { messageList } = args;
        const inputIds = args.messages.map(({ id }) => id);
        const check = messageList.makeMessageSourceChecker();
        snapshot = snapshotPromptMessages(messageList);
        const result: unknown = await inner.processInput({ ...args, abort });
        applyInputResult(messageList, result, inputIds, check);
        recordProcessorAdditions(messageList, snapshot);
      } catch (error) {
        if (
          (isObjectValue(error) && abortErrors.has(error)) ||
          error instanceof TripWire
        ) {
          return stopWithoutCallMessages(args.messageList, { snapshot }, () => {
            throw error;
          });
        }
        return failInputProcessor(args, resource, audit, inner.id, snapshot);
      }
      return args.messageList;
    },
  } as InputProcessor;
}

function validateOutputProcessors(
  processors: readonly GuardedOutputProcessor[],
): readonly GuardedOutputProcessor[] {
  if (!Array.isArray(processors)) {
    throw new TypeError(
      'createGuardedAgent: applicationOutputProcessors must be an array',
    );
  }
  for (const processor of processors) {
    if (!processor || typeof processor !== 'object') {
      throw new TypeError(
        'createGuardedAgent: application output processors must be processor objects',
      );
    }
    assertProcessorId(processor, 'application output');
    for (const hook of OUTPUT_PROCESSOR_REQUIRED_HOOKS) {
      if (!hasHook(processor, hook)) {
        throw new TypeError(
          `createGuardedAgent: output processor '${processor.id}' must implement ${hook}`,
        );
      }
    }
    for (const hook of OUTPUT_PROCESSOR_FORBIDDEN_HOOKS) {
      if (hasHook(processor, hook)) {
        throw new TypeError(
          `createGuardedAgent: output processor '${processor.id}' must not implement ${hook}`,
        );
      }
    }
  }
  return Object.freeze([...processors]);
}

function guardedCallOptions(options: unknown): GuardedAgentCallOptions {
  if (!options || typeof options !== 'object' || Array.isArray(options)) {
    throw new TypeError(
      'GuardedAgent: options must be a plain object with requestContext',
    );
  }
  const prototype = Object.getPrototypeOf(options);
  if (prototype !== Object.prototype && prototype !== null) {
    throw new TypeError('GuardedAgent: options must be a plain object');
  }
  for (const key of Reflect.ownKeys(options)) {
    if (typeof key !== 'string' || !GUARDED_CALL_OPTION_KEYS.has(key)) {
      throw new TypeError(
        `GuardedAgent: call option '${String(key)}' is not allowed`,
      );
    }
    const descriptor = Object.getOwnPropertyDescriptor(options, key);
    if (descriptor?.get || descriptor?.set) {
      throw new TypeError(
        `GuardedAgent: call option '${key}' must be a data property`,
      );
    }
  }
  const candidate = options as Partial<GuardedAgentCallOptions>;
  if (!(candidate.requestContext instanceof RequestContext)) {
    throw new TypeError(
      'GuardedAgent: requestContext is required and must be a RequestContext',
    );
  }
  if (candidate.runId !== undefined && typeof candidate.runId !== 'string') {
    throw new TypeError('GuardedAgent: runId must be a string');
  }
  const memory =
    candidate.memory !== undefined
      ? guardedMemoryOption(candidate.memory as unknown)
      : undefined;
  return Object.freeze({
    requestContext: candidate.requestContext,
    ...(candidate.runId !== undefined ? { runId: candidate.runId } : {}),
    ...(memory !== undefined ? { memory } : {}),
    ...(candidate.abortSignal !== undefined
      ? { abortSignal: candidate.abortSignal }
      : {}),
  });
}

const MEMORY_OPTION_KEYS: ReadonlySet<string> = new Set(['thread', 'resource']);
const MEMORY_THREAD_KEYS: ReadonlySet<string> = new Set(['id']);

// The own data fields of a plain object, or a TypeError naming what is
// refused.
function readDataFields(
  label: string,
  value: unknown,
  keys: ReadonlySet<string>,
): Readonly<Record<string, unknown>> {
  const prototype =
    typeof value === 'object' && value !== null && !Array.isArray(value)
      ? Object.getPrototypeOf(value)
      : undefined;
  if (prototype !== Object.prototype && prototype !== null) {
    throw new TypeError(`GuardedAgent: ${label} must be a plain object`);
  }
  const fields = value as Readonly<Record<string, unknown>>;
  for (const key of Reflect.ownKeys(fields)) {
    if (typeof key !== 'string' || !keys.has(key)) {
      throw new TypeError(
        `GuardedAgent: ${label} field '${String(key)}' is not allowed`,
      );
    }
    const descriptor = Object.getOwnPropertyDescriptor(fields, key);
    if (descriptor?.get || descriptor?.set) {
      throw new TypeError(
        `GuardedAgent: ${label} field '${key}' must be a data property`,
      );
    }
  }
  return fields;
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

// Mastra takes a memory option's `options` as the call's memory configuration
// and saves a thread object's other fields; working memory renders both, the
// thread's metadata included, into the system prompt, which the input policies
// do not read. Only the thread and resource ids pass, copied.
function guardedMemoryOption(
  memory: unknown,
): NonNullable<GuardedAgentCallOptions['memory']> {
  const fields = readDataFields('memory', memory, MEMORY_OPTION_KEYS);
  const { thread, resource } = fields;
  let threadId: string | { readonly id: string };
  if (isNonEmptyString(thread)) {
    threadId = thread;
  } else if (typeof thread === 'object' && thread !== null) {
    const { id } = readDataFields('memory.thread', thread, MEMORY_THREAD_KEYS);
    if (!isNonEmptyString(id)) {
      throw new TypeError(
        'GuardedAgent: memory.thread.id must be a non-empty string',
      );
    }
    threadId = Object.freeze({ id });
  } else {
    throw new TypeError(
      'GuardedAgent: memory.thread must be a non-empty string or an object whose only field is a non-empty string id',
    );
  }
  if (!isNonEmptyString(resource)) {
    throw new TypeError(
      'GuardedAgent: memory.resource must be a non-empty string',
    );
  }
  return Object.freeze({ thread: threadId, resource });
}

// Mastra moves a system-role message into the call's system messages, which
// the input policies do not read, and gives it instruction authority. System
// instructions belong in the agent's `instructions`. The entries are
// normalized as Mastra's `MessageList.add` normalizes them: it flattens one
// nested list and throws on a deeper one, which is refused here first.
function assertNoSystemMessages(messages: unknown): void {
  const entries: unknown[] = Array.isArray(messages)
    ? messages.flat()
    : [messages];
  for (const message of entries) {
    if (Array.isArray(message)) {
      throw new TypeError(
        'GuardedAgent: a message list nested more than one level deep is not accepted',
      );
    }
    if (
      typeof message === 'object' &&
      message !== null &&
      (message as { role?: unknown }).role === 'system'
    ) {
      throw new TypeError(
        "GuardedAgent: a message with role 'system' is not accepted; set system instructions through the agent's instructions",
      );
    }
  }
}

function directAuthorizationError(reason: string): never {
  throw new Error(`GuardedAgent authorization denied: ${reason}`);
}

class GuardedAgent<
  TAgentId extends string,
  TTools extends ToolsInput,
  TRequestContext extends Record<string, unknown> | unknown,
> extends Agent<TAgentId, TTools, undefined, TRequestContext, false> {
  readonly allowedRoles: readonly Role[];
  readonly allowedPrincipalKinds: readonly PrincipalKind[];
  readonly maxSteps: number;
  readonly [GUARDED_AGENT_HOST_PROTOCOL]: GuardedAgentHostProtocol;
  readonly #audit: AuditLogger;
  readonly #applicationInputProcessors: readonly InputProcessor[];
  readonly #applicationOutputProcessors: readonly GuardedOutputProcessor[];
  readonly #inputAssets: InputProcessor;
  readonly #clientToolOutput: InputProcessor;
  readonly #policy: PolicyEngine;
  readonly #rbac: RBACMiddleware;
  readonly #toolChoice: GuardedToolChoice;

  constructor(options: GuardedAgentConfig<TAgentId, TTools, TRequestContext>) {
    assertConstructionOptions(options);
    assertKnownFields(
      'createGuardedAgent: config',
      options,
      GUARDED_AGENT_CONFIG_KEYS,
    );
    if (!options.audit || typeof options.audit.record !== 'function') {
      throw new TypeError('createGuardedAgent: audit must be an AuditLogger');
    }
    if (!Number.isSafeInteger(options.maxSteps) || options.maxSteps < 1) {
      throw new TypeError(
        'createGuardedAgent: maxSteps must be a positive safe integer',
      );
    }
    if (!Array.isArray(options.policies)) {
      throw new TypeError('createGuardedAgent: policies must be an array');
    }
    const allowedRoles = readAllowedRoles(
      'createGuardedAgent',
      options.allowedRoles,
    );
    const allowedPrincipalKinds = assertPrincipalKinds(
      options.allowedPrincipalKinds,
      'createGuardedAgent',
    );
    const allowedInputAssetOrigins = readAllowedInputAssetOrigins(
      options.allowedInputAssetOrigins,
    );
    const maxSteps = options.maxSteps;
    const toolChoice = assertToolChoice(options.toolChoice);
    // Wrapped before any hand-off to Mastra: its standard loop runs the
    // per-call processors, not listInputProcessors.
    const applicationInputProcessors = Object.freeze(
      validateInputProcessors(options.applicationInputProcessors ?? []).map(
        (processor) =>
          guardInputProcessor(processor, `agent:${options.id}`, options.audit),
      ),
    );
    const applicationOutputProcessors = validateOutputProcessors(
      options.applicationOutputProcessors ?? [],
    );
    const {
      allowedRoles: _allowedRoles,
      allowedPrincipalKinds: _allowedPrincipalKinds,
      allowedInputAssetOrigins: _allowedInputAssetOrigins,
      policies,
      audit,
      maxSteps: _maxSteps,
      toolChoice: _toolChoice,
      applicationInputProcessors: _applicationInputProcessors,
      applicationOutputProcessors: _applicationOutputProcessors,
      ...agentConfig
    } = options;
    const policy = new PolicyEngine({
      policies: Object.freeze([...policies]),
      audit,
      holdBack: true,
      resource: `agent:${options.id}`,
    });
    if (policy.objectOnlyPolicyNames.length > 0) {
      throw new TypeError(
        `createGuardedAgent: object-only polic${policy.objectOnlyPolicyNames.length === 1 ? 'y' : 'ies'} [${policy.objectOnlyPolicyNames.join(', ')}] cannot be enforced because guarded structured output is unavailable under the tested @mastra/core peer; include 'answer' in each affected policy's channels`,
      );
    }
    const rbac = new RBACMiddleware({
      allowedRoles,
      allowedPrincipalKinds,
      audit,
      resource: `agent:${options.id}`,
    });
    const inputAssets = inputAssetProcessor(
      allowedInputAssetOrigins,
      `agent:${options.id}`,
      audit,
    );
    super({
      ...agentConfig,
      inputProcessors: [
        ...applicationInputProcessors,
      ] as InputProcessorOrWorkflow[],
      outputProcessors: [
        ...applicationOutputProcessors,
      ] as OutputProcessorOrWorkflow[],
      defaultOptions: {
        maxSteps,
        toolChoice,
        disableBackgroundTasks: true,
      },
    } as AgentConfig<TAgentId, TTools, undefined, TRequestContext, false>);
    this.allowedRoles = allowedRoles;
    this.allowedPrincipalKinds = allowedPrincipalKinds;
    this.maxSteps = maxSteps;
    this[GUARDED_AGENT_HOST_PROTOCOL] = Object.freeze({
      version: 1,
      supportsDurableStructuredOutput: false,
    });
    this.#audit = audit;
    this.#applicationInputProcessors = applicationInputProcessors;
    this.#applicationOutputProcessors = applicationOutputProcessors;
    this.#inputAssets = inputAssets;
    // Core's execution assembly applies renamed keys and converted tool types
    // that its own client-output mapper reads.
    this.#clientToolOutput = clientToolOutputProcessor(
      (toolOptions) => this.getToolsForExecution(toolOptions),
      (args) =>
        failInputProcessor(
          args,
          `agent:${options.id}`,
          audit,
          CLIENT_TOOL_OUTPUT_PROCESSOR_ID,
        ),
    );
    this.#policy = policy;
    this.#rbac = rbac;
    this.#toolChoice = toolChoice;
    this.disableBackgroundTasks();
  }

  override async generate(
    messages: MessageListInput,
    rawOptions?: unknown,
    // biome-ignore lint/suspicious/noExplicitAny: the protected subclass must remain override-compatible with every inherited structured-output overload; runtime validation rejects structured output and the factory narrows the public handle to undefined.
  ): Promise<FullOutput<any>> {
    const options = guardedCallOptions(rawOptions);
    assertNoSystemMessages(messages);
    this.#preauthorize(options.requestContext);
    return super.generate(messages, this.#executionOptions(options));
  }

  override async stream(
    messages: MessageListInput,
    rawOptions?: unknown,
    // biome-ignore lint/suspicious/noExplicitAny: the protected subclass must remain override-compatible with every inherited structured-output overload; runtime validation rejects structured output and the factory narrows the public handle to undefined.
  ): Promise<MastraModelOutput<any>> {
    const options = guardedCallOptions(rawOptions);
    assertNoSystemMessages(messages);
    this.#preauthorize(options.requestContext);
    return super.stream(messages, this.#executionOptions(options));
  }

  override async listInputProcessors(
    _requestContext?: RequestContext,
  ): Promise<InputProcessorOrWorkflow[]> {
    return [
      this.#rbac,
      ...this.#applicationInputProcessors,
      this.#inputAssets,
      this.#clientToolOutput,
      this.#policy,
    ];
  }

  override async listOutputProcessors(
    _requestContext?: RequestContext,
  ): Promise<OutputProcessorOrWorkflow[]> {
    return [...this.#applicationOutputProcessors, this.#policy];
  }

  #preauthorize(requestContext: RequestContext): void {
    authorizeActor({
      allowedRoles: this.allowedRoles,
      // Direct calls bypass the processor chain entirely, so this gate must
      // carry the same kind allowlist or it is a hole around the middleware.
      allowedPrincipalKinds: this.allowedPrincipalKinds,
      audit: this.#audit,
      resource: `agent:${this.id}`,
      requestContext,
      resolveActor: () => actorFromRequestContext(requestContext),
      deny: directAuthorizationError,
    });
  }

  #executionOptions(
    options: GuardedAgentCallOptions,
  ): AgentExecutionOptionsBase<unknown> & { structuredOutput?: never } {
    return {
      requestContext: options.requestContext,
      ...(options.runId !== undefined ? { runId: options.runId } : {}),
      ...(options.memory !== undefined ? { memory: options.memory } : {}),
      ...(options.abortSignal !== undefined
        ? { abortSignal: options.abortSignal }
        : {}),
      maxSteps: this.maxSteps,
      toolChoice: this.#toolChoice,
      disableBackgroundTasks: true,
      inputProcessors: [
        ...this.#applicationInputProcessors,
        this.#inputAssets,
        this.#clientToolOutput,
        this.#policy,
      ],
      outputProcessors: [...this.#applicationOutputProcessors, this.#policy],
    };
  }
}

/**
 * Construct a mandatory-policy agent and return only its narrow guarded
 * handle.
 */
export function createGuardedAgent<
  TAgentId extends string,
  TTools extends ToolsInput = ToolsInput,
  TRequestContext extends Record<string, unknown> | unknown = unknown,
>(
  options: GuardedAgentConfig<TAgentId, TTools, TRequestContext>,
): GuardedAgentHandle {
  const agent = new GuardedAgent(options);
  guardedAgentHandles.add(agent);
  return agent;
}
